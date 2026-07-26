from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum

from .config import ControllerConfig
from .models import MetricSample


class ScalingState(str, Enum):
    ONE_NODE = "one_node"
    TWO_NODE = "two_node"
    FAULT = "fault"


@dataclass(frozen=True)
class Decision:
    realtime_ns: int
    current_state: str
    target_state: str
    action: str | None
    reason: str
    expand_matches: int
    shrink_elapsed_seconds: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class PressurePolicy:
    def __init__(self, config: ControllerConfig):
        self.config = config
        self.state = ScalingState.ONE_NODE
        self.expand_matches = 0
        self.rollback_matches = 0
        self.shrink_since_ns: int | None = None
        self.last_transition_ns: int | None = None
        self.last_action: str | None = None

    def force_state(self, state: ScalingState, timestamp_ns: int | None = None) -> None:
        self.state = state
        if timestamp_ns is not None:
            self.last_transition_ns = timestamp_ns
        self.expand_matches = 0
        self.rollback_matches = 0
        self.shrink_since_ns = None

    def _elapsed(self, now: int, since: int | None) -> float:
        return (now - since) / 1e9 if since is not None else 0.0

    def evaluate(self, sample: MetricSample) -> Decision:
        now = sample.monotonic_ns
        current = self.state
        reason = "hold"
        action: str | None = None
        if not sample.workload_active:
            self.expand_matches = 0
            self.rollback_matches = 0
            self.shrink_since_ns = None
            reason = "workload_inactive"
        elif not sample.valid:
            self.expand_matches = 0
            self.rollback_matches = 0
            self.shrink_since_ns = None
            reason = "invalid_sample"
        else:
            high = bool(
                sample.run_pressure is not None
                and sample.rq_pressure is not None
                and sample.run_pressure >= self.config.expand_run_pressure
                and sample.rq_pressure >= self.config.expand_rq_pressure
            )
            low = bool(
                sample.run_cpu_equiv is not None
                and sample.rq_pressure is not None
                and sample.run_cpu_equiv <= self.config.shrink_run_cpu_equiv
                and sample.rq_pressure <= self.config.shrink_rq_pressure
            )
            cooldown = (
                self.last_transition_ns is not None
                and self._elapsed(now, self.last_transition_ns) < self.config.cooldown_seconds
            )
            if self.state == ScalingState.ONE_NODE:
                self.shrink_since_ns = None
                self.expand_matches = self.expand_matches + 1 if high else 0
                rollback = self.last_action == "shrink" and cooldown
                required = (
                    self.config.rollback_confirm_samples
                    if rollback
                    else self.config.decision_window_samples
                )
                if self.expand_matches >= required and (rollback or not cooldown):
                    self.state = ScalingState.TWO_NODE
                    self.last_transition_ns = now
                    self.last_action = "rollback_expand" if rollback else "expand"
                    action = self.last_action
                    reason = "sustained_high_pressure"
                    self.expand_matches = 0
                else:
                    reason = "high_pressure_confirming" if high else "one_node_pressure_below_gate"
            elif self.state == ScalingState.TWO_NODE:
                self.expand_matches = 0
                dwell = self._elapsed(now, self.last_transition_ns)
                if low:
                    self.shrink_since_ns = self.shrink_since_ns or now
                else:
                    self.shrink_since_ns = None
                shrink_elapsed = self._elapsed(now, self.shrink_since_ns)
                if (
                    low
                    and shrink_elapsed >= self.config.shrink_confirm_seconds
                    and dwell >= self.config.minimum_two_node_dwell_seconds
                    and not cooldown
                ):
                    self.state = ScalingState.ONE_NODE
                    self.last_transition_ns = now
                    self.last_action = "shrink"
                    action = "shrink"
                    reason = "sustained_low_pressure"
                    self.shrink_since_ns = None
                elif low:
                    reason = "low_pressure_confirming"
                else:
                    reason = "two_node_pressure_above_shrink_gate"
        return Decision(
            realtime_ns=sample.realtime_ns,
            current_state=current.value,
            target_state=self.state.value,
            action=action,
            reason=reason,
            expand_matches=self.expand_matches,
            shrink_elapsed_seconds=self._elapsed(now, self.shrink_since_ns),
        )
