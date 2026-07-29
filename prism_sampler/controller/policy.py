from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from collections import deque
from typing import Any

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
    decision_source: str = "pressure"
    expected_gain_pct: float | None = None
    signature_threads: int | None = None
    signature_distance: float | None = None

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


class BenefitPolicy(PressurePolicy):
    def __init__(self, config: ControllerConfig, signatures: list[dict[str, Any]]):
        super().__init__(config)
        self.signature_rows = [
            row for row in signatures if int(row.get("offered_threads", 0)) > 0
        ]
        self.signatures = {
            int(row["offered_threads"]): row
            for row in self.signature_rows
        }
        self.kpi_windows: deque[dict[str, Any]] = deque(maxlen=config.decision_window_samples)
        self.last_kpi_key: tuple[str, int] | None = None

    def force_state(self, state: ScalingState, timestamp_ns: int | None = None) -> None:
        super().force_state(state, timestamp_ns)
        self.kpi_windows.clear()

    def evaluate(self, sample: MetricSample) -> Decision:
        if not sample.workload_active:
            self.kpi_windows.clear()
            return super().evaluate(sample)
        kpi = sample.kpi
        if kpi and kpi.get("complete"):
            key = (str(kpi.get("phase", "")), int(kpi.get("sequence", 0)))
            if key != self.last_kpi_key:
                if self.kpi_windows and self.kpi_windows[-1].get("phase") != key[0]:
                    self.kpi_windows.clear()
                self.kpi_windows.append({
                    **dict(kpi),
                    "_run_cpu_equiv": sample.run_cpu_equiv,
                    "_rq_cpu_equiv": sample.rq_cpu_equiv,
                })
                self.last_kpi_key = key
        if len(self.kpi_windows) < self.config.decision_window_samples:
            fallback = super().evaluate(sample)
            return Decision(
                **{
                    **fallback.to_dict(),
                    "reason": (
                        fallback.reason
                        if fallback.action
                        else "kpi_window_incomplete"
                    ),
                    "decision_source": (
                        "pressure_safety" if fallback.action else "benefit"
                    ),
                }
            )
        offered = [int(row.get("offered_threads", 0)) for row in self.kpi_windows]
        signature = None
        signature_distance = None
        pressure_signatures_available = False
        prefix = "one" if self.state == ScalingState.ONE_NODE else "two"
        observed_runs = [
            float(row["_run_cpu_equiv"])
            for row in self.kpi_windows if row.get("_run_cpu_equiv") is not None
        ]
        observed_rqs = [
            float(row["_rq_cpu_equiv"])
            for row in self.kpi_windows if row.get("_rq_cpu_equiv") is not None
        ]
        if observed_runs and observed_rqs:
            observed_run = sorted(observed_runs)[len(observed_runs) // 2]
            observed_rq = sorted(observed_rqs)[len(observed_rqs) // 2]
            candidates = []
            for row in self.signature_rows:
                expected_run = row.get(f"{prefix}_run_cpu_equiv")
                expected_rq = row.get(f"{prefix}_rq_cpu_equiv")
                if expected_run is None or expected_rq is None:
                    continue
                try:
                    expected_run = float(expected_run)
                    expected_rq = float(expected_rq)
                except (TypeError, ValueError):
                    continue
                if expected_run != expected_run or expected_rq != expected_rq:
                    continue
                distance = (
                    abs(observed_run - expected_run) / max(abs(expected_run), 1.0)
                    + abs(observed_rq - expected_rq) / max(abs(expected_rq), 1.0)
                ) / 2
                candidates.append((distance, int(row["offered_threads"]), row))
            if candidates:
                pressure_signatures_available = True
                signature_distance, _, signature = min(candidates, key=lambda value: value[:2])
                if signature_distance > self.config.maximum_signature_distance:
                    signature = None
        if (
            signature is None
            and not pressure_signatures_available
            and len(set(offered)) == 1
        ):
            signature = self.signatures.get(offered[0])
        if signature is None:
            fallback = super().evaluate(sample)
            return Decision(
                **{
                    **fallback.to_dict(),
                    "reason": (
                        fallback.reason if fallback.action else "outside_calibrated_signatures"
                    ),
                    "decision_source": "pressure_safety",
                    "signature_distance": signature_distance,
                }
            )
        one = float(signature["one_throughput_ops_s"])
        two = float(signature["two_throughput_ops_s"])
        preferred = ScalingState.TWO_NODE if two > one else ScalingState.ONE_NODE
        current_value = one if self.state == ScalingState.ONE_NODE else two
        alternate_value = two if self.state == ScalingState.ONE_NODE else one
        gain = 100.0 * (alternate_value / current_value - 1.0) if current_value else 0.0
        current = self.state
        action = None
        reason = "calibrated_state_is_current"
        dwell = self._elapsed(sample.monotonic_ns, self.last_transition_ns)
        cooldown = (
            self.last_transition_ns is not None
            and dwell < self.config.cooldown_seconds
        )
        if preferred != self.state:
            if gain < self.config.minimum_expected_gain_pct:
                reason = "expected_gain_below_gate"
            elif cooldown:
                reason = "cooldown"
            elif preferred == ScalingState.ONE_NODE and dwell < self.config.minimum_two_node_dwell_seconds:
                reason = "minimum_two_node_dwell"
            else:
                self.state = preferred
                self.last_transition_ns = sample.monotonic_ns
                action = "expand" if preferred == ScalingState.TWO_NODE else "shrink"
                self.last_action = action
                reason = "calibrated_benefit"
                self.kpi_windows.clear()
        return Decision(
            realtime_ns=sample.realtime_ns,
            current_state=current.value,
            target_state=self.state.value,
            action=action,
            reason=reason,
            expand_matches=0,
            shrink_elapsed_seconds=0.0,
            decision_source="benefit",
            expected_gain_pct=gain,
            signature_threads=int(signature["offered_threads"]),
            signature_distance=signature_distance,
        )
