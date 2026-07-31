from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from collections import deque
import statistics
from typing import Any

from .config import ControllerConfig
from .dynamic_model import GScaleModel
from .blackbox_model import BlackboxGScaleModel
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
    pressure_ref: float | None = None
    gain_stddev_pct: float | None = None
    gain_lower_bound_pct: float | None = None
    model_distance: float | None = None
    model_confidence: float | None = None
    nearest_anchor: str | None = None
    pressure_baseline: float | None = None
    feature_coverage: float | None = None
    model_contributions: dict[str, float] | None = None

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
        # No timestamp means the initial placement was not reached by a
        # recent controller action, so there is no dwell period to enforce.
        return (now - since) / 1e9 if since is not None else float("inf")

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


class ContinuousBenefitPolicy(PressurePolicy):
    """Continuous black-box policy over P_ref and live database KPI context."""

    def __init__(self, config: ControllerConfig, model: GScaleModel):
        super().__init__(config)
        self.model = model
        self.windows: deque[dict[str, Any]] = deque(
            maxlen=config.decision_window_samples
        )
        self.last_kpi_key: tuple[str, int] | None = None
        self.pressure_baseline: float | None = None
        self.pressure_change_matches = 0
        self.shadow_recommendation: ScalingState | None = None
        self.pending_rebase = False

    def _reset_windows(self, *, reset_baseline: bool) -> None:
        self.windows.clear()
        self.last_kpi_key = None
        self.pressure_change_matches = 0
        if reset_baseline:
            self.pressure_baseline = None
            self.shadow_recommendation = None
            self.pending_rebase = False

    def force_state(self, state: ScalingState, timestamp_ns: int | None = None) -> None:
        super().force_state(state, timestamp_ns)
        if hasattr(self, "windows"):
            self._reset_windows(reset_baseline=False)
            self.pending_rebase = False

    def _decision(
        self,
        sample: MetricSample,
        *,
        reason: str,
        action: str | None = None,
        current: ScalingState | None = None,
        estimate: Any = None,
    ) -> Decision:
        return Decision(
            realtime_ns=sample.realtime_ns,
            current_state=(current or self.state).value,
            target_state=self.state.value,
            action=action,
            reason=reason,
            expand_matches=self.pressure_change_matches,
            shrink_elapsed_seconds=0.0,
            decision_source="continuous_g_scale",
            expected_gain_pct=(estimate.expected_gain_pct if estimate else None),
            pressure_ref=(estimate.pressure_ref if estimate else None),
            gain_stddev_pct=(estimate.gain_stddev_pct if estimate else None),
            gain_lower_bound_pct=(estimate.gain_lower_bound_pct if estimate else None),
            model_distance=(estimate.model_distance if estimate else None),
            model_confidence=(estimate.confidence if estimate else None),
            nearest_anchor=(estimate.nearest_anchor if estimate else None),
            pressure_baseline=self.pressure_baseline,
        )

    def evaluate(self, sample: MetricSample) -> Decision:
        if not sample.workload_active:
            self._reset_windows(reset_baseline=True)
            return self._decision(sample, reason="workload_inactive")
        if (
            not sample.valid
            or sample.run_cpu_equiv is None
            or sample.rq_cpu_equiv is None
        ):
            self._reset_windows(reset_baseline=False)
            return self._decision(sample, reason="invalid_sample")

        kpi = sample.kpi
        if not kpi or not kpi.get("complete"):
            return self._decision(sample, reason="kpi_window_incomplete")
        key = (str(kpi.get("phase", "")), int(kpi.get("sequence", 0)))
        if key == self.last_kpi_key:
            return self._decision(sample, reason="duplicate_kpi_window")
        if self.last_kpi_key is not None:
            same_phase = key[0] == self.last_kpi_key[0]
            consecutive = key[1] == self.last_kpi_key[1] + 1
            if not same_phase or not consecutive:
                self._reset_windows(reset_baseline=not same_phase)
        self.last_kpi_key = key
        pressure = self.model.pressure_ref(
            self.state.value, sample.run_cpu_equiv, sample.rq_cpu_equiv
        )
        self.windows.append(
            {
                "pressure_ref": pressure,
                "throughput_ops_s": float(kpi.get("throughput_ops_s") or 0.0),
                "p99_us": float(kpi.get("max_client_p99_latency_us") or 0.0),
            }
        )
        if len(self.windows) < self.config.decision_window_samples:
            return self._decision(sample, reason="decision_window_incomplete")

        pressure_ref = statistics.median(
            float(row["pressure_ref"]) for row in self.windows
        )
        throughput = statistics.median(
            float(row["throughput_ops_s"]) for row in self.windows
        )
        p99 = statistics.median(float(row["p99_us"]) for row in self.windows)
        if throughput <= 0 or p99 <= 0:
            return self._decision(sample, reason="invalid_kpi_values")
        try:
            estimate = self.model.estimate(
                self.state.value,
                pressure_ref,
                throughput,
                p99,
                uncertainty_multiplier=self.config.gain_uncertainty_multiplier,
            )
        except ValueError:
            estimate = None

        high_pressure = bool(
            sample.run_pressure is not None
            and sample.rq_pressure is not None
            and sample.run_pressure >= self.config.expand_run_pressure
            and sample.rq_pressure >= self.config.expand_rq_pressure
        )
        if estimate is None or estimate.model_distance > self.config.maximum_model_distance:
            if self.state == ScalingState.ONE_NODE and high_pressure:
                if (
                    self.config.mode == "shadow"
                    and self.shadow_recommendation == ScalingState.TWO_NODE
                ):
                    return self._decision(
                        sample,
                        reason="shadow_recommendation_stable",
                        estimate=estimate,
                    )
                current = self.state
                self.state = ScalingState.TWO_NODE
                self.last_transition_ns = sample.monotonic_ns
                self.last_action = "expand"
                self.shadow_recommendation = self.state
                self._reset_windows(reset_baseline=False)
                return self._decision(
                    sample,
                    reason="pressure_safety_expand",
                    action="expand",
                    current=current,
                    estimate=estimate,
                )
            return self._decision(
                sample, reason="outside_continuous_model", estimate=estimate
            )

        baseline_changed = self.pressure_baseline is None
        if self.pressure_baseline is not None:
            delta = abs(pressure_ref - self.pressure_baseline)
            relative_base = max(abs(self.pressure_baseline), 0.25)
            changed = bool(
                delta >= self.config.pressure_change_absolute
                and delta / relative_base >= self.config.pressure_change_relative
            )
            self.pressure_change_matches = (
                self.pressure_change_matches + 1 if changed else 0
            )
            baseline_changed = (
                self.pressure_change_matches
                >= self.config.pressure_change_confirm_samples
            )
        if baseline_changed:
            self.pressure_baseline = pressure_ref
            self.pressure_change_matches = 0
            self.pending_rebase = True

        emergency = self.state == ScalingState.ONE_NODE and high_pressure
        if not self.pending_rebase and not emergency:
            return self._decision(
                sample, reason="pressure_baseline_stable", estimate=estimate
            )

        gain = estimate.gain_lower_bound_pct
        if gain < self.config.minimum_expected_gain_pct and not emergency:
            # A positive but uncertain estimate may cross the confidence gate
            # as later KPI windows replace startup/transient observations.
            self.pending_rebase = estimate.expected_gain_pct > 0
            return self._decision(
                sample, reason="gain_lower_bound_below_gate", estimate=estimate
            )
        dwell = self._elapsed(sample.monotonic_ns, self.last_transition_ns)
        cooldown = (
            self.last_transition_ns is not None
            and dwell < self.config.cooldown_seconds
        )
        if cooldown:
            return self._decision(sample, reason="cooldown", estimate=estimate)
        if (
            self.state == ScalingState.TWO_NODE
            and dwell < self.config.minimum_two_node_dwell_seconds
        ):
            return self._decision(
                sample, reason="minimum_two_node_dwell", estimate=estimate
            )

        current = self.state
        target = (
            ScalingState.TWO_NODE
            if current == ScalingState.ONE_NODE
            else ScalingState.ONE_NODE
        )
        if self.config.mode == "shadow" and self.shadow_recommendation == target:
            self.pending_rebase = False
            return self._decision(
                sample, reason="shadow_recommendation_stable", estimate=estimate
            )
        self.state = target
        action = "expand" if self.state == ScalingState.TWO_NODE else "shrink"
        self.last_transition_ns = sample.monotonic_ns
        self.last_action = action
        self.shadow_recommendation = self.state
        self.pending_rebase = False
        self._reset_windows(reset_baseline=False)
        return self._decision(
            sample,
            reason="continuous_gain",
            action=action,
            current=current,
            estimate=estimate,
        )


class BlackboxBenefitPolicy(PressurePolicy):
    """System-telemetry-only policy; application KPI is never an input."""

    def __init__(self, config: ControllerConfig, model: BlackboxGScaleModel):
        super().__init__(config)
        self.model = model
        self.matches = 0
        self.last_recommendation: ScalingState | None = None

    def force_state(self, state: ScalingState, timestamp_ns: int | None = None) -> None:
        super().force_state(state, timestamp_ns)
        if hasattr(self, "matches"):
            self.matches = 0

    def _decision(
        self,
        sample: MetricSample,
        reason: str,
        estimate: Any = None,
        action: str | None = None,
        current: ScalingState | None = None,
    ) -> Decision:
        return Decision(
            realtime_ns=sample.realtime_ns,
            current_state=(current or self.state).value,
            target_state=self.state.value,
            action=action,
            reason=reason,
            expand_matches=self.matches,
            shrink_elapsed_seconds=0.0,
            decision_source="blackbox_system_g_scale",
            expected_gain_pct=estimate.expected_gain_pct if estimate else None,
            model_distance=estimate.model_distance if estimate else None,
            model_confidence=estimate.confidence if estimate else None,
            feature_coverage=estimate.feature_coverage if estimate else None,
            model_contributions=estimate.contributions if estimate else None,
        )

    def evaluate(self, sample: MetricSample) -> Decision:
        if not sample.workload_active:
            self.matches = 0
            self.last_recommendation = None
            return self._decision(sample, "workload_inactive")
        if not sample.valid:
            self.matches = 0
            return self._decision(sample, "invalid_sample")
        direction = "expand" if self.state == ScalingState.ONE_NODE else "shrink"
        estimate = self.model.estimate(direction, sample.system_features)
        high_pressure = bool(
            self.state == ScalingState.ONE_NODE
            and sample.run_pressure is not None
            and sample.rq_pressure is not None
            and sample.run_pressure >= self.config.expand_run_pressure
            and sample.rq_pressure >= self.config.expand_rq_pressure
        )
        if (
            not high_pressure
            and (
                estimate.feature_coverage < self.config.minimum_feature_coverage
                or estimate.confidence < self.config.minimum_model_confidence
            )
        ):
            self.matches = 0
            return self._decision(sample, "insufficient_system_evidence", estimate)
        gain_gate = (
            self.config.minimum_expected_gain_pct
            if direction == "expand"
            else -self.config.minimum_expected_gain_pct
        )
        beneficial = estimate.expected_gain_pct >= gain_gate
        if not beneficial and not high_pressure:
            self.matches = 0
            self.last_recommendation = None
            return self._decision(sample, "predicted_gain_below_gate", estimate)
        target = (
            ScalingState.TWO_NODE
            if self.state == ScalingState.ONE_NODE else ScalingState.ONE_NODE
        )
        self.matches += 1
        if self.matches < self.config.decision_window_samples:
            return self._decision(sample, "system_gain_confirming", estimate)
        dwell = self._elapsed(sample.monotonic_ns, self.last_transition_ns)
        if self.last_transition_ns is not None and dwell < self.config.cooldown_seconds:
            return self._decision(sample, "cooldown", estimate)
        if self.state == ScalingState.TWO_NODE and dwell < self.config.minimum_two_node_dwell_seconds:
            return self._decision(sample, "minimum_two_node_dwell", estimate)
        if self.config.mode == "shadow" and self.last_recommendation == target:
            self.matches = 0
            return self._decision(sample, "shadow_recommendation_stable", estimate)
        current = self.state
        self.state = target
        action = "expand" if target == ScalingState.TWO_NODE else "shrink"
        self.last_action = action
        self.last_transition_ns = sample.monotonic_ns
        self.last_recommendation = target
        self.matches = 0
        return self._decision(
            sample,
            "pressure_safety_expand" if high_pressure else "blackbox_predicted_gain",
            estimate,
            action,
            current,
        )
