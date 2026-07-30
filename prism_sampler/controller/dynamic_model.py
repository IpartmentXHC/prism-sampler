from __future__ import annotations

import csv
import itertools
import json
import math
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


SCHEMA = "prism-sampler.pressure-g-scale.v1"


@dataclass(frozen=True)
class PressureReferenceModel:
    reference_capacity_cpus: int
    two_run_coefficient: float
    two_rq_coefficient: float
    mapping_p95_error: float

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PressureReferenceModel:
        coefficients = value["coefficients"]
        return cls(
            reference_capacity_cpus=int(value["reference_capacity_cpus"]),
            two_run_coefficient=float(coefficients["run_coefficient"]),
            two_rq_coefficient=float(coefficients["rq_coefficient"]),
            mapping_p95_error=float(
                value.get("validation", {}).get(
                    "leave_one_load_out_p95_absolute_error", 0.0
                )
            ),
        )

    def estimate(self, state: str, run_cpu_equiv: float, rq_cpu_equiv: float) -> float:
        run = run_cpu_equiv / self.reference_capacity_cpus
        rq = rq_cpu_equiv / self.reference_capacity_cpus
        if state == "one_node":
            return run + rq
        if state == "two_node":
            return self.two_run_coefficient * run + self.two_rq_coefficient * rq
        raise ValueError(f"unknown scaling state: {state}")


@dataclass(frozen=True)
class GainAnchor:
    label: str
    pressure_ref: float
    one_throughput_ops_s: float
    two_throughput_ops_s: float
    one_p99_us: float
    two_p99_us: float

    def current_throughput(self, state: str) -> float:
        return (
            self.one_throughput_ops_s
            if state == "one_node"
            else self.two_throughput_ops_s
        )

    def current_p99(self, state: str) -> float:
        return self.one_p99_us if state == "one_node" else self.two_p99_us

    def alternate_gain_pct(self, state: str) -> float:
        current = self.current_throughput(state)
        alternate = (
            self.two_throughput_ops_s
            if state == "one_node"
            else self.one_throughput_ops_s
        )
        return 100.0 * (alternate / current - 1.0)


@dataclass(frozen=True)
class GainEstimate:
    pressure_ref: float
    expected_gain_pct: float
    gain_stddev_pct: float
    gain_lower_bound_pct: float
    model_distance: float
    confidence: float
    effective_anchor_count: float
    nearest_anchor: str


class GScaleModel:
    def __init__(self, value: dict[str, Any]):
        if value.get("schema") != SCHEMA:
            raise ValueError(f"unsupported dynamic model schema: {value.get('schema')}")
        self.raw = value
        self.pressure = PressureReferenceModel.from_dict(value["pressure_reference"])
        self.anchors = [GainAnchor(**row) for row in value["anchors"]]
        scales = value["g_scale"]["feature_scales"]
        self.pressure_scale = float(scales["pressure_ref"])
        self.throughput_log_scale = float(scales["log_throughput"])
        self.p99_log_scale = float(scales["log_p99"])

    @classmethod
    def load(cls, path: Path) -> GScaleModel:
        return cls(json.loads(path.read_text(encoding="utf-8")))

    def pressure_ref(self, state: str, run: float, rq: float) -> float:
        return self.pressure.estimate(state, run, rq)

    def _distance(
        self,
        anchor: GainAnchor,
        state: str,
        pressure_ref: float,
        throughput_ops_s: float,
        p99_us: float,
    ) -> float:
        pressure = (pressure_ref - anchor.pressure_ref) / self.pressure_scale
        throughput = math.log(
            max(throughput_ops_s, 1e-9) / max(anchor.current_throughput(state), 1e-9)
        ) / self.throughput_log_scale
        latency = math.log(
            max(p99_us, 1e-9) / max(anchor.current_p99(state), 1e-9)
        ) / self.p99_log_scale
        return math.sqrt(pressure * pressure + throughput * throughput + latency * latency)

    def estimate(
        self,
        state: str,
        pressure_ref: float,
        throughput_ops_s: float,
        p99_us: float,
        *,
        uncertainty_multiplier: float,
    ) -> GainEstimate:
        weighted = []
        for anchor in self.anchors:
            distance = self._distance(
                anchor, state, pressure_ref, throughput_ops_s, p99_us
            )
            weighted.append(
                (math.exp(-0.5 * distance * distance), distance, anchor)
            )
        total = sum(weight for weight, _, _ in weighted)
        if total <= 1e-12:
            raise ValueError("observation is outside dynamic model support")
        mean = sum(
            weight * anchor.alternate_gain_pct(state)
            for weight, _, anchor in weighted
        ) / total
        variance = sum(
            weight * (anchor.alternate_gain_pct(state) - mean) ** 2
            for weight, _, anchor in weighted
        ) / total
        squared_weights = sum((weight / total) ** 2 for weight, _, _ in weighted)
        nearest_weight, nearest_distance, nearest = min(
            weighted, key=lambda item: item[1]
        )
        del nearest_weight
        stddev = math.sqrt(max(variance, 0.0))
        return GainEstimate(
            pressure_ref=pressure_ref,
            expected_gain_pct=mean,
            gain_stddev_pct=stddev,
            gain_lower_bound_pct=mean - uncertainty_multiplier * stddev,
            model_distance=nearest_distance,
            confidence=math.exp(-0.5 * nearest_distance * nearest_distance),
            effective_anchor_count=(1.0 / squared_weights if squared_weights else 0.0),
            nearest_anchor=nearest.label,
        )


def _read_anchors(path: Path) -> list[GainAnchor]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    return [
        GainAnchor(
            label=str(row["load"]),
            pressure_ref=float(row["pressure_ref"]),
            one_throughput_ops_s=float(row["one_throughput_ops_s"]),
            two_throughput_ops_s=float(row["two_throughput_ops_s"]),
            one_p99_us=float(row["one_p99_us"]),
            two_p99_us=float(row["two_p99_us"]),
        )
        for row in rows
    ]


def _predict(
    observation: GainAnchor,
    state: str,
    training: list[GainAnchor],
    scales: tuple[float, float, float],
) -> float | None:
    pressure_scale, throughput_scale, p99_scale = scales
    weighted = []
    for anchor in training:
        distance_squared = (
            (observation.pressure_ref - anchor.pressure_ref) / pressure_scale
        ) ** 2
        distance_squared += (
            math.log(
                observation.current_throughput(state)
                / anchor.current_throughput(state)
            )
            / throughput_scale
        ) ** 2
        distance_squared += (
            math.log(observation.current_p99(state) / anchor.current_p99(state))
            / p99_scale
        ) ** 2
        weighted.append(
            (math.exp(-0.5 * distance_squared), anchor.alternate_gain_pct(state))
        )
    total = sum(weight for weight, _ in weighted)
    if total <= 1e-12:
        return None
    return sum(weight * gain for weight, gain in weighted) / total


def build_dynamic_model(
    anchors_path: Path, pressure_model_path: Path, output: Path
) -> dict[str, Any]:
    anchors = _read_anchors(anchors_path)
    if len(anchors) < 6:
        raise ValueError("at least six paired static anchors are required")
    candidates = itertools.product(
        (0.15, 0.25, 0.40, 0.60, 1.0),
        (0.10, 0.20, 0.40, 0.80),
        (0.25, 0.50, 1.0, 2.0),
    )
    scored = []
    for scales in candidates:
        errors = []
        directions = []
        unsupported = 0
        for held_out in anchors:
            training = [row for row in anchors if row.label != held_out.label]
            for state in ("one_node", "two_node"):
                prediction = _predict(held_out, state, training, scales)
                if prediction is None:
                    unsupported += 1
                    continue
                actual = held_out.alternate_gain_pct(state)
                errors.append(abs(prediction - actual))
                directions.append((prediction > 0) == (actual > 0))
        score = statistics.mean(errors) + unsupported * 100.0
        scored.append((score, scales, errors, directions, unsupported))
    score, scales, errors, directions, unsupported = min(scored, key=lambda row: row[0])
    pressure = json.loads(pressure_model_path.read_text(encoding="utf-8"))
    value = {
        "schema": SCHEMA,
        "pressure_reference": pressure,
        "g_scale": {
            "formula": (
                "Gaussian local regression of alternate-state throughput gain over "
                "P_ref, log(current throughput), log(current P99), conditioned on current state"
            ),
            "feature_scales": {
                "pressure_ref": scales[0],
                "log_throughput": scales[1],
                "log_p99": scales[2],
            },
            "online_profile_fields_used": [],
            "transition_cost_pct": {"expand": 0.0, "shrink": 0.0},
        },
        "anchors": [asdict(row) for row in anchors],
        "validation": {
            "method": "leave-one-workload-out",
            "rows": len(errors),
            "mean_absolute_error_pct_points": statistics.mean(errors),
            "median_absolute_error_pct_points": statistics.median(errors),
            "p95_absolute_error_pct_points": sorted(errors)[
                min(len(errors) - 1, math.ceil(0.95 * len(errors)) - 1)
            ],
            "direction_accuracy": sum(directions) / len(directions),
            "unsupported_rows": unsupported,
            "selection_score": score,
        },
        "limitations": [
            "Low-pressure leave-one-workload-out direction accuracy is not yet sufficient for blind active placement.",
            "Throughput and P99 are database KPI context, not YBA workload labels.",
            "The first model reports zero transition cost until randomized transition probes recalibrate it.",
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"output": str(output), **value["validation"]}


def replay_pressure_windows(
    windows_path: Path,
    model_path: Path,
    output: Path,
    *,
    minimum_expected_gain_pct: float = 2.0,
    gain_uncertainty_multiplier: float = 0.5,
) -> dict[str, Any]:
    from .config import ControllerConfig
    from .models import MetricSample
    from .policy import ContinuousBenefitPolicy, ScalingState

    model = GScaleModel.load(model_path)
    with windows_path.open(newline="", encoding="utf-8") as stream:
        windows = list(csv.DictReader(stream))
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in windows:
        groups.setdefault(
            (str(row["load"]), str(row["state"]), str(row["phase"])), []
        ).append(row)
    anchors = {anchor.label: anchor for anchor in model.anchors}
    details = []
    wrong_actions = correct_actions = missed_actions = 0
    windows_evaluated = 0
    for (label, state, phase), rows in sorted(groups.items()):
        rows.sort(key=lambda row: int(row["window_index"]))
        anchor = anchors[label]
        oracle = (
            "two_node"
            if anchor.two_throughput_ops_s > anchor.one_throughput_ops_s
            else "one_node"
        )
        policy = ContinuousBenefitPolicy(
            ControllerConfig(
                mode="shadow",
                cooldown_seconds=0,
                minimum_two_node_dwell_seconds=0,
                minimum_expected_gain_pct=minimum_expected_gain_pct,
                gain_uncertainty_multiplier=gain_uncertainty_multiplier,
            ),
            model,
        )
        recorded_state = ScalingState(state)
        policy.force_state(recorded_state, 0)
        actions = []
        reasons: dict[str, int] = {}
        for index, row in enumerate(rows, 1):
            throughput = anchor.current_throughput(state)
            p99 = anchor.current_p99(state)
            sample = MetricSample(
                realtime_ns=index * 10_000_000_000,
                monotonic_ns=index * 10_000_000_000,
                interval_seconds=10.0,
                workload_active=True,
                valid=True,
                run_cpu_equiv=float(row["run_cpu_equiv"]),
                rq_cpu_equiv=float(row["rq_cpu_equiv"]),
                run_pressure=float(row["run_cpu_equiv"]) / 32.0,
                rq_pressure=float(row["rq_cpu_equiv"]) / 32.0,
                tids_observed=0,
                kpi={
                    "schema": "replay.black-box-kpi.v1",
                    "phase": "opaque",
                    "sequence": index,
                    "complete": True,
                    "throughput_ops_s": throughput,
                    "max_client_p99_latency_us": p99,
                },
            )
            decision = policy.evaluate(sample)
            windows_evaluated += 1
            reasons[decision.reason] = reasons.get(decision.reason, 0) + 1
            if decision.action:
                actions.append(
                    {
                        "window": index,
                        "target": decision.target_state,
                        "gain_lower_bound_pct": decision.gain_lower_bound_pct,
                        "pressure_ref": decision.pressure_ref,
                    }
                )
                if decision.target_state == oracle:
                    correct_actions += 1
                else:
                    wrong_actions += 1
                policy.force_state(recorded_state, sample.monotonic_ns)
        opportunity = state != oracle
        correct = any(action["target"] == oracle for action in actions)
        if opportunity and not correct:
            missed_actions += 1
        details.append(
            {
                "load": label,
                "recorded_state": state,
                "phase": phase,
                "windows": len(rows),
                "oracle_state": oracle,
                "transition_opportunity": opportunity,
                "correct_transition": correct,
                "actions": actions,
                "reasons": reasons,
            }
        )
    report = {
        "schema": "prism-sampler.dynamic-replay.v1",
        "model": str(model_path.resolve()),
        "windows": str(windows_path.resolve()),
        "windows_evaluated": windows_evaluated,
        "phase_runs": len(details),
        "transition_opportunities": sum(
            bool(row["transition_opportunity"]) for row in details
        ),
        "correct_actions": correct_actions,
        "wrong_actions": wrong_actions,
        "missed_transition_runs": missed_actions,
        "oscillations": 0,
        "profile_fields_used_online": [],
        "minimum_expected_gain_pct": minimum_expected_gain_pct,
        "gain_uncertainty_multiplier": gain_uncertainty_multiplier,
        "details": details,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        key: value for key, value in report.items()
        if key not in {"details"}
    } | {"output": str(output)}


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def validate_hidden_shadow(
    experiments: list[Path], model_path: Path, manifest_path: Path, output: Path
) -> dict[str, Any]:
    model = GScaleModel.load(model_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    hidden = {str(row["phase"]): str(row["load"]) for row in manifest["phases"]}
    anchors = {anchor.label: anchor for anchor in model.anchors}
    rows = []
    for experiment in experiments:
        controller = experiment / "controller"
        resolved = json.loads(
            (controller / "resolved-config.json").read_text(encoding="utf-8")
        )
        actual_state = str(resolved["controller"]["initial_state"])
        actions = _jsonl(controller / "actions.jsonl")
        decisions = _jsonl(controller / "decisions.jsonl")
        kpis = _jsonl(controller / "kpi.jsonl")
        starts = {}
        for context_path in experiment.glob("runs/**/meta/phase.json"):
            context = json.loads(context_path.read_text(encoding="utf-8"))
            if context.get("workload_start_epoch_ns") is not None:
                starts[str(context.get("phase", ""))] = int(
                    context["workload_start_epoch_ns"]
                )
        for phase, load in hidden.items():
            anchor = anchors[load]
            oracle = (
                "two_node"
                if anchor.two_throughput_ops_s > anchor.one_throughput_ops_s
                else "one_node"
            )
            phase_actions = [
                row
                for row in actions
                if row.get("phase") == phase
                and row.get("action") not in {"initialize", "restore"}
            ]
            phase_decisions = [row for row in decisions if row.get("phase") == phase]
            phase_kpis = [row for row in kpis if row.get("phase") == phase]
            sequences = sorted(int(row["sequence"]) for row in phase_kpis)
            contiguous = bool(sequences) and sequences == list(
                range(sequences[0], sequences[-1] + 1)
            )
            complete = sum(bool(row.get("complete")) for row in phase_kpis)
            expected_action = oracle if oracle != actual_state else None
            correct = [row for row in phase_actions if row.get("to_state") == oracle]
            wrong = [row for row in phase_actions if row.get("to_state") != oracle]
            action_delay = None
            if correct and phase in starts:
                action_delay = (
                    int(correct[0]["realtime_ns"]) - starts[phase]
                ) / 1e9
            evidence = [
                row for row in phase_decisions if row.get("pressure_ref") is not None
            ]
            rows.append(
                {
                    "experiment": experiment.name,
                    "phase": phase,
                    "hidden_load": load,
                    "actual_state": actual_state,
                    "oracle_state": oracle,
                    "expected_action": expected_action,
                    "recommendations": len(phase_actions),
                    "correct_recommendations": len(correct),
                    "wrong_recommendations": len(wrong),
                    "first_correct_delay_seconds": action_delay,
                    "decision_evidence_windows": len(evidence),
                    "kpi_windows": len(phase_kpis),
                    "complete_kpi_windows": complete,
                    "kpi_sequences_contiguous": contiguous,
                }
            )
    opportunities = [row for row in rows if row["expected_action"]]
    recalls = [bool(row["correct_recommendations"]) for row in opportunities]
    delays = [
        float(row["first_correct_delay_seconds"])
        for row in opportunities
        if row["first_correct_delay_seconds"] is not None
    ]
    validation = {
        "schema": "prism-sampler.hidden-shadow-validation.v1",
        "experiments": len(experiments),
        "phases": len(rows),
        "transition_opportunities": len(opportunities),
        "recommendation_recall": sum(recalls) / len(recalls) if recalls else 1.0,
        "wrong_recommendations": sum(int(row["wrong_recommendations"]) for row in rows),
        "duplicate_recommendation_phases": sum(
            int(row["recommendations"] > 1) for row in rows
        ),
        "maximum_correct_delay_seconds": max(delays) if delays else None,
        "phases_with_model_evidence": sum(
            int(row["decision_evidence_windows"] > 0) for row in rows
        ),
        "phases_with_contiguous_kpi": sum(
            bool(row["kpi_sequences_contiguous"]) for row in rows
        ),
        "profile_fields_used_online": model.raw["g_scale"][
            "online_profile_fields_used"
        ],
        "rows": rows,
    }
    validation["passed"] = bool(
        validation["recommendation_recall"] >= 0.65
        and validation["wrong_recommendations"] == 0
        and validation["duplicate_recommendation_phases"] == 0
        and validation["phases_with_model_evidence"] == len(rows)
        and validation["phases_with_contiguous_kpi"] == len(rows)
        and not validation["profile_fields_used_online"]
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        key: value for key, value in validation.items() if key != "rows"
    } | {"output": str(output)}


def _summary_rows(experiment: Path) -> dict[str, dict[str, str]]:
    path = experiment / "controller" / "summary.csv"
    with path.open(newline="", encoding="utf-8") as stream:
        return {str(row["phase"]): row for row in csv.DictReader(stream)}


def validate_hidden_active(
    active: Path,
    static_one: Path,
    static_two: Path,
    manifest_path: Path,
    output: Path,
    *,
    equivalent_gain_pct: float = 2.0,
    settling_seconds: float = 20.0,
) -> dict[str, Any]:
    """Validate an active hidden lifecycle against frozen same-day oracles."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    phases = [(str(row["phase"]), str(row["load"])) for row in manifest["phases"]]
    one = _summary_rows(static_one)
    two = _summary_rows(static_two)
    dynamic = _summary_rows(active)
    controller = active / "controller"
    resolved = json.loads(
        (controller / "resolved-config.json").read_text(encoding="utf-8")
    )
    current_state = str(resolved["controller"]["initial_state"])
    actions = _jsonl(controller / "actions.jsonl")
    kpis = _jsonl(controller / "kpi.jsonl")
    starts: dict[str, int] = {}
    for context_path in active.glob("runs/**/meta/phase.json"):
        context = json.loads(context_path.read_text(encoding="utf-8"))
        if context.get("workload_start_epoch_ns") is not None:
            starts[str(context.get("phase", ""))] = int(
                context["workload_start_epoch_ns"]
            )

    rows: list[dict[str, Any]] = []
    for phase, load in phases:
        one_tput = float(one[phase]["throughput_ops_s"])
        two_tput = float(two[phase]["throughput_ops_s"])
        oracle_tput = max(one_tput, two_tput)
        delta_pct = 100.0 * abs(two_tput / one_tput - 1.0) if one_tput else math.inf
        if delta_pct <= equivalent_gain_pct:
            accepted_states = {"one_node", "two_node"}
            oracle_state = "equivalent"
        elif two_tput > one_tput:
            accepted_states = {"two_node"}
            oracle_state = "two_node"
        else:
            accepted_states = {"one_node"}
            oracle_state = "one_node"

        phase_actions = [
            row for row in actions
            if row.get("phase") == phase
            and row.get("action") not in {"initialize", "restore"}
        ]
        state_before = current_state
        for action in phase_actions:
            if action.get("status") == "applied":
                current_state = str(action.get("to_state", current_state))
        state_after = current_state
        expected_transition = state_before not in accepted_states
        correct_actions = [
            row for row in phase_actions
            if row.get("status") == "applied"
            and str(row.get("to_state")) in accepted_states
        ]
        wrong_actions = [
            row for row in phase_actions
            if str(row.get("to_state")) not in accepted_states
        ]
        failed_actions = [row for row in phase_actions if row.get("status") != "applied"]
        action_delay = None
        if correct_actions and phase in starts:
            action_delay = (
                int(correct_actions[0]["realtime_ns"]) - starts[phase]
            ) / 1e9

        phase_kpis = [
            row for row in kpis
            if row.get("phase") == phase and bool(row.get("complete"))
        ]
        sequences = sorted(int(row["sequence"]) for row in phase_kpis)
        contiguous = bool(sequences) and sequences == list(
            range(sequences[0], sequences[-1] + 1)
        )
        cutoff_ns = starts.get(phase, 0) + int(settling_seconds * 1e9)
        if phase_actions:
            cutoff_ns = max(
                cutoff_ns,
                max(int(row.get("finished_realtime_ns", row["realtime_ns"])) for row in phase_actions)
                + int(settling_seconds * 1e9),
            )
        steady = [
            row for row in phase_kpis
            if int(row.get("window_start_target_epoch_ns", 0)) >= cutoff_ns
        ]
        steady_tput = (
            statistics.mean(float(row["throughput_ops_s"]) for row in steady)
            if steady else math.nan
        )
        steady_p99 = (
            statistics.median(float(row["max_client_p99_latency_us"]) for row in steady)
            if steady else math.nan
        )
        dynamic_tput = float(dynamic[phase]["throughput_ops_s"])
        rows.append({
            "phase": phase,
            "hidden_load": load,
            "one_throughput_ops_s": one_tput,
            "two_throughput_ops_s": two_tput,
            "static_delta_pct": delta_pct,
            "oracle_state": oracle_state,
            "state_before": state_before,
            "state_after": state_after,
            "expected_transition": expected_transition,
            "actions": len(phase_actions),
            "correct_actions": len(correct_actions),
            "wrong_actions": len(wrong_actions),
            "failed_actions": len(failed_actions),
            "action_delay_seconds": action_delay,
            "dynamic_throughput_ops_s": dynamic_tput,
            "whole_phase_oracle_ratio": dynamic_tput / oracle_tput,
            "steady_windows": len(steady),
            "steady_throughput_ops_s": steady_tput,
            "steady_p99_us": steady_p99,
            "steady_oracle_ratio": steady_tput / oracle_tput,
            "kpi_windows": len(phase_kpis),
            "kpi_sequences_contiguous": contiguous,
        })

    opportunities = [row for row in rows if row["expected_transition"]]
    delays = [
        float(row["action_delay_seconds"])
        for row in opportunities if row["action_delay_seconds"] is not None
    ]
    oracle_total = sum(
        max(float(one[phase]["throughput_ops_s"]), float(two[phase]["throughput_ops_s"]))
        for phase, _ in phases
    )
    dynamic_total = sum(float(dynamic[phase]["throughput_ops_s"]) for phase, _ in phases)
    rollbacks = [
        row for row in actions
        if "rollback" in str(row.get("action", ""))
    ]
    validation = {
        "schema": "prism-sampler.hidden-active-validation.v1",
        "active_experiment": str(active),
        "static_one_experiment": str(static_one),
        "static_two_experiment": str(static_two),
        "phases": len(rows),
        "transition_opportunities": len(opportunities),
        "successful_transitions": sum(bool(row["correct_actions"]) for row in opportunities),
        "transition_recall": (
            sum(bool(row["correct_actions"]) for row in opportunities) / len(opportunities)
            if opportunities else 1.0
        ),
        "maximum_transition_delay_seconds": max(delays) if delays else None,
        "wrong_actions": sum(int(row["wrong_actions"]) for row in rows),
        "failed_actions": sum(int(row["failed_actions"]) for row in rows),
        "rollbacks": len(rollbacks),
        "phases_with_contiguous_kpi": sum(bool(row["kpi_sequences_contiguous"]) for row in rows),
        "phases_with_steady_windows": sum(int(row["steady_windows"] > 0) for row in rows),
        "lifecycle_oracle_ratio": dynamic_total / oracle_total,
        "minimum_whole_phase_oracle_ratio": min(float(row["whole_phase_oracle_ratio"]) for row in rows),
        "minimum_steady_oracle_ratio": min(float(row["steady_oracle_ratio"]) for row in rows),
        "equivalent_gain_pct": equivalent_gain_pct,
        "settling_seconds": settling_seconds,
        "rows": rows,
    }
    validation["passed"] = bool(
        validation["transition_recall"] == 1.0
        and validation["maximum_transition_delay_seconds"] is not None
        and validation["maximum_transition_delay_seconds"] <= 40.0
        and validation["wrong_actions"] == 0
        and validation["failed_actions"] == 0
        and validation["rollbacks"] == 0
        and validation["phases_with_contiguous_kpi"] == len(rows)
        and validation["phases_with_steady_windows"] == len(rows)
        and validation["lifecycle_oracle_ratio"] >= 0.98
        and validation["minimum_steady_oracle_ratio"] >= 0.98
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    csv_path = output.with_suffix(".csv")
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return {
        key: value for key, value in validation.items() if key != "rows"
    } | {"output": str(output), "csv": str(csv_path)}
