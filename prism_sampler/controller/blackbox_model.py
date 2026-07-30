from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from ..sidecars import derived_pmu_metrics, parse_numa_maps, parse_perf_stat


SCHEMA = "prism-sampler.blackbox-g-scale.v1"
FORBIDDEN_FEATURE_TOKENS = (
    "throughput",
    "latency",
    "p99",
    "workload",
    "profile",
    "client",
    "offered",
    "thread",
    "query",
)
PMU_METRICS = (
    "ipc",
    "cache_refill_per_kinst",
    "backend_stall_per_cycle",
    "frontend_stall_per_cycle",
    "mem_access_per_s",
    "remote_access_ratio",
    "ddrc_read_per_s",
    "ddrc_write_per_s",
    "cross_sccl_traffic_per_s",
)
EQUIVALENCE_BAND_PCT = 2.0
FEATURES = (
    "run_pressure",
    "rq_pressure",
    "allowed_node_utilization",
    "unused_node_utilization",
    "psi_cpu_some_avg10",
    "psi_cpu_full_avg10",
    "numa_local_page_ratio",
    "numa_page_entropy",
    *(f"pmu_{name}" for name in PMU_METRICS),
    "r_graph_confidence",
    "r_pair_score_max",
    "r_pair_score_sum",
    "r_self_score_max",
    "r_self_score_sum",
)


class BlackboxGainEstimate:
    def __init__(self, expected_gain_pct, model_distance, confidence, feature_coverage, contributions):
        self.expected_gain_pct = expected_gain_pct
        self.model_distance = model_distance
        self.confidence = confidence
        self.feature_coverage = feature_coverage
        self.contributions = contributions


class BlackboxGScaleModel:
    def __init__(self, value: dict[str, Any]):
        if value.get("schema") != SCHEMA:
            raise ValueError(f"unsupported blackbox model schema: {value.get('schema')}")
        self.raw = value
        self.models = value["model"]["directions"]

    def estimate(self, direction: str, features: dict[str, Any]) -> BlackboxGainEstimate:
        raw_model = self.models[direction]
        names = list(raw_model["feature_names"])
        raw = np.asarray([
            np.nan if _finite(features.get(name)) is None else float(features[name])
            for name in names
        ])
        impute = np.asarray(raw_model["impute"])
        center = np.asarray(raw_model["center"])
        scale = np.asarray(raw_model["scale"])
        coefficients = np.asarray(raw_model["coefficients"])
        present = np.isfinite(raw)
        values = np.where(present, raw, impute)
        normalized = (values - center) / scale
        contributions = {
            name: float(coefficient * value)
            for name, coefficient, value in zip(names, coefficients, normalized)
        }
        distance = float(math.sqrt(np.mean(np.square(normalized))))
        coverage = float(np.mean(present))
        return BlackboxGainEstimate(
            float(raw_model["intercept"] + sum(contributions.values())),
            distance,
            coverage * math.exp(-0.5 * distance * distance),
            coverage,
            contributions,
        )


class LiveSystemFeatureSource:
    def __init__(self) -> None:
        self.samples: list[dict[str, Any]] = []

    @staticmethod
    def _tail_jsonl(path: Path, count: int = 3) -> list[dict[str, Any]]:
        return _jsonl(path)[-count:]

    @staticmethod
    def _perf(directory: Path) -> dict[str, float]:
        samples = []
        for filename, scope in (("perf-core.csv", "process"), ("perf-uncore.csv", "system")):
            path = directory / filename
            if path.is_file():
                samples.extend(parse_perf_stat(path.read_text(errors="replace"), scope))
        if not samples:
            return {}
        latest = max(sample.interval_s for sample in samples)
        recent = [sample for sample in samples if sample.interval_s >= latest - 25]
        derived = derived_pmu_metrics(recent, 10.0)
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in derived:
            value = _finite(row.get("value"))
            if value is not None:
                grouped[str(row["metric"])].append(value)
        return {
            f"pmu_{name}": statistics.median(values)
            for name, values in grouped.items() if name in PMU_METRICS
        }

    @staticmethod
    def _numa(directory: Path, state: str) -> dict[str, float]:
        snapshots = LiveSystemFeatureSource._tail_jsonl(directory / "system-pressure.jsonl")
        local_nodes = {0} if state == "one_node" else {0, 1}
        ratios, entropies = [], []
        for snapshot in snapshots:
            pages: dict[int, float] = defaultdict(float)
            for process in snapshot.get("processes") or []:
                for node, value in parse_numa_maps(str(process.get("numa_maps") or "")).items():
                    pages[node] += value
            total = sum(pages.values())
            if total <= 0:
                continue
            shares = [value / total for value in pages.values() if value > 0]
            ratios.append(sum(pages.get(node, 0.0) for node in local_nodes) / total)
            entropies.append(
                -sum(value * math.log(value) for value in shares)
                / math.log(max(2, len(pages)))
            )
        return {
            "numa_local_page_ratio": statistics.median(ratios),
            "numa_page_entropy": statistics.median(entropies),
        } if ratios else {}

    @staticmethod
    def _relations(path: Path) -> dict[str, float]:
        candidate_path = path.parent / "live-candidates.jsonl"
        rows = LiveSystemFeatureSource._tail_jsonl(candidate_path)
        if not rows and path.is_file():
            rows = [json.loads(path.read_text())]
        summaries = []
        for row in rows:
            pairs = [float(item.get("relationship_score_r") or 0) for item in row.get("pair_candidates") or []]
            selves = [float(item.get("self_score_r") or 0) for item in row.get("self_candidates") or []]
            summaries.append({
                "r_graph_confidence": float((row.get("quality") or {}).get("confidence") or 0),
                "r_pair_score_max": max(pairs, default=0.0),
                "r_pair_score_sum": sum(pairs),
                "r_self_score_max": max(selves, default=0.0),
                "r_self_score_sum": sum(selves),
            })
        return {
            name: float(_median(summaries, name) or 0.0)
            for name in summaries[0]
        } if summaries else {}

    def build(
        self,
        sample: Any,
        *,
        state: str,
        telemetry_dir: Path | None,
        relationship_candidates: Path | None,
    ) -> dict[str, Any]:
        self.samples.append(sample.to_dict())
        self.samples = self.samples[-3:]
        features: dict[str, Any] = {
            "run_pressure": _median(self.samples, "run_pressure"),
            "rq_pressure": _median(self.samples, "rq_pressure"),
            "psi_cpu_some_avg10": _nested_median(self.samples, "psi_cpu", "some_avg10"),
            "psi_cpu_full_avg10": _nested_median(self.samples, "psi_cpu", "full_avg10"),
        }
        allowed = ("0",) if state == "one_node" else ("0", "1")
        unused = ("1", "2", "3") if state == "one_node" else ("2", "3")
        for name, nodes in (("allowed_node_utilization", allowed), ("unused_node_utilization", unused)):
            values = [
                value for node in nodes
                if (value := _nested_median(self.samples, "node_cpu_utilization", node)) is not None
            ]
            features[name] = statistics.mean(values) if values else None
        if telemetry_dir:
            features.update(self._perf(telemetry_dir))
            features.update(self._numa(telemetry_dir, state))
        if relationship_candidates:
            features.update(self._relations(relationship_candidates))
        return features


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _effect_direction(value: float) -> int:
    if value > EQUIVALENCE_BAND_PCT:
        return 1
    if value < -EQUIVALENCE_BAND_PCT:
        return -1
    return 0


def _median(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    values = [_finite(row.get(key)) for row in rows]
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def _nested_median(
    rows: Iterable[dict[str, Any]], outer: str, key: str
) -> float | None:
    values = []
    for row in rows:
        nested = row.get(outer)
        if isinstance(nested, dict):
            value = _finite(nested.get(key))
            if value is not None:
                values.append(value)
    return statistics.median(values) if values else None


def _phase_database(experiment: Path, phase: str) -> Path | None:
    for database in experiment.glob("runs/**/dataset/telemetry.db3"):
        context = database.parents[1] / "meta" / "phase.json"
        if not context.is_file():
            continue
        value = json.loads(context.read_text(encoding="utf-8"))
        if str(value.get("phase")) == phase:
            return database
    return None


def _pmu_features(database: Path | None, start_ns: int, end_ns: int) -> dict[str, float]:
    if database is None or not database.is_file():
        return {}
    import duckdb

    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "pmu_derived" not in tables:
            return {}
        rows = con.execute(
            "SELECT metric, median(value) FROM pmu_derived "
            "WHERE epoch_ns(ts) >= ? AND epoch_ns(ts) < ? AND value IS NOT NULL "
            "GROUP BY metric",
            [start_ns, end_ns],
        ).fetchall()
        return {
            f"pmu_{metric}": float(value)
            for metric, value in rows
            if str(metric) in PMU_METRICS and value is not None
        }
    finally:
        con.close()


def _numa_features(
    database: Path | None, start_ns: int, end_ns: int, state: str
) -> dict[str, float]:
    if database is None or not database.is_file():
        return {}
    import duckdb

    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "numa_samples" not in tables:
            return {}
        rows = con.execute(
            "SELECT ts, node, sum(value) FROM numa_samples "
            "WHERE metric = 'mapped' AND unit = 'pages' "
            "AND epoch_ns(ts) >= ? AND epoch_ns(ts) < ? "
            "GROUP BY ts, node ORDER BY ts, node",
            [start_ns, end_ns],
        ).fetchall()
    finally:
        con.close()
    snapshots: dict[Any, dict[int, float]] = defaultdict(dict)
    for timestamp, node, value in rows:
        snapshots[timestamp][int(node)] = float(value)
    local_nodes = {0} if state == "one_node" else {0, 1}
    local_ratios = []
    entropies = []
    for values in snapshots.values():
        total = sum(values.values())
        if total <= 0:
            continue
        shares = [value / total for value in values.values() if value > 0]
        local_ratios.append(sum(values.get(node, 0.0) for node in local_nodes) / total)
        denominator = math.log(max(2, len(values)))
        entropies.append(-sum(share * math.log(share) for share in shares) / denominator)
    result = {}
    if local_ratios:
        result["numa_local_page_ratio"] = statistics.median(local_ratios)
    if entropies:
        result["numa_page_entropy"] = statistics.median(entropies)
    return result


def _r_features(experiment: Path, start_ns: int, end_ns: int) -> dict[str, float]:
    rows = []
    for path in experiment.glob("runs/**/raw/live-candidates.jsonl"):
        for row in _jsonl(path):
            timestamp = int(row.get("generated_at_epoch_ms", 0)) * 1_000_000
            if start_ns <= timestamp < end_ns:
                rows.append(row)
    if not rows:
        return {}
    summaries = []
    for row in rows:
        pairs = row.get("pair_candidates") or []
        selves = row.get("self_candidates") or []
        pair_scores = [
            float(value.get("relationship_score_r") or 0.0) for value in pairs
        ]
        self_scores = [float(value.get("self_score_r") or 0.0) for value in selves]
        summaries.append({
            "r_graph_confidence": float((row.get("quality") or {}).get("confidence") or 0.0),
            "r_pair_score_max": max(pair_scores, default=0.0),
            "r_pair_score_sum": sum(pair_scores),
            "r_self_score_max": max(self_scores, default=0.0),
            "r_self_score_sum": sum(self_scores),
        })
    return {name: float(_median(summaries, name) or 0.0) for name in summaries[0]}


def _system_features(
    experiment: Path,
    phase: str,
    state: str,
    action_ns: int,
    samples: list[dict[str, Any]],
) -> tuple[dict[str, float | None], dict[str, Any]]:
    before = [
        row
        for row in samples
        if row.get("phase") == phase
        and row.get("valid")
        and row.get("workload_active")
        and action_ns - 35_000_000_000 <= int(row.get("realtime_ns", 0)) < action_ns
    ][-3:]
    features: dict[str, float | None] = {
        "run_pressure": _median(before, "run_pressure"),
        "rq_pressure": _median(before, "rq_pressure"),
        "psi_cpu_some_avg10": _nested_median(before, "psi_cpu", "some_avg10"),
        "psi_cpu_full_avg10": _nested_median(before, "psi_cpu", "full_avg10"),
    }
    allowed = ("0",) if state == "one_node" else ("0", "1")
    unused = ("1", "2", "3") if state == "one_node" else ("2", "3")
    allowed_values = [
        value
        for node in allowed
        if (value := _nested_median(before, "node_cpu_utilization", node)) is not None
    ]
    unused_values = [
        value
        for node in unused
        if (value := _nested_median(before, "node_cpu_utilization", node)) is not None
    ]
    features["allowed_node_utilization"] = (
        statistics.mean(allowed_values) if allowed_values else None
    )
    features["unused_node_utilization"] = (
        statistics.mean(unused_values) if unused_values else None
    )
    database = _phase_database(experiment, phase)
    start_ns = action_ns - 30_000_000_000
    features.update(_pmu_features(database, start_ns, action_ns))
    features.update(_numa_features(database, start_ns, action_ns, state))
    features.update(_r_features(experiment, start_ns, action_ns))
    return features, {
        "system_windows": len(before),
        "has_proc": len(before) >= 3 and features["run_pressure"] is not None,
        "has_pmu": any(features.get(f"pmu_{name}") is not None for name in PMU_METRICS),
        "has_numa": features.get("numa_local_page_ratio") is not None,
        "has_r": features.get("r_graph_confidence") is not None,
    }


def _label(
    action_ns: int, kpis: list[dict[str, Any]]
) -> tuple[dict[str, float], dict[str, int]] | None:
    complete = [row for row in kpis if row.get("complete")]
    before = [
        row
        for row in complete
        if int(row.get("window_end_target_epoch_ns", 0)) <= action_ns
    ][-3:]
    after = [
        row
        for row in complete
        if int(row.get("window_end_target_epoch_ns", 0)) >= action_ns + 20_000_000_000
    ][:3]
    if len(before) < 3 or len(after) < 3:
        return None
    pre = statistics.median(float(row["throughput_ops_s"]) for row in before)
    post = statistics.median(float(row["throughput_ops_s"]) for row in after)
    settling = [
        row
        for row in complete
        if action_ns < int(row.get("window_end_target_epoch_ns", 0)) < action_ns + 20_000_000_000
    ]
    expected = pre * 10 * len(settling)
    actual = sum(float(row.get("operations_delta", 0)) for row in settling)
    penalty = 100 * max(0.0, expected - actual) / (pre * 60) if pre else 0.0
    return (
        {
            "label_gain_pct": 100 * (post / pre - 1.0) - penalty,
            "label_pre_throughput_ops_s": pre,
            "label_post_throughput_ops_s": post,
            "label_transition_penalty_pct": penalty,
        },
        {"label_pre_windows": len(before), "label_post_windows": len(after)},
    )


def extract_action_dataset(experiments: list[Path]) -> list[dict[str, Any]]:
    output = []
    for experiment in experiments:
        controller = experiment / "controller"
        samples = _jsonl(controller / "samples.jsonl")
        kpis = _jsonl(controller / "kpi.jsonl")
        for action in _jsonl(controller / "actions.jsonl"):
            if not str(action.get("action", "")).startswith("scripted_"):
                continue
            if action.get("status") not in {"applied", "shadow"}:
                continue
            phase = str(action.get("phase") or "")
            action_ns = int(action.get("finished_realtime_ns") or action["realtime_ns"])
            label = _label(action_ns, kpis)
            if label is None:
                continue
            features, quality = _system_features(
                experiment, phase, str(action["from_state"]), action_ns, samples
            )
            direction = (
                "expand" if action["to_state"] == "two_node" else "shrink"
            )
            output.append({
                "experiment": experiment.name,
                "validation_group": phase,
                "direction": direction,
                "current_state": str(action["from_state"]),
                "action_realtime_ns": action_ns,
                **features,
                **quality,
                **label[0],
                **label[1],
            })
    return output


def _fit_ridge(
    rows: list[dict[str, Any]], feature_names: tuple[str, ...], alpha: float
) -> dict[str, Any]:
    values = np.asarray([
        [np.nan if row.get(name) is None else float(row[name]) for name in feature_names]
        for row in rows
    ])
    target = np.asarray([float(row["label_gain_pct"]) for row in rows])
    medians = np.asarray([
        float(np.median(column[np.isfinite(column)]))
        if np.isfinite(column).any() else 0.0
        for column in values.T
    ])
    values = np.where(np.isfinite(values), values, medians)
    center = np.median(values, axis=0)
    q25 = np.percentile(values, 25, axis=0)
    q75 = np.percentile(values, 75, axis=0)
    scale = q75 - q25
    scale = np.where(scale > 1e-12, scale, 1.0)
    normalized = (values - center) / scale
    design = np.column_stack([np.ones(len(normalized)), normalized])
    penalty = np.eye(design.shape[1]) * alpha
    penalty[0, 0] = 0.0
    coefficients = np.linalg.pinv(design.T @ design + penalty) @ design.T @ target
    return {
        "feature_names": feature_names,
        "impute": medians,
        "center": center,
        "scale": scale,
        "intercept": float(coefficients[0]),
        "coefficients": coefficients[1:],
    }


def _usable_features(
    rows: list[dict[str, Any]], *, minimum_coverage: float = 0.5
) -> tuple[str, ...]:
    return tuple(
        name for name in FEATURES
        if rows and sum(row.get(name) is not None for row in rows) / len(rows) >= minimum_coverage
    )


def _predict(model: dict[str, Any], row: dict[str, Any]) -> tuple[float, dict[str, float]]:
    raw = np.asarray([
        np.nan if row.get(name) is None else float(row[name])
        for name in model["feature_names"]
    ])
    values = np.where(np.isfinite(raw), raw, model["impute"])
    normalized = (values - model["center"]) / model["scale"]
    contributions = {
        name: float(coefficient * value)
        for name, coefficient, value in zip(
            model["feature_names"], model["coefficients"], normalized
        )
    }
    return float(model["intercept"] + sum(contributions.values())), contributions


def _serializable_model(model: dict[str, Any]) -> dict[str, Any]:
    return {
        "feature_names": list(model["feature_names"]),
        "impute": model["impute"].tolist(),
        "center": model["center"].tolist(),
        "scale": model["scale"].tolist(),
        "intercept": model["intercept"],
        "coefficients": model["coefficients"].tolist(),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def train_blackbox_model(
    experiments: list[Path], output: Path, *, alpha: float = 10.0
) -> dict[str, Any]:
    for name in FEATURES:
        lowered = name.lower()
        if any(token in lowered for token in FORBIDDEN_FEATURE_TOKENS):
            raise ValueError(f"forbidden online feature: {name}")
    rows = extract_action_dataset(experiments)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "blackbox-action-dataset.csv", rows)
    valid = [
        row for row in rows
        if row["system_windows"] >= 3
        and row["has_proc"] and row["has_pmu"] and row["has_numa"] and row["has_r"]
    ]
    groups = sorted({str(row["validation_group"]) for row in valid})
    predictions = []
    for held_out in groups:
        for direction in ("expand", "shrink"):
            training = [
                row for row in valid
                if row["validation_group"] != held_out and row["direction"] == direction
            ]
            testing = [
                row for row in valid
                if row["validation_group"] == held_out and row["direction"] == direction
            ]
            if not training:
                continue
            model = _fit_ridge(training, _usable_features(training), alpha)
            for row in testing:
                predicted, contributions = _predict(model, row)
                predictions.append({
                    "experiment": row["experiment"],
                    "validation_group": held_out,
                    "direction": direction,
                    "actual_gain_pct": row["label_gain_pct"],
                    "predicted_gain_pct": predicted,
                    "actual_direction": _effect_direction(float(row["label_gain_pct"])),
                    "predicted_direction": _effect_direction(predicted),
                    "direction_correct": _effect_direction(predicted)
                    == _effect_direction(float(row["label_gain_pct"])),
                    **{f"contribution_{key}": value for key, value in contributions.items()},
                })
    _write_csv(output / "blackbox-lolo-predictions.csv", predictions)
    models = {}
    contributions = []
    direction_diversity = {}
    for direction in ("expand", "shrink"):
        selected = [row for row in valid if row["direction"] == direction]
        if not selected:
            continue
        model = _fit_ridge(selected, _usable_features(selected), alpha)
        models[direction] = _serializable_model(model)
        gains = [float(row["label_gain_pct"]) for row in selected]
        direction_diversity[direction] = {
            "positive": sum(_effect_direction(value) > 0 for value in gains),
            "neutral": sum(_effect_direction(value) == 0 for value in gains),
            "negative": sum(_effect_direction(value) < 0 for value in gains),
        }
        for row in selected:
            predicted, values = _predict(model, row)
            for feature, contribution in values.items():
                contributions.append({
                    "experiment": row["experiment"],
                    "direction": direction,
                    "feature": feature,
                    "feature_value": row.get(feature),
                    "contribution_pct_points": contribution,
                    "predicted_gain_pct": predicted,
                })
    _write_csv(output / "blackbox-action-contributions.csv", contributions)
    feature_coverage = {
        name: (
            sum(row.get(name) is not None for row in rows) / len(rows) if rows else 0.0
        )
        for name in FEATURES
    }
    coverage = len(valid) / len(rows) if rows else 0.0
    accuracy = (
        sum(bool(row["direction_correct"]) for row in predictions) / len(predictions)
        if predictions else 0.0
    )
    identifiable = all(
        value["positive"] > 0 and value["negative"] > 0
        for value in direction_diversity.values()
    ) and len(direction_diversity) == 2
    report = {
        "schema": SCHEMA,
        "online_contract": {
            "feature_names": list(FEATURES),
            "forbidden_online_inputs": list(FORBIDDEN_FEATURE_TOKENS),
            "labels_only": ["YBA throughput", "YBA P99", "YBA errors"],
            "uses_workload_identity_online": False,
        },
        "dataset": {
            "actions": len(rows),
            "valid_actions": len(valid),
            "coverage": coverage,
            "feature_coverage": feature_coverage,
            "direction_diversity": direction_diversity,
        },
        "validation": {
            "method": "leave-one-load-out",
            "predictions": len(predictions),
            "direction_accuracy": accuracy,
            "minimum_coverage": 0.90,
            "minimum_direction_accuracy": 0.80,
            "equivalence_band_pct": EQUIVALENCE_BAND_PCT,
            "label_direction_identifiable": identifiable,
            "passed": coverage >= 0.90 and accuracy >= 0.80 and identifiable,
        },
        "model": {
            "type": "separate-direction robust-scaled ridge",
            "alpha": alpha,
            "minimum_training_feature_coverage": 0.5,
            "excluded_features": sorted(
                set(FEATURES)
                - set().union(*(
                    set(value["feature_names"]) for value in models.values()
                ))
            ),
            "directions": models,
        },
    }
    (output / "blackbox-g-scale.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report
