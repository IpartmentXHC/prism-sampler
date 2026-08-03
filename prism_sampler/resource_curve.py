from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any

SCHEMA = "prism-sampler.calibration-bundle.v1"
POLICY_SCHEMA = "prism-sampler.resource-policy.v1"
MINIMUM_ANCHOR_ROUNDS = 3
MINIMUM_RESIDUAL_ANCHORS = 5
MAXIMUM_EXCLUDED_RATIO = 0.10
MAXIMUM_PRESSURE_MAE = 0.10
MAXIMUM_PRESSURE_P95_ERROR = 0.25
FIXED_SOURCE_KINDS = {"pressure_v2_fixed_config", "new_fixed_config_measurement"}
CONTEXT_FEATURES = (
    "rq_pressure",
    "pmu_ipc",
    "pmu_cache_refill_per_kinst",
    "pmu_backend_stall_per_cycle",
    "pmu_frontend_stall_per_cycle",
    "pmu_mem_access_per_s",
    "pmu_remote_access_ratio",
    "pmu_ddrc_read_per_s",
    "pmu_ddrc_write_per_s",
    "pmu_cross_sccl_traffic_per_s",
    "numa_local_page_ratio",
    "numa_page_entropy",
    "r_graph_confidence",
    "r_pair_score_max",
    "r_pair_score_sum",
    "r_self_score_max",
    "r_self_score_sum",
)


def _float(row: dict[str, str], name: str) -> float:
    value = float(row[name])
    if not math.isfinite(value):
        raise ValueError(f"non-finite {name}")
    return value


def _int(row: dict[str, str], name: str) -> int:
    return int(float(row[name]))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValueError(f"anchor table does not exist: {path}")
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("anchor table is empty")
    return rows


def _pava(values: list[float], weights: list[float]) -> list[float]:
    blocks: list[dict[str, Any]] = []
    for index, (value, weight) in enumerate(zip(values, weights)):
        blocks.append(
            {"start": index, "end": index, "weight": weight, "sum": value * weight}
        )
        while len(blocks) >= 2:
            left, right = blocks[-2:]
            if left["sum"] / left["weight"] <= right["sum"] / right["weight"]:
                break
            blocks[-2:] = [
                {
                    "start": left["start"],
                    "end": right["end"],
                    "weight": left["weight"] + right["weight"],
                    "sum": left["sum"] + right["sum"],
                }
            ]
    fitted = [0.0] * len(values)
    for block in blocks:
        mean = block["sum"] / block["weight"]
        for index in range(block["start"], block["end"] + 1):
            fitted[index] = mean
    return fitted


def _mad(values: list[float]) -> float:
    if not values:
        return 0.0
    center = statistics.median(values)
    return statistics.median(abs(value - center) for value in values)


def _anchor(row: dict[str, str]) -> dict[str, Any]:
    one = _float(row, "one_throughput_ops_s")
    two = _float(row, "two_throughput_ops_s")
    if one <= 0 or two <= 0:
        raise ValueError("throughput must be positive")
    errors = _int(row, "errors")
    timeouts = _int(row, "timeouts")
    one_rounds = _int(row, "one_rounds")
    two_rounds = _int(row, "two_rounds")
    source = str(row["one_source_kind"])
    quality_reasons = []
    if errors or timeouts:
        quality_reasons.append("errors_or_timeouts")
    if one_rounds < MINIMUM_ANCHOR_ROUNDS or two_rounds < MINIMUM_ANCHOR_ROUNDS:
        quality_reasons.append("fewer_than_three_rounds")
    if source not in FIXED_SOURCE_KINDS:
        quality_reasons.append("legacy_or_unmatched_one_config")
    return {
        "load": str(row["load"]),
        "clients": _int(row, "clients"),
        "threads_per_client": _int(row, "threads_per_client"),
        "offered_threads": _int(row, "offered_threads"),
        "pressure_ref": None,
        "one_rounds": one_rounds,
        "two_rounds": two_rounds,
        "one_source_kind": source,
        "one_throughput_ops_s": one,
        "two_throughput_ops_s": two,
        "two_advantage_pct": 100.0 * (two / one - 1.0),
        "one_oracle_ratio": one / max(one, two),
        "two_oracle_ratio": two / max(one, two),
        "one_p99_us": _float(row, "one_p99_us"),
        "two_p99_us": _float(row, "two_p99_us"),
        "quality_reasons": quality_reasons,
    }


def _pressure_by_load(path: Path) -> dict[str, float]:
    return {str(row["load"]): _float(row, "pressure_ref") for row in _read_csv(path)}


def _quality_exclusions(anchors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "load": row["load"],
            "stage": "quality",
            "reason": ";".join(row["quality_reasons"]),
            "pressure_ref": row["pressure_ref"],
            "observed_gain_pct": row["two_advantage_pct"],
            "fitted_gain_pct": None,
            "residual_pct_points": None,
            "source_kind": row["one_source_kind"],
        }
        for row in anchors
        if row["quality_reasons"]
    ]


def _retest_proposal(
    anchors: list[dict[str, Any]], residual_exclusions: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    residual_loads = {row["load"] for row in residual_exclusions}
    proposal = []
    for row in anchors:
        matched_one_rounds = (
            row["one_rounds"] if row["one_source_kind"] in FIXED_SOURCE_KINDS else 0
        )
        add_one = max(0, MINIMUM_ANCHOR_ROUNDS - matched_one_rounds)
        add_two = max(0, MINIMUM_ANCHOR_ROUNDS - row["two_rounds"])
        reasons = list(row["quality_reasons"])
        if row["load"] in residual_loads:
            reasons.append("curve_residual_requires_two_matched_retest_rounds")
            add_one = max(add_one, 2)
            add_two = max(add_two, 2)
            status = "curve_residual_retest_required"
        elif not row["quality_reasons"]:
            status = "complete"
        elif "errors_or_timeouts" in row["quality_reasons"]:
            status = "quality_retest_required"
            add_one = max(add_one, MINIMUM_ANCHOR_ROUNDS)
            add_two = max(add_two, MINIMUM_ANCHOR_ROUNDS)
        else:
            status = "matched_rounds_required"
        proposal.append(
            {
                "load": row["load"],
                "pressure_ref": row["pressure_ref"],
                "status": status,
                "one_source_kind": row["one_source_kind"],
                "matched_one_rounds": matched_one_rounds,
                "matched_two_rounds": row["two_rounds"],
                "additional_one_rounds": add_one,
                "additional_two_rounds": add_two,
                "reason": ";".join(reasons),
            }
        )
    return proposal


def _fit_curve(
    anchors: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    eligible = sorted(
        (row for row in anchors if not row["quality_reasons"]),
        key=lambda row: row["pressure_ref"],
    )
    if not eligible:
        return [], []
    gains = [float(row["two_advantage_pct"]) for row in eligible]
    pressures = [float(row["pressure_ref"]) for row in eligible]
    slopes = [
        (gains[right] - gains[left]) / (pressures[right] - pressures[left])
        for left in range(len(gains))
        for right in range(left + 1, len(gains))
        if pressures[right] > pressures[left]
    ]
    if len(gains) >= MINIMUM_RESIDUAL_ANCHORS and slopes:
        slope = statistics.median(slopes)
        intercept = statistics.median(
            gain - slope * pressure for pressure, gain in zip(pressures, gains)
        )
        robust_expected = [intercept + slope * pressure for pressure in pressures]
    else:
        robust_expected = list(gains)
    residuals = [actual - fitted for actual, fitted in zip(gains, robust_expected)]
    threshold = max(3.0 * 1.4826 * _mad(residuals), 10.0)
    suspects = (
        [abs(residual) > threshold for residual in residuals]
        if len(gains) >= MINIMUM_RESIDUAL_ANCHORS
        else [False] * len(gains)
    )
    included = [row for row, suspect in zip(eligible, suspects) if not suspect]
    fitted_values = (
        _pava(
            [float(row["two_advantage_pct"]) for row in included],
            [float(min(row["one_rounds"], row["two_rounds"])) for row in included],
        )
        if included
        else []
    )
    fit_by_load = {row["load"]: value for row, value in zip(included, fitted_values)}
    curve = []
    excluded = []
    for row, fitted, residual, suspect in zip(
        eligible, robust_expected, residuals, suspects
    ):
        if suspect:
            excluded.append(
                {
                    "load": row["load"],
                    "stage": "curve_residual",
                    "reason": "requires_two_matched_retest_rounds",
                    "pressure_ref": row["pressure_ref"],
                    "observed_gain_pct": row["two_advantage_pct"],
                    "fitted_gain_pct": fitted,
                    "residual_pct_points": residual,
                    "source_kind": row["one_source_kind"],
                }
            )
            continue
        curve.append({**row, "fitted_two_advantage_pct": fit_by_load[row["load"]]})
    return curve, excluded


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _policy_toml(bundle: dict[str, Any]) -> str:
    objective = bundle["objective"]
    pressure = bundle["pressure_reference"]
    coefficients = pressure["coefficients"]
    lines = [
        f'schema = "{POLICY_SCHEMA}"',
        f'status = "{bundle["status"]}"',
        f'system = "{bundle["system"]}"',
        f'platform = "{bundle["platform"]}"',
        f"minimum_oracle_ratio = {objective['minimum_oracle_ratio']}",
        f"measurement_equivalence_pct = {objective['measurement_equivalence_pct']}",
        f"expand_required_advantage_pct = {objective['expand_required_advantage_pct']}",
        "",
        "[pressure_reference]",
        f"reference_capacity_cpus = {pressure['reference_capacity_cpus']}",
        f"run_coefficient = {coefficients['run_coefficient']}",
        f"rq_coefficient = {coefficients['rq_coefficient']}",
    ]
    for state in bundle["resource_states"]:
        lines.extend(
            [
                "",
                "[[resource_states]]",
                f'name = "{state["name"]}"',
                f"nodes = {json.dumps(state['nodes'])}",
                f'cpus = "{state["cpus"]}"',
                f"max_threads = {state['max_threads']}",
                f"global_slots = {state['global_slots']}",
                f'memory_policy = "{state["memory_policy"]}"',
                f"memory_action_allowed = {str(state['memory_action_allowed']).lower()}",
            ]
        )
    return "\n".join(lines) + "\n"


def build_resource_curve(
    anchors_path: Path,
    pressure_anchors_path: Path,
    pressure_model_path: Path,
    output: Path,
    *,
    system: str = "clickhouse",
    platform: str = "kunpeng-920",
    minimum_oracle_ratio: float = 0.90,
    measurement_equivalence_pct: float = 2.0,
    g_model_path: Path | None = None,
) -> dict[str, Any]:
    if not 0 < minimum_oracle_ratio <= 1:
        raise ValueError("minimum_oracle_ratio must be in (0, 1]")
    anchors = [_anchor(row) for row in _read_csv(anchors_path)]
    pressures = _pressure_by_load(pressure_anchors_path)
    for row in anchors:
        if row["load"] not in pressures:
            raise ValueError(f"missing P_ref for {row['load']}")
        row["pressure_ref"] = pressures[row["load"]]
    pressure = json.loads(pressure_model_path.read_text(encoding="utf-8"))
    validation = pressure.get("validation", {})
    pressure_ok = bool(
        float(validation.get("leave_one_load_out_mae_pressure_units", math.inf))
        <= MAXIMUM_PRESSURE_MAE
        and float(validation.get("leave_one_load_out_p95_absolute_error", math.inf))
        <= MAXIMUM_PRESSURE_P95_ERROR
    )
    curve, residual_exclusions = _fit_curve(anchors)
    exclusions = _quality_exclusions(anchors) + residual_exclusions
    retest_proposal = _retest_proposal(anchors, residual_exclusions)
    residual_ratio = len(residual_exclusions) / len(anchors)
    complete = len(curve) == len(anchors)
    validation_passed = (
        pressure_ok and complete and residual_ratio <= MAXIMUM_EXCLUDED_RATIO
    )
    required_advantage = 100.0 * (1.0 / minimum_oracle_ratio - 1.0)
    for row in curve:
        row["selected_state"] = (
            "one_node"
            if float(row["fitted_two_advantage_pct"]) <= required_advantage
            else "two_node"
        )
        row["minimum_oracle_ratio"] = minimum_oracle_ratio
    g_model = None
    if g_model_path:
        g_model = json.loads(g_model_path.read_text(encoding="utf-8"))
    output.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema": SCHEMA,
        "status": "active_eligible" if validation_passed else "candidate_only",
        "system": system,
        "workload_domain": "YBA/YCSB",
        "platform": platform,
        "objective": {
            "selection": "minimum resources within throughput budget",
            "minimum_oracle_ratio": minimum_oracle_ratio,
            "maximum_throughput_loss_pct": 100.0 * (1.0 - minimum_oracle_ratio),
            "measurement_equivalence_pct": measurement_equivalence_pct,
            "expand_required_advantage_pct": required_advantage,
        },
        "pressure_reference": pressure,
        "bottleneck_context": {
            "candidate_features": list(CONTEXT_FEATURES),
            "admitted_features": [],
            "status": "diagnostic_until_orthogonal_validation",
        },
        "resource_states": [
            {
                "name": "one_node",
                "nodes": [0],
                "cpus": "0-31",
                "max_threads": 32,
                "global_slots": 128,
                "memory_policy": "default",
                "memory_action_allowed": False,
            },
            {
                "name": "two_node",
                "nodes": [0, 1],
                "cpus": "0-63",
                "max_threads": 32,
                "global_slots": 64,
                "memory_policy": "default",
                "memory_action_allowed": False,
            },
        ],
        "curve": curve,
        "g_scale": g_model,
        "validation": {
            "passed": validation_passed,
            "pressure_mapping_passed": pressure_ok,
            "anchors_total": len(anchors),
            "anchors_in_curve": len(curve),
            "quality_exclusions": len(exclusions) - len(residual_exclusions),
            "curve_residual_exclusions": len(residual_exclusions),
            "curve_residual_exclusion_ratio": residual_ratio,
            "maximum_curve_residual_exclusion_ratio": MAXIMUM_EXCLUDED_RATIO,
            "required_rounds_per_state": MINIMUM_ANCHOR_ROUNDS,
            "maximum_pressure_mae": MAXIMUM_PRESSURE_MAE,
            "maximum_pressure_p95_error": MAXIMUM_PRESSURE_P95_ERROR,
            "additional_one_rounds": sum(
                row["additional_one_rounds"] for row in retest_proposal
            ),
            "additional_two_rounds": sum(
                row["additional_two_rounds"] for row in retest_proposal
            ),
        },
        "evidence": {
            "anchors": {"path": str(anchors_path), "sha256": _sha256(anchors_path)},
            "pressure_anchors": {
                "path": str(pressure_anchors_path),
                "sha256": _sha256(pressure_anchors_path),
            },
            "pressure_model": {
                "path": str(pressure_model_path),
                "sha256": _sha256(pressure_model_path),
            },
            "g_model": (
                {"path": str(g_model_path), "sha256": _sha256(g_model_path)}
                if g_model_path
                else None
            ),
        },
    }
    bundle_path = output / "calibration-bundle.json"
    bundle_path.write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / f"{system}-{platform}-policy.toml").write_text(
        _policy_toml(bundle), encoding="utf-8"
    )
    _write_csv(
        output / "resource-curve.csv",
        curve,
        [
            "load",
            "pressure_ref",
            "one_throughput_ops_s",
            "two_throughput_ops_s",
            "two_advantage_pct",
            "fitted_two_advantage_pct",
            "one_oracle_ratio",
            "two_oracle_ratio",
            "selected_state",
            "minimum_oracle_ratio",
        ],
    )
    _write_csv(
        output / "excluded-observations.csv",
        exclusions,
        [
            "load",
            "stage",
            "reason",
            "pressure_ref",
            "observed_gain_pct",
            "fitted_gain_pct",
            "residual_pct_points",
            "source_kind",
        ],
    )
    _write_csv(
        output / "retest-proposal.csv",
        retest_proposal,
        [
            "load",
            "pressure_ref",
            "status",
            "one_source_kind",
            "matched_one_rounds",
            "matched_two_rounds",
            "additional_one_rounds",
            "additional_two_rounds",
            "reason",
        ],
    )
    write_resource_curve_report(bundle, output / "report.md")
    return bundle


def validate_resource_curve(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    errors = []
    if value.get("schema") != SCHEMA:
        errors.append("unsupported schema")
    objective = value.get("objective", {})
    ratio = objective.get("minimum_oracle_ratio")
    if not isinstance(ratio, (int, float)) or not 0 < ratio <= 1:
        errors.append("invalid minimum_oracle_ratio")
    states = value.get("resource_states", [])
    if [state.get("name") for state in states] != ["one_node", "two_node"]:
        errors.append("resource states must be one_node then two_node")
    curve = value.get("curve", [])
    pressures = [row.get("pressure_ref") for row in curve]
    if pressures != sorted(pressures):
        errors.append("curve is not sorted by P_ref")
    validation = value.get("validation", {})
    if (
        float(validation.get("curve_residual_exclusion_ratio", 1.0))
        > MAXIMUM_EXCLUDED_RATIO
    ):
        errors.append("curve residual exclusion ratio exceeds limit")
    result = {
        "schema": "prism-sampler.resource-curve-validation.v1",
        "passed": not errors and bool(validation.get("passed")),
        "bundle_status": value.get("status"),
        "errors": errors,
    }
    return result


def render_resource_curve(bundle_path: Path, output: Path) -> Path:
    import matplotlib.pyplot as plt

    value = json.loads(bundle_path.read_text(encoding="utf-8"))
    curve = value.get("curve", [])
    if not curve:
        raise ValueError("resource curve has no eligible anchors")
    pressure = [float(row["pressure_ref"]) for row in curve]
    one = [float(row["one_throughput_ops_s"]) for row in curve]
    two = [float(row["two_throughput_ops_s"]) for row in curve]
    states = [1 if row["selected_state"] == "two_node" else 0 for row in curve]
    figure, primary = plt.subplots(figsize=(11, 6.2))
    primary.plot(pressure, one, "o-", color="#26734d", label="ONE throughput")
    primary.plot(pressure, two, "s-", color="#b84a3a", label="TWO throughput")
    primary.set_xlabel("P_ref (32-CPU reference pressure)")
    primary.set_ylabel("Throughput (ops/s)")
    primary.grid(alpha=0.22)
    secondary = primary.twinx()
    secondary.step(
        pressure,
        states,
        where="mid",
        color="#34495e",
        linewidth=2,
        label="Selected state",
    )
    secondary.set_yticks([0, 1], ["ONE", "TWO"])
    secondary.set_ylim(-0.25, 1.25)
    lines = primary.get_lines() + secondary.get_lines()
    primary.legend(lines, [line.get_label() for line in lines], loc="lower right")
    figure.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=150)
    plt.close(figure)
    return output


def write_resource_curve_report(bundle: dict[str, Any], output: Path) -> Path:
    validation = bundle["validation"]
    objective = bundle["objective"]
    lines = [
        "# Database Resource Curve",
        "",
        f"- Status: `{bundle['status']}`",
        f"- System/domain: `{bundle['system']}` / `{bundle['workload_domain']}`",
        f"- Minimum oracle ratio: `{objective['minimum_oracle_ratio']:.2f}`",
        f"- Expand advantage boundary: `{objective['expand_required_advantage_pct']:.4f}%`",
        f"- Anchors in curve: `{validation['anchors_in_curve']}/{validation['anchors_total']}`",
        f"- Pressure mapping gate: `{'PASS' if validation['pressure_mapping_passed'] else 'FAIL'}`",
        f"- Additional matched rounds: `ONE={validation['additional_one_rounds']}, TWO={validation['additional_two_rounds']}`",
        "",
        "The 2% measurement band describes experimental noise. The 90% oracle ratio is the resource-selection objective; they are not interchangeable.",
        "",
        "| Load | P_ref | ONE ops/s | TWO ops/s | Fitted TWO gain | Selected |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in bundle["curve"]:
        lines.append(
            f"| {row['load']} | {row['pressure_ref']:.4f} | "
            f"{row['one_throughput_ops_s']:.2f} | {row['two_throughput_ops_s']:.2f} | "
            f"{row['fitted_two_advantage_pct']:.2f}% | {row['selected_state']} |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output
