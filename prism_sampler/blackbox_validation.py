from __future__ import annotations

import csv
import json
import os
import statistics
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .config import load_config
from .deploy import build_bundle, install_bundle, install_client
from .orchestration.runner import run_yba
from .pressure_v2 import render_controller_config
from .remote import Host


SCHEMA = "prism-sampler.blackbox-stage-d.v1"
MAXIMUM_ISOLATED_UNUSED_NODE_UTILIZATION = 0.10
FORBIDDEN_TOKENS = (
    "throughput", "latency", "p99", "workload", "profile", "client",
    "offered", "thread", "query",
)


@contextmanager
def _environment(values: dict[str, str]):
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _selected_env(selected: Path, initial: str, *, active: bool) -> dict[str, str]:
    value = json.loads(selected.read_text(encoding="utf-8"))
    slots = value["one_node_slots"] if initial == "one_node" else value["two_node_slots"]
    return {
        "CLICKHOUSE_MAX_THREADS": str(value["max_threads"]),
        "CLICKHOUSE_CONCURRENT_THREADS": str(slots),
        "CLICKHOUSE_CONCURRENT_RATIO": "0",
        "SERVER_CPU_NODES": "" if active else ("0" if initial == "one_node" else "0,1"),
    }


def _find_experiment(name: str, before: set[Path]) -> Path:
    parent = Path("/data/threadState/experiments/clickhouse")
    matches = set(parent.glob(f"*-{name}")) - before
    if len(matches) != 1:
        raise RuntimeError(f"expected one new experiment for {name}: {sorted(matches)}")
    return matches.pop()


def _run(
    generated: Path,
    selected: Path,
    model: Path,
    base_config: Path,
    scenario: Path,
    *,
    mode: str,
    initial: str,
    name: str,
) -> Path:
    local = generated / f"{name}-local.toml"
    remote = generated / f"{name}-remote.toml"
    remote_model = Path("/home/xhc/.config/prism-sampler/blackbox-g-scale.json")
    render_controller_config(
        selected, local, target_host="kunpen183",
        output_root="/data/threadState/experiments", mode=mode,
        initial_state=initial, dynamic_model_path=model, use_kpi_online=False,
    )
    render_controller_config(
        selected, remote, target_host="192.168.70.183",
        output_root="/home/xhc/.local/share/prism-sampler/experiments", mode=mode,
        initial_state=initial, dynamic_model_path=remote_model, use_kpi_online=False,
    )
    client = Host("ubuntu197")
    client.run("mkdir -p /home/xhc/.config/prism-sampler")
    client.copy_to(model, str(remote_model))
    client.copy_to(remote, "/home/xhc/.config/prism-sampler/local.toml")
    parent = Path("/data/threadState/experiments/clickhouse")
    before = set(parent.glob(f"*-{name}"))
    with _environment(_selected_env(selected, initial, active=mode == "active")):
        code = run_yba(
            load_config(local), base_config, scenario, experiment_name=name
        )
    if code:
        raise RuntimeError(f"YBA returned {code}: {name}")
    return _find_experiment(name, before)


def validate_shadow(experiments: list[Path], output: Path) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    placements: list[dict[str, Any]] = []
    confirmation_failures = 0
    repeated_within_minute = 0
    activity_marker_enabled = 0
    for experiment in experiments:
        resolved_path = experiment / "controller" / "resolved-config.json"
        if resolved_path.is_file():
            resolved = json.loads(resolved_path.read_text(encoding="utf-8"))
            activity_marker_enabled += bool(
                resolved.get("controller", {}).get("use_workload_activity_marker", True)
            )
        current_samples = _jsonl(experiment / "controller" / "samples.jsonl")
        current_decisions = _jsonl(experiment / "controller" / "decisions.jsonl")
        current_actions = [
            row for row in _jsonl(experiment / "controller" / "actions.jsonl")
            if row.get("status") == "shadow" and row.get("action") not in {"initialize", "restore"}
        ]
        samples.extend(current_samples)
        decisions.extend(current_decisions)
        action_rows.extend(current_actions)
        placements.extend(_jsonl(experiment / "controller" / "fine-placement.jsonl"))
        stamps = sorted(int(row.get("realtime_ns") or 0) for row in current_actions)
        repeated_within_minute += sum(
            current - previous < 60_000_000_000
            for previous, current in zip(stamps, stamps[1:])
        )
        for action in current_actions:
            stamp = int(action.get("realtime_ns") or 0)
            matching = [
                i for i, row in enumerate(current_decisions)
                if row.get("action") == action.get("action")
                and int(row.get("realtime_ns") or 0) <= stamp
            ]
            index = matching[-1] if matching else -1
            prior = current_decisions[max(0, index - 2):index]
            if index < 0 or len(prior) < 2 or not all(
                row.get("reason") == "system_gain_confirming" for row in prior
            ):
                confirmation_failures += 1
    active_samples = [row for row in samples if row.get("workload_active") and row.get("valid")]
    model_windows = [
        row for row in decisions
        if row.get("decision_source") == "blackbox_system_g_scale"
        and row.get("expected_gain_pct") is not None
    ]
    recommendations = [row for row in action_rows if row.get("action") in {"expand", "shrink"}]
    model_recommendations = [
        row for row in decisions
        if row.get("action") in {"expand", "shrink"}
        and row.get("reason") != "pressure_safety_expand"
    ]
    unused_node_utilization = [
        float(value)
        for row in active_samples
        if (value := (row.get("system_features") or {}).get("unused_node_utilization"))
        is not None
    ]
    isolated_environment = bool(
        unused_node_utilization
        and statistics.median(unused_node_utilization)
        <= MAXIMUM_ISOLATED_UNUSED_NODE_UTILIZATION
    )
    online_kpi_rows = sum(bool(row.get("kpi")) for row in samples)
    online_database_rows = sum(bool(row.get("clickhouse_metrics")) for row in samples)
    forbidden_feature_keys = sorted({
        key
        for row in samples
        for key in (row.get("system_features") or {})
        if any(token in key.lower() for token in FORBIDDEN_TOKENS)
    })
    report = {
        "schema": SCHEMA,
        "experiments": [str(path) for path in experiments],
        "valid_system_windows": len(active_samples),
        "model_decision_windows": len(model_windows),
        "recommendations": len(recommendations),
        "model_recommendations": len(model_recommendations),
        "directions": sorted({str(row.get("action")) for row in recommendations}),
        "minimum_recommendation_confidence": min(
            (float(row.get("model_confidence") or 0) for row in model_recommendations),
            default=None,
        ),
        "minimum_recommendation_feature_coverage": min(
            (float(row.get("feature_coverage") or 0) for row in model_recommendations),
            default=None,
        ),
        "calibration_environment": {
            "unused_node_windows": len(unused_node_utilization),
            "median_unused_node_utilization": (
                statistics.median(unused_node_utilization)
                if unused_node_utilization else None
            ),
            "maximum_median_unused_node_utilization": (
                MAXIMUM_ISOLATED_UNUSED_NODE_UTILIZATION
            ),
            "isolated": isolated_environment,
        },
        "confirmation_failures": confirmation_failures,
        "repeated_within_minute": repeated_within_minute,
        "capacity_infeasible_windows": sum(
            row.get("status") == "capacity_infeasible" for row in placements
        ),
        "unsafe_capacity_recommendations": sum(
            bool(row.get("apply_allowed")) and not bool(row.get("capacity_feasible"))
            for row in placements
        ),
        "online_kpi_rows": online_kpi_rows,
        "online_database_metric_rows": online_database_rows,
        "activity_marker_enabled_runs": activity_marker_enabled,
        "forbidden_feature_keys": forbidden_feature_keys,
    }
    report["passed"] = bool(
        report["valid_system_windows"] >= 30
        and report["model_decision_windows"] >= 30
        and report["model_recommendations"] >= 2
        and set(report["directions"]) == {"expand", "shrink"}
        and report["minimum_recommendation_confidence"] is not None
        and report["minimum_recommendation_confidence"] >= 0.8
        and report["minimum_recommendation_feature_coverage"] is not None
        and report["minimum_recommendation_feature_coverage"] >= 0.8
        and isolated_environment
        and report["confirmation_failures"] == 0
        and report["repeated_within_minute"] == 0
        and report["unsafe_capacity_recommendations"] == 0
        and report["online_kpi_rows"] == 0
        and report["online_database_metric_rows"] == 0
        and report["activity_marker_enabled_runs"] == 0
        and not report["forbidden_feature_keys"]
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _phase_kpis(experiment: Path) -> dict[str, dict[str, float]]:
    output = {}
    for path in experiment.glob("yba-*/phases/*/summary.csv"):
        with path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        if rows:
            row = rows[0]
            output[str(row["label"])] = {
                "throughput_ops_s": float(row["throughput"]),
                "p99_latency_us": float(row["p99_latency"]),
                "errors": float(row.get("error_count") or 0),
                "timeouts": float(row.get("timeout_count") or 0),
            }
    return output


def validate_active(
    experiment: Path, manifest: Path, anchors: Path, output: Path
) -> dict[str, Any]:
    phase_load = {
        str(row["phase"]): str(row["load"])
        for row in json.loads(manifest.read_text(encoding="utf-8"))["phases"]
    }
    with anchors.open(newline="", encoding="utf-8") as stream:
        anchor_rows = {row["load"]: row for row in csv.DictReader(stream)}
    kpis = _phase_kpis(experiment)
    rows = []
    for phase, load in phase_load.items():
        if phase not in kpis or load not in anchor_rows:
            continue
        anchor = anchor_rows[load]
        oracle = max(float(anchor["one_throughput_ops_s"]), float(anchor["two_throughput_ops_s"]))
        rows.append({
            "phase": phase,
            "load": load,
            **kpis[phase],
            "static_oracle_ops_s": oracle,
            "oracle_ratio": kpis[phase]["throughput_ops_s"] / oracle if oracle else 0.0,
        })
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as stream:
        if rows:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    weighted_ratio = (
        sum(row["throughput_ops_s"] for row in rows)
        / sum(row["static_oracle_ops_s"] for row in rows)
        if rows else 0.0
    )
    report = {
        "schema": SCHEMA,
        "experiment": str(experiment),
        "phases": len(rows),
        "weighted_oracle_ratio": weighted_ratio,
        "minimum_phase_oracle_ratio": min(
            (row["oracle_ratio"] for row in rows), default=0.0
        ),
        "errors": sum(row["errors"] for row in rows),
        "timeouts": sum(row["timeouts"] for row in rows),
        "online_kpi_rows": sum(
            bool(row.get("kpi"))
            for row in _jsonl(experiment / "controller" / "samples.jsonl")
        ),
    }
    report["passed"] = bool(
        report["phases"] == len(phase_load)
        and report["weighted_oracle_ratio"] >= 0.98
        and report["errors"] == 0
        and report["timeouts"] == 0
        and report["online_kpi_rows"] == 0
    )
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def execute_stage_d(
    root: Path,
    selected: Path,
    model: Path,
    stage_c_validation: Path,
    base_config: Path,
    scenario: Path,
    manifest: Path,
    anchors: Path,
    *,
    deploy: bool = True,
) -> dict[str, Any]:
    model_value = json.loads(model.read_text(encoding="utf-8"))
    stage_c = json.loads(stage_c_validation.read_text(encoding="utf-8"))
    prerequisites = {
        "stage_b_model_passed": bool(model_value.get("validation", {}).get("passed")),
        "stage_c_passed": bool(stage_c.get("passed")),
    }
    if not all(prerequisites.values()):
        result = {"schema": SCHEMA, "prerequisites": prerequisites, "status": "blocked"}
        (root / "summary").mkdir(parents=True, exist_ok=True)
        (root / "summary" / "stage-d-validation.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n"
        )
        return result
    generated = root / "generated-config"
    summary = root / "summary"
    generated.mkdir(parents=True, exist_ok=True)
    summary.mkdir(parents=True, exist_ok=True)
    deployment = {"status": "skipped"}
    if deploy:
        bundle = generated / "prism-blackbox-arm64.tar.gz"
        build_bundle(bundle, source_host="kunpen183", source_root="/home/xhc/prism-threads")
        install_bundle(Host("kunpen183"), bundle, "/home/xhc/prism-sampler")
        install_client(Host("ubuntu197"), "/home/xhc/.local/src/prism-sampler")
        deployment = {"status": "complete", "bundle": str(bundle)}
    shadow = [
        _run(
            generated, selected, model, base_config, scenario,
            mode="shadow", initial=initial, name=f"blackbox-stage-d-shadow-{initial}",
        )
        for initial in ("one_node", "two_node")
    ]
    shadow_report = validate_shadow(shadow, summary / "stage-d-shadow-validation.json")
    active_path = None
    active_report = {"status": "blocked", "reason": "shadow_gate_failed"}
    if shadow_report["passed"]:
        active_path = _run(
            generated, selected, model, base_config, scenario,
            mode="active", initial="one_node", name="blackbox-stage-d-active",
        )
        active_report = validate_active(
            active_path, manifest, anchors, summary / "stage-d-active-validation.json"
        )
    result = {
        "schema": SCHEMA,
        "finished_realtime_ns": time.time_ns(),
        "prerequisites": prerequisites,
        "deployment": deployment,
        "shadow": shadow_report,
        "active": active_report,
        "active_experiment": str(active_path) if active_path else None,
        "passed": bool(shadow_report["passed"] and active_report.get("passed")),
    }
    (summary / "stage-d-validation.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    return result
