from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _phase_runs(experiment: Path) -> list[tuple[Path, dict[str, Any]]]:
    result = []
    for db in sorted(experiment.glob("runs/**/dataset/telemetry.db3")):
        phase_path = db.parents[1] / "meta" / "phase.json"
        if phase_path.is_file():
            phase = json.loads(phase_path.read_text(encoding="utf-8"))
            if phase.get("workload_start_epoch_ns") is not None:
                result.append((db, phase))
    return result


def _between(row: dict[str, Any], start: int, end: int) -> bool:
    stamp = int(row.get("realtime_ns") or row.get("started_realtime_ns") or 0)
    return start <= stamp < end


def _import_tables(
    db: Path,
    phase: str,
    samples: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> dict[str, int]:
    import duckdb
    import pandas as pd

    sample_rows = [{
        "realtime_ns": int(row.get("realtime_ns", 0)),
        "monotonic_ns": int(row.get("monotonic_ns", 0)),
        "phase": phase,
        "policy_state": row.get("policy_state", ""),
        "actual_state": row.get("actual_state", ""),
        "workload_active": bool(row.get("workload_active")),
        "valid": bool(row.get("valid")),
        "interval_seconds": float(row.get("interval_seconds") or 0),
        "run_cpu_equiv": row.get("run_cpu_equiv"),
        "rq_cpu_equiv": row.get("rq_cpu_equiv"),
        "run_pressure": row.get("run_pressure"),
        "rq_pressure": row.get("rq_pressure"),
        "tids_observed": int(row.get("tids_observed") or 0),
        "node_cpu_json": json.dumps(row.get("node_cpu_utilization", {}), sort_keys=True),
        "numa_pages_json": json.dumps(row.get("numa_pages", {}), sort_keys=True),
        "psi_cpu_json": json.dumps(row.get("psi_cpu", {}), sort_keys=True),
        "error": row.get("error"),
    } for row in samples]
    decision_rows = [{
        "realtime_ns": int(row.get("realtime_ns", 0)),
        "phase": phase,
        "mode": row.get("mode", ""),
        "current_state": row.get("current_state", ""),
        "target_state": row.get("target_state", ""),
        "action": row.get("action"),
        "reason": row.get("reason", ""),
        "expand_matches": int(row.get("expand_matches") or 0),
        "shrink_elapsed_seconds": float(row.get("shrink_elapsed_seconds") or 0),
    } for row in decisions]
    action_rows = [{
        "realtime_ns": int(row.get("realtime_ns") or row.get("started_realtime_ns") or 0),
        "phase": phase,
        "mode": row.get("mode", ""),
        "action": row.get("action", ""),
        "from_state": row.get("from_state", ""),
        "to_state": row.get("to_state", ""),
        "status": row.get("status", ""),
        "cpus": row.get("cpus", ""),
        "error": row.get("error"),
        "details_json": json.dumps(row, sort_keys=True),
    } for row in actions]
    con = duckdb.connect(str(db))
    definitions = {
        "controller_samples": (
            "realtime_ns BIGINT, monotonic_ns BIGINT, phase VARCHAR, policy_state VARCHAR, "
            "actual_state VARCHAR, workload_active BOOLEAN, valid BOOLEAN, interval_seconds DOUBLE, "
            "run_cpu_equiv DOUBLE, rq_cpu_equiv DOUBLE, run_pressure DOUBLE, rq_pressure DOUBLE, "
            "tids_observed INTEGER, node_cpu_json VARCHAR, numa_pages_json VARCHAR, "
            "psi_cpu_json VARCHAR, error VARCHAR"
        ),
        "controller_decisions": (
            "realtime_ns BIGINT, phase VARCHAR, mode VARCHAR, current_state VARCHAR, "
            "target_state VARCHAR, action VARCHAR, reason VARCHAR, expand_matches INTEGER, "
            "shrink_elapsed_seconds DOUBLE"
        ),
        "controller_actions": (
            "realtime_ns BIGINT, phase VARCHAR, mode VARCHAR, action VARCHAR, from_state VARCHAR, "
            "to_state VARCHAR, status VARCHAR, cpus VARCHAR, error VARCHAR, details_json VARCHAR"
        ),
    }
    frames = {
        "controller_samples": sample_rows,
        "controller_decisions": decision_rows,
        "controller_actions": action_rows,
    }
    for table, definition in definitions.items():
        con.execute(f"CREATE OR REPLACE TABLE {table} ({definition})")
        if frames[table]:
            frame = pd.DataFrame(frames[table])
            con.register("controller_frame", frame)
            con.execute(f"INSERT INTO {table} SELECT * FROM controller_frame")
            con.unregister("controller_frame")
    con.close()
    return {name: len(rows) for name, rows in frames.items()}


def _yba_kpi(yba_output: Path | None) -> dict[str, dict[str, str]]:
    result = {}
    if yba_output is None:
        return result
    for path in yba_output.glob("phases/*/summary.csv"):
        with path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        if rows:
            result[rows[0].get("label", path.parent.name)] = rows[0]
    return result


def import_controller_experiment(
    experiment: Path, *, yba_output: Path | None = None
) -> dict[str, Any]:
    controller = experiment / "controller"
    samples = _jsonl(controller / "samples.jsonl")
    decisions = _jsonl(controller / "decisions.jsonl")
    actions = _jsonl(controller / "actions.jsonl")
    if not samples and not decisions and not actions:
        return {"status": "skipped", "reason": "controller artifacts are absent"}
    summary_rows = []
    imported = 0
    kpis = _yba_kpi(yba_output)
    for db, context in _phase_runs(experiment):
        start = int(context["workload_start_epoch_ns"])
        end = int(context["workload_end_epoch_ns"])
        phase = str(context.get("phase", ""))
        phase_samples = [row for row in samples if _between(row, start, end)]
        phase_decisions = [row for row in decisions if _between(row, start, end)]
        phase_actions = [row for row in actions if _between(row, start, end)]
        _import_tables(db, phase, phase_samples, phase_decisions, phase_actions)
        imported += 1
        valid = [row for row in phase_samples if row.get("valid")]
        interval = sum(float(row.get("interval_seconds") or 0) for row in valid)
        two_seconds = sum(
            float(row.get("interval_seconds") or 0)
            for row in valid
            if row.get("actual_state") == "two_node"
        )
        kpi = kpis.get(phase, {})
        summary_rows.append({
            "phase": phase,
            "samples": len(phase_samples),
            "valid_samples": len(valid),
            "run_cpu_equiv_mean": (
                sum(float(row["run_cpu_equiv"]) for row in valid) / len(valid)
                if valid else ""
            ),
            "rq_cpu_equiv_mean": (
                sum(float(row["rq_cpu_equiv"]) for row in valid) / len(valid)
                if valid else ""
            ),
            "two_node_seconds": two_seconds,
            "observed_seconds": interval,
            "two_node_share": two_seconds / interval if interval else "",
            "actions": sum(1 for row in phase_actions if row.get("action") not in {"initialize", "restore"}),
            "throughput_ops_s": kpi.get("throughput", ""),
            "p99_latency_us": kpi.get("p99_latency", ""),
        })
    controller.mkdir(parents=True, exist_ok=True)
    output = controller / "summary.csv"
    if summary_rows:
        with output.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(summary_rows[0]))
            writer.writeheader()
            writer.writerows(summary_rows)
    total = sum(float(row["observed_seconds"] or 0) for row in summary_rows)
    two = sum(float(row["two_node_seconds"] or 0) for row in summary_rows)
    report = [
        "# Pressure-Aware NUMA Controller Report",
        "",
        f"- Imported phases: `{imported}`",
        f"- Controller samples: `{len(samples)}`",
        f"- Controller actions: `{len(actions)}`",
        f"- Two-node residency: `{two:.1f}s / {total:.1f}s` ({(two / total if total else 0):.2%})",
        "",
        "The controller reports realized state and pressure evidence only; no numeric G is claimed.",
    ]
    (controller / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return {"status": "complete", "phases": imported, "samples": len(samples), "actions": len(actions)}
