from __future__ import annotations

import json
from pathlib import Path

import duckdb

from prism_sampler.controller.artifacts import import_controller_experiment


def append(path: Path, row: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row) + "\n")


def test_controller_rows_are_sliced_into_phase_database(tmp_path: Path) -> None:
    experiment = tmp_path / "experiment"
    run = experiment / "runs/low/r1"
    (run / "dataset").mkdir(parents=True)
    (run / "meta").mkdir()
    db = run / "dataset/telemetry.db3"
    duckdb.connect(str(db)).close()
    (run / "meta/phase.json").write_text(json.dumps({
        "phase": "low",
        "workload_start_epoch_ns": 100,
        "workload_end_epoch_ns": 200,
    }))
    append(experiment / "controller/samples.jsonl", {
        "realtime_ns": 150,
        "monotonic_ns": 10,
        "phase": "low",
        "policy_state": "one_node",
        "actual_state": "one_node",
        "workload_active": True,
        "valid": True,
        "interval_seconds": 10,
        "run_cpu_equiv": 4,
        "rq_cpu_equiv": 0,
        "run_pressure": 0.125,
        "rq_pressure": 0,
        "tids_observed": 2,
    })
    append(experiment / "controller/decisions.jsonl", {
        "realtime_ns": 150,
        "current_state": "one_node",
        "target_state": "one_node",
        "reason": "hold",
    })
    result = import_controller_experiment(experiment)
    assert result["status"] == "complete"
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT count(*) FROM controller_samples").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM controller_decisions").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM controller_actions").fetchone()[0] == 0
    con.close()
    assert (experiment / "controller/summary.csv").is_file()
