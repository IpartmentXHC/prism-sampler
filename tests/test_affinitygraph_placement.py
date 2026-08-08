from __future__ import annotations

import json
from pathlib import Path

import duckdb

from prism_sampler.affinitygraph_placement import (
    _legacy_unframed_canaries,
    compare_placement,
    export_placement,
    export_placement_excel,
)
from prism_sampler.cli import build_parser


def _write_log(path: Path) -> None:
    events = [
        {"timestamp_ns": 1, "type": "runtime_start", "effective_mode": "observe"},
        {"timestamp_ns": 2, "type": "topology_cpu", "cpu": 0, "node": 0,
         "online": True, "in_envelope": True},
        {"timestamp_ns": 3, "type": "topology_cpu", "cpu": 1, "node": 0,
         "online": True, "in_envelope": True},
        {"timestamp_ns": 4, "type": "topology_edge", "from_node": 0,
         "to_node": 0, "numa_distance": 10},
        {"timestamp_ns": 10, "type": "solve_window_begin", "window_id": "run-1",
         "sequence": 1, "mode": "observe"},
        {"timestamp_ns": 11, "type": "thread_window", "window_id": "run-1",
         "tgid": 7, "tid": 11, "starttime": 101, "comm": "worker_1",
         "parent_tid": 7, "start_routine": 0, "start_symbol": "", "group": "worker",
         "demand": 0.5, "confidence": 1.0, "current_cpu": 0,
         "allowed_cpus": [0, 1], "state": "S", "sample_count": 10,
         "runtime_delta_ns": 5, "runqueue_delta_ns": 1,
         "voluntary_switches_delta": 2, "involuntary_switches_delta": 0},
        {"timestamp_ns": 12, "type": "thread_window", "window_id": "run-1",
         "tgid": 7, "tid": 12, "starttime": 102, "comm": "worker_2",
         "parent_tid": 7, "start_routine": 0, "start_symbol": "", "group": "worker",
         "demand": 0.4, "confidence": 1.0, "current_cpu": 1,
         "allowed_cpus": [0, 1], "state": "S", "sample_count": 10,
         "runtime_delta_ns": 4, "runqueue_delta_ns": 1,
         "voluntary_switches_delta": 2, "involuntary_switches_delta": 0},
        {"timestamp_ns": 13, "type": "relation_edge", "window_id": "run-1",
         "from_tid": 11, "to_tid": 12, "activity": 0.8, "sync": 0.7,
         "share": 0.1, "stability": 0.9, "score": 10.0, "handoff_rate": 2.0,
         "shared_vfs_seconds": 0.1, "active_overlap": 1.0,
         "observation_count": 10, "coverage": 1.0, "cv": 0.1},
        {"timestamp_ns": 14, "type": "bpf_health", "window_id": "run-1",
         "valid": True, "error": 0, "emitted": 100, "dropped": 0,
         "loss_ratio": 0.0, "window_ready": True},
        {"timestamp_ns": 19, "type": "plan", "window_id": "run-1",
         "strategy_id": "numa-domain-v1",
         "family_metrics": [{"name": "worker@pool", "thread_count": 2,
                              "demand": 0.9, "internal_relation": 10.0,
                              "external_relation": 0.0, "self_containment": 1.0,
                              "relative_internal": 1.0, "confirmation": 3,
                              "anchor": True}],
         "domains": [{"id": "worker@pool", "families": ["worker@pool"],
                      "thread_count": 2, "target_nodes": [0],
                      "target_mask": "0-1", "demand": 0.9,
                      "confirmation": 3, "valid": True,
                      "invalid_reason": ""}],
         "planned_masks": {"11": "0-1", "12": "1,0"}},
        {"timestamp_ns": 20, "type": "solve_window_end", "window_id": "run-1",
         "complete": True, "outcome": "observed"},
    ]
    path.write_text("".join(json.dumps(event) + "\n" for event in events))


def test_export_placement_preserves_window_identity_and_views(tmp_path: Path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    _write_log(run / "runtime.jsonl")
    output = tmp_path / "dataset"
    report = export_placement(run, output)
    assert report["snapshots"] == 1
    assert (output / "placement.duckdb").is_file()
    assert (output / "threads.csv").is_file()
    sequence = json.loads((output / "sequence.json").read_text())
    assert sequence == {
        "schema": "affinitygraph.replay-sequence.v1",
        "frames": [{"timestamp_ns": 10, "snapshot": "snapshots/run-1.json"}],
    }
    snapshot = json.loads((output / "snapshots" / "run-1.json").read_text())
    assert {(row["tid"], row["starttime"]) for row in snapshot["threads"]} == {
        (11, 101), (12, 102)
    }
    connection = duckdb.connect(str(output / "placement.duckdb"), read_only=True)
    assert connection.execute("SELECT count(*) FROM threads").fetchone()[0] == 2
    assert connection.execute("SELECT from_starttime, to_starttime FROM edges").fetchone() == (101, 102)
    assert connection.execute("SELECT family_name, anchor FROM family_metrics").fetchone() == (
        "worker@pool", True
    )
    assert connection.execute("SELECT target_nodes, target_mask FROM domains").fetchone() == (
        [0], [0, 1]
    )
    assert connection.execute("SELECT count(*) FROM planned_masks").fetchone()[0] == 2
    assert connection.execute("SELECT value FROM manifest WHERE key='source_hashes'").fetchone()[0]
    connection.close()


def test_affinitygraph_placement_cli_contract() -> None:
    parser = build_parser()
    export_args = parser.parse_args([
        "affinitygraph", "export-placement", "--run", "/tmp/run", "--output", "/tmp/out"
    ])
    assert export_args.affinitygraph_command == "export-placement"
    compare_args = parser.parse_args([
        "affinitygraph", "compare-placement", "--dataset", "/tmp/data.duckdb",
        "--candidates", "/tmp/candidates", "--output", "/tmp/report.json"
    ])
    assert compare_args.affinitygraph_command == "compare-placement"


def test_canonical_v11_concentration_canary() -> None:
    other_cpus = [cpu for cpu in range(128) if cpu != 17]
    assignments = {str(tid): (17 if tid < 568 else other_cpus[(tid - 568) % len(other_cpus)])
                   for tid in range(721)}
    canary = _legacy_unframed_canaries(
        [{"type": "action", "timestamp_ns": 1, "assignments": assignments}], 128
    )[0]
    assert canary["classification"] == "legacy_unframed"
    assert canary["max_threads_on_one_cpu"] == 568
    assert canary["dynamic_slot_cap"] == 8
    assert canary["pathological_concentration"] is True


def test_compare_placement_runs_fixed_candidates(tmp_path: Path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    _write_log(run / "runtime.jsonl")
    dataset_root = tmp_path / "dataset"
    export_placement(run, dataset_root)
    report_path = tmp_path / "comparison.json"
    strategies = Path(__file__).resolve().parents[2] / "affinitygraph" / "strategies"
    report = compare_placement(dataset_root / "placement.duckdb", strategies, report_path)
    assert report_path.is_file()
    assert len(report["candidates"]) == 7
    assert any(row["strategy_id"] == "legacy-v1" for row in report["candidates"])
    assert all(row["relative_to_legacy"] is not None for row in report["candidates"])
    assert report["hard_gate_then_pareto"] is True


def test_legacy_plan_batch_can_be_reconstructed_and_exported_to_excel(tmp_path: Path) -> None:
    run = tmp_path / "legacy"
    run.mkdir()
    events = [
        {"timestamp_ns": 1, "type": "runtime_start", "root_pid": 7,
         "effective_mode": "plan"},
        {"timestamp_ns": 10, "type": "thread_window", "tid": 11,
         "starttime": 101, "group": "worker", "demand": 0.5,
         "confidence": 1.0, "current_cpu": 0},
        {"timestamp_ns": 11, "type": "thread_window", "tid": 12,
         "starttime": 102, "group": "worker", "demand": 0.4,
         "confidence": 1.0, "current_cpu": 1},
        {"timestamp_ns": 12, "type": "relation_edge", "from_tid": 11,
         "to_tid": 12, "activity": 1.0, "sync": 1.0, "share": 0.0,
         "stability": 1.0, "score": 10.0},
        {"timestamp_ns": 13, "type": "plan", "threads": 2,
         "overload": 0.0, "relation_cost": 10.0, "migration_cost": 0.0,
         "confirmation": 1, "assignments": {"11": 0, "12": 1}},
    ]
    (run / "runtime.jsonl").write_text(
        "".join(json.dumps(event) + "\n" for event in events)
    )
    output = tmp_path / "dataset"
    report = export_placement(run, output)
    assert report["snapshots"] == 1
    connection = duckdb.connect(str(output / "placement.duckdb"), read_only=True)
    assert connection.execute("SELECT outcome FROM solve_windows").fetchone()[0] == "legacy_reconstructed"
    assert connection.execute("SELECT count(*) FROM assignments").fetchone()[0] == 2
    connection.close()
    workbook = tmp_path / "placement.xlsx"
    counts = export_placement_excel(output / "placement.duckdb", workbook)
    assert workbook.is_file()
    assert counts["Threads"] == 2
