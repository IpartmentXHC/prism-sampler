from __future__ import annotations

import csv
from pathlib import Path

import json
import pytest

from prism_sampler.config import load_config
from prism_sampler.pressure_v2 import (
    _numa_node_share_window,
    aggregate_matrix,
    analyze_g,
    _taskstats_pressure,
    closed_loop_rows,
    crossover_samples,
    render_controller_config,
    select_config,
    select_finalist_config,
    validate_controller_actions,
    write_hardware_graph_reference,
    write_online_graph_index,
)
from prism_sampler.pressure_v2_runner import closed_loop_schedule


def test_select_config_uses_shared_max_threads_and_state_specific_slots() -> None:
    rows = []
    for cpus in (32, 64):
        for maximum in (32, 64, 128):
            for slots in (32, 64, 128):
                for load, offered in (("C2T2", 4), ("C4T6", 24), ("C5T16", 80)):
                    throughput = 100.0
                    if maximum == 64:
                        throughput += 20
                    if slots == (32 if cpus == 32 else 64):
                        throughput += 10
                    rows.append({
                        "cpu_count": cpus,
                        "max_threads": maximum,
                        "slots": slots,
                        "load": load,
                        "offered_threads": offered,
                        "throughput_ops_s": throughput,
                        "p99_latency_us": 10,
                        "error_count": 0,
                        "timeout_count": 0,
                    })
    selected = select_config(aggregate_matrix(rows))
    assert selected["max_threads"] == 64
    assert selected["one_node_slots"] == 32
    assert selected["two_node_slots"] == 64
    assert len(selected["signatures"]) == 3


def test_taskstats_pressure_time_weights_targets_and_rejects_invalid_shares(
    tmp_path: Path,
) -> None:
    import duckdb

    database = tmp_path / "telemetry.db3"
    con = duckdb.connect(str(database))
    con.execute(
        "CREATE TABLE taskstats_view("
        "ts TIMESTAMP, pid INTEGER, time_diff BIGINT, "
        "run_share DOUBLE, rq_share DOUBLE)"
    )
    con.execute(
        "INSERT INTO taskstats_view VALUES "
        "(TIMESTAMP '1970-01-01 00:00:10', 42, 1000000000, 0.5, 0.25), "
        "(TIMESTAMP '1970-01-01 00:00:11', 42, 1000000000, 1.0, 0.5), "
        "(TIMESTAMP '1970-01-01 00:00:11', 99, 1000000000, 1.0, 1.0), "
        "(TIMESTAMP '1970-01-01 00:00:11', 42, 1000000000, 2.0, 3.0)"
    )
    con.close()
    context = tmp_path / "phase.json"
    context.write_text(json.dumps({
        "workload_start_epoch_ns": 10_000_000_000,
        "workload_end_epoch_ns": 12_000_000_000,
        "target_processes": [{"pid": 42, "start_time": 1}],
    }))

    run, rq = _taskstats_pressure(database, context)

    assert run == pytest.approx(0.75)
    assert rq == pytest.approx(0.375)


def test_final_selection_cannot_reopen_an_unreplicated_strategy() -> None:
    rows = []
    for cpus in (32, 64):
        for maximum in (32, 64, 128):
            for slots in (32, 64, 128):
                for load in ("C2T2", "C4T6", "C5T16"):
                    throughput = 100.0
                    if maximum == 64:
                        throughput = 120.0
                    if maximum == 128:
                        throughput = 1000.0
                    rows.append({
                        "cpu_count": cpus,
                        "max_threads": maximum,
                        "slots": slots,
                        "default_reference": False,
                        "load": load,
                        "throughput_median_ops_s": throughput,
                    })
    finalists = [
        {"max_threads": 32, "one_node_slots": 32, "two_node_slots": 32},
        {"max_threads": 64, "one_node_slots": 64, "two_node_slots": 64},
    ]

    selected = select_finalist_config(rows, finalists)

    assert selected["max_threads"] == 64
    assert {row["max_threads"] for row in selected["finalists"]} == {32, 64}


def test_rendered_active_config_loads_with_transactional_slots(tmp_path: Path) -> None:
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps({
        "max_threads": 64,
        "one_node_slots": 32,
        "two_node_slots": 64,
        "signatures": [],
    }))
    output = tmp_path / "runtime.toml"
    render_controller_config(
        selected,
        output,
        target_host="server",
        output_root=str(tmp_path),
        mode="active",
        initial_state="two_node",
        scripted_transitions=["90:one_node", "210:two_node"],
    )
    config = load_config(output)
    assert config.section("controller")["one_node_slots"] == 32
    assert config.section("controller")["scripted_transitions"] == [
        "90:one_node", "210:two_node"
    ]


def test_crossover_sample_separates_raw_and_static_normalized_gain(tmp_path: Path) -> None:
    experiment = tmp_path / "crossover"
    controller = experiment / "controller"
    controller.mkdir(parents=True)
    kpis = []
    for seconds, throughput, operations in (
        (70, 100, 1000), (80, 100, 1000), (90, 100, 1000),
        (110, 90, 900),
        (120, 140, 1400), (130, 140, 1400), (140, 140, 1400),
    ):
        kpis.append({
            "phase": "C5T16",
            "sequence": len(kpis) + 1,
            "complete": True,
            "window_end_target_epoch_ns": seconds * 1_000_000_000,
            "throughput_ops_s": throughput,
            "operations_delta": operations,
        })
    (controller / "kpi.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in kpis), encoding="utf-8"
    )
    (controller / "actions.jsonl").write_text(json.dumps({
        "action": "scripted_expand",
        "status": "applied",
        "phase": "C5T16",
        "from_state": "one_node",
        "to_state": "two_node",
        "realtime_ns": 100_000_000_000,
    }) + "\n", encoding="utf-8")
    samples = []
    for seconds, run, rq, query, node1 in (
        (70, 28, 8, 20, 10), (80, 28, 8, 20, 10), (90, 28, 8, 20, 10),
        (120, 40, 2, 30, 40), (130, 40, 2, 30, 40), (140, 40, 2, 30, 40),
    ):
        samples.append({
            "phase": "C5T16",
            "realtime_ns": seconds * 1_000_000_000,
            "run_cpu_equiv": run,
            "rq_cpu_equiv": rq,
            "clickhouse_metrics": {"QueryThread": query},
            "numa_pages": {"0": 100 - node1, "1": node1},
        })
    (controller / "samples.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in samples), encoding="utf-8"
    )

    rows = crossover_samples([experiment], {"C5T16": {
        "one_throughput_ops_s": 100,
        "two_throughput_ops_s": 150,
    }})

    assert len(rows) == 1
    row = rows[0]
    assert row["raw_gain_pct"] == pytest.approx(40)
    assert round(row["steady_gain_pct"], 6) == round(100 * (1.4 / 1.5 - 1), 6)
    assert round(row["transition_penalty_pct"], 6) == round(100 / 6000 * 100, 6)
    assert row["run_cpu_equiv_delta"] == 12
    assert row["rq_cpu_equiv_delta"] == -6
    assert row["query_thread_delta"] == 10
    assert round(row["numa_node1_page_share_delta"], 6) == 0.3

    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps({
        "signatures": [{
            "load": "C5T16", "offered_threads": 80,
            "one_throughput_ops_s": 100, "two_throughput_ops_s": 150,
        }],
    }))
    analyzed = analyze_g([experiment], selected, tmp_path / "g")
    assert analyzed["samples"] == 1
    assert analyzed["table"][0]["raw_gain_median_pct"] == pytest.approx(40)


def test_numa_node_share_window_uses_mapped_page_medians(tmp_path: Path) -> None:
    import duckdb

    database = tmp_path / "telemetry.db3"
    con = duckdb.connect(str(database))
    con.execute(
        "CREATE TABLE numa_samples("
        "ts TIMESTAMP, node INTEGER, metric VARCHAR, value DOUBLE, unit VARCHAR)"
    )
    con.execute(
        "INSERT INTO numa_samples VALUES "
        "(TIMESTAMP '1970-01-01 00:00:10', 0, 'mapped', 75, 'pages'), "
        "(TIMESTAMP '1970-01-01 00:00:10', 1, 'mapped', 25, 'pages'), "
        "(TIMESTAMP '1970-01-01 00:00:20', 0, 'mapped', 25, 'pages'), "
        "(TIMESTAMP '1970-01-01 00:00:20', 1, 'mapped', 75, 'pages'), "
        "(TIMESTAMP '1970-01-01 00:00:30', 1, 'resident', 100, 'MB')"
    )
    con.close()

    share = _numa_node_share_window(
        database, 10_000_000_000, 30_000_000_000, 1
    )

    assert share == pytest.approx(0.5)


def test_controller_action_validation_checks_transaction_steps(tmp_path: Path) -> None:
    experiment = tmp_path / "experiment"
    controller = experiment / "controller"
    controller.mkdir(parents=True)
    action = {
        "realtime_ns": 10_000_000_000,
        "action": "expand",
        "phase": "C5T16",
        "from_state": "one_node",
        "to_state": "two_node",
        "status": "applied",
        "steps": [
            {"affinity": {
                "status": "applied", "started_realtime_ns": 10_000_000_000,
                "finished_realtime_ns": 11_000_000_000,
            }},
            {"slots": {
                "status": "applied", "started_realtime_ns": 11_000_000_000,
                "finished_realtime_ns": 12_000_000_000,
            }},
        ],
    }
    (controller / "actions.jsonl").write_text(json.dumps(action) + "\n")

    validation = validate_controller_actions([experiment], tmp_path / "summary")

    assert validation["passed"]
    assert validation["transaction_verified_ratio"] == 1
    assert validation["action_duration_p95_seconds"] == 2


def test_graph_artifact_references_are_content_addressed(tmp_path: Path) -> None:
    calibration = tmp_path / "calibration"
    for name in (
        "manifest.json", "kunpeng920.csv", "kunpeng920.png",
        "derived/hardware-node-edges.csv",
        "derived/core-latency-by-cluster.csv", "derived/memory-latency.csv",
        "derived/stream-bandwidth.csv",
    ):
        path = calibration / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    reference_path = tmp_path / "hardware.json"
    reference = write_hardware_graph_reference(calibration, reference_path)
    assert len(reference["files"]) == 7
    assert all(len(row["sha256"]) == 64 for row in reference["files"])

    experiment = tmp_path / "experiment"
    raw = experiment / "runs" / "C5T16" / "r1" / "raw"
    raw.mkdir(parents=True)
    (raw / "live-summary.json").write_text(json.dumps({
        "snapshots": 30, "emissions": 3,
        "last_pair_candidates": 2, "last_self_candidates": 1,
    }))
    (raw / "live-candidates.jsonl").write_text("{}\n")
    (raw / "live-candidates-latest.json").write_text(json.dumps({
        "quality": {"flags": []},
    }))
    graph = write_online_graph_index([experiment], tmp_path / "graphs.csv")
    assert graph["phase_graphs"] == 1
    assert graph["snapshots"] == 30


def test_closed_loop_rows_accept_randomized_standalone_experiments(tmp_path: Path) -> None:
    def experiment(name: str, throughput: int) -> Path:
        root = tmp_path / name
        phase = root / f"yba-{name}" / "phases" / "0001-C2T2"
        phase.mkdir(parents=True)
        (phase / "summary.csv").write_text(
            "label,throughput,p99_latency,error_count,timeout_count\n"
            f"C2T2,{throughput},100,0,0\n",
            encoding="utf-8",
        )
        return root

    one = experiment("one", 100)
    two = experiment("two", 120)
    dynamic = experiment("dynamic", 118)
    rows = closed_loop_rows(
        {"one_node": [one], "two_node": [two]}, [dynamic]
    )

    assert [row["mode"] for row in rows] == ["one_node", "two_node", "dynamic"]
    assert [row["throughput_ops_s"] for row in rows] == [100, 120, 118]


def test_closed_loop_schedule_is_reproducible_randomized_blocks() -> None:
    schedule = closed_loop_schedule()
    assert schedule == closed_loop_schedule()
    assert len(schedule) == 9
    for start in range(0, 9, 3):
        assert {mode for mode, _ in schedule[start:start + 3]} == {
            "static_one", "static_two", "dynamic"
        }
    for mode in ("static_one", "static_two", "dynamic"):
        assert sorted(round_number for value, round_number in schedule if value == mode) == [1, 2, 3]
