from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from prism_sampler.affinitygraph_runner import (
    AffinityGraphRunner,
    DorisFormalRunner,
    _continuous_active_interval,
    analyze,
    analyze_doris_formal,
    analyze_incremental_smoke,
    analyze_positive_control,
    approve_smoke,
    doris_formal_design,
    positive_control_profiles,
    positive_control_schedule,
    schedule,
)
from prism_sampler.affinitygraph_hook import (
    _active_ready,
    _relay,
    _requires_active_readiness,
    _target_identity,
)
from prism_sampler.cli import build_parser
from prism_sampler.remote import CommandResult


def test_schedule_is_deterministic_and_paired() -> None:
    first = schedule(3, 7)
    assert first == schedule(3, 7)
    assert len(first) == 18
    for pair in range(1, 10):
        rows = [row for row in first if row["pair_index"] == pair]
        assert len(rows) == 2
        assert {row["treatment"] for row in rows} == {"baseline", "active"}
        assert len({(row["load"], row["round"]) for row in rows}) == 1


def test_doris_formal_design_is_frozen_and_balanced() -> None:
    design = doris_formal_design()
    assert design["name"] == "doris-random-v1"
    assert design["run_order_seed"] == 20260811
    assert len(design["run_order"]) == 8
    assert sum(row["treatment"] == "baseline" for row in design["run_order"]) == 2
    assert sum(row["treatment"] == "active" for row in design["run_order"]) == 6
    assert design["gates"]["maximum_supervisor_rss_kib"] == 512 * 1024
    for trajectory in design["trajectories"].values():
        phases = trajectory["phases"]
        assert len(phases) == 6
        assert sum(row["seconds"] for row in phases) == 540
        assert {row["load"] for row in phases} == {
            "C1T1", "C2T2", "C3T4", "C4T8", "C5T12", "C5T16",
        }


def test_doris_formal_scenario_uses_same_acquisition_for_all_treatments(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "doris.env"
    config.write_text("DB_TYPE=doris\n", encoding="utf-8")
    runner = DorisFormalRunner(tmp_path / "run", config, source)
    scenario, phases = runner._formal_scenario("S1")
    value = scenario.read_text(encoding="utf-8")
    assert "SCENARIO_PHASES='PLACEMENT WARMUP M01_C3T4" in value
    assert "SCENARIO_PHASE_PLACEMENT_CLIENTS=4" in value
    assert "SCENARIO_PHASE_PLACEMENT_THREADS=8" in value
    assert "SCENARIO_PHASE_PLACEMENT_VALUE=150" in value
    assert len(phases) == 6


def test_doris_formal_analysis_uses_two_independent_seed_blocks() -> None:
    rows = []
    for trajectory, baseline in (("S1", 100.0), ("S2", 200.0)):
        rows.append({
            "status": "complete", "valid": True, "trajectory": trajectory,
            "treatment": "baseline", "repeat": 1,
            "active_throughput": baseline, "lifecycle_throughput": baseline * 0.8,
            "p99_latency_us": 100,
        })
        for repeat, ratio in enumerate((1.01, 1.02, 1.03), 1):
            rows.append({
                "status": "complete", "valid": True, "trajectory": trajectory,
                "treatment": "active", "repeat": repeat,
                "active_throughput": baseline * ratio,
                "lifecycle_throughput": baseline * 0.8 * ratio,
                "p99_latency_us": 105,
                "formal_health": {"passed": True},
            })
    report = analyze_doris_formal(rows, samples=100)
    assert report["complete"]
    assert report["independent_baseline_blocks"] == 2
    assert report["active_repeats"] == 6
    assert report["candidate_pass"]
    assert abs(report["seed_balanced_mean_uplift"] - 0.02) < 1e-12


def test_trajectory_kpi_excludes_phase_gaps_from_primary_throughput(
    tmp_path: Path,
) -> None:
    output = tmp_path / "yba"
    phases = ["M01_C1T1", "M02_C2T2"]
    for sequence, phase in enumerate(phases, 1):
        phase_dir = output / f"phases/{sequence:04d}-{phase}"
        phase_dir.mkdir(parents=True)
        (phase_dir / "summary.csv").write_text(
            "label,throughput,p95_latency,p99_latency,p999_latency,"
            "error_count,timeout_count,runtime_ms_max,client_logs,read_ops\n"
            f"{phase},100,10,20,30,0,0,60000,1,6000\n",
            encoding="utf-8",
        )
    (output / "scenario-timeline.csv").write_text(
        "sequence,phase,started_epoch_ns,finished_epoch_ns\n"
        "1,M01_C1T1,1000000000,61000000000\n"
        "2,M02_C2T2,101000000000,161000000000\n",
        encoding="utf-8",
    )
    result = AffinityGraphRunner._parse_trajectory_kpi(output, phases)
    assert result["active_throughput"] == 100
    assert result["lifecycle_throughput"] == 75
    assert result["phase_gaps"][0]["seconds"] == 40


def test_baseline_stability_uses_same_load_across_trajectories() -> None:
    rows = []
    for trajectory, scale in (("S1", 1.0), ("S2", 1.04)):
        rows.append({
            "status": "complete", "treatment": "baseline",
            "trajectory": trajectory,
            "measurement_phases": [
                {"load": load, "throughput": 100 * scale}
                for load in ("C1T1", "C2T2", "C3T4", "C4T8", "C5T12", "C5T16")
            ],
        })
    report = DorisFormalRunner._baseline_stability(rows)
    assert report["passed"]
    assert report["median_relative_difference"] < 0.05


def test_positive_control_profiles_match_validated_static_masks() -> None:
    doris = positive_control_profiles("doris")
    assert list(doris) == [
        "unrestricted", "one_node", "light_pipe_unlimited",
        "light_pipe_limited",
    ]
    assert doris["unrestricted"] == {}
    assert doris["one_node"]["THREAD_CLUSTER_RULES"] == "all:.*:0-31"
    assert doris["one_node"]["THREAD_CLUSTER_BIND_MAX_SECONDS"] == "0"
    assert doris["light_pipe_unlimited"]["THREAD_CLUSTER_DEFAULT_CPUS"] == "0-127"
    assert doris["light_pipe_limited"]["THREAD_CLUSTER_DEFAULT_CPUS"] == "96-127"
    clickhouse = positive_control_profiles("clickhouse")
    assert list(clickhouse) == ["two_node", "threadpool_compact"]
    assert "SERVER_CPU_NODES" not in clickhouse["two_node"]
    assert clickhouse["two_node"]["THREAD_CLUSTER_RULES"] == "all:.*:0-63"
    assert clickhouse["threadpool_compact"]["THREAD_CLUSTER_RULES"] == (
        "candidate:^ThreadPool$:0-31"
    )


def test_positive_control_schedule_is_deterministic_and_balanced() -> None:
    rows = positive_control_schedule("doris", 2, 17)
    assert rows == positive_control_schedule("doris", 2, 17)
    assert len(rows) == 8
    for round_number in (1, 2):
        round_rows = [row for row in rows if row["round"] == round_number]
        assert {row["profile"] for row in round_rows} == set(
            positive_control_profiles("doris")
        )
        assert {row["position"] for row in round_rows} == {1, 2, 3, 4}


def test_positive_control_analysis_attributes_selector_when_all_contrasts_win() -> None:
    rows = []
    for round_number in (1, 2):
        for profile, throughput in {
            "unrestricted": 100, "light_pipe_unlimited": 110,
            "one_node": 90, "light_pipe_limited": 99,
        }.items():
            rows.append({
                "round": round_number, "profile": profile,
                "throughput": throughput, "valid": True,
            })
    report = analyze_positive_control(rows, "doris")
    assert report["complete"]
    assert report["attribution"] == "selector"
    assert all(item["pairs"] == 2 for item in report["contrasts"])


def test_positive_control_analysis_requires_every_round_and_detects_no_gain() -> None:
    rows = [
        {"round": round_number, "profile": profile, "throughput": throughput}
        for round_number in (1, 2)
        for profile, throughput in {
            "two_node": 100, "threadpool_compact": 95,
        }.items()
    ]
    report = analyze_positive_control(rows, "clickhouse")
    assert report["complete"]
    assert report["attribution"] == "collector_or_manual_evidence"
    incomplete = analyze_positive_control(rows[:2], "clickhouse")
    assert not incomplete["complete"]
    assert incomplete["attribution"] == "mixed_or_incomplete"


def test_active_duration_excludes_blocking_readiness_interval() -> None:
    assert _continuous_active_interval(
        10.0,
        100.0,
        previous_measurement_open=False,
        current_measurement_open=True,
        previous_active=False,
        current_active=True,
    ) == 0.0


def test_active_readiness_is_checked_after_acquisition_and_before_measurement() -> None:
    assert not _requires_active_readiness(
        "active", "phase_before", "PLACEMENT", "C4T4", "WARMUP"
    )
    assert _requires_active_readiness(
        "active", "phase_before", "WARMUP", "C4T4", "WARMUP"
    )
    assert _requires_active_readiness(
        "active", "phase_before", "C4T4", "C4T4", "WARMUP"
    )
    assert not _requires_active_readiness(
        "plan", "phase_before", "C4T4", "C4T4", "WARMUP"
    )
    assert _continuous_active_interval(
        100.0,
        101.0,
        previous_measurement_open=True,
        current_measurement_open=True,
        previous_active=True,
        current_active=True,
    ) == 1.0
    assert _continuous_active_interval(
        101.0,
        190.0,
        previous_measurement_open=True,
        current_measurement_open=False,
        previous_active=True,
        current_active=True,
    ) == 0.0


def test_analysis_uses_paired_uplift_and_frozen_weights() -> None:
    rows = []
    gains = {"C2T2": 0.01, "C4T6": 0.02, "C5T16": 0.05}
    for load, gain in gains.items():
        for round_number in range(1, 4):
            rows.extend([
                {"load": load, "round": round_number, "treatment": "baseline", "throughput": 100},
                {"load": load, "round": round_number, "treatment": "active", "throughput": 100 * (1 + gain)},
            ])
    result = analyze(rows, samples=100)
    assert abs(result["weighted_mean_uplift"] - 0.033) < 1e-12
    assert result["candidate_pass"]
    assert result["load_summary"]["C2T2"]["baseline_cv"] == 0
    assert len(result["weighted_bootstrap_95_ci"]) == 2


def test_analysis_excludes_invalid_pairs_and_requires_five() -> None:
    rows = []
    for load in ("C2T2", "C4T6", "C5T16"):
        for round_number in range(1, 6):
            valid = not (load == "C4T6" and round_number == 5)
            rows.extend([
                {"load": load, "round": round_number, "treatment": "baseline",
                 "throughput": 100, "valid": valid},
                {"load": load, "round": round_number, "treatment": "active",
                 "throughput": 105, "valid": valid},
            ])
    result = analyze(rows, samples=20, required_pairs=5)
    assert not result["complete"]
    assert not result["candidate_pass"]
    assert result["load_summary"]["C4T6"]["valid_pairs"] == 4


def test_scenario_has_independent_warmup_and_measurement(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "yba.env"
    config.write_text("", encoding="utf-8")
    runner = AffinityGraphRunner(tmp_path / "run", config, source, rounds=5, seed=7)
    assert runner.state["smoke_observe_max_overhead_ratio"] == 0.05
    value = runner._scenario("C4T6", 300).read_text(encoding="utf-8")
    assert "SCENARIO_PHASES='WARMUP C4T6'" in value
    assert "SCENARIO_MIN_PHASE_SECONDS=30" in value
    assert "SCENARIO_PHASE_WARMUP_VALUE=30" in value
    assert "SCENARIO_PHASE_C4T6_VALUE=300" in value

    active = runner._scenario(
        "C4T6", 300, acquisition_seconds=210
    ).read_text(encoding="utf-8")
    assert "SCENARIO_PHASES='PLACEMENT WARMUP C4T6'" in active
    assert "SCENARIO_PHASE_PLACEMENT_CLIENTS=4" in active
    assert "SCENARIO_PHASE_PLACEMENT_THREADS=6" in active
    assert "SCENARIO_PHASE_PLACEMENT_VALUE=210" in active
    assert active.index("PLACEMENT WARMUP C4T6") < active.index(
        "SCENARIO_PHASE_WARMUP_VALUE=30"
    )


def test_cold_start_scenario_has_idle_phase_only_before_measurement(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "yba.env"
    config.write_text("", encoding="utf-8")
    runner = AffinityGraphRunner(tmp_path / "run", config, source, rounds=1, seed=7)
    value = runner._scenario("C4T4", 60, cold_start=True).read_text(encoding="utf-8")
    assert "SCENARIO_BUDGET_MODE=cold_start_duration" in value
    assert "SCENARIO_PHASES='server_idle C4T4'" in value
    assert "SCENARIO_PHASE_WARMUP" not in value
    assert "SCENARIO_PHASE_server_idle_VALUE=30" in value


def test_doris_controls_are_external_and_runtime_only_profile_is_empty(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "config/thread-profiles").mkdir(parents=True)
    config = tmp_path / "doris.env"
    config.write_text("DB_TYPE=doris\n", encoding="utf-8")
    runner = AffinityGraphRunner(tmp_path / "run", config, source, rounds=1, seed=7, system="doris")
    fallback = runner.doris_other_fallback_env()
    assert fallback["THREAD_CLUSTER_DEFAULT_CPUS"] == "0-63,96-127"
    assert "brpc_light" in fallback["THREAD_CLUSTER_RULES"]
    assert runner.runtime_only_profile().name == "runtime-only-empty.json"


def test_baseline_reference_is_validated_and_hashed(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "yba.env"
    config.write_text("", encoding="utf-8")
    experiment = tmp_path / "source-experiment"
    baseline_path = experiment / "runs/smoke-baseline-C4T6/result.json"
    baseline_path.parent.mkdir(parents=True)
    baseline_path.write_text(json.dumps({
        "treatment": "baseline", "load": "C4T6", "throughput": 100,
        "p99_latency_us": 10, "error_count": 0, "timeout_count": 0,
        "runtime_ms": 300_000,
    }), encoding="utf-8")
    runner = AffinityGraphRunner(
        tmp_path / "run", config, source, rounds=5, seed=7,
        smoke_baseline_result=baseline_path,
    )
    result = runner._baseline_reference()
    assert result["throughput"] == 100
    assert result["reference"]["source_experiment"] == "source-experiment"
    assert len(result["reference"]["sha256"]) == 64


def test_c2t2_smoke_load_controls_scenario_and_baseline_validation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "yba.env"
    config.write_text("", encoding="utf-8")
    experiment = tmp_path / "source-experiment"
    baseline_path = experiment / "runs/smoke-baseline-C2T2/result.json"
    baseline_path.parent.mkdir(parents=True)
    baseline_path.write_text(json.dumps({
        "treatment": "baseline", "load": "C2T2", "throughput": 100,
        "p99_latency_us": 10, "error_count": 0, "timeout_count": 0,
        "runtime_ms": 300_000,
    }), encoding="utf-8")
    runner = AffinityGraphRunner(
        tmp_path / "run", config, source, rounds=1, seed=7,
        smoke_baseline_result=baseline_path, smoke_load="C2T2",
    )
    scenario = runner._scenario("C2T2", 210).read_text(encoding="utf-8")
    assert "SCENARIO_PHASES='WARMUP C2T2'" in scenario
    assert runner._baseline_reference()["load"] == "C2T2"
    assert runner.state["smoke_load"] == "C2T2"


def test_doris_c4t4_uses_doris_calibration_and_supervised_be(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "doris.env"
    config.write_text("DB_TYPE=doris\n", encoding="utf-8")
    runner = AffinityGraphRunner(
        tmp_path / "run", config, source, rounds=1, seed=7,
        smoke_load="C4T4", system="doris",
    )
    runner.release = "/opt/affinitygraph"
    runner.host = type("Host", (), {
        "run": lambda *args, **kwargs: CommandResult("", 0, "", ""),
        "copy_to": lambda *args, **kwargs: None,
    })()
    scenario = runner._scenario("C4T4", 210).read_text(encoding="utf-8")
    runtime, _, wrapper = runner._runtime_config("doris-active", "active", tmp_path)
    runtime_value = runtime.read_text(encoding="utf-8")
    wrapper_value = (tmp_path / "doris-affinitygraph").read_text(encoding="utf-8")
    assert "SCENARIO_PHASE_C4T4_CLIENTS=4" in scenario
    assert "SCENARIO_PHASE_C4T4_THREADS=4" in scenario
    assert 'id = "doris-20260721-p95-v1"' in runtime_value
    assert 'solver = "numa-domain-v1"' in runtime_value
    assert 'affinity_granularity = "numa_node_mask"' in runtime_value
    assert "maximum_threads_per_domain = 1024" in runtime_value
    assert "start_be.sh --console" in wrapper_value
    assert wrapper.endswith("/doris-affinitygraph")


def test_soft_idle_gate_warns_but_cleanliness_remains_hard() -> None:
    class FakeHost:
        def __init__(self, cleanliness_ok: bool = True) -> None:
            self.calls = 0
            self.cleanliness_ok = cleanliness_ok

        def run(self, command: str, **_: object) -> CommandResult:
            self.calls += 1
            if self.calls == 1:
                return CommandResult(command, 0 if self.cleanliness_ok else 1, "", "").check()
            return CommandResult(command, 0, "affinitygraph_idle_busy_ratio=0.250000\n", "")

    runner = object.__new__(AffinityGraphRunner)
    runner.host = FakeHost()
    report = runner._idle_gate(soft=True)
    assert not report["passed"]
    assert report["enforcement"] == "warning"
    runner.host = FakeHost(cleanliness_ok=False)
    with pytest.raises(RuntimeError):
        runner._idle_gate(soft=True)


def test_kpi_parser_selects_sequence_two_and_ignores_stale_phase(tmp_path: Path) -> None:
    output = tmp_path / "yba"
    stale = output / "phases/0001-C4T6"
    measurement = output / "phases/0002-C4T6"
    stale.mkdir(parents=True)
    measurement.mkdir(parents=True)
    header = (
        "label,throughput,p99_latency,error_count,timeout_count,"
        "runtime_ms_max,client_logs\n"
    )
    (stale / "summary.csv").write_text(
        header + "C4T6,1,2,0,0,90,1\n", encoding="utf-8"
    )
    (measurement / "summary.csv").write_text(
        header + "C4T6,100,20,0,0,210,1\n", encoding="utf-8"
    )
    runner = object.__new__(AffinityGraphRunner)
    assert runner._parse_kpi(output, "C4T6")["throughput"] == 100


def test_kpi_parser_ignores_zero_duration_compatibility_alias(tmp_path: Path) -> None:
    output = tmp_path / "yba"
    alias = output / "phases/0002-C4T4"
    measurement = output / "phases/0003-C4T4"
    alias.mkdir(parents=True)
    measurement.mkdir(parents=True)
    header = (
        "label,throughput,p99_latency,error_count,timeout_count,"
        "runtime_ms_max,client_logs\n"
    )
    (alias / "summary.csv").write_text(
        header + "C4T4,0,0,0,0,0,0\n", encoding="utf-8"
    )
    (measurement / "summary.csv").write_text(
        header + "C4T4,594,60031,0,0,300017,4\n", encoding="utf-8"
    )
    runner = object.__new__(AffinityGraphRunner)
    parsed = runner._parse_kpi(output, "C4T4")
    assert parsed["throughput"] == 594
    assert parsed["runtime_ms"] == 300017


def test_runtime_health_uses_distinct_smoke_and_formal_action_gates(tmp_path: Path) -> None:
    log = tmp_path / "runtime.jsonl"
    events = [
        {"type": "bpf_health", "valid": True, "window_ready": True,
         "window_loss_ratio": 0.0},
        {"type": "thread_window", "confidence": 0.9},
        {"type": "plan"},
        {"type": "shadow_commit"},
        {"type": "action", "requested": 10, "committed": 8,
         "vanished": 0, "assignments": {"11": 3}},
    ]
    log.write_text("".join(json.dumps(row) + "\n" for row in events), encoding="utf-8")
    result = {
        "runtime_log": str(log), "restored": True,
        "runtime_status": {"action_requested": 10, "action_committed": 8},
        "measurement_start_status": {
            "effective_mode": "active", "active_effective": True,
            "policy_armed": True, "active_cohort_threads": 1,
            "pinned_threads": 1, "action_committed": 8,
            "planned_assignments": {"11": 3},
        },
        "measurement_start": {"masks": {"11": "3"}},
        "monitor": {"active_effective_seconds": 271,
                    "cohort_workload_seconds": 271,
                    "cohort_effective_seconds": 270,
                    "runtime_average_cpu_cores": 0.5, "runtime_peak_rss_kib": 1024,
                    "runtime_resource_samples": 10},
    }
    smoke = AffinityGraphRunner._runtime_health(
        result, require_plan=True, require_action=True, action_threshold=0.80
    )
    formal = AffinityGraphRunner._runtime_health(
        result, require_plan=True, require_action=True,
        action_threshold=0.95, minimum_active_seconds=270,
    )
    assert smoke["passed"]
    assert smoke["cohort_effective_coverage_ratio"] == 270 / 271
    assert not formal["passed"]

    rss_heavy = json.loads(json.dumps(result))
    rss_heavy["monitor"]["runtime_peak_rss_kib"] = 371_296
    strict_rss = AffinityGraphRunner._runtime_health(
        rss_heavy, require_plan=True, require_action=True,
        action_threshold=0.80,
    )
    formal_rss = AffinityGraphRunner._runtime_health(
        rss_heavy, require_plan=True, require_action=True,
        action_threshold=0.80, maximum_supervisor_rss_kib=512 * 1024,
    )
    assert not strict_rss["passed"]
    assert formal_rss["passed"]
    assert formal_rss["maximum_supervisor_rss_kib"] == 512 * 1024

    quiescent = json.loads(json.dumps(result))
    quiescent["measurement_start_status"].update({
        "active_cohort_threads": 0,
        "pinned_threads": 0,
        "planned_assignments": {},
        "planned_masks": {},
    })
    quiescent["measurement_start"]["masks"] = {}
    empty = AffinityGraphRunner._runtime_health(
        quiescent, require_plan=True, require_action=True,
    )
    assert not empty["measurement_start_ready"]
    assert not empty["passed"]


def test_incremental_smoke_analysis_reports_budget_latency_and_numa_moves(
    tmp_path: Path,
) -> None:
    plan_log = tmp_path / "plan.jsonl"
    active_dir = tmp_path / "active"
    active_dir.mkdir()
    active_log = active_dir / "runtime.jsonl"
    plan_log.write_text(json.dumps({"type": "plan"}) + "\n", encoding="utf-8")
    active_rows = [
        {"type": "bpf_health", "timestamp_ns": 1_000_000_000},
        {
            "type": "plan", "timestamp_ns": 3_000_000_000,
            "initial_plan_confirmed": True, "migration_budget": 2,
            "dirty_threads": 4, "candidate_threads": 3,
            "cooldown_skipped_threads": 1, "solve_duration_ns": 2_000_000,
            "actions": [
                {"tid": 11, "from_cpu": 0, "target_cpu": 2, "initial_pin": True},
                {"tid": 12, "from_cpu": 1, "target_cpu": 1, "initial_pin": True},
            ],
        },
        {"type": "action", "timestamp_ns": 3_100_000_000, "committed": 2},
        {"type": "action_commit", "timestamp_ns": 3_200_000_000, "generation": 1},
    ]
    active_log.write_text(
        "".join(json.dumps(row) + "\n" for row in active_rows), encoding="utf-8"
    )
    (active_dir / "runtime-monitor.jsonl").write_text(
        json.dumps({"monotonic": 10, "status": {"active_effective": False}}) + "\n"
        + json.dumps({"monotonic": 13, "status": {"active_effective": True}}) + "\n",
        encoding="utf-8",
    )
    baseline = {
        "throughput": 100, "p99_latency_us": 20,
        "error_count": 0, "timeout_count": 0,
    }
    active = {
        "runtime_log": str(active_log), "throughput": 105, "p99_latency_us": 18,
        "error_count": 0, "timeout_count": 0,
        "measurement_start": {"active_ready_wait_seconds": 4},
    }
    report = analyze_incremental_smoke(
        baseline, {"runtime_log": str(plan_log)}, active,
        {0: 0, 1: 0, 2: 1},
    )
    assert abs(report["performance"]["throughput_uplift"] - 0.05) < 1e-12
    assert report["initialization"]["initial_cross_node_actions"] == 1
    assert report["incremental_behavior"]["migration_budget_compliant"]
    assert report["solver_latency_ms"]["p95"] == 2


def test_active_ready_requires_all_singleton_masks() -> None:
    status = {
        "policy_armed": True,
        "active_effective": True,
        "active_cohort_threads": 2,
        "pinned_threads": 2,
        "planned_assignments": {"11": 3, "12": 4},
    }
    assert _active_ready(status, {"11": "3", "12": "4"})
    assert not _active_ready(status, {"11": "3", "12": "3-4"})
    assert not _active_ready({**status, "pinned_threads": 1}, {"11": "3", "12": "4"})
    assert not _active_ready({**status, "planned_assignments": {}}, {})


def test_active_ready_compares_normalized_numa_masks() -> None:
    status = {
        "policy_armed": True,
        "active_effective": True,
        "active_cohort_threads": 2,
        "pinned_threads": 2,
        "planned_masks": {"11": "0-3,6", "12": "0-3,6"},
    }
    assert _active_ready(status, {"11": "6,2-3,0-1", "12": "0-3,6"})
    assert not _active_ready(status, {"11": "0-3", "12": "0-3,6"})
    assert not _active_ready(
        {**status, "active_cohort_threads": 0, "pinned_threads": 0},
        {"11": "0-3,6", "12": "0-3,6"},
    )


def test_domain_oracle_is_validation_not_policy_selection() -> None:
    clickhouse = AffinityGraphRunner._domain_oracle(
        "clickhouse",
        [{"valid": True, "families": ["ThreadPool@lib+0x1"]}],
    )
    assert clickhouse["passed"]
    assert not AffinityGraphRunner._domain_oracle(
        "clickhouse",
        [{"valid": True, "families": ["ThreadPool@x", "TCPHandler@y"]}],
    )["passed"]
    assert AffinityGraphRunner._domain_oracle(
        "doris",
        [{"valid": True, "families": ["Pipe_normal [wo@x", "brpc_light@y"]}],
    )["passed"]


def test_active_ready_rejects_armed_quiescent_selector() -> None:
    status = {
        "policy_armed": True,
        "selector_ready": True,
        "active_effective": True,
        "active_cohort_threads": 0,
        "pinned_threads": 0,
        "planned_assignments": {},
    }
    assert not _active_ready(status, {})
    assert not _active_ready({**status, "policy_armed": False}, {})
    assert not _active_ready({**status, "pinned_threads": 1}, {})


def test_busy_smoke_flag_is_explicit() -> None:
    parser = build_parser()
    normal = parser.parse_args([
        "affinitygraph", "execute", "--root", "/tmp/run",
        "--base-config", "/tmp/yba.env",
    ])
    proof = parser.parse_args([
        "affinitygraph", "execute", "--root", "/tmp/run",
        "--base-config", "/tmp/yba.env", "--allow-busy-smoke",
    ])
    assert not normal.allow_busy_smoke
    assert proof.allow_busy_smoke


def test_smoke_approval_is_bound_to_fingerprint(tmp_path: Path) -> None:
    root = tmp_path / "run"
    (root / "summary").mkdir(parents=True)
    state = {"experiment_fingerprint": "abc"}
    smoke = {
        "passed": True, "completed": True, "safety_passed": True,
        "completed_with_warnings": True,
        "warnings": [{"gate": "host_busy"}], "fingerprint": "abc",
    }
    (root / "resume-state.json").write_text(json.dumps(state), encoding="utf-8")
    (root / "summary/smoke-result.json").write_text(json.dumps(smoke), encoding="utf-8")
    approval = approve_smoke(root, "reviewed")
    assert approval["fingerprint"] == "abc"
    assert approval["accepted_warning_count"] == 1
    saved = json.loads((root / "resume-state.json").read_text(encoding="utf-8"))
    assert saved["status"] == "approved"


def test_affinitygraph_cli_defaults_to_three_rounds() -> None:
    parser = build_parser()
    args = parser.parse_args([
        "affinitygraph", "execute", "--root", "/tmp/run",
        "--base-config", "/tmp/yba.env", "--allow-busy-smoke",
    ])
    assert args.rounds == 3
    assert args.allow_busy_smoke
    assert args.smoke_gates == "soft"
    assert args.system == "clickhouse"
    doris = parser.parse_args([
        "affinitygraph", "execute", "--root", "/tmp/doris-run",
        "--base-config", "/tmp/doris.env", "--system", "doris",
        "--smoke-load", "C4T4",
    ])
    assert doris.system == "doris"
    assert doris.smoke_load == "C4T4"
    reference = parser.parse_args([
        "affinitygraph", "execute", "--root", "/tmp/run",
        "--base-config", "/tmp/yba.env", "--smoke-gates", "strict",
        "--smoke-baseline-result", "/tmp/baseline.json",
    ])
    assert reference.smoke_gates == "strict"
    assert reference.smoke_baseline_result == Path("/tmp/baseline.json")
    approval = parser.parse_args([
        "affinitygraph", "approve-smoke", "--root", "/tmp/run", "--note", "ok",
    ])
    assert approval.note == "ok"
    diagnostic = parser.parse_args([
        "affinitygraph", "diagnose-observe", "--root", "/tmp/run",
        "--base-config", "/tmp/yba.env",
    ])
    assert diagnostic.seconds == 180
    assert diagnostic.system == "clickhouse"
    doris_diagnostic = parser.parse_args([
        "affinitygraph", "diagnose-observe", "--root", "/tmp/doris-diag",
        "--base-config", "/tmp/doris.env", "--system", "doris",
        "--load", "C4T4",
    ])
    assert doris_diagnostic.system == "doris"
    assert doris_diagnostic.load == "C4T4"
    positive = parser.parse_args([
        "affinitygraph", "positive-control", "--root", "/tmp/positive",
        "--base-config", "/tmp/doris.env", "--system", "doris",
    ])
    assert positive.system == "doris"
    assert positive.rounds == 2
    assert positive.seconds == 120
    assert positive.load is None
    profile = parser.parse_args([
        "affinitygraph", "validate-doris-thread-profile", "--root", "/tmp/profile",
        "--base-config", "/tmp/doris.env", "--profile", "/tmp/profile.json",
    ])
    assert profile.seconds == 120
    assert profile.rounds == 5
    assert profile.profile == Path("/tmp/profile.json")
    assert profile.loads == ("C2T2", "C4T4", "C5T16")
    assert profile.startup_timeout_seconds == 360
    assert not profile.steady_warmup
    controls = parser.parse_args([
        "affinitygraph", "validate-doris-controls", "--root", "/tmp/control",
        "--base-config", "/tmp/doris.env", "--control", "runtime_only",
    ])
    assert controls.control == "runtime_only"
    assert controls.rounds == 3
    recovery = parser.parse_args([
        "affinitygraph", "recover", "--root", "/tmp/old", "--note", "reviewed",
        "--diagnostic-root", "/tmp/diagnostic",
    ])
    assert recovery.diagnostic_root == Path("/tmp/diagnostic")
    formal = parser.parse_args([
        "affinitygraph", "formal", "--root", "/tmp/formal",
        "--base-config", "/tmp/doris.env",
    ])
    assert formal.design == "doris-random-v1"


def test_observe_diagnostic_reports_event_rates_and_ring_pressure(tmp_path: Path) -> None:
    log = tmp_path / "runtime.jsonl"
    rows = [
        {
            "type": "bpf_health", "timestamp_ns": 0, "valid": True,
            "emitted": 0, "dropped": 0, "window_ready": False,
            "emitted_by_kind": {"futex": 0, "task_rename": 0},
            "dropped_by_kind": {"futex": 0, "task_rename": 0},
            "suppressed_by_kind": {"task_rename": 0},
            "ring_capacity_bytes": 1024, "ring_max_occupancy_bytes": 0,
        },
        {
            "type": "bpf_health", "timestamp_ns": 30_000_000_000, "valid": True,
            "emitted": 300, "dropped": 0, "window_ready": True,
            "window_loss_ratio": 0,
            "emitted_by_kind": {"futex": 200, "task_rename": 100},
            "dropped_by_kind": {"futex": 0, "task_rename": 0},
            "suppressed_by_kind": {"task_rename": 50},
            "ring_capacity_bytes": 1024, "ring_max_occupancy_bytes": 512,
            "consumer_max_batch": 8, "consumer_max_drain_ns": 100,
            "consumer_max_lag_ns": 200,
        },
    ]
    log.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    report = AffinityGraphRunner._bpf_observe_diagnostic({
        "runtime_log": str(log), "restored": True,
        "monitor": {
            "runtime_average_cpu_cores": 0.5, "runtime_peak_rss_kib": 1024,
            "host_busy_ratio": 0.1, "host_maximum_load_1m": 2,
        },
    })
    assert report["passed"]
    assert report["ring_max_utilization"] == 0.5
    assert report["per_second_rates"][0]["futex_emitted_per_second"] == 200 / 30


def test_hook_reuses_validated_server_ready_identity(tmp_path: Path) -> None:
    class FakeHost:
        def __init__(self) -> None:
            self.commands: list[str] = []

        def run(self, command: str, *, check: bool = True) -> CommandResult:
            self.commands.append(command)
            return CommandResult(command, 0, "", "")

    host = FakeHost()
    identity = {"target_processes": [{"pid": 123, "start_time": 456}]}
    assert _target_identity(host, identity, tmp_path, "/opt/clickhouse") == (123, 456)
    assert json.loads((tmp_path / "target-identity.json").read_text()) == {
        "pid": 123, "start_time": 456,
    }
    assert len(host.commands) == 1
    assert "sudo" not in host.commands[0]


def test_hook_uses_cached_identity_when_phase_context_has_no_pid(tmp_path: Path) -> None:
    (tmp_path / "target-identity.json").write_text(
        json.dumps({"pid": 123, "start_time": 456}), encoding="utf-8"
    )

    class FakeHost:
        def run(self, command: str, *, check: bool = True) -> CommandResult:
            return CommandResult(command, 0, "", "")

    assert _target_identity(FakeHost(), {}, tmp_path, "/opt/clickhouse") == (123, 456)


def test_remote_hook_relay_returns_controller_response(tmp_path: Path, monkeypatch) -> None:
    context = tmp_path / "context.json"
    context.write_text(json.dumps({"phase": "WARMUP"}), encoding="utf-8")
    monkeypatch.setenv("AFFINITYGRAPH_HOOK_ROOT", str(tmp_path))

    def respond() -> None:
        request = tmp_path / "request-phase_before-WARMUP.json"
        while not request.is_file():
            time.sleep(0.01)
        (tmp_path / "response-phase_before-WARMUP.json").write_text(
            json.dumps({"returncode": 0, "stdout": "ok\n", "stderr": ""}),
            encoding="utf-8",
        )

    thread = threading.Thread(target=respond)
    thread.start()
    assert _relay("phase_before", context)["returncode"] == 0
    thread.join()
    request = json.loads((tmp_path / "request-phase_before-WARMUP.json").read_text())
    assert request["event"] == "phase_before"
