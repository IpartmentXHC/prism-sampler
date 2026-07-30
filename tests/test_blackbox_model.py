from __future__ import annotations

import json

import duckdb
import pytest

from prism_sampler.controller.blackbox_model import (
    BlackboxGScaleModel,
    FEATURES,
    FORBIDDEN_FEATURE_TOKENS,
    LiveSystemFeatureSource,
    SCHEMA,
    extract_action_dataset,
    train_blackbox_model,
)
from prism_sampler.controller.config import ControllerConfig
from prism_sampler.controller.models import MetricSample
from prism_sampler.controller.policy import BlackboxBenefitPolicy


def _experiment(root, name: str, phase: str, gain: float):
    experiment = root / name
    controller = experiment / "controller"
    raw = experiment / "runs" / phase / "r1" / "raw"
    dataset = experiment / "runs" / phase / "r1" / "dataset"
    meta = experiment / "runs" / phase / "r1" / "meta"
    for path in (controller, raw, dataset, meta):
        path.mkdir(parents=True, exist_ok=True)
    action_ns = 100_000_000_000
    (meta / "phase.json").write_text(json.dumps({"phase": phase}))
    samples = []
    for seconds in (70, 80, 90):
        samples.append({
            "phase": phase,
            "realtime_ns": seconds * 1_000_000_000,
            "valid": True,
            "workload_active": True,
            "run_pressure": 0.5 + gain / 1000,
            "rq_pressure": 0.1,
            "node_cpu_utilization": {"0": 0.5, "1": 0.1, "2": 0.1, "3": 0.1},
            "psi_cpu": {"some_avg10": 0.2},
        })
    (controller / "samples.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in samples)
    )
    (controller / "actions.jsonl").write_text(json.dumps({
        "action": "scripted_expand",
        "status": "applied",
        "phase": phase,
        "from_state": "one_node",
        "to_state": "two_node",
        "realtime_ns": action_ns,
    }) + "\n")
    kpis = []
    for seconds, throughput in (
        (70, 100), (80, 100), (90, 100),
        (120, 100 + gain), (130, 100 + gain), (140, 100 + gain),
    ):
        kpis.append({
            "complete": True,
            "window_end_target_epoch_ns": seconds * 1_000_000_000,
            "throughput_ops_s": throughput,
            "operations_delta": throughput * 10,
        })
    (controller / "kpi.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in kpis)
    )
    (raw / "live-candidates.jsonl").write_text(json.dumps({
        "generated_at_epoch_ms": 90_000,
        "quality": {"confidence": 1.0},
        "pair_candidates": [{"relationship_score_r": 10}],
        "self_candidates": [{"self_score_r": 5}],
    }) + "\n")
    database = dataset / "telemetry.db3"
    con = duckdb.connect(str(database))
    con.execute("CREATE TABLE pmu_derived(ts TIMESTAMP, metric VARCHAR, value DOUBLE)")
    con.execute(
        "INSERT INTO pmu_derived VALUES "
        "(TIMESTAMP '1970-01-01 00:01:30', 'ipc', 1.2)"
    )
    con.execute(
        "CREATE TABLE numa_samples(ts TIMESTAMP, node INTEGER, metric VARCHAR, value DOUBLE, unit VARCHAR)"
    )
    con.execute(
        "INSERT INTO numa_samples VALUES "
        "(TIMESTAMP '1970-01-01 00:01:30', 0, 'mapped', 90, 'pages'),"
        "(TIMESTAMP '1970-01-01 00:01:30', 1, 'mapped', 10, 'pages')"
    )
    con.close()
    return experiment


def test_blackbox_features_exclude_application_kpis(tmp_path):
    assert not any(
        token in name.lower()
        for name in FEATURES
        for token in FORBIDDEN_FEATURE_TOKENS
    )
    experiment = _experiment(tmp_path, "e1", "low", 10)
    rows = extract_action_dataset([experiment])
    assert len(rows) == 1
    assert rows[0]["system_windows"] == 3
    assert rows[0]["label_gain_pct"] == pytest.approx(10)
    assert rows[0]["pmu_ipc"] == 1.2
    assert rows[0]["numa_local_page_ratio"] == 0.9
    assert rows[0]["r_pair_score_sum"] == 10


def test_blackbox_gate_rejects_single_direction_labels(tmp_path):
    experiments = [
        _experiment(tmp_path, f"e{index}", f"load-{index}", 5 + index)
        for index in range(3)
    ]
    report = train_blackbox_model(experiments, tmp_path / "output")
    assert report["dataset"]["coverage"] == 1
    assert not report["validation"]["label_direction_identifiable"]
    assert not report["validation"]["passed"]
    assert (tmp_path / "output" / "blackbox-action-contributions.csv").is_file()


def _online_model(gain: float = 5.0) -> BlackboxGScaleModel:
    direction = {
        "feature_names": ["run_pressure", "rq_pressure"],
        "impute": [0.5, 0.1],
        "center": [0.5, 0.1],
        "scale": [1.0, 1.0],
        "intercept": gain,
        "coefficients": [1.0, 1.0],
    }
    return BlackboxGScaleModel({
        "schema": SCHEMA,
        "model": {"directions": {"expand": direction, "shrink": direction}},
    })


def _sample(index: int, throughput: float) -> MetricSample:
    return MetricSample(
        realtime_ns=index * 10_000_000_000,
        monotonic_ns=index * 10_000_000_000,
        interval_seconds=10,
        workload_active=True,
        valid=True,
        run_cpu_equiv=16,
        rq_cpu_equiv=3.2,
        run_pressure=0.5,
        rq_pressure=0.1,
        tids_observed=10,
        kpi={"throughput_ops_s": throughput, "p99_latency_us": throughput},
        system_features={"run_pressure": 0.5, "rq_pressure": 0.1},
    )


def test_blackbox_policy_cannot_observe_application_kpi():
    config = ControllerConfig(
        decision_window_samples=3,
        cooldown_seconds=0,
        minimum_model_confidence=0.8,
        minimum_feature_coverage=0.8,
    )
    low_kpi = BlackboxBenefitPolicy(config, _online_model())
    high_kpi = BlackboxBenefitPolicy(config, _online_model())
    low = [low_kpi.evaluate(_sample(index, 1.0)) for index in range(1, 4)]
    high = [high_kpi.evaluate(_sample(index, 1_000_000.0)) for index in range(1, 4)]
    assert [item.to_dict() for item in low] == [item.to_dict() for item in high]
    assert low[-1].action == "expand"
    assert low[-1].model_confidence == pytest.approx(1.0)


def test_live_system_features_use_proc_without_kpi():
    source = LiveSystemFeatureSource()
    features = source.build(
        _sample(1, 999999),
        state="one_node",
        telemetry_dir=None,
        relationship_candidates=None,
    )
    assert features["run_pressure"] == pytest.approx(0.5)
    assert features["rq_pressure"] == pytest.approx(0.1)
    assert not any(
        token in key.lower()
        for key in features
        for token in FORBIDDEN_FEATURE_TOKENS
    )
