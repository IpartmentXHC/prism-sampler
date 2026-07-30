from __future__ import annotations

import json
from unittest.mock import patch

from prism_sampler.blackbox_validation import execute_stage_d, validate_shadow


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _shadow_experiment(root, name: str, action: str):
    experiment = root / name
    samples = []
    decisions = []
    for index in range(15):
        stamp = (index + 1) * 10_000_000_000
        samples.append({
            "realtime_ns": stamp,
            "workload_active": True,
            "valid": True,
            "kpi": {},
            "clickhouse_metrics": {},
            "system_features": {
                "run_pressure": 0.5,
                "r_pair_score_max": 10,
                "unused_node_utilization": 0.01,
            },
        })
        decisions.append({
            "realtime_ns": stamp,
            "decision_source": "blackbox_system_g_scale",
            "expected_gain_pct": 5,
            "model_confidence": 0.9,
            "feature_coverage": 1.0,
            "reason": "system_gain_confirming" if index < 2 else "predicted_gain_below_gate",
            "action": action if index == 2 else None,
        })
    decisions[2]["reason"] = "blackbox_predicted_gain"
    _write_jsonl(experiment / "controller" / "samples.jsonl", samples)
    _write_jsonl(experiment / "controller" / "decisions.jsonl", decisions)
    _write_jsonl(experiment / "controller" / "actions.jsonl", [{
        "realtime_ns": decisions[2]["realtime_ns"] + 1,
        "status": "shadow",
        "action": action,
    }])
    (experiment / "controller" / "resolved-config.json").write_text(json.dumps({
        "controller": {"use_workload_activity_marker": False},
        "online_input_mode": "system_blackbox",
    }))
    return experiment


def test_shadow_gate_requires_only_system_features(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    report = validate_shadow(experiments, tmp_path / "report.json")
    assert report["passed"]
    assert report["model_decision_windows"] == 30
    assert report["online_kpi_rows"] == 0
    assert report["directions"] == ["expand", "shrink"]


def test_shadow_gate_rejects_online_kpi(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    sample_path = experiments[0] / "controller" / "samples.jsonl"
    rows = [json.loads(line) for line in sample_path.read_text().splitlines()]
    rows[0]["kpi"] = {"throughput_ops_s": 123}
    _write_jsonl(sample_path, rows)
    report = validate_shadow(experiments, tmp_path / "report.json")
    assert not report["passed"]
    assert report["online_kpi_rows"] == 1


def test_shadow_gate_rejects_low_confidence_recommendation(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    path = experiments[0] / "controller" / "decisions.jsonl"
    rows = _read_jsonl(path)
    rows[2]["model_confidence"] = 0.79
    _write_jsonl(path, rows)

    report = validate_shadow(experiments, tmp_path / "report.json")

    assert not report["passed"]
    assert report["minimum_recommendation_confidence"] == 0.79


def test_shadow_gate_reports_missing_model_recommendations(tmp_path):
    experiment = _shadow_experiment(tmp_path, "one", "expand")
    decisions = _read_jsonl(experiment / "controller" / "decisions.jsonl")
    for row in decisions:
        row["action"] = None
    _write_jsonl(experiment / "controller" / "decisions.jsonl", decisions)
    _write_jsonl(experiment / "controller" / "actions.jsonl", [])

    report = validate_shadow([experiment], tmp_path / "report.json")

    assert not report["passed"]
    assert report["model_recommendations"] == 0
    assert report["minimum_recommendation_confidence"] is None
    assert report["minimum_recommendation_feature_coverage"] is None


def test_shadow_gate_rejects_contaminated_calibration_host(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    path = experiments[0] / "controller" / "samples.jsonl"
    rows = _read_jsonl(path)
    for row in rows:
        row["system_features"]["unused_node_utilization"] = 0.25
    _write_jsonl(path, rows)

    report = validate_shadow(experiments, tmp_path / "report.json")

    assert not report["passed"]
    assert not report["calibration_environment"]["isolated"]


def test_shadow_gate_rejects_missing_three_window_confirmation(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    path = experiments[0] / "controller" / "decisions.jsonl"
    rows = _read_jsonl(path)
    rows[1]["reason"] = "predicted_gain_below_gate"
    _write_jsonl(path, rows)

    report = validate_shadow(experiments, tmp_path / "report.json")

    assert not report["passed"]
    assert report["confirmation_failures"] == 1


def test_shadow_gate_rejects_repeated_action_within_minute(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    action_path = experiments[0] / "controller" / "actions.jsonl"
    actions = _read_jsonl(action_path)
    actions.append({
        "realtime_ns": actions[0]["realtime_ns"] + 30_000_000_000,
        "status": "shadow",
        "action": "expand",
    })
    _write_jsonl(action_path, actions)

    report = validate_shadow(experiments, tmp_path / "report.json")

    assert not report["passed"]
    assert report["repeated_within_minute"] == 1


def test_shadow_gate_rejects_unsafe_capacity_recommendation(tmp_path):
    experiments = [
        _shadow_experiment(tmp_path, "one", "expand"),
        _shadow_experiment(tmp_path, "two", "shrink"),
    ]
    _write_jsonl(
        experiments[0] / "controller" / "fine-placement.jsonl",
        [{"apply_allowed": True, "capacity_feasible": False}],
    )

    report = validate_shadow(experiments, tmp_path / "report.json")

    assert not report["passed"]
    assert report["unsafe_capacity_recommendations"] == 1


def test_stage_d_does_not_run_active_when_shadow_gate_fails(tmp_path):
    model = tmp_path / "model.json"
    stage_c = tmp_path / "stage-c.json"
    model.write_text(json.dumps({"validation": {"passed": True}}))
    stage_c.write_text(json.dumps({"passed": True}))
    placeholders = {
        name: tmp_path / name
        for name in ("selected", "base", "scenario", "manifest", "anchors")
    }
    for path in placeholders.values():
        path.touch()
    shadow_paths = [tmp_path / "shadow-one", tmp_path / "shadow-two"]

    with (
        patch("prism_sampler.blackbox_validation._run", side_effect=shadow_paths) as run,
        patch(
            "prism_sampler.blackbox_validation.validate_shadow",
            return_value={"passed": False},
        ),
    ):
        result = execute_stage_d(
            tmp_path / "root",
            placeholders["selected"],
            model,
            stage_c,
            placeholders["base"],
            placeholders["scenario"],
            placeholders["manifest"],
            placeholders["anchors"],
            deploy=False,
        )

    assert run.call_count == 2
    assert result["active"]["status"] == "blocked"
    assert result["active_experiment"] is None
    assert not result["passed"]
