from __future__ import annotations

import json

from prism_sampler.blackbox_validation import validate_shadow


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


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
            "system_features": {"run_pressure": 0.5, "r_pair_score_max": 10},
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
