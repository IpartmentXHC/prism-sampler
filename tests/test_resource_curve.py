from __future__ import annotations

import csv
import json

import pytest

from prism_sampler.resource_curve import (
    _fit_curve,
    _retest_proposal,
    build_resource_curve,
    validate_resource_curve,
)

FIELDS = [
    "load",
    "clients",
    "threads_per_client",
    "offered_threads",
    "one_rounds",
    "two_rounds",
    "one_source_kind",
    "one_throughput_ops_s",
    "two_throughput_ops_s",
    "two_vs_one_gain_pct",
    "one_cpu_utilization",
    "two_cpu_utilization",
    "one_rq_per_cpu",
    "two_rq_per_cpu",
    "one_p99_us",
    "two_p99_us",
    "errors",
    "timeouts",
]


def _inputs(tmp_path, gains=(5.0, 20.0)):
    anchors = tmp_path / "anchors.csv"
    pressure_anchors = tmp_path / "pressure.csv"
    pressure_model = tmp_path / "pressure.json"
    rows = []
    pressure_rows = []
    for index, gain in enumerate(gains, 1):
        one = 100.0
        two = one * (1.0 + gain / 100.0)
        rows.append(
            {
                "load": f"L{index}",
                "clients": index,
                "threads_per_client": 1,
                "offered_threads": index,
                "one_rounds": 3,
                "two_rounds": 3,
                "one_source_kind": "pressure_v2_fixed_config",
                "one_throughput_ops_s": one,
                "two_throughput_ops_s": two,
                "two_vs_one_gain_pct": gain,
                "one_cpu_utilization": 0.1 * index,
                "two_cpu_utilization": 0.05 * index,
                "one_rq_per_cpu": 0,
                "two_rq_per_cpu": 0,
                "one_p99_us": 100,
                "two_p99_us": 100,
                "errors": 0,
                "timeouts": 0,
            }
        )
        pressure_rows.append({"load": f"L{index}", "pressure_ref": 0.1 * index})
    with anchors.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    with pressure_anchors.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["load", "pressure_ref"])
        writer.writeheader()
        writer.writerows(pressure_rows)
    pressure_model.write_text(
        json.dumps(
            {
                "schema": "project-numa-thread.pressure-reference.v1",
                "reference_capacity_cpus": 32,
                "coefficients": {"run_coefficient": 1.1, "rq_coefficient": 0.6},
                "validation": {
                    "leave_one_load_out_mae_pressure_units": 0.05,
                    "leave_one_load_out_p95_absolute_error": 0.10,
                },
            }
        )
    )
    return anchors, pressure_anchors, pressure_model


def test_resource_curve_chooses_minimum_resource_within_ninety_percent(tmp_path):
    anchors, pressure_anchors, pressure_model = _inputs(tmp_path)
    output = tmp_path / "output"

    bundle = build_resource_curve(anchors, pressure_anchors, pressure_model, output)

    assert bundle["status"] == "active_eligible"
    assert [row["selected_state"] for row in bundle["curve"]] == [
        "one_node",
        "two_node",
    ]
    assert bundle["objective"]["expand_required_advantage_pct"] == pytest.approx(
        11.1111111111
    )
    assert validate_resource_curve(output / "calibration-bundle.json")["passed"]
    assert (output / "clickhouse-kunpeng-920-policy.toml").is_file()
    assert (output / "retest-proposal.csv").is_file()


def test_curve_residual_is_audited_instead_of_silently_deleted():
    anchors = []
    for index, gain in enumerate((0.0, 10.0, 80.0, 20.0, 30.0)):
        anchors.append(
            {
                "load": f"L{index}",
                "pressure_ref": index / 10,
                "two_advantage_pct": gain,
                "one_rounds": 3,
                "two_rounds": 3,
                "quality_reasons": [],
                "one_source_kind": "pressure_v2_fixed_config",
            }
        )

    curve, excluded = _fit_curve(anchors)

    assert len(curve) < len(anchors)
    assert excluded
    assert all(row["stage"] == "curve_residual" for row in excluded)
    assert all(
        row["reason"] == "requires_two_matched_retest_rounds" for row in excluded
    )
    proposal = _retest_proposal(anchors, excluded)
    suspect = next(row for row in proposal if row["load"] == excluded[0]["load"])
    assert suspect["additional_one_rounds"] == 2
    assert suspect["additional_two_rounds"] == 2
    assert suspect["status"] == "curve_residual_retest_required"


def test_three_anchors_are_not_enough_to_declare_a_curve_outlier():
    anchors = []
    for index, gain in enumerate((5.0, 47.0, 75.0)):
        anchors.append(
            {
                "load": f"L{index}",
                "pressure_ref": index / 10,
                "two_advantage_pct": gain,
                "one_rounds": 3,
                "two_rounds": 3,
                "quality_reasons": [],
                "one_source_kind": "pressure_v2_fixed_config",
            }
        )

    curve, excluded = _fit_curve(anchors)

    assert len(curve) == 3
    assert excluded == []


def test_legacy_or_under_repeated_anchors_keep_bundle_candidate_only(tmp_path):
    anchors, pressure_anchors, pressure_model = _inputs(tmp_path)
    rows = list(csv.DictReader(anchors.open()))
    rows[0]["one_source_kind"] = "legacy_one_node_bridge_transfer"
    rows[1]["two_rounds"] = "2"
    with anchors.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    bundle = build_resource_curve(
        anchors, pressure_anchors, pressure_model, tmp_path / "output"
    )

    assert bundle["status"] == "candidate_only"
    assert bundle["validation"]["anchors_in_curve"] == 0
    assert bundle["validation"]["additional_one_rounds"] == 3
    assert bundle["validation"]["additional_two_rounds"] == 1
