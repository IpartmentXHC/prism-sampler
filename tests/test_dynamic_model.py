from __future__ import annotations

import csv
import json

from prism_sampler.controller.dynamic_model import GScaleModel, build_dynamic_model


def test_build_dynamic_model_is_continuous_and_ignores_profile_fields(tmp_path) -> None:
    anchors = tmp_path / "anchors.csv"
    rows = [
        {
            "load": f"anchor-{index}",
            "pressure_ref": pressure,
            "one_throughput_ops_s": one,
            "two_throughput_ops_s": two,
            "one_p99_us": 10_000 * index,
            "two_p99_us": 9_000 * index,
        }
        for index, (pressure, one, two) in enumerate(
            [
                (0.1, 100, 80),
                (0.3, 200, 190),
                (0.5, 300, 330),
                (0.8, 400, 520),
                (1.2, 450, 700),
                (1.8, 460, 850),
            ],
            1,
        )
    ]
    with anchors.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    pressure = tmp_path / "pressure.json"
    pressure.write_text(json.dumps({
        "reference_capacity_cpus": 32,
        "coefficients": {"run_coefficient": 1.1, "rq_coefficient": 0.6},
        "validation": {"leave_one_load_out_p95_absolute_error": 0.4},
    }))
    output = tmp_path / "model.json"

    result = build_dynamic_model(anchors, pressure, output)
    model = GScaleModel.load(output)
    low = model.estimate(
        "one_node", 0.1, 100, 10_000, uncertainty_multiplier=0
    )
    nearby = model.estimate(
        "one_node", 0.11, 101, 10_100, uncertainty_multiplier=0
    )

    assert result["rows"] == 12
    assert model.raw["g_scale"]["online_profile_fields_used"] == []
    assert low.expected_gain_pct < 0
    assert abs(low.expected_gain_pct - nearby.expected_gain_pct) < 5
    assert model.pressure_ref("two_node", 16, 4) == 0.625
