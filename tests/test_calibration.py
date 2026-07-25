from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from prism_sampler.calibration import _suite_kpi, calibrate_experiment


def test_calibration_is_read_only_proposal() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        summary = root / "summary"
        summary.mkdir()
        scales = {
            "pair_activity_log_p95": 1, "pair_sync_log_p95": 1,
            "pair_sharing_log_p95": 1, "self_activity_log_p95": 1,
            "self_sync_log_p95": 1, "self_sharing_log_p95": 1,
        }
        (summary / "relation-scales.json").write_text(json.dumps(scales))
        pair_rows = []
        self_rows = []
        kpi_rows = []
        for round_number in range(1, 6):
            pair_rows.append({
                "profile": "one_node", "phase": "c2t2", "round": round_number,
                "group_a": "A", "group_b": "B", "activity_raw": 1,
                "sync_raw": 1, "sharing_raw": 1, "active_overlap_ratio": 1,
                "window_coverage": 1,
            })
            self_rows.append({
                "profile": "one_node", "phase": "c2t2", "round": round_number,
                "group_name": "A", "activity_raw": 1, "sync_raw": 1,
                "sharing_raw": 1, "window_coverage": 1,
            })
            kpi_rows.append({"profile": "one_node", "load": "c2t2", "round": round_number,
                             "status": "ok", "throughput": 100, "p99_latency": 10})
            kpi_rows.append({"profile": "self_compact_limited", "load": "c2t2",
                             "round": round_number, "status": "ok", "throughput": 110,
                             "p99_latency": 9})
        pd.DataFrame(pair_rows).to_csv(summary / "pair-features.csv", index=False)
        pd.DataFrame(self_rows).to_csv(summary / "self-features.csv", index=False)
        suite = root / "yba-suite"
        suite.mkdir()
        pd.DataFrame(kpi_rows).to_csv(suite / "suite-summary.csv", index=False)
        result = calibrate_experiment(root)
        assert result["gate_a_passed"]
        model = json.loads((summary / "g-model-proposal.json").read_text())
        assert model["expected_gain"] is None
        assert model["apply_allowed"] is False
        effects = pd.read_csv(summary / "paired-effects.csv")
        assert effects[effects.label_type == "g"].effect_percent.tolist() == pytest.approx(
            [10.0] * 5
        )


def test_suite_kpi_prefers_measurement_db() -> None:
    import duckdb

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        suite = root / "yba-suite"
        suite.mkdir()
        pd.DataFrame([{
            "profile": "one_node", "load": "c2t2", "round": 1,
            "status": "ok", "throughput": 50, "p99_latency": 20,
        }]).to_csv(suite / "suite-summary.csv", index=False)
        dataset = root / "runs/one_node/c2t2/r1/dataset"
        dataset.mkdir(parents=True)
        con = duckdb.connect(str(dataset / "telemetry.db3"))
        con.execute(
            "create table yba_phase_kpi(phase varchar, throughput_ops_s double, "
            "p99_latency_us double, error_count double, timeout_count double)"
        )
        con.execute("insert into yba_phase_kpi values ('measure_c2t2',100,10,0,0)")
        con.close()

        result = _suite_kpi(root)

        assert result.iloc[0].throughput == 100
        assert result.iloc[0].p99_latency == 10
