from __future__ import annotations

import tempfile
import unittest
from datetime import timezone
from pathlib import Path
from unittest.mock import patch

from prism_sampler.artifacts import _duckdb_timestamp as artifact_timestamp
from prism_sampler.collectors.session import measure_clock_offset
from prism_sampler.orchestration.runner import (
    _postprocess_experiment,
    _target_workload_bounds,
    _timeline,
)
from prism_sampler.remote import Host
from prism_sampler.sidecars import _duckdb_timestamp as sidecar_timestamp


class TimeContractTest(unittest.TestCase):
    def test_timeline_is_converted_from_client_to_target_clock(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scenario-timeline.csv"
            path.write_text(
                "phase,started_epoch_ns,finished_epoch_ns\nload,1000,9000\n",
                encoding="utf-8",
            )
            client = _timeline(path)["load"]
            target = _target_workload_bounds(client, {"target_clock_offset_ns": 250})
            self.assertEqual(target["client_workload_start_epoch_ns"], 1000)
            self.assertEqual(target["workload_start_epoch_ns"], 1250)
            self.assertEqual(target["workload_end_epoch_ns"], 9250)
            self.assertEqual(target["workload_clock"], "target_realtime")

    def test_target_bounds_require_a_measured_offset(self):
        with self.assertRaisesRegex(ValueError, "clock offset"):
            _target_workload_bounds(
                {"client_workload_start_epoch_ns": 1, "client_workload_end_epoch_ns": 2},
                {},
            )

    def test_duckdb_timestamps_are_naive_utc(self):
        for convert in (artifact_timestamp, sidecar_timestamp):
            value = convert(0)
            self.assertIsNone(value.tzinfo)
            self.assertEqual(value.replace(tzinfo=timezone.utc).timestamp(), 0)

    def test_local_clock_has_zero_offset(self):
        result = measure_clock_offset(Host("local"))
        self.assertEqual(result["target_clock_offset_ns"], 0)
        self.assertEqual(result["target_clock_uncertainty_ns"], 0)

    def test_postprocess_skips_policy_when_no_relationship_exists(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "prism_sampler.relations.analyze_experiment",
                return_value={"runs": 2, "errors": 0, "candidates": 0},
            ):
                result = _postprocess_experiment(root, 2, yba_returncode=0)
            self.assertEqual(result["status"], "complete")
            self.assertEqual(result["policy"]["status"], "skipped")
            self.assertTrue((root / "summary/postprocess.json").is_file())


if __name__ == "__main__":
    unittest.main()
