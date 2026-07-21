from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

from prism_sampler.policies import generate_policies, validate_policy
from prism_sampler.policies.generator import _node_pressure


class PolicyTest(unittest.TestCase):
    def test_node_pressure_uses_only_workload_interval(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary) / "runs/load/r1"
            (run / "dataset").mkdir(parents=True)
            (run / "meta").mkdir()
            start = datetime(2026, 1, 1)
            con = duckdb.connect(str(run / "dataset/telemetry.db3"))
            con.execute("CREATE TABLE system_pressure_samples(ts TIMESTAMP, metric VARCHAR, value VARCHAR)")
            rows = [
                (start, "cpu0 10 0 0 90\ncpu1 10 0 0 90"),
                (start + timedelta(seconds=10), "cpu0 100 0 0 100\ncpu1 100 0 0 100"),
                (start + timedelta(seconds=20), "cpu0 110 0 0 190\ncpu1 110 0 0 190"),
                (start + timedelta(seconds=30), "cpu0 300 0 0 200\ncpu1 300 0 0 200"),
            ]
            con.executemany("INSERT INTO system_pressure_samples VALUES (?, 'proc_stat', ?)", rows)
            con.close()
            epoch = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
            (run / "meta/phase.json").write_text(json.dumps({
                "workload_clock": "target_realtime",
                "workload_start_epoch_ns": int((epoch + 10) * 1e9),
                "workload_end_epoch_ns": int((epoch + 21) * 1e9),
            }))
            pressure = _node_pressure(Path(temporary), {0: "0", 1: "1"})
            self.assertAlmostEqual(pressure[0], 0.1)
            self.assertAlmostEqual(pressure[1], 0.1)

    def test_generates_candidate_only_capacity_checked_policy(self):
        with tempfile.TemporaryDirectory() as temporary:
            experiment = Path(temporary)
            summary = experiment / "summary"
            raw = experiment / "runs/load/r1/raw"
            summary.mkdir(parents=True)
            raw.mkdir(parents=True)
            rows = []
            for phase, score in (("low", 70.0), ("high", 90.0)):
                rows.append({
                    "phase": phase, "group_a": "a", "group_b": "b",
                    "relationship_score_r": score, "active_cpus_a": 2,
                    "active_cpus_b": 3, "runqueue_cpus_a": 0.5,
                    "runqueue_cpus_b": 0.5, "activity": .8,
                    "synchronization": .9, "sharing": .2, "stability": .9,
                })
            pd.DataFrame(rows).to_csv(summary / "relation-candidates.csv", index=False)
            (raw / "capabilities.json").write_text(json.dumps({
                "platform": {"numa_cpu_lists": {"0": "0-7", "1": "8-15"}}
            }))
            result = generate_policies(experiment, top_k=2)
            selected = json.loads((experiment / "policy/selected-policy.json").read_text())
            validate_policy(selected)
            self.assertFalse(selected["apply_allowed"])
            self.assertIsNone(selected["expected_gain"])
            self.assertLessEqual(selected["cpu_demand"], selected["capacity_limit"])
            self.assertEqual(selected["groups"], ["a", "b"])
            env = (experiment / "policy/yba-profile.env").read_text()
            self.assertIn("ENABLE_THREAD_CLUSTER=0", env)
            self.assertGreater(result["candidates"], 0)


if __name__ == "__main__":
    unittest.main()
