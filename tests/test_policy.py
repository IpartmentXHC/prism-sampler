from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from prism_sampler.policies import generate_policies, validate_policy


class PolicyTest(unittest.TestCase):
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
            env = (experiment / "policy/yba-profile.env").read_text()
            self.assertIn("ENABLE_THREAD_CLUSTER=0", env)
            self.assertGreater(result["candidates"], 0)


if __name__ == "__main__":
    unittest.main()

