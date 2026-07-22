from __future__ import annotations

import unittest

from prism_sampler.sidecars import PerfSample, derived_pmu_metrics, select_core_events


class PmuEventTest(unittest.TestCase):
    def test_selects_kernel_cpu_cycles_alias(self):
        selected = select_core_events(
            ["cpu-cycles", "instructions"],
            ["cpu-cycles", "instructions", "remote_access"],
        )
        self.assertEqual(selected, ["cpu-cycles", "instructions"])

    def test_derives_ipc_from_normalized_cpu_cycles(self):
        samples = [
            PerfSample(10.0, "process", "software-or-core", "cpu_cycles", 200.0, "", 1.0, None, None),
            PerfSample(10.0, "process", "software-or-core", "instructions", 300.0, "", 1.0, None, None),
            PerfSample(10.0, "process", "software-or-core", "stall_backend", 50.0, "", 1.0, None, None),
        ]
        metrics = {
            row["metric"]: row["value"] for row in derived_pmu_metrics(samples, 10.0)
        }
        self.assertEqual(metrics["ipc"], 1.5)
        self.assertEqual(metrics["backend_stall_per_cycle"], 0.25)


if __name__ == "__main__":
    unittest.main()
