from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from prism_sampler.agent import snapshot
from prism_sampler.config import load_config, validate_config
from prism_sampler.hooks import _run_dir


class AgentConfigTest(unittest.TestCase):
    def test_snapshot_has_dual_clocks_and_pressure(self):
        value = snapshot([999_999_999])
        self.assertGreater(value["realtime_ns"], 0)
        self.assertGreater(value["monotonic_ns"], 0)
        self.assertIn("pressure_cpu", value)
        self.assertIn("cpu_frequency_khz", value)

    def test_loads_layered_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "local.toml"
            path.write_text(
                """
[experiment]
output_root = "/tmp/out"
system = "doris"
[target]
host = "local"
remote_root = "/tmp/remote"
[collector]
binary = "/tmp/collector"
[yba]
root = "/tmp/yba"
[sampling]
profile = "policy"
platform = "kunpeng-920"
"""
            )
            config = load_config(path)
            self.assertEqual(validate_config(config), [])
            self.assertEqual(config.values["platform"]["name"], "kunpeng-920")
            run = _run_dir(config, {"system": "clickhouse"}, "run", "load", 1)
            self.assertEqual(run, Path("/tmp/out/clickhouse/run/runs/load/r1"))


if __name__ == "__main__":
    unittest.main()
