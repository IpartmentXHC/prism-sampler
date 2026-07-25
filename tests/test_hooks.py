from __future__ import annotations

import json
import tempfile
from pathlib import Path

from prism_sampler.hooks import handle


def test_hook_skips_non_measurement_phase() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        config = root / "local.toml"
        config.write_text(
            """
[experiment]
output_root = "{output}"
system = "clickhouse"
[target]
host = "local"
remote_root = "/tmp/prism-sampler-test"
[collector]
binary = "/nonexistent"
[yba]
root = "/tmp"
[sampling]
profile = "minimal"
platform = "kunpeng-920"
collect_phase_patterns = ["^measure_"]
""".format(output=root / "output"),
            encoding="utf-8",
        )
        context = root / "context.json"
        context.write_text(json.dumps({
            "run_id": "test", "phase": "warmup_c5t16", "round": 1,
            "system": "clickhouse",
        }))
        before = handle("phase_before", context, config)
        after = handle("phase_after", context, config)
        assert before["status"] == "skipped"
        assert after["status"] == "skipped"
        assert not (root / "output").exists()
