from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

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


def test_phase_after_without_prism_does_not_require_db3() -> None:
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
profile = "placement-validation"
platform = "kunpeng-920"
[controller]
use_kpi_online = false
use_workload_activity_marker = false
""".format(output=root / "output"),
            encoding="utf-8",
        )
        context = root / "context.json"
        context.write_text(json.dumps({
            "run_id": "test", "phase": "C2T2", "round": 1,
            "system": "clickhouse",
            "target_processes": [{"pid": 123, "start_time": 456}],
        }))
        running = {
            "profile": "placement-validation",
            "plugins": [{"name": "phase-marker", "started": True}],
        }
        stopped = {**running, "state": "stopped"}

        with (
            patch("prism_sampler.hooks.CollectionSession") as session_class,
            patch("prism_sampler.hooks.validate_raw") as validate,
        ):
            session = session_class.return_value
            session.requested = ["phase-marker"]
            session.start.return_value = running
            session.stop.return_value = stopped
            before = handle("phase_before", context, config)
            after = handle("phase_after", context, config)

        assert before["status"] == "ready"
        assert after["status"] == "complete"
        assert after["health"]["status"] == "not_applicable"
        assert after["health"]["collection"] == stopped
        validate.assert_not_called()
