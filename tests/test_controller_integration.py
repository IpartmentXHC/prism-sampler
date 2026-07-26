from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from prism_sampler.config import load_config
from prism_sampler.controller.config import controller_config
from prism_sampler.controller.integration import mark_controller, start_controller


def config_file(root: Path) -> Path:
    path = root / "local.toml"
    path.write_text(f"""
[experiment]
output_root = "{root}/output"
system = "clickhouse"
[target]
host = "server"
remote_root = "/remote"
[collector]
binary = "/collector"
[yba]
root = "/yba"
[sampling]
profile = "minimal"
platform = "kunpeng-920"
[controller]
mode = "shadow"
agent_command = "/agent"
""")
    return path


def test_duplicate_start_reuses_running_controller() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        config = load_config(config_file(Path(temporary)))
        host = Mock()
        host.run.return_value.returncode = 0
        host.run.return_value.stdout = json.dumps({"status": "running"})
        with patch("prism_sampler.controller.integration.Host", return_value=host):
            result = start_controller(
                config,
                controller_config(config),
                session="run",
                system="clickhouse",
                pids=[10],
                start_times={10: 20},
            )
        assert result["status"] == "already_running"
        host.start.assert_not_called()
        host.copy_to.assert_not_called()


def test_mark_is_nonfatal_when_controller_is_absent() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        config = load_config(config_file(Path(temporary)))
        host = Mock()
        host.run.return_value.returncode = 0
        host.run.return_value.stdout = json.dumps({"status": "not_running"})
        with patch("prism_sampler.controller.integration.Host", return_value=host):
            result = mark_controller(
                config,
                controller_config(config),
                session="run",
                phase="low",
                active=True,
            )
        assert result["status"] == "not_running"
