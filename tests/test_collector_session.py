from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from prism_sampler.collectors.session import CollectionSession, SessionContext


class SnapshotCommandTest(unittest.TestCase):
    def test_snapshot_uses_configured_privilege_prefix(self):
        config = Mock()
        config.target = {
            "host": "local",
            "sudo": "sudo -n",
            "remote_root": "/tmp/prism-sampler",
            "agent_command": "/opt/prism-sampler-agent",
        }
        config.sampling = {"profile": "relationship", "interval_seconds": 10}
        config.values = {
            "sampling_profiles": {
                "relationship": {"required": ["snapshot"], "optional": []}
            }
        }
        context = SessionContext(
            session_id="test",
            phase="load",
            round=1,
            pids=(123,),
            pid_start_times={},
            local_run_dir=Path("/tmp/prism-sampler-test"),
        )
        session = CollectionSession(config, context)

        with patch.object(session, "_start_process") as start:
            session._start_snapshot()

        command = start.call_args.args[1]
        self.assertTrue(command.startswith("sudo -n "))
        self.assertIn("/opt/prism-sampler-agent", command)
        self.assertIn("--pid 123", command)


if __name__ == "__main__":
    unittest.main()
