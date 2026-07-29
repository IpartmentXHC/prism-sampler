from __future__ import annotations

import hashlib
import tempfile
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


class LiveRelationshipCommandTest(unittest.TestCase):
    def make_session(self) -> CollectionSession:
        config = Mock()
        config.target = {
            "host": "local",
            "sudo": "sudo -n",
            "remote_root": "/tmp/prism-sampler",
            "live_analyzer_command": "/opt/prism-live-analyzer",
        }
        config.collector = {
            "binary": "/opt/metric-collector",
            "attach_wait_seconds": 0,
            "startup_attempts": 1,
        }
        config.sampling = {"profile": "online-relationship"}
        config.relations = {
            "live_interval_ms": 500,
            "live_queue_capacity": 32,
            "window_seconds": 60,
            "emit_seconds": 10,
            "minimum_evidence_windows": 3,
            "group_rules": [{"name": "Worker", "pattern": "^worker"}],
        }
        config.values = {
            "sampling_profiles": {
                "online-relationship": {
                    "required": ["prism", "live-relations"],
                    "optional": [],
                }
            }
        }
        return CollectionSession(
            config,
            SessionContext(
                session_id="test",
                phase="load",
                round=1,
                pids=(123,),
                pid_start_times={},
                local_run_dir=Path("/tmp/prism-sampler-test"),
            ),
        )

    def test_prism_enables_live_socket_only_for_live_profile(self):
        session = self.make_session()
        session.host.run = Mock(
            return_value=Mock(
                stdout="--live-socket --platform-profile --subsystems"
            )
        )

        with (
            patch.object(session, "_start_process") as start,
            patch.object(session, "_assert_process_alive"),
            patch.object(session, "_validate_pids"),
        ):
            session._start_prism()

        command = start.call_args.args[1]
        digest = hashlib.sha256(
            "/tmp/prism-sampler/test/load/r1".encode("utf-8")
        ).hexdigest()[:20]
        self.assertIn(f"--live-socket /tmp/prism-live-{digest}.sock", command)
        self.assertIn("--live-interval-ms 500", command)
        self.assertIn("--live-queue-capacity 32", command)

    def test_live_socket_path_is_bounded_for_long_session_names(self):
        session = self.make_session()
        session.remote_dir = "/home/xhc/prism-sampler/data/" + "x" * 300

        socket_path = session._live_socket()

        self.assertLess(len(socket_path.encode("utf-8")), 108)
        self.assertTrue(socket_path.startswith("/tmp/prism-live-"))

    def test_live_analyzer_is_shadow_only_and_receives_group_rules(self):
        session = self.make_session()

        with patch.object(session, "_start_process") as start:
            session._start_live_relations()

        plugin, command = start.call_args.args
        self.assertEqual(plugin, "live-relations")
        self.assertIn("/opt/prism-live-analyzer", command)
        self.assertIn("--pid 123", command)
        self.assertIn("--group-rule 'Worker=^worker'", command)
        self.assertNotIn("taskset", command)


class PrismStartupTest(unittest.TestCase):
    def make_session(self) -> CollectionSession:
        config = Mock()
        config.target = {
            "host": "local",
            "sudo": "sudo -n",
            "remote_root": "/tmp/prism-sampler",
        }
        config.collector = {
            "binary": "/opt/metric-collector",
            "runtime_lib": "/opt/lib",
            "attach_wait_seconds": 0,
            "startup_attempts": 3,
        }
        config.sampling = {"profile": "relationship"}
        config.values = {
            "sampling_profiles": {
                "relationship": {"required": ["prism"], "optional": []}
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
        return CollectionSession(config, context)

    def test_prism_retries_when_process_dies_during_attach(self):
        session = self.make_session()
        session.host.run = Mock(return_value=Mock(stdout="legacy help"))

        def mark_started(*_args):
            session.status["prism"].started = True
            session.status["prism"].healthy = True

        with (
            patch.object(session, "_start_process", side_effect=mark_started) as start,
            patch.object(
                session, "_assert_process_alive",
                side_effect=[RuntimeError("malloc failure"), None],
            ) as alive,
            patch.object(session, "_prepare_prism_retry") as retry,
            patch.object(session, "_validate_pids"),
        ):
            session._start_prism()

        self.assertEqual(start.call_count, 2)
        self.assertEqual(alive.call_count, 2)
        retry.assert_called_once_with(1)
        self.assertTrue(session.status["prism"].healthy)

    def test_prism_reports_failure_after_configured_attempts(self):
        session = self.make_session()
        session.config.collector["startup_attempts"] = 2
        session.host.run = Mock(return_value=Mock(stdout="legacy help"))
        with (
            patch.object(session, "_start_process"),
            patch.object(
                session, "_assert_process_alive", side_effect=RuntimeError("crashed")
            ),
            patch.object(session, "_prepare_prism_retry") as retry,
        ):
            with self.assertRaisesRegex(RuntimeError, "failed after 2 startup attempts"):
                session._start_prism()

        self.assertEqual(retry.call_count, 2)

    def test_prism_uses_configured_collector_subsystems(self):
        session = self.make_session()
        session.config.collector.update({
            "subsystems": ["taskstats"],
            "required_subsystems": ["taskstats"],
            "best_effort": True,
        })
        session.host.run = Mock(
            return_value=Mock(stdout="--platform-profile --subsystems")
        )

        with (
            patch.object(session, "_start_process") as start,
            patch.object(session, "_assert_process_alive"),
            patch.object(session, "_validate_pids"),
        ):
            session._start_prism()

        command = start.call_args.args[1]
        self.assertIn("--subsystems taskstats", command)
        self.assertIn("--required-subsystems taskstats", command)
        self.assertIn("--best-effort", command)
        self.assertNotIn("taskstats,vfs", command)

    def test_prism_rejects_required_subsystem_not_requested(self):
        session = self.make_session()
        session.config.collector.update({
            "subsystems": ["taskstats"],
            "required_subsystems": ["taskstats", "vfs"],
        })
        session.host.run = Mock(
            return_value=Mock(stdout="--platform-profile --subsystems")
        )

        with self.assertRaisesRegex(ValueError, "must be included"):
            session._start_prism()

    def test_remote_phase_directory_is_removed_with_privilege_then_created_as_user(self):
        session = self.make_session()
        session.host.run = Mock(return_value=Mock())

        session._reset_remote_dir()

        self.assertEqual(session.host.run.call_count, 3)
        remove = session.host.run.call_args_list[0].args[0]
        remove_socket = session.host.run.call_args_list[1].args[0]
        create = session.host.run.call_args_list[2].args[0]
        self.assertEqual(remove, "sudo -n rm -rf /tmp/prism-sampler/test/load/r1")
        self.assertEqual(remove_socket, f"sudo -n rm -f {session._live_socket()}")
        self.assertEqual(create, "mkdir -p /tmp/prism-sampler/test/load/r1")

    def test_copy_replaces_stale_local_raw_directory(self):
        session = self.make_session()
        session.requested = []
        session.status = {}
        session.host.run = Mock(return_value=Mock(returncode=0))

        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            stale = run_dir / "raw" / "collector.db3.wal"
            stale.parent.mkdir(parents=True)
            stale.write_text("stale")
            session.context = SessionContext(
                session.context.session_id,
                session.context.phase,
                session.context.round,
                session.context.pids,
                session.context.pid_start_times,
                run_dir,
            )

            def copy_from(_remote, local, *, recursive=False):
                self.assertTrue(recursive)
                self.assertFalse((local / "collector.db3.wal").exists())
                nested = local / "r1"
                nested.mkdir()
                (nested / "collector.db3").write_text("current")

            session.host.copy_from = Mock(side_effect=copy_from)
            session.stop(copy=True)

            self.assertFalse(stale.exists())
            self.assertEqual((run_dir / "raw" / "collector.db3").read_text(), "current")


if __name__ == "__main__":
    unittest.main()
