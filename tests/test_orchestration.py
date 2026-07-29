from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from prism_sampler.orchestration.runner import _collect_suite_runs


class SuiteCollectionResumeTest(unittest.TestCase):
    def test_existing_local_run_does_not_require_remote_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            experiment = root / "experiment"
            suite = experiment / "yba-suite"
            cell = suite / "runs" / "0001-two_node-c5t16-r1"
            (cell / "meta").mkdir(parents=True)
            (cell / ".complete").touch()
            (cell / "meta" / "suite-run.env").write_text(
                "profile=two_node\nload=c5t16\nround=1\n",
                encoding="utf-8",
            )
            destination = experiment / "runs" / "two_node" / "c5t16" / "r1"
            (destination / "dataset").mkdir(parents=True)
            (destination / "dataset" / "telemetry.db3").touch()
            config = Mock()
            config.section.side_effect = lambda name: {
                "client": {"host": "ubuntu197", "output_root": "/remote/output"},
                "experiment": {"system": "clickhouse"},
            }[name]

            with patch("prism_sampler.orchestration.runner.Host") as host:
                finalized = _collect_suite_runs(config, experiment, suite)

            self.assertEqual(finalized, 0)
            host.return_value.run.assert_not_called()
            host.return_value.copy_from.assert_not_called()

    def test_existing_multiphase_lifecycle_does_not_require_remote_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            experiment = root / "experiment"
            suite = experiment / "yba-suite"
            cell = suite / "runs" / "0001-profile-screen-r1"
            (cell / "meta").mkdir(parents=True)
            (cell / ".complete").touch()
            (cell / "meta" / "suite-run.env").write_text(
                "profile=profile\nscenario=screen\nround=1\n",
                encoding="utf-8",
            )
            (cell / "scenario-timeline.csv").write_text(
                "phase,started_epoch_ns,finished_epoch_ns\n"
                "C2T2,1,2\nC4T6,3,4\nC5T16,5,6\n",
                encoding="utf-8",
            )
            for phase in ("C2T2", "C4T6", "C5T16"):
                destination = experiment / "runs" / "profile" / phase / "r1"
                (destination / "dataset").mkdir(parents=True)
                (destination / "dataset" / "telemetry.db3").touch()
            config = Mock()
            config.section.side_effect = lambda name: {
                "client": {"host": "ubuntu197", "output_root": "/remote/output"},
                "experiment": {"system": "clickhouse"},
            }[name]

            with patch("prism_sampler.orchestration.runner.Host") as host:
                finalized = _collect_suite_runs(config, experiment, suite)

            self.assertEqual(finalized, 0)
            host.return_value.run.assert_not_called()
            host.return_value.copy_from.assert_not_called()


if __name__ == "__main__":
    unittest.main()
