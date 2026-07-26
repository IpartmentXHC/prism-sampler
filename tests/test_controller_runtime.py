from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from prism_sampler.controller.policy import ScalingState
from prism_sampler.controller.runtime import ControllerRuntime


def test_required_initial_affinity_failure_aborts_startup(tmp_path) -> None:
    runtime = object.__new__(ControllerRuntime)
    runtime.run_dir = tmp_path
    runtime.config = SimpleNamespace(mode="active")
    runtime.actual_state = ScalingState.ONE_NODE
    runtime.one_cpus = "0-31"
    runtime.two_cpus = "0-63"
    runtime.policy = Mock()
    runtime.actuator = Mock()
    runtime.actuator.apply.side_effect = RuntimeError("operation not permitted")

    with pytest.raises(RuntimeError, match="required controller action failed"):
        runtime._action("initialize", ScalingState.ONE_NODE, fatal=True)

    row = json.loads((tmp_path / "actions.jsonl").read_text())
    assert row["status"] == "failed"
    assert row["action"] == "initialize"
    assert runtime.actual_state == ScalingState.ONE_NODE

