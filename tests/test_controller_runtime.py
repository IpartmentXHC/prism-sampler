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


def test_kpi_guard_rolls_back_on_sustained_throughput_drop() -> None:
    runtime = object.__new__(ControllerRuntime)
    runtime.config = SimpleNamespace(
        rollback_throughput_drop_pct=5,
        rollback_p99_increase_pct=50,
    )
    runtime.pending_validation = {
        "previous_state": "one_node",
        "settle_until_ns": 100,
        "baseline": {"throughput_ops_s": 100, "p99_latency_us": 10},
    }
    runtime.kpi_history = [
        {
            "observed_monotonic_ns": 100 + index,
            "throughput_ops_s": 90,
            "max_client_p99_latency_us": 10,
            "error_count_delta": 0,
            "timeout_count_delta": 0,
        }
        for index in range(3)
    ]
    runtime._action = Mock(return_value={"status": "applied"})
    runtime.policy = Mock()
    assert runtime._validate_pending_action(200)
    assert runtime._action.call_args.args[1] == ScalingState.ONE_NODE
    runtime.policy.force_state.assert_called_once_with(ScalingState.ONE_NODE, 200)
