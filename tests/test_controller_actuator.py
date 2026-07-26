from __future__ import annotations

from unittest.mock import Mock

import pytest

from prism_sampler.controller.actuator import TasksetActuator


def actuator_without_proc() -> TasksetActuator:
    actuator = object.__new__(TasksetActuator)
    actuator.pids = [10]
    actuator.start_times = {10: 99}
    actuator.command_prefix = ""
    actuator.runner = Mock()
    actuator.retries = 3
    actuator.original_affinity = {10: "0-127", 11: "0-127"}
    actuator._validate_pids = Mock()
    actuator._tids = Mock(return_value=[10, 11])
    actuator._taskset = Mock()
    return actuator


def test_taskset_actuator_verifies_every_thread() -> None:
    actuator = actuator_without_proc()
    actuator._status_affinity = Mock(side_effect=lambda _tid: "0-31")
    result = actuator.apply("0-31")
    assert result["status"] == "applied"
    actuator._taskset.assert_called_once_with("0-31", 10, all_tasks=True)


def test_taskset_actuator_fails_after_partial_verification() -> None:
    actuator = actuator_without_proc()
    actuator._status_affinity = Mock(side_effect=lambda tid: "0-31" if tid == 10 else "0-63")
    with pytest.raises(RuntimeError, match="verification failed"):
        actuator.apply("0-31")
    assert actuator._taskset.call_count == 3


def test_restore_uses_all_tasks_for_uniform_original_affinity() -> None:
    actuator = actuator_without_proc()
    actuator._status_affinity = Mock(return_value="0-127")

    result = actuator.restore()

    assert result["status"] == "restored"
    assert result["method"] == "uniform-all-tasks"
    actuator._taskset.assert_called_once_with("0-127", 10, all_tasks=True)
