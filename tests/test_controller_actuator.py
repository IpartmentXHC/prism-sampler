from __future__ import annotations

from unittest.mock import Mock

import pytest

import subprocess

from prism_sampler.controller.actuator import ClickHouseSlotsActuator, TasksetActuator


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


def test_clickhouse_slots_actuator_reloads_and_verifies(tmp_path) -> None:
    path = tmp_path / "90-experiment.xml"
    original = b"<clickhouse><concurrent_threads_soft_limit_num>32</concurrent_threads_soft_limit_num></clickhouse>\n"
    path.write_bytes(original)
    outputs = iter(["32\n", "", "64\n", "", "32\n"])

    def runner(_command: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, next(outputs), "")

    actuator = ClickHouseSlotsActuator(path, "clickhouse client", runner=runner)
    result = actuator.apply(64)
    assert result["slots"] == 64
    assert "<concurrent_threads_soft_limit_ratio_to_cores>0" in path.read_text()
    restored = actuator.restore()
    assert restored["slots"] == 32
    assert path.read_bytes() == original


def test_clickhouse_slots_actuator_uses_reloaded_preprocessed_config(tmp_path) -> None:
    path = tmp_path / "90-experiment.xml"
    preprocessed = tmp_path / "preprocessed.xml"
    original = (
        b"<clickhouse><concurrent_threads_soft_limit_num>128"
        b"</concurrent_threads_soft_limit_num></clickhouse>\n"
    )
    path.write_bytes(original)
    preprocessed.write_bytes(original)

    def runner(command: str) -> subprocess.CompletedProcess[str]:
        if "SYSTEM RELOAD CONFIG" in command:
            preprocessed.write_bytes(path.read_bytes())
        if "system.settings" in command:
            return subprocess.CompletedProcess([], 0, "32\n", "")
        if "system.server_settings" in command:
            return subprocess.CompletedProcess([], 0, "128\n", "")
        return subprocess.CompletedProcess([], 0, "", "")

    actuator = ClickHouseSlotsActuator(
        path,
        "clickhouse client",
        preprocessed_config_path=preprocessed,
        fixed_max_threads=32,
        runner=runner,
    )
    result = actuator.apply(64)

    assert result["slots"] == 64
    assert result["verification_source"] == "preprocessed_config"
    restored = actuator.restore()
    assert restored["slots"] == 128
