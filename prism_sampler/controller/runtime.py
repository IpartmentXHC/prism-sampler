from __future__ import annotations

import json
import os
import signal
import time
from pathlib import Path
from threading import Event
from typing import Any

from .actuator import TasksetActuator
from .config import ControllerConfig
from .journal import append_jsonl, read_json, write_json
from .metrics import ProcMetricSource, cpu_nodes, format_cpu_list, process_start_time
from .policy import PressurePolicy, ScalingState


def _runtime_controller_config(value: dict[str, Any]) -> ControllerConfig:
    config = dict(value)
    for name in ("one_node_nodes", "two_node_nodes"):
        if name in config:
            config[name] = tuple(int(item) for item in config[name])
    result = ControllerConfig(**config)
    result.validate()
    return result


def _cpus_for_nodes(nodes: tuple[int, ...]) -> str:
    topology = cpu_nodes()
    missing = sorted(set(nodes) - set(topology))
    if missing:
        raise RuntimeError(f"NUMA nodes are unavailable: {missing}")
    return format_cpu_list(set().union(*(topology[node] for node in nodes)))


def _self_start_time() -> int:
    return process_start_time(Path(f"/proc/{os.getpid()}/stat").read_text())


class ControllerRuntime:
    def __init__(self, runtime_config: Path):
        raw = json.loads(runtime_config.read_text(encoding="utf-8"))
        self.run_dir = Path(raw["run_dir"])
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.config = _runtime_controller_config(raw["controller"])
        self.pids = [int(pid) for pid in raw["pids"]]
        self.start_times = {int(k): int(v) for k, v in raw["start_times"].items()}
        self.command_prefix = str(raw.get("command_prefix", ""))
        self.one_cpus = _cpus_for_nodes(self.config.one_node_nodes)
        self.two_cpus = _cpus_for_nodes(self.config.two_node_nodes)
        self.one_capacity = len(set(int(cpu) for cpu in _expand(self.one_cpus)))
        self.policy = PressurePolicy(self.config)
        self.source = ProcMetricSource(self.pids, self.start_times)
        self.actuator = TasksetActuator(
            self.pids, self.start_times, command_prefix=self.command_prefix
        )
        self.stop_event = Event()
        self.actual_state = ScalingState.ONE_NODE
        self.invalid_samples = 0

    def _signal(self, _signum: int, _frame: object) -> None:
        self.stop_event.set()

    def _action(
        self, action: str, target: ScalingState, *, fatal: bool = False
    ) -> dict[str, Any]:
        previous = self.actual_state
        cpus = self.one_cpus if target == ScalingState.ONE_NODE else self.two_cpus
        record: dict[str, Any] = {
            "schema": "prism-sampler.controller-action.v1",
            "realtime_ns": time.time_ns(),
            "action": action,
            "from_state": previous.value,
            "to_state": target.value,
            "cpus": cpus,
            "mode": self.config.mode,
        }
        if self.config.mode == "shadow":
            record["status"] = "shadow"
            append_jsonl(self.run_dir / "actions.jsonl", record)
            return record
        failure: Exception | None = None
        try:
            record.update(self.actuator.apply(cpus))
            self.actual_state = target
        except Exception as exc:
            failure = exc
            record.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            previous_cpus = self.one_cpus if previous == ScalingState.ONE_NODE else self.two_cpus
            try:
                record["rollback"] = self.actuator.apply(previous_cpus)
            except Exception as rollback_exc:
                record["rollback_error"] = (
                    f"{type(rollback_exc).__name__}: {rollback_exc}"
                )
            self.policy.force_state(previous, time.monotonic_ns())
        append_jsonl(self.run_dir / "actions.jsonl", record)
        if failure is not None and fatal:
            raise RuntimeError(
                f"required controller action failed: {action} -> {target.value}"
            ) from failure
        return record

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        write_json(
            self.run_dir / "controller.pid.json",
            {"pid": os.getpid(), "start_time": _self_start_time()},
        )
        write_json(
            self.run_dir / "resolved-config.json",
            {
                "schema": "prism-sampler.controller-runtime.v1",
                "controller": self.config.to_dict(),
                "pids": self.pids,
                "start_times": {str(k): v for k, v in self.start_times.items()},
                "one_node_cpus": self.one_cpus,
                "two_node_cpus": self.two_cpus,
            },
        )
        if self.config.mode == "active":
            self._action("initialize", ScalingState.ONE_NODE, fatal=True)
        deadline = time.monotonic()
        try:
            while not self.stop_event.is_set():
                control = read_json(
                    self.run_dir / "control.json",
                    {"workload_active": False, "phase": ""},
                )
                sample = self.source.sample(
                    workload_active=bool(control.get("workload_active")),
                    capacity_cpus=self.one_capacity,
                )
                sample_row = {
                    "schema": "prism-sampler.controller-sample.v1",
                    **sample.to_dict(),
                    "phase": str(control.get("phase", "")),
                    "policy_state": self.policy.state.value,
                    "actual_state": self.actual_state.value,
                }
                append_jsonl(self.run_dir / "samples.jsonl", sample_row)
                decision = self.policy.evaluate(sample)
                append_jsonl(
                    self.run_dir / "decisions.jsonl",
                    {
                        "schema": "prism-sampler.controller-decision.v1",
                        **decision.to_dict(),
                        "phase": str(control.get("phase", "")),
                        "mode": self.config.mode,
                    },
                )
                if sample.valid:
                    self.invalid_samples = 0
                else:
                    self.invalid_samples += 1
                if decision.action:
                    target = ScalingState(decision.target_state)
                    self._action(decision.action, target)
                write_json(
                    self.run_dir / "status.json",
                    {
                        "status": "running",
                        "pid": os.getpid(),
                        "realtime_ns": sample.realtime_ns,
                        "policy_state": self.policy.state.value,
                        "actual_state": self.actual_state.value,
                        "workload_active": sample.workload_active,
                        "phase": str(control.get("phase", "")),
                        "invalid_samples": self.invalid_samples,
                    },
                )
                deadline += self.config.sample_interval_seconds
                self.stop_event.wait(max(0.0, deadline - time.monotonic()))
        finally:
            restore: dict[str, Any]
            if self.config.mode == "active":
                try:
                    restore = self.actuator.restore()
                except Exception as exc:
                    restore = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
            else:
                restore = {"status": "not_required"}
            append_jsonl(
                self.run_dir / "actions.jsonl",
                {
                    "schema": "prism-sampler.controller-action.v1",
                    "realtime_ns": time.time_ns(),
                    "action": "restore",
                    "from_state": self.actual_state.value,
                    "to_state": "original",
                    **restore,
                },
            )
            write_json(
                self.run_dir / "status.json",
                {"status": "stopped", "pid": os.getpid(), "restore": restore},
            )


def _expand(value: str) -> list[int]:
    result = []
    for item in value.split(","):
        if "-" in item:
            start, end = map(int, item.split("-", 1))
            result.extend(range(start, end + 1))
        elif item:
            result.append(int(item))
    return result


def run(runtime_config: Path) -> None:
    ControllerRuntime(runtime_config).run()
