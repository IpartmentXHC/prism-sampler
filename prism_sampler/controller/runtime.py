from __future__ import annotations

import json
import os
import signal
import statistics
import time
from pathlib import Path
from threading import Event
from typing import Any

from .actuator import ClickHouseSlotsActuator, TasksetActuator
from .config import ControllerConfig
from .blackbox_model import BlackboxGScaleModel, LiveSystemFeatureSource, SCHEMA as BLACKBOX_SCHEMA
from .dynamic_model import GScaleModel
from .fine_placement import FinePlacementShadow
from .journal import append_jsonl, read_json, write_json
from .metrics import ProcMetricSource, cpu_nodes, format_cpu_list, process_start_time
from .policy import (
    BenefitPolicy,
    BlackboxBenefitPolicy,
    ContinuousBenefitPolicy,
    Decision,
    PressurePolicy,
    ScalingState,
)


def _runtime_controller_config(value: dict[str, Any]) -> ControllerConfig:
    config = dict(value)
    for name in ("one_node_nodes", "two_node_nodes"):
        if name in config:
            config[name] = tuple(int(item) for item in config[name])
    if "scripted_transitions" in config:
        config["scripted_transitions"] = tuple(
            str(item) for item in config["scripted_transitions"]
        )
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
        self.node_cpus = cpu_nodes()
        self.one_capacity = len(set(int(cpu) for cpu in _expand(self.one_cpus)))
        signatures = list(raw.get("benefit_signatures", []))
        dynamic_model = raw.get("dynamic_model")
        if dynamic_model and dynamic_model.get("schema") == BLACKBOX_SCHEMA:
            if self.config.use_kpi_online:
                raise ValueError(
                    "blackbox-g-scale requires controller.use_kpi_online=false"
                )
            if self.config.use_workload_activity_marker:
                raise ValueError(
                    "blackbox-g-scale requires "
                    "controller.use_workload_activity_marker=false"
                )
            self.policy = BlackboxBenefitPolicy(
                self.config, BlackboxGScaleModel(dict(dynamic_model))
            )
        elif dynamic_model:
            self.policy = ContinuousBenefitPolicy(
                self.config, GScaleModel(dict(dynamic_model))
            )
        elif signatures:
            self.policy = BenefitPolicy(self.config, signatures)
        else:
            self.policy = PressurePolicy(self.config)
        self.source = ProcMetricSource(self.pids, self.start_times)
        self.system_features = LiveSystemFeatureSource()
        self.actuator = TasksetActuator(
            self.pids, self.start_times, command_prefix=self.command_prefix
        )
        self.slots = (
            ClickHouseSlotsActuator(
                Path(self.config.clickhouse_config_path),
                self.config.clickhouse_client_command,
                preprocessed_config_path=(
                    Path(self.config.clickhouse_preprocessed_config_path)
                    if self.config.clickhouse_preprocessed_config_path
                    else None
                ),
                fixed_max_threads=self.config.fixed_max_threads,
            )
            if self.config.one_node_slots
            else None
        )
        self.stop_event = Event()
        self.actual_state = ScalingState(self.config.initial_state)
        self.policy.force_state(self.actual_state)
        self.invalid_samples = 0
        self.kpi_history: list[dict[str, Any]] = []
        self.last_kpi_key: tuple[str, int] | None = None
        self.pending_validation: dict[str, Any] | None = None
        self.fine_placement = FinePlacementShadow()
        self.current_phase = ""
        self.scripted = [
            (float(item.split(":", 1)[0]), ScalingState(item.split(":", 1)[1]))
            for item in self.config.scripted_transitions
        ]
        self.script_phase = ""
        self.script_started_ns: int | None = None
        self.script_index = 0

    def _scripted_decision(
        self, sample: Any, phase: str
    ) -> Decision | None:
        if not self.scripted:
            return None
        if not sample.workload_active:
            self.script_started_ns = None
            self.script_phase = phase
            self.script_index = 0
            return Decision(
                sample.realtime_ns, self.policy.state.value, self.policy.state.value,
                None, "workload_inactive", 0, 0.0, "scripted",
            )
        if self.script_started_ns is None or self.script_phase != phase:
            self.script_started_ns = sample.monotonic_ns
            self.script_phase = phase
            self.script_index = 0
        elapsed = (sample.monotonic_ns - self.script_started_ns) / 1e9
        current = self.policy.state
        action = None
        reason = "waiting_for_scripted_transition"
        target = current
        if self.script_index < len(self.scripted):
            seconds, scheduled = self.scripted[self.script_index]
            if elapsed >= seconds:
                target = scheduled
                self.script_index += 1
                if target != current:
                    action = "scripted_expand" if target == ScalingState.TWO_NODE else "scripted_shrink"
                    self.policy.force_state(target, sample.monotonic_ns)
                    reason = "scripted_transition"
                else:
                    reason = "scripted_state_already_current"
        return Decision(
            sample.realtime_ns,
            current.value,
            target.value,
            action,
            reason,
            0,
            0.0,
            "scripted",
        )

    def _record_kpi(self, value: dict[str, Any], monotonic_ns: int) -> None:
        if not value or not value.get("complete"):
            return
        key = (str(value.get("phase", "")), int(value.get("sequence", 0)))
        if key == self.last_kpi_key:
            return
        if self.kpi_history and self.kpi_history[-1]["phase"] != key[0]:
            self.kpi_history.clear()
        self.last_kpi_key = key
        self.kpi_history.append({**value, "observed_monotonic_ns": monotonic_ns})
        self.kpi_history = self.kpi_history[-20:]

    def _kpi_baseline(self) -> dict[str, float]:
        rows = self.kpi_history[-3:]
        if not rows:
            return {}
        return {
            "throughput_ops_s": statistics.median(
                float(row.get("throughput_ops_s", 0)) for row in rows
            ),
            "p99_latency_us": statistics.median(
                float(row.get("max_client_p99_latency_us", 0)) for row in rows
            ),
        }

    def _start_action_validation(
        self,
        previous: ScalingState,
        action_monotonic_ns: int,
        baseline: dict[str, float],
    ) -> None:
        self.pending_validation = {
            "previous_state": previous.value,
            "action_monotonic_ns": action_monotonic_ns,
            "settle_until_ns": action_monotonic_ns + int(self.config.settling_seconds * 1e9),
            "baseline": baseline,
        }

    def _validate_pending_action(self, now_ns: int) -> bool:
        pending = self.pending_validation
        if not pending or now_ns < int(pending["settle_until_ns"]):
            return False
        rows = [
            row for row in self.kpi_history
            if int(row["observed_monotonic_ns"]) >= int(pending["settle_until_ns"])
        ]
        if len(rows) < 3:
            return False
        rows = rows[-3:]
        baseline = pending["baseline"]
        throughput = statistics.median(float(row.get("throughput_ops_s", 0)) for row in rows)
        p99 = statistics.median(float(row.get("max_client_p99_latency_us", 0)) for row in rows)
        errors = sum(
            int(row.get("error_count_delta", 0)) + int(row.get("timeout_count_delta", 0))
            for row in rows
        )
        reasons = []
        baseline_throughput = float(baseline.get("throughput_ops_s", 0))
        baseline_p99 = float(baseline.get("p99_latency_us", 0))
        if baseline_throughput and throughput < baseline_throughput * (
            1 - self.config.rollback_throughput_drop_pct / 100
        ):
            reasons.append("throughput_drop")
        if baseline_p99 and p99 > baseline_p99 * (
            1 + self.config.rollback_p99_increase_pct / 100
        ):
            reasons.append("p99_increase")
        if errors:
            reasons.append("errors")
        self.pending_validation = None
        if not reasons:
            return False
        target = ScalingState(str(pending["previous_state"]))
        self._action("kpi_guard_rollback:" + ",".join(reasons), target)
        self.policy.force_state(target, now_ns)
        return True

    def _signal(self, _signum: int, _frame: object) -> None:
        self.stop_event.set()

    def _action(
        self, action: str, target: ScalingState, *, fatal: bool = False
    ) -> dict[str, Any]:
        previous = self.actual_state
        cpus = self.one_cpus if target == ScalingState.ONE_NODE else self.two_cpus
        target_slots = (
            getattr(self.config, "one_node_slots", 0)
            if target == ScalingState.ONE_NODE
            else getattr(self.config, "two_node_slots", 0)
        )
        previous_slots = (
            getattr(self.config, "one_node_slots", 0)
            if previous == ScalingState.ONE_NODE
            else getattr(self.config, "two_node_slots", 0)
        )
        slots_actuator = getattr(self, "slots", None)
        record: dict[str, Any] = {
            "schema": "prism-sampler.controller-action.v1",
            "realtime_ns": time.time_ns(),
            "action": action,
            "from_state": previous.value,
            "to_state": target.value,
            "cpus": cpus,
            "slots": target_slots or None,
            "mode": self.config.mode,
            "phase": getattr(self, "current_phase", ""),
        }
        if self.config.mode == "shadow":
            record.update(status="shadow", finished_realtime_ns=time.time_ns())
            append_jsonl(self.run_dir / "actions.jsonl", record)
            return record
        failure: Exception | None = None
        try:
            steps = []
            if target == ScalingState.TWO_NODE:
                steps.append({"affinity": self.actuator.apply(cpus)})
                if slots_actuator:
                    steps.append({"slots": slots_actuator.apply(target_slots)})
            else:
                if slots_actuator:
                    steps.append({"slots": slots_actuator.apply(target_slots)})
                steps.append({"affinity": self.actuator.apply(cpus)})
            record.update(status="applied", steps=steps)
            self.actual_state = target
        except Exception as exc:
            failure = exc
            record.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            previous_cpus = self.one_cpus if previous == ScalingState.ONE_NODE else self.two_cpus
            try:
                rollback = []
                if previous == ScalingState.TWO_NODE:
                    rollback.append({"affinity": self.actuator.apply(previous_cpus)})
                    if slots_actuator:
                        rollback.append({"slots": slots_actuator.apply(previous_slots)})
                else:
                    if slots_actuator:
                        rollback.append({"slots": slots_actuator.apply(previous_slots)})
                    rollback.append({"affinity": self.actuator.apply(previous_cpus)})
                record["rollback"] = rollback
            except Exception as rollback_exc:
                record["rollback_error"] = (
                    f"{type(rollback_exc).__name__}: {rollback_exc}"
                )
            self.policy.force_state(previous, time.monotonic_ns())
        record["finished_realtime_ns"] = time.time_ns()
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
                "policy": type(self.policy).__name__,
                "dynamic_model_schema": (
                    self.policy.model.raw.get("schema")
                    if isinstance(
                        self.policy, (ContinuousBenefitPolicy, BlackboxBenefitPolicy)
                    )
                    else None
                ),
                "online_input_mode": (
                    "application_kpi" if self.config.use_kpi_online else "system_blackbox"
                ),
            },
        )
        if self.config.mode == "active":
            self._action("initialize", self.actual_state, fatal=True)
        deadline = time.monotonic()
        try:
            while not self.stop_event.is_set():
                control = read_json(
                    self.run_dir / "control.json",
                    {"workload_active": False, "phase": ""},
                )
                self.current_phase = str(control.get("phase", ""))
                sample = self.source.sample(
                    workload_active=(
                        bool(control.get("workload_active"))
                        if self.config.use_workload_activity_marker else True
                    ),
                    capacity_cpus=self.one_capacity,
                )
                if isinstance(self.policy, BlackboxBenefitPolicy):
                    sample.system_features.update(
                        self.system_features.build(
                            sample,
                            state=self.actual_state.value,
                            telemetry_dir=(
                                Path(str(control["telemetry_dir"]))
                                if control.get("telemetry_dir") else None
                            ),
                            relationship_candidates=(
                                Path(str(control["relationship_candidates"]))
                                if control.get("relationship_candidates") else None
                            ),
                        )
                    )
                if self.config.use_kpi_online:
                    latest_kpi = read_json(self.run_dir / "kpi-latest.json")
                    if latest_kpi.get("phase") == str(control.get("phase", "")):
                        sample.kpi.update(latest_kpi)
                if self.slots and self.config.use_kpi_online:
                    try:
                        sample.clickhouse_metrics.update(self.slots.metrics())
                        sample.clickhouse_metrics["configured_slots"] = float(
                            self.slots.current_slots()
                        )
                    except (RuntimeError, ValueError) as exc:
                        sample.clickhouse_metrics["query_error"] = str(exc)
                if self.config.use_kpi_online:
                    self._record_kpi(sample.kpi, sample.monotonic_ns)
                sample_row = {
                    "schema": "prism-sampler.controller-sample.v1",
                    **sample.to_dict(),
                    "phase": str(control.get("phase", "")),
                    "policy_state": self.policy.state.value,
                    "actual_state": self.actual_state.value,
                }
                append_jsonl(self.run_dir / "samples.jsonl", sample_row)
                if (
                    self.config.fine_placement_mode == "shadow"
                    and sample.workload_active
                    and control.get("relationship_candidates")
                ):
                    available_nodes = (
                        self.config.one_node_nodes
                        if self.actual_state == ScalingState.ONE_NODE
                        else self.config.two_node_nodes
                    )
                    placement = self.fine_placement.poll(
                        Path(str(control["relationship_candidates"])),
                        phase=str(control.get("phase", "")),
                        scaling_state=self.actual_state.value,
                        available_nodes=available_nodes,
                        node_cpus=self.node_cpus,
                        pair_threshold=self.config.fine_placement_pair_threshold,
                        self_threshold=self.config.fine_placement_self_threshold,
                        minimum_confidence=self.config.fine_placement_minimum_confidence,
                        cluster_size=self.config.fine_placement_cluster_size,
                    )
                    if placement is not None:
                        append_jsonl(
                            self.run_dir / "fine-placement.jsonl", placement
                        )
                rolled_back = (
                    self._validate_pending_action(sample.monotonic_ns)
                    if self.config.use_kpi_online else False
                )
                decision = self._scripted_decision(
                    sample, str(control.get("phase", ""))
                ) or self.policy.evaluate(sample)
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
                if decision.action and not rolled_back:
                    target = ScalingState(decision.target_state)
                    previous = self.actual_state
                    baseline = self._kpi_baseline() if self.config.use_kpi_online else {}
                    result = self._action(decision.action, target)
                    if self.config.mode == "shadow":
                        self.policy.force_state(self.actual_state, sample.monotonic_ns)
                    if (
                        self.config.mode == "active"
                        and result.get("status") == "applied"
                        and not self.scripted
                        and self.config.use_kpi_online
                    ):
                        self._start_action_validation(
                            previous, time.monotonic_ns(), baseline
                        )
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
                    restore = {"affinity": self.actuator.restore()}
                    if self.slots:
                        restore["slots"] = self.slots.restore()
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
