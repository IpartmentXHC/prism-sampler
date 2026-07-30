from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import SamplerConfig


@dataclass(frozen=True)
class ControllerConfig:
    mode: str = "off"
    sample_interval_seconds: float = 10.0
    decision_window_samples: int = 3
    one_node_nodes: tuple[int, ...] = (0,)
    two_node_nodes: tuple[int, ...] = (0, 1)
    one_node_slots: int = 0
    two_node_slots: int = 0
    fixed_max_threads: int = 0
    initial_state: str = "one_node"
    scripted_transitions: tuple[str, ...] = ()
    clickhouse_config_path: str = ""
    clickhouse_preprocessed_config_path: str = ""
    clickhouse_client_command: str = ""
    benefit_signatures_file: str = ""
    dynamic_model_file: str = ""
    use_kpi_online: bool = True
    use_workload_activity_marker: bool = True
    minimum_model_confidence: float = 0.8
    minimum_feature_coverage: float = 0.8
    minimum_expected_gain_pct: float = 2.0
    maximum_signature_distance: float = 0.75
    maximum_model_distance: float = 2.5
    gain_uncertainty_multiplier: float = 0.5
    pressure_change_absolute: float = 0.15
    pressure_change_relative: float = 0.25
    pressure_change_confirm_samples: int = 3
    expand_run_pressure: float = 0.90
    expand_rq_pressure: float = 0.50
    shrink_run_cpu_equiv: float = 24.0
    shrink_rq_pressure: float = 0.05
    shrink_confirm_seconds: float = 180.0
    minimum_two_node_dwell_seconds: float = 60.0
    cooldown_seconds: float = 60.0
    settling_seconds: float = 20.0
    rollback_throughput_drop_pct: float = 5.0
    rollback_p99_increase_pct: float = 50.0
    rollback_confirm_samples: int = 2
    fine_placement_mode: str = "off"
    fine_placement_pair_threshold: float = 10.0
    fine_placement_self_threshold: float = 10.0
    fine_placement_minimum_confidence: float = 0.7
    fine_placement_cluster_size: int = 4
    actuator: str = "taskset"
    migrate_pages: bool = False
    agent_command: str = "/home/xhc/prism-sampler/bin/prism-numa-controller"

    def validate(self) -> None:
        if self.mode not in {"off", "shadow", "active"}:
            raise ValueError(f"unknown controller mode: {self.mode}")
        if self.actuator != "taskset":
            raise ValueError("the prototype only supports the taskset actuator")
        if self.migrate_pages:
            raise ValueError("page migration is not supported by the prototype")
        if self.initial_state not in {"one_node", "two_node"}:
            raise ValueError("controller.initial_state must be one_node or two_node")
        if self.fine_placement_mode not in {"off", "shadow"}:
            raise ValueError("controller.fine_placement_mode must be off or shadow")
        previous = -1.0
        for transition in self.scripted_transitions:
            try:
                raw_seconds, state = transition.split(":", 1)
                seconds = float(raw_seconds)
            except (ValueError, AttributeError) as exc:
                raise ValueError(f"invalid scripted transition: {transition}") from exc
            if seconds <= previous or state not in {"one_node", "two_node"}:
                raise ValueError(f"invalid scripted transition: {transition}")
            previous = seconds
        if self.sample_interval_seconds <= 0 or self.decision_window_samples <= 0:
            raise ValueError("controller sampling values must be positive")
        if not self.one_node_nodes or not self.two_node_nodes:
            raise ValueError("controller node sets cannot be empty")
        if not set(self.one_node_nodes).issubset(self.two_node_nodes):
            raise ValueError("one_node_nodes must be a subset of two_node_nodes")
        if len(set(self.two_node_nodes)) != len(self.two_node_nodes):
            raise ValueError("controller node sets cannot contain duplicates")
        if bool(self.one_node_slots or self.two_node_slots) != bool(
            self.clickhouse_config_path and self.clickhouse_client_command
        ):
            raise ValueError(
                "slot control requires both slots and ClickHouse config/client settings"
            )
        if bool(self.one_node_slots) != bool(self.two_node_slots):
            raise ValueError("one_node_slots and two_node_slots must be configured together")
        for name in (
            "expand_run_pressure",
            "expand_rq_pressure",
            "shrink_run_cpu_equiv",
            "shrink_rq_pressure",
            "shrink_confirm_seconds",
            "minimum_two_node_dwell_seconds",
            "cooldown_seconds",
            "minimum_expected_gain_pct",
            "minimum_model_confidence",
            "minimum_feature_coverage",
            "maximum_signature_distance",
            "maximum_model_distance",
            "gain_uncertainty_multiplier",
            "pressure_change_absolute",
            "pressure_change_relative",
            "settling_seconds",
            "rollback_throughput_drop_pct",
            "rollback_p99_increase_pct",
            "fine_placement_pair_threshold",
            "fine_placement_self_threshold",
            "fine_placement_minimum_confidence",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"controller.{name} cannot be negative")
        for name in ("minimum_model_confidence", "minimum_feature_coverage"):
            if getattr(self, name) > 1:
                raise ValueError(f"controller.{name} cannot exceed 1")
        for name in ("one_node_slots", "two_node_slots", "fixed_max_threads"):
            if getattr(self, name) < 0:
                raise ValueError(f"controller.{name} cannot be negative")
        if self.pressure_change_confirm_samples <= 0:
            raise ValueError("controller.pressure_change_confirm_samples must be positive")
        if self.fine_placement_cluster_size <= 0:
            raise ValueError("controller.fine_placement_cluster_size must be positive")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["one_node_nodes"] = list(self.one_node_nodes)
        value["two_node_nodes"] = list(self.two_node_nodes)
        return value


def controller_config(
    config: SamplerConfig, *, mode_override: str | None = None
) -> ControllerConfig:
    values = config.section("controller")
    if mode_override is not None:
        values["mode"] = mode_override
    known = {field.name for field in ControllerConfig.__dataclass_fields__.values()}
    unknown = sorted(set(values) - known)
    if unknown:
        raise ValueError("unknown controller keys: " + ", ".join(unknown))
    for name in ("one_node_nodes", "two_node_nodes"):
        if name in values:
            values[name] = tuple(int(item) for item in values[name])
    if "scripted_transitions" in values:
        values["scripted_transitions"] = tuple(
            str(item) for item in values["scripted_transitions"]
        )
    result = ControllerConfig(**values)
    result.validate()
    return result
