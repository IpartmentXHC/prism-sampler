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
    expand_run_pressure: float = 0.90
    expand_rq_pressure: float = 0.50
    shrink_run_cpu_equiv: float = 24.0
    shrink_rq_pressure: float = 0.05
    shrink_confirm_seconds: float = 180.0
    minimum_two_node_dwell_seconds: float = 300.0
    cooldown_seconds: float = 120.0
    rollback_confirm_samples: int = 2
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
        if self.sample_interval_seconds <= 0 or self.decision_window_samples <= 0:
            raise ValueError("controller sampling values must be positive")
        if not self.one_node_nodes or not self.two_node_nodes:
            raise ValueError("controller node sets cannot be empty")
        if not set(self.one_node_nodes).issubset(self.two_node_nodes):
            raise ValueError("one_node_nodes must be a subset of two_node_nodes")
        if len(set(self.two_node_nodes)) != len(self.two_node_nodes):
            raise ValueError("controller node sets cannot contain duplicates")
        for name in (
            "expand_run_pressure",
            "expand_rq_pressure",
            "shrink_run_cpu_equiv",
            "shrink_rq_pressure",
            "shrink_confirm_seconds",
            "minimum_two_node_dwell_seconds",
            "cooldown_seconds",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"controller.{name} cannot be negative")

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
    result = ControllerConfig(**values)
    result.validate()
    return result
