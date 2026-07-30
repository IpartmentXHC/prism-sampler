from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class MetricSample:
    realtime_ns: int
    monotonic_ns: int
    interval_seconds: float
    workload_active: bool
    valid: bool
    run_cpu_equiv: float | None
    rq_cpu_equiv: float | None
    run_pressure: float | None
    rq_pressure: float | None
    tids_observed: int
    cpu_affinity: dict[str, str] = field(default_factory=dict)
    node_cpu_utilization: dict[str, float] = field(default_factory=dict)
    numa_pages: dict[str, int] = field(default_factory=dict)
    psi_cpu: dict[str, float] = field(default_factory=dict)
    kpi: dict[str, Any] = field(default_factory=dict)
    clickhouse_metrics: dict[str, Any] = field(default_factory=dict)
    system_features: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetricSource(Protocol):
    def sample(self, *, workload_active: bool, capacity_cpus: int) -> MetricSample: ...


class Actuator(Protocol):
    def apply(self, cpus: str) -> dict[str, Any]: ...

    def restore(self) -> dict[str, Any]: ...
