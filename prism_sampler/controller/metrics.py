from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from pathlib import Path

from .models import MetricSample


class TargetIdentityError(RuntimeError):
    pass


def process_start_time(stat: str) -> int:
    end = stat.rfind(")")
    fields = stat[end + 2 :].split()
    if end < 0 or len(fields) < 20:
        raise ValueError("invalid /proc PID stat")
    return int(fields[19])


def parse_cpu_list(value: str) -> set[int]:
    result: set[int] = set()
    for item in value.strip().split(","):
        if not item:
            continue
        if "-" in item:
            start, end = (int(part) for part in item.split("-", 1))
            result.update(range(start, end + 1))
        else:
            result.add(int(item))
    return result


def format_cpu_list(cpus: set[int]) -> str:
    if not cpus:
        return ""
    ordered = sorted(cpus)
    ranges: list[str] = []
    start = previous = ordered[0]
    for cpu in ordered[1:]:
        if cpu == previous + 1:
            previous = cpu
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = cpu
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def cpu_nodes(sys_root: Path = Path("/sys")) -> dict[int, set[int]]:
    result: dict[int, set[int]] = {}
    for path in (sys_root / "devices/system/node").glob("node[0-9]*"):
        try:
            node = int(path.name.removeprefix("node"))
            result[node] = parse_cpu_list((path / "cpulist").read_text())
        except (OSError, ValueError):
            continue
    return result


def _allowed_list(status: str) -> str:
    for line in status.splitlines():
        if line.startswith("Cpus_allowed_list:"):
            return line.split(":", 1)[1].strip()
    return ""


def _proc_stat(value: str) -> dict[int, tuple[int, int]]:
    result = {}
    for line in value.splitlines():
        fields = line.split()
        if not fields or not re.fullmatch(r"cpu\d+", fields[0]):
            continue
        counters = [int(item) for item in fields[1:]]
        if len(counters) >= 5:
            result[int(fields[0][3:])] = (sum(counters), counters[3] + counters[4])
    return result


def _psi(value: str) -> dict[str, float]:
    result: dict[str, float] = {}
    for line in value.splitlines():
        fields = line.split()
        if not fields:
            continue
        for item in fields[1:]:
            if "=" not in item:
                continue
            key, raw = item.split("=", 1)
            try:
                result[f"{fields[0]}_{key}"] = float(raw)
            except ValueError:
                continue
    return result


class ProcMetricSource:
    def __init__(
        self,
        pids: list[int],
        start_times: dict[int, int],
        *,
        proc_root: Path = Path("/proc"),
        sys_root: Path = Path("/sys"),
    ):
        self.pids = sorted(set(pids))
        self.start_times = dict(start_times)
        self.proc_root = proc_root
        self.nodes = cpu_nodes(sys_root)
        self._previous_tasks: dict[int, tuple[int, int]] = {}
        self._previous_proc_stat: dict[int, tuple[int, int]] = {}
        self._previous_monotonic_ns: int | None = None

    def _read(self, path: Path) -> str:
        return path.read_text(encoding="utf-8", errors="replace")

    def _validate_targets(self) -> None:
        for pid in self.pids:
            path = self.proc_root / str(pid) / "stat"
            try:
                actual = process_start_time(self._read(path))
            except OSError as exc:
                raise TargetIdentityError(f"target PID disappeared: {pid}") from exc
            expected = self.start_times.get(pid)
            if expected is not None and actual != expected:
                raise TargetIdentityError(
                    f"target PID identity changed: {pid} expected={expected} actual={actual}"
                )

    def _tasks(self) -> tuple[dict[int, tuple[int, int]], dict[str, str]]:
        counters: dict[int, tuple[int, int]] = {}
        affinities: dict[str, str] = {}
        for pid in self.pids:
            task_root = self.proc_root / str(pid) / "task"
            for task in task_root.glob("[0-9]*"):
                try:
                    tid = int(task.name)
                    values = self._read(task / "schedstat").split()
                    if len(values) < 2:
                        continue
                    counters[tid] = (int(values[0]), int(values[1]))
                    affinities[str(tid)] = _allowed_list(self._read(task / "status"))
                except (OSError, ValueError):
                    continue
        return counters, affinities

    def _numa_pages(self) -> dict[str, int]:
        pages: dict[int, int] = defaultdict(int)
        for pid in self.pids:
            try:
                value = self._read(self.proc_root / str(pid) / "numa_maps")
            except OSError:
                continue
            for line in value.splitlines():
                for node, count in re.findall(r"\bN(\d+)=(\d+)\b", line):
                    pages[int(node)] += int(count)
        return {str(node): count for node, count in sorted(pages.items())}

    def _node_utilization(
        self, current: dict[int, tuple[int, int]]
    ) -> dict[str, float]:
        result = {}
        for node, cpus in self.nodes.items():
            busy = total = 0
            for cpu in cpus:
                if cpu not in current or cpu not in self._previous_proc_stat:
                    continue
                total_delta = current[cpu][0] - self._previous_proc_stat[cpu][0]
                idle_delta = current[cpu][1] - self._previous_proc_stat[cpu][1]
                if total_delta > 0:
                    total += total_delta
                    busy += total_delta - idle_delta
            if total:
                result[str(node)] = busy / total
        return result

    def sample(self, *, workload_active: bool, capacity_cpus: int) -> MetricSample:
        realtime_ns = time.time_ns()
        monotonic_ns = time.monotonic_ns()
        try:
            self._validate_targets()
            tasks, affinities = self._tasks()
            proc_stat = _proc_stat(self._read(self.proc_root / "stat"))
            try:
                psi = _psi(self._read(self.proc_root / "pressure/cpu"))
            except OSError:
                psi = {}
            interval = (
                (monotonic_ns - self._previous_monotonic_ns) / 1e9
                if self._previous_monotonic_ns is not None
                else 0.0
            )
            run_ns = rq_ns = 0
            common = 0
            for tid, current in tasks.items():
                previous = self._previous_tasks.get(tid)
                if previous is None:
                    continue
                run_delta = current[0] - previous[0]
                rq_delta = current[1] - previous[1]
                if run_delta < 0 or rq_delta < 0:
                    continue
                run_ns += run_delta
                rq_ns += rq_delta
                common += 1
            valid = interval > 0 and common > 0
            run_cpu = run_ns / (interval * 1e9) if valid else None
            rq_cpu = rq_ns / (interval * 1e9) if valid else None
            sample = MetricSample(
                realtime_ns=realtime_ns,
                monotonic_ns=monotonic_ns,
                interval_seconds=interval,
                workload_active=workload_active,
                valid=valid,
                run_cpu_equiv=run_cpu,
                rq_cpu_equiv=rq_cpu,
                run_pressure=(run_cpu / capacity_cpus if run_cpu is not None else None),
                rq_pressure=(rq_cpu / capacity_cpus if rq_cpu is not None else None),
                tids_observed=len(tasks),
                cpu_affinity=affinities,
                node_cpu_utilization=self._node_utilization(proc_stat),
                numa_pages=self._numa_pages(),
                psi_cpu=psi,
                error=None if valid else "initial or empty schedstat interval",
            )
            self._previous_tasks = tasks
            self._previous_proc_stat = proc_stat
            self._previous_monotonic_ns = monotonic_ns
            return sample
        except (OSError, ValueError, TargetIdentityError) as exc:
            return MetricSample(
                realtime_ns=realtime_ns,
                monotonic_ns=monotonic_ns,
                interval_seconds=0.0,
                workload_active=workload_active,
                valid=False,
                run_cpu_equiv=None,
                rq_cpu_equiv=None,
                run_pressure=None,
                rq_pressure=None,
                tids_observed=0,
                error=f"{type(exc).__name__}: {exc}",
            )
