from __future__ import annotations

import csv
import fnmatch
import json
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .remote import Host


def _duckdb_timestamp(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).replace(tzinfo=None)


def _number(value: object) -> float | None:
    text = str(value).strip().replace(" ", "")
    if not text or text.startswith("<not") or text.lower() in {"nan", "n/a"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_event(raw: str) -> str:
    event = raw.strip().replace(":u", "").replace(":k", "")
    if "/" in event:
        pmu, inner, *_ = event.split("/")
        if inner:
            return f"{pmu}/{inner.replace('-', '_')}"
    return event.replace("-", "_")


@dataclass(frozen=True)
class PerfSample:
    interval_s: float
    scope: str
    pmu: str
    event: str
    value: float | None
    unit: str
    scale: float | None
    time_enabled: float | None
    time_running: float | None


def parse_perf_stat(text: str, scope: str = "process") -> list[PerfSample]:
    samples: list[PerfSample] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        fields = next(csv.reader([line]))
        if len(fields) < 4:
            continue
        interval = _number(fields[0])
        if interval is None:
            continue
        raw_event = fields[3].strip()
        if not raw_event:
            continue
        normalized = normalize_event(raw_event)
        pmu = raw_event.split("/", 1)[0] if "/" in raw_event else "software-or-core"
        time_running = _number(fields[4]) if len(fields) > 4 else None
        percentage = _number(fields[5].rstrip("%")) if len(fields) > 5 else None
        scale = percentage / 100.0 if percentage is not None else 1.0
        time_enabled = time_running / scale if time_running is not None and scale else None
        samples.append(
            PerfSample(
                interval_s=interval,
                scope=scope,
                pmu=pmu,
                event=normalized,
                value=_number(fields[1]),
                unit=fields[2].strip(),
                scale=scale,
                time_enabled=time_enabled,
                time_running=time_running,
            )
        )
    return samples


def select_core_events(candidates: Iterable[str], available: Iterable[str]) -> list[str]:
    available_map = {normalize_event(event).lower(): event for event in available}
    selected: list[str] = []
    for candidate in candidates:
        normalized = normalize_event(candidate).lower()
        exact = available_map.get(normalized)
        if exact:
            selected.append(exact)
            continue
        suffix = "/" + normalized
        match = next((raw for key, raw in available_map.items() if key.endswith(suffix)), None)
        if match:
            selected.append(match)
    return list(dict.fromkeys(selected))


def discover_uncore_events(
    host: Host, globs: Iterable[str], event_names: Iterable[str] = ()
) -> list[str]:
    rows = host.run(
        "find -L /sys/bus/event_source/devices -path '*/events/*' -type f "
        "-printf '%h|%f\\n' 2>/dev/null",
        check=False,
    ).stdout.splitlines()
    events: list[str] = []
    allowed = set(event_names)
    for row in rows:
        if "|" not in row:
            continue
        directory, name = row.split("|", 1)
        parts = Path(directory).parts
        try:
            pmu = parts[parts.index("devices") + 1]
        except (ValueError, IndexError):
            continue
        if (
            name
            and any(fnmatch.fnmatch(pmu, pattern) for pattern in globs)
            and (not allowed or name in allowed)
        ):
            events.append(f"{pmu}/{name}/")
    return sorted(set(events))


def derived_pmu_metrics(samples: Iterable[PerfSample], interval_seconds: float) -> list[dict[str, Any]]:
    grouped: dict[tuple[float, str], dict[str, float]] = defaultdict(dict)
    for sample in samples:
        if sample.value is not None:
            grouped[(sample.interval_s, sample.scope)][sample.event.lower()] = sample.value
    output: list[dict[str, Any]] = []
    for (interval, scope), values in sorted(grouped.items()):
        def find(*suffixes: str) -> float | None:
            for key, value in values.items():
                if any(key == suffix or key.endswith("/" + suffix) for suffix in suffixes):
                    return value
            return None

        cycles = find("cycles", "cpu_cycles")
        instructions = find("instructions")
        cache_refill = find("l2d_cache_refill", "cache_misses", "ll_cache_miss_rd")
        stalls = find("stall_backend", "stalled_cycles_backend")
        mem = find("mem_access")
        remote = find("remote_access")
        frontend_stalls = find("stall_frontend", "stalled_cycles_frontend")
        ddrc_read = sum(
            value
            for key, value in values.items()
            if "ddrc" in key and any(token in key for token in ("flux_rd", "read", "rd_"))
        )
        ddrc_write = sum(
            value
            for key, value in values.items()
            if "ddrc" in key and any(token in key for token in ("flux_wr", "write", "wr_"))
        )
        cross_sccl = sum(
            value
            for key, value in values.items()
            if "hha" in key and any(token in key for token in ("remote", "cross", "outer"))
        )
        metrics = {
            "ipc": instructions / cycles if instructions is not None and cycles else None,
            "cache_refill_per_kinst": 1000 * cache_refill / instructions
            if cache_refill is not None and instructions
            else None,
            "backend_stall_per_cycle": stalls / cycles if stalls is not None and cycles else None,
            "frontend_stall_per_cycle": frontend_stalls / cycles
            if frontend_stalls is not None and cycles
            else None,
            "mem_access_per_s": mem / interval_seconds if mem is not None else None,
            "remote_access_ratio": remote / mem if remote is not None and mem else None,
            "ddrc_read_per_s": ddrc_read / interval_seconds if ddrc_read else None,
            "ddrc_write_per_s": ddrc_write / interval_seconds if ddrc_write else None,
            "cross_sccl_traffic_per_s": cross_sccl / interval_seconds if cross_sccl else None,
        }
        for metric, value in metrics.items():
            output.append(
                {"interval_s": interval, "scope": scope, "metric": metric, "value": value}
            )
    return output


def parse_numastat(text: str) -> dict[int, float]:
    nodes: list[int] = []
    for line in text.splitlines():
        found = [int(value) for value in re.findall(r"Node\s+(\d+)", line)]
        if found:
            nodes = found
            break
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == "Total" and len(fields) >= len(nodes) + 2:
            try:
                return {node: float(fields[index + 1]) for index, node in enumerate(nodes)}
            except ValueError:
                return {}
    return {}


def parse_numa_maps(text: str) -> dict[int, int]:
    pages: dict[int, int] = defaultdict(int)
    for node, count in re.findall(r"(?:^|\s)N(\d+)=(\d+)(?=\s|$)", text):
        pages[int(node)] += int(count)
    return dict(pages)


def expand_cpu_list(value: str) -> set[int]:
    cpus: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            cpus.update(range(int(start), int(end) + 1))
        else:
            cpus.add(int(part))
    return cpus


def parse_cpu_nodes(text: str) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        node, cpulist = line.split(":", 1)
        for cpu in expand_cpu_list(cpulist):
            mapping[cpu] = int(node)
    return mapping


def parse_thread_placement(
    text: str, cpu_nodes: dict[int, int], affinity: str = ""
) -> list[dict[str, Any]]:
    rows = []
    for line in text.splitlines():
        fields = line.split(None, 3)
        if len(fields) != 4 or not all(field.isdigit() for field in fields[:3]):
            continue
        pid, tid, cpu = map(int, fields[:3])
        rows.append(
            {
                "pid": pid,
                "tid": tid,
                "comm": fields[3],
                "cpu": cpu,
                "numa_node": cpu_nodes.get(cpu, -1),
                "affinity": affinity,
            }
        )
    return rows


class SnapshotSampler:
    def __init__(self, host: Host, pids: int | Iterable[int], interval_seconds: int, output: Path):
        self.host = host
        self.pids = [pids] if isinstance(pids, int) else sorted(set(pids))
        self.interval_seconds = interval_seconds
        self.output = output
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.samples: list[dict[str, Any]] = []

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="prism-numa-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval_seconds + 5)
        self.output.parent.mkdir(parents=True, exist_ok=True)
        with self.output.open("w", encoding="utf-8") as stream:
            for sample in self.samples:
                stream.write(json.dumps(sample, sort_keys=True) + "\n")

    def _run(self) -> None:
        topology = self.host.run(
            "for n in /sys/devices/system/node/node[0-9]*; do "
            "test -r \"$n/cpulist\" && printf '%s:%s\\n' \"${n##*node}\" \"$(cat \"$n/cpulist\")\"; done",
            check=False,
        ).stdout
        cpu_nodes = parse_cpu_nodes(topology)
        while not self._stop.is_set():
            epoch = time.time()
            for pid in self.pids:
                numastat = self.host.run(f"numastat -p {pid} 2>/dev/null", check=False).stdout
                numa_maps = self.host.run(f"cat /proc/{pid}/numa_maps 2>/dev/null", check=False).stdout
                placement = self.host.run(
                    f"ps -L -p {pid} -o pid=,lwp=,psr=,comm= 2>/dev/null",
                    check=False,
                ).stdout
                status = self.host.run(
                    f"awk '/^Cpus_allowed_list:/ {{print $2}}' /proc/{pid}/status 2>/dev/null",
                    check=False,
                ).stdout.strip()
                self.samples.append(
                    {
                        "epoch": epoch,
                        "pid": pid,
                        "numastat_mb": parse_numastat(numastat),
                        "numa_pages": parse_numa_maps(numa_maps),
                        "threads": parse_thread_placement(placement, cpu_nodes, status),
                    }
                )
            self._stop.wait(self.interval_seconds)


def merge_sidecars(
    db_path: Path,
    *,
    perf_files: Iterable[tuple[Path, str, float]] = (),
    snapshot_file: Path | None = None,
) -> dict[str, int]:
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE OR REPLACE TABLE pmu_samples (
          ts TIMESTAMP, interval_s DOUBLE, scope VARCHAR, pmu VARCHAR, event VARCHAR,
          raw_value DOUBLE, unit VARCHAR, scale DOUBLE, time_enabled DOUBLE, time_running DOUBLE
        );
        CREATE OR REPLACE TABLE pmu_derived (
          ts TIMESTAMP, interval_s DOUBLE, scope VARCHAR, metric VARCHAR, value DOUBLE
        );
        CREATE OR REPLACE TABLE numa_samples (
          ts TIMESTAMP, pid UINTEGER, node UINTEGER, metric VARCHAR, value DOUBLE, unit VARCHAR
        );
        CREATE OR REPLACE TABLE thread_placement_samples (
          ts TIMESTAMP, pid UINTEGER, tid UINTEGER, comm VARCHAR, cpu INTEGER,
          numa_node INTEGER, affinity VARCHAR
        );
        """
    )
    counts = {"pmu_samples": 0, "pmu_derived": 0, "numa_samples": 0, "thread_placement_samples": 0}
    perf_rows = []
    derived_rows = []
    for path, scope, start_epoch in perf_files:
        if not path.is_file():
            continue
        samples = parse_perf_stat(path.read_text(encoding="utf-8", errors="replace"), scope)
        for sample in samples:
            ts = _duckdb_timestamp(start_epoch + sample.interval_s)
            perf_rows.append({
                "ts": ts, "interval_s": sample.interval_s, "scope": sample.scope,
                "pmu": sample.pmu, "event": sample.event, "raw_value": sample.value,
                "unit": sample.unit, "scale": sample.scale,
                "time_enabled": sample.time_enabled, "time_running": sample.time_running,
            })
        intervals = sorted({sample.interval_s for sample in samples})
        interval = intervals[1] - intervals[0] if len(intervals) > 1 else 10.0
        for row in derived_pmu_metrics(samples, interval):
            ts = _duckdb_timestamp(start_epoch + float(row["interval_s"]))
            derived_rows.append({"ts": ts, **row})
    if perf_rows:
        import pandas as pd

        frame = pd.DataFrame(perf_rows)
        con.register("perf_frame", frame)
        con.execute("INSERT INTO pmu_samples SELECT * FROM perf_frame")
        con.unregister("perf_frame")
        counts["pmu_samples"] = len(perf_rows)
    if derived_rows:
        import pandas as pd

        frame = pd.DataFrame(derived_rows)[["ts", "interval_s", "scope", "metric", "value"]]
        con.register("derived_frame", frame)
        con.execute("INSERT INTO pmu_derived SELECT * FROM derived_frame")
        con.unregister("derived_frame")
        counts["pmu_derived"] = len(derived_rows)
    if snapshot_file and snapshot_file.is_file():
        for line in snapshot_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            sample = json.loads(line)
            ts = _duckdb_timestamp(float(sample["epoch"]))
            for node, value in sample.get("numastat_mb", {}).items():
                con.execute("INSERT INTO numa_samples VALUES (?, ?, ?, 'resident', ?, 'MB')", [ts, sample["pid"], int(node), value])
                counts["numa_samples"] += 1
            for node, value in sample.get("numa_pages", {}).items():
                con.execute("INSERT INTO numa_samples VALUES (?, ?, ?, 'mapped', ?, 'pages')", [ts, sample["pid"], int(node), value])
                counts["numa_samples"] += 1
            for thread in sample.get("threads", []):
                con.execute(
                    "INSERT INTO thread_placement_samples VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [ts, thread["pid"], thread["tid"], thread["comm"], thread["cpu"], thread["numa_node"], thread["affinity"]],
                )
                counts["thread_placement_samples"] += 1
    con.close()
    return counts
