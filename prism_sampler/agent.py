from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any


STOP = False


def _stop(_signum: int, _frame: object) -> None:
    global STOP
    STOP = True


def _read(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _command(argv: list[str]) -> str:
    try:
        return subprocess.run(argv, text=True, capture_output=True, check=False).stdout
    except OSError:
        return ""


def _cpu_nodes() -> dict[int, int]:
    result = {}
    for node_path in Path("/sys/devices/system/node").glob("node[0-9]*"):
        try:
            node = int(node_path.name.removeprefix("node"))
            value = (node_path / "cpulist").read_text().strip()
        except (OSError, ValueError):
            continue
        for item in value.split(","):
            if "-" in item:
                start, end = map(int, item.split("-", 1))
                for cpu in range(start, end + 1):
                    result[cpu] = node
            elif item:
                result[int(item)] = node
    return result


def _cpu_frequency_khz() -> dict[int, int]:
    frequencies = {}
    for cpu_path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*"):
        try:
            cpu = int(cpu_path.name.removeprefix("cpu"))
            value = int((cpu_path / "cpufreq" / "scaling_cur_freq").read_text().strip())
        except (OSError, ValueError):
            continue
        frequencies[cpu] = value
    return frequencies


def _process(pid: int) -> dict[str, Any]:
    status = _read(f"/proc/{pid}/status")
    affinity = ""
    for line in status.splitlines():
        if line.startswith("Cpus_allowed_list:"):
            affinity = line.split(":", 1)[1].strip()
            break
    return {
        "pid": pid,
        "stat": _read(f"/proc/{pid}/stat"),
        "status": status,
        "numa_maps": _read(f"/proc/{pid}/numa_maps"),
        "numastat": _command(["numastat", "-p", str(pid)]),
        "affinity": affinity,
        "threads": _command(["ps", "-L", "-p", str(pid), "-o", "pid=,lwp=,psr=,comm="]),
    }


def snapshot(pids: list[int]) -> dict[str, Any]:
    cpu_nodes = _cpu_nodes()
    return {
        "schema": "prism-sampler.snapshot.v1",
        "realtime_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "loadavg": _read("/proc/loadavg").strip(),
        "proc_stat": _read("/proc/stat"),
        "schedstat": _read("/proc/schedstat"),
        "pressure_cpu": _read("/proc/pressure/cpu"),
        "pressure_memory": _read("/proc/pressure/memory"),
        "pressure_io": _read("/proc/pressure/io"),
        "cpu_nodes": {str(cpu): node for cpu, node in cpu_nodes.items()},
        "cpu_frequency_khz": {
            str(cpu): frequency for cpu, frequency in _cpu_frequency_khz().items()
        },
        "processes": [_process(pid) for pid in pids if Path(f"/proc/{pid}").exists()],
    }


def run(output: Path, pids: list[int], interval: float) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    with output.open("a", encoding="utf-8", buffering=1) as stream:
        while not STOP:
            stream.write(json.dumps(snapshot(pids), sort_keys=True) + "\n")
            stream.flush()
            deadline = time.monotonic() + interval
            while not STOP and time.monotonic() < deadline:
                time.sleep(min(0.2, max(deadline - time.monotonic(), 0)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Target-local Prism Sampler snapshot agent")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pid", action="append", required=True, type=int)
    parser.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args()
    run(args.output, sorted(set(args.pid)), args.interval)


if __name__ == "__main__":
    main()
