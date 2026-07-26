from __future__ import annotations

import os
import shlex
import subprocess
import time
from pathlib import Path
from typing import Callable

from .metrics import format_cpu_list, parse_cpu_list, process_start_time


Runner = Callable[[str], subprocess.CompletedProcess[str]]


def _run(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-lc", command], text=True, capture_output=True, check=False
    )


class TasksetActuator:
    def __init__(
        self,
        pids: list[int],
        start_times: dict[int, int],
        *,
        proc_root: Path = Path("/proc"),
        command_prefix: str = "",
        runner: Runner = _run,
        retries: int = 3,
    ):
        self.pids = sorted(set(pids))
        self.start_times = dict(start_times)
        self.proc_root = proc_root
        self.command_prefix = command_prefix.strip()
        self.runner = runner
        self.retries = retries
        self.original_affinity = self._affinities()

    def _status_affinity(self, tid: int) -> str:
        value = (self.proc_root / str(tid) / "status").read_text(
            encoding="utf-8", errors="replace"
        )
        for line in value.splitlines():
            if line.startswith("Cpus_allowed_list:"):
                return line.split(":", 1)[1].strip()
        raise RuntimeError(f"affinity is unavailable for TID {tid}")

    def _validate_pids(self) -> None:
        for pid in self.pids:
            value = (self.proc_root / str(pid) / "stat").read_text(
                encoding="utf-8", errors="replace"
            )
            actual = process_start_time(value)
            expected = self.start_times.get(pid)
            if expected is not None and actual != expected:
                raise RuntimeError(f"target PID identity changed: {pid}")

    def _tids(self) -> list[int]:
        result = []
        for pid in self.pids:
            result.extend(
                int(path.name)
                for path in (self.proc_root / str(pid) / "task").glob("[0-9]*")
            )
        return sorted(set(result))

    def _affinities(self) -> dict[int, str]:
        result = {}
        for tid in self._tids():
            try:
                result[tid] = self._status_affinity(tid)
            except OSError:
                continue
        return result

    def _taskset(self, cpus: str, target: int, *, all_tasks: bool) -> None:
        flag = "-apc" if all_tasks else "-pc"
        command = " ".join(
            item
            for item in (
                self.command_prefix,
                "taskset",
                flag,
                shlex.quote(cpus),
                str(target),
            )
            if item
        )
        result = self.runner(command)
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"taskset failed for {target}: {detail}")

    def apply(self, cpus: str) -> dict[str, object]:
        self._validate_pids()
        expected = parse_cpu_list(cpus)
        started = time.time_ns()
        for attempt in range(1, self.retries + 1):
            for pid in self.pids:
                self._taskset(cpus, pid, all_tasks=True)
            mismatches = {}
            for tid in self._tids():
                try:
                    actual = self._status_affinity(tid)
                except OSError:
                    continue
                if parse_cpu_list(actual) != expected:
                    mismatches[str(tid)] = actual
            if not mismatches:
                return {
                    "status": "applied",
                    "cpus": format_cpu_list(expected),
                    "attempts": attempt,
                    "threads": len(self._tids()),
                    "started_realtime_ns": started,
                    "finished_realtime_ns": time.time_ns(),
                }
        raise RuntimeError(f"affinity verification failed: {mismatches}")

    def restore(self) -> dict[str, object]:
        self._validate_pids()
        current = self._tids()
        fallback = next(iter(self.original_affinity.values()), "")
        original_masks = {mask for mask in self.original_affinity.values() if mask}
        if len(original_masks) == 1:
            cpus = next(iter(original_masks))
            expected = parse_cpu_list(cpus)
            for pid in self.pids:
                self._taskset(cpus, pid, all_tasks=True)
            mismatches = {}
            for tid in self._tids():
                try:
                    actual = self._status_affinity(tid)
                except OSError:
                    continue
                if parse_cpu_list(actual) != expected:
                    mismatches[str(tid)] = actual
            if mismatches:
                raise RuntimeError(f"affinity restore verification failed: {mismatches}")
            return {
                "status": "restored",
                "threads": len(self._tids()),
                "cpus": format_cpu_list(expected),
                "method": "uniform-all-tasks",
                "finished_realtime_ns": time.time_ns(),
            }
        restored = 0
        for tid in current:
            cpus = self.original_affinity.get(tid, fallback)
            if not cpus:
                continue
            try:
                self._taskset(cpus, tid, all_tasks=False)
                restored += 1
            except RuntimeError:
                if (self.proc_root / str(tid)).exists():
                    raise
        return {
            "status": "restored",
            "threads": restored,
            "method": "per-thread",
            "finished_realtime_ns": time.time_ns(),
        }


def taskset_preflight(pids: list[int], proc_root: Path = Path("/proc")) -> dict[str, object]:
    details = []
    for pid in pids:
        status = (proc_root / str(pid) / "status").read_text(encoding="utf-8")
        uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
        owner = int(uid_line.split()[1])
        details.append({"pid": pid, "owner_uid": owner, "same_uid": owner == os.geteuid()})
    available = subprocess.run(["bash", "-lc", "command -v taskset"], capture_output=True).returncode == 0
    return {"taskset": available, "targets": details}
