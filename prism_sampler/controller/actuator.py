from __future__ import annotations

import os
import shlex
import subprocess
import time
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable

from .metrics import format_cpu_list, parse_cpu_list, process_start_time


Runner = Callable[[str], subprocess.CompletedProcess[str]]


def _run(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-lc", command], text=True, capture_output=True, check=False
    )


class ClickHouseSlotsActuator:
    METRICS = ("QueryThread", "GlobalThreadActive", "GlobalThreadScheduled")

    def __init__(
        self,
        config_path: Path,
        client_command: str,
        *,
        preprocessed_config_path: Path | None = None,
        fixed_max_threads: int = 0,
        runner: Runner = _run,
    ):
        self.config_path = config_path
        self.client_command = client_command
        self.preprocessed_config_path = preprocessed_config_path
        self.runner = runner
        self.fixed_max_threads = fixed_max_threads
        self.original = config_path.read_bytes() if config_path.exists() else None
        self.original_slots = self.current_slots()
        if fixed_max_threads:
            actual = int(self._query(
                "SELECT value FROM system.settings WHERE name='max_threads'"
            ))
            if actual != fixed_max_threads:
                raise RuntimeError(
                    f"fixed max_threads verification failed: {actual} != {fixed_max_threads}"
                )

    def _query(self, sql: str) -> str:
        command = f"{self.client_command} --query {shlex.quote(sql)}"
        result = self.runner(command)
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"ClickHouse query failed: {detail}")
        return result.stdout.strip()

    def current_slots(self) -> int:
        if self.preprocessed_config_path is not None:
            root = ET.parse(self.preprocessed_config_path).getroot()
            value = root.findtext("concurrent_threads_soft_limit_num")
            if value is None:
                raise RuntimeError(
                    "concurrent_threads_soft_limit_num is missing from "
                    f"{self.preprocessed_config_path}"
                )
            return int(value)
        value = self._query(
            "SELECT value FROM system.server_settings WHERE "
            "name='concurrent_threads_soft_limit_num'"
        )
        return int(value)

    def _write_slots(self, slots: int) -> None:
        if self.config_path.exists():
            root = ET.parse(self.config_path).getroot()
        else:
            root = ET.Element("clickhouse")
        for name, value in (
            ("concurrent_threads_soft_limit_num", slots),
            ("concurrent_threads_soft_limit_ratio_to_cores", 0),
        ):
            element = root.find(name)
            if element is None:
                element = ET.SubElement(root, name)
            element.text = str(value)
        content = ET.tostring(root, encoding="utf-8", xml_declaration=True) + b"\n"
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.config_path.name}.", dir=self.config_path.parent
        )
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.config_path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def apply(self, slots: int) -> dict[str, object]:
        started = time.time_ns()
        self._write_slots(slots)
        self._query("SYSTEM RELOAD CONFIG")
        actual = self.current_slots()
        if actual != slots:
            raise RuntimeError(f"slot verification failed: {actual} != {slots}")
        return {
            "status": "applied",
            "slots": actual,
            "verification_source": (
                "preprocessed_config"
                if self.preprocessed_config_path is not None
                else "system.server_settings"
            ),
            "started_realtime_ns": started,
            "finished_realtime_ns": time.time_ns(),
        }

    def metrics(self) -> dict[str, float]:
        names = ",".join(f"'{name}'" for name in self.METRICS)
        rows = self._query(
            "SELECT metric || '=' || toString(value) FROM system.metrics "
            f"WHERE metric IN ({names}) ORDER BY metric"
        )
        result: dict[str, float] = {}
        for row in rows.splitlines():
            if "=" in row:
                name, value = row.split("=", 1)
                result[name] = float(value)
        return result

    def restore(self) -> dict[str, object]:
        if self.original is None:
            self.config_path.unlink(missing_ok=True)
        else:
            fd, temporary = tempfile.mkstemp(
                prefix=f".{self.config_path.name}.", dir=self.config_path.parent
            )
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(self.original)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.config_path)
            finally:
                Path(temporary).unlink(missing_ok=True)
        self._query("SYSTEM RELOAD CONFIG")
        actual = self.current_slots()
        if actual != self.original_slots:
            raise RuntimeError(
                f"slot restore verification failed: {actual} != {self.original_slots}"
            )
        return {
            "status": "restored",
            "slots": actual,
            "verification_source": (
                "preprocessed_config"
                if self.preprocessed_config_path is not None
                else "system.server_settings"
            ),
        }


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
