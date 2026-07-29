from __future__ import annotations

import os
import shlex
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from ..config import SamplerConfig
from .config import ControllerConfig
from .integration import remote_controller_dir
from .journal import read_json, write_json
from .metrics import process_start_time


def _pid_start(pid: int) -> int | None:
    try:
        return process_start_time(Path(f"/proc/{pid}/stat").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _record_path(yba_run_dir: Path) -> Path:
    return yba_run_dir / "meta" / "prism-kpi-forwarder.json"


def start_kpi_forwarder(
    config: SamplerConfig,
    controller: ControllerConfig,
    *,
    session: str,
    yba_run_dir: Path,
) -> dict[str, Any]:
    if controller.mode == "off":
        return {"status": "disabled"}
    client = config.section("client")
    command = str(
        client.get(
            "kpi_forwarder_command", "/home/xhc/.local/bin/prism-kpi-forwarder"
        )
    )
    target = remote_controller_dir(config, session)
    record_path = _record_path(yba_run_dir)
    existing = read_json(record_path)
    pid = int(existing.get("pid", 0))
    if pid and _pid_start(pid) == int(existing.get("start_time", -1)):
        return {"status": "already_running", "pid": pid}
    metrics = yba_run_dir / "metrics"
    meta = yba_run_dir / "meta"
    metrics.mkdir(parents=True, exist_ok=True)
    meta.mkdir(parents=True, exist_ok=True)
    args = [
        *shlex.split(command),
        "--input", str(metrics / "realtime-kpi.jsonl"),
        "--stop-file", str(meta / "realtime-kpi.stop"),
        "--state", str(meta / "prism-kpi-forward-state.json"),
        "--host", str(config.target["host"]),
        "--agent", controller.agent_command,
        "--run-dir", target,
    ]
    with (meta / "prism-kpi-forwarder.log").open("ab") as log:
        process = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    start = None
    for _ in range(20):
        start = _pid_start(process.pid)
        if start is not None:
            break
        time.sleep(0.01)
    if start is None:
        raise RuntimeError("KPI forwarder exited before its identity could be recorded")
    value = {
        "schema": "prism-sampler.kpi-forwarder-process.v1",
        "status": "running",
        "pid": process.pid,
        "start_time": start,
        "target_host": str(config.target["host"]),
        "target_run_dir": target,
        "command": args,
    }
    write_json(record_path, value)
    return value


def stop_kpi_forwarder(yba_run_dir: Path, timeout: float = 15.0) -> dict[str, Any]:
    record_path = _record_path(yba_run_dir)
    record = read_json(record_path)
    pid = int(record.get("pid", 0))
    start = int(record.get("start_time", -1))
    if not pid or _pid_start(pid) != start:
        return {"status": "stopped" if pid else "not_started", "pid": pid}
    deadline = time.monotonic() + timeout
    while _pid_start(pid) == start and time.monotonic() < deadline:
        time.sleep(0.1)
    if _pid_start(pid) == start:
        os.killpg(pid, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while _pid_start(pid) == start and time.monotonic() < deadline:
            time.sleep(0.1)
    status = "stopped" if _pid_start(pid) != start else "failed"
    value = {**record, "status": status, "stopped_realtime_ns": time.time_ns()}
    write_json(record_path, value)
    if status == "failed":
        raise RuntimeError(f"KPI forwarder did not stop: {pid}")
    return value
