from __future__ import annotations

import argparse
import json
import os
import signal
import time
from pathlib import Path

from .journal import read_json, write_json
from .metrics import process_start_time
from .runtime import run


def _current(pid: int, start_time: int) -> bool:
    try:
        value = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        return process_start_time(value) == start_time
    except (OSError, ValueError):
        return False


def mark(run_dir: Path, *, active: bool, phase: str) -> dict[str, object]:
    pid_record = read_json(run_dir / "controller.pid.json")
    if not pid_record or not _current(
        int(pid_record.get("pid", 0)), int(pid_record.get("start_time", 0))
    ):
        return {"status": "not_running", "run_dir": str(run_dir)}
    value = {
        "workload_active": active,
        "phase": phase,
        "realtime_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
    }
    write_json(run_dir / "control.json", value)
    return {"status": "marked", **value}


def stop(run_dir: Path, timeout: float = 30.0) -> dict[str, object]:
    record = read_json(run_dir / "controller.pid.json")
    if not record:
        return {"status": "not_running"}
    pid, start = int(record.get("pid", 0)), int(record.get("start_time", 0))
    if not _current(pid, start):
        return {"status": "not_running", "stale_pid": pid}
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while _current(pid, start) and time.monotonic() < deadline:
        time.sleep(0.2)
    if _current(pid, start):
        raise RuntimeError(f"controller did not stop in {timeout}s: {pid}")
    return {"status": "stopped", "pid": pid}


def main() -> None:
    parser = argparse.ArgumentParser(prog="prism-numa-controller")
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("run")
    start.add_argument("--runtime-config", required=True, type=Path)
    marker = commands.add_parser("mark")
    marker.add_argument("--run-dir", required=True, type=Path)
    marker.add_argument("--phase", default="")
    activity = marker.add_mutually_exclusive_group(required=True)
    activity.add_argument("--active", action="store_true")
    activity.add_argument("--inactive", action="store_true")
    stopping = commands.add_parser("stop")
    stopping.add_argument("--run-dir", required=True, type=Path)
    stopping.add_argument("--timeout", type=float, default=30.0)
    status = commands.add_parser("status")
    status.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "run":
        run(args.runtime_config)
        return
    if args.command == "mark":
        result = mark(args.run_dir, active=args.active, phase=args.phase)
    elif args.command == "stop":
        result = stop(args.run_dir, args.timeout)
    else:
        result = read_json(args.run_dir / "status.json", {"status": "not_running"})
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
