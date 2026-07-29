from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import time
from pathlib import Path
from typing import Any

from .journal import read_json, write_json
from .kpi import ACK_SCHEMA, kpi_key


class SshKpiChannel:
    def __init__(self, host: str, agent: str, run_dir: str):
        self.host = host
        self.agent = agent
        self.run_dir = run_dir
        self.process: subprocess.Popen[str] | None = None

    def connect(self) -> None:
        self.close()
        command = (
            f"{shlex.quote(self.agent)} ingest-kpi --run-dir "
            f"{shlex.quote(self.run_dir)}"
        )
        self.process = subprocess.Popen(
            ["ssh", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=10",
             "-o", "ServerAliveCountMax=3", self.host, command],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def send(self, value: dict[str, Any]) -> dict[str, Any]:
        if self.process is None or self.process.poll() is not None:
            self.connect()
        assert self.process is not None and self.process.stdin and self.process.stdout
        self.process.stdin.write(json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n")
        self.process.stdin.flush()
        raw = self.process.stdout.readline()
        if not raw:
            detail = self.process.stderr.read().strip() if self.process.stderr else ""
            raise ConnectionError(detail or "KPI SSH channel closed before ACK")
        ack = json.loads(raw)
        if ack.get("schema") != ACK_SCHEMA or ack.get("error"):
            raise RuntimeError(f"KPI receiver rejected row: {ack}")
        return ack

    def close(self) -> None:
        if self.process is None:
            return
        if self.process.stdin:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.process = None


def forward(
    input_path: Path,
    stop_file: Path,
    state_path: Path,
    channel: SshKpiChannel,
    *,
    poll_seconds: float = 0.2,
    reconnect_seconds: float = 1.0,
) -> dict[str, Any]:
    state = read_json(state_path, {"acked": []})
    acked = set(str(item) for item in state.get("acked", []))
    pending: dict[str, dict[str, Any]] = {}
    input_position = 0
    stopped_empty_polls = 0
    sent = duplicates = reconnects = 0
    try:
        while True:
            if input_path.is_file():
                size = input_path.stat().st_size
                if size < input_position:
                    input_position = 0
                with input_path.open("r", encoding="utf-8") as stream:
                    stream.seek(input_position)
                    for raw in stream:
                        value = json.loads(raw)
                        pending[kpi_key(value)] = value
                    input_position = stream.tell()
            progressed = False
            for key in list(pending):
                if key in acked:
                    pending.pop(key)
                    continue
                value = dict(pending[key])
                value["forward_client_epoch_ns"] = time.time_ns()
                try:
                    ack = channel.send(value)
                except (ConnectionError, OSError, RuntimeError, json.JSONDecodeError):
                    reconnects += 1
                    channel.close()
                    time.sleep(reconnect_seconds)
                    break
                if ack.get("duplicate"):
                    duplicates += 1
                sent += 1
                acked.add(key)
                pending.pop(key)
                write_json(state_path, {
                    "schema": "prism-sampler.kpi-forward-state.v1",
                    "acked": sorted(acked),
                    "input_position": input_position,
                    "sent": sent,
                    "duplicates": duplicates,
                    "reconnects": reconnects,
                })
                progressed = True
            if stop_file.exists() and not pending:
                stopped_empty_polls = stopped_empty_polls + 1 if not progressed else 0
                if stopped_empty_polls >= 2:
                    break
            else:
                stopped_empty_polls = 0
            time.sleep(poll_seconds)
    finally:
        channel.close()
    return {"sent": sent, "duplicates": duplicates, "reconnects": reconnects}


def main() -> None:
    parser = argparse.ArgumentParser(prog="prism-kpi-forwarder")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--stop-file", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--host", required=True)
    parser.add_argument("--agent", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--poll", type=float, default=0.2)
    args = parser.parse_args()
    result = forward(
        args.input, args.stop_file, args.state,
        SshKpiChannel(args.host, args.agent, args.run_dir),
        poll_seconds=args.poll,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
