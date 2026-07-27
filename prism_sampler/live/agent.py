from __future__ import annotations

import argparse
import json
import os
import re
import signal
import time
from pathlib import Path
from threading import Event
from typing import Any

from ..relations.groups import GroupRule
from .client import UnixSnapshotClient
from .graph import LiveRelationshipGraph


SUMMARY_SCHEMA = "prism-sampler.live-summary.v1"


def _rule(value: str) -> GroupRule:
    if "=" not in value:
        raise argparse.ArgumentTypeError("group rule must be NAME=REGEX")
    name, pattern = value.split("=", 1)
    if not name or not pattern:
        raise argparse.ArgumentTypeError("group rule must have a name and regex")
    try:
        re.compile(pattern)
    except re.error as exc:
        raise argparse.ArgumentTypeError(f"invalid group regex: {exc}") from exc
    return GroupRule(name, pattern)


def _load_scales(path: Path | None) -> dict[str, float] | None:
    if path is None:
        return None
    values = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(values, dict):
        raise ValueError("scale file must contain a JSON object")
    result = {}
    for name, value in values.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise ValueError(f"invalid scale {name}: {value!r}")
        result[str(name)] = float(value)
    return result


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def run(
    socket_path: Path,
    output_dir: Path,
    *,
    pids: set[int] | None = None,
    rules: list[GroupRule] | None = None,
    horizon_seconds: float = 60.0,
    stability_window_seconds: float = 10.0,
    emit_seconds: float = 10.0,
    minimum_evidence_windows: int = 3,
    fixed_scales: dict[str, float] | None = None,
    connect_timeout_seconds: float = 30.0,
    duration_seconds: float | None = None,
    record_snapshots: bool = True,
    stop: Event | None = None,
) -> dict[str, Any]:
    if emit_seconds <= 0:
        raise ValueError("emit_seconds must be positive")
    output_dir.mkdir(parents=True, exist_ok=True)
    graph = LiveRelationshipGraph(
        pids=pids,
        rules=rules,
        horizon_seconds=horizon_seconds,
        stability_window_seconds=stability_window_seconds,
        minimum_evidence_windows=minimum_evidence_windows,
        fixed_scales=fixed_scales,
    )
    stop = stop or Event()
    client = UnixSnapshotClient(
        socket_path, connect_timeout_seconds=connect_timeout_seconds
    )
    started = time.monotonic()
    snapshots = 0
    emissions = 0
    last_emit_epoch_ms: int | None = None
    last_emitted_sequence: int | None = None
    latest: dict[str, Any] | None = None
    candidates_path = output_dir / "live-candidates.jsonl"
    raw_path = output_dir / "live-stream.jsonl"
    with candidates_path.open("w", encoding="utf-8") as candidate_output:
        raw_context = raw_path.open("wb") if record_snapshots else None
        try:
            for snapshot, raw_line in client.snapshots(stop):
                snapshots += 1
                if raw_context is not None:
                    raw_context.write(raw_line)
                    raw_context.flush()
                graph.ingest(snapshot)
                due = (
                    last_emit_epoch_ms is None
                    or snapshot.window_end_epoch_ms - last_emit_epoch_ms
                    >= emit_seconds * 1000
                )
                if due:
                    latest = graph.score()
                    candidate_output.write(
                        json.dumps(latest, separators=(",", ":"), sort_keys=True) + "\n"
                    )
                    candidate_output.flush()
                    _atomic_json(output_dir / "live-candidates-latest.json", latest)
                    emissions += 1
                    last_emit_epoch_ms = snapshot.window_end_epoch_ms
                    last_emitted_sequence = snapshot.sequence
                if duration_seconds is not None and time.monotonic() - started >= duration_seconds:
                    stop.set()
                    break
        finally:
            if raw_context is not None:
                raw_context.close()
    if graph.windows and graph.windows[-1].sequence != last_emitted_sequence:
        latest = graph.score()
        with candidates_path.open("a", encoding="utf-8") as candidate_output:
            candidate_output.write(
                json.dumps(latest, separators=(",", ":"), sort_keys=True) + "\n"
            )
        _atomic_json(output_dir / "live-candidates-latest.json", latest)
        emissions += 1
    summary = {
        "schema": SUMMARY_SCHEMA,
        "socket": str(socket_path),
        "snapshots": snapshots,
        "emissions": emissions,
        "horizon_seconds": horizon_seconds,
        "stability_window_seconds": stability_window_seconds,
        "emit_seconds": emit_seconds,
        "minimum_evidence_windows": minimum_evidence_windows,
        "record_snapshots": record_snapshots,
        "last_sequence": graph.windows[-1].sequence if graph.windows else None,
        "last_quality": latest["quality"] if latest else None,
        "last_pair_candidates": len(latest["pair_candidates"]) if latest else 0,
        "last_self_candidates": len(latest["self_candidates"]) if latest else 0,
        "scale_mode": latest["scale_mode"] if latest else None,
        "stopped_at_epoch_ns": time.time_ns(),
    }
    _atomic_json(output_dir / "live-summary.json", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prism-live-analyzer")
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--pid", action="append", type=int)
    parser.add_argument("--group-rule", action="append", type=_rule, default=[])
    parser.add_argument("--window-seconds", type=float, default=60.0)
    parser.add_argument("--stability-window-seconds", type=float, default=10.0)
    parser.add_argument("--emit-seconds", type=float, default=10.0)
    parser.add_argument("--minimum-evidence-windows", type=int, default=3)
    parser.add_argument("--scales", type=Path)
    parser.add_argument("--connect-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--no-record-snapshots", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    stop = Event()

    def request_stop(_signum: int, _frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    summary = run(
        args.socket,
        args.output_dir,
        pids=set(args.pid or ()),
        rules=args.group_rule,
        horizon_seconds=args.window_seconds,
        stability_window_seconds=args.stability_window_seconds,
        emit_seconds=args.emit_seconds,
        minimum_evidence_windows=args.minimum_evidence_windows,
        fixed_scales=_load_scales(args.scales),
        connect_timeout_seconds=args.connect_timeout_seconds,
        duration_seconds=args.duration_seconds,
        record_snapshots=not args.no_record_snapshots,
        stop=stop,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
