from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .journal import append_jsonl, read_json, write_json


KPI_SCHEMA = "yba.realtime-kpi.v1"
ACK_SCHEMA = "prism-sampler.kpi-ack.v1"


def kpi_key(value: dict[str, Any]) -> str:
    if value.get("schema") != KPI_SCHEMA:
        raise ValueError(f"unsupported KPI schema: {value.get('schema')}")
    phase = str(value.get("phase", ""))
    sequence = value.get("sequence")
    if not phase or isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
        raise ValueError("KPI phase and positive sequence are required")
    return f"{phase}\0{sequence}"


class KpiStore:
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.index_path = run_dir / "kpi-index.json"
        self.index = read_json(self.index_path, {"keys": []})
        self.keys = set(str(item) for item in self.index.get("keys", []))

    def ingest(self, value: dict[str, Any]) -> dict[str, Any]:
        key = kpi_key(value)
        duplicate = key in self.keys
        if not duplicate:
            received = time.time_ns()
            sender = int(value.get("forward_client_epoch_ns") or received)
            offset = received - sender
            stored = dict(value)
            stored.update(
                received_target_epoch_ns=received,
                estimated_client_to_target_offset_ns=offset,
                window_start_target_epoch_ns=(
                    int(value["window_start_client_epoch_ns"]) + offset
                ),
                window_end_target_epoch_ns=(
                    int(value["window_end_client_epoch_ns"]) + offset
                ),
                receive_lag_seconds=max(
                    0.0,
                    (received - (int(value["window_end_client_epoch_ns"]) + offset))
                    / 1e9,
                ),
            )
            append_jsonl(self.run_dir / "kpi.jsonl", stored)
            write_json(self.run_dir / "kpi-latest.json", stored)
            self.keys.add(key)
            write_json(self.index_path, {"schema": ACK_SCHEMA, "keys": sorted(self.keys)})
        return {
            "schema": ACK_SCHEMA,
            "phase": str(value["phase"]),
            "sequence": int(value["sequence"]),
            "duplicate": duplicate,
        }


def ingest_stream(run_dir: Path, input_stream: Any, output_stream: Any) -> None:
    store = KpiStore(run_dir)
    for raw in input_stream:
        try:
            value = json.loads(raw)
            ack = store.ingest(value)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            ack = {"schema": ACK_SCHEMA, "error": f"{type(exc).__name__}: {exc}"}
        output_stream.write(json.dumps(ack, separators=(",", ":"), sort_keys=True) + "\n")
        output_stream.flush()
