from __future__ import annotations

import io
import json
import threading
import time
from pathlib import Path

from prism_sampler.controller.kpi import KpiStore, ingest_stream
from prism_sampler.controller.kpi_forwarder import forward


def row(sequence: int = 1) -> dict[str, object]:
    return {
        "schema": "yba.realtime-kpi.v1",
        "phase": "C2T2",
        "sequence": sequence,
        "window_start_client_epoch_ns": 1_000_000_000,
        "window_end_client_epoch_ns": 2_000_000_000,
        "throughput_ops_s": 100.0,
        "complete": True,
        "forward_client_epoch_ns": 3_000_000_000,
    }


def test_kpi_store_is_idempotent_and_updates_latest(tmp_path: Path) -> None:
    store = KpiStore(tmp_path)
    first = store.ingest(row())
    second = store.ingest(row())
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert len((tmp_path / "kpi.jsonl").read_text().splitlines()) == 1
    assert json.loads((tmp_path / "kpi-latest.json").read_text())["sequence"] == 1


def test_ingest_stream_returns_error_ack_without_terminating(tmp_path: Path) -> None:
    source = io.StringIO("{}\n" + json.dumps(row()) + "\n")
    output = io.StringIO()
    ingest_stream(tmp_path, source, output)
    acks = [json.loads(line) for line in output.getvalue().splitlines()]
    assert "error" in acks[0]
    assert acks[1]["sequence"] == 1


class FlakyChannel:
    def __init__(self) -> None:
        self.attempts = 0
        self.values: list[dict[str, object]] = []

    def send(self, value: dict[str, object]) -> dict[str, object]:
        self.attempts += 1
        if self.attempts == 1:
            raise ConnectionError("test disconnect")
        self.values.append(value)
        return {
            "schema": "prism-sampler.kpi-ack.v1",
            "phase": value["phase"],
            "sequence": value["sequence"],
            "duplicate": False,
        }

    def close(self) -> None:
        pass


def test_forwarder_retries_unacked_row_and_persists_progress(tmp_path: Path) -> None:
    input_path = tmp_path / "kpi.jsonl"
    stop = tmp_path / "stop"
    state = tmp_path / "state.json"
    input_path.write_text(json.dumps(row()) + "\n")
    stop.touch()
    channel = FlakyChannel()
    result = forward(
        input_path, stop, state, channel,
        poll_seconds=0.001, reconnect_seconds=0.001,
    )
    assert result == {"sent": 1, "duplicates": 0, "reconnects": 1}
    assert len(channel.values) == 1
    assert json.loads(state.read_text())["acked"] == ["C2T2\u00001"]


def test_forwarder_waits_for_file_then_drains_on_stop(tmp_path: Path) -> None:
    input_path = tmp_path / "kpi.jsonl"
    stop = tmp_path / "stop"
    channel = FlakyChannel()
    channel.attempts = 1
    result: dict[str, object] = {}

    def worker() -> None:
        result.update(forward(
            input_path, stop, tmp_path / "state.json", channel,
            poll_seconds=0.005, reconnect_seconds=0.001,
        ))

    thread = threading.Thread(target=worker)
    thread.start()
    time.sleep(0.02)
    input_path.write_text(json.dumps(row()) + "\n")
    stop.touch()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert result["sent"] == 1
