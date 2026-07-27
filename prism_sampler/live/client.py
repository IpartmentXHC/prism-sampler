from __future__ import annotations

import json
import socket
import time
from collections.abc import Iterator
from pathlib import Path
from threading import Event

from .protocol import LiveSnapshot, parse_snapshot


class UnixSnapshotClient:
    def __init__(
        self,
        socket_path: Path,
        *,
        connect_timeout_seconds: float = 30.0,
        max_line_bytes: int = 64 * 1024 * 1024,
    ):
        self.socket_path = socket_path
        self.connect_timeout_seconds = connect_timeout_seconds
        self.max_line_bytes = max_line_bytes

    def snapshots(self, stop: Event | None = None) -> Iterator[tuple[LiveSnapshot, bytes]]:
        stop = stop or Event()
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        deadline = time.monotonic() + self.connect_timeout_seconds
        while True:
            try:
                connection.connect(str(self.socket_path))
                break
            except (FileNotFoundError, ConnectionRefusedError):
                if stop.is_set():
                    connection.close()
                    return
                if time.monotonic() >= deadline:
                    connection.close()
                    raise TimeoutError(f"live socket was not ready: {self.socket_path}")
                time.sleep(0.1)
        try:
            with connection.makefile("rb") as stream:
                while not stop.is_set():
                    line = stream.readline(self.max_line_bytes + 1)
                    if not line:
                        return
                    if len(line) > self.max_line_bytes:
                        raise ValueError(
                            f"live snapshot exceeds {self.max_line_bytes} bytes"
                        )
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"invalid live JSON: {exc}") from exc
                    yield parse_snapshot(payload), line
        finally:
            connection.close()
