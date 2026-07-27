from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, TypeVar


LIVE_SCHEMA = "prism.live.v1"


@dataclass(frozen=True)
class TaskstatsRow:
    machine_id: int
    ts_epoch_ms: int
    pid: int
    tid: int
    comm: str
    nvcsw: int
    nivcsw: int
    run_time_total_ns: int
    rq_time_total_ns: int
    rq_count: int
    blkio_time_total_ns: int
    blkio_count: int


@dataclass(frozen=True)
class FutexWaitRow:
    machine_id: int
    pid: int
    tid: int
    futex_key_addr: int
    futex_key_word: int
    futex_key_offset: int
    total_requests: int
    total_time_ns: int


@dataclass(frozen=True)
class FutexWakeRow:
    machine_id: int
    pid: int
    tid: int
    futex_key_addr: int
    futex_key_word: int
    futex_key_offset: int
    total_requests: int
    successful_count: int


@dataclass(frozen=True)
class VfsRow:
    machine_id: int
    pid: int
    tid: int
    fs_magic: int
    device_id: int
    inode_id: int
    op: int
    total_requests: int
    total_time_ns: int
    total_bytes: int


@dataclass(frozen=True)
class IoWaitRow:
    machine_id: int
    pid: int
    tid: int
    part0: int
    bdev: int
    op: int
    total_requests: int
    total_time_ns: int
    sector_count: int


@dataclass(frozen=True)
class StreamStats:
    queue_capacity: int
    queue_depth: int
    snapshots_enqueued: int
    snapshots_dropped: int
    consumer_count: int
    consumer_connected: bool
    consumer_queue_capacity: int
    consumer_snapshots_dropped: int


@dataclass(frozen=True)
class LiveSnapshot:
    schema: str
    sequence: int
    window_start_epoch_ms: int
    window_end_epoch_ms: int
    events_seen: int
    event_counts: dict[str, int]
    taskstats: tuple[TaskstatsRow, ...]
    futex_wait: tuple[FutexWaitRow, ...]
    futex_wake: tuple[FutexWakeRow, ...]
    vfs: tuple[VfsRow, ...]
    iowait: tuple[IoWaitRow, ...]
    stream: StreamStats

    @property
    def duration_seconds(self) -> float:
        return (self.window_end_epoch_ms - self.window_start_epoch_ms) / 1000.0


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} must be an object")
    return value


def _integer(value: Mapping[str, Any], name: str, path: str) -> int:
    item = value.get(name)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise ValueError(f"{path}.{name} must be a non-negative integer")
    return item


def _string(value: Mapping[str, Any], name: str, path: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise ValueError(f"{path}.{name} must be a string")
    return item


def _boolean(value: Mapping[str, Any], name: str, path: str) -> bool:
    item = value.get(name)
    if not isinstance(item, bool):
        raise ValueError(f"{path}.{name} must be a boolean")
    return item


T = TypeVar("T")


def _rows(
    value: Mapping[str, Any], name: str, parser: Callable[[Mapping[str, Any], str], T]
) -> tuple[T, ...]:
    items = value.get(name)
    if not isinstance(items, list):
        raise ValueError(f"snapshot.{name} must be an array")
    return tuple(
        parser(_mapping(item, f"snapshot.{name}[{index}]"), f"snapshot.{name}[{index}]")
        for index, item in enumerate(items)
    )


def _taskstats(value: Mapping[str, Any], path: str) -> TaskstatsRow:
    return TaskstatsRow(
        machine_id=_integer(value, "machine_id", path),
        ts_epoch_ms=_integer(value, "ts_epoch_ms", path),
        pid=_integer(value, "pid", path),
        tid=_integer(value, "tid", path),
        comm=_string(value, "comm", path),
        nvcsw=_integer(value, "nvcsw", path),
        nivcsw=_integer(value, "nivcsw", path),
        run_time_total_ns=_integer(value, "run_time_total_ns", path),
        rq_time_total_ns=_integer(value, "rq_time_total_ns", path),
        rq_count=_integer(value, "rq_count", path),
        blkio_time_total_ns=_integer(value, "blkio_time_total_ns", path),
        blkio_count=_integer(value, "blkio_count", path),
    )


def _futex_wait(value: Mapping[str, Any], path: str) -> FutexWaitRow:
    return FutexWaitRow(
        machine_id=_integer(value, "machine_id", path),
        pid=_integer(value, "pid", path),
        tid=_integer(value, "tid", path),
        futex_key_addr=_integer(value, "futex_key_addr", path),
        futex_key_word=_integer(value, "futex_key_word", path),
        futex_key_offset=_integer(value, "futex_key_offset", path),
        total_requests=_integer(value, "total_requests", path),
        total_time_ns=_integer(value, "total_time_ns", path),
    )


def _futex_wake(value: Mapping[str, Any], path: str) -> FutexWakeRow:
    return FutexWakeRow(
        machine_id=_integer(value, "machine_id", path),
        pid=_integer(value, "pid", path),
        tid=_integer(value, "tid", path),
        futex_key_addr=_integer(value, "futex_key_addr", path),
        futex_key_word=_integer(value, "futex_key_word", path),
        futex_key_offset=_integer(value, "futex_key_offset", path),
        total_requests=_integer(value, "total_requests", path),
        successful_count=_integer(value, "successful_count", path),
    )


def _vfs(value: Mapping[str, Any], path: str) -> VfsRow:
    return VfsRow(
        machine_id=_integer(value, "machine_id", path),
        pid=_integer(value, "pid", path),
        tid=_integer(value, "tid", path),
        fs_magic=_integer(value, "fs_magic", path),
        device_id=_integer(value, "device_id", path),
        inode_id=_integer(value, "inode_id", path),
        op=_integer(value, "op", path),
        total_requests=_integer(value, "total_requests", path),
        total_time_ns=_integer(value, "total_time_ns", path),
        total_bytes=_integer(value, "total_bytes", path),
    )


def _iowait(value: Mapping[str, Any], path: str) -> IoWaitRow:
    return IoWaitRow(
        machine_id=_integer(value, "machine_id", path),
        pid=_integer(value, "pid", path),
        tid=_integer(value, "tid", path),
        part0=_integer(value, "part0", path),
        bdev=_integer(value, "bdev", path),
        op=_integer(value, "op", path),
        total_requests=_integer(value, "total_requests", path),
        total_time_ns=_integer(value, "total_time_ns", path),
        sector_count=_integer(value, "sector_count", path),
    )


def parse_snapshot(payload: Any) -> LiveSnapshot:
    value = _mapping(payload, "snapshot")
    schema = _string(value, "schema", "snapshot")
    if schema != LIVE_SCHEMA:
        raise ValueError(f"unsupported live schema: {schema}")
    sequence = _integer(value, "sequence", "snapshot")
    if sequence == 0:
        raise ValueError("snapshot.sequence must be positive")
    start = _integer(value, "window_start_epoch_ms", "snapshot")
    end = _integer(value, "window_end_epoch_ms", "snapshot")
    if end <= start:
        raise ValueError("snapshot window must have positive duration")
    counts_value = _mapping(value.get("event_counts"), "snapshot.event_counts")
    event_counts = {
        str(name): _integer(counts_value, str(name), "snapshot.event_counts")
        for name in counts_value
    }
    stream_value = _mapping(value.get("stream"), "snapshot.stream")
    stream = StreamStats(
        queue_capacity=_integer(stream_value, "queue_capacity", "snapshot.stream"),
        queue_depth=_integer(stream_value, "queue_depth", "snapshot.stream"),
        snapshots_enqueued=_integer(stream_value, "snapshots_enqueued", "snapshot.stream"),
        snapshots_dropped=_integer(stream_value, "snapshots_dropped", "snapshot.stream"),
        consumer_count=_integer(stream_value, "consumer_count", "snapshot.stream"),
        consumer_connected=_boolean(stream_value, "consumer_connected", "snapshot.stream"),
        consumer_queue_capacity=_integer(
            stream_value, "consumer_queue_capacity", "snapshot.stream"
        ),
        consumer_snapshots_dropped=_integer(
            stream_value, "consumer_snapshots_dropped", "snapshot.stream"
        ),
    )
    return LiveSnapshot(
        schema=schema,
        sequence=sequence,
        window_start_epoch_ms=start,
        window_end_epoch_ms=end,
        events_seen=_integer(value, "events_seen", "snapshot"),
        event_counts=event_counts,
        taskstats=_rows(value, "taskstats", _taskstats),
        futex_wait=_rows(value, "futex_wait", _futex_wait),
        futex_wake=_rows(value, "futex_wake", _futex_wake),
        vfs=_rows(value, "vfs", _vfs),
        iowait=_rows(value, "iowait", _iowait),
        stream=stream,
    )
