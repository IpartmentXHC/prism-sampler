from __future__ import annotations

import math
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass, field
from itertools import combinations
from typing import Any

from ..relations.groups import GroupRule, group_name
from .protocol import LiveSnapshot, StreamStats, TaskstatsRow


PAIR_FORMULA_ID = "sync_share_benefit_live_v1"
SELF_FORMULA_ID = "sync_share_self_benefit_live_v1"
LIVE_CANDIDATE_SCHEMA = "prism-sampler.live-candidates.v1"

ThreadKey = tuple[int, int, int]
PairKey = tuple[str, str]
FutexKey = tuple[int, int, int, int]
VfsKey = tuple[int, int, int, int]


@dataclass(frozen=True)
class _TaskCounter:
    ts_epoch_ms: int
    run_time_total_ns: int
    rq_time_total_ns: int


@dataclass
class _VfsAccess:
    thread: ThreadKey
    group: str
    requests: int = 0
    time_ns: int = 0
    bytes: int = 0


@dataclass
class WindowObservation:
    sequence: int
    start_epoch_ms: int
    end_epoch_ms: int
    duration_seconds: float
    group_run_seconds: dict[str, float] = field(default_factory=dict)
    group_rq_seconds: dict[str, float] = field(default_factory=dict)
    group_threads: dict[str, int] = field(default_factory=dict)
    group_thread_ids: dict[str, set[ThreadKey]] = field(default_factory=dict)
    directed_sync_seconds: dict[tuple[str, str], float] = field(default_factory=dict)
    pair_sharing_seconds: dict[PairKey, float] = field(default_factory=dict)
    self_sharing_seconds: dict[str, float] = field(default_factory=dict)
    pair_relations: set[PairKey] = field(default_factory=set)
    self_relations: set[str] = field(default_factory=set)
    pair_relation_seconds: dict[PairKey, float] = field(default_factory=dict)
    self_relation_seconds: dict[str, float] = field(default_factory=dict)
    mapped_relation_rows: int = 0
    total_relation_rows: int = 0
    missing_snapshots_before: int = 0
    producer_drops_delta: int = 0
    consumer_drops_delta: int = 0
    counter_resets: int = 0

    def active_cpus(self, group: str) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.group_run_seconds.get(group, 0.0) / self.duration_seconds


def _pair(group_a: str, group_b: str) -> PairKey:
    return (group_a, group_b) if group_a < group_b else (group_b, group_a)


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _scale(values: list[float]) -> float:
    return _percentile([math.log1p(max(value, 0.0)) for value in values], 0.95)


def _normalize(value: float, scale: float) -> float:
    return min(math.log1p(max(value, 0.0)) / scale, 1.0) if scale > 0 else 0.0


def _repeatability(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = statistics.fmean(values)
    if mean <= 0:
        return 0.0
    deviation = statistics.stdev(values) if len(values) > 1 else 0.0
    return 1.0 / (1.0 + deviation / mean)


class LiveRelationshipGraph:
    def __init__(
        self,
        *,
        pids: set[int] | None = None,
        rules: list[GroupRule] | None = None,
        horizon_seconds: float = 60.0,
        stability_window_seconds: float = 10.0,
        minimum_evidence_windows: int = 3,
        fixed_scales: dict[str, float] | None = None,
    ):
        if horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if minimum_evidence_windows <= 0:
            raise ValueError("minimum_evidence_windows must be positive")
        if stability_window_seconds <= 0:
            raise ValueError("stability_window_seconds must be positive")
        if stability_window_seconds > horizon_seconds:
            raise ValueError("stability_window_seconds cannot exceed horizon_seconds")
        self.pids = set(pids or ())
        self.rules = list(rules or ())
        self.horizon_seconds = float(horizon_seconds)
        self.stability_window_seconds = float(stability_window_seconds)
        self.minimum_evidence_windows = minimum_evidence_windows
        self.fixed_scales = dict(fixed_scales) if fixed_scales else None
        self.windows: deque[WindowObservation] = deque()
        self._groups: dict[ThreadKey, tuple[str, int]] = {}
        self._task_counters: dict[ThreadKey, _TaskCounter] = {}
        self._last_sequence: int | None = None
        self._last_stream: StreamStats | None = None

    def _target(self, pid: int) -> bool:
        return not self.pids or pid in self.pids

    @staticmethod
    def _thread(row: Any) -> ThreadKey:
        return (int(row.machine_id), int(row.pid), int(row.tid))

    def _lookup_group(self, row: Any) -> str | None:
        value = self._groups.get(self._thread(row))
        return value[0] if value else None

    def _observe_taskstats(
        self, snapshot: LiveSnapshot, observation: WindowObservation
    ) -> None:
        threads: dict[str, set[ThreadKey]] = defaultdict(set)
        for row in snapshot.taskstats:
            if not self._target(row.pid):
                continue
            thread = self._thread(row)
            group = group_name(row.comm, self.rules)
            self._groups[thread] = (group, snapshot.sequence)
            threads[group].add(thread)
            previous = self._task_counters.get(thread)
            current = _TaskCounter(
                row.ts_epoch_ms, row.run_time_total_ns, row.rq_time_total_ns
            )
            self._task_counters[thread] = current
            if previous is None:
                continue
            if (
                current.ts_epoch_ms <= previous.ts_epoch_ms
                or current.run_time_total_ns < previous.run_time_total_ns
                or current.rq_time_total_ns < previous.rq_time_total_ns
            ):
                observation.counter_resets += 1
                continue
            observation.group_run_seconds[group] = (
                observation.group_run_seconds.get(group, 0.0)
                + (current.run_time_total_ns - previous.run_time_total_ns) / 1e9
            )
            observation.group_rq_seconds[group] = (
                observation.group_rq_seconds.get(group, 0.0)
                + (current.rq_time_total_ns - previous.rq_time_total_ns) / 1e9
            )
        observation.group_threads = {
            group: len(values) for group, values in threads.items()
        }
        observation.group_thread_ids = dict(threads)
        expiry = snapshot.sequence - max(10, int(self.horizon_seconds * 2))
        self._groups = {
            thread: value for thread, value in self._groups.items() if value[1] >= expiry
        }
        self._task_counters = {
            thread: value
            for thread, value in self._task_counters.items()
            if thread in self._groups
        }

    def _observe_futex(
        self, snapshot: LiveSnapshot, observation: WindowObservation
    ) -> None:
        waits: dict[FutexKey, list[Any]] = defaultdict(list)
        wakes: dict[FutexKey, list[Any]] = defaultdict(list)
        for row in snapshot.futex_wait:
            if not self._target(row.pid):
                continue
            observation.total_relation_rows += 1
            if self._lookup_group(row) is not None:
                observation.mapped_relation_rows += 1
            key = (
                row.machine_id,
                row.futex_key_addr,
                row.futex_key_word,
                row.futex_key_offset,
            )
            waits[key].append(row)
        for row in snapshot.futex_wake:
            if not self._target(row.pid):
                continue
            observation.total_relation_rows += 1
            if self._lookup_group(row) is not None:
                observation.mapped_relation_rows += 1
            key = (
                row.machine_id,
                row.futex_key_addr,
                row.futex_key_word,
                row.futex_key_offset,
            )
            wakes[key].append(row)
        for key in waits.keys() & wakes.keys():
            total_success = sum(row.successful_count for row in wakes[key])
            if total_success <= 0:
                continue
            for wake in wakes[key]:
                waker = self._lookup_group(wake)
                if waker is None or wake.successful_count <= 0:
                    continue
                for wait in waits[key]:
                    if self._thread(wake) == self._thread(wait):
                        continue
                    waiter = self._lookup_group(wait)
                    if waiter is None:
                        continue
                    attributed = (
                        wait.total_time_ns * wake.successful_count / total_success / 1e9
                    )
                    direction = (waker, waiter)
                    observation.directed_sync_seconds[direction] = (
                        observation.directed_sync_seconds.get(direction, 0.0)
                        + attributed
                    )
                    if waker == waiter:
                        observation.self_relations.add(waker)
                    else:
                        observation.pair_relations.add(_pair(waker, waiter))

    def _observe_vfs(self, snapshot: LiveSnapshot, observation: WindowObservation) -> None:
        accesses: dict[tuple[VfsKey, ThreadKey], _VfsAccess] = {}
        for row in snapshot.vfs:
            if not self._target(row.pid):
                continue
            observation.total_relation_rows += 1
            group = self._lookup_group(row)
            if group is None:
                continue
            observation.mapped_relation_rows += 1
            resource = (row.machine_id, row.fs_magic, row.device_id, row.inode_id)
            thread = self._thread(row)
            access = accesses.setdefault(
                (resource, thread), _VfsAccess(thread=thread, group=group)
            )
            access.requests += row.total_requests
            access.time_ns += row.total_time_ns
            access.bytes += row.total_bytes
        by_resource: dict[VfsKey, list[_VfsAccess]] = defaultdict(list)
        for (resource, _), access in accesses.items():
            by_resource[resource].append(access)
        for resource_accesses in by_resource.values():
            by_group: dict[str, list[_VfsAccess]] = defaultdict(list)
            for access in resource_accesses:
                by_group[access.group].append(access)
            group_totals = {
                group: sum(access.time_ns for access in values) / 1e9
                for group, values in by_group.items()
            }
            degree = len(group_totals)
            for group_a, group_b in combinations(sorted(group_totals), 2):
                pair = (group_a, group_b)
                selective = min(group_totals[group_a], group_totals[group_b]) / max(
                    degree - 1, 1
                )
                observation.pair_sharing_seconds[pair] = (
                    observation.pair_sharing_seconds.get(pair, 0.0) + selective
                )
                observation.pair_relations.add(pair)
            thread_degree = len(resource_accesses)
            for group, values in by_group.items():
                if len(values) < 2:
                    continue
                selective = sum(
                    min(left.time_ns, right.time_ns) / 1e9 / max(thread_degree - 1, 1)
                    for left, right in combinations(values, 2)
                )
                observation.self_sharing_seconds[group] = (
                    observation.self_sharing_seconds.get(group, 0.0) + selective
                )
                observation.self_relations.add(group)

    def ingest(self, snapshot: LiveSnapshot) -> WindowObservation:
        if self._last_sequence is not None and snapshot.sequence <= self._last_sequence:
            raise ValueError(
                f"live sequence is not increasing: {snapshot.sequence} <= {self._last_sequence}"
            )
        missing = (
            max(snapshot.sequence - self._last_sequence - 1, 0)
            if self._last_sequence is not None
            else 0
        )
        producer_delta = 0
        consumer_delta = 0
        if self._last_stream is not None:
            producer_delta = max(
                snapshot.stream.snapshots_dropped
                - self._last_stream.snapshots_dropped,
                0,
            )
            consumer_delta = max(
                snapshot.stream.consumer_snapshots_dropped
                - self._last_stream.consumer_snapshots_dropped,
                0,
            )
        observation = WindowObservation(
            sequence=snapshot.sequence,
            start_epoch_ms=snapshot.window_start_epoch_ms,
            end_epoch_ms=snapshot.window_end_epoch_ms,
            duration_seconds=snapshot.duration_seconds,
            missing_snapshots_before=missing,
            producer_drops_delta=producer_delta,
            consumer_drops_delta=consumer_delta,
        )
        self._observe_taskstats(snapshot, observation)
        self._observe_futex(snapshot, observation)
        self._observe_vfs(snapshot, observation)
        self.windows.append(observation)
        self._last_sequence = snapshot.sequence
        self._last_stream = snapshot.stream
        cutoff = snapshot.window_end_epoch_ms - int(self.horizon_seconds * 1000)
        while self.windows and self.windows[0].end_epoch_ms <= cutoff:
            self.windows.popleft()
        return observation

    def _quality(self) -> dict[str, Any]:
        if not self.windows:
            return {
                "confidence": 0.0,
                "warmup": 0.0,
                "stream_completeness": 0.0,
                "mapping_coverage": 0.0,
                "flags": ["no_snapshots"],
            }
        observed = len(self.windows)
        missing = sum(window.missing_snapshots_before for window in self.windows)
        total_rows = sum(window.total_relation_rows for window in self.windows)
        mapped_rows = sum(window.mapped_relation_rows for window in self.windows)
        span = (
            self.windows[-1].end_epoch_ms - self.windows[0].start_epoch_ms
        ) / 1000.0
        warmup = min(span / self.horizon_seconds, 1.0)
        completeness = observed / (observed + missing) if observed + missing else 0.0
        mapping = mapped_rows / total_rows if total_rows else 1.0
        flags = []
        if warmup < 1.0:
            flags.append("window_warmup")
        if missing:
            flags.append("sequence_gap")
        if any(window.producer_drops_delta for window in self.windows):
            flags.append("producer_drop")
        if any(window.consumer_drops_delta for window in self.windows):
            flags.append("consumer_drop")
        if mapping < 0.95:
            flags.append("low_tid_mapping")
        if any(window.counter_resets for window in self.windows):
            flags.append("taskstats_counter_reset")
        return {
            "confidence": warmup * completeness * mapping,
            "warmup": warmup,
            "stream_completeness": completeness,
            "mapping_coverage": mapping,
            "observed_snapshots": observed,
            "missing_snapshots": missing,
            "mapped_relation_rows": mapped_rows,
            "total_relation_rows": total_rows,
            "producer_drops": sum(
                window.producer_drops_delta for window in self.windows
            ),
            "consumer_drops": sum(
                window.consumer_drops_delta for window in self.windows
            ),
            "counter_resets": sum(window.counter_resets for window in self.windows),
            "flags": flags,
        }

    @staticmethod
    def _merge_values(target: dict[Any, float], source: dict[Any, float]) -> None:
        for key, value in source.items():
            target[key] = target.get(key, 0.0) + value

    def _stability_windows(self) -> list[WindowObservation]:
        bucket_ms = int(self.stability_window_seconds * 1000)
        buckets: dict[int, WindowObservation] = {}
        for window in self.windows:
            bucket_id = max(window.end_epoch_ms - 1, 0) // bucket_ms
            bucket = buckets.setdefault(
                bucket_id,
                WindowObservation(
                    sequence=window.sequence,
                    start_epoch_ms=bucket_id * bucket_ms,
                    end_epoch_ms=(bucket_id + 1) * bucket_ms,
                    duration_seconds=0.0,
                ),
            )
            bucket.sequence = max(bucket.sequence, window.sequence)
            bucket.duration_seconds += window.duration_seconds
            self._merge_values(bucket.group_run_seconds, window.group_run_seconds)
            self._merge_values(bucket.group_rq_seconds, window.group_rq_seconds)
            self._merge_values(bucket.directed_sync_seconds, window.directed_sync_seconds)
            self._merge_values(bucket.pair_sharing_seconds, window.pair_sharing_seconds)
            self._merge_values(bucket.self_sharing_seconds, window.self_sharing_seconds)
            for group, count in window.group_threads.items():
                bucket.group_threads[group] = max(
                    bucket.group_threads.get(group, 0), count
                )
            for group, thread_ids in window.group_thread_ids.items():
                bucket.group_thread_ids.setdefault(group, set()).update(thread_ids)
                bucket.group_threads[group] = len(bucket.group_thread_ids[group])
            bucket.pair_relations.update(window.pair_relations)
            bucket.self_relations.update(window.self_relations)
            for pair in window.pair_relations:
                bucket.pair_relation_seconds[pair] = (
                    bucket.pair_relation_seconds.get(pair, 0.0)
                    + window.duration_seconds
                )
            for group in window.self_relations:
                bucket.self_relation_seconds[group] = (
                    bucket.self_relation_seconds.get(group, 0.0)
                    + window.duration_seconds
                )
        return [buckets[key] for key in sorted(buckets)]

    def _raw_pair_rows(
        self, duration: float, windows: list[WindowObservation]
    ) -> list[dict[str, Any]]:
        groups = set().union(*(window.group_run_seconds for window in windows))
        active = {
            group: sum(window.group_run_seconds.get(group, 0.0) for window in windows)
            / duration
            for group in groups
        }
        runqueue = {
            group: sum(window.group_rq_seconds.get(group, 0.0) for window in windows)
            / duration
            for group in groups
        }
        pairs = set().union(*(window.pair_relations for window in windows))
        rows = []
        for group_a, group_b in sorted(pairs):
            if active.get(group_a, 0.0) <= 0 or active.get(group_b, 0.0) <= 0:
                continue
            sync_ab = sum(
                window.directed_sync_seconds.get((group_a, group_b), 0.0)
                for window in windows
            ) / duration
            sync_ba = sum(
                window.directed_sync_seconds.get((group_b, group_a), 0.0)
                for window in windows
            ) / duration
            sharing = sum(
                window.pair_sharing_seconds.get((group_a, group_b), 0.0)
                for window in windows
            ) / duration
            overlaps = []
            for window in windows:
                left = window.active_cpus(group_a)
                right = window.active_cpus(group_b)
                if left > 0 and right > 0:
                    overlaps.append(min(left, right) / max(left, right))
            rows.append(
                {
                    "group_a": group_a,
                    "group_b": group_b,
                    "active_cpus_a": active[group_a],
                    "active_cpus_b": active[group_b],
                    "runqueue_cpus_a": runqueue[group_a],
                    "runqueue_cpus_b": runqueue[group_b],
                    "activity_raw": math.sqrt(active[group_a] * active[group_b]),
                    "sync_ab_s_per_s": sync_ab,
                    "sync_ba_s_per_s": sync_ba,
                    "sync_raw": max(sync_ab, sync_ba),
                    "sharing_raw": sharing,
                    "active_overlap_ratio": statistics.fmean(overlaps) if overlaps else 0.0,
                    "relation_windows": sum(
                        (group_a, group_b) in window.pair_relations
                        for window in windows
                    ),
                    "relation_covered_seconds": sum(
                        window.pair_relation_seconds.get((group_a, group_b), 0.0)
                        for window in windows
                    ),
                }
            )
        return rows

    def _raw_self_rows(
        self, duration: float, windows: list[WindowObservation]
    ) -> list[dict[str, Any]]:
        groups = set().union(*(window.self_relations for window in windows))
        rows = []
        for group in sorted(groups):
            active = sum(
                window.group_run_seconds.get(group, 0.0) for window in windows
            ) / duration
            if active <= 0:
                continue
            sync = sum(
                window.directed_sync_seconds.get((group, group), 0.0)
                for window in windows
            ) / duration
            sharing = sum(
                window.self_sharing_seconds.get(group, 0.0)
                for window in windows
            ) / duration
            rows.append(
                {
                    "group_name": group,
                    "thread_count": max(
                        2,
                        max(
                            (window.group_threads.get(group, 0) for window in windows),
                            default=0,
                        ),
                    ),
                    "active_cpus": active,
                    "runqueue_cpus": sum(
                        window.group_rq_seconds.get(group, 0.0)
                        for window in windows
                    )
                    / duration,
                    "activity_raw": active,
                    "sync_raw": sync,
                    "sharing_raw": sharing,
                    "relation_windows": sum(
                        group in window.self_relations for window in windows
                    ),
                    "relation_covered_seconds": sum(
                        window.self_relation_seconds.get(group, 0.0)
                        for window in windows
                    ),
                }
            )
        return rows

    def _scales(
        self, pairs: list[dict[str, Any]], self_rows: list[dict[str, Any]]
    ) -> tuple[dict[str, float], str]:
        if self.fixed_scales is not None:
            required = {
                "pair_activity_log_p95",
                "pair_sync_log_p95",
                "pair_sharing_log_p95",
                "self_activity_log_p95",
                "self_sync_log_p95",
                "self_sharing_log_p95",
            }
            missing = sorted(required - self.fixed_scales.keys())
            if missing:
                raise ValueError("fixed scale file is missing: " + ", ".join(missing))
            return self.fixed_scales, "fixed_calibration"
        return (
            {
                "pair_activity_log_p95": _scale([row["activity_raw"] for row in pairs]),
                "pair_sync_log_p95": _scale([row["sync_raw"] for row in pairs]),
                "pair_sharing_log_p95": _scale([row["sharing_raw"] for row in pairs]),
                "self_activity_log_p95": _scale(
                    [row["activity_raw"] for row in self_rows]
                ),
                "self_sync_log_p95": _scale([row["sync_raw"] for row in self_rows]),
                "self_sharing_log_p95": _scale(
                    [row["sharing_raw"] for row in self_rows]
                ),
            },
            "rolling_p95_uncalibrated",
        )

    def _pair_window_signals(
        self,
        group_a: str,
        group_b: str,
        scales: dict[str, float],
        windows: list[WindowObservation],
    ) -> list[float]:
        signals = []
        for window in windows:
            if (group_a, group_b) not in window.pair_relations:
                continue
            left = window.active_cpus(group_a)
            right = window.active_cpus(group_b)
            activity = _normalize(
                math.sqrt(left * right), scales["pair_activity_log_p95"]
            )
            sync = max(
                window.directed_sync_seconds.get((group_a, group_b), 0.0),
                window.directed_sync_seconds.get((group_b, group_a), 0.0),
            ) / window.duration_seconds
            sharing = (
                window.pair_sharing_seconds.get((group_a, group_b), 0.0)
                / window.duration_seconds
            )
            overlap = min(left, right) / max(left, right) if left > 0 and right > 0 else 0.0
            signals.append(
                activity
                * (
                    0.7 * _normalize(sync, scales["pair_sync_log_p95"])
                    + 0.3
                    * _normalize(sharing, scales["pair_sharing_log_p95"])
                    * overlap
                )
            )
        return signals

    def _self_window_signals(
        self,
        group: str,
        scales: dict[str, float],
        windows: list[WindowObservation],
    ) -> list[float]:
        signals = []
        for window in windows:
            if group not in window.self_relations:
                continue
            activity = _normalize(
                window.active_cpus(group), scales["self_activity_log_p95"]
            )
            sync = (
                window.directed_sync_seconds.get((group, group), 0.0)
                / window.duration_seconds
            )
            sharing = (
                window.self_sharing_seconds.get(group, 0.0)
                / window.duration_seconds
            )
            signals.append(
                activity
                * (
                    0.7 * _normalize(sync, scales["self_sync_log_p95"])
                    + 0.3 * _normalize(sharing, scales["self_sharing_log_p95"])
                )
            )
        return signals

    def score(self) -> dict[str, Any]:
        if not self.windows:
            raise ValueError("cannot score an empty live graph")
        duration = sum(window.duration_seconds for window in self.windows)
        if duration <= 0:
            raise ValueError("live graph has no observed duration")
        quality = self._quality()
        stability_windows = self._stability_windows()
        raw_pairs = self._raw_pair_rows(duration, stability_windows)
        raw_self = self._raw_self_rows(duration, stability_windows)
        scales, scale_mode = self._scales(raw_pairs, raw_self)
        pair_candidates = []
        for row in raw_pairs:
            activity = _normalize(row["activity_raw"], scales["pair_activity_log_p95"])
            synchronization = _normalize(row["sync_raw"], scales["pair_sync_log_p95"])
            sharing_norm = _normalize(
                row["sharing_raw"], scales["pair_sharing_log_p95"]
            )
            sharing = sharing_norm * row["active_overlap_ratio"]
            base_signal = activity * (0.7 * synchronization + 0.3 * sharing)
            signals = self._pair_window_signals(
                row["group_a"], row["group_b"], scales, stability_windows
            )
            repeatability = _repeatability(signals)
            coverage = min(row["relation_covered_seconds"] / duration, 1.0)
            stability = coverage * repeatability
            evidence = min(
                row["relation_windows"] / self.minimum_evidence_windows, 1.0
            )
            sync_ab = row["sync_ab_s_per_s"]
            sync_ba = row["sync_ba_s_per_s"]
            total_sync = sync_ab + sync_ba
            waker, waiter = (
                (row["group_a"], row["group_b"])
                if sync_ab >= sync_ba
                else (row["group_b"], row["group_a"])
            )
            pair_candidates.append(
                {
                    "formula_id": PAIR_FORMULA_ID,
                    **row,
                    "activity": activity,
                    "synchronization": synchronization,
                    "sharing": sharing,
                    "dominant_waker": waker,
                    "dominant_waiter": waiter,
                    "direction_share": max(sync_ab, sync_ba) / total_sync
                    if total_sync
                    else 0.0,
                    "window_coverage": coverage,
                    "repeatability": repeatability,
                    "stability": stability,
                    "relationship_score_r": 100.0 * base_signal * stability,
                    "confidence": quality["confidence"] * evidence,
                }
            )
        pair_candidates.sort(
            key=lambda row: (-row["relationship_score_r"], row["group_a"], row["group_b"])
        )
        for rank, row in enumerate(pair_candidates, 1):
            row["rank"] = rank

        self_candidates = []
        for row in raw_self:
            activity = _normalize(row["activity_raw"], scales["self_activity_log_p95"])
            synchronization = _normalize(row["sync_raw"], scales["self_sync_log_p95"])
            sharing = _normalize(row["sharing_raw"], scales["self_sharing_log_p95"])
            base_signal = activity * (0.7 * synchronization + 0.3 * sharing)
            signals = self._self_window_signals(
                row["group_name"], scales, stability_windows
            )
            repeatability = _repeatability(signals)
            coverage = min(row["relation_covered_seconds"] / duration, 1.0)
            stability = coverage * repeatability
            evidence = min(
                row["relation_windows"] / self.minimum_evidence_windows, 1.0
            )
            self_candidates.append(
                {
                    "formula_id": SELF_FORMULA_ID,
                    **row,
                    "activity": activity,
                    "synchronization": synchronization,
                    "sharing": sharing,
                    "window_coverage": coverage,
                    "repeatability": repeatability,
                    "stability": stability,
                    "self_score_r": 100.0 * base_signal * stability,
                    "confidence": quality["confidence"] * evidence,
                }
            )
        self_candidates.sort(
            key=lambda row: (-row["self_score_r"], row["group_name"])
        )
        for rank, row in enumerate(self_candidates, 1):
            row["rank"] = rank

        flags = list(quality["flags"])
        if scale_mode != "fixed_calibration":
            flags.append("uncalibrated_rolling_scales")
        quality["flags"] = flags
        return {
            "schema": LIVE_CANDIDATE_SCHEMA,
            "generated_at_epoch_ms": self.windows[-1].end_epoch_ms,
            "window_start_epoch_ms": self.windows[0].start_epoch_ms,
            "window_end_epoch_ms": self.windows[-1].end_epoch_ms,
            "window_seconds": self.horizon_seconds,
            "stability_window_seconds": self.stability_window_seconds,
            "sequence_start": self.windows[0].sequence,
            "sequence_end": self.windows[-1].sequence,
            "scale_mode": scale_mode,
            "scales": scales,
            "quality": quality,
            "pair_candidates": pair_candidates,
            "self_candidates": self_candidates,
        }
