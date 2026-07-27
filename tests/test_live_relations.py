from __future__ import annotations

import unittest
import json
import socket
import tempfile
import threading
from dataclasses import asdict
from pathlib import Path

from prism_sampler.live import LiveRelationshipGraph, parse_snapshot
from prism_sampler.live.agent import run


def task(tid: int, comm: str, run: int, rq: int = 0) -> dict[str, object]:
    return {
        "machine_id": 1,
        "ts_epoch_ms": 1000 + run // 1_000_000,
        "pid": 10,
        "tid": tid,
        "comm": comm,
        "nvcsw": 0,
        "nivcsw": 0,
        "run_time_total_ns": run,
        "rq_time_total_ns": rq,
        "rq_count": 0,
        "blkio_time_total_ns": 0,
        "blkio_count": 0,
    }


def snapshot(
    sequence: int,
    rows: list[dict[str, object]],
    *,
    waits: list[dict[str, object]] | None = None,
    wakes: list[dict[str, object]] | None = None,
    vfs: list[dict[str, object]] | None = None,
) -> object:
    start = sequence * 1000
    return parse_snapshot(
        {
            "schema": "prism.live.v1",
            "sequence": sequence,
            "window_start_epoch_ms": start,
            "window_end_epoch_ms": start + 1000,
            "events_seen": len(rows),
            "event_counts": {},
            "taskstats": rows,
            "futex_wait": waits or [],
            "futex_wake": wakes or [],
            "vfs": vfs or [],
            "iowait": [],
            "stream": {
                "queue_capacity": 64,
                "queue_depth": 0,
                "snapshots_enqueued": sequence,
                "snapshots_dropped": 0,
                "consumer_count": 1,
                "consumer_connected": True,
                "consumer_queue_capacity": 8,
                "consumer_snapshots_dropped": 0,
            },
        }
    )


def interval_snapshot(
    sequence: int,
    start_ms: int,
    end_ms: int,
    run_total_ns: int,
    *,
    relation: bool = True,
) -> object:
    rows = [
        task(11, "A", run_total_ns),
        task(12, "B", run_total_ns),
    ]
    for row in rows:
        row["ts_epoch_ms"] = end_ms
    value = json.loads(json.dumps(asdict(snapshot(sequence, rows))))
    value["window_start_epoch_ms"] = start_ms
    value["window_end_epoch_ms"] = end_ms
    if relation:
        duration_scale = (end_ms - start_ms) / 1000
        value["futex_wait"] = [wait(12, int(100_000_000 * duration_scale))]
        value["futex_wake"] = [wake(11)]
    return parse_snapshot(value)


def wait(tid: int, time_ns: int = 200_000_000) -> dict[str, int]:
    return {
        "machine_id": 1,
        "pid": 10,
        "tid": tid,
        "futex_key_addr": 100,
        "futex_key_word": 200,
        "futex_key_offset": 0,
        "total_requests": 2,
        "total_time_ns": time_ns,
    }


def wake(tid: int) -> dict[str, int]:
    return {
        "machine_id": 1,
        "pid": 10,
        "tid": tid,
        "futex_key_addr": 100,
        "futex_key_word": 200,
        "futex_key_offset": 0,
        "total_requests": 2,
        "successful_count": 2,
    }


def access(tid: int, time_ns: int = 50_000_000) -> dict[str, int]:
    return {
        "machine_id": 1,
        "pid": 10,
        "tid": tid,
        "fs_magic": 1,
        "device_id": 2,
        "inode_id": 3,
        "op": 0,
        "total_requests": 1,
        "total_time_ns": time_ns,
        "total_bytes": 4096,
    }


SCALES = {
    "pair_activity_log_p95": 1.0,
    "pair_sync_log_p95": 1.0,
    "pair_sharing_log_p95": 1.0,
    "self_activity_log_p95": 1.0,
    "self_sync_log_p95": 1.0,
    "self_sharing_log_p95": 1.0,
}


class ProtocolTest(unittest.TestCase):
    def test_rejects_unknown_schema(self):
        value = {
            "schema": "prism.live.v2",
        }
        with self.assertRaisesRegex(ValueError, "unsupported live schema"):
            parse_snapshot(value)


class LiveRelationshipGraphTest(unittest.TestCase):
    def graph(self) -> LiveRelationshipGraph:
        return LiveRelationshipGraph(
            pids={10},
            horizon_seconds=2,
            stability_window_seconds=2,
            minimum_evidence_windows=1,
            fixed_scales=SCALES,
        )

    def test_scores_directional_futex_and_shared_vfs_pair(self):
        graph = self.graph()
        graph.ingest(
            snapshot(
                1,
                [task(11, "A", 1_000_000_000), task(12, "B", 500_000_000)],
                waits=[wait(12)],
                wakes=[wake(11)],
                vfs=[access(11), access(12)],
            )
        )
        graph.ingest(
            snapshot(
                2,
                [task(11, "A", 2_000_000_000), task(12, "B", 1_000_000_000)],
                waits=[wait(12)],
                wakes=[wake(11)],
                vfs=[access(11), access(12)],
            )
        )

        result = graph.score()
        candidate = result["pair_candidates"][0]
        self.assertEqual((candidate["group_a"], candidate["group_b"]), ("A", "B"))
        self.assertEqual(candidate["dominant_waker"], "A")
        self.assertEqual(candidate["dominant_waiter"], "B")
        self.assertGreater(candidate["synchronization"], 0)
        self.assertGreater(candidate["sharing"], 0)
        self.assertGreater(candidate["relationship_score_r"], 0)
        self.assertEqual(candidate["confidence"], 1.0)

    def test_scores_intra_group_resource_sharing(self):
        graph = self.graph()
        graph.ingest(
            snapshot(
                1,
                [task(11, "Worker", 500_000_000), task(12, "Worker", 500_000_000)],
                vfs=[access(11), access(12)],
            )
        )
        graph.ingest(
            snapshot(
                2,
                [task(11, "Worker", 1_000_000_000), task(12, "Worker", 1_000_000_000)],
                vfs=[access(11), access(12)],
            )
        )

        result = graph.score()
        candidate = result["self_candidates"][0]
        self.assertEqual(candidate["group_name"], "Worker")
        self.assertEqual(candidate["thread_count"], 2)
        self.assertGreater(candidate["sharing"], 0)
        self.assertGreater(candidate["self_score_r"], 0)

    def test_sequence_gap_reduces_confidence(self):
        graph = self.graph()
        graph.ingest(snapshot(1, [task(11, "A", 100), task(12, "B", 100)]))
        graph.ingest(snapshot(3, [task(11, "A", 200), task(12, "B", 200)]))

        quality = graph.score()["quality"]
        self.assertEqual(quality["missing_snapshots"], 1)
        self.assertIn("sequence_gap", quality["flags"])
        self.assertLess(quality["confidence"], 1.0)

    def test_rejects_stability_window_larger_than_horizon(self):
        with self.assertRaisesRegex(ValueError, "cannot exceed horizon"):
            LiveRelationshipGraph(
                horizon_seconds=5,
                stability_window_seconds=10,
            )

    def test_empty_candidate_set_is_valid_during_warmup(self):
        graph = self.graph()
        graph.ingest(snapshot(1, [task(11, "A", 100)]))
        result = graph.score()
        self.assertEqual(result["pair_candidates"], [])
        self.assertEqual(result["self_candidates"], [])

    def test_self_candidate_counts_short_lived_tids_in_stability_window(self):
        graph = LiveRelationshipGraph(
            pids={10},
            horizon_seconds=4,
            stability_window_seconds=4,
            minimum_evidence_windows=1,
            fixed_scales=SCALES,
        )
        graph.ingest(
            snapshot(
                1,
                [task(11, "Worker", 100), task(12, "Worker", 100)],
            )
        )
        graph.ingest(
            snapshot(
                2,
                [task(11, "Worker", 100_000_100), task(12, "Worker", 100_000_100)],
                vfs=[access(11), access(12)],
            )
        )
        graph.ingest(
            snapshot(
                3,
                [task(13, "Worker", 100), task(14, "Worker", 100)],
            )
        )
        graph.ingest(
            snapshot(
                4,
                [task(13, "Worker", 100_000_100), task(14, "Worker", 100_000_100)],
                vfs=[access(13), access(14)],
            )
        )

        candidate = graph.score()["self_candidates"][0]
        self.assertEqual(candidate["thread_count"], 4)

    def test_stability_is_independent_of_transport_interval(self):
        def score(interval_seconds: int) -> dict[str, object]:
            graph = LiveRelationshipGraph(
                pids={10},
                horizon_seconds=20,
                stability_window_seconds=10,
                minimum_evidence_windows=1,
                fixed_scales=SCALES,
            )
            run_total = 0
            sequence = 0
            for start in range(0, 30, interval_seconds):
                sequence += 1
                run_total += int(0.5e9 * interval_seconds)
                graph.ingest(
                    interval_snapshot(
                        sequence,
                        start * 1000,
                        (start + interval_seconds) * 1000,
                        run_total,
                    )
                )
            return graph.score()["pair_candidates"][0]

        one_second = score(1)
        ten_seconds = score(10)
        for field in (
            "activity",
            "synchronization",
            "window_coverage",
            "repeatability",
            "stability",
            "relationship_score_r",
        ):
            self.assertAlmostEqual(one_second[field], ten_seconds[field], places=9)

    def test_partial_stability_bucket_uses_relation_duration(self):
        graph = LiveRelationshipGraph(
            pids={10},
            horizon_seconds=10,
            stability_window_seconds=10,
            minimum_evidence_windows=1,
            fixed_scales=SCALES,
        )
        run_total = 0
        for sequence in range(1, 11):
            run_total += 500_000_000
            graph.ingest(
                interval_snapshot(
                    sequence,
                    (sequence - 1) * 1000,
                    sequence * 1000,
                    run_total,
                    relation=sequence <= 4,
                )
            )

        candidate = graph.score()["pair_candidates"][0]
        self.assertAlmostEqual(candidate["relation_covered_seconds"], 4.0)
        self.assertAlmostEqual(candidate["window_coverage"], 0.4)


class LiveAgentIntegrationTest(unittest.TestCase):
    def test_consumes_unix_stream_and_writes_shadow_artifacts(self):
        first = snapshot(
            1,
            [task(11, "A", 1_000_000_000), task(12, "B", 500_000_000)],
            waits=[wait(12)],
            wakes=[wake(11)],
        )
        second = snapshot(
            2,
            [task(11, "A", 2_000_000_000), task(12, "B", 1_000_000_000)],
            waits=[wait(12)],
            wakes=[wake(11)],
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            socket_path = root / "events.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(socket_path))
            listener.listen(1)

            def publish() -> None:
                connection, _ = listener.accept()
                with connection:
                    for value in (first, second):
                        connection.sendall(
                            json.dumps(asdict(value), separators=(",", ":")).encode()
                            + b"\n"
                        )
                listener.close()

            thread = threading.Thread(target=publish)
            thread.start()
            summary = run(
                socket_path,
                root / "output",
                pids={10},
                horizon_seconds=2,
                stability_window_seconds=2,
                emit_seconds=10,
                minimum_evidence_windows=1,
                fixed_scales=SCALES,
            )
            thread.join(timeout=5)

            self.assertEqual(summary["snapshots"], 2)
            self.assertGreaterEqual(summary["emissions"], 2)
            latest = json.loads(
                (root / "output/live-candidates-latest.json").read_text()
            )
            self.assertEqual(latest["pair_candidates"][0]["dominant_waker"], "A")
            self.assertTrue((root / "output/live-stream.jsonl").is_file())
            self.assertTrue((root / "output/live-summary.json").is_file())


if __name__ == "__main__":
    unittest.main()
