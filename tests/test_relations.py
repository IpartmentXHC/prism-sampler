from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

from prism_sampler.relations.analyzer import GroupRule, analyze_db, analyze_experiment


def sample_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE taskstats_view(
          ts TIMESTAMP,time_diff UBIGINT,pid UINTEGER,tid UINTEGER,comm VARCHAR,
          run_share DOUBLE,rq_share DOUBLE,interruptible_share DOUBLE,
          uninterruptible_share DOUBLE,blkio_share DOUBLE
        );
        CREATE TABLE futex_wait(
          ts_s TIMESTAMP,pid UINTEGER,tid UINTEGER,futex_key_addr UBIGINT,
          futex_key_word UBIGINT,futex_key_offset UINTEGER,total_requests UBIGINT,total_time UBIGINT
        );
        CREATE TABLE futex_wake(
          ts_s TIMESTAMP,pid UINTEGER,tid UINTEGER,futex_key_addr UBIGINT,
          futex_key_word UBIGINT,futex_key_offset UINTEGER,total_requests UBIGINT,successful_count UBIGINT
        );
        CREATE TABLE vfs(
          ts_s TIMESTAMP,pid UINTEGER,tid UINTEGER,device_id UINTEGER,inode_id UBIGINT,
          total_requests UINTEGER,total_time UBIGINT,total_bytes UBIGINT
        );
        """
    )
    start = datetime(2026, 1, 1)
    for index in range(7):
        ts = start + timedelta(seconds=index * 10)
        for tid, comm, run, rq in (
            (11, "pool-a-1", 0.5, 0.1),
            (12, "pool-b", 0.4, 0.2),
            (13, "pool-c", 0.2, 0.0),
        ):
            con.execute(
                "INSERT INTO taskstats_view VALUES (?,?,?,?,?,?,?,?,?,?)",
                [ts, 10_000_000_000, 10, tid, comm, run, rq, 1-run-rq, 0, 0],
            )
        con.execute(
            "INSERT INTO taskstats_view VALUES (?,?,?,?,?,?,?,?,?,?)",
            [ts, 10_000_000_000, 99, 991, "noise", 1, 0, 0, 0, 0],
        )
    for offset in (12, 22):
        ts = start + timedelta(seconds=offset)
        con.execute("INSERT INTO futex_wait VALUES (?,?,?,?,?,?,?,?)", [ts,10,12,1,2,3,10,10_000_000_000])
        con.execute("INSERT INTO futex_wake VALUES (?,?,?,?,?,?,?,?)", [ts,10,11,1,2,3,3,3])
        con.execute("INSERT INTO futex_wake VALUES (?,?,?,?,?,?,?,?)", [ts,10,13,1,2,3,1,1])
    ts = start + timedelta(seconds=15)
    for tid, requests, nanos, size in ((11,10,1000,100),(12,6,800,80),(13,2,500,50)):
        con.execute("INSERT INTO vfs VALUES (?,?,?,?,?,?,?,?)", [ts,10,tid,1,42,requests,nanos,size])
    con.close()


class RelationTest(unittest.TestCase):
    def test_discovers_groups_and_scores_without_hardcoded_names(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db = root / "sample.db3"
            sample_db(db)
            result = analyze_db(
                db, [10], output=root / "features", warmup=0, tail=0,
                rules=[GroupRule("pool-a", r"^pool-a")],
            )
            groups = pd.read_csv(root / "features/group-activity.csv")
            self.assertEqual(set(groups["group_name"]), {"pool-a", "pool-b", "pool-c"})
            self.assertNotIn("noise", set(groups["group_name"]))
            futex = pd.read_csv(root / "features/futex-directional.csv")
            ab = futex[(futex.waker_group == "pool-a") & (futex.waiter_group == "pool-b")].iloc[0]
            cb = futex[(futex.waker_group == "pool-c") & (futex.waiter_group == "pool-b")].iloc[0]
            self.assertAlmostEqual(ab.attributed_wait_seconds / cb.attributed_wait_seconds, 3.0)
            candidates = pd.read_csv(root / "features/relation-candidates.csv")
            row = candidates[(candidates.group_a == "pool-a") & (candidates.group_b == "pool-b")].iloc[0]
            self.assertAlmostEqual(row.stability, row.window_coverage)
            self.assertGreater(row.synchronization, 0)
            self.assertGreater(row.relationship_score_r, 0)
            self.assertEqual(result["context"]["pids"], [10])

    def test_requires_at_least_one_window_after_trimming(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "sample.db3"
            sample_db(db)
            with self.assertRaisesRegex(ValueError, "too short"):
                analyze_db(db, [10], warmup=30, tail=30)

    def test_experiment_uses_target_clock_workload_bounds(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "runs" / "load" / "r1"
            db = run / "dataset" / "telemetry.db3"
            db.parent.mkdir(parents=True)
            (run / "meta").mkdir()
            sample_db(db)
            start = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
            (run / "meta" / "phase.json").write_text(
                json.dumps({
                    "phase": "load",
                    "round": 1,
                    "target_processes": [{"pid": 10, "start_time": 1}],
                    "workload_clock": "target_realtime",
                    "workload_start_epoch_ns": int((start + 10) * 1e9),
                    "workload_end_epoch_ns": int((start + 40) * 1e9),
                    "target_clock_offset_ns": 123,
                    "target_clock_uncertainty_ns": 10,
                })
            )
            result = analyze_experiment(root, window_seconds=10)
            self.assertEqual(result["runs"], 1)
            self.assertEqual(result["errors"], 0)
            summary = json.loads((run / "features" / "run-summary.json").read_text())
            self.assertEqual(summary["duration_seconds"], 30)
            self.assertEqual(summary["workload_clock"], "target_realtime")


if __name__ == "__main__":
    unittest.main()
