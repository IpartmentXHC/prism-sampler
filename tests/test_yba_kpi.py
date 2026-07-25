from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from prism_sampler.artifacts import import_yba_kpi, merge_sched_trace


class YbaKpiImportTest(unittest.TestCase):
    def test_merges_target_sched_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db = root / "telemetry.db3"
            con = duckdb.connect(str(db))
            con.execute("CREATE TABLE taskstats(tid UINTEGER,comm VARCHAR)")
            con.execute("INSERT INTO taskstats VALUES (10,'ThreadPool'),(11,'QueryPipelineEx')")
            con.close()
            trace = root / "sched-events.txt"
            trace.write_text(
                " ThreadPool 10/10 100.000000: sched:sched_process_fork: "
                "comm=ThreadPool pid=10 child_comm=ThreadPool child_pid=11\n"
                " ThreadPool 10/10 101.000000: sched:sched_waking: "
                "comm=QueryPipelineEx pid=11 prio=120 target_cpu=001\n"
            )
            clock = root / "sched-events.clock"
            clock.write_text("1100 100\n")
            counts = merge_sched_trace(db, trace, clock)
            assert counts == {"thread_fork_events": 1, "sched_wake_events": 1,
                              "sched_wake_edges": 1}
            con = duckdb.connect(str(db), read_only=True)
            assert con.execute(
                "SELECT parent_comm,child_comm_resolved FROM thread_fork_events"
            ).fetchone() == ("ThreadPool", "QueryPipelineEx")
            assert con.execute(
                "SELECT waker_comm,wakee_comm,wake_count FROM sched_wake_edges"
            ).fetchone() == ("ThreadPool", "QueryPipelineEx", 1)
            con.close()

    def test_imports_phase_and_operation_kpis(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "run"
            phase = root / "phase"
            (run / "dataset").mkdir(parents=True)
            phase.mkdir()
            (phase / "metrics" / "ycsb-raw").mkdir(parents=True)
            con = duckdb.connect(str(run / "dataset" / "telemetry.db3"))
            con.execute("CREATE TABLE marker(value INTEGER)")
            con.close()
            (phase / "summary.csv").write_text(
                "label,clients,threads_per_client,total_threads,throughput,runtime_ms_max,"
                "read_ops,avg_latency,p95_latency,p99_latency,p999_latency,error_count,timeout_count\n"
                "t2,1,2,2,140.5,15000,2100,14,19,22,83,0,0\n"
            )
            (phase / "operation-summary.csv").write_text(
                "label,operation,operations,avg_latency,p95_latency,p99_latency,p999_latency,error_count\n"
                "t2,READ,2100,14,19,22,83,0\n"
            )
            (phase / "metrics" / "ycsb-raw" / "client-1.log").write_text(
                "2026-07-22 17:54:23:792 10 sec: 415 operations; "
                "41.5 current ops/sec; est completion in 1 day\n"
            )
            (phase / "metrics" / "ycsb-raw" / "client-2.log").write_text(
                "2026-07-22 17:54:23:793 10 sec: 390 operations; "
                "39 current ops/sec; est completion in 1 day\n"
            )

            result = import_yba_kpi(run, phase)

            self.assertEqual(result["phase"], "t2")
            con = duckdb.connect(str(run / "dataset" / "telemetry.db3"), read_only=True)
            row = con.execute(
                "SELECT total_threads, throughput_ops_s, p99_latency_us FROM yba_phase_kpi"
            ).fetchone()
            operations = con.execute("SELECT count(*) FROM yba_operation_kpi").fetchone()[0]
            throughput = con.execute(
                "SELECT operations,throughput_ops_s,clients_reporting "
                "FROM yba_throughput_windows"
            ).fetchone()
            con.close()
            self.assertEqual(row, (2, 140.5, 22.0))
            self.assertEqual(operations, 1)
            self.assertEqual(throughput, (805, 80.5, 2))
            report = json.loads((run / "meta" / "kpi.json").read_text())
            self.assertEqual(report["operation_rows"], 1)


if __name__ == "__main__":
    unittest.main()
