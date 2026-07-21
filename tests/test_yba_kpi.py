from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from prism_sampler.artifacts import import_yba_kpi


class YbaKpiImportTest(unittest.TestCase):
    def test_imports_phase_and_operation_kpis(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "run"
            phase = root / "phase"
            (run / "dataset").mkdir(parents=True)
            phase.mkdir()
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

            result = import_yba_kpi(run, phase)

            self.assertEqual(result["phase"], "t2")
            con = duckdb.connect(str(run / "dataset" / "telemetry.db3"), read_only=True)
            row = con.execute(
                "SELECT total_threads, throughput_ops_s, p99_latency_us FROM yba_phase_kpi"
            ).fetchone()
            operations = con.execute("SELECT count(*) FROM yba_operation_kpi").fetchone()[0]
            con.close()
            self.assertEqual(row, (2, 140.5, 22.0))
            self.assertEqual(operations, 1)
            report = json.loads((run / "meta" / "kpi.json").read_text())
            self.assertEqual(report["operation_rows"], 1)


if __name__ == "__main__":
    unittest.main()
