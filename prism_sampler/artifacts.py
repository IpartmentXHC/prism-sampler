from __future__ import annotations

import csv
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .sidecars import merge_sidecars, parse_numa_maps, parse_numastat, parse_thread_placement


_YCSB_STATUS = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3}) "
    r"(?P<elapsed>\d+) sec: (?P<operations>\d+) operations; "
    r"(?P<throughput>[0-9.]+) current ops/sec"
)
_SCHED_LINE = re.compile(
    r"^\s*(?P<current_comm>.*?)\s+(?P<pid>\d+)/(?P<tid>\d+)\s+"
    r"(?P<time>[0-9.]+):\s+sched:sched_(?P<event>process_fork|waking):\s+"
    r"(?P<trace>.*)$"
)


def parse_ycsb_throughput_windows(phase_dir: Path) -> list[dict[str, object]]:
    grouped: dict[int, dict[str, object]] = {}
    for path in sorted((phase_dir / "metrics" / "ycsb-raw").glob("client-*.log")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = _YCSB_STATUS.match(line)
            if not match:
                continue
            elapsed = int(match.group("elapsed"))
            row = grouped.setdefault(elapsed, {
                "elapsed_seconds": elapsed, "operations": 0,
                "throughput_ops_s": 0.0, "clients_reporting": 0,
                "timestamp": match.group("timestamp"),
            })
            row["operations"] = int(row["operations"]) + int(match.group("operations"))
            row["throughput_ops_s"] = (
                float(row["throughput_ops_s"]) + float(match.group("throughput"))
            )
            row["clients_reporting"] = int(row["clients_reporting"]) + 1
    return [grouped[key] for key in sorted(grouped)]


def _trace_fields(value: str) -> dict[str, str]:
    return dict(re.findall(r"([A-Za-z_]+)=([^ ]+)", value))


def merge_sched_trace(db_path: Path, text_path: Path, clock_path: Path) -> dict[str, int]:
    import duckdb
    import pandas as pd

    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE OR REPLACE TABLE thread_fork_events (
          ts TIMESTAMP, parent_tid UINTEGER, parent_comm VARCHAR,
          child_tid UINTEGER, child_comm_initial VARCHAR, child_comm_resolved VARCHAR
        );
        CREATE OR REPLACE TABLE sched_wake_events (
          ts TIMESTAMP, waker_tid UINTEGER, waker_comm VARCHAR,
          wakee_tid UINTEGER, wakee_comm VARCHAR, target_cpu INTEGER
        );
        CREATE OR REPLACE TABLE sched_wake_edges (
          waker_comm VARCHAR, wakee_comm VARCHAR, wake_count UBIGINT,
          unique_wakers UBIGINT, unique_wakees UBIGINT
        );
        """
    )
    if not text_path.is_file() or not clock_path.is_file():
        con.close()
        return {"thread_fork_events": 0, "sched_wake_events": 0, "sched_wake_edges": 0}
    realtime, uptime = map(float, clock_path.read_text().split()[:2])
    boot_epoch = realtime - uptime
    target_tids = {int(row[0]) for row in con.execute("SELECT DISTINCT tid FROM taskstats").fetchall()}
    comm_by_tid = {
        int(tid): str(comm) for tid, comm in con.execute(
            "SELECT tid,mode(comm) FROM taskstats GROUP BY tid"
        ).fetchall()
    }
    forks = []
    wakes = []
    for line in text_path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = _SCHED_LINE.match(line)
        if not match:
            continue
        fields = _trace_fields(match.group("trace"))
        current_tid = int(match.group("tid"))
        ts = _duckdb_timestamp(boot_epoch + float(match.group("time")))
        if match.group("event") == "process_fork":
            child = int(fields.get("child_pid", "0"))
            parent = int(fields.get("pid", str(current_tid)))
            if parent not in target_tids and child not in target_tids:
                continue
            forks.append({
                "ts": ts, "parent_tid": parent,
                "parent_comm": fields.get("comm", match.group("current_comm")).strip(),
                "child_tid": child, "child_comm_initial": fields.get("child_comm", ""),
                "child_comm_resolved": comm_by_tid.get(child, fields.get("child_comm", "")),
            })
        else:
            wakee = int(fields.get("pid", "0"))
            if current_tid not in target_tids and wakee not in target_tids:
                continue
            wakes.append({
                "ts": ts, "waker_tid": current_tid,
                "waker_comm": match.group("current_comm").strip(),
                "wakee_tid": wakee, "wakee_comm": fields.get("comm", ""),
                "target_cpu": int(fields.get("target_cpu", "-1")),
            })
    for table, rows in (("thread_fork_events", forks), ("sched_wake_events", wakes)):
        if rows:
            frame = pd.DataFrame(rows)
            con.register("sched_frame", frame)
            con.execute(f'INSERT INTO "{table}" SELECT * FROM sched_frame')
            con.unregister("sched_frame")
    con.execute(
        """
        INSERT INTO sched_wake_edges
        SELECT waker_comm,wakee_comm,count(*),count(DISTINCT waker_tid),count(DISTINCT wakee_tid)
        FROM sched_wake_events GROUP BY waker_comm,wakee_comm
        """
    )
    edges = int(con.execute("SELECT count(*) FROM sched_wake_edges").fetchone()[0])
    con.close()
    return {"thread_fork_events": len(forks), "sched_wake_events": len(wakes),
            "sched_wake_edges": edges}


def _duckdb_timestamp(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).replace(tzinfo=None)


def validate_raw(run_dir: Path) -> dict[str, Any]:
    import duckdb

    db = run_dir / "raw" / "collector.db3"
    if not db.is_file() or db.stat().st_size == 0:
        raise RuntimeError(f"collector DB3 is missing or empty: {db}")
    con = duckdb.connect(str(db), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    counts = {
        table: int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
        for table in ("taskstats", "taskstats_view", "futex_wait", "futex_wake", "vfs")
        if table in tables
    }
    con.close()
    if counts.get("taskstats", 0) <= 0 or counts.get("taskstats_view", 0) <= 0:
        raise RuntimeError("collector DB3 has no taskstats samples")
    value = {
        "schema": "prism-sampler.raw-health.v1", "raw_db": str(db),
        "raw_db_sha256": sha256(db), "table_rows": counts,
    }
    meta = run_dir / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / "raw-health.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return value


def _agent_snapshots(db_path: Path, snapshot_path: Path) -> dict[str, int]:
    import duckdb

    counts = {"system_pressure_samples": 0, "numa_samples": 0, "thread_placement_samples": 0}
    if not snapshot_path.is_file():
        return counts
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE OR REPLACE TABLE system_pressure_samples (
          ts TIMESTAMP, monotonic_ns UBIGINT, metric VARCHAR, value VARCHAR
        );
        CREATE TABLE IF NOT EXISTS numa_samples (
          ts TIMESTAMP, pid UINTEGER, node UINTEGER, metric VARCHAR, value DOUBLE, unit VARCHAR
        );
        CREATE TABLE IF NOT EXISTS thread_placement_samples (
          ts TIMESTAMP, pid UINTEGER, tid UINTEGER, comm VARCHAR, cpu INTEGER,
          numa_node INTEGER, affinity VARCHAR
        );
        """
    )
    system_rows = []
    numa_rows = []
    placement_rows = []
    for line in snapshot_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        sample = json.loads(line)
        ts = _duckdb_timestamp(float(sample["realtime_ns"]) / 1e9)
        mono = int(sample["monotonic_ns"])
        for metric in (
            "loadavg", "proc_stat", "schedstat", "pressure_cpu", "pressure_memory", "pressure_io",
            "cpu_frequency_khz",
        ):
            system_rows.append({
                "ts": ts, "monotonic_ns": mono, "metric": metric,
                "value": sample.get(metric, ""),
            })
        for process in sample.get("processes", []):
            pid = int(process["pid"])
            for node, value in parse_numastat(process.get("numastat", "")).items():
                numa_rows.append({
                    "ts": ts, "pid": pid, "node": node, "metric": "resident",
                    "value": value, "unit": "MB",
                })
            for node, value in parse_numa_maps(process.get("numa_maps", "")).items():
                numa_rows.append({
                    "ts": ts, "pid": pid, "node": node, "metric": "mapped",
                    "value": value, "unit": "pages",
                })
            thread_affinities = process.get("thread_affinities", {})
            for thread in parse_thread_placement(
                process.get("threads", ""),
                {int(cpu): int(node) for cpu, node in sample.get("cpu_nodes", {}).items()},
                process.get("affinity", ""),
            ):
                thread["affinity"] = thread_affinities.get(
                    str(thread["tid"]), thread["affinity"]
                )
                placement_rows.append({
                    "ts": ts, "pid": thread["pid"], "tid": thread["tid"],
                    "comm": thread["comm"], "cpu": thread["cpu"],
                    "numa_node": thread["numa_node"], "affinity": thread["affinity"],
                })
    import pandas as pd

    for table, rows in (
        ("system_pressure_samples", system_rows),
        ("numa_samples", numa_rows),
        ("thread_placement_samples", placement_rows),
    ):
        if not rows:
            continue
        frame = pd.DataFrame(rows)
        con.register("snapshot_frame", frame)
        con.execute(f'INSERT INTO "{table}" SELECT * FROM snapshot_frame')
        con.unregister("snapshot_frame")
        counts[table] = len(rows)
    con.close()
    return counts


def finalize_run(run_dir: Path, phase_context: dict[str, Any]) -> dict[str, Any]:
    raw = run_dir / "raw"
    dataset = run_dir / "dataset"
    meta = run_dir / "meta"
    dataset.mkdir(parents=True, exist_ok=True)
    meta.mkdir(parents=True, exist_ok=True)
    source = raw / "collector.db3"
    target = dataset / "telemetry.db3"
    if not source.is_file() or source.stat().st_size == 0:
        raise RuntimeError(f"collector DB3 is missing or empty: {source}")
    Path(str(target) + ".wal").unlink(missing_ok=True)
    shutil.copy2(source, target)
    perf_files = []
    for name, scope in (("perf-core.csv", "process"), ("perf-uncore.csv", "system")):
        path = raw / name
        start = raw / f"{name}.start"
        if path.is_file() and start.is_file():
            perf_files.append((path, scope, float(start.read_text().strip())))
    merged = merge_sidecars(target, perf_files=perf_files)
    for key, value in _agent_snapshots(target, raw / "system-pressure.jsonl").items():
        merged[key] = merged.get(key, 0) + value
    for key, value in merge_sched_trace(
        target, raw / "sched-events.txt", raw / "sched-events.clock"
    ).items():
        merged[key] = value
    import duckdb

    con = duckdb.connect(str(target))
    con.execute(
        """
        CREATE OR REPLACE TABLE phase_markers (
          event VARCHAR, realtime_ns UBIGINT, monotonic_ns UBIGINT,
          phase VARCHAR, round INTEGER, context_json VARCHAR
        )
        """
    )
    for event in phase_context.get("events", []):
        con.execute(
            "INSERT INTO phase_markers VALUES (?, ?, ?, ?, ?, ?)",
            [event["event"], event["realtime_ns"], event["monotonic_ns"],
             phase_context.get("phase", ""), int(phase_context.get("round", 1)),
             json.dumps(event, sort_keys=True)],
        )
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    counts = {
        table: int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
        for table in tables
    }
    con.close()
    health = {
        "schema": "prism-sampler.finalize.v1",
        "telemetry": str(target),
        "raw_db_sha256": sha256(source),
        "merged_rows": merged,
        "table_rows": counts,
        "required_tables": {
            "taskstats": counts.get("taskstats", 0) > 0,
            "taskstats_view": counts.get("taskstats_view", 0) > 0,
        },
    }
    (meta / "health.json").write_text(json.dumps(health, indent=2, sort_keys=True) + "\n")
    return health


def import_yba_kpi(run_dir: Path, phase_dir: Path) -> dict[str, Any]:
    """Import authoritative YBA phase KPIs into the derived telemetry DB."""
    import duckdb
    import pandas as pd

    telemetry = run_dir / "dataset" / "telemetry.db3"
    summary_path = phase_dir / "summary.csv"
    operation_path = phase_dir / "operation-summary.csv"
    if not telemetry.is_file() or not summary_path.is_file():
        raise RuntimeError(f"YBA KPI import inputs are missing for {run_dir}")

    with summary_path.open(newline="", encoding="utf-8") as stream:
        phase_rows = list(csv.DictReader(stream))
    if len(phase_rows) != 1:
        raise RuntimeError(f"expected one YBA phase summary row: {summary_path}")
    operation_rows: list[dict[str, str]] = []
    if operation_path.is_file():
        with operation_path.open(newline="", encoding="utf-8") as stream:
            operation_rows = list(csv.DictReader(stream))

    phase = phase_rows[0]
    phase_frame = pd.DataFrame([{
        "phase": phase.get("label", ""),
        "clients": int(float(phase.get("clients") or 0)),
        "threads_per_client": int(float(phase.get("threads_per_client") or 0)),
        "total_threads": int(float(phase.get("total_threads") or 0)),
        "operations": float(phase.get("read_ops") or 0),
        "throughput_ops_s": float(phase.get("throughput") or 0),
        "runtime_seconds": float(phase.get("runtime_ms_max") or 0) / 1000.0,
        "avg_latency_us": float(phase.get("avg_latency") or 0),
        "p95_latency_us": float(phase.get("p95_latency") or 0),
        "p99_latency_us": float(phase.get("p99_latency") or 0),
        "p999_latency_us": float(phase.get("p999_latency") or 0),
        "error_count": float(phase.get("error_count") or 0),
        "timeout_count": float(phase.get("timeout_count") or 0),
    }])
    operation_frame = pd.DataFrame([{
        "phase": row.get("label", ""),
        "operation": row.get("operation", ""),
        "operations": float(row.get("operations") or 0),
        "avg_latency_us": float(row.get("avg_latency") or 0),
        "p95_latency_us": float(row.get("p95_latency") or 0),
        "p99_latency_us": float(row.get("p99_latency") or 0),
        "p999_latency_us": float(row.get("p999_latency") or 0),
        "error_count": float(row.get("error_count") or 0),
    } for row in operation_rows])
    throughput_frame = pd.DataFrame(parse_ycsb_throughput_windows(phase_dir))

    con = duckdb.connect(str(telemetry))
    con.register("phase_kpi_frame", phase_frame)
    con.execute("CREATE OR REPLACE TABLE yba_phase_kpi AS SELECT * FROM phase_kpi_frame")
    con.unregister("phase_kpi_frame")
    con.execute(
        """
        CREATE OR REPLACE TABLE yba_operation_kpi (
          phase VARCHAR, operation VARCHAR, operations DOUBLE,
          avg_latency_us DOUBLE, p95_latency_us DOUBLE, p99_latency_us DOUBLE,
          p999_latency_us DOUBLE, error_count DOUBLE
        )
        """
    )
    if not operation_frame.empty:
        con.register("operation_kpi_frame", operation_frame)
        con.execute("INSERT INTO yba_operation_kpi SELECT * FROM operation_kpi_frame")
        con.unregister("operation_kpi_frame")
    con.execute(
        """
        CREATE OR REPLACE TABLE yba_throughput_windows (
          elapsed_seconds INTEGER, operations UBIGINT, throughput_ops_s DOUBLE,
          clients_reporting INTEGER, timestamp VARCHAR
        )
        """
    )
    if not throughput_frame.empty:
        throughput_frame = throughput_frame[[
            "elapsed_seconds", "operations", "throughput_ops_s",
            "clients_reporting", "timestamp",
        ]]
        con.register("throughput_frame", throughput_frame)
        con.execute("INSERT INTO yba_throughput_windows SELECT * FROM throughput_frame")
        con.unregister("throughput_frame")
    con.close()

    report = {
        "schema": "prism-sampler.yba-kpi.v1",
        "phase": phase.get("label", ""),
        "phase_rows": 1,
        "operation_rows": len(operation_rows),
        "throughput_window_rows": len(throughput_frame),
        "sources": {
            "summary": {"path": str(summary_path), "sha256": sha256(summary_path)},
            "operations": (
                {"path": str(operation_path), "sha256": sha256(operation_path)}
                if operation_path.is_file() else None
            ),
        },
    }
    meta = run_dir / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / "kpi.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    health_path = meta / "health.json"
    if health_path.is_file():
        health = json.loads(health_path.read_text())
        health["yba_kpi"] = {"phase_rows": 1, "operation_rows": len(operation_rows)}
        health.setdefault("table_rows", {})["yba_phase_kpi"] = 1
        health["table_rows"]["yba_operation_kpi"] = len(operation_rows)
        health["table_rows"]["yba_throughput_windows"] = len(throughput_frame)
        health_path.write_text(json.dumps(health, indent=2, sort_keys=True) + "\n")
    return report


def sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
