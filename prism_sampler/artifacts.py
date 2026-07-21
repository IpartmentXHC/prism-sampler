from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .sidecars import merge_sidecars, parse_numa_maps, parse_numastat, parse_thread_placement


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
            "loadavg", "proc_stat", "schedstat", "pressure_cpu", "pressure_memory", "pressure_io"
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
            for thread in parse_thread_placement(
                process.get("threads", ""),
                {int(cpu): int(node) for cpu, node in sample.get("cpu_nodes", {}).items()},
                process.get("affinity", ""),
            ):
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


def sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
