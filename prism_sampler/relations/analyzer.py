from __future__ import annotations

import json
import math
import re
import statistics
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd


FORMULA_ID = "sync_share_benefit_v2"


@dataclass(frozen=True)
class GroupRule:
    name: str
    pattern: str


def _group_name(comm: str, rules: list[GroupRule]) -> str:
    for rule in rules:
        if re.search(rule.pattern, comm):
            return rule.name
    return comm


def _pid_clause(pids: list[int], alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}pid IN ({','.join(str(pid) for pid in sorted(set(pids)))})"


def _window_expr(column: str, start: float, window_seconds: int) -> str:
    return f"CAST(FLOOR((epoch({column})-{start})/{window_seconds}) AS BIGINT)"


def _prepare_groups(con: Any, pids: list[int], rules: list[GroupRule]) -> None:
    comms = [str(row[0]) for row in con.execute(
        f"SELECT DISTINCT comm FROM taskstats_view WHERE {_pid_clause(pids)} ORDER BY comm"
    ).fetchall()]
    con.execute("CREATE TEMP TABLE comm_groups(comm VARCHAR, group_name VARCHAR)")
    con.executemany(
        "INSERT INTO comm_groups VALUES (?, ?)",
        [(comm, _group_name(comm, rules)) for comm in comms],
    )


def _range(con: Any, pids: list[int], warmup: float, tail: float,
           start: float | None, end: float | None, window_seconds: int) -> tuple[float, float]:
    low, high = con.execute(
        f"SELECT min(epoch(ts)),max(epoch(ts)) FROM taskstats_view WHERE {_pid_clause(pids)}"
    ).fetchone()
    if low is None or high is None:
        raise ValueError("taskstats_view contains no rows for the requested PID")
    selected_start = float(start) if start is not None else float(low) + warmup
    selected_end = float(end) if end is not None else float(high) - tail
    if selected_start < float(low) or selected_end > float(high):
        raise ValueError(
            "analysis interval is outside taskstats range: "
            f"requested=[{selected_start:.6f},{selected_end:.6f}] "
            f"available=[{float(low):.6f},{float(high):.6f}]"
        )
    if selected_end - selected_start < window_seconds:
        raise ValueError(
            f"analysis interval is too short: {selected_end-selected_start:.3f}s; "
            f"at least {window_seconds}s is required"
        )
    return selected_start, selected_end


def _extract_frames(
    db_path: Path,
    pids: list[int],
    *,
    rules: list[GroupRule],
    warmup: float,
    tail: float,
    start: float | None,
    end: float | None,
    window_seconds: int,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "taskstats_view" not in tables:
        con.close()
        raise ValueError("DB3 has no taskstats_view")
    selected_start, selected_end = _range(
        con, pids, warmup, tail, start, end, window_seconds
    )
    duration = selected_end - selected_start
    windows = int(math.ceil(duration / window_seconds))
    _prepare_groups(con, pids, rules)
    target = _pid_clause(pids, "t")
    window = _window_expr("t.ts", selected_start, window_seconds)
    activity = con.execute(
        f"""
        SELECT g.group_name,count(DISTINCT t.tid) AS thread_count,
          sum(t.run_share*t.time_diff/1e9)/? AS active_cpus,
          sum(t.rq_share*t.time_diff/1e9)/? AS runqueue_cpus,
          sum(t.interruptible_share*t.time_diff/1e9)/? AS sleeping_cpus,
          sum(t.blkio_share*t.time_diff/1e9)/? AS blkio_cpus
        FROM taskstats_view t JOIN comm_groups g USING(comm)
        WHERE {target} AND epoch(t.ts)>=? AND epoch(t.ts)<?
        GROUP BY g.group_name ORDER BY g.group_name
        """,
        [duration] * 4 + [selected_start, selected_end],
    ).fetchdf()
    group_windows = con.execute(
        f"""
        SELECT {window} AS window_index,g.group_name,
          sum(t.run_share*t.time_diff/1e9)/{window_seconds} AS active_cpus,
          sum(t.rq_share*t.time_diff/1e9)/{window_seconds} AS runqueue_cpus
        FROM taskstats_view t JOIN comm_groups g USING(comm)
        WHERE {target} AND epoch(t.ts)>=? AND epoch(t.ts)<?
        GROUP BY window_index,g.group_name ORDER BY window_index,g.group_name
        """,
        [selected_start, selected_end],
    ).fetchdf()
    # Map short-lived event TIDs by window first and fall back to their modal group.
    con.execute(
        f"""
        CREATE TEMP TABLE tid_group_windows AS
        SELECT {window} AS window_index,t.pid,t.tid,mode(g.group_name) AS group_name
        FROM taskstats_view t JOIN comm_groups g USING(comm)
        WHERE {target} AND epoch(t.ts)>=? AND epoch(t.ts)<?
        GROUP BY window_index,t.pid,t.tid
        """,
        [selected_start, selected_end],
    )
    con.execute(
        f"""
        CREATE TEMP TABLE tid_groups AS
        SELECT t.pid,t.tid,mode(g.group_name) AS group_name
        FROM taskstats_view t JOIN comm_groups g USING(comm)
        WHERE {target} AND epoch(t.ts)>=? AND epoch(t.ts)<?
        GROUP BY t.pid,t.tid
        """,
        [selected_start, selected_end],
    )
    empty_futex = pd.DataFrame(columns=[
        "waker_group", "waiter_group", "active_windows", "shared_keys",
        "attributed_wait_count", "attributed_wait_seconds", "paired_wake_success",
    ])
    empty_vfs = pd.DataFrame(columns=[
        "group_a", "group_b", "shared_resource_windows", "shared_resources",
        "active_windows", "selective_requests", "selective_seconds", "selective_bytes",
    ])
    relation_windows: set[tuple[int, str, str]] = set()
    unresolved = {"futex_wait": 0, "futex_wake": 0, "vfs": 0}
    futex = empty_futex
    if {"futex_wait", "futex_wake"}.issubset(tables):
        fw = _window_expr("f.ts_s", selected_start, window_seconds)
        futex = con.execute(
            f"""
            WITH waits0 AS (
              SELECT {fw} window_index,f.pid,f.tid,
                f.futex_key_addr,f.futex_key_word,f.futex_key_offset,
                sum(f.total_requests) wait_count,sum(f.total_time)/1e9 wait_seconds
              FROM futex_wait f WHERE {_pid_clause(pids, 'f')}
                AND epoch(f.ts_s)>=? AND epoch(f.ts_s)<?
              GROUP BY window_index,f.pid,f.tid,f.futex_key_addr,f.futex_key_word,f.futex_key_offset
            ), waits AS (
              SELECT w.*,coalesce(x.group_name,a.group_name) waiter_group FROM waits0 w
              LEFT JOIN tid_group_windows x USING(window_index,pid,tid)
              LEFT JOIN tid_groups a USING(pid,tid)
            ), wakes0 AS (
              SELECT {fw} window_index,f.pid,f.tid,
                f.futex_key_addr,f.futex_key_word,f.futex_key_offset,
                sum(f.total_requests) wake_count,sum(f.successful_count) wake_success
              FROM futex_wake f WHERE {_pid_clause(pids, 'f')}
                AND epoch(f.ts_s)>=? AND epoch(f.ts_s)<?
              GROUP BY window_index,f.pid,f.tid,f.futex_key_addr,f.futex_key_word,f.futex_key_offset
            ), wakes AS (
              SELECT w.*,coalesce(x.group_name,a.group_name) waker_group FROM wakes0 w
              LEFT JOIN tid_group_windows x USING(window_index,pid,tid)
              LEFT JOIN tid_groups a USING(pid,tid)
            ), totals AS (
              SELECT window_index,futex_key_addr,futex_key_word,futex_key_offset,
                sum(wake_success) total_success FROM wakes
              GROUP BY window_index,futex_key_addr,futex_key_word,futex_key_offset
            ), attributed AS (
              SELECT w.window_index,w.waker_group,q.waiter_group,
                w.futex_key_addr,w.futex_key_word,w.futex_key_offset,
                q.wait_count*w.wake_success/nullif(t.total_success,0) attributed_wait_count,
                q.wait_seconds*w.wake_success/nullif(t.total_success,0) attributed_wait_seconds,
                w.wake_success
              FROM wakes w JOIN waits q USING(window_index,futex_key_addr,futex_key_word,futex_key_offset)
              JOIN totals t USING(window_index,futex_key_addr,futex_key_word,futex_key_offset)
              WHERE w.waker_group IS NOT NULL AND q.waiter_group IS NOT NULL AND t.total_success>0
            )
            SELECT waker_group,waiter_group,count(DISTINCT window_index) active_windows,
              count(DISTINCT futex_key_addr||'-'||futex_key_word||'-'||futex_key_offset) shared_keys,
              sum(attributed_wait_count) attributed_wait_count,
              sum(attributed_wait_seconds) attributed_wait_seconds,
              sum(wake_success) paired_wake_success
            FROM attributed GROUP BY waker_group,waiter_group
            ORDER BY attributed_wait_seconds DESC
            """,
            [selected_start, selected_end, selected_start, selected_end],
        ).fetchdf()
        rows = con.execute(
            f"""
            WITH e AS (
              SELECT {fw} window_index,f.pid,f.tid FROM futex_wait f
              WHERE {_pid_clause(pids, 'f')} AND epoch(f.ts_s)>=? AND epoch(f.ts_s)<?
              UNION ALL
              SELECT {fw} window_index,f.pid,f.tid FROM futex_wake f
              WHERE {_pid_clause(pids, 'f')} AND epoch(f.ts_s)>=? AND epoch(f.ts_s)<?
            ) SELECT count(*) FROM e LEFT JOIN tid_group_windows x USING(window_index,pid,tid)
              LEFT JOIN tid_groups a USING(pid,tid) WHERE coalesce(x.group_name,a.group_name) IS NULL
            """,
            [selected_start, selected_end, selected_start, selected_end],
        ).fetchone()[0]
        unresolved["futex_wait"] = unresolved["futex_wake"] = int(rows)
        direction_rows = con.execute(
            f"""
            WITH waits AS (
              SELECT {fw} window_index,f.pid,f.tid,f.futex_key_addr,f.futex_key_word,f.futex_key_offset
              FROM futex_wait f WHERE {_pid_clause(pids, 'f')} AND epoch(f.ts_s)>=? AND epoch(f.ts_s)<?
            ), wakes AS (
              SELECT {fw} window_index,f.pid,f.tid,f.futex_key_addr,f.futex_key_word,f.futex_key_offset,
                f.successful_count FROM futex_wake f WHERE {_pid_clause(pids, 'f')}
                AND epoch(f.ts_s)>=? AND epoch(f.ts_s)<? AND f.successful_count>0
            )
            SELECT DISTINCT w.window_index,
              least(coalesce(wx.group_name,wa.group_name),coalesce(qx.group_name,qa.group_name)) group_a,
              greatest(coalesce(wx.group_name,wa.group_name),coalesce(qx.group_name,qa.group_name)) group_b
            FROM wakes w JOIN waits q USING(window_index,futex_key_addr,futex_key_word,futex_key_offset)
            LEFT JOIN tid_group_windows wx ON wx.window_index=w.window_index AND wx.pid=w.pid AND wx.tid=w.tid
            LEFT JOIN tid_groups wa ON wa.pid=w.pid AND wa.tid=w.tid
            LEFT JOIN tid_group_windows qx ON qx.window_index=q.window_index AND qx.pid=q.pid AND qx.tid=q.tid
            LEFT JOIN tid_groups qa ON qa.pid=q.pid AND qa.tid=q.tid
            WHERE coalesce(wx.group_name,wa.group_name) IS NOT NULL
              AND coalesce(qx.group_name,qa.group_name) IS NOT NULL
              AND coalesce(wx.group_name,wa.group_name)<>coalesce(qx.group_name,qa.group_name)
            """,
            [selected_start, selected_end, selected_start, selected_end],
        ).fetchall()
        relation_windows.update((int(i), str(a), str(b)) for i, a, b in direction_rows)
    vfs = empty_vfs
    if "vfs" in tables:
        vw = _window_expr("v.ts_s", selected_start, window_seconds)
        vfs = con.execute(
            f"""
            WITH access0 AS (
              SELECT {vw} window_index,v.pid,v.tid,v.device_id,v.inode_id,
                sum(v.total_requests) AS requests,sum(v.total_time)/1e9 AS time_seconds,
                sum(v.total_bytes) AS bytes
              FROM vfs v WHERE {_pid_clause(pids, 'v')} AND epoch(v.ts_s)>=? AND epoch(v.ts_s)<?
              GROUP BY window_index,v.pid,v.tid,v.device_id,v.inode_id
            ), access AS (
              SELECT a.window_index,coalesce(x.group_name,t.group_name) group_name,
                a.device_id,a.inode_id,sum(a.requests) AS requests,
                sum(a.time_seconds) AS time_seconds,sum(a.bytes) AS bytes
              FROM access0 a LEFT JOIN tid_group_windows x USING(window_index,pid,tid)
              LEFT JOIN tid_groups t USING(pid,tid)
              WHERE coalesce(x.group_name,t.group_name) IS NOT NULL
              GROUP BY 1,2,3,4
            ), degrees AS (
              SELECT window_index,device_id,inode_id,count(*) group_degree FROM access
              GROUP BY window_index,device_id,inode_id
            ), pairs AS (
              SELECT a.window_index,a.group_name group_a,b.group_name group_b,a.device_id,a.inode_id,
                d.group_degree,least(a.requests,b.requests) overlap_requests,
                least(a.time_seconds,b.time_seconds) overlap_seconds,
                least(a.bytes,b.bytes) overlap_bytes
              FROM access a JOIN access b USING(window_index,device_id,inode_id)
              JOIN degrees d USING(window_index,device_id,inode_id)
              WHERE a.group_name<b.group_name
            )
            SELECT group_a,group_b,count(*) shared_resource_windows,
              count(DISTINCT device_id||'-'||inode_id) shared_resources,
              count(DISTINCT window_index) active_windows,
              sum(overlap_requests/greatest(group_degree-1,1)) selective_requests,
              sum(overlap_seconds/greatest(group_degree-1,1)) selective_seconds,
              sum(overlap_bytes/greatest(group_degree-1,1)) selective_bytes
            FROM pairs GROUP BY group_a,group_b ORDER BY selective_seconds DESC
            """,
            [selected_start, selected_end],
        ).fetchdf()
        vfs_windows = con.execute(
            f"""
            WITH access AS (
              SELECT DISTINCT {vw} window_index,coalesce(x.group_name,t.group_name) group_name,
                v.device_id,v.inode_id
              FROM vfs v LEFT JOIN tid_group_windows x
                ON x.window_index={vw} AND x.pid=v.pid AND x.tid=v.tid
              LEFT JOIN tid_groups t ON t.pid=v.pid AND t.tid=v.tid
              WHERE {_pid_clause(pids, 'v')} AND epoch(v.ts_s)>=? AND epoch(v.ts_s)<?
            ) SELECT DISTINCT a.window_index,a.group_name,b.group_name FROM access a JOIN access b
              USING(window_index,device_id,inode_id) WHERE a.group_name<b.group_name
            """,
            [selected_start, selected_end],
        ).fetchall()
        relation_windows.update((int(i), str(a), str(b)) for i, a, b in vfs_windows)
    con.close()
    relation_frame = pd.DataFrame(
        sorted(relation_windows), columns=["window_index", "group_a", "group_b"]
    )
    frames = {
        "group_activity": activity,
        "group_windows": group_windows,
        "futex_directional": futex,
        "vfs_pairs": vfs,
        "relation_windows": relation_frame,
    }
    context = {
        "db": str(db_path), "pids": sorted(set(pids)), "start_epoch": selected_start,
        "end_epoch": selected_end, "duration_seconds": duration,
        "window_seconds": window_seconds, "window_count": windows,
        "unresolved_events": unresolved,
    }
    return frames, context


def _pair_features(frames: dict[str, pd.DataFrame], context: dict[str, Any]) -> pd.DataFrame:
    activity = frames["group_activity"].set_index("group_name")
    futex = frames["futex_directional"]
    vfs = frames["vfs_pairs"]
    windows = frames["group_windows"]
    relation_windows = frames["relation_windows"]
    pairs: set[tuple[str, str]] = set()
    for _, row in futex.iterrows():
        if row["waker_group"] != row["waiter_group"]:
            pairs.add(tuple(sorted((str(row["waker_group"]), str(row["waiter_group"])))))
    for _, row in vfs.iterrows():
        pairs.add((str(row["group_a"]), str(row["group_b"])))
    pivot = windows.pivot_table(index="window_index", columns="group_name", values="active_cpus", aggfunc="sum")
    rows = []
    duration = float(context["duration_seconds"])
    for group_a, group_b in sorted(pairs):
        if group_a not in activity.index or group_b not in activity.index:
            continue
        active_a = float(activity.loc[group_a, "active_cpus"])
        active_b = float(activity.loc[group_b, "active_cpus"])
        if active_a <= 0 or active_b <= 0:
            continue
        directed = futex[
            futex["waker_group"].isin((group_a, group_b))
            & futex["waiter_group"].isin((group_a, group_b))
            & futex["waker_group"].ne(futex["waiter_group"])
        ]
        sync_ab = float(directed[
            directed["waker_group"].eq(group_a) & directed["waiter_group"].eq(group_b)
        ]["attributed_wait_seconds"].sum()) / duration
        sync_ba = float(directed[
            directed["waker_group"].eq(group_b) & directed["waiter_group"].eq(group_a)
        ]["attributed_wait_seconds"].sum()) / duration
        dominant_waker, dominant_waiter = (group_a, group_b) if sync_ab >= sync_ba else (group_b, group_a)
        sync_total = sync_ab + sync_ba
        vrow = vfs[vfs["group_a"].eq(group_a) & vfs["group_b"].eq(group_b)]
        selective_seconds = float(vrow["selective_seconds"].sum()) if not vrow.empty else 0.0
        overlap = 0.0
        if group_a in pivot.columns and group_b in pivot.columns:
            values = pivot[[group_a, group_b]].dropna()
            if not values.empty:
                ratios = values.min(axis=1) / values.max(axis=1).replace(0, float("nan"))
                overlap = float(ratios.fillna(0).mean())
        rw = relation_windows[
            relation_windows["group_a"].eq(group_a) & relation_windows["group_b"].eq(group_b)
        ]
        rows.append({
            "group_a": group_a, "group_b": group_b,
            "active_cpus_a": active_a, "active_cpus_b": active_b,
            "runqueue_cpus_a": float(activity.loc[group_a, "runqueue_cpus"]),
            "runqueue_cpus_b": float(activity.loc[group_b, "runqueue_cpus"]),
            "activity_raw": math.sqrt(active_a * active_b),
            "sync_ab_s_per_s": sync_ab, "sync_ba_s_per_s": sync_ba,
            "sync_raw": max(sync_ab, sync_ba),
            "dominant_waker": dominant_waker, "dominant_waiter": dominant_waiter,
            "direction_share": max(sync_ab, sync_ba) / sync_total if sync_total else 0.0,
            "vfs_selective_seconds": selective_seconds,
            "sharing_raw": selective_seconds / duration,
            "active_overlap_ratio": overlap,
            "relation_active_windows": int(rw["window_index"].nunique()),
            "window_coverage": int(rw["window_index"].nunique()) / int(context["window_count"]),
        })
    return pd.DataFrame(rows)


def _scale(values: pd.Series) -> float:
    logs = values.fillna(0).clip(lower=0).map(math.log1p)
    return float(logs.quantile(0.95)) if len(logs) else 0.0


def _normalize(value: float, scale: float) -> float:
    return min(math.log1p(max(value, 0.0)) / scale, 1.0) if scale > 0 else 0.0


def _score(rows: pd.DataFrame, scales: dict[str, float], *, grouped: bool) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    output = []
    keys = ["phase", "group_a", "group_b"] if grouped else ["group_a", "group_b"]
    for key, values in rows.groupby(keys, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        base = values.copy()
        base["activity_norm"] = base["activity_raw"].map(lambda x: _normalize(float(x), scales["activity_log_p95"]))
        base["sync_norm"] = base["sync_raw"].map(lambda x: _normalize(float(x), scales["sync_log_p95"]))
        base["sharing_norm"] = base["sharing_raw"].map(lambda x: _normalize(float(x), scales["sharing_log_p95"]))
        base["sharing_score"] = base["sharing_norm"] * base["active_overlap_ratio"]
        base["base_signal"] = base["activity_norm"] * (
            0.7 * base["sync_norm"] + 0.3 * base["sharing_score"]
        )
        mean_signal = float(base["base_signal"].mean())
        deviation = float(base["base_signal"].std(ddof=1)) if len(base) > 1 else 0.0
        repeatability = 1.0 / (1.0 + deviation / mean_signal) if mean_signal > 0 else 0.0
        coverage = float(base["window_coverage"].mean())
        stability = coverage * repeatability
        sync_ab = float(base["sync_ab_s_per_s"].mean())
        sync_ba = float(base["sync_ba_s_per_s"].mean())
        group_a = str(base.iloc[0]["group_a"])
        group_b = str(base.iloc[0]["group_b"])
        waker, waiter = (group_a, group_b) if sync_ab >= sync_ba else (group_b, group_a)
        total = sync_ab + sync_ba
        row = {
            "formula_id": FORMULA_ID,
            **{name: value for name, value in zip(keys, key)},
            "runs": len(base),
            "active_cpus_a": float(base["active_cpus_a"].mean()),
            "active_cpus_b": float(base["active_cpus_b"].mean()),
            "runqueue_cpus_a": float(base["runqueue_cpus_a"].mean()),
            "runqueue_cpus_b": float(base["runqueue_cpus_b"].mean()),
            "activity_raw": float(base["activity_raw"].mean()),
            "activity": float(base["activity_norm"].mean()),
            "sync_ab_s_per_s": sync_ab, "sync_ba_s_per_s": sync_ba,
            "sync_raw": float(base["sync_raw"].mean()),
            "synchronization": float(base["sync_norm"].mean()),
            "dominant_waker": waker, "dominant_waiter": waiter,
            "direction_share": max(sync_ab, sync_ba) / total if total else 0.0,
            "sharing_raw": float(base["sharing_raw"].mean()),
            "active_overlap_ratio": float(base["active_overlap_ratio"].mean()),
            "sharing": float(base["sharing_score"].mean()),
            "window_coverage": coverage, "repeatability": repeatability,
            "stability": stability,
            "relationship_score_r": 100.0 * mean_signal * stability,
        }
        output.append(row)
    result = pd.DataFrame(output)
    rank_group = ["phase"] if grouped else None
    result["rank"] = (
        result.groupby(rank_group)["relationship_score_r"].rank(method="min", ascending=False)
        if rank_group else result["relationship_score_r"].rank(method="min", ascending=False)
    )
    return result.sort_values((["phase"] if grouped else []) + ["rank", "group_a", "group_b"])


def _write_frames(output: Path, frames: dict[str, pd.DataFrame]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in frames.items():
        frame.to_csv(output / f"{name.replace('_', '-')}.csv", index=False)
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        return
    with pd.ExcelWriter(output / "relation-analysis.xlsx", engine="openpyxl") as writer:
        for name, frame in frames.items():
            frame.head(1_000_000).to_excel(writer, sheet_name=name[:31], index=False)


def analyze_db(
    db_path: Path,
    pids: list[int],
    *,
    output: Path | None = None,
    rules: list[GroupRule] | None = None,
    warmup: float = 30,
    tail: float = 5,
    start: float | None = None,
    end: float | None = None,
    window_seconds: int = 10,
) -> dict[str, Any]:
    output = output or db_path.parent / "features"
    frames, context = _extract_frames(
        db_path, pids, rules=rules or [], warmup=warmup, tail=tail,
        start=start, end=end, window_seconds=window_seconds,
    )
    pairs = _pair_features(frames, context)
    scales = {
        "activity_log_p95": _scale(pairs["activity_raw"]) if not pairs.empty else 0.0,
        "sync_log_p95": _scale(pairs["sync_raw"]) if not pairs.empty else 0.0,
        "sharing_log_p95": _scale(pairs["sharing_raw"]) if not pairs.empty else 0.0,
    }
    candidates = _score(pairs, scales, grouped=False)
    result_frames = {**frames, "pair_features": pairs, "relation_candidates": candidates}
    _write_frames(output, result_frames)
    (output / "relation-scales.json").write_text(json.dumps(scales, indent=2, sort_keys=True) + "\n")
    (output / "run-summary.json").write_text(json.dumps(context, indent=2, sort_keys=True) + "\n")
    return {"context": context, "scales": scales, "candidates": len(candidates), "output": str(output)}


def analyze_experiment(experiment: Path, *, window_seconds: int = 10) -> dict[str, Any]:
    runs: list[pd.DataFrame] = []
    errors = []
    for db_path in sorted(experiment.glob("runs/**/dataset/telemetry.db3")):
        run_dir = db_path.parents[1]
        phase_path = run_dir / "meta" / "phase.json"
        try:
            context_meta = json.loads(phase_path.read_text())
            pids = [int(row["pid"]) for row in context_meta.get("target_processes", [])]
            if not pids:
                raise ValueError("phase metadata has no target PID")
            if context_meta.get("workload_clock") != "target_realtime":
                raise ValueError("phase metadata has no target-realtime workload boundary")
            frames, context = _extract_frames(
                db_path, pids, rules=[], warmup=0, tail=0,
                start=(float(context_meta["workload_start_epoch_ns"]) / 1e9
                       if context_meta.get("workload_start_epoch_ns") else None),
                end=(float(context_meta["workload_end_epoch_ns"]) / 1e9
                     if context_meta.get("workload_end_epoch_ns") else None),
                window_seconds=window_seconds,
            )
            pairs = _pair_features(frames, context)
            phase = str(context_meta.get("phase") or run_dir.parent.name)
            round_number = int(context_meta.get("round") or run_dir.name.removeprefix("r") or 1)
            pairs["phase"] = phase
            pairs["round"] = round_number
            runs.append(pairs)
            _write_frames(run_dir / "features", {**frames, "pair_features": pairs})
            run_summary = {
                **context,
                "phase": phase,
                "round": round_number,
                "workload_clock": context_meta["workload_clock"],
                "target_clock_offset_ns": context_meta.get("target_clock_offset_ns"),
                "target_clock_uncertainty_ns": context_meta.get(
                    "target_clock_uncertainty_ns"
                ),
                "clock_offset_source": context_meta.get("clock_offset_source", "phase_probe"),
            }
            (run_dir / "features" / "run-summary.json").write_text(
                json.dumps(run_summary, indent=2, sort_keys=True) + "\n"
            )
        except Exception as exc:
            errors.append({"db": str(db_path), "error": f"{type(exc).__name__}: {exc}"})
    all_pairs = pd.concat(runs, ignore_index=True) if runs else pd.DataFrame()
    scales = {
        "activity_log_p95": _scale(all_pairs["activity_raw"]) if not all_pairs.empty else 0.0,
        "sync_log_p95": _scale(all_pairs["sync_raw"]) if not all_pairs.empty else 0.0,
        "sharing_log_p95": _scale(all_pairs["sharing_raw"]) if not all_pairs.empty else 0.0,
    }
    candidates = _score(all_pairs, scales, grouped=True) if not all_pairs.empty else pd.DataFrame()
    summary = experiment / "summary"
    summary.mkdir(parents=True, exist_ok=True)
    all_pairs.to_csv(summary / "pair-features.csv", index=False)
    candidates.to_csv(summary / "relation-candidates.csv", index=False)
    pd.DataFrame(errors, columns=["db", "error"]).to_csv(summary / "analysis-errors.csv", index=False)
    (summary / "relation-scales.json").write_text(json.dumps(scales, indent=2, sort_keys=True) + "\n")
    return {"runs": len(runs), "errors": len(errors), "candidates": len(candidates), "summary": str(summary)}
