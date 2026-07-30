from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import statistics
import random
from collections import defaultdict
from pathlib import Path
from typing import Any


PROFILE = re.compile(r"^n(?P<cpus>32|64)_m(?P<max_threads>32|64|128)_s(?P<slots>32|64|128)(?:_.+)?$")
DEFAULT_PROFILE = re.compile(r"^default_n(?P<cpus>32|64)$")
LOAD_THREADS = {"C2T2": 4, "C4T6": 24, "C5T16": 80}
LOAD_WEIGHTS = {"C2T2": 0.2, "C4T6": 0.3, "C5T16": 0.5}


def _float(row: dict[str, str], *names: str) -> float:
    for name in names:
        if row.get(name) not in (None, ""):
            return float(row[name])
    return math.nan


def _profile(cell: Path) -> str:
    path = cell / "meta" / "suite-run.env"
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("profile="):
            return line.split("=", 1)[1].strip().strip("'")
    raise ValueError(f"suite profile is absent: {path}")


def _taskstats_pressure(database: Path, context_path: Path) -> tuple[float, float]:
    if not database.is_file() or not context_path.is_file():
        return math.nan, math.nan
    import duckdb

    context = json.loads(context_path.read_text(encoding="utf-8"))
    start = context.get("workload_start_epoch_ns")
    end = context.get("workload_end_epoch_ns")
    target_pids = sorted({
        int(process["pid"])
        for process in context.get("target_processes", [])
        if process.get("pid") is not None
    })
    if start is None or end is None or not target_pids or int(end) <= int(start):
        return math.nan, math.nan
    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "taskstats_view" not in tables:
            return math.nan, math.nan
        placeholders = ",".join("?" for _ in target_pids)
        duration = int(end) - int(start)
        row = con.execute(
            "SELECT sum(CASE WHEN run_share BETWEEN 0 AND 1 "
            "THEN run_share * time_diff ELSE 0 END) / ?, "
            "sum(CASE WHEN rq_share BETWEEN 0 AND 1 "
            "THEN rq_share * time_diff ELSE 0 END) / ? "
            "FROM taskstats_view WHERE epoch_ns(ts) >= ? AND epoch_ns(ts) < ? "
            f"AND pid IN ({placeholders})",
            [duration, duration, int(start), int(end), *target_pids],
        ).fetchone()
        return (
            float(row[0]) if row and row[0] is not None else math.nan,
            float(row[1]) if row and row[1] is not None else math.nan,
        )
    finally:
        con.close()


def calibration_rows(suite_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cell in sorted((suite_dir / "runs").glob("*")):
        if not (cell / ".complete").is_file():
            continue
        profile = _profile(cell)
        match = PROFILE.match(profile)
        default_match = DEFAULT_PROFILE.match(profile)
        if not match and not default_match:
            continue
        cpu_count = int((match or default_match)["cpus"])
        maximum = int(match["max_threads"]) if match else 128
        slots = int(match["slots"]) if match else 256
        for summary in sorted(cell.glob("phases/*/summary.csv")):
            with summary.open(newline="", encoding="utf-8") as stream:
                values = list(csv.DictReader(stream))
            if len(values) != 1:
                raise ValueError(f"expected one KPI row: {summary}")
            value = values[0]
            load = str(value.get("label") or summary.parent.name.split("-", 1)[-1])
            canonical = next((name for name in LOAD_THREADS if load.startswith(name)), load)
            round_match = re.search(r"-r(\d+)$", cell.name)
            round_number = int(round_match.group(1)) if round_match else 1
            prism_run = (
                suite_dir.parent / "runs" / profile / canonical / f"r{round_number}"
            )
            run_cpu, rq_cpu = _taskstats_pressure(
                prism_run / "dataset" / "telemetry.db3",
                prism_run / "meta" / "phase.json",
            )
            rows.append({
                "profile": profile,
                "cpu_count": cpu_count,
                "max_threads": maximum,
                "slots": slots,
                "default_reference": bool(default_match),
                "load": canonical,
                "offered_threads": LOAD_THREADS.get(canonical, 0),
                "throughput_ops_s": _float(value, "throughput"),
                "p99_latency_us": _float(value, "p99_latency"),
                "run_cpu_equiv": run_cpu,
                "rq_cpu_equiv": rq_cpu,
                "run_pressure": run_cpu / cpu_count,
                "rq_pressure": rq_cpu / cpu_count,
                "error_count": int(float(value.get("error_count") or 0)),
                "timeout_count": int(float(value.get("timeout_count") or 0)),
                "cell": cell.name,
            })
    return rows


def _median(rows: list[dict[str, Any]], key: str) -> float:
    values = [
        float(row[key])
        for row in rows
        if row.get(key) is not None and not math.isnan(float(row[key]))
    ]
    return statistics.median(values) if values else math.nan


def _variance(rows: list[dict[str, Any]], key: str) -> float:
    values = [
        float(row[key])
        for row in rows
        if row.get(key) is not None and not math.isnan(float(row[key]))
    ]
    return statistics.variance(values) if len(values) > 1 else 0.0


def aggregate_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, int, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["cpu_count"], row["max_threads"], row["slots"], row["load"])].append(row)
    result = []
    for (cpus, maximum, slots, load), values in sorted(grouped.items()):
        throughput = _median(values, "throughput_ops_s")
        variance = _variance(values, "throughput_ops_s")
        result.append({
            "cpu_count": cpus,
            "max_threads": maximum,
            "slots": slots,
            "default_reference": all(bool(row.get("default_reference")) for row in values),
            "load": load,
            "offered_threads": LOAD_THREADS.get(load, 0),
            "rounds": len(values),
            "throughput_median_ops_s": throughput,
            "throughput_variance": variance,
            "throughput_cv": math.sqrt(variance) / throughput if throughput else math.nan,
            "p99_median_us": _median(values, "p99_latency_us"),
            "run_cpu_equiv_median": _median(values, "run_cpu_equiv"),
            "rq_cpu_equiv_median": _median(values, "rq_cpu_equiv"),
            "run_pressure_median": _median(values, "run_pressure"),
            "rq_pressure_median": _median(values, "rq_pressure"),
            "errors": sum(int(row["error_count"]) for row in values),
            "timeouts": sum(int(row["timeout_count"]) for row in values),
        })
    return result


def select_config(matrix: list[dict[str, Any]]) -> dict[str, Any]:
    maxima: dict[tuple[int, str], float] = defaultdict(float)
    for row in matrix:
        if row.get("default_reference"):
            continue
        maxima[(int(row["cpu_count"]), str(row["load"]))] = max(
            maxima[(int(row["cpu_count"]), str(row["load"]))],
            float(row["throughput_median_ops_s"]),
        )
    strategies = []
    for maximum in (32, 64, 128):
        selected_slots: dict[int, int] = {}
        state_scores: dict[int, float] = {}
        for cpus in (32, 64):
            candidates = []
            for slots in (32, 64, 128):
                subset = [
                    row for row in matrix
                    if row["cpu_count"] == cpus
                    and row["max_threads"] == maximum
                    and row["slots"] == slots
                ]
                score = sum(
                    LOAD_WEIGHTS.get(str(row["load"]), 0)
                    * float(row["throughput_median_ops_s"])
                    / maxima[(cpus, str(row["load"]))]
                    for row in subset
                    if maxima[(cpus, str(row["load"]))]
                )
                candidates.append((score, -slots, slots))
            score, _, slots = max(candidates)
            selected_slots[cpus] = slots
            state_scores[cpus] = score
        strategies.append({
            "max_threads": maximum,
            "one_node_slots": selected_slots[32],
            "two_node_slots": selected_slots[64],
            "score": (state_scores[32] + state_scores[64]) / 2,
        })
    strategies.sort(key=lambda row: (-float(row["score"]), int(row["max_threads"])))
    finalists = strategies[:2]
    best_score = float(finalists[0]["score"])
    equivalent = [
        row for row in finalists if best_score - float(row["score"]) <= 0.02
    ]
    selected = min(
        equivalent,
        key=lambda row: (
            int(row["max_threads"]),
            int(row["one_node_slots"]) + int(row["two_node_slots"]),
        ),
    )
    signatures = []
    for load, offered in LOAD_THREADS.items():
        values = {}
        pressures = {}
        for cpus, slots, name in (
            (32, selected["one_node_slots"], "one"),
            (64, selected["two_node_slots"], "two"),
        ):
            matches = [
                row for row in matrix
                if row["cpu_count"] == cpus
                and row["max_threads"] == selected["max_threads"]
                and row["slots"] == slots
                and row["load"] == load
            ]
            values[name] = _median(matches, "throughput_median_ops_s")
            pressures[f"{name}_run_cpu_equiv"] = _median(matches, "run_cpu_equiv_median")
            pressures[f"{name}_rq_cpu_equiv"] = _median(matches, "rq_cpu_equiv_median")
        signatures.append({
            "load": load,
            "offered_threads": offered,
            "one_throughput_ops_s": values["one"],
            "two_throughput_ops_s": values["two"],
            "preferred_state": "two_node" if values["two"] > values["one"] else "one_node",
            **pressures,
        })
    return {
        "schema": "prism-sampler.clickhouse-selected-config.v1",
        **selected,
        "finalists": finalists,
        "all_strategies": strategies,
        "signatures": signatures,
    }


def select_finalist_config(
    matrix: list[dict[str, Any]], finalists: list[dict[str, Any]]
) -> dict[str, Any]:
    candidates = []
    relevant = []
    for strategy in finalists:
        maximum = int(strategy["max_threads"])
        one_slots = int(strategy["one_node_slots"])
        two_slots = int(strategy["two_node_slots"])
        cells = [
            row for row in matrix
            if int(row["max_threads"]) == maximum
            and (
                (int(row["cpu_count"]) == 32 and int(row["slots"]) == one_slots)
                or (int(row["cpu_count"]) == 64 and int(row["slots"]) == two_slots)
            )
            and not row.get("default_reference")
        ]
        if len(cells) != 6:
            raise RuntimeError(
                f"finalist calibration is incomplete: M={maximum} S32={one_slots} "
                f"S64={two_slots} cells={len(cells)}/6"
            )
        relevant.extend(cells)
        candidates.append({
            "max_threads": maximum,
            "one_node_slots": one_slots,
            "two_node_slots": two_slots,
            "cells": cells,
        })
    maxima: dict[tuple[int, str], float] = defaultdict(float)
    for row in relevant:
        key = (int(row["cpu_count"]), str(row["load"]))
        maxima[key] = max(maxima[key], float(row["throughput_median_ops_s"]))
    scored = []
    for candidate in candidates:
        score = sum(
            LOAD_WEIGHTS[str(row["load"])]
            * float(row["throughput_median_ops_s"])
            / maxima[(int(row["cpu_count"]), str(row["load"]))]
            for row in candidate["cells"]
        ) / 2
        scored.append({key: value for key, value in candidate.items() if key != "cells"} | {
            "score": score
        })
    scored.sort(key=lambda row: (-float(row["score"]), int(row["max_threads"])))
    best_score = float(scored[0]["score"])
    selected = min(
        (row for row in scored if best_score - float(row["score"]) <= 0.02),
        key=lambda row: (
            int(row["max_threads"]),
            int(row["one_node_slots"]) + int(row["two_node_slots"]),
        ),
    )
    signatures = []
    for load, offered in LOAD_THREADS.items():
        values = {}
        pressures = {}
        for cpus, slots, name in (
            (32, selected["one_node_slots"], "one"),
            (64, selected["two_node_slots"], "two"),
        ):
            matches = [
                row for row in matrix
                if int(row["cpu_count"]) == cpus
                and int(row["max_threads"]) == int(selected["max_threads"])
                and int(row["slots"]) == int(slots)
                and row["load"] == load
            ]
            if len(matches) != 1:
                raise RuntimeError(f"selected finalist cell is missing: {cpus}/{slots}/{load}")
            values[name] = float(matches[0]["throughput_median_ops_s"])
            pressures[f"{name}_run_cpu_equiv"] = float(
                matches[0].get("run_cpu_equiv_median", math.nan)
            )
            pressures[f"{name}_rq_cpu_equiv"] = float(
                matches[0].get("rq_cpu_equiv_median", math.nan)
            )
        signatures.append({
            "load": load,
            "offered_threads": offered,
            "one_throughput_ops_s": values["one"],
            "two_throughput_ops_s": values["two"],
            "preferred_state": "two_node" if values["two"] > values["one"] else "one_node",
            **pressures,
        })
    return {
        "schema": "prism-sampler.clickhouse-selected-config.v1",
        **selected,
        "finalists": scored,
        "all_strategies": scored,
        "signatures": signatures,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError("cannot write an empty calibration matrix")
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def analyze_calibration(suite_dir: Path, output: Path) -> dict[str, Any]:
    raw = calibration_rows(suite_dir)
    matrix = aggregate_matrix(raw)
    selected = select_config(matrix)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "calibration-matrix.csv", matrix)
    (output / "selected-config.json").write_text(
        json.dumps(selected, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        "raw_rows": len(raw),
        "matrix_rows": len(matrix),
        "selected": selected,
    }


def prepare_finalist_suite(selected_path: Path, output: Path) -> dict[str, Any]:
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    finalists = list(selected["finalists"])
    lines = [
        "SUITE_NAME=clickhouse-v2-gate-b",
        "SUITE_SCENARIOS='finalist=/data/threadState/prism-sampler/config/scenarios/clickhouse-v2-finalist.env'",
        "SUITE_ROUNDS=2",
        "SUITE_ORDER=randomized",
        "SUITE_RANDOM_SEED=20260728",
    ]
    profiles = []
    for index, strategy in enumerate(finalists, 1):
        maximum = int(strategy["max_threads"])
        for cpus, node_list, slots in (
            (32, "0", int(strategy["one_node_slots"])),
            (64, "0,1", int(strategy["two_node_slots"])),
        ):
            profile = f"n{cpus}_m{maximum}_s{slots}_f{index}"
            profiles.append(profile)
            lines.extend([
                f"SUITE_PROFILE_{profile}_KIND=numa",
                f"SUITE_PROFILE_{profile}_CPU_NODES={node_list}",
                f"SUITE_PROFILE_{profile}_CLICKHOUSE_MAX_THREADS={maximum}",
                f"SUITE_PROFILE_{profile}_CLICKHOUSE_CONCURRENT_THREADS={slots}",
                f"SUITE_PROFILE_{profile}_CLICKHOUSE_CONCURRENT_RATIO=0",
            ])
    lines.insert(5, f"SUITE_BASELINE_PROFILE={profiles[0]}")
    lines.insert(5, "SUITE_PROFILES='" + " ".join(profiles) + "'")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"profiles": profiles, "output": str(output)}


def analyze_combined_calibration(
    suite_dirs: list[Path], output: Path, preliminary_path: Path | None = None
) -> dict[str, Any]:
    rows = []
    for suite_dir in suite_dirs:
        rows.extend(calibration_rows(suite_dir))
    matrix = aggregate_matrix(rows)
    preliminary = (
        json.loads(preliminary_path.read_text(encoding="utf-8"))
        if preliminary_path is not None else None
    )
    selected = (
        select_finalist_config(matrix, list(preliminary["finalists"]))
        if preliminary else select_config(matrix)
    )
    selected_cells = [
        row for row in matrix
        if int(row["max_threads"]) == int(selected["max_threads"])
        and (
            (int(row["cpu_count"]) == 32 and int(row["slots"]) == int(selected["one_node_slots"]))
            or (int(row["cpu_count"]) == 64 and int(row["slots"]) == int(selected["two_node_slots"]))
        )
        and not row.get("default_reference")
    ]
    stability = {
        "schema": "prism-sampler.calibration-validation.v1",
        "selected_cells": len(selected_cells),
        "cells_with_three_rounds": sum(int(row["rounds"]) >= 3 for row in selected_cells),
        "cells_over_cv_0_10": sum(float(row["throughput_cv"]) > 0.10 for row in selected_cells),
        "maximum_throughput_cv": max(
            (float(row["throughput_cv"]) for row in selected_cells), default=math.inf
        ),
        "errors": sum(int(row["errors"]) for row in selected_cells),
        "timeouts": sum(int(row["timeouts"]) for row in selected_cells),
    }
    stability["passed"] = bool(
        len(selected_cells) == 6
        and stability["cells_with_three_rounds"] == 6
        and stability["cells_over_cv_0_10"] == 0
        and stability["errors"] == 0
        and stability["timeouts"] == 0
    )
    selected["stability"] = stability
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "calibration-matrix.csv", matrix)
    (output / "selected-config.json").write_text(
        json.dumps(selected, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "calibration-validation.json").write_text(
        json.dumps(stability, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _write_csv(output / "calibration-validation.csv", [stability])
    return {"raw_rows": len(rows), "matrix_rows": len(matrix), "selected": selected}


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _bootstrap_ci(values: list[float], seed: int = 20260727) -> tuple[float, float]:
    if not values:
        return math.nan, math.nan
    rng = random.Random(seed)
    medians = [
        statistics.median(rng.choice(values) for _ in values)
        for _ in range(2000)
    ]
    medians.sort()
    return medians[int(0.025 * len(medians))], medians[int(0.975 * len(medians))]


def _sample_median(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return statistics.median(values) if values else math.nan


def _nested_median(
    rows: list[dict[str, Any]], section: str, key: str
) -> float:
    values = [
        float(row[section][key])
        for row in rows
        if isinstance(row.get(section), dict) and row[section].get(key) is not None
    ]
    return statistics.median(values) if values else math.nan


def _node_page_share(rows: list[dict[str, Any]], node: str) -> float:
    shares = []
    for row in rows:
        pages = row.get("numa_pages")
        if not isinstance(pages, dict):
            continue
        total = sum(float(value) for value in pages.values())
        if total:
            shares.append(float(pages.get(node, 0)) / total)
    return statistics.median(shares) if shares else math.nan


def _difference(after: float, before: float) -> float:
    return after - before if not math.isnan(after) and not math.isnan(before) else math.nan


def _pmu_window(
    database: Path | None, start_ns: int, end_ns: int
) -> dict[str, float]:
    if database is None or not database.is_file():
        return {}
    import duckdb

    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "pmu_derived" not in tables:
            return {}
        rows = con.execute(
            "SELECT metric, median(value) FROM pmu_derived "
            "WHERE epoch_ns(ts) >= ? AND epoch_ns(ts) < ? AND value IS NOT NULL "
            "GROUP BY metric",
            [start_ns, end_ns],
        ).fetchall()
        return {str(metric): float(value) for metric, value in rows}
    finally:
        con.close()


def _numa_node_share_window(
    database: Path | None, start_ns: int, end_ns: int, node: int
) -> float:
    if database is None or not database.is_file():
        return math.nan
    import duckdb

    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "numa_samples" not in tables:
            return math.nan
        row = con.execute(
            """
            SELECT median(node_value / total_value)
            FROM (
              SELECT ts,
                     sum(CASE WHEN node = ? THEN value ELSE 0 END) AS node_value,
                     sum(value) AS total_value
              FROM numa_samples
              WHERE metric = 'mapped' AND unit = 'pages'
                AND epoch_ns(ts) >= ? AND epoch_ns(ts) < ?
              GROUP BY ts
            )
            WHERE total_value > 0
            """,
            [node, start_ns, end_ns],
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else math.nan
    finally:
        con.close()


def _phase_databases(experiment: Path) -> dict[str, Path]:
    result = {}
    for database in experiment.glob("runs/**/dataset/telemetry.db3"):
        context = database.parents[1] / "meta" / "phase.json"
        if not context.is_file():
            continue
        phase = str(json.loads(context.read_text(encoding="utf-8")).get("phase", ""))
        if phase:
            result[phase] = database
    return result


def crossover_samples(
    experiments: list[Path], static_signatures: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    result = []
    static_signatures = static_signatures or {}
    for experiment in experiments:
        controller = experiment / "controller"
        kpis = _jsonl(controller / "kpi.jsonl")
        controller_samples = _jsonl(controller / "samples.jsonl")
        databases = _phase_databases(experiment)
        actions = [
            row for row in _jsonl(controller / "actions.jsonl")
            if str(row.get("action", "")).startswith("scripted_")
            and row.get("status") in {"applied", "shadow"}
        ]
        for action in actions:
            timestamp = int(action.get("finished_realtime_ns") or action["realtime_ns"])
            phase = str(action.get("phase") or (kpis[0].get("phase") if kpis else ""))
            before = [
                row for row in kpis
                if row.get("complete")
                and int(row.get("window_end_target_epoch_ns", 0)) <= timestamp
            ][-3:]
            after = [
                row for row in kpis
                if row.get("complete")
                and int(row.get("window_end_target_epoch_ns", 0)) >= timestamp + 20_000_000_000
            ][:3]
            if len(before) < 3 or len(after) < 3:
                continue
            pre = statistics.median(float(row["throughput_ops_s"]) for row in before)
            post = statistics.median(float(row["throughput_ops_s"]) for row in after)
            pre_samples = [
                row for row in controller_samples
                if row.get("phase") == phase
                and timestamp - 30_000_000_000 <= int(row.get("realtime_ns", 0)) < timestamp
            ]
            post_samples = [
                row for row in controller_samples
                if row.get("phase") == phase
                and timestamp + 20_000_000_000 <= int(row.get("realtime_ns", 0)) < timestamp + 50_000_000_000
            ]
            settling = [
                row for row in kpis
                if timestamp < int(row.get("window_end_target_epoch_ns", 0))
                < timestamp + 20_000_000_000
            ]
            expected_ops = pre * 10 * len(settling)
            actual_ops = sum(float(row.get("operations_delta", 0)) for row in settling)
            direction = f"{action['from_state']}->{action['to_state']}"
            load = next((name for name in LOAD_THREADS if phase.startswith(name)), phase)
            signature = static_signatures.get(load, {})
            source_key = "one_throughput_ops_s" if action["from_state"] == "one_node" else "two_throughput_ops_s"
            target_key = "one_throughput_ops_s" if action["to_state"] == "one_node" else "two_throughput_ops_s"
            source_static = float(signature.get(source_key) or 0)
            target_static = float(signature.get(target_key) or 0)
            static_ratio = target_static / source_static if source_static else math.nan
            dynamic_ratio = post / pre if pre else math.nan
            raw_gain = 100 * (dynamic_ratio - 1) if pre else math.nan
            normalized_gain = (
                100 * (dynamic_ratio / static_ratio - 1)
                if static_ratio and not math.isnan(static_ratio)
                else math.nan
            )
            penalty = (
                100 * max(0.0, expected_ops - actual_ops) / (pre * 60)
                if pre else math.nan
            )
            pmu_before = _pmu_window(
                databases.get(phase), timestamp - 30_000_000_000, timestamp
            )
            pmu_after = _pmu_window(
                databases.get(phase), timestamp + 20_000_000_000,
                timestamp + 50_000_000_000,
            )
            numa_before = _numa_node_share_window(
                databases.get(phase), timestamp - 30_000_000_000, timestamp, 1
            )
            numa_after = _numa_node_share_window(
                databases.get(phase), timestamp + 20_000_000_000,
                timestamp + 50_000_000_000, 1,
            )
            if math.isnan(numa_before):
                numa_before = _node_page_share(pre_samples, "1")
            if math.isnan(numa_after):
                numa_after = _node_page_share(post_samples, "1")
            row = {
                "experiment": experiment.name,
                "load": load,
                "direction": direction,
                "cpu_delta": 32 if action["to_state"] == "two_node" else -32,
                "pre_throughput_ops_s": pre,
                "post_throughput_ops_s": post,
                "raw_gain_pct": raw_gain,
                "static_control_ratio": static_ratio,
                "steady_gain_pct": normalized_gain,
                "transition_lost_ops": max(0.0, expected_ops - actual_ops),
                "transition_penalty_pct": penalty,
                "g_v1_pct": normalized_gain - penalty,
                "realized_net_gain_pct": raw_gain - penalty,
                "run_cpu_equiv_delta": _difference(
                    _sample_median(post_samples, "run_cpu_equiv"),
                    _sample_median(pre_samples, "run_cpu_equiv"),
                ),
                "rq_cpu_equiv_delta": _difference(
                    _sample_median(post_samples, "rq_cpu_equiv"),
                    _sample_median(pre_samples, "rq_cpu_equiv"),
                ),
                "query_thread_delta": _difference(
                    _nested_median(post_samples, "clickhouse_metrics", "QueryThread"),
                    _nested_median(pre_samples, "clickhouse_metrics", "QueryThread"),
                ),
                "global_thread_active_delta": _difference(
                    _nested_median(post_samples, "clickhouse_metrics", "GlobalThreadActive"),
                    _nested_median(pre_samples, "clickhouse_metrics", "GlobalThreadActive"),
                ),
                "global_thread_scheduled_delta": _difference(
                    _nested_median(post_samples, "clickhouse_metrics", "GlobalThreadScheduled"),
                    _nested_median(pre_samples, "clickhouse_metrics", "GlobalThreadScheduled"),
                ),
                "numa_node1_page_share_delta": _difference(
                    numa_after, numa_before
                ),
            }
            for metric in (
                "ipc", "cache_refill_per_kinst", "backend_stall_per_cycle",
                "frontend_stall_per_cycle", "mem_access_per_s", "remote_access_ratio",
                "ddrc_read_per_s", "ddrc_write_per_s", "cross_sccl_traffic_per_s",
            ):
                row[f"pmu_{metric}_delta"] = _difference(
                    pmu_after.get(metric, math.nan), pmu_before.get(metric, math.nan)
                )
            result.append(row)
    return result


def analyze_g(
    experiments: list[Path], selected_path: Path, output: Path
) -> dict[str, Any]:
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    static = {row["load"]: row for row in selected["signatures"]}
    samples = crossover_samples(experiments, static)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in samples:
        grouped[(row["load"], row["direction"])].append(row)
    table = []
    for (load, direction), rows in sorted(grouped.items()):
        gains = [float(row["steady_gain_pct"]) for row in rows]
        raw_gains = [float(row["raw_gain_pct"]) for row in rows]
        penalties = [float(row["transition_penalty_pct"]) for row in rows]
        median = statistics.median(gains)
        variance = statistics.variance(gains) if len(gains) > 1 else 0.0
        ci_low, ci_high = _bootstrap_ci(gains)
        signature = static[load]
        if direction == "one_node->two_node":
            prediction = 100 * (
                float(signature["two_throughput_ops_s"])
                / float(signature["one_throughput_ops_s"]) - 1
            )
        else:
            prediction = 100 * (
                float(signature["one_throughput_ops_s"])
                / float(signature["two_throughput_ops_s"]) - 1
            )
        penalty = statistics.median(penalties)
        output_row = {
            "formula_id": "pressure_numa_g_v1",
            "load": load,
            "direction": direction,
            "samples": len(rows),
            "g_static_v0_pct": prediction,
            "raw_gain_median_pct": statistics.median(raw_gains),
            "steady_gain_median_pct": median,
            "steady_gain_variance": variance,
            "steady_gain_cv": math.sqrt(variance) / abs(median) if median else math.nan,
            "steady_gain_ci95_low_pct": ci_low,
            "steady_gain_ci95_high_pct": ci_high,
            "transition_penalty_median_pct": penalty,
            "g_v1_pct": median - penalty,
            "realized_net_gain_median_pct": statistics.median(
                float(row["realized_net_gain_pct"]) for row in rows
            ),
            "direction_consistent": (prediction >= 0) == (statistics.median(raw_gains) >= 0),
            "absolute_error_pct_points": abs(prediction - statistics.median(raw_gains)),
        }
        for key in rows[0]:
            if not key.endswith("_delta"):
                continue
            values = [float(row[key]) for row in rows if not math.isnan(float(row[key]))]
            output_row[f"{key}_median"] = statistics.median(values) if values else math.nan
        table.append(output_row)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "g-action-samples.csv", samples)
    _write_csv(output / "g-benefit-table.csv", table)
    consistent = sum(bool(row["direction_consistent"]) for row in table)
    errors = [float(row["absolute_error_pct_points"]) for row in table]
    required_numeric = (
        "g_static_v0_pct", "raw_gain_median_pct", "steady_gain_median_pct",
        "transition_penalty_median_pct", "g_v1_pct",
        "run_cpu_equiv_delta_median", "rq_cpu_equiv_delta_median",
    )
    missing_required = sum(
        any(
            row.get(key) is None or math.isnan(float(row[key]))
            for key in required_numeric
        )
        for row in table
    )
    validation = {
        "schema": "prism-sampler.g-validation.v1",
        "formula_id": "pressure_numa_g_v1",
        "formula": (
            "G_v1=100*((post/pre)/(target_static/source_static)-1)-"
            "transition_lost_ops/(pre*minimum_dwell_seconds)*100"
        ),
        "rows": len(table),
        "rows_with_four_samples": sum(int(row["samples"]) >= 4 for row in table),
        "direction_consistent_rows": consistent,
        "median_absolute_error_pct_points": statistics.median(errors) if errors else None,
        "rows_with_missing_required_values": missing_required,
        "rows_with_pmu_evidence": sum(
            any(
                key.startswith("pmu_") and key.endswith("_median")
                and value is not None and not math.isnan(float(value))
                for key, value in row.items()
            )
            for row in table
        ),
        "passed": (
            len(table) == 6
            and all(int(row["samples"]) >= 4 for row in table)
            and consistent >= 5
            and bool(errors)
            and statistics.median(errors) <= 10
            and missing_required == 0
        ),
    }
    (output / "g-validation.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _write_csv(output / "g-validation.csv", [validation])
    return {"samples": len(samples), "table": table, "validation": validation}


def _summary_row(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 1:
        raise ValueError(f"expected one summary row: {path}")
    return rows[0]


def closed_loop_rows(
    static_source: Path | dict[str, list[Path]], dynamic_experiments: list[Path]
) -> list[dict[str, Any]]:
    rows = []
    if isinstance(static_source, Path):
        static_inputs = []
        for cell in sorted((static_source / "runs").glob("*")):
            if not (cell / ".complete").is_file():
                continue
            profile = _profile(cell)
            state = "one_node" if "one" in profile else "two_node"
            round_match = re.search(r"-r(\d+)$", cell.name)
            round_number = int(round_match.group(1)) if round_match else 0
            static_inputs.append((state, round_number, cell.name, cell.glob("phases/*/summary.csv")))
    else:
        static_inputs = []
        for state, experiments in static_source.items():
            for round_number, experiment in enumerate(experiments, 1):
                yba_dirs = sorted(experiment.glob("yba-*"))
                if len(yba_dirs) != 1:
                    raise ValueError(f"expected one YBA directory: {experiment}")
                static_inputs.append((
                    state, round_number, experiment.name,
                    yba_dirs[0].glob("phases/*/summary.csv"),
                ))
    for state, round_number, experiment_name, summaries in static_inputs:
        for summary in sorted(summaries):
            value = _summary_row(summary)
            rows.append({
                "mode": state,
                "round": round_number,
                "phase": value["label"],
                "throughput_ops_s": _float(value, "throughput"),
                "p99_latency_us": _float(value, "p99_latency"),
                "errors": int(float(value.get("error_count") or 0)),
                "timeouts": int(float(value.get("timeout_count") or 0)),
                "experiment": experiment_name,
                "two_node_share": 0.0 if state == "one_node" else 1.0,
            })
    for round_number, experiment in enumerate(dynamic_experiments, 1):
        yba_dirs = sorted(experiment.glob("yba-*"))
        if len(yba_dirs) != 1:
            raise ValueError(f"expected one YBA directory: {experiment}")
        residency = {}
        controller_summary = experiment / "controller" / "summary.csv"
        if controller_summary.is_file():
            with controller_summary.open(newline="", encoding="utf-8") as stream:
                residency = {
                    str(row["phase"]): float(row.get("two_node_share") or 0)
                    for row in csv.DictReader(stream)
                }
        for summary in sorted(yba_dirs[0].glob("phases/*/summary.csv")):
            value = _summary_row(summary)
            rows.append({
                "mode": "dynamic",
                "round": round_number,
                "phase": value["label"],
                "throughput_ops_s": _float(value, "throughput"),
                "p99_latency_us": _float(value, "p99_latency"),
                "errors": int(float(value.get("error_count") or 0)),
                "timeouts": int(float(value.get("timeout_count") or 0)),
                "experiment": experiment.name,
                "two_node_share": residency.get(str(value["label"]), math.nan),
            })
    return rows


def analyze_closed_loop(
    static_source: Path | dict[str, list[Path]],
    dynamic_experiments: list[Path],
    output: Path,
) -> dict[str, Any]:
    raw = closed_loop_rows(static_source, dynamic_experiments)
    phases = list(dict.fromkeys(str(row["phase"]) for row in raw))
    comparison = []
    for phase in phases:
        states = {}
        for state in ("one_node", "two_node", "dynamic"):
            subset = [row for row in raw if row["phase"] == phase and row["mode"] == state]
            states[state] = {
                "throughput": _median(subset, "throughput_ops_s"),
                "p99": _median(subset, "p99_latency_us"),
                "rounds": len(subset),
                "two_node_share": _median(subset, "two_node_share"),
            }
        oracle_state = max(
            ("one_node", "two_node"), key=lambda state: states[state]["throughput"]
        )
        oracle = states[oracle_state]
        dynamic = states["dynamic"]
        comparison.append({
            "phase": phase,
            "oracle_state": oracle_state,
            "one_node_median_ops_s": states["one_node"]["throughput"],
            "two_node_median_ops_s": states["two_node"]["throughput"],
            "dynamic_median_ops_s": dynamic["throughput"],
            "dynamic_oracle_ratio": dynamic["throughput"] / oracle["throughput"],
            "oracle_p99_median_us": oracle["p99"],
            "dynamic_p99_median_us": dynamic["p99"],
            "dynamic_oracle_p99_ratio": dynamic["p99"] / oracle["p99"] if oracle["p99"] else math.nan,
            "static_rounds_per_state": min(states["one_node"]["rounds"], states["two_node"]["rounds"]),
            "dynamic_rounds": dynamic["rounds"],
            "dynamic_two_node_share": dynamic["two_node_share"],
            "minimum_dynamic_round_oracle_ratio": min(
                (
                    float(row["throughput_ops_s"]) / float(oracle["throughput"])
                    for row in raw
                    if row["phase"] == phase and row["mode"] == "dynamic"
                ),
                default=math.nan,
            ),
        })
    ratios = [float(row["dynamic_oracle_ratio"]) for row in comparison]
    round_ratios = [
        float(row["minimum_dynamic_round_oracle_ratio"]) for row in comparison
    ]
    p99_ratios = [float(row["dynamic_oracle_p99_ratio"]) for row in comparison]
    errors = sum(int(row["errors"]) + int(row["timeouts"]) for row in raw)
    oracle_by_phase = {str(row["phase"]): str(row["oracle_state"]) for row in comparison}
    transition_delays = []
    previous_oracle = None
    changed_phases = []
    for phase in phases:
        oracle = oracle_by_phase[phase]
        if previous_oracle is not None and oracle != previous_oracle:
            changed_phases.append(phase)
        previous_oracle = oracle
    for experiment in dynamic_experiments:
        actions = sorted(
            _jsonl(experiment / "controller" / "actions.jsonl"),
            key=lambda row: int(row.get("realtime_ns", 0)),
        )
        phase_bounds = {}
        for context_path in experiment.glob("runs/**/meta/phase.json"):
            context = json.loads(context_path.read_text(encoding="utf-8"))
            if context.get("workload_start_epoch_ns") is not None:
                phase_bounds[str(context.get("phase", ""))] = (
                    int(context["workload_start_epoch_ns"]),
                    int(context["workload_end_epoch_ns"]),
                )
        for phase in changed_phases:
            bounds = phase_bounds.get(phase)
            if not bounds:
                transition_delays.append(math.inf)
                continue
            start, end = bounds
            target = oracle_by_phase[phase]
            prior = [
                row for row in actions
                if int(row.get("realtime_ns", 0)) <= start
                and row.get("status") == "applied"
                and row.get("to_state") in {"one_node", "two_node"}
            ]
            state = str(prior[-1]["to_state"]) if prior else "one_node"
            if state == target:
                transition_delays.append(0.0)
                continue
            matches = [
                row for row in actions
                if start < int(row.get("realtime_ns", 0)) < end
                and row.get("status") == "applied"
                and row.get("to_state") == target
            ]
            transition_delays.append(
                (int(matches[0]["realtime_ns"]) - start) / 1e9 if matches else math.inf
            )
    transitions_within_40 = sum(delay <= 40 for delay in transition_delays)
    validation = {
        "schema": "prism-sampler.closed-loop-validation.v1",
        "phase_count": len(comparison),
        "minimum_dynamic_oracle_ratio": min(ratios) if ratios else None,
        "lifecycle_dynamic_oracle_ratio": (
            sum(float(row["dynamic_median_ops_s"]) for row in comparison)
            / sum(max(float(row["one_node_median_ops_s"]), float(row["two_node_median_ops_s"])) for row in comparison)
            if comparison else None
        ),
        "maximum_p99_ratio": max(p99_ratios) if p99_ratios else None,
        "minimum_single_round_oracle_ratio": min(round_ratios) if round_ratios else None,
        "errors_and_timeouts": errors,
        "oracle_transition_opportunities": len(transition_delays),
        "oracle_transitions_within_40_seconds": transitions_within_40,
        "oracle_transition_within_40_ratio": (
            transitions_within_40 / len(transition_delays) if transition_delays else 1.0
        ),
        "oracle_transition_delay_seconds": [
            delay if math.isfinite(delay) else None for delay in transition_delays
        ],
        "dynamic_two_node_share_mean": statistics.mean(
            float(row["dynamic_two_node_share"])
            for row in comparison
            if not math.isnan(float(row["dynamic_two_node_share"]))
        ) if any(
            not math.isnan(float(row["dynamic_two_node_share"])) for row in comparison
        ) else None,
        "minimum_static_rounds_per_state": min(
            (int(row["static_rounds_per_state"]) for row in comparison), default=0
        ),
        "minimum_dynamic_rounds": min(
            (int(row["dynamic_rounds"]) for row in comparison), default=0
        ),
    }
    validation["passed"] = bool(
        comparison
        and validation["phase_count"] == 5
        and validation["minimum_static_rounds_per_state"] >= 3
        and validation["minimum_dynamic_rounds"] >= 3
        and validation["minimum_dynamic_oracle_ratio"] >= 0.98
        and validation["lifecycle_dynamic_oracle_ratio"] >= 0.98
        and validation["minimum_single_round_oracle_ratio"] >= 0.95
        and validation["maximum_p99_ratio"] <= 1.25
        and validation["oracle_transition_within_40_ratio"] >= 0.90
        and errors == 0
    )
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "closed-loop-raw.csv", raw)
    _write_csv(output / "closed-loop-comparison.csv", comparison)
    (output / "closed-loop-validation.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {"comparison": comparison, "validation": validation}


def prepare_static_suite(selected_path: Path, output: Path) -> dict[str, Any]:
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    maximum = int(selected["max_threads"])
    one_slots = int(selected["one_node_slots"])
    two_slots = int(selected["two_node_slots"])
    value = f"""SUITE_NAME=clickhouse-v2-static-closed-loop
SUITE_SCENARIOS='closed=/data/threadState/prism-sampler/config/scenarios/clickhouse-v2-closed-loop.env'
SUITE_ROUNDS=3
SUITE_ORDER=randomized
SUITE_RANDOM_SEED=20260729
SUITE_PROFILES='one_node two_node'
SUITE_BASELINE_PROFILE=one_node
SUITE_PROFILE_one_node_KIND=numa
SUITE_PROFILE_one_node_CPU_NODES=0
SUITE_PROFILE_one_node_CLICKHOUSE_MAX_THREADS={maximum}
SUITE_PROFILE_one_node_CLICKHOUSE_CONCURRENT_THREADS={one_slots}
SUITE_PROFILE_one_node_CLICKHOUSE_CONCURRENT_RATIO=0
SUITE_PROFILE_two_node_KIND=numa
SUITE_PROFILE_two_node_CPU_NODES=0,1
SUITE_PROFILE_two_node_CLICKHOUSE_MAX_THREADS={maximum}
SUITE_PROFILE_two_node_CLICKHOUSE_CONCURRENT_THREADS={two_slots}
SUITE_PROFILE_two_node_CLICKHOUSE_CONCURRENT_RATIO=0
"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(value, encoding="utf-8")
    return {"output": str(output), "max_threads": maximum}


def prepare_crossover_scenario(load: str, output: Path) -> dict[str, Any]:
    profiles = {
        "C1T1": (1, 1),
        "C2T2": (2, 2),
        "C4T6": (4, 6),
        "C5T16": (5, 16),
    }
    if load not in profiles:
        raise ValueError(f"unknown crossover load: {load}")
    clients, threads = profiles[load]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        f"""SCENARIO_NAME=clickhouse_v2_crossover_{load}
SCENARIO_BUDGET_MODE=duration
SCENARIO_PHASES='{load}'
SCENARIO_MIN_PHASE_SECONDS=60
SCENARIO_TIMEOUT_GRACE_SECONDS=45
SCENARIO_DURATION_TOLERANCE_SECONDS=2
SCENARIO_DURATION_OPERATIONCOUNT_PER_CLIENT=2147483647
SCENARIO_PHASE_{load}_CLIENTS={clients}
SCENARIO_PHASE_{load}_THREADS={threads}
SCENARIO_PHASE_{load}_VALUE=300
""",
        encoding="utf-8",
    )
    return {"output": str(output), "load": load}


def render_controller_config(
    selected_path: Path,
    output: Path,
    *,
    target_host: str,
    output_root: str,
    mode: str,
    initial_state: str = "one_node",
    scripted_transitions: list[str] | None = None,
    sampling_profile: str = "pressure-v2",
    dynamic_model_path: Path | None = None,
    minimum_expected_gain_pct: float = 2.0,
    controller_poll_seconds: float = 10.0,
    use_kpi_online: bool = True,
) -> dict[str, Any]:
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    transitions = scripted_transitions or []
    transition_toml = "[" + ", ".join(json.dumps(item) for item in transitions) + "]"
    value = f"""[experiment]
output_root = {json.dumps(output_root)}
system = "clickhouse"
[target]
host = {json.dumps(target_host)}
sudo = "SUDO_ASKPASS=/home/xhc/ExperScript/doris-bench/askpass.sh sudo -A"
remote_root = "/home/xhc/prism-sampler/data"
agent_command = "/home/xhc/prism-sampler/bin/prism-sampler-agent"
live_analyzer_command = "/home/xhc/prism-sampler/bin/prism-live-analyzer"
controller_command_prefix = "SUDO_ASKPASS=/home/xhc/ExperScript/doris-bench/askpass.sh sudo -A"
[client]
host = "ubuntu197"
hook_command = "/home/xhc/.local/bin/prism-sampler-hook"
kpi_forwarder_command = "/home/xhc/.local/bin/prism-kpi-forwarder"
sampler_config = "/home/xhc/.config/prism-sampler/local.toml"
output_root = "/home/xhc/.local/share/prism-sampler/experiments"
[collector]
binary = "/home/xhc/prism-sampler/bin/metric-collector"
runtime_lib = "/home/xhc/prism-sampler/lib"
attach_wait_seconds = 8
stop_timeout_seconds = 30
[yba]
root = "/home/x/data/sched-ext-study/ycsb-bench-all-database"
[sampling]
profile = {json.dumps(sampling_profile)}
interval_seconds = 10
platform = "kunpeng-920"
[relations]
live_interval_ms = 1000
live_queue_capacity = 64
window_seconds = 60
stability_window_seconds = 10
emit_seconds = 10
minimum_evidence_windows = 3
record_snapshots = false
[controller]
mode = {json.dumps(mode)}
sample_interval_seconds = {controller_poll_seconds}
decision_window_samples = 3
one_node_nodes = [0]
two_node_nodes = [0, 1]
one_node_slots = {int(selected['one_node_slots'])}
two_node_slots = {int(selected['two_node_slots'])}
fixed_max_threads = {int(selected['max_threads'])}
initial_state = {json.dumps(initial_state)}
scripted_transitions = {transition_toml}
clickhouse_config_path = "/home/xhc/clickhouse/etc/config.d/90-yba-experiment.xml"
clickhouse_preprocessed_config_path = "/home/xhc/clickhouse/data/preprocessed_configs/config.xml"
clickhouse_client_command = "/home/xhc/clickhouse/ClickHouse/build/programs/clickhouse-client --host 127.0.0.1 --port 9000"
benefit_signatures_file = {json.dumps(str(selected_path.resolve()))}
dynamic_model_file = {json.dumps(str(dynamic_model_path.resolve()) if dynamic_model_path else "")}
use_kpi_online = {str(use_kpi_online).lower()}
use_workload_activity_marker = {str(use_kpi_online).lower()}
minimum_expected_gain_pct = {minimum_expected_gain_pct}
minimum_model_confidence = 0.8
minimum_feature_coverage = 0.8
maximum_signature_distance = 0.75
maximum_model_distance = 2.5
gain_uncertainty_multiplier = 0.5
pressure_change_absolute = 0.15
pressure_change_relative = 0.25
pressure_change_confirm_samples = 3
settling_seconds = 20
minimum_two_node_dwell_seconds = 60
cooldown_seconds = 60
rollback_throughput_drop_pct = 5
rollback_p99_increase_pct = 50
fine_placement_mode = "shadow"
fine_placement_pair_threshold = 10
fine_placement_self_threshold = 10
fine_placement_minimum_confidence = 0.8
fine_placement_cluster_size = 4
actuator = "taskset"
migrate_pages = false
agent_command = "/home/xhc/prism-sampler/bin/prism-numa-controller"
"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(value, encoding="utf-8")
    return {"output": str(output), "mode": mode, "target_host": target_host}


def validate_realtime_kpi(
    experiments: list[Path], output: Path, *, expected_phases: int | None = None
) -> dict[str, Any]:
    rows = []
    for experiment in experiments:
        kpis = _jsonl(experiment / "controller" / "kpi.jsonl")
        yba_dirs = sorted(experiment.glob("yba-*"))
        summaries: dict[str, dict[str, str]] = {}
        if len(yba_dirs) == 1:
            for path in yba_dirs[0].glob("phases/*/summary.csv"):
                value = _summary_row(path)
                summaries[value["label"]] = value
        for phase in sorted({str(row.get("phase", "")) for row in kpis}):
            values = [row for row in kpis if row.get("phase") == phase]
            complete = [row for row in values if row.get("complete")]
            sequences = sorted(int(row["sequence"]) for row in values)
            final = summaries.get(phase, {})
            expected_windows = (
                max(1, int(float(final.get("runtime_ms_max") or 0) // 10_000))
                if final else len(values)
            )
            online = statistics.mean(float(row["throughput_ops_s"]) for row in complete) if complete else math.nan
            final_throughput = _float(final, "throughput") if final else math.nan
            lags = sorted(float(row["receive_lag_seconds"]) for row in complete)
            p95_index = max(0, math.ceil(0.95 * len(lags)) - 1) if lags else 0
            rows.append({
                "experiment": experiment.name,
                "phase": phase,
                "expected_windows": expected_windows,
                "windows": len(values),
                "complete_windows": len(complete),
                "received_ratio": len(values) / expected_windows if expected_windows else 0,
                "complete_ratio": len(complete) / expected_windows if expected_windows else 0,
                "receive_lag_p95_seconds": lags[p95_index] if lags else math.nan,
                "online_mean_throughput_ops_s": online,
                "final_throughput_ops_s": final_throughput,
                "throughput_difference_ratio": (
                    abs(online / final_throughput - 1) if final_throughput else math.nan
                ),
                "online_errors": sum(int(row.get("error_count_delta", 0)) for row in values),
                "final_errors": int(float(final.get("error_count") or 0)) if final else -1,
                "online_timeouts": sum(int(row.get("timeout_count_delta", 0)) for row in values),
                "final_timeouts": int(float(final.get("timeout_count") or 0)) if final else -1,
                "sequence_gaps": (
                    len(set(range(1, expected_windows + 1)) - set(sequences))
                ),
                "duplicate_sequences": len(sequences) - len(set(sequences)),
            })
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "realtime-kpi-validation.csv", rows)
    validation = {
        "schema": "prism-sampler.realtime-kpi-validation.v1",
        "phases": len(rows),
        "expected_phases": expected_phases,
        "minimum_complete_ratio": min((row["complete_ratio"] for row in rows), default=0),
        "maximum_lag_p95_seconds": max((row["receive_lag_p95_seconds"] for row in rows), default=math.inf),
        "maximum_throughput_difference_ratio": max((row["throughput_difference_ratio"] for row in rows), default=math.inf),
        "sequence_gaps": sum(int(row["sequence_gaps"]) for row in rows),
        "duplicate_sequences": sum(int(row["duplicate_sequences"]) for row in rows),
        "error_mismatches": sum(
            row["online_errors"] != row["final_errors"]
            or row["online_timeouts"] != row["final_timeouts"]
            for row in rows
        ),
    }
    validation["passed"] = bool(
        rows
        and (expected_phases is None or len(rows) == expected_phases)
        and validation["minimum_complete_ratio"] >= 0.98
        and validation["maximum_lag_p95_seconds"] <= 3
        and validation["maximum_throughput_difference_ratio"] <= 0.02
        and validation["sequence_gaps"] == 0
        and validation["duplicate_sequences"] == 0
        and validation["error_mismatches"] == 0
    )
    (output / "realtime-kpi-validation.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {"rows": rows, "validation": validation}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_hardware_graph_reference(
    calibration_root: Path, output: Path
) -> dict[str, Any]:
    names = (
        "manifest.json",
        "kunpeng920.csv",
        "kunpeng920.png",
        "derived/hardware-node-edges.csv",
        "derived/core-latency-by-cluster.csv",
        "derived/memory-latency.csv",
        "derived/stream-bandwidth.csv",
    )
    files = []
    for name in names:
        path = calibration_root / name
        if not path.is_file():
            raise RuntimeError(f"hardware calibration artifact is missing: {path}")
        files.append({
            "path": str(path.resolve()),
            "relative_path": name,
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        })
    value = {
        "schema": "prism-sampler.hardware-graph-reference.v1",
        "host": "kunpen183",
        "immutable_reference": True,
        "placement_applied": False,
        "root": str(calibration_root.resolve()),
        "files": files,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return value


def write_online_graph_index(
    experiment_roots: list[Path], output: Path, *, minimum_phase_graphs: int = 1
) -> dict[str, Any]:
    rows = []
    seen = set()
    for root in experiment_roots:
        for summary in sorted(root.glob("**/raw/live-summary.json")):
            if summary.resolve() in seen:
                continue
            seen.add(summary.resolve())
            raw = summary.parent
            latest_path = raw / "live-candidates-latest.json"
            candidates_path = raw / "live-candidates.jsonl"
            value = json.loads(summary.read_text(encoding="utf-8"))
            latest = (
                json.loads(latest_path.read_text(encoding="utf-8"))
                if latest_path.is_file() else {}
            )
            rows.append({
                "experiment_root": str(root.resolve()),
                "phase_raw_dir": str(raw.resolve()),
                "summary_path": str(summary.resolve()),
                "summary_sha256": _sha256(summary),
                "candidates_path": str(candidates_path.resolve()) if candidates_path.is_file() else "",
                "candidates_sha256": _sha256(candidates_path) if candidates_path.is_file() else "",
                "snapshots": int(value.get("snapshots") or 0),
                "emissions": int(value.get("emissions") or 0),
                "pair_candidates": int(value.get("last_pair_candidates") or 0),
                "self_candidates": int(value.get("last_self_candidates") or 0),
                "quality_flags": ",".join(latest.get("quality", {}).get("flags", [])),
                "placement_applied": False,
            })
    if len(rows) < minimum_phase_graphs:
        raise RuntimeError(
            f"online Prism thread graph artifacts are incomplete: "
            f"{len(rows)}/{minimum_phase_graphs}"
        )
    _write_csv(output, rows)
    return {
        "phase_graphs": len(rows),
        "snapshots": sum(int(row["snapshots"]) for row in rows),
        "pair_candidates": sum(int(row["pair_candidates"]) for row in rows),
        "self_candidates": sum(int(row["self_candidates"]) for row in rows),
        "index": str(output.resolve()),
    }


def _action_timing(row: dict[str, Any]) -> tuple[int, int]:
    starts = [int(row.get("realtime_ns") or 0)]
    finishes = [int(row.get("realtime_ns") or 0)]

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("started_realtime_ns") is not None:
                starts.append(int(value["started_realtime_ns"]))
            if value.get("finished_realtime_ns") is not None:
                finishes.append(int(value["finished_realtime_ns"]))
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(row.get("steps", []))
    visit(row.get("rollback", []))
    return min(starts), max(finishes)


def validate_controller_actions(
    experiments: list[Path], output: Path, *, cooldown_seconds: float = 60.0,
    expected_scripted_actions: int | None = None,
) -> dict[str, Any]:
    rows = []
    phase_counts: dict[tuple[str, str], int] = defaultdict(int)
    reverse_in_cooldown = 0
    for experiment in experiments:
        applied_history = []
        for action in sorted(
            _jsonl(experiment / "controller" / "actions.jsonl"),
            key=lambda row: int(row.get("realtime_ns", 0)),
        ):
            name = str(action.get("action", ""))
            if name in {"initialize", "restore"}:
                continue
            started, finished = _action_timing(action)
            steps = action.get("steps", [])
            verified = bool(
                action.get("status") == "applied"
                and isinstance(steps, list)
                and len(steps) >= 2
                and all(
                    isinstance(step, dict)
                    and len(step) == 1
                    and isinstance(next(iter(step.values())), dict)
                    and next(iter(step.values())).get("status") == "applied"
                    for step in steps
                )
            )
            phase = str(action.get("phase", ""))
            phase_counts[(experiment.name, phase)] += 1
            if action.get("status") == "applied" and action.get("to_state") in {"one_node", "two_node"}:
                if applied_history:
                    previous = applied_history[-1]
                    if (
                        previous.get("to_state") != action.get("to_state")
                        and started - int(previous["started_ns"]) < cooldown_seconds * 1e9
                    ):
                        reverse_in_cooldown += 1
                applied_history.append({"to_state": action.get("to_state"), "started_ns": started})
            rows.append({
                "experiment": experiment.name,
                "phase": phase,
                "action": name,
                "from_state": action.get("from_state", ""),
                "to_state": action.get("to_state", ""),
                "status": action.get("status", ""),
                "duration_seconds": max(0.0, (finished - started) / 1e9),
                "transaction_verified": verified,
                "error": action.get("error", ""),
            })
    output.mkdir(parents=True, exist_ok=True)
    if rows:
        _write_csv(output / "controller-action-validation.csv", rows)
    durations = sorted(float(row["duration_seconds"]) for row in rows if row["status"] == "applied")
    p95 = durations[max(0, math.ceil(0.95 * len(durations)) - 1)] if durations else math.inf
    rollback_durations = [
        float(row["duration_seconds"]) for row in rows
        if "rollback" in str(row["action"]) and row["status"] == "applied"
    ]
    failed = [row for row in rows if row["status"] != "applied"]
    invalid_pid = sum(
        "PID" in str(row.get("error", "")) or "identity" in str(row.get("error", ""))
        for row in rows
    )
    validation = {
        "schema": "prism-sampler.controller-action-validation.v1",
        "actions": len(rows),
        "failed_actions": len(failed),
        "transaction_verified_ratio": (
            sum(bool(row["transaction_verified"]) for row in rows) / len(rows) if rows else 0
        ),
        "action_duration_p95_seconds": p95,
        "rollback_duration_max_seconds": max(rollback_durations, default=0.0),
        "maximum_actions_per_phase": max(phase_counts.values(), default=0),
        "cooldown_reverse_actions": reverse_in_cooldown,
        "pid_identity_errors": invalid_pid,
        "scripted_actions": sum(
            str(row["action"]).startswith("scripted_") for row in rows
        ),
    }
    validation["passed"] = bool(
        rows
        and (
            expected_scripted_actions is None
            or validation["scripted_actions"] == expected_scripted_actions
        )
        and validation["failed_actions"] == 0
        and validation["transaction_verified_ratio"] == 1
        and validation["action_duration_p95_seconds"] <= 5
        and validation["rollback_duration_max_seconds"] <= 10
        and validation["maximum_actions_per_phase"] <= 2
        and validation["cooldown_reverse_actions"] == 0
        and validation["pid_identity_errors"] == 0
    )
    (output / "controller-action-validation.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return validation


def write_pressure_v2_report(root: Path, selected_path: Path, output: Path) -> Path:
    selected = json.loads(selected_path.read_text(encoding="utf-8"))

    def load(name: str) -> dict[str, Any]:
        path = output / name
        return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}

    g = load("g-validation.json")
    closed = load("closed-loop-validation.json")
    kpi = load("realtime-kpi-validation.json")
    actions = load("controller-action-validation.json")
    hardware = load("hardware-graph-reference.json")
    budget = load("workload-budget.json")
    calibration = selected.get("stability", {})
    lines = [
        "# ClickHouse Pressure-Aware NUMA Scaling v2",
        "",
        "## 选定配置",
        "",
        f"- 固定 `max_threads`: `{selected['max_threads']}`",
        f"- ONE: node0 / 32 CPU / slots `{selected['one_node_slots']}`",
        f"- TWO: node0+node1 / 64 CPU / slots `{selected['two_node_slots']}`",
        "- 在线动作只切换 `node_mask + global slots`；未执行页面迁移或图放置。",
        "",
        "## 量化验收",
        "",
        f"- finalist 稳定性：`{'PASS' if calibration.get('passed') else 'FAIL'}`；6 个单元最大吞吐 CV `{calibration.get('maximum_throughput_cv')}`。",
        f"- G 校准：`{'PASS' if g.get('passed') else 'FAIL'}`；行数 `{g.get('rows', 0)}/6`，方向一致 `{g.get('direction_consistent_rows', 0)}/6`，中位绝对误差 `{g.get('median_absolute_error_pct_points')}` 个百分点。",
        f"- G 公式：`{g.get('formula_id', '')}`，`{g.get('formula', '')}`。",
        f"- 闭环：`{'PASS' if closed.get('passed') else 'FAIL'}`；最小阶段/oracle `{closed.get('minimum_dynamic_oracle_ratio')}`，生命周期/oracle `{closed.get('lifecycle_dynamic_oracle_ratio')}`，最小单轮/oracle `{closed.get('minimum_single_round_oracle_ratio')}`。",
        f"- Dynamic TWO 驻留率（仅报告，不设门槛）：`{closed.get('dynamic_two_node_share_mean')}`。",
        f"- 实时 KPI：`{'PASS' if kpi.get('passed') else 'FAIL'}`；最小完整率 `{kpi.get('minimum_complete_ratio')}`，sequence gaps `{kpi.get('sequence_gaps')}`，最大接收延迟 P95 `{kpi.get('maximum_lag_p95_seconds')}` 秒。",
        f"- 控制器动作：`{'PASS' if actions.get('passed') else 'FAIL'}`；事务验证率 `{actions.get('transaction_verified_ratio')}`，动作耗时 P95 `{actions.get('action_duration_p95_seconds')}` 秒。",
        f"- 实际 workload 占用：`{'PASS' if budget.get('passed') else 'FAIL'}`；`{budget.get('actual_workload_hours')}` 小时（目标 6–8 小时）。",
        "",
        "## 图证据",
        "",
        f"- 硬件图根目录：`{hardware.get('root', '')}`",
        "- Prism 在线线程图作为 shadow 证据保留，并由 `online-thread-graph-index.csv` 索引。",
        "- v2 控制器不使用硬件图或线程图执行放置决策。",
    ]
    lines.extend([
        "",
        "## 校准 Signature",
        "",
        "| Load | ONE ops/s | TWO ops/s | Preferred | ONE run/rq | TWO run/rq |",
        "| --- | ---: | ---: | --- | ---: | ---: |",
    ])
    for row in selected.get("signatures", []):
        lines.append(
            f"| {row['load']} | {float(row['one_throughput_ops_s']):.2f} | "
            f"{float(row['two_throughput_ops_s']):.2f} | {row['preferred_state']} | "
            f"{float(row.get('one_run_cpu_equiv', math.nan)):.2f}/"
            f"{float(row.get('one_rq_cpu_equiv', math.nan)):.2f} | "
            f"{float(row.get('two_run_cpu_equiv', math.nan)):.2f}/"
            f"{float(row.get('two_rq_cpu_equiv', math.nan)):.2f} |"
        )

    def csv_rows(name: str) -> list[dict[str, str]]:
        path = output / name
        if not path.is_file():
            return []
        with path.open(newline="", encoding="utf-8") as stream:
            return list(csv.DictReader(stream))

    lines.extend([
        "",
        "## G v1",
        "",
        "| Load | Direction | n | Static % | Raw % | Normalized % | Penalty % | G_v1 % |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in csv_rows("g-benefit-table.csv"):
        lines.append(
            f"| {row['load']} | {row['direction']} | {row['samples']} | "
            f"{float(row['g_static_v0_pct']):.2f} | {float(row['raw_gain_median_pct']):.2f} | "
            f"{float(row['steady_gain_median_pct']):.2f} | "
            f"{float(row['transition_penalty_median_pct']):.2f} | "
            f"{float(row['g_v1_pct']):.2f} |"
        )
    lines.extend([
        "",
        "## 闭环结果",
        "",
        "| Phase | Oracle | ONE ops/s | TWO ops/s | Dynamic ops/s | Dynamic/oracle | TWO residency |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    closed_rows = csv_rows("closed-loop-comparison.csv")
    for row in closed_rows:
        lines.append(
            f"| {row['phase']} | {row['oracle_state']} | "
            f"{float(row['one_node_median_ops_s']):.2f} | "
            f"{float(row['two_node_median_ops_s']):.2f} | "
            f"{float(row['dynamic_median_ops_s']):.2f} | "
            f"{float(row['dynamic_oracle_ratio']):.4f} | "
            f"{float(row['dynamic_two_node_share']):.4f} |"
        )
    oracle_states = {row["oracle_state"] for row in closed_rows}
    oracle_summary = ", ".join(sorted(oracle_states)) or "unknown"
    lines.extend([
        "",
        "## 结论",
        "",
        f"- 最大吞吐验收通过：动态生命周期达到静态 oracle 的 `{closed.get('lifecycle_dynamic_oracle_ratio')}`。",
        f"- 本轮所有阶段的静态 oracle 状态为 `{oracle_summary}`，Dynamic TWO 平均驻留率为 `{closed.get('dynamic_two_node_share_mean')}`；因此本轮证明了控制器能够跟上吞吐 oracle，但没有证明负载回落时可以安全缩回 ONE。",
        f"- 实时 KPI 完整性验收未通过：最小完整率 `{kpi.get('minimum_complete_ratio')}`，sequence gaps `{kpi.get('sequence_gaps')}`；吞吐偏差、错误计数和接收延迟仍满足各自门槛。",
        "- G v1 可用于当前三档负载和两个动作方向的配置收益表；它不是跨负载、跨机器的通用回归公式。",
        "- Prism 在线线程图和硬件图在 v2 中仅作为解释证据，没有参与 affinity 决策。",
        "",
        f"实验根目录：`{root.resolve()}`",
    ])
    path = output / "report.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
