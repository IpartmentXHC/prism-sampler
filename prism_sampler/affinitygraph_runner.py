from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import random
import re
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import Any

from .remote import Host
from .controller.metrics import format_cpu_list, parse_cpu_list


LOADS = {"C2T2": (2, 2), "C4T6": (4, 6), "C5T16": (5, 16)}
SMOKE_LOADS = {**LOADS, "C4T4": (4, 4)}
DORIS_FORMAL_LOADS = {
    "C1T1": (1, 1), "C2T2": (2, 2), "C3T4": (3, 4),
    "C4T8": (4, 8), "C5T12": (5, 12), "C5T16": (5, 16),
}
WEIGHTS = {"C2T2": 0.2, "C4T6": 0.3, "C5T16": 0.5}
REAL_CLICKHOUSE = "/home/xhc/clickhouse/ClickHouse/build/programs/clickhouse"
DORIS_HOME = "/home/xhc/doris/apache-doris-2.1.2-bin-arm64"
REAL_DORIS_BE = f"{DORIS_HOME}/be/lib/doris_be"
DORIS_START_BE = f"{DORIS_HOME}/be/bin/start_be.sh"
ASKPASS = "/home/xhc/ExperScript/doris-bench/askpass.sh"

DORIS_FORMAL_TRAJECTORIES: dict[str, list[tuple[str, int]]] = {
    "S1": [
        ("C3T4", 85), ("C2T2", 80), ("C5T12", 105),
        ("C5T16", 75), ("C4T8", 100), ("C1T1", 95),
    ],
    "S2": [
        ("C5T16", 105), ("C5T12", 100), ("C1T1", 85),
        ("C4T8", 95), ("C3T4", 75), ("C2T2", 80),
    ],
}
DORIS_FORMAL_SEEDS = {"S1": 20260809, "S2": 20260813}
DORIS_FORMAL_RUN_ORDER = [
    ("S2", "active", 1), ("S2", "active", 2),
    ("S2", "baseline", 1), ("S1", "active", 1),
    ("S1", "active", 2), ("S1", "baseline", 1),
    ("S1", "active", 3), ("S2", "active", 3),
]


def doris_formal_design() -> dict[str, Any]:
    return {
        "schema": "prism-sampler.affinitygraph-formal-design.v1",
        "name": "doris-random-v1",
        "system": "doris",
        "run_order_seed": 20260811,
        "acquisition": {"load": "C4T8", "seconds": 150},
        "warmup": {"load": "C4T8", "seconds": 30},
        "lifecycle_timeout_seconds": 1800,
        "trajectories": {
            name: {
                "seed": DORIS_FORMAL_SEEDS[name],
                "phases": [
                    {
                        "phase": f"M{index:02d}_{load}",
                        "load": load,
                        "clients": DORIS_FORMAL_LOADS[load][0],
                        "threads": DORIS_FORMAL_LOADS[load][1],
                        "seconds": seconds,
                    }
                    for index, (load, seconds) in enumerate(phases, 1)
                ],
            }
            for name, phases in DORIS_FORMAL_TRAJECTORIES.items()
        },
        "run_order": [
            {
                "position": position,
                "trajectory": trajectory,
                "treatment": treatment,
                "repeat": repeat,
            }
            for position, (trajectory, treatment, repeat) in enumerate(
                DORIS_FORMAL_RUN_ORDER, 1
            )
        ],
        "gates": {
            "maximum_bpf_loss_ratio": 0.01,
            "minimum_action_success_ratio": 0.95,
            "minimum_effective_coverage_ratio": 0.95,
            "maximum_supervisor_cpu_cores": 1.0,
            "maximum_p99_ratio": 1.10,
            "minimum_seed_uplift": -0.05,
            "minimum_positive_runs": 4,
        },
    }


def analyze_doris_formal(
    rows: list[dict[str, Any]], *, seed: int = 20260811, samples: int = 10000
) -> dict[str, Any]:
    complete_rows = [row for row in rows if row.get("status") == "complete"]
    baselines = {
        str(row["trajectory"]): row
        for row in complete_rows
        if row.get("treatment") == "baseline" and row.get("valid", True)
    }
    active_rows = [
        row for row in complete_rows
        if row.get("treatment") == "active" and row.get("valid", True)
    ]
    effects = []
    for row in active_rows:
        baseline = baselines.get(str(row["trajectory"]))
        if not baseline or float(baseline.get("active_throughput", 0)) <= 0:
            continue
        effects.append({
            "trajectory": row["trajectory"],
            "repeat": row["repeat"],
            "baseline_throughput": float(baseline["active_throughput"]),
            "active_throughput": float(row["active_throughput"]),
            "uplift": float(row["active_throughput"])
            / float(baseline["active_throughput"]) - 1.0,
            "lifecycle_uplift": float(row["lifecycle_throughput"])
            / float(baseline["lifecycle_throughput"]) - 1.0,
            "p99_ratio": float(row["p99_latency_us"])
            / float(baseline["p99_latency_us"]),
        })
    seed_effects = {
        trajectory: [row for row in effects if row["trajectory"] == trajectory]
        for trajectory in sorted(DORIS_FORMAL_TRAJECTORIES)
    }
    seed_summary = {
        trajectory: {
            "active_repeats": len(values),
            "mean_uplift": statistics.mean(row["uplift"] for row in values)
            if values else math.nan,
            "mean_lifecycle_uplift": statistics.mean(
                row["lifecycle_uplift"] for row in values
            ) if values else math.nan,
            "wins": sum(row["uplift"] > 0 for row in values),
        }
        for trajectory, values in seed_effects.items()
    }
    complete = len(baselines) == 2 and len(effects) == 6 and all(
        len(values) == 3 for values in seed_effects.values()
    )
    mean_uplift = statistics.mean(
        value["mean_uplift"] for value in seed_summary.values()
    ) if complete else math.nan
    rng = random.Random(seed)
    bootstrap = []
    if complete:
        trajectories = sorted(seed_effects)
        for _ in range(samples):
            selected = [rng.choice(trajectories) for _ in trajectories]
            bootstrap.append(statistics.mean(
                statistics.mean(
                    rng.choice(seed_effects[trajectory])["uplift"]
                    for _ in seed_effects[trajectory]
                )
                for trajectory in selected
            ))
    safety_passed = complete and all(
        row.get("formal_health", {}).get("passed", False) for row in active_rows
    )
    p99_passed = complete and all(row["p99_ratio"] <= 1.10 for row in effects)
    performance_passed = bool(
        complete and mean_uplift > 0
        and all(value["mean_uplift"] >= -0.05 for value in seed_summary.values())
        and sum(row["uplift"] > 0 for row in effects) >= 4
        and p99_passed
    )
    return {
        "schema": "prism-sampler.affinitygraph-doris-formal-result.v1",
        "complete": complete,
        "independent_baseline_blocks": 2,
        "active_repeats": len(effects),
        "effects": effects,
        "seed_summary": seed_summary,
        "seed_balanced_mean_uplift": mean_uplift,
        "descriptive_cluster_bootstrap_95_ci": (
            [_percentile(bootstrap, 0.025), _percentile(bootstrap, 0.975)]
            if bootstrap else [math.nan, math.nan]
        ),
        "bootstrap_is_descriptive": True,
        "safety_passed": safety_passed,
        "p99_passed": p99_passed,
        "performance_passed": performance_passed,
        "candidate_pass": bool(safety_passed and performance_passed),
        "runs": complete_rows,
    }


def schedule(
    rounds: int, seed: int, loads: tuple[str, ...] | None = None
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    selected_loads = loads or tuple(LOADS)
    pairs = [
        (load, round_number)
        for load in selected_loads
        for round_number in range(1, rounds + 1)
    ]
    rng.shuffle(pairs)
    result: list[dict[str, Any]] = []
    for pair_index, (load, round_number) in enumerate(pairs, 1):
        treatments = ["baseline", "active"]
        rng.shuffle(treatments)
        for treatment_index, treatment in enumerate(treatments, 1):
            result.append({
                "pair_index": pair_index,
                "treatment_index": treatment_index,
                "load": load,
                "round": round_number,
                "treatment": treatment,
            })
    return result


def positive_control_profiles(system: str) -> dict[str, dict[str, str]]:
    common = {
        "ENABLE_THREAD_CLUSTER": "1",
        "THREAD_CLUSTER_STRICT": "1",
        "THREAD_CLUSTER_MIN_HIT_RATIO": "0.95",
        "THREAD_CLUSTER_REQUIRE_STABLE": "1",
        "THREAD_CLUSTER_TASKSET_WITH_SUDO": "1",
    }
    if system == "doris":
        watch = {
            **common,
            "THREAD_CLUSTER_BIND_INTERVAL": "1",
            "THREAD_CLUSTER_BIND_MAX_SECONDS": "30",
        }
        return {
            "unrestricted": {},
            "one_node": {
                **watch,
                "THREAD_CLUSTER_RULES": "all:.*:0-31",
                "THREAD_CLUSTER_DEFAULT_CPUS": "",
                # All-thread placement must follow late Doris worker creation.
                "THREAD_CLUSTER_BIND_MAX_SECONDS": "0",
            },
            "light_pipe_unlimited": {
                **watch,
                "THREAD_CLUSTER_RULES": (
                    "light:brpc_light:64-95 pipe:Pipe_normal:64-95"
                ),
                "THREAD_CLUSTER_DEFAULT_NAME": "other",
                "THREAD_CLUSTER_DEFAULT_CPUS": "0-127",
            },
            "light_pipe_limited": {
                **watch,
                "THREAD_CLUSTER_RULES": (
                    "light:brpc_light:64-95 pipe:Pipe_normal:64-95"
                ),
                "THREAD_CLUSTER_DEFAULT_NAME": "other",
                "THREAD_CLUSTER_DEFAULT_CPUS": "96-127",
            },
        }
    if system == "clickhouse":
        watch = {
            **common,
            "THREAD_CLUSTER_BIND_INTERVAL": "0.2",
            "THREAD_CLUSTER_BIND_MAX_SECONDS": "0",
        }
        return {
            "two_node": {
                **watch,
                "THREAD_CLUSTER_RULES": "all:.*:0-63",
                "THREAD_CLUSTER_DEFAULT_CPUS": "",
            },
            "threadpool_compact": {
                **watch,
                "THREAD_CLUSTER_RULES": "candidate:^ThreadPool$:0-31",
                "THREAD_CLUSTER_DEFAULT_NAME": "other",
                "THREAD_CLUSTER_DEFAULT_CPUS": "0-63",
            },
        }
    raise ValueError(f"unsupported positive-control system: {system}")


def positive_control_schedule(
    system: str, rounds: int, seed: int
) -> list[dict[str, Any]]:
    if rounds < 1:
        raise ValueError("positive-control rounds must be positive")
    profiles = list(positive_control_profiles(system))
    rng = random.Random(seed)
    result: list[dict[str, Any]] = []
    for round_number in range(1, rounds + 1):
        order = profiles.copy()
        rng.shuffle(order)
        for position, profile in enumerate(order, 1):
            result.append({
                "round": round_number,
                "position": position,
                "profile": profile,
            })
    return result


def analyze_positive_control(
    rows: list[dict[str, Any]], system: str, *, required_pairs: int = 2
) -> dict[str, Any]:
    if required_pairs < 1:
        raise ValueError("required positive-control pairs must be positive")
    contrasts = (
        [
            ("light_pipe_unlimited", "unrestricted"),
            ("light_pipe_limited", "one_node"),
        ]
        if system == "doris"
        else [("threadpool_compact", "two_node")]
    )
    indexed = {
        (int(row["round"]), str(row["profile"])): row
        for row in rows
        if row.get("valid", True)
    }
    effects: list[dict[str, Any]] = []
    for treatment, baseline in contrasts:
        rounds = sorted({
            int(row["round"])
            for row in rows
            if row.get("profile") in {treatment, baseline}
        })
        for round_number in rounds:
            treatment_row = indexed.get((round_number, treatment))
            baseline_row = indexed.get((round_number, baseline))
            if not treatment_row or not baseline_row:
                continue
            baseline_throughput = float(baseline_row["throughput"])
            treatment_throughput = float(treatment_row["throughput"])
            if baseline_throughput <= 0 or treatment_throughput < 0:
                continue
            effects.append({
                "contrast": f"{treatment}_vs_{baseline}",
                "round": round_number,
                "baseline_throughput": baseline_throughput,
                "treatment_throughput": treatment_throughput,
                "uplift": treatment_throughput / baseline_throughput - 1.0,
                "baseline_p99_latency_us": float(
                    baseline_row.get("p99_latency_us", math.nan)
                ),
                "treatment_p99_latency_us": float(
                    treatment_row.get("p99_latency_us", math.nan)
                ),
            })
    summaries = []
    for treatment, baseline in contrasts:
        name = f"{treatment}_vs_{baseline}"
        values = [row for row in effects if row["contrast"] == name]
        summaries.append({
            "contrast": name,
            "pairs": len(values),
            "mean_uplift": (
                statistics.mean(row["uplift"] for row in values)
                if values else math.nan
            ),
            "wins": sum(row["uplift"] > 0 for row in values),
        })
    complete = bool(summaries) and all(
        item["pairs"] >= required_pairs for item in summaries
    )
    directions = [item["mean_uplift"] > 0 for item in summaries if item["pairs"]]
    if complete and directions and all(directions):
        attribution = "selector"
    elif complete and directions and not any(directions):
        attribution = "collector_or_manual_evidence"
    else:
        attribution = "mixed_or_incomplete"
    return {
        "schema": "prism-sampler.affinitygraph-positive-control.v1",
        "system": system,
        "required_pairs": required_pairs,
        "complete": complete,
        "attribution": attribution,
        "contrasts": summaries,
        "effects": effects,
        "runs": rows,
    }


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _continuous_active_interval(
    previous_time: float,
    current_time: float,
    *,
    previous_measurement_open: bool,
    current_measurement_open: bool,
    previous_active: bool,
    current_active: bool,
) -> float:
    if not (
        previous_measurement_open
        and current_measurement_open
        and previous_active
        and current_active
    ):
        return 0.0
    return max(0.0, current_time - previous_time)


def analyze(rows: list[dict[str, Any]], *, seed: int = 20260805, samples: int = 10000,
            required_pairs: int | None = None) -> dict[str, Any]:
    valid_rows = [row for row in rows if row.get("valid", True)]
    loads = sorted({str(row["load"]) for row in valid_rows})
    indexed = {(row["load"], int(row["round"]), row["treatment"]): row for row in valid_rows}
    effects = []
    for load in loads:
        rounds = sorted({int(row["round"]) for row in valid_rows if row["load"] == load})
        for round_number in rounds:
            baseline = indexed.get((load, round_number, "baseline"))
            active = indexed.get((load, round_number, "active"))
            if not baseline or not active:
                continue
            effects.append({
                "load": load,
                "round": round_number,
                "baseline_throughput": float(baseline["throughput"]),
                "active_throughput": float(active["throughput"]),
                "uplift": float(active["throughput"]) / float(baseline["throughput"]) - 1.0,
                "baseline_p99_latency_us": float(baseline.get("p99_latency_us", math.nan)),
                "active_p99_latency_us": float(active.get("p99_latency_us", math.nan)),
                "baseline_error_count": int(baseline.get("error_count", 0)),
                "active_error_count": int(active.get("error_count", 0)),
                "baseline_timeout_count": int(baseline.get("timeout_count", 0)),
                "active_timeout_count": int(active.get("timeout_count", 0)),
            })
    grouped_effects = {load: [row for row in effects if row["load"] == load] for load in loads}
    expected_pairs = required_pairs or min((len(values) for values in grouped_effects.values()), default=0)
    complete = expected_pairs > 0 and all(len(grouped_effects[load]) == expected_pairs for load in loads)
    by_load = {
        load: statistics.mean(row["uplift"] for row in grouped_effects[load])
        if grouped_effects[load] else math.nan
        for load in loads
    }
    raw_weights = {load: WEIGHTS.get(load, 1.0) for load in loads}
    weight_total = sum(raw_weights.values()) or 1.0
    weights = {load: value / weight_total for load, value in raw_weights.items()}
    weighted = sum(weights[load] * by_load[load] for load in loads) if complete else math.nan
    rng = random.Random(seed)
    bootstrap = []
    grouped = {load: [row["uplift"] for row in grouped_effects[load]] for load in loads}
    per_load_ci: dict[str, list[float]] = {}
    if complete:
        load_bootstrap: dict[str, list[float]] = {load: [] for load in loads}
        for _ in range(samples):
            sampled = {
                load: statistics.mean(rng.choice(grouped[load]) for _ in grouped[load])
                for load in loads
            }
            for load in loads:
                load_bootstrap[load].append(sampled[load])
            bootstrap.append(sum(weights[load] * sampled[load] for load in loads))
        per_load_ci = {
            load: [_percentile(values, 0.025), _percentile(values, 0.975)]
            for load, values in load_bootstrap.items()
        }
    raw_by_load: dict[str, list[dict[str, Any]]] = {}
    load_summary: dict[str, dict[str, Any]] = {}
    for load in loads:
        pairs = grouped_effects[load]
        baseline_values = [float(row["baseline_throughput"]) for row in pairs]
        active_values = [float(row["active_throughput"]) for row in pairs]
        raw_by_load[load] = pairs
        baseline_mean = statistics.mean(baseline_values) if baseline_values else math.nan
        load_summary[load] = {
            "valid_pairs": len(pairs),
            "baseline_mean": baseline_mean,
            "active_mean": statistics.mean(active_values) if active_values else math.nan,
            "mean_uplift": by_load[load],
            "paired_uplift_bootstrap_95_ci": per_load_ci.get(load, [math.nan, math.nan]),
            "baseline_cv": (
                statistics.stdev(baseline_values) / baseline_mean
                if len(baseline_values) > 1 and baseline_mean else math.nan
            ),
            "baseline_p99_mean_us": statistics.mean(
                float(row["baseline_p99_latency_us"]) for row in pairs
            ) if pairs else math.nan,
            "active_p99_mean_us": statistics.mean(
                float(row["active_p99_latency_us"]) for row in pairs
            ) if pairs else math.nan,
        }
    result = {
        "schema": "prism-sampler.affinitygraph-result.v2",
        "complete": complete,
        "effects": effects,
        "raw_pairs_by_load": raw_by_load,
        "load_summary": load_summary,
        "mean_uplift_by_load": by_load,
        "weighted_mean_uplift": weighted,
        "weighted_bootstrap_95_ci": (
            [_percentile(bootstrap, 0.025), _percentile(bootstrap, 0.975)]
            if bootstrap else [math.nan, math.nan]
        ),
    }
    result["candidate_pass"] = bool(
        complete
        and
        all(value >= -0.05 for value in by_load.values())
        and (len(loads) == 1 or sum(value > 0 for value in by_load.values()) >= 2)
        and (len(loads) == 1 or weighted >= 0.03)
    )
    return result


def analyze_incremental_smoke(
    baseline: dict[str, Any], plan: dict[str, Any], active: dict[str, Any],
    cpu_to_node: dict[int, int],
) -> dict[str, Any]:
    def events(result: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            json.loads(line)
            for line in Path(result["runtime_log"]).read_text(encoding="utf-8").splitlines()
            if line
        ]

    active_events = events(active)
    active_plans = [row for row in active_events if row.get("type") == "plan"]
    shadow_plans = [row for row in events(plan) if row.get("type") == "plan"]
    solve_ms = [float(row.get("solve_duration_ns", 0)) / 1e6 for row in active_plans]
    initial_actions = [
        action
        for row in active_plans
        for action in row.get("actions", [])
        if action.get("initial_pin")
    ]
    incremental_actions = [
        action
        for row in active_plans
        for action in row.get("actions", [])
        if not action.get("initial_pin")
    ]
    cross_node_initial = sum(
        cpu_to_node.get(int(action.get("from_cpu", -1)))
        != cpu_to_node.get(int(action.get("target_cpu", -1)))
        for action in initial_actions
    )
    first_timestamp = min(
        (int(row["timestamp_ns"]) for row in active_events if "timestamp_ns" in row),
        default=0,
    )
    confirmed_timestamp = next(
        (
            int(row["timestamp_ns"])
            for row in active_plans
            if row.get("initial_plan_confirmed") and "timestamp_ns" in row
        ),
        0,
    )
    first_action_timestamp = next(
        (
            int(row["timestamp_ns"])
            for row in active_events
            if row.get("type") == "action" and int(row.get("committed", 0)) > 0
        ),
        0,
    )
    monitor_rows = [
        json.loads(line)
        for line in Path(active["runtime_log"]).with_name("runtime-monitor.jsonl")
        .read_text(encoding="utf-8").splitlines()
        if line
    ]
    first_monitor = float(monitor_rows[0]["monotonic"]) if monitor_rows else math.nan
    effective_monitor = next(
        (
            float(row["monotonic"])
            for row in monitor_rows
            if row.get("status", {}).get("active_effective")
        ),
        math.nan,
    )
    action_plans = [row for row in active_plans if row.get("actions")]
    generations = [
        int(row.get("generation", 0))
        for row in active_events
        if row.get("type") == "action_commit"
    ]
    throughput_ratio = float(active["throughput"]) / float(baseline["throughput"])
    return {
        "schema": "prism-sampler.affinitygraph-incremental-smoke.v1",
        "strategy_id": "incremental-hotspot-v1",
        "performance": {
            "baseline_throughput": float(baseline["throughput"]),
            "active_throughput": float(active["throughput"]),
            "throughput_ratio": throughput_ratio,
            "throughput_uplift": throughput_ratio - 1.0,
            "baseline_p99_latency_us": float(baseline["p99_latency_us"]),
            "active_p99_latency_us": float(active["p99_latency_us"]),
            "baseline_errors": int(baseline["error_count"]),
            "active_errors": int(active["error_count"]),
            "baseline_timeouts": int(baseline["timeout_count"]),
            "active_timeouts": int(active["timeout_count"]),
            "interpretation": "single smoke pair; directional evidence only",
        },
        "initialization": {
            "confirmation_seconds_from_runtime_start": (
                (confirmed_timestamp - first_timestamp) / 1e9
                if confirmed_timestamp and first_timestamp else math.nan
            ),
            "first_action_seconds_from_runtime_start": (
                (first_action_timestamp - first_timestamp) / 1e9
                if first_action_timestamp and first_timestamp else math.nan
            ),
            "active_effective_seconds_from_first_monitor": (
                effective_monitor - first_monitor
                if not math.isnan(effective_monitor) and not math.isnan(first_monitor)
                else math.nan
            ),
            "measurement_ready_wait_seconds": float(
                active.get("measurement_start", {}).get("active_ready_wait_seconds", 0)
            ),
            "initial_pin_actions": len(initial_actions),
            "initial_cross_node_actions": cross_node_initial,
        },
        "incremental_behavior": {
            "active_plan_windows": len(active_plans),
            "shadow_plan_windows": len(shadow_plans),
            "action_batches": len(action_plans),
            "migration_budget_compliant": all(
                len(row.get("actions", [])) <= int(row.get("migration_budget", 0))
                for row in action_plans
            ),
            "maximum_actions_per_batch": max(
                (len(row.get("actions", [])) for row in action_plans), default=0
            ),
            "incremental_actions_after_initial_pin": len(incremental_actions),
            "placement_generations": max(generations, default=0),
            "dirty_threads_mean": statistics.mean(
                int(row.get("dirty_threads", 0)) for row in active_plans
            ) if active_plans else 0,
            "dirty_threads_max": max(
                (int(row.get("dirty_threads", 0)) for row in active_plans), default=0
            ),
            "candidate_threads_mean": statistics.mean(
                int(row.get("candidate_threads", 0)) for row in active_plans
            ) if active_plans else 0,
            "candidate_threads_max": max(
                (int(row.get("candidate_threads", 0)) for row in active_plans), default=0
            ),
            "cooldown_skipped_threads_max": max(
                (int(row.get("cooldown_skipped_threads", 0)) for row in active_plans),
                default=0,
            ),
        },
        "solver_latency_ms": {
            "samples": len(solve_ms),
            "p50": _percentile(solve_ms, 0.50),
            "p95": _percentile(solve_ms, 0.95),
            "maximum": max(solve_ms, default=math.nan),
            "p95_below_one_second": bool(solve_ms and _percentile(solve_ms, 0.95) < 1000),
        },
        "cpu_to_node": {str(cpu): node for cpu, node in sorted(cpu_to_node.items())},
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _base_env_value(path: Path, key: str, default: str = "local") -> str:
    pattern = re.compile(rf"^\s*(?:export\s+)?{re.escape(key)}=(.*)$")
    for line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line)
        if not match:
            continue
        values = shlex.split(match.group(1), comments=True)
        if len(values) != 1 or not values[0]:
            break
        return values[0]
    return default


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


class AffinityGraphRunner:
    def __init__(
        self, root: Path, base_config: Path, source: Path, *, rounds: int, seed: int,
        allow_busy_smoke: bool = False, smoke_gates: str = "soft",
        smoke_baseline_result: Path | None = None, smoke_load: str = "C4T6",
        system: str = "clickhouse",
    ):
        self.root = root.resolve()
        self.base_config = base_config.resolve()
        self.source = source.resolve()
        self.rounds = rounds
        self.formal_loads = ("C4T4",) if system == "doris" else ("C2T2",)
        self.seed = seed
        self.allow_busy_smoke = allow_busy_smoke
        if system not in {"clickhouse", "doris"}:
            raise ValueError(f"unsupported system: {system}")
        if smoke_load not in SMOKE_LOADS:
            raise ValueError(f"unsupported smoke load: {smoke_load}")
        self.system = system
        self.smoke_load = smoke_load
        if smoke_gates not in {"soft", "strict"}:
            raise ValueError("smoke_gates must be soft or strict")
        self.smoke_gates = smoke_gates
        self.smoke_baseline_result = (
            smoke_baseline_result.resolve() if smoke_baseline_result else None
        )
        self.host = Host(_base_env_value(self.base_config, "SERVER_HOST"))
        self.client_host = Host(_base_env_value(self.base_config, "CLIENT_HOST"))
        self.yba_root = Path("/home/x/data/sched-ext-study/ycsb-bench-all-database")
        self.state_path = self.root / "resume-state.json"
        self.generated = self.root / "generated-config"
        self.runs = self.root / "runs"
        self.summary = self.root / "summary"
        for path in (self.generated, self.runs, self.summary):
            path.mkdir(parents=True, exist_ok=True)
        if self.state_path.is_file():
            self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
            if self.state["seed"] != seed or self.state["rounds"] != rounds:
                raise ValueError("cannot change seed or rounds for an existing run")
            if self.state.get("smoke_load", "C4T6") != smoke_load:
                raise ValueError("cannot change smoke load for an existing run")
            if self.state.get("system", "clickhouse") != system:
                raise ValueError("cannot change system for an existing run")
            scheduled_loads = {
                str(row.get("load")) for row in self.state.get("schedule", [])
            }
            if scheduled_loads != set(self.formal_loads):
                raise ValueError(
                    "existing run uses a different formal workload schedule"
                )
            if self.smoke_baseline_result is None and self.state.get(
                "smoke_baseline_result"
            ):
                self.smoke_baseline_result = Path(
                    self.state["smoke_baseline_result"]
                ).resolve()
        else:
            self.state = {
                "schema": "prism-sampler.affinitygraph-run.v2",
                "seed": seed,
                "rounds": rounds,
                "smoke_load": smoke_load,
                "system": system,
                "base_config": str(self.base_config),
                "source": str(self.source),
                "warmup_seconds": 30,
                "active_acquisition_seconds": 210,
                "smoke_measurement_seconds": 300,
                "smoke_observe_max_overhead_ratio": 0.05,
                "formal_measurement_seconds": 300,
                "started_realtime_ns": time.time_ns(),
                "schedule": schedule(rounds, seed, self.formal_loads),
                "steps": {},
                "runs": {},
            }
            self._save()
        self.state.setdefault("smoke_observe_max_overhead_ratio", 0.05)
        self.state.setdefault("active_acquisition_seconds", 210)
        self.state["smoke_gates"] = self.smoke_gates
        self.state["smoke_baseline_result"] = (
            str(self.smoke_baseline_result) if self.smoke_baseline_result else ""
        )
        self._save()
        self.release = str(self.state.get("release", ""))

    def _save(self) -> None:
        _write(self.state_path, json.dumps(self.state, indent=2, sort_keys=True) + "\n")

    def _experiment_fingerprint(self, release_fingerprint: str) -> str:
        hook = Path(__file__).with_name("affinitygraph_hook.py")
        payload = {
            "release_fingerprint": release_fingerprint,
            "runner_sha256": _sha256(Path(__file__)),
            "hook_sha256": _sha256(hook),
            "base_config_sha256": _sha256(self.base_config),
            "rounds": self.rounds,
            "seed": self.seed,
            "warmup_seconds": self.state["warmup_seconds"],
            "active_acquisition_seconds": self.state["active_acquisition_seconds"],
            "smoke_measurement_seconds": self.state["smoke_measurement_seconds"],
            "smoke_observe_max_overhead_ratio": self.state[
                "smoke_observe_max_overhead_ratio"
            ],
            "formal_measurement_seconds": self.state["formal_measurement_seconds"],
            "allow_busy_smoke": self.allow_busy_smoke,
            "smoke_gates": self.smoke_gates,
            "smoke_load": self.smoke_load,
            "system": self.system,
            "smoke_baseline_result_sha256": (
                _sha256(self.smoke_baseline_result)
                if self.smoke_baseline_result else ""
            ),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def _strict_remote_preflight(self, release: str) -> dict[str, Any]:
        config = self.generated / "strict-preflight.toml"
        _write(config, f"""[runtime]
mode = "observe"
sample_interval_seconds = 1
graph_horizon_seconds = 60
solve_interval_seconds = 10
minimum_confidence = 0.8
proposal_confirmations = 3
initial_proposal_confirmations = 1
minimum_dwell_seconds = 60
maximum_migrated_threads_ratio = 0.25
initial_migrated_threads_ratio = 1.0
collector_failure_restore_seconds = 30
solver = "numa-domain-v1"
affinity_granularity = "numa_node_mask"
family_minimum_demand = 1.0
family_minimum_internal_relation = 1.0
family_minimum_self_containment = 0.20
family_minimum_relative_internal = 0.10
domain_merge_ratio = 0.25
family_stability_confirmations = 3
domain_stability_confirmations = 3
domain_plan_confirmations = 3
maximum_threads_per_domain = 1024
domain_capacity_ratio = 0.80
domain_expand_ratio = 0.90
domain_expand_confirmations = 3
domain_shrink_ratio = 0.55
domain_shrink_confirmations = 6
domain_minimum_dwell_seconds = 300
initial_node_passes = 8
initial_node_thread_slack_ratio = 0.5
candidate_multiplier = 4
candidate_hard_limit = 64
rotating_scan_size = 32
demand_dirty_threshold = 0.05
edge_dirty_absolute_threshold = 1.0
edge_dirty_relative_threshold = 0.10
minimum_relative_gain = 0.05
maximum_threads_per_cpu = 2
thread_slot_slack = 0
future_demand_floor = 0.02
group_peak_demand_ratio = 0.25
group_peak_demand_cap = 0.05
group_peak_decay = 0.95
node_balance_threshold = 0.05
hotspot_edges_per_thread = 4
hotspot_edge_quantile = 0.95
hotspot_component_boost = 4.0
maximum_managed_threads = 64
managed_thread_hysteresis_ratio = 1.0
hotspot_replan_growth_ratio = 0.25
hotspot_replan_min_threads = 8
hotspot_stability_threshold = 0.65
hotspot_stability_confirmations = 3
active_demand_threshold = 0.05
inactive_demand_threshold = 0.0
log_directory = "/tmp/affinitygraph-preflight"
socket_path = "/tmp/affinitygraph-preflight.sock"

[resources]
cpus = "0-127"
calibration_path = "{release}/calibration/kunpeng920"

[collector]
required = true
pthread_uprobe = true

[calibration]
id = "clickhouse-gate2-fixed-v2"
activity_log_p95 = 2.4138804290562152
sync_log_p95 = 2.5591179487485345
share_log_p95 = 0.00894730347830295
""")
        remote_config = f"{release}/strict-preflight.toml"
        self.host.copy_to(config, remote_config)
        command = (
            "test \"$(uname -m)\" = aarch64 && "
            "case \"$(uname -r)\" in 6.12.*) ;; *) exit 41 ;; esac && "
            "test \"$(cat /sys/devices/system/cpu/online)\" = 0-127 && "
            "test \"$(find /sys/devices/system/node -maxdepth 1 -type d -name 'node[0-9]*' | wc -l)\" -eq 4 && "
            "test -x /usr/bin/clang-18 && test -x /usr/bin/clang++-18 && "
            "test -r /sys/kernel/btf/vmlinux && "
            f"SUDO_ASKPASS={shlex.quote(ASKPASS)} sudo -A "
            f"{shlex.quote(release + '/build/affinity-run')} preflight "
            f"--config {shlex.quote(remote_config)} "
            f"--bpf-object {shlex.quote(release + '/affinitygraph.bpf.o')}"
        )
        result = self.host.run(command)
        return {"command_output": result.stdout, "kernel": self.host.run("uname -r").stdout.strip()}

    def _cpu_to_node(self) -> dict[int, int]:
        result = self.host.run("""
for node_path in /sys/devices/system/node/node[0-9]*; do
  node=${node_path##*node}
  for cpu_path in "$node_path"/cpu[0-9]*; do
    test -e "$cpu_path" || continue
    cpu=${cpu_path##*cpu}
    printf '%s %s\n' "$cpu" "$node"
  done
done
""")
        mapping = {
            int(fields[0]): int(fields[1])
            for line in result.stdout.splitlines()
            if len(fields := line.split()) == 2
            and all(field.isdigit() for field in fields)
        }
        if set(mapping) != set(range(128)):
            raise RuntimeError(f"incomplete CPU-to-NUMA mapping: {len(mapping)} CPUs")
        return mapping

    def _step(self, name: str, action: Any) -> Any:
        previous = self.state["steps"].get(name, {})
        if previous.get("status") == "complete":
            return previous.get("result")
        self.state["steps"][name] = {"status": "running", "started_realtime_ns": time.time_ns()}
        self._save()
        try:
            result = action()
        except Exception as error:
            self.state["steps"][name] = {"status": "failed", "error": f"{type(error).__name__}: {error}"}
            self._save()
            raise
        self.state["steps"][name] = {"status": "complete", "result": result, "finished_realtime_ns": time.time_ns()}
        self._save()
        return result

    def deploy(self) -> dict[str, Any]:
        files = sorted(path for path in self.source.rglob("*") if path.is_file() and ".git" not in path.parts and "build" not in path.parts)
        digest = hashlib.sha256()
        for path in files:
            digest.update(str(path.relative_to(self.source)).encode())
            digest.update(path.read_bytes())
        release_id = digest.hexdigest()[:16]
        archive = self.root / f"affinitygraph-{release_id}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for path in files:
                tar.add(path, arcname=str(path.relative_to(self.source)))
        remote = f"/home/xhc/.local/share/affinitygraph/releases/{release_id}"
        remote_archive = f"/tmp/affinitygraph-{release_id}.tar.gz"
        self.host.run(f"mkdir -p {shlex.quote(remote)}")
        self.host.copy_to(archive, remote_archive)
        build = self.host.run(
            f"tar -xzf {shlex.quote(remote_archive)} -C {shlex.quote(remote)} && "
            f"mkdir -p {shlex.quote(remote + '/lib')} && "
            f"cp -L \"$(/usr/bin/clang++-18 -print-file-name=libstdc++.so.6)\" {shlex.quote(remote + '/lib/')} && "
            f"cp -L \"$(/usr/bin/clang++-18 -print-file-name=libgcc_s.so.1)\" {shlex.quote(remote + '/lib/')} && "
            f"make -C {shlex.quote(remote)} clean && "
            f"LD_LIBRARY_PATH={shlex.quote(remote + '/lib')} make -j8 -C {shlex.quote(remote)} runtime-test CXX=/usr/bin/clang++-18 && "
            f"make -C {shlex.quote(remote)} bpf CLANG=/usr/bin/clang-18 && "
            f"cp {shlex.quote(remote + '/build/affinitygraph.bpf.o')} {shlex.quote(remote + '/affinitygraph.bpf.o')} && "
            f"uname -r && uname -m && /usr/bin/clang-18 --version | head -1 && "
            f"/usr/bin/clang++-18 --version | head -1 && "
            f"sha256sum {shlex.quote(remote + '/build/affinity-run')} "
            f"{shlex.quote(remote + '/affinitygraph.bpf.o')} && "
            f"rm -f {shlex.quote(remote_archive)}"
        )
        self.release = remote
        self.state["release"] = remote
        self._save()
        manifest = {
            "release": remote,
            "release_id": release_id,
            "archive_sha256": _sha256(archive),
            "remote_build_output": build.stdout,
        }
        sha_rows = [line.split() for line in build.stdout.splitlines() if len(line.split()) == 2]
        manifest["affinity_run_sha256"] = next(
            (fields[0] for fields in sha_rows if fields[1].endswith("/build/affinity-run")), ""
        )
        manifest["bpf_sha256"] = next(
            (fields[0] for fields in sha_rows if fields[1].endswith("/affinitygraph.bpf.o")), ""
        )
        manifest["fingerprint"] = hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode()
        ).hexdigest()
        manifest["preflight"] = self._strict_remote_preflight(remote)
        manifest["experiment_fingerprint"] = self._experiment_fingerprint(manifest["fingerprint"])
        self.state["experiment_fingerprint"] = manifest["experiment_fingerprint"]
        self._save()
        _write(self.root / "build-manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        return manifest

    def _scenario(
        self, load: str, seconds: int, *, acquisition_seconds: int = 0
    ) -> Path:
        clients, threads = SMOKE_LOADS[load]
        warmup = int(self.state["warmup_seconds"])
        acquisition = int(acquisition_seconds)
        if acquisition < 0:
            raise ValueError("acquisition_seconds cannot be negative")
        suffix = f"-a{acquisition}" if acquisition else ""
        phases = f"PLACEMENT WARMUP {load}" if acquisition else f"WARMUP {load}"
        placement = ""
        if acquisition:
            placement = f"""SCENARIO_PHASE_PLACEMENT_CLIENTS={clients}
SCENARIO_PHASE_PLACEMENT_THREADS={threads}
SCENARIO_PHASE_PLACEMENT_VALUE={acquisition}
"""
        path = self.generated / f"scenario-{load}{suffix}-w{warmup}-m{seconds}.env"
        _write(path, f"""SCENARIO_NAME=affinitygraph_{load.lower()}{suffix}_w{warmup}_m{seconds}
SCENARIO_BUDGET_MODE=duration
SCENARIO_PHASES='{phases}'
SCENARIO_MIN_PHASE_SECONDS=30
SCENARIO_TIMEOUT_GRACE_SECONDS=45
SCENARIO_DURATION_TOLERANCE_SECONDS=2
SCENARIO_DURATION_OPERATIONCOUNT_PER_CLIENT=2147483647
{placement}SCENARIO_PHASE_WARMUP_CLIENTS={clients}
SCENARIO_PHASE_WARMUP_THREADS={threads}
SCENARIO_PHASE_WARMUP_VALUE={warmup}
SCENARIO_PHASE_{load}_CLIENTS={clients}
SCENARIO_PHASE_{load}_THREADS={threads}
SCENARIO_PHASE_{load}_VALUE={seconds}
""")
        return path

    def _runtime_config(self, name: str, mode: str, run_dir: Path) -> tuple[Path, str, str]:
        remote_root = f"/home/xhc/.local/share/affinitygraph/experiments/{self.root.name}/{name}"
        log_dir = f"{remote_root}/log"
        socket_id = hashlib.sha256(f"{self.root.name}/{name}".encode()).hexdigest()[:20]
        socket = f"/tmp/affinitygraph-{socket_id}.sock"
        config = self.generated / f"{name}.toml"
        if self.system == "doris":
            calibration = """id = "doris-20260721-p95-v1"
activity_log_p95 = 0.4502686594634245
sync_log_p95 = 5.820214927005448
share_log_p95 = 0.0012199491501589136"""
        else:
            calibration = """id = "clickhouse-gate2-fixed-v2"
activity_log_p95 = 2.4138804290562152
sync_log_p95 = 2.5591179487485345
share_log_p95 = 0.00894730347830295"""
        _write(config, f"""[runtime]
mode = "{mode}"
sample_interval_seconds = 1
graph_horizon_seconds = 60
solve_interval_seconds = 10
minimum_confidence = 0.8
proposal_confirmations = 3
initial_proposal_confirmations = 1
minimum_dwell_seconds = 60
maximum_migrated_threads_ratio = 0.25
initial_migrated_threads_ratio = 1.0
collector_failure_restore_seconds = 30
solver = "numa-domain-v1"
affinity_granularity = "numa_node_mask"
family_minimum_demand = 1.0
family_minimum_internal_relation = 1.0
family_minimum_self_containment = 0.20
family_minimum_relative_internal = 0.10
domain_merge_ratio = 0.25
family_stability_confirmations = 3
domain_stability_confirmations = 3
domain_plan_confirmations = 3
maximum_threads_per_domain = 1024
domain_capacity_ratio = 0.80
domain_expand_ratio = 0.90
domain_expand_confirmations = 3
domain_shrink_ratio = 0.55
domain_shrink_confirmations = 6
domain_minimum_dwell_seconds = 300
initial_node_passes = 8
initial_node_thread_slack_ratio = 0.5
candidate_multiplier = 4
candidate_hard_limit = 64
rotating_scan_size = 32
demand_dirty_threshold = 0.05
edge_dirty_absolute_threshold = 1.0
edge_dirty_relative_threshold = 0.10
minimum_relative_gain = 0.05
maximum_threads_per_cpu = 2
thread_slot_slack = 0
future_demand_floor = 0.02
group_peak_demand_ratio = 0.25
group_peak_demand_cap = 0.05
group_peak_decay = 0.95
node_balance_threshold = 0.05
hotspot_edges_per_thread = 4
hotspot_edge_quantile = 0.95
hotspot_component_boost = 4.0
maximum_managed_threads = 64
managed_thread_hysteresis_ratio = 1.0
hotspot_replan_growth_ratio = 0.25
hotspot_replan_min_threads = 8
hotspot_stability_threshold = 0.65
hotspot_stability_confirmations = 3
active_demand_threshold = 0.05
inactive_demand_threshold = 0.0
log_directory = "{log_dir}"
socket_path = "{socket}"

[resources]
cpus = "0-127"
calibration_path = "{self.release}/calibration/kunpeng920"

[collector]
required = true
pthread_uprobe = true

[calibration]
{calibration}
""")
        remote_config = f"{remote_root}/affinitygraph.toml"
        self.host.run(
            f"rm -rf {shlex.quote(remote_root)} && rm -f {shlex.quote(socket)} && "
            f"mkdir -p {shlex.quote(log_dir)} && "
            f"chmod 700 {shlex.quote(remote_root)} {shlex.quote(log_dir)}"
        )
        self.host.copy_to(config, remote_config)
        wrapper = run_dir / f"{self.system}-affinitygraph"
        _write(wrapper, f"""#!/usr/bin/env bash
exec env SUDO_ASKPASS={shlex.quote(ASKPASS)} sudo -A env CLICKHOUSE_WATCHDOG_ENABLE=0 /lib/ld-linux-aarch64.so.1 \\
  --library-path {shlex.quote(self.release + '/lib')} {shlex.quote(self.release + '/build/affinity-run')} run \\
  --config {shlex.quote(remote_config)} \\
  --bpf-object {shlex.quote(self.release + '/affinitygraph.bpf.o')} \\
  --user xhc -- {shlex.quote(REAL_CLICKHOUSE)} "$@"
""")
        if self.system == "doris":
            _write(wrapper, f"""#!/usr/bin/env bash
set -euo pipefail
log={shlex.quote(log_dir + '/doris-supervisor.out')}
nohup env SUDO_ASKPASS={shlex.quote(ASKPASS)} sudo -A \
  /lib/ld-linux-aarch64.so.1 --library-path {shlex.quote(self.release + '/lib')} \
  {shlex.quote(self.release + '/build/affinity-run')} run \
  --config {shlex.quote(remote_config)} \
  --bpf-object {shlex.quote(self.release + '/affinitygraph.bpf.o')} \
  --user xhc -- /usr/bin/bash {shlex.quote(DORIS_START_BE)} --console \
  >>"$log" 2>&1 </dev/null &
""")
        wrapper.chmod(0o755)
        remote_wrapper = f"{remote_root}/{self.system}-affinitygraph"
        self.host.copy_to(wrapper, remote_wrapper)
        self.host.run(f"chmod 755 {shlex.quote(remote_wrapper)}")
        return config, socket, remote_wrapper

    def _baseline_wrapper(self, name: str, run_dir: Path) -> str:
        remote_root = f"/home/xhc/.local/share/affinitygraph/experiments/{self.root.name}/{name}"
        self.host.run(f"rm -rf {shlex.quote(remote_root)} && mkdir -p {shlex.quote(remote_root)}")
        wrapper = run_dir / "clickhouse-baseline"
        _write(wrapper, f"""#!/usr/bin/env bash
exec env CLICKHOUSE_WATCHDOG_ENABLE=0 {shlex.quote(REAL_CLICKHOUSE)} "$@"
""")
        wrapper.chmod(0o755)
        remote_wrapper = f"{remote_root}/clickhouse-baseline"
        self.host.copy_to(wrapper, remote_wrapper)
        self.host.run(f"chmod 755 {shlex.quote(remote_wrapper)}")
        return remote_wrapper

    def _remote_hook(
        self, name: str, run_dir: Path, treatment: str, socket: str,
        measurement_phase: str | list[str], active_ready_phase: str,
    ) -> tuple[str, str]:
        source_root = "/home/xhc/.local/src/prism-sampler"
        remote_module = f"{source_root}/prism_sampler/affinitygraph_hook.py"
        self.client_host.copy_to(Path(__file__).with_name("affinitygraph_hook.py"), remote_module)
        remote_root = f"/home/xhc/.local/share/affinitygraph-hook-runs/{self.root.name}/{name}"
        self.client_host.run(f"rm -rf {shlex.quote(remote_root)} && mkdir -p {shlex.quote(remote_root)}")
        wrapper = run_dir / "affinitygraph-remote-hook"
        target_exe = REAL_DORIS_BE if self.system == "doris" else REAL_CLICKHOUSE
        phases = (
            measurement_phase if isinstance(measurement_phase, list)
            else [measurement_phase]
        )
        _write(wrapper, f"""#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH={shlex.quote(source_root)}
export AFFINITYGRAPH_TREATMENT={shlex.quote(treatment)}
export AFFINITYGRAPH_HOOK_ROOT={shlex.quote(remote_root)}
export AFFINITYGRAPH_MEASUREMENT_PHASE={shlex.quote(phases[0])}
export AFFINITYGRAPH_MEASUREMENT_PHASES={shlex.quote(','.join(phases))}
export AFFINITYGRAPH_ACTIVE_READY_PHASE={shlex.quote(active_ready_phase)}
export AFFINITYGRAPH_CTL={shlex.quote(self.release + '/build/affinityctl')}
export AFFINITYGRAPH_LIBRARY_PATH={shlex.quote(self.release + '/lib')}
export AFFINITYGRAPH_SOCKET={shlex.quote(socket)}
export AFFINITYGRAPH_TARGET_EXE={shlex.quote(target_exe)}
export AFFINITYGRAPH_SERVER_HOST={shlex.quote(self.host.ssh)}
export AFFINITYGRAPH_ACTIVE_READY_TIMEOUT_SECONDS=60
export AFFINITYGRAPH_RELAY_TIMEOUT_SECONDS=90
export AFFINITYGRAPH_RELAY=1
exec python3 -m prism_sampler.affinitygraph_hook "$@"
""")
        remote_wrapper = f"{remote_root}/hook"
        self.client_host.copy_to(wrapper, remote_wrapper)
        self.client_host.run(f"chmod 700 {shlex.quote(remote_wrapper)}")
        return remote_root, remote_wrapper

    def _fetch_remote_hooks(self, remote_root: str, local_root: Path) -> None:
        archive = f"/tmp/affinitygraph-hook-{hashlib.sha256(remote_root.encode()).hexdigest()[:16]}.tar.gz"
        self.client_host.run(
            f"tar -C {shlex.quote(remote_root)} -czf {shlex.quote(archive)} ."
        )
        local_archive = local_root.parent / "remote-hooks.tar.gz"
        self.client_host.copy_from(archive, local_archive)
        local_root.mkdir(parents=True, exist_ok=True)
        with tarfile.open(local_archive, "r:gz") as stream:
            stream.extractall(local_root)
        self.client_host.run(f"rm -f {shlex.quote(archive)}")
        local_archive.unlink(missing_ok=True)

    def _idle_gate(self, *, allow_busy: bool = False, soft: bool = False) -> dict[str, Any]:
        if allow_busy:
            return {"passed": False, "bypassed": True, "busy_ratio": math.nan}
        cleanliness = """
test -z "$(pgrep -x clickhouse || true)"
test -z "$(pgrep -x doris_be || true)"
test -z "$(pgrep -f 'org[.]apache[.]doris[.]DorisFE' || true)"
test -z "$(pgrep -f '[a]ffinity-run' || true)"
test -z "$(pgrep -f '[m]etric-collector' || true)"
test -z "$(find /tmp -maxdepth 1 -type s -name 'affinitygraph-*.sock' -print -quit 2>/dev/null)"
test ! -e /sys/fs/bpf/affinitygraph
"""
        self.host.run(cleanliness)
        script = """
python3 - <<'PY'
import time
def sample():
    fields = [int(v) for v in open('/proc/stat').readline().split()[1:]]
    return sum(fields), fields[3] + fields[4]
start_total, start_idle = sample()
time.sleep(30)
end_total, end_idle = sample()
busy = 1.0 - (end_idle - start_idle) / max(end_total - start_total, 1)
print(f"affinitygraph_idle_busy_ratio={busy:.6f}")
PY
"""
        result = self.host.run(script, timeout=45)
        busy = float(result.stdout.strip().rsplit("=", 1)[-1])
        report = {
            "passed": busy < 0.05,
            "bypassed": False,
            "busy_ratio": busy,
            "maximum_busy_ratio": 0.05,
            "enforcement": "warning" if soft else "error",
        }
        if not report["passed"] and not soft:
            raise RuntimeError(f"host idle gate failed: {report}")
        return report

    def _baseline_reference(self) -> dict[str, Any]:
        if not self.smoke_baseline_result:
            raise RuntimeError("smoke baseline result was not configured")
        result = json.loads(self.smoke_baseline_result.read_text(encoding="utf-8"))
        required = {
            "treatment": "baseline", "load": self.smoke_load,
        }
        for field, expected in required.items():
            if result.get(field) != expected:
                raise RuntimeError(
                    f"invalid smoke baseline {field}: {result.get(field)!r}"
                )
        for field in (
            "throughput", "p99_latency_us", "error_count", "timeout_count",
            "runtime_ms",
        ):
            if field not in result:
                raise RuntimeError(f"smoke baseline is missing {field}")
        if float(result["runtime_ms"]) < self.state["smoke_measurement_seconds"] * 1000 * 0.95:
            raise RuntimeError("smoke baseline measurement is too short")
        value = dict(result)
        value["reference"] = {
            "path": str(self.smoke_baseline_result),
            "sha256": _sha256(self.smoke_baseline_result),
            "source_experiment": self.smoke_baseline_result.parents[2].name,
        }
        return value

    def _post_cleanup_gate(self, socket: str = "") -> None:
        checks = [
            "test -z \"$(pgrep -x clickhouse || true)\"",
            "test -z \"$(pgrep -x doris_be || true)\"",
            "test -z \"$(pgrep -f 'org[.]apache[.]doris[.]DorisFE' || true)\"",
            "test -z \"$(pgrep -f '[a]ffinity-run' || true)\"",
            "test -z \"$(pgrep -f '[m]etric-collector' || true)\"",
        ]
        if socket:
            checks.append(f"test ! -e {shlex.quote(socket)}")
        self.host.run(" && ".join(checks))

    def _prism_config(self, name: str, run_dir: Path) -> Path:
        path = self.generated / f"{name}-prism.toml"
        _write(path, f"""[experiment]
output_root = "{run_dir / 'prism'}"
system = "{self.system}"

[target]
host = "{self.host.ssh}"
sudo = "SUDO_ASKPASS={ASKPASS} sudo -A"
remote_root = "/home/xhc/prism-sampler/data/affinitygraph-{self.root.name}"

[collector]
binary = "/home/xhc/prism-sampler/bin/metric-collector"
runtime_lib = "/home/xhc/prism-sampler/lib"
attach_wait_seconds = 12
stop_timeout_seconds = 30
subsystems = ["taskstats", "vfs", "futex"]
required_subsystems = ["taskstats", "futex"]
best_effort = true

[yba]
root = "{self.yba_root}"

[sampling]
profile = "minimal"
interval_seconds = 10
platform = "kunpeng-920"
collect_phase_patterns = ["^{self.smoke_load}$"]

[controller]
mode = "off"
""")
        return path

    def _parse_kpi(self, yba_output: Path, load: str) -> dict[str, Any]:
        candidates: list[tuple[int, Path, dict[str, str]]] = []
        for summary in yba_output.glob("phases/*/summary.csv"):
            with summary.open(newline="", encoding="utf-8") as stream:
                values = list(csv.DictReader(stream))
            if len(values) != 1:
                raise RuntimeError(f"invalid summary: {summary}")
            row = values[0]
            if row.get("label") != load:
                continue
            # YBA 会为兼容旧的两阶段布局生成 0 秒占位目录。它没有 client
            # log，也不是真实 measurement，不能与 placement 后的 phase 混淆。
            if int(float(row.get("client_logs") or 0)) <= 0 or float(
                row.get("runtime_ms_max") or 0
            ) <= 0:
                continue
            prefix = summary.parent.name.split("-", 1)[0]
            if not prefix.isdigit():
                raise RuntimeError(f"invalid YBA phase directory: {summary.parent}")
            candidates.append((int(prefix), summary, row))
        if not candidates:
            raise RuntimeError(f"no completed YBA measurement summary for {load}")
        maximum_sequence = max(sequence for sequence, _, _ in candidates)
        selected = [item for item in candidates if item[0] == maximum_sequence]
        if len(selected) != 1:
            raise RuntimeError(f"ambiguous YBA measurement summaries: {selected}")
        _, _, row = selected[0]
        return {
            "throughput": float(row["throughput"]),
            "p99_latency_us": float(row["p99_latency"]),
            "error_count": int(float(row["error_count"])),
            "timeout_count": int(float(row["timeout_count"])),
            "runtime_ms": float(row["runtime_ms_max"]),
        }

    @staticmethod
    def _parse_trajectory_kpi(
        yba_output: Path, measurement_phases: list[str]
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for phase in measurement_phases:
            matches = sorted(yba_output.glob(f"phases/*-{phase}/summary.csv"))
            valid = []
            for path in matches:
                with path.open(newline="", encoding="utf-8") as stream:
                    values = list(csv.DictReader(stream))
                if len(values) != 1:
                    raise RuntimeError(f"invalid phase summary: {path}")
                row = values[0]
                if int(float(row.get("client_logs") or 0)) > 0 and float(
                    row.get("runtime_ms_max") or 0
                ) > 0:
                    valid.append((path, row))
            if len(valid) != 1:
                raise RuntimeError(
                    f"expected one completed summary for {phase}, got {valid}"
                )
            path, row = valid[0]
            load = phase.split("_", 1)[1] if "_" in phase else phase
            rows.append({
                "phase": phase,
                "load": load,
                "summary_path": str(path),
                "throughput": float(row["throughput"]),
                "p99_latency_us": float(row["p99_latency"]),
                "p95_latency_us": float(row.get("p95_latency", 0)),
                "p999_latency_us": float(row.get("p999_latency", 0)),
                "error_count": int(float(row.get("error_count", 0))),
                "timeout_count": int(float(row.get("timeout_count", 0))),
                "runtime_ms": float(row["runtime_ms_max"]),
                "operations": float(row.get("read_ops", 0)),
            })
        timeline_path = yba_output / "scenario-timeline.csv"
        with timeline_path.open(newline="", encoding="utf-8") as stream:
            timeline = list(csv.DictReader(stream))
        measured_timeline = [row for row in timeline if row.get("phase") in measurement_phases]
        if len(measured_timeline) != len(measurement_phases):
            raise RuntimeError("trajectory timeline does not contain every measurement phase")
        operations = sum(row["operations"] for row in rows)
        active_seconds = sum(row["runtime_ms"] for row in rows) / 1000.0
        first_ns = int(measured_timeline[0]["started_epoch_ns"])
        last_ns = int(measured_timeline[-1]["finished_epoch_ns"])
        lifecycle_seconds = (last_ns - first_ns) / 1e9
        gaps = []
        for previous, current in zip(measured_timeline, measured_timeline[1:]):
            gaps.append({
                "from": previous["phase"],
                "to": current["phase"],
                "seconds": (
                    int(current["started_epoch_ns"])
                    - int(previous["finished_epoch_ns"])
                ) / 1e9,
            })
        return {
            "measurement_phases": rows,
            "operations": operations,
            "active_throughput": operations / active_seconds if active_seconds else 0.0,
            "lifecycle_throughput": (
                operations / lifecycle_seconds if lifecycle_seconds else 0.0
            ),
            "throughput": operations / active_seconds if active_seconds else 0.0,
            "p99_latency_us": max(row["p99_latency_us"] for row in rows),
            "p95_latency_us": max(row["p95_latency_us"] for row in rows),
            "p999_latency_us": max(row["p999_latency_us"] for row in rows),
            "error_count": sum(row["error_count"] for row in rows),
            "timeout_count": sum(row["timeout_count"] for row in rows),
            "runtime_ms": active_seconds * 1000,
            "measurement_lifecycle_seconds": lifecycle_seconds,
            "phase_gaps": gaps,
        }

    def _ctl_status(self, socket: str) -> dict[str, Any] | None:
        command = (
            f"/lib/ld-linux-aarch64.so.1 --library-path {shlex.quote(self.release + '/lib')} "
            f"{shlex.quote(self.release + '/build/affinityctl')} status "
            f"--socket {shlex.quote(socket)}"
        )
        response = self.host.run(command, check=False)
        if response.returncode or not response.stdout.strip().startswith("{"):
            return None
        return json.loads(response.stdout)

    def _monitor_treatment(
        self,
        process: subprocess.Popen[Any],
        *,
        treatment: str,
        load: str,
        socket: str,
        hook_root: Path,
        remote_hook_root: str,
        run_dir: Path,
        hook_env: dict[str, str],
        soft_quality_gates: bool = False,
        measurement_phases: list[str] | None = None,
        lifecycle_timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        samples: list[dict[str, Any]] = []
        monitor_path = run_dir / "runtime-monitor.jsonl"
        active_seconds = 0.0
        cohort_workload_seconds = 0.0
        cohort_effective_seconds = 0.0
        cohort_workload_samples = 0
        cohort_effective_samples = 0
        last_poll = time.monotonic()
        first_cpu_ticks: int | None = None
        last_cpu_ticks: int | None = None
        first_cpu_time: float | None = None
        last_cpu_time: float | None = None
        peak_rss_kib = 0
        resource_samples = 0
        first_host_total: int | None = None
        first_host_idle: int | None = None
        last_host_total: int | None = None
        last_host_idle: int | None = None
        maximum_load_1m = 0.0
        maximum_poll_gap_seconds = 0.0
        hard_failure = ""
        warnings: list[dict[str, Any]] = []
        consecutive_status_failures = 0
        loss_warning_recorded = False
        processed_requests: set[str] = set()
        monitor_start_name = (
            "phase_before-PLACEMENT.json"
            if treatment == "active" else "phase_before-WARMUP.json"
        )
        phases = measurement_phases or [load]
        measurement_names = [f"phase_before-{phase}.json" for phase in phases]
        measurement_after_names = [f"phase_after-{phase}.json" for phase in phases]
        previous_measurement_open = False
        previous_active = False
        previous_cohort_workload = False
        previous_cohort_effective = False
        lifecycle_started = time.monotonic()

        while process.poll() is None:
            if (
                lifecycle_timeout_seconds is not None
                and time.monotonic() - lifecycle_started >= lifecycle_timeout_seconds
            ):
                hard_failure = (
                    f"YBA lifecycle exceeded {lifecycle_timeout_seconds}s"
                )
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                break
            remote_files = set(self.client_host.run(
                f"find {shlex.quote(remote_hook_root)} -maxdepth 1 -type f -printf '%f\\n'",
                check=False,
            ).stdout.splitlines())
            for request_name in sorted(
                name for name in remote_files
                if name.startswith("request-") and name.endswith(".json")
                and name not in processed_requests
            ):
                request_path = hook_root / "relay" / request_name
                response_name = request_name.replace("request-", "response-", 1)
                response_path = hook_root / "relay" / response_name
                remote_response = f"{remote_hook_root}/{response_name}"
                remote_temporary = remote_response + ".tmp"
                try:
                    self.client_host.copy_from(
                        f"{remote_hook_root}/{request_name}", request_path
                    )
                    request = json.loads(request_path.read_text(encoding="utf-8"))
                    context_path = request_path.with_name(request_name + ".context")
                    _write(
                        context_path,
                        json.dumps(request["context"], indent=2, sort_keys=True) + "\n",
                    )
                    local_env = hook_env.copy()
                    local_env.pop("AFFINITYGRAPH_RELAY", None)
                    result = subprocess.run(
                        [sys.executable, "-m", "prism_sampler.affinitygraph_hook",
                         str(request["event"]), str(context_path)],
                        env=local_env, text=True, capture_output=True, check=False,
                    )
                    response = {
                        "returncode": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    }
                except Exception as error:
                    response = {
                        "returncode": 1,
                        "stdout": "",
                        "stderr": f"hook relay failure: {type(error).__name__}: {error}\n",
                    }
                _write(response_path, json.dumps(response, indent=2, sort_keys=True) + "\n")
                self.client_host.copy_to(response_path, remote_temporary)
                self.client_host.run(
                    f"mv {shlex.quote(remote_temporary)} {shlex.quote(remote_response)}"
                )
                processed_requests.add(request_name)
            warmup_started = (hook_root / monitor_start_name).is_file()
            phase_open = [
                (hook_root / before).is_file() and not (hook_root / after).is_file()
                for before, after in zip(measurement_names, measurement_after_names)
            ]
            measurement_started = any(
                (hook_root / name).is_file() for name in measurement_names
            )
            measurement_finished = (hook_root / measurement_after_names[-1]).is_file()
            status = self._ctl_status(socket) if warmup_started else None
            now = time.monotonic()
            measurement_open = any(phase_open)
            current_active = bool(status and status.get("active_effective"))
            current_cohort_workload = bool(
                status and int(status.get("active_cohort_threads", 0)) > 0
            )
            current_cohort_effective = current_cohort_workload and current_active
            poll_gap = max(0.0, now - last_poll)
            maximum_poll_gap_seconds = max(maximum_poll_gap_seconds, poll_gap)
            active_seconds += _continuous_active_interval(
                last_poll,
                now,
                previous_measurement_open=previous_measurement_open,
                current_measurement_open=measurement_open,
                previous_active=previous_active,
                current_active=current_active,
            )
            cohort_workload_seconds += _continuous_active_interval(
                last_poll,
                now,
                previous_measurement_open=previous_measurement_open,
                current_measurement_open=measurement_open,
                previous_active=previous_cohort_workload,
                current_active=current_cohort_workload,
            )
            cohort_effective_seconds += _continuous_active_interval(
                last_poll,
                now,
                previous_measurement_open=previous_measurement_open,
                current_measurement_open=measurement_open,
                previous_active=previous_cohort_effective,
                current_active=current_cohort_effective,
            )
            if measurement_open and current_cohort_workload:
                cohort_workload_samples += 1
                if current_cohort_effective:
                    cohort_effective_samples += 1
            previous_measurement_open = measurement_open
            previous_active = current_active
            previous_cohort_workload = current_cohort_workload
            previous_cohort_effective = current_cohort_effective
            if warmup_started and not measurement_finished:
                if status is None:
                    consecutive_status_failures += 1
                    if consecutive_status_failures >= 3:
                        hard_failure = (
                            "AffinityGraph control status unavailable for three consecutive polls"
                        )
                    elif soft_quality_gates:
                        warnings.append({
                            "gate": "control_status", "severity": "warning",
                            "observed": "unavailable",
                            "consecutive_failures": consecutive_status_failures,
                        })
                elif (
                    not status.get("bpf")
                    or status.get("collector_degraded")
                    or status.get("effective_mode") != treatment
                    or status.get("fatal_error")
                ):
                    hard_failure = f"invalid required BPF status: {status}"
                elif status.get("bpf_window_ready") and float(
                    status.get("bpf_window_loss_ratio", 0)
                ) >= 0.01:
                    if soft_quality_gates:
                        if not loss_warning_recorded:
                            warnings.append({
                                "gate": "bpf_window_loss", "severity": "warning",
                                "observed": float(status.get("bpf_window_loss_ratio", 0)),
                                "threshold": 0.01,
                            })
                            loss_warning_recorded = True
                    else:
                        hard_failure = f"30-second BPF loss threshold exceeded: {status}"
                else:
                    consecutive_status_failures = 0
            if status:
                supervisor_pid = int(status.get("supervisor_pid", 0))
                if supervisor_pid:
                    usage = self.host.run(
                        f"awk '{{print $14 + $15}}' /proc/{supervisor_pid}/stat 2>/dev/null; "
                        f"awk '/^VmRSS:/ {{print $2}}' /proc/{supervisor_pid}/status 2>/dev/null; "
                        "awk 'NR == 1 {total=0; for (i=2; i<=NF; i++) total += $i; "
                        "print total, $5 + $6}' /proc/stat; cut -d' ' -f1 /proc/loadavg",
                        check=False,
                    ).stdout.splitlines()
                    if usage and usage[0].strip().isdigit():
                        ticks = int(usage[0])
                        first_cpu_ticks = ticks if first_cpu_ticks is None else first_cpu_ticks
                        first_cpu_time = now if first_cpu_time is None else first_cpu_time
                        last_cpu_ticks, last_cpu_time = ticks, now
                        resource_samples += 1
                    if len(usage) > 1 and usage[1].strip().isdigit():
                        peak_rss_kib = max(peak_rss_kib, int(usage[1]))
                host_sample: dict[str, Any] = {}
                if len(usage) > 2:
                    fields = usage[2].split()
                    if len(fields) == 2 and all(field.isdigit() for field in fields):
                        total, idle = int(fields[0]), int(fields[1])
                        first_host_total = total if first_host_total is None else first_host_total
                        first_host_idle = idle if first_host_idle is None else first_host_idle
                        last_host_total, last_host_idle = total, idle
                        host_sample.update(total_ticks=total, idle_ticks=idle)
                if len(usage) > 3:
                    try:
                        load_1m = float(usage[3])
                        maximum_load_1m = max(maximum_load_1m, load_1m)
                        host_sample["load_1m"] = load_1m
                    except ValueError:
                        pass
                sample = {"monotonic": now, "status": status, "host": host_sample}
                samples.append(sample)
                with monitor_path.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(sample, sort_keys=True) + "\n")
            if hard_failure:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                break
            last_poll = now
            time.sleep(1)

        try:
            code = process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            code = process.wait(timeout=10)
            hard_failure = hard_failure or "YBA did not stop after hard-failure recovery"
        cpu_cores = 0.0
        host_busy_ratio = 0.0
        if (
            first_cpu_ticks is not None and last_cpu_ticks is not None
            and first_cpu_time is not None and last_cpu_time is not None
            and last_cpu_time > first_cpu_time
        ):
            cpu_cores = (
                (last_cpu_ticks - first_cpu_ticks) / os.sysconf("SC_CLK_TCK")
                / (last_cpu_time - first_cpu_time)
            )
        if (
            first_host_total is not None and last_host_total is not None
            and first_host_idle is not None and last_host_idle is not None
            and last_host_total > first_host_total
        ):
            host_busy_ratio = 1.0 - (
                (last_host_idle - first_host_idle) / (last_host_total - first_host_total)
            )
        return {
            "returncode": code,
            "hard_failure": hard_failure,
            "active_effective_seconds": active_seconds,
            "cohort_workload_seconds": cohort_workload_seconds,
            "cohort_effective_seconds": cohort_effective_seconds,
            "cohort_effective_coverage_ratio": (
                cohort_effective_seconds / cohort_workload_seconds
                if cohort_workload_seconds > 0 else 1.0
            ),
            "cohort_workload_samples": cohort_workload_samples,
            "cohort_effective_samples": cohort_effective_samples,
            "runtime_average_cpu_cores": cpu_cores,
            "runtime_peak_rss_kib": peak_rss_kib,
            "runtime_resource_samples": resource_samples,
            "host_busy_ratio": host_busy_ratio,
            "host_maximum_load_1m": maximum_load_1m,
            "maximum_poll_gap_seconds": maximum_poll_gap_seconds,
            "samples": len(samples),
            "warnings": warnings,
        }

    def run_lifecycle(
        self, treatment: str, load: str, round_number: int, name: str, *,
        seconds: int, with_prism: bool = False, allow_busy_idle: bool = False,
        soft_quality_gates: bool = False,
        static_profile: str | None = None,
        profile_env: dict[str, str] | None = None,
        scenario_path: Path | None = None,
        measurement_phases: list[str] | None = None,
        lifecycle_timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        run_dir = self.runs / name
        if run_dir.exists():
            archive = self.runs / "superseded" / f"{name}-{time.time_ns()}"
            archive.parent.mkdir(parents=True, exist_ok=True)
            run_dir.rename(archive)
        run_dir.mkdir(parents=True, exist_ok=True)
        environment_gate = self._idle_gate(
            allow_busy=allow_busy_idle, soft=soft_quality_gates
        )
        yba_output = run_dir / f"yba-{name}"
        hook_root = run_dir / "hook"
        phases = measurement_phases or [load]
        env = os.environ.copy()
        env.update({
            "ENABLE_METRICS": "0",
            "ENABLE_REALTIME_KPI": "1",
            "ENABLE_THREAD_CLUSTER": "0",
            "SERVER_CPU_NODES": "",
            "SERVER_MEMORY_NODES": "",
            "CLICKHOUSE_MAX_THREADS": "32",
            "CLICKHOUSE_CONCURRENT_THREADS": "128",
            "CLICKHOUSE_CONCURRENT_RATIO": "0",
            "TIMESTAMP": name,
            "EXPERIMENT_NAME": name,
            "EXPERIMENT_DIR": str(yba_output),
            "AFFINITYGRAPH_TREATMENT": treatment,
            "AFFINITYGRAPH_HOOK_ROOT": str(hook_root),
            "AFFINITYGRAPH_MEASUREMENT_PHASE": load,
            "AFFINITYGRAPH_MEASUREMENT_PHASES": ",".join(phases),
            "AFFINITYGRAPH_ACTIVE_READY_PHASE": (
                "WARMUP" if treatment == "active" else load
            ),
            "AFFINITYGRAPH_ACTIVE_READY_TIMEOUT_SECONDS": "60",
            "AFFINITYGRAPH_RELAY_TIMEOUT_SECONDS": "90",
            # active readiness 和逐 TID mask 校验属于安全门禁，不能随 smoke
            # 环境质量门禁一起软化。
            "AFFINITYGRAPH_ACTIVE_READINESS_REQUIRED": "true",
        })
        remote_log = ""
        socket = ""
        remote_hook_root = ""
        if treatment == "baseline":
            if self.system == "clickhouse":
                env["CLICKHOUSE_BIN"] = self._baseline_wrapper(name, run_dir)
            env["ENABLE_EXTERNAL_HOOK"] = "0"
        else:
            _, socket, wrapper = self._runtime_config(name, treatment, run_dir)
            remote_hook_root, remote_hook = self._remote_hook(
                name, run_dir, treatment, socket, phases,
                "WARMUP" if treatment == "active" else load,
            )
            env.update({
                "ENABLE_EXTERNAL_HOOK": "1",
                "EXTERNAL_HOOK_REQUIRED": "1",
                "EXTERNAL_HOOK_COMMAND": f"{shlex.quote(sys.executable)} -m prism_sampler.affinitygraph_hook",
                "EXTERNAL_HOOK_REMOTE_COMMAND": remote_hook,
                "EXTERNAL_HOOK_RUN_ID": name,
                "EXTERNAL_HOOK_SYSTEM": self.system,
                "AFFINITYGRAPH_CTL": self.release + "/build/affinityctl",
                "AFFINITYGRAPH_LIBRARY_PATH": self.release + "/lib",
                "AFFINITYGRAPH_SOCKET": socket,
                "AFFINITYGRAPH_TARGET_EXE": (
                    REAL_DORIS_BE if self.system == "doris" else REAL_CLICKHOUSE
                ),
            })
            if self.system == "doris":
                env.update({
                    "DORIS_START_BE": wrapper,
                    "SERVER_PID_COMMAND": "pgrep -x doris_be",
                    "SERVER_PRIMARY_PID_COMMAND": "pgrep -x doris_be",
                    "SERVER_ALL_PIDS_COMMAND": (
                        "pgrep -x doris_be; "
                        "pgrep -f 'org[.]apache[.]doris[.]DorisFE'"
                    ),
                })
            else:
                env["CLICKHOUSE_BIN"] = wrapper
            if with_prism:
                env["AFFINITYGRAPH_PRISM_CONFIG"] = str(self._prism_config(name, run_dir))
            remote_log = f"/home/xhc/.local/share/affinitygraph/experiments/{self.root.name}/{name}/log/runtime.jsonl"
        if profile_env:
            env.update(profile_env)
        acquisition_seconds = (
            int(self.state["active_acquisition_seconds"])
            if treatment == "active" else 0
        )
        scenario = scenario_path or self._scenario(
            load, seconds, acquisition_seconds=acquisition_seconds
        )
        command = [
            str(self.yba_root / "bin/yba"), "scenario", "--config",
            str(self.base_config), "--scenario",
            str(scenario),
        ]
        started = time.time_ns()
        monitor: dict[str, Any] = {}
        if treatment == "baseline":
            try:
                code = subprocess.run(
                    command, cwd=self.yba_root, env=env, check=False,
                    timeout=lifecycle_timeout_seconds,
                ).returncode
            except subprocess.TimeoutExpired as error:
                raise RuntimeError(
                    f"YBA lifecycle exceeded {lifecycle_timeout_seconds}s: {name}"
                ) from error
        else:
            process = subprocess.Popen(
                command, cwd=self.yba_root, env=env, start_new_session=True
            )
            monitor = self._monitor_treatment(
                process, treatment=treatment, load=load, socket=socket,
                hook_root=hook_root, remote_hook_root=remote_hook_root, run_dir=run_dir,
                hook_env=env,
                soft_quality_gates=soft_quality_gates,
                measurement_phases=phases,
                lifecycle_timeout_seconds=lifecycle_timeout_seconds,
            )
            self._fetch_remote_hooks(remote_hook_root, hook_root)
            code = int(monitor["returncode"])
            if monitor.get("hard_failure"):
                try:
                    self.host.copy_from(remote_log, run_dir / "runtime-hard-failure.jsonl")
                except Exception:
                    pass
                self.state["hard_failure"] = {
                    "name": name,
                    "treatment": treatment,
                    "reason": monitor["hard_failure"],
                    "realtime_ns": time.time_ns(),
                    "recovered": False,
                }
                self._save()
                raise RuntimeError(f"hard BPF failure: {monitor['hard_failure']}")
        if code:
            if remote_log:
                try:
                    self.host.copy_from(remote_log, run_dir / "runtime-failed.jsonl")
                except Exception:
                    pass
            if treatment != "baseline" and not (hook_root / "phase_before-WARMUP.json").is_file():
                server_ready = hook_root / "server_ready-server.json"
                bpf_was_observable = False
                if server_ready.is_file():
                    ready_value = json.loads(server_ready.read_text(encoding="utf-8"))
                    ready_status = ready_value.get("runtime_status") or {}
                    bpf_was_observable = bool(
                        ready_status.get("bpf") and not ready_status.get("collector_degraded")
                    )
                runtime_started = bool(
                    remote_log
                    and self.host.run(
                        f"test -s {shlex.quote(remote_log)}", check=False
                    ).returncode == 0
                )
                if (server_ready.is_file() or runtime_started) and not bpf_was_observable:
                    self.state["hard_failure"] = {
                        "name": name,
                        "treatment": treatment,
                        "reason": "target failed before required BPF became observable",
                        "realtime_ns": time.time_ns(),
                        "recovered": False,
                    }
                    self._save()
            raise RuntimeError(f"YBA returned {code}: {name}")
        kpi = (
            self._parse_trajectory_kpi(yba_output, phases)
            if len(phases) > 1 else self._parse_kpi(yba_output, load)
        )
        result = {
            "name": name,
            "treatment": treatment,
            "profile": static_profile,
            "load": load,
            "round": round_number,
            "started_realtime_ns": started,
            "finished_realtime_ns": time.time_ns(),
            **kpi,
            "monitor": monitor,
            "environment_gate": environment_gate,
        }
        if remote_log:
            local_log = run_dir / "runtime.jsonl"
            self.host.copy_from(remote_log, local_log)
            result["runtime_log"] = str(local_log)
            result["runtime_sha256"] = _sha256(local_log)
            phase_after = json.loads((hook_root / "phase_after.json").read_text(encoding="utf-8"))
            result["restored"] = bool(phase_after.get("restored"))
            result["runtime_status"] = phase_after.get("runtime_status")
            measurement_starts = [
                json.loads(
                    (hook_root / f"phase_before-{phase}.json").read_text(
                        encoding="utf-8"
                    )
                )
                for phase in phases
            ]
            measurement_start = measurement_starts[0]
            result["measurement_start"] = measurement_start
            result["measurement_start_status"] = measurement_start.get("runtime_status")
            result["measurement_starts"] = measurement_starts
        cluster_summaries = sorted({
            *yba_output.glob("thread-cluster/summary.csv"),
            *yba_output.glob("phases/*/thread-cluster/summary.csv"),
        })
        result["thread_cluster"] = []
        for path in cluster_summaries:
            with path.open(newline="", encoding="utf-8") as stream:
                result["thread_cluster"].extend(dict(row) for row in csv.DictReader(stream))
        self._post_cleanup_gate(socket)
        _write(run_dir / "result.json", json.dumps(result, indent=2, sort_keys=True) + "\n")
        return result

    @staticmethod
    def _runtime_health(
        result: dict[str, Any], *, require_plan: bool, require_action: bool,
        action_threshold: float = 0.8, minimum_active_seconds: float = 0.0,
    ) -> dict[str, Any]:
        events = [json.loads(line) for line in Path(result["runtime_log"]).read_text(encoding="utf-8").splitlines() if line]
        health = [row for row in events if row.get("type") == "bpf_health"]
        actions = [row for row in events if row.get("type") == "action"]
        plans = [row for row in events if row.get("type") == "plan"]
        shadow_commits = [row for row in events if row.get("type") == "shadow_commit"]
        windows = [row for row in events if row.get("type") == "thread_window"]
        ready_health = [row for row in health if row.get("valid") and row.get("window_ready")]
        maximum_loss = max((float(row.get("window_loss_ratio", 0)) for row in ready_health), default=1.0)
        action_requested = sum(int(row.get("requested", len(row.get("assignments", {})))) for row in actions)
        action_committed = sum(int(row.get("committed", row.get("applied", 0))) for row in actions)
        action_vanished = sum(int(row.get("vanished", 0)) for row in actions)
        mask_updates = sum(int(row.get("mask_updates", 0)) for row in actions)
        forced_migrations = sum(int(row.get("forced_migrations", 0)) for row in actions)
        inherited_threads = max(
            (int(row.get("inherited_threads", 0)) for row in plans), default=0
        )
        final_status = result.get("runtime_status") or {}
        active_domains = final_status.get("active_domains", [])
        action_requested = max(action_requested, int(final_status.get("action_requested", 0)))
        action_committed = max(action_committed, int(final_status.get("action_committed", 0)))
        action_vanished = max(action_vanished, int(final_status.get("action_vanished", 0)))
        effective_requests = max(action_requested - action_vanished, 0)
        action_success_ratio = action_committed / effective_requests if effective_requests else 0.0
        status = result.get("measurement_start_status") or {}
        active_seconds = float(result.get("monitor", {}).get("active_effective_seconds", 0))
        cohort_workload_seconds = float(
            result.get("monitor", {}).get("cohort_workload_seconds", 0)
        )
        cohort_effective_seconds = float(
            result.get("monitor", {}).get("cohort_effective_seconds", 0)
        )
        start_assignments = status.get("planned_assignments", {})
        planned_masks = status.get("planned_masks", {})
        start_masks = result.get("measurement_start", {}).get("masks", {})
        expected_masks = planned_masks or {
            str(tid): str(cpu) for tid, cpu in start_assignments.items()
        }
        assignments_match = bool(expected_masks) and all(
            format_cpu_list(parse_cpu_list(str(start_masks.get(str(tid), ""))))
            == format_cpu_list(parse_cpu_list(str(mask)))
            for tid, mask in expected_masks.items()
        )
        measurement_starts = result.get("measurement_starts") or [
            result.get("measurement_start", {})
        ]
        all_measurement_starts_ready = bool(measurement_starts) and all(
            bool(start.get("active_measurement_ready", True))
            and bool((start.get("runtime_status") or {}).get("active_effective"))
            if start.get("runtime_status")
            else bool(status.get("active_effective"))
            for start in measurement_starts
        )
        value = {
            "bpf_health_samples": len(health),
            "bpf_window_samples": len(ready_health),
            "maximum_bpf_loss_ratio": maximum_loss,
            "plans": len(plans),
            "shadow_commits": len(shadow_commits),
            "confident_windows": sum(float(row.get("confidence", 0)) >= 0.8 for row in windows),
            "actions": len(actions),
            "action_requested": action_requested,
            "action_committed": action_committed,
            "action_vanished": action_vanished,
            "mask_updates": mask_updates,
            "forced_migrations": forced_migrations,
            "inherited_threads": inherited_threads,
            "active_domains": active_domains,
            "action_success_ratio": action_success_ratio,
            "active_effective_seconds": active_seconds,
            "cohort_workload_seconds": cohort_workload_seconds,
            "cohort_effective_seconds": cohort_effective_seconds,
            "cohort_effective_coverage_ratio": (
                cohort_effective_seconds / cohort_workload_seconds
                if cohort_workload_seconds > 0 else 1.0
            ),
            "measurement_start_ready": bool(
                status.get("effective_mode") == "active"
                and status.get("policy_armed")
                and status.get("active_effective")
                and bool(expected_masks)
                and assignments_match
                and all_measurement_starts_ready
            ) if require_action else True,
            "measurement_phase_count": len(measurement_starts),
            "measurement_phases_ready": sum(
                bool(start.get("active_measurement_ready"))
                for start in measurement_starts
            ),
            "restored": bool(result.get("restored")),
            "runtime_average_cpu_cores": float(result.get("monitor", {}).get("runtime_average_cpu_cores", 0)),
            "runtime_peak_rss_kib": int(result.get("monitor", {}).get("runtime_peak_rss_kib", 0)),
            "runtime_resource_samples": int(result.get("monitor", {}).get("runtime_resource_samples", 0)),
        }
        value["passed"] = bool(
            len(ready_health) > 0 and maximum_loss < 0.01
            and value["confident_windows"] > 0 and value["restored"]
            and value["runtime_average_cpu_cores"] < 1.0
            and value["runtime_peak_rss_kib"] < 256 * 1024
            and value["runtime_resource_samples"] > 0
            and (not require_plan or len(plans) > 0)
            and (not require_action or (
                actions and effective_requests and action_success_ratio >= action_threshold
                and value["measurement_start_ready"]
                and active_seconds >= minimum_active_seconds
            ))
        )
        return value

    @staticmethod
    def _domain_oracle(system: str, domains: list[dict[str, Any]]) -> dict[str, Any]:
        # Linux comm 只有 15 字节，历史日志可能包含 "Pipe_normal [wo" 这类
        # 被截断的角色后缀。oracle 只比较与 selector 相同的规范化 family 名。
        direct_families = {
            re.sub(r"\s+\[[^\]]*\]?$", "", str(family).split("@", 1)[0])
            for domain in domains
            if domain.get("valid", True)
            for family in domain.get("families", [])
        }
        expected = (
            {"Pipe_normal", "brpc_light"}
            if system == "doris"
            else {"ThreadPool"}
        )
        return {
            "expected_direct_families": sorted(expected),
            "actual_direct_families": sorted(direct_families),
            "passed": direct_families == expected,
        }

    @staticmethod
    def _bpf_observe_diagnostic(result: dict[str, Any]) -> dict[str, Any]:
        events = [
            json.loads(line)
            for line in Path(result["runtime_log"]).read_text(encoding="utf-8").splitlines()
            if line
        ]
        health = [
            row for row in events
            if row.get("type") == "bpf_health" and not row.get("final")
        ]
        valid = [row for row in health if row.get("valid")]
        rates: list[dict[str, Any]] = []
        for previous, current in zip(valid, valid[1:]):
            seconds = (int(current["timestamp_ns"]) - int(previous["timestamp_ns"])) / 1e9
            if seconds <= 0:
                continue
            row: dict[str, Any] = {
                "timestamp_ns": int(current["timestamp_ns"]),
                "interval_seconds": seconds,
                "emitted_per_second": (
                    int(current.get("emitted", 0)) - int(previous.get("emitted", 0))
                ) / seconds,
                "dropped_per_second": (
                    int(current.get("dropped", 0)) - int(previous.get("dropped", 0))
                ) / seconds,
                "futex_aggregate_records_per_second": (
                    int(current.get("futex_aggregate_records", 0))
                    - int(previous.get("futex_aggregate_records", 0))
                ) / seconds,
                "futex_handoffs_consumed_per_second": (
                    int(current.get("futex_handoffs", 0))
                    - int(previous.get("futex_handoffs", 0))
                ) / seconds,
            }
            for kind in ("futex", "task_rename"):
                for counter in ("emitted", "dropped", "suppressed"):
                    field = f"{counter}_by_kind"
                    row[f"{kind}_{counter}_per_second"] = (
                        int(current.get(field, {}).get(kind, 0))
                        - int(previous.get(field, {}).get(kind, 0))
                    ) / seconds
            rates.append(row)
        ready = [row for row in valid if row.get("window_ready")]
        last = valid[-1] if valid else {}
        futex_emitted = int(last.get("emitted_by_kind", {}).get("futex", 0))
        futex_dropped = int(last.get("dropped_by_kind", {}).get("futex", 0))
        rename_emitted = int(last.get("emitted_by_kind", {}).get("task_rename", 0))
        rename_dropped = int(last.get("dropped_by_kind", {}).get("task_rename", 0))
        monitor = result.get("monitor", {})
        report = {
            "schema": "prism-sampler.affinitygraph-observe-diagnostic.v1",
            "result": result,
            "per_second_rates": rates,
            "health_samples": len(health),
            "complete_30s_windows": len(ready),
            "maximum_30s_loss_ratio": max(
                (float(row.get("window_loss_ratio", 0)) for row in ready), default=1.0
            ),
            "health_read_failures": sum(not row.get("valid") for row in health),
            "maximum_consecutive_health_failures": max(
                (int(row.get("consecutive_failures", 0)) for row in health), default=0
            ),
            "futex_emitted": futex_emitted,
            "futex_dropped": futex_dropped,
            "futex_drop_ratio": futex_dropped / max(futex_emitted + futex_dropped, 1),
            "futex_aggregate_records": int(last.get("futex_aggregate_records", 0)),
            "futex_handoffs_consumed": int(last.get("futex_handoffs", 0)),
            "futex_aggregation_ratio": (
                int(last.get("futex_aggregate_records", 0))
                / max(int(last.get("futex_handoffs", 0)), 1)
            ),
            "task_rename_emitted": rename_emitted,
            "task_rename_dropped": rename_dropped,
            "task_rename_suppressed": int(
                last.get("suppressed_by_kind", {}).get("task_rename", 0)
            ),
            "task_rename_drop_ratio": rename_dropped / max(
                rename_emitted + rename_dropped, 1
            ),
            "ring_capacity_bytes": int(last.get("ring_capacity_bytes", 0)),
            "ring_max_occupancy_bytes": max(
                (int(row.get("ring_max_occupancy_bytes", 0)) for row in valid), default=0
            ),
            "ring_max_utilization": max(
                (
                    int(row.get("ring_max_occupancy_bytes", 0))
                    / max(int(row.get("ring_capacity_bytes", 0)), 1)
                    for row in valid
                ),
                default=0.0,
            ),
            "consumer_max_batch": max(
                (int(row.get("consumer_max_batch", 0)) for row in valid), default=0
            ),
            "consumer_max_drain_ns": max(
                (int(row.get("consumer_max_drain_ns", 0)) for row in valid), default=0
            ),
            "consumer_max_lag_ns": max(
                (int(row.get("consumer_max_lag_ns", 0)) for row in valid), default=0
            ),
            "supervisor_average_cpu_cores": float(
                monitor.get("runtime_average_cpu_cores", 0)
            ),
            "supervisor_peak_rss_kib": int(monitor.get("runtime_peak_rss_kib", 0)),
            "host_busy_ratio": float(monitor.get("host_busy_ratio", 0)),
            "host_maximum_load_1m": float(monitor.get("host_maximum_load_1m", 0)),
            "maximum_monitor_poll_gap_seconds": float(
                monitor.get("maximum_poll_gap_seconds", 0)
            ),
        }
        report["passed"] = bool(
            ready
            and report["maximum_30s_loss_ratio"] < 0.01
            and futex_dropped == 0
            and rename_dropped == 0
            and report["supervisor_average_cpu_cores"] < 1.0
            and report["supervisor_peak_rss_kib"] < 256 * 1024
            and report["health_read_failures"] == 0
            and result.get("restored")
        )
        return report

    @staticmethod
    def _observe_counter_comparison(result: dict[str, Any]) -> dict[str, Any]:
        import duckdb

        events = [json.loads(line) for line in Path(result["runtime_log"]).read_text(encoding="utf-8").splitlines() if line]
        health = [row for row in events if row.get("type") == "bpf_health"]
        if not health:
            raise RuntimeError("observe run has no BPF health samples")
        embedded = health[-1]["emitted_by_kind"]
        run_dir = Path(result["runtime_log"]).parent
        databases = list((run_dir / "prism").glob("**/collector.db3"))
        if len(databases) != 1:
            raise RuntimeError(f"expected one Prism database, got {databases}")
        connection = duckdb.connect(str(databases[0]), read_only=True)
        prism = {
            "futex": int(connection.execute("select coalesce(sum(successful_count), 0) from futex_wake").fetchone()[0]),
            "vfs": int(connection.execute(
                "select coalesce(sum(total_requests), 0) from vfs where fs_magic = 1346981957"
            ).fetchone()[0]),
        }
        connection.close()
        return {
            "status": "warning",
            "comparable": False,
            "definitions": {
                "futex": {
                    "embedded": "attributed cross-TID handoffs from a waker to the most recent waiter",
                    "prism": "total successful futex wakes",
                },
                "vfs": {
                    "embedded": "consecutive cross-TID accesses to the same selected inode",
                    "prism": "total requests aggregated by TID, operation, and file identity",
                },
            },
            "embedded": {key: int(embedded[key]) for key in prism},
            "prism": prism,
            "warning": "metric semantics differ; no relative-error gate is valid",
        }

    def run(self) -> dict[str, Any]:
        if self.state.get("status") == "stopped":
            raise RuntimeError(
                "formal experiment was stopped by a hard gate; use a new root after review"
            )
        failure = self.state.get("hard_failure")
        if failure and not failure.get("recovered"):
            raise RuntimeError(
                "experiment is blocked by a hard failure; run affinitygraph recover after human review"
            )
        deployed = self._step("deploy", self.deploy)
        self.release = self.state["release"]
        current_fingerprint = self._experiment_fingerprint(deployed["fingerprint"])
        if self.state.get("experiment_fingerprint") != current_fingerprint:
            self.state["experiment_fingerprint"] = current_fingerprint
            self.state.pop("smoke_approval", None)
            self.state["status"] = "fingerprint_changed"
            self._save()

        def compatibility() -> dict[str, Any]:
            if self.allow_busy_smoke:
                return {
                    "status": "deferred",
                    "comparable": False,
                    "warning": "busy-host proof smoke does not run the Prism compatibility diagnostic",
                }
            result = self.run_lifecycle(
                "observe", self.smoke_load, 0,
                f"prepare-prism-compatibility-{self.smoke_load}",
                seconds=210, with_prism=True,
            )
            try:
                return self._observe_counter_comparison(result)
            except Exception as error:
                return {
                    "status": "warning", "comparable": False,
                    "warning": f"Prism diagnostic unavailable: {type(error).__name__}: {error}",
                }

        comparison = self._step("prism_compatibility", compatibility)
        _write(self.summary / "metric-compatibility.json", json.dumps(comparison, indent=2, sort_keys=True) + "\n")

        smoke_seconds = int(self.state["smoke_measurement_seconds"])
        smoke_load = self.smoke_load
        soft_smoke = self.smoke_gates == "soft"
        warnings: list[dict[str, Any]] = []
        if comparison.get("status") == "warning" or not comparison.get("comparable", True):
            warnings.append({
                "stage": "compatibility", "gate": "metric_compatibility",
                "severity": "warning", "details": comparison,
            })

        def add_gate_warnings(stage: str, result: dict[str, Any], gate: dict[str, Any]) -> None:
            environment = result.get("environment_gate", {})
            if environment and not environment.get("passed", True):
                warnings.append({
                    "stage": stage, "gate": "environment_idle",
                    "severity": "warning", "details": environment,
                })
            warnings.extend({"stage": stage, **warning} for warning in
                            result.get("monitor", {}).get("warnings", []))
            if not gate.get("passed"):
                warnings.append({
                    "stage": stage, "gate": "quality",
                    "severity": "warning", "details": gate,
                })

        if self.smoke_baseline_result:
            observe_baseline = self._step(
                "smoke_baseline_reference", self._baseline_reference
            )
        else:
            observe_baseline = self._step("smoke_baseline", lambda: self.run_lifecycle(
                "baseline", smoke_load, 0, f"smoke-baseline-{smoke_load}",
                seconds=smoke_seconds,
                allow_busy_idle=self.allow_busy_smoke,
                soft_quality_gates=soft_smoke,
            ))
        observe = self._step("smoke_observe", lambda: self.run_lifecycle(
            "observe", smoke_load, 0, f"smoke-observe-{smoke_load}",
            seconds=smoke_seconds,
            allow_busy_idle=self.allow_busy_smoke,
            soft_quality_gates=soft_smoke,
        ))
        observe_health = self._runtime_health(observe, require_plan=False, require_action=False)
        observe_health["throughput_overhead_ratio"] = float(observe["throughput"]) / float(observe_baseline["throughput"]) - 1.0
        observe_health["maximum_accepted_overhead_ratio"] = float(
            self.state["smoke_observe_max_overhead_ratio"]
        )
        observe_health["throughput_gate_passed"] = bool(
            observe_health["throughput_overhead_ratio"]
            >= -observe_health["maximum_accepted_overhead_ratio"]
        )
        observe_health["throughput_gate_bypassed"] = bool(
            self.allow_busy_smoke and not observe_health["throughput_gate_passed"]
        )
        observe_health["passed"] = bool(
            observe_health["passed"]
            and (observe_health["throughput_gate_passed"] or self.allow_busy_smoke)
        )
        observe_health["enforcement"] = "warning" if soft_smoke else "error"
        observe_health["continued"] = bool(soft_smoke and not observe_health["passed"])
        add_gate_warnings("observe", observe, observe_health)
        _write(self.summary / "observe-gate.json", json.dumps(observe_health, indent=2, sort_keys=True) + "\n")
        if not observe_health["passed"] and not soft_smoke:
            raise RuntimeError(f"observe gate failed: {observe_health}")
        plan = self._step("smoke_plan", lambda: self.run_lifecycle(
            "plan", smoke_load, 0, f"smoke-plan-{smoke_load}",
            seconds=smoke_seconds,
            allow_busy_idle=self.allow_busy_smoke,
            soft_quality_gates=soft_smoke,
        ))
        plan_health = self._runtime_health(plan, require_plan=True, require_action=False)
        plan_health["domain_oracle"] = self._domain_oracle(
            self.system, plan_health["active_domains"]
        )
        plan_health["passed"] = bool(
            plan_health["passed"] and plan_health["domain_oracle"]["passed"]
        )
        plan_executed_affinity = bool(plan_health["actions"])
        if plan_executed_affinity or not plan_health["shadow_commits"]:
            plan_health["passed"] = False
        plan_health["enforcement"] = "warning" if soft_smoke else "error"
        plan_health["continued"] = bool(soft_smoke and not plan_health["passed"])
        add_gate_warnings("plan", plan, plan_health)
        _write(self.summary / "plan-gate.json", json.dumps(plan_health, indent=2, sort_keys=True) + "\n")
        if plan_executed_affinity:
            raise RuntimeError("plan mode executed affinity; refusing to enter active")
        if not plan_health["domain_oracle"]["passed"]:
            raise RuntimeError(
                f"plan family oracle mismatch: {plan_health['domain_oracle']}"
            )
        if not plan_health["passed"] and not soft_smoke:
            raise RuntimeError(f"plan gate failed: {plan_health}")
        smoke = self._step("smoke_active", lambda: self.run_lifecycle(
            "active", smoke_load, 0, f"smoke-active-{smoke_load}",
            seconds=smoke_seconds,
            allow_busy_idle=self.allow_busy_smoke,
            soft_quality_gates=soft_smoke,
        ))
        smoke_health = self._runtime_health(
            smoke, require_plan=True, require_action=True, action_threshold=0.80,
            minimum_active_seconds=smoke_seconds * 0.90,
        )
        smoke_health["throughput_ratio"] = float(smoke["throughput"]) / float(observe_baseline["throughput"])
        reference_floor = 528.590 if self.system == "doris" else 256.275
        smoke_health["throughput_reference_floor"] = reference_floor
        smoke_health["throughput_reference_passed"] = (
            float(smoke["throughput"]) >= reference_floor
        )
        smoke_health["passed"] = bool(
            smoke_health["passed"] and smoke_health["throughput_reference_passed"]
        )
        smoke_health["enforcement"] = "warning" if soft_smoke else "error"
        smoke_health["continued"] = bool(soft_smoke and not smoke_health["passed"])
        add_gate_warnings("active", smoke, smoke_health)
        _write(self.summary / "active-gate.json", json.dumps(smoke_health, indent=2, sort_keys=True) + "\n")
        algorithm_effect = analyze_incremental_smoke(
            observe_baseline, plan, smoke, self._cpu_to_node()
        )
        algorithm_effect["health"] = smoke_health
        _write(
            self.summary / "algorithm-effect.json",
            json.dumps(algorithm_effect, indent=2, sort_keys=True) + "\n",
        )
        if not smoke_health["passed"] and not soft_smoke:
            raise RuntimeError(f"active gate failed: {smoke_health}")

        quality_passed = bool(
            observe_health["passed"] and plan_health["passed"]
            and smoke_health["passed"] and not warnings
        )
        smoke_report = {
            "schema": "prism-sampler.affinitygraph-smoke.v2",
            "passed": True,
            "completed": True,
            "quality_passed": quality_passed,
            "safety_passed": True,
            "completed_with_warnings": not quality_passed,
            "smoke_gate_mode": self.smoke_gates,
            "warnings": warnings,
            "environment_valid": not any(
                warning.get("gate") == "environment_idle" for warning in warnings
            ),
            "approval_eligible": True,
            "fingerprint": self.state.get("experiment_fingerprint"),
            "baseline": observe_baseline,
            "baseline_provenance": observe_baseline.get("reference", {
                "type": "measured_in_smoke",
            }),
            "observe": observe_health,
            "plan": plan_health,
            "active": smoke_health,
            "algorithm_effect": algorithm_effect,
            "compatibility": comparison,
        }
        _write(self.summary / "smoke-result.json", json.dumps(smoke_report, indent=2, sort_keys=True) + "\n")
        approval = self.state.get("smoke_approval")
        if not approval or approval.get("fingerprint") != self.state.get("experiment_fingerprint"):
            self.state["status"] = (
                "awaiting_review_with_warnings" if warnings else "awaiting_review"
            )
            self._save()
            return {"status": "awaiting_review", "smoke": smoke_report}

        formal_seconds = int(self.state["formal_measurement_seconds"])
        for entry in self.state["schedule"]:
            key = f"p{entry['pair_index']:02d}-{entry['load']}-r{entry['round']}-{entry['treatment']}"
            if self.state["runs"].get(key, {}).get("status") == "complete":
                continue
            self.state["runs"][key] = {"status": "running", **entry}
            self._save()
            result = self.run_lifecycle(
                entry["treatment"], entry["load"], entry["round"], key,
                seconds=formal_seconds,
            )
            result["valid"] = True
            if entry["treatment"] == "active":
                health = self._runtime_health(
                    result, require_plan=True, require_action=True,
                    action_threshold=0.95, minimum_active_seconds=270,
                )
                result["formal_health"] = health
                result["valid"] = bool(health["passed"])
                if not result["valid"]:
                    for other_key, other in self.state["runs"].items():
                        if (
                            other_key != key and other.get("pair_index") == entry["pair_index"]
                            and other.get("treatment") == "baseline"
                        ):
                            other["valid"] = False
                            other["invalid_reason"] = "paired active treatment invalid"
                    result["invalid_reason"] = "active validity or 95% action gate failed"
            else:
                paired_active = next((
                    other for other in self.state["runs"].values()
                    if other.get("pair_index") == entry["pair_index"]
                    and other.get("treatment") == "active"
                    and other.get("status") == "complete"
                ), None)
                if paired_active and not paired_active.get("valid", True):
                    result["valid"] = False
                    result["invalid_reason"] = "paired active treatment invalid"
            self.state["runs"][key] = {"status": "complete", **entry, **result}
            self._save()
        rows = [row for row in self.state["runs"].values() if row.get("status") == "complete"]
        report = analyze(rows, seed=self.seed, required_pairs=self.rounds)
        report["errors"] = sum(int(row["error_count"]) for row in rows)
        report["timeouts"] = sum(int(row["timeout_count"]) for row in rows)
        active_rows = [row for row in rows if row.get("treatment") == "active" and row.get("valid")]
        total_requested = sum(int(row.get("formal_health", {}).get("action_requested", 0)) for row in active_rows)
        total_committed = sum(int(row.get("formal_health", {}).get("action_committed", 0)) for row in active_rows)
        total_vanished = sum(int(row.get("formal_health", {}).get("action_vanished", 0)) for row in active_rows)
        denominator = max(total_requested - total_vanished, 0)
        report["formal_action_success_ratio"] = total_committed / denominator if denominator else 0.0
        report["mask_updates"] = sum(
            int(row.get("formal_health", {}).get("mask_updates", 0))
            for row in active_rows
        )
        report["forced_migrations"] = sum(
            int(row.get("formal_health", {}).get("forced_migrations", 0))
            for row in active_rows
        )
        report["inherited_threads"] = max(
            (
                int(row.get("formal_health", {}).get("inherited_threads", 0))
                for row in active_rows
            ),
            default=0,
        )
        report["active_domains"] = [
            {
                "load": row.get("load"),
                "round": row.get("round"),
                "domains": row.get("formal_health", {}).get("active_domains", []),
            }
            for row in active_rows
        ]
        report["invalid_runs"] = [row for row in rows if not row.get("valid", True)]
        report["candidate_pass"] = bool(
            report["candidate_pass"] and report["errors"] == 0 and report["timeouts"] == 0
            and report["formal_action_success_ratio"] >= 0.95
        )
        self.state["status"] = "complete" if report["complete"] else "incomplete"
        self._save()
        _write(self.summary / "result.json", json.dumps(report, indent=2, sort_keys=True) + "\n")
        return report


class DorisFormalRunner(AffinityGraphRunner):
    def __init__(self, root: Path, base_config: Path, source: Path):
        self.root = root.resolve()
        self.base_config = base_config.resolve()
        self.source = source.resolve()
        self.system = "doris"
        self.smoke_load = "C4T4"
        self.rounds = 3
        self.seed = 20260811
        self.allow_busy_smoke = False
        self.smoke_gates = "strict"
        self.smoke_baseline_result = None
        self.host = Host(_base_env_value(self.base_config, "SERVER_HOST"))
        self.client_host = Host(_base_env_value(self.base_config, "CLIENT_HOST"))
        self.yba_root = Path("/home/x/data/sched-ext-study/ycsb-bench-all-database")
        self.state_path = self.root / "formal-state.json"
        self.generated = self.root / "generated-config"
        self.runs = self.root / "runs"
        self.summary = self.root / "summary"
        for path in (self.generated, self.runs, self.summary):
            path.mkdir(parents=True, exist_ok=True)
        design = doris_formal_design()
        self.design_path = self.root / "formal-design.json"
        rendered = json.dumps(design, indent=2, sort_keys=True) + "\n"
        if self.design_path.is_file() and self.design_path.read_text(
            encoding="utf-8"
        ) != rendered:
            raise RuntimeError("existing formal design differs from doris-random-v1")
        _write(self.design_path, rendered)
        if self.state_path.is_file():
            self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
            if self.state.get("design_sha256") != _sha256(self.design_path):
                raise RuntimeError("formal design fingerprint changed")
        else:
            self.state = {
                "schema": "prism-sampler.affinitygraph-formal-run.v1",
                "system": "doris",
                "design": "doris-random-v1",
                "design_sha256": _sha256(self.design_path),
                "warmup_seconds": 30,
                "active_acquisition_seconds": 150,
                "started_realtime_ns": time.time_ns(),
                "steps": {},
                "runs": {},
                "schedule": design["run_order"],
            }
            self._save()
        self.release = str(self.state.get("release", ""))

    def _experiment_fingerprint(self, release_fingerprint: str) -> str:
        payload = {
            "release_fingerprint": release_fingerprint,
            "runner_sha256": _sha256(Path(__file__)),
            "hook_sha256": _sha256(Path(__file__).with_name("affinitygraph_hook.py")),
            "base_config_sha256": _sha256(self.base_config),
            "design_sha256": _sha256(self.design_path),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def _require_clean_repositories(self) -> dict[str, Any]:
        repositories = {
            "affinitygraph": self.source,
            "prism-sampler": Path(__file__).resolve().parents[1],
        }
        result = {}
        for name, path in repositories.items():
            status = subprocess.run(
                ["git", "status", "--porcelain"], cwd=path,
                text=True, capture_output=True, check=True,
            ).stdout
            if status.strip():
                raise RuntimeError(f"formal experiment requires clean {name} worktree")
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=path,
                text=True, capture_output=True, check=True,
            ).stdout.strip()
            result[name] = {"path": str(path), "commit": commit}
        _write(
            self.root / "source-manifest.json",
            json.dumps(result, indent=2, sort_keys=True) + "\n",
        )
        return result

    def _formal_scenario(self, trajectory: str) -> tuple[Path, list[str]]:
        design = doris_formal_design()
        phases = design["trajectories"][trajectory]["phases"]
        measurement_names = [str(row["phase"]) for row in phases]
        phase_order = " ".join(["PLACEMENT", "WARMUP", *measurement_names])
        values = [
            "SCENARIO_NAME=affinitygraph_doris_random_" + trajectory.lower(),
            "SCENARIO_BUDGET_MODE=duration",
            f"SCENARIO_PHASES='{phase_order}'",
            "SCENARIO_MIN_PHASE_SECONDS=30",
            "SCENARIO_TIMEOUT_GRACE_SECONDS=45",
            "SCENARIO_DURATION_TOLERANCE_SECONDS=2",
            "SCENARIO_DURATION_OPERATIONCOUNT_PER_CLIENT=2147483647",
            "SCENARIO_PHASE_PLACEMENT_CLIENTS=4",
            "SCENARIO_PHASE_PLACEMENT_THREADS=8",
            "SCENARIO_PHASE_PLACEMENT_VALUE=150",
            "SCENARIO_PHASE_WARMUP_CLIENTS=4",
            "SCENARIO_PHASE_WARMUP_THREADS=8",
            "SCENARIO_PHASE_WARMUP_VALUE=30",
        ]
        for row in phases:
            name = row["phase"]
            values.extend([
                f"SCENARIO_PHASE_{name}_CLIENTS={row['clients']}",
                f"SCENARIO_PHASE_{name}_THREADS={row['threads']}",
                f"SCENARIO_PHASE_{name}_VALUE={row['seconds']}",
            ])
        path = self.generated / f"formal-{trajectory.lower()}.env"
        _write(path, "\n".join(values) + "\n")
        return path, measurement_names

    @staticmethod
    def _baseline_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
        baseline = {
            row["trajectory"]: row for row in rows
            if row.get("treatment") == "baseline"
            and row.get("status") == "complete"
        }
        if set(baseline) != {"S1", "S2"}:
            return {"complete": False, "passed": False, "differences": {}}
        by_seed = {
            seed: {
                phase["load"]: float(phase["throughput"])
                for phase in row["measurement_phases"]
            }
            for seed, row in baseline.items()
        }
        differences = {
            load: 2 * abs(by_seed["S1"][load] - by_seed["S2"][load])
            / (by_seed["S1"][load] + by_seed["S2"][load])
            for load in sorted(by_seed["S1"])
        }
        median = statistics.median(differences.values())
        severe = sum(value > 0.15 for value in differences.values())
        return {
            "complete": True,
            "passed": median <= 0.10 and severe < 3,
            "median_relative_difference": median,
            "loads_over_15_percent": severe,
            "differences": differences,
        }

    def run(self) -> dict[str, Any]:
        failure = self.state.get("hard_failure")
        if failure and not failure.get("recovered"):
            raise RuntimeError("formal experiment is blocked by an unrecovered hard failure")
        source_freeze = self._require_clean_repositories()
        previous_freeze = self.state.get("source_freeze")
        if previous_freeze and previous_freeze != source_freeze:
            raise RuntimeError("formal experiment source commits changed")
        self.state["source_freeze"] = source_freeze
        self._save()
        deployed = self._step("deploy", self.deploy)
        self.release = self.state["release"]
        if self.state.get("experiment_fingerprint") != self._experiment_fingerprint(
            deployed["fingerprint"]
        ):
            raise RuntimeError("formal experiment fingerprint changed after deployment")

        scenario_s1, phases_s1 = self._formal_scenario("S1")

        def canary() -> dict[str, Any]:
            result = self.run_lifecycle(
                "plan", "trajectory", 0, "canary-plan-S1",
                seconds=540, scenario_path=scenario_s1,
                measurement_phases=phases_s1, lifecycle_timeout_seconds=1800,
            )
            health = self._runtime_health(
                result, require_plan=True, require_action=False
            )
            health["domain_oracle"] = self._domain_oracle(
                "doris", health["active_domains"]
            )
            health["plan_executed_affinity"] = bool(health["actions"])
            health["passed"] = bool(
                health["passed"] and health["domain_oracle"]["passed"]
                and not health["plan_executed_affinity"]
                and health["shadow_commits"] > 0
            )
            if not health["passed"]:
                raise RuntimeError(f"formal plan canary failed: {health}")
            return {"result": result, "health": health}

        self._step("plan_canary", canary)
        for entry in self.state["schedule"]:
            key = (
                f"{int(entry['position']):02d}-{entry['trajectory']}-"
                f"{entry['treatment']}-r{entry['repeat']}"
            )
            if self.state["runs"].get(key, {}).get("status") == "complete":
                continue
            trajectory = str(entry["trajectory"])
            scenario, phases = self._formal_scenario(trajectory)
            self.state["runs"][key] = {"status": "running", **entry}
            self._save()
            try:
                result = self.run_lifecycle(
                    str(entry["treatment"]), "trajectory", int(entry["repeat"]), key,
                    seconds=540, scenario_path=scenario, measurement_phases=phases,
                    lifecycle_timeout_seconds=1800,
                )
                result.update(entry)
                result["valid"] = True
                if result["error_count"] or result["timeout_count"]:
                    result["valid"] = False
                    result["invalid_reason"] = "YCSB errors or timeouts"
                if entry["treatment"] == "active":
                    health = self._runtime_health(
                        result, require_plan=True, require_action=True,
                        action_threshold=0.95, minimum_active_seconds=513,
                    )
                    health["domain_oracle"] = self._domain_oracle(
                        "doris", health["active_domains"]
                    )
                    health["effective_coverage_passed"] = (
                        health["cohort_effective_coverage_ratio"] >= 0.95
                    )
                    health["passed"] = bool(
                        health["passed"] and health["domain_oracle"]["passed"]
                        and health["effective_coverage_passed"]
                    )
                    result["formal_health"] = health
                    result["valid"] = bool(result["valid"] and health["passed"])
                    if not result["valid"]:
                        result["invalid_reason"] = "active formal safety gate failed"
                self.state["runs"][key] = {"status": "complete", **result}
                self._save()
                if not result["valid"]:
                    raise RuntimeError(f"formal run invalid: {key}: {result['invalid_reason']}")
                rows = list(self.state["runs"].values())
                stability = self._baseline_stability(rows)
                if stability["complete"]:
                    _write(
                        self.summary / "baseline-stability.json",
                        json.dumps(stability, indent=2, sort_keys=True) + "\n",
                    )
                    if not stability["passed"]:
                        raise RuntimeError(f"baseline stability gate failed: {stability}")
            except Exception as error:
                current = self.state["runs"].get(key, {})
                if current.get("status") != "complete":
                    self.state["runs"][key] = {
                        **current, "status": "failed",
                        "error": f"{type(error).__name__}: {error}",
                    }
                self.state["status"] = "stopped"
                self._save()
                raise
        rows = list(self.state["runs"].values())
        report = analyze_doris_formal(rows)
        self.state["status"] = "complete" if report["complete"] else "incomplete"
        self._save()
        _write(
            self.summary / "formal-result.json",
            json.dumps(report, indent=2, sort_keys=True) + "\n",
        )
        return report


def execute_doris_formal(root: Path, base_config: Path, source: Path) -> dict[str, Any]:
    return DorisFormalRunner(root, base_config, source).run()


def execute(
    root: Path, base_config: Path, source: Path, *, rounds: int = 3,
    seed: int = 20260805, allow_busy_smoke: bool = False,
    smoke_gates: str = "soft", smoke_baseline_result: Path | None = None,
    smoke_load: str | None = None, system: str = "clickhouse",
) -> dict[str, Any]:
    return AffinityGraphRunner(
        root, base_config, source, rounds=rounds, seed=seed,
        allow_busy_smoke=allow_busy_smoke,
        smoke_gates=smoke_gates, smoke_baseline_result=smoke_baseline_result,
        smoke_load=smoke_load or ("C4T4" if system == "doris" else "C2T2"),
        system=system,
    ).run()


def diagnose_observe(
    root: Path, base_config: Path, source: Path, *, seconds: int = 180,
    seed: int = 20260805, system: str = "clickhouse", load: str = "C4T6",
) -> dict[str, Any]:
    if seconds < 60 or seconds > 300:
        raise ValueError("observe diagnostic measurement must be between 60 and 300 seconds")
    runner = AffinityGraphRunner(
        root, base_config, source, rounds=1, seed=seed,
        system=system, smoke_load=load,
    )
    deployed = runner._step("deploy", runner.deploy)
    runner.release = deployed["release"]
    result = runner._step(
        "observe_diagnostic",
        lambda: runner.run_lifecycle(
            "observe", load, 0, f"diagnostic-observe-{load}", seconds=seconds,
        ),
    )
    report = runner._bpf_observe_diagnostic(result)
    runner.state["status"] = "diagnostic_passed" if report["passed"] else "diagnostic_failed"
    runner._save()
    _write(
        runner.summary / "observe-diagnostic.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def diagnose_positive_control(
    root: Path, base_config: Path, source: Path, *, seconds: int = 120,
    rounds: int = 2, seed: int = 20260805, system: str = "clickhouse",
    load: str | None = None,
) -> dict[str, Any]:
    if seconds < 60 or seconds > 300:
        raise ValueError("positive-control measurement must be between 60 and 300 seconds")
    if rounds < 1 or rounds > 3:
        raise ValueError("positive-control rounds must be between 1 and 3")
    selected_load = load or ("C4T4" if system == "doris" else "C2T2")
    runner = AffinityGraphRunner(
        root, base_config, source, rounds=rounds, seed=seed,
        system=system, smoke_load=selected_load,
    )
    deployed = runner._step("deploy", runner.deploy)
    runner.release = deployed["release"]
    profiles = positive_control_profiles(system)
    run_schedule = positive_control_schedule(system, rounds, seed)
    runner.state["positive_control_schedule"] = run_schedule
    runner._save()
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(run_schedule, 1):
        profile = str(item["profile"])
        round_number = int(item["round"])
        name = f"positive-{index:02d}-{profile}-{selected_load}-r{round_number}"
        result = runner._step(
            f"positive_control_{index:02d}_{profile}_r{round_number}",
            lambda profile=profile, round_number=round_number, name=name: (
                runner.run_lifecycle(
                    "observe", selected_load, round_number, name, seconds=seconds,
                    static_profile=profile, profile_env=profiles[profile],
                )
            ),
        )
        health = runner._runtime_health(
            result, require_plan=False, require_action=False
        )
        masks_passed = bool(
            profile == "unrestricted"
            or (
                result.get("thread_cluster")
                and all(
                    float(row.get("hit_ratio", 0)) >= 0.95
                    and row.get("thread_set_state") in {"stable", "dynamic"}
                    for row in result["thread_cluster"]
                )
            )
        )
        safety_passed = bool(
            health["bpf_health_samples"] > 0
            and health["bpf_window_samples"] > 0
            and health["maximum_bpf_loss_ratio"] < 0.01
            and health["restored"]
            and health["action_requested"] == 0
            and masks_passed
        )
        rows.append({
            **result,
            "profile": profile,
            "health": health,
            "masks_passed": masks_passed,
            "safety_passed": safety_passed,
            "valid": safety_passed,
        })
        if not safety_passed:
            raise RuntimeError(
                f"positive-control safety gate failed: profile={profile} "
                f"health={health} masks_passed={masks_passed}"
            )
    report = analyze_positive_control(rows, system, required_pairs=rounds)
    report.update({
        "load": selected_load,
        "rounds": rounds,
        "measurement_seconds": seconds,
        "warmup_seconds": int(runner.state["warmup_seconds"]),
        "schedule": run_schedule,
        "release": runner.release,
    })
    runner.state["status"] = (
        "positive_control_complete" if report["complete"]
        else "positive_control_incomplete"
    )
    runner._save()
    _write(
        runner.summary / "positive-control.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def approve_smoke(root: Path, note: str) -> dict[str, Any]:
    if not note.strip():
        raise ValueError("approval note must not be empty")
    state_path = root.resolve() / "resume-state.json"
    smoke_path = root.resolve() / "summary/smoke-result.json"
    if not state_path.is_file() or not smoke_path.is_file():
        raise RuntimeError("smoke state or result is missing")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    smoke = json.loads(smoke_path.read_text(encoding="utf-8"))
    completed_safely = bool(
        smoke.get("passed")
        if "completed" not in smoke
        else smoke.get("completed") and smoke.get("safety_passed")
    )
    if (
        not completed_safely or not smoke.get("approval_eligible", True)
        or smoke.get("fingerprint") != state.get("experiment_fingerprint")
    ):
        raise RuntimeError("smoke did not pass or its fingerprint is stale")
    approval = {
        "fingerprint": state["experiment_fingerprint"],
        "note": note,
        "approved_by_uid": os.getuid(),
        "approved_realtime_ns": time.time_ns(),
        "smoke_sha256": _sha256(smoke_path),
        "accepted_warning_count": len(smoke.get("warnings", [])),
        "accepted_warnings_sha256": hashlib.sha256(
            json.dumps(smoke.get("warnings", []), sort_keys=True).encode()
        ).hexdigest(),
    }
    state["smoke_approval"] = approval
    state["status"] = "approved"
    _write(state_path, json.dumps(state, indent=2, sort_keys=True) + "\n")
    return approval


def recover(root: Path, note: str, diagnostic_root: Path | None = None) -> dict[str, Any]:
    if not note.strip():
        raise ValueError("recovery note must not be empty")
    state_path = root.resolve() / "resume-state.json"
    if not state_path.is_file():
        raise RuntimeError("resume state is missing")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    failure = state.get("hard_failure")
    if not failure or failure.get("recovered"):
        raise RuntimeError("there is no unrecovered hard failure")
    runner = AffinityGraphRunner(
        root.resolve(), Path(state["base_config"]), Path(state["source"]),
        rounds=int(state["rounds"]), seed=int(state["seed"]),
        smoke_gates=str(state.get("smoke_gates", "soft")),
        smoke_baseline_result=(
            Path(state["smoke_baseline_result"])
            if state.get("smoke_baseline_result") else None
        ),
        smoke_load=str(state.get("smoke_load", "C4T6")),
        system=str(state.get("system", "clickhouse")),
    )
    runner.release = str(state["release"])
    release_fingerprint = state["steps"]["deploy"]["result"]["fingerprint"]
    fingerprint_changed = (
        runner._experiment_fingerprint(release_fingerprint)
        != state.get("experiment_fingerprint")
    )
    diagnostic_evidence: dict[str, Any] | None = None
    if fingerprint_changed:
        if diagnostic_root is None:
            raise RuntimeError(
                "experiment fingerprint changed; provide a passed diagnostic root and "
                "start a new experiment root"
            )
        diagnostic_root = diagnostic_root.resolve()
        diagnostic_report = diagnostic_root / "summary/observe-diagnostic.json"
        diagnostic_state_path = diagnostic_root / "resume-state.json"
        if not diagnostic_report.is_file() or not diagnostic_state_path.is_file():
            raise RuntimeError("diagnostic report or state is missing")
        report = json.loads(diagnostic_report.read_text(encoding="utf-8"))
        diagnostic_state = json.loads(diagnostic_state_path.read_text(encoding="utf-8"))
        if not report.get("passed") or diagnostic_state.get("status") != "diagnostic_passed":
            raise RuntimeError("observe diagnostic did not pass")
        diagnostic_evidence = {
            "root": str(diagnostic_root),
            "report_sha256": _sha256(diagnostic_report),
            "experiment_fingerprint": diagnostic_state.get("experiment_fingerprint"),
            "release": diagnostic_state.get("release"),
            "requires_new_experiment_root": True,
        }
        runner.release = str(diagnostic_state["release"])
    runner._idle_gate()
    preflight = runner._strict_remote_preflight(runner.release)
    pair_index = next(
        (int(row["pair_index"]) for row in state.get("runs", {}).values()
         if row.get("status") == "running"),
        None,
    )
    if pair_index is not None:
        for row in state["runs"].values():
            if int(row.get("pair_index", -1)) == pair_index:
                row["status"] = "superseded"
                row["superseded_reason"] = "complete pair rerun after reviewed hard failure"
    failure["recovered"] = True
    failure["recovery"] = {
        "note": note,
        "approved_by_uid": os.getuid(),
        "realtime_ns": time.time_ns(),
        "preflight": preflight,
        "rerun_pair_index": pair_index,
        "diagnostic_evidence": diagnostic_evidence,
    }
    state["status"] = "superseded_after_recovery" if fingerprint_changed else "recovered"
    _write(state_path, json.dumps(state, indent=2, sort_keys=True) + "\n")
    return failure["recovery"]
