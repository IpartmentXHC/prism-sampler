from __future__ import annotations

import csv
import json
import math
import os
import random
import re
import statistics
import time
from pathlib import Path
from typing import Any

from .config import load_config
from .deploy import install_client
from .orchestration.runner import run_yba_suite
from .pressure_v2 import render_controller_config
from .remote import Host


SCHEMA = "prism-sampler.g-place-calibration.v1"
HISTORICAL_GLOB = "20260727-2*-pressure-v2-crossover-*"
LOADS = {"C2T2": (2, 2), "C4T6": (4, 6), "C5T16": (5, 16)}
PROFILES = ("self_compact", "self_split", "pair_colocate", "pair_separate")


def prepare_stage_c_client(remote_config: Path) -> None:
    client = Host("ubuntu197")
    install_client(client, "/home/xhc/.local/src/prism-sampler")
    client.copy_to(remote_config, "/home/xhc/.config/prism-sampler/local.toml")


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def select_r_candidates(
    experiments: list[Path], *, minimum_confidence: float = 0.8
) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict_nested()
    for experiment in experiments:
        for path in experiment.glob("runs/**/raw/live-candidates.jsonl"):
            phase = path.parts[-4]
            if phase not in LOADS:
                continue
            for snapshot in _jsonl(path):
                quality = float((snapshot.get("quality") or {}).get("confidence") or 0.0)
                if quality < minimum_confidence:
                    continue
                for row in snapshot.get("self_candidates") or []:
                    if float(row.get("confidence") or 0.0) < minimum_confidence:
                        continue
                    evidence[phase]["self"].append({
                        "key": str(row["group_name"]),
                        "score": float(row.get("self_score_r") or 0.0),
                        "demand": float(row.get("active_cpus") or 0.0),
                        "confidence": float(row.get("confidence") or 0.0),
                    })
                for row in snapshot.get("pair_candidates") or []:
                    if float(row.get("confidence") or 0.0) < minimum_confidence:
                        continue
                    pair = tuple(sorted((str(row["group_a"]), str(row["group_b"]))))
                    evidence[phase]["pair"].append({
                        "key": "\0".join(pair),
                        "groups": pair,
                        "score": float(row.get("relationship_score_r") or 0.0),
                        "demand": float(row.get("active_cpus_a") or 0.0)
                        + float(row.get("active_cpus_b") or 0.0),
                        "confidence": float(row.get("confidence") or 0.0),
                    })
    result = {}
    for load in LOADS:
        selected: dict[str, Any] = {}
        for kind in ("self", "pair"):
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in evidence[load][kind]:
                grouped.setdefault(row["key"], []).append(row)
            ranked = []
            maximum_windows = max((len(rows) for rows in grouped.values()), default=0)
            for key, rows in grouped.items():
                median_score = statistics.median(row["score"] for row in rows)
                presence_ratio = len(rows) / maximum_windows if maximum_windows else 0.0
                ranked.append({
                    "key": key,
                    "groups": list(rows[0].get("groups") or (key,)),
                    "median_score": median_score,
                    "presence_ratio": presence_ratio,
                    "robust_score": median_score * presence_ratio,
                    "median_demand_cpu_equiv": statistics.median(row["demand"] for row in rows),
                    "median_confidence": statistics.median(row["confidence"] for row in rows),
                    "windows": len(rows),
                })
            ranked.sort(
                key=lambda row: (-row["robust_score"], -row["windows"], row["key"])
            )
            if not ranked:
                raise RuntimeError(f"no mature {kind} R candidate for {load}")
            selected[kind] = ranked[0]
            selected[f"{kind}_ranking"] = ranked
        result[load] = selected
    return result


def defaultdict_nested() -> dict[str, dict[str, list[dict[str, Any]]]]:
    return {load: {"self": [], "pair": []} for load in LOADS}


def _rule_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)[:32] or "candidate"


def render_place_suite(
    load: str,
    candidate: dict[str, Any],
    scenario: Path,
    output: Path,
    *,
    rounds: int = 5,
    seed: int = 20260730,
) -> dict[str, Any]:
    self_group = str(candidate["self"]["groups"][0])
    pair_a, pair_b = (str(value) for value in candidate["pair"]["groups"])
    self_pattern = f"^{re.escape(self_group)}$"
    pair_a_pattern = f"^{re.escape(pair_a)}$"
    pair_b_pattern = f"^{re.escape(pair_b)}$"
    value = f"""SUITE_NAME=clickhouse-g-place-{load.lower()}
SUITE_SCENARIOS='{load.lower()}={scenario}'
SUITE_ROUNDS={rounds}
SUITE_ORDER=randomized
SUITE_RANDOM_SEED={seed}
SUITE_BASELINE_PROFILE=self_split
SUITE_PROFILES='{' '.join(PROFILES)}'
SUITE_PROFILE_self_compact_KIND=thread_cluster
SUITE_PROFILE_self_compact_CPU_NODES=0,1
SUITE_PROFILE_self_compact_CPU_RULES='{_rule_name(self_group)}:{self_pattern}:0-31'
SUITE_PROFILE_self_compact_DEFAULT_CPUS=0-63
SUITE_PROFILE_self_split_KIND=thread_cluster
SUITE_PROFILE_self_split_CPU_NODES=0,1
SUITE_PROFILE_self_split_CPU_RULES='{_rule_name(self_group)}:{self_pattern}:0-15,32-47'
SUITE_PROFILE_self_split_DEFAULT_CPUS=0-63
SUITE_PROFILE_pair_colocate_KIND=thread_cluster
SUITE_PROFILE_pair_colocate_CPU_NODES=0,1
SUITE_PROFILE_pair_colocate_CPU_RULES='pair:{pair_a_pattern}|{pair_b_pattern}:0-31'
SUITE_PROFILE_pair_colocate_DEFAULT_CPUS=0-63
SUITE_PROFILE_pair_separate_KIND=thread_cluster
SUITE_PROFILE_pair_separate_CPU_NODES=0,1
SUITE_PROFILE_pair_separate_CPU_RULES='a:{pair_a_pattern}:0-31 b:{pair_b_pattern}:32-63'
SUITE_PROFILE_pair_separate_DEFAULT_CPUS=0-63
"""
    for profile in PROFILES:
        value += (
            f"SUITE_PROFILE_{profile}_MODE=watch\n"
            f"SUITE_PROFILE_{profile}_BIND_INTERVAL=0.2\n"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(value)
    return {"output": str(output), "self_group": self_group, "pair": [pair_a, pair_b]}


def render_place_scenario(load: str, output: Path, *, seconds: int = 60) -> Path:
    clients, threads = LOADS[load]
    output.write_text(f"""SCENARIO_NAME=g_place_{load.lower()}
SCENARIO_BUDGET_MODE=duration
SCENARIO_PHASES='{load}'
SCENARIO_MIN_PHASE_SECONDS=60
SCENARIO_TIMEOUT_GRACE_SECONDS=60
SCENARIO_DURATION_TOLERANCE_SECONDS=3
SCENARIO_DURATION_OPERATIONCOUNT_PER_CLIENT=2147483647
SCENARIO_PHASE_{load}_CLIENTS={clients}
SCENARIO_PHASE_{load}_THREADS={threads}
SCENARIO_PHASE_{load}_VALUE={seconds}
""")
    return output


def _paired_effects(suite: Path, load: str) -> list[dict[str, Any]]:
    with (suite / "suite-summary.csv").open(newline="", encoding="utf-8") as stream:
        rows = [row for row in csv.DictReader(stream) if row["status"] == "ok"]
    indexed = {(row["profile"], int(row["round"])): row for row in rows}
    output = []
    for action, reference, action_type in (
        ("self_compact", "self_split", "G_self"),
        ("pair_colocate", "pair_separate", "G_pair"),
    ):
        for round_number in range(1, 6):
            left = indexed.get((action, round_number))
            right = indexed.get((reference, round_number))
            if not left or not right:
                continue
            left_value = float(left["throughput"])
            right_value = float(right["throughput"])
            output.append({
                "load": load,
                "round": round_number,
                "action_type": action_type,
                "action_profile": action,
                "reference_profile": reference,
                "action_throughput_ops_s": left_value,
                "reference_throughput_ops_s": right_value,
                "gain_pct": 100 * (left_value / right_value - 1.0),
                "action_p99_us": float(left["p99_latency"]),
                "reference_p99_us": float(right["p99_latency"]),
            })
    return output


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def execute_stage_c(
    root: Path,
    stage_b_state: Path,
    selected: Path,
    base_config: Path,
    *,
    seed: int = 20260730,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    generated = root / "generated-config"
    summary = root / "summary"
    generated.mkdir(exist_ok=True)
    summary.mkdir(exist_ok=True)
    state = json.loads(stage_b_state.read_text())
    experiments = [
        Path(row["experiment"])
        for row in state["runs"].values()
        if row.get("status") == "complete"
    ]
    experiments.extend(
        sorted(Path("/data/threadState/experiments/clickhouse").glob(HISTORICAL_GLOB))
    )
    experiments = list(dict.fromkeys(experiments))
    candidates = select_r_candidates(experiments)
    (summary / "g-place-candidates.json").write_text(
        json.dumps(candidates, indent=2, sort_keys=True) + "\n"
    )
    base = generated / "base.env"
    base.write_text(
        base_config.read_text()
        + "\nTHREAD_CLUSTER_TASKSET_WITH_SUDO=1\n"
        + "THREAD_CLUSTER_MIN_HIT_RATIO=0.95\n"
        + f"CLICKHOUSE_MAX_THREADS={json.loads(selected.read_text())['max_threads']}\n"
        + f"CLICKHOUSE_CONCURRENT_THREADS={json.loads(selected.read_text())['two_node_slots']}\n"
        + "CLICKHOUSE_CONCURRENT_RATIO=0\n"
    )
    local = generated / "sampler-local.toml"
    remote = generated / "sampler-remote.toml"
    render_controller_config(
        selected, local, target_host="kunpen183",
        output_root="/data/threadState/experiments", mode="off",
        initial_state="two_node", sampling_profile="placement-validation",
    )
    render_controller_config(
        selected, remote, target_host="192.168.70.183",
        output_root="/home/xhc/.local/share/prism-sampler/experiments", mode="off",
        initial_state="two_node", sampling_profile="placement-validation",
    )
    prepare_stage_c_client(remote)
    run_roots = {}
    for index, load in enumerate(LOADS):
        scenario = render_place_scenario(load, generated / f"scenario-{load}.env")
        suite = generated / f"suite-{load}.env"
        render_place_suite(load, candidates[load], scenario, suite, seed=seed + index)
        experiment_root = root / f"stage-c-v4-{load}"
        code = run_yba_suite(
            load_config(local), base, suite, experiment_root=experiment_root,
            resume=(experiment_root / "yba-suite").exists(),
        )
        if code:
            raise RuntimeError(f"G_place suite failed for {load}: {code}")
        run_roots[load] = str(experiment_root)
    effects = []
    for load, path in run_roots.items():
        effects.extend(_paired_effects(Path(path) / "yba-suite", load))
    _write_csv(summary / "g-place-effects.csv", effects)
    table = []
    for load in LOADS:
        for action_type in ("G_self", "G_pair"):
            values = [
                float(row["gain_pct"]) for row in effects
                if row["load"] == load and row["action_type"] == action_type
            ]
            table.append({
                "load": load,
                "action_type": action_type,
                "samples": len(values),
                "median_gain_pct": statistics.median(values) if values else math.nan,
                "variance": statistics.variance(values) if len(values) > 1 else math.nan,
                "positive_rounds": sum(value > 0 for value in values),
            })
    _write_csv(summary / "g-place-benefit-table.csv", table)
    report = {
        "schema": SCHEMA,
        "candidates": candidates,
        "run_roots": run_roots,
        "effects": len(effects),
        "cells_with_five_rounds": sum(row["samples"] == 5 for row in table),
        "capacity_envelope": {"nodes": [0, 1], "cpus": "0-63", "memory_policy": "default"},
        "passed": len(effects) == 30 and all(row["samples"] == 5 for row in table),
    }
    (summary / "stage-c-validation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report
