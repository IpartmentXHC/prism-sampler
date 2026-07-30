from __future__ import annotations

import json
import os
import random
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .config import load_config
from .controller.blackbox_model import extract_action_dataset, train_blackbox_model
from .orchestration.runner import run_yba
from .pressure_v2 import prepare_crossover_scenario, render_controller_config
from .remote import Host


SCHEMA = "prism-sampler.blackbox-stage-b.v1"
HISTORICAL_GLOB = "20260727-2*-pressure-v2-crossover-*"


@contextmanager
def _environment(values: dict[str, str]):
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _schedule(seed: int) -> list[dict[str, Any]]:
    entries = []
    for load, rounds in (("C1T1", 10), ("C4T6", 6), ("C5T16", 6)):
        for round_number in range(1, rounds + 1):
            initial = "one_node" if round_number % 2 else "two_node"
            order = "one-two-one" if initial == "one_node" else "two-one-two"
            transitions = (
                ["90:two_node", "210:one_node"]
                if initial == "one_node"
                else ["90:one_node", "210:two_node"]
            )
            entries.append({
                "load": load,
                "round": round_number,
                "initial": initial,
                "order": order,
                "transitions": transitions,
            })
    random.Random(seed).shuffle(entries)
    for index, entry in enumerate(entries, 1):
        entry["order_index"] = index
    return entries


def _selected_env(selected: Path, initial: str) -> dict[str, str]:
    value = json.loads(selected.read_text(encoding="utf-8"))
    slots = value["one_node_slots"] if initial == "one_node" else value["two_node_slots"]
    return {
        "CLICKHOUSE_MAX_THREADS": str(value["max_threads"]),
        "CLICKHOUSE_CONCURRENT_THREADS": str(slots),
        "CLICKHOUSE_CONCURRENT_RATIO": "0",
        "SERVER_CPU_NODES": "",
    }


def _write_state(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _space_gate(root: Path) -> None:
    local_free = shutil.disk_usage(root).free
    if local_free < 30 * 1024**3:
        raise RuntimeError(f"local free space below 30 GiB: {local_free / 1024**3:.1f}")
    remote_free = int(
        Host("ubuntu197").run("df -Pk / | awk 'NR==2 {print $4 * 1024}'").stdout.strip()
    )
    if remote_free < 50 * 1024**3:
        raise RuntimeError(f"ubuntu197 free space below 50 GiB: {remote_free / 1024**3:.1f}")


def _find_experiment(name: str, before: set[Path]) -> Path:
    parent = Path("/data/threadState/experiments/clickhouse")
    matches = set(parent.glob(f"*-{name}")) - before
    if len(matches) != 1:
        raise RuntimeError(f"expected one new experiment for {name}: {sorted(matches)}")
    return matches.pop()


def _validate_experiment(path: Path) -> dict[str, Any]:
    rows = extract_action_dataset([path])
    valid = [
        row for row in rows
        if row["system_windows"] >= 3
        and row["label_pre_windows"] >= 3
        and row["label_post_windows"] >= 3
        and row["has_proc"] and row["has_pmu"] and row["has_numa"] and row["has_r"]
    ]
    result = {
        "actions": len(rows),
        "valid_actions": len(valid),
        "directions": sorted(row["direction"] for row in valid),
    }
    if result != {
        "actions": 2,
        "valid_actions": 2,
        "directions": ["expand", "shrink"],
    }:
        raise RuntimeError(f"invalid crossover evidence: {path}: {result}")
    return result


def execute_stage_b(
    root: Path,
    selected: Path,
    base_config: Path,
    *,
    seed: int = 20260730,
) -> dict[str, Any]:
    root = root.resolve()
    selected = selected.resolve()
    base_config = base_config.resolve()
    root.mkdir(parents=True, exist_ok=True)
    generated = root / "generated-config"
    summary = root / "summary"
    generated.mkdir(exist_ok=True)
    summary.mkdir(exist_ok=True)
    state_path = root / "stage-b-state.json"
    if state_path.is_file():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("seed") != seed:
            raise ValueError("cannot change seed for an existing stage B run")
    else:
        state = {
            "schema": SCHEMA,
            "seed": seed,
            "started_realtime_ns": time.time_ns(),
            "schedule": _schedule(seed),
            "runs": {},
        }
        _write_state(state_path, state)
    server_config_root = "/home/xhc/.local/share/prism-sampler/experiments"
    for entry in state["schedule"]:
        key = f"{entry['order_index']:02d}-{entry['load']}-{entry['order']}-r{entry['round']}"
        if state["runs"].get(key, {}).get("status") == "complete":
            continue
        _space_gate(root)
        scenario = generated / f"scenario-{entry['load']}.env"
        prepare_crossover_scenario(entry["load"], scenario)
        local = generated / f"{key}-local.toml"
        remote = generated / f"{key}-remote.toml"
        render_controller_config(
            selected,
            local,
            target_host="kunpen183",
            output_root="/data/threadState/experiments",
            mode="active",
            initial_state=entry["initial"],
            scripted_transitions=entry["transitions"],
        )
        render_controller_config(
            selected,
            remote,
            target_host="192.168.70.183",
            output_root=server_config_root,
            mode="active",
            initial_state=entry["initial"],
            scripted_transitions=entry["transitions"],
        )
        Host("ubuntu197").copy_to(remote, "/home/xhc/.config/prism-sampler/local.toml")
        name = f"blackbox-stage-b-{entry['load']}-{entry['order']}-r{entry['round']}"
        parent = Path("/data/threadState/experiments/clickhouse")
        existing = sorted(parent.glob(f"*-{name}"))
        state["runs"][key] = {"status": "running", "name": name}
        _write_state(state_path, state)
        path = None
        if existing:
            try:
                _validate_experiment(existing[-1])
                path = existing[-1]
            except RuntimeError:
                state["runs"][key]["abandoned_experiment"] = str(existing[-1])
                _write_state(state_path, state)
        if path is None:
            before = set(parent.glob(f"*-{name}"))
            with _environment(_selected_env(selected, entry["initial"])):
                code = run_yba(
                    load_config(local), base_config, scenario, experiment_name=name
                )
            if code:
                state["runs"][key] = {"status": "failed", "returncode": code}
                _write_state(state_path, state)
                raise RuntimeError(f"YBA returned {code}: {name}")
            path = _find_experiment(name, before)
        validation = _validate_experiment(path)
        state["runs"][key] = {
            "status": "complete",
            "experiment": str(path),
            "validation": validation,
        }
        _write_state(state_path, state)
    historical = sorted(
        Path("/data/threadState/experiments/clickhouse").glob(HISTORICAL_GLOB)
    )
    fresh = [Path(row["experiment"]) for row in state["runs"].values()]
    report = train_blackbox_model(historical + fresh, summary / "stage-b-model")
    actions = extract_action_dataset(historical + fresh)
    counts: dict[str, dict[str, int]] = {}
    for row in actions:
        group = counts.setdefault(row["validation_group"], {"expand": 0, "shrink": 0})
        group[row["direction"]] += 1
    result = {
        "schema": SCHEMA,
        "experiments": len(fresh),
        "new_valid_actions": sum(
            int(row["validation"]["valid_actions"]) for row in state["runs"].values()
        ),
        "all_valid_actions": report["dataset"]["valid_actions"],
        "actions_by_pressure_group": counts,
        "model_validation": report["validation"],
        "output": str(summary / "stage-b-model"),
    }
    (summary / "stage-b-validation.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    state["finished_realtime_ns"] = time.time_ns()
    state["result"] = result
    _write_state(state_path, state)
    return result
