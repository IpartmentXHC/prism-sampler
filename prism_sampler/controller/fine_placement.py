from __future__ import annotations

import itertools
import json
import math
import time
from pathlib import Path
from typing import Any


SCHEMA = "prism-sampler.fine-placement-shadow.v1"


def _partition(
    groups: list[str],
    demand: dict[str, float],
    edges: list[dict[str, Any]],
    nodes: tuple[int, ...],
    capacities: dict[int, int],
) -> tuple[dict[str, int], dict[int, float], float, float, bool]:
    if len(nodes) == 1:
        node = nodes[0]
        load = sum(demand.values())
        return (
            {group: node for group in groups},
            {node: load},
            0.0,
            sum(float(edge["score"]) for edge in edges),
            load <= capacities[node],
        )

    total = sum(demand.values())
    target = total / len(nodes) if nodes else total
    best: tuple[float, tuple[int, ...], dict[int, float], float, float, bool] | None = None
    assignments = itertools.product(nodes, repeat=max(0, len(groups) - 1))
    for suffix in assignments:
        values = (nodes[0], *suffix)
        mapping = dict(zip(groups, values))
        loads = {
            node: sum(demand[group] for group in groups if mapping[group] == node)
            for node in nodes
        }
        overload = sum(max(0.0, loads[node] - capacities[node]) ** 2 for node in nodes)
        cut = sum(
            float(edge["score"])
            for edge in edges
            if mapping[edge["group_a"]] != mapping[edge["group_b"]]
        )
        internal = sum(float(edge["score"]) for edge in edges) - cut
        imbalance = sum((loads[node] - target) ** 2 for node in nodes)
        objective = overload * 1_000_000.0 + cut + imbalance
        candidate = (objective, values, loads, cut, internal, overload == 0)
        if best is None or candidate[:2] < best[:2]:
            best = candidate
    assert best is not None
    _, values, loads, cut, internal, feasible = best
    return dict(zip(groups, values)), loads, cut, internal, feasible


def build_fine_placement(
    snapshot: dict[str, Any],
    *,
    phase: str,
    scaling_state: str,
    available_nodes: tuple[int, ...],
    node_cpus: dict[int, set[int]],
    pair_threshold: float,
    self_threshold: float,
    minimum_confidence: float,
    cluster_size: int,
) -> dict[str, Any]:
    quality = dict(snapshot.get("quality") or {})
    confidence = float(quality.get("confidence") or 0.0)
    pair_rows = [
        {
            "group_a": str(row["group_a"]),
            "group_b": str(row["group_b"]),
            "score": float(row.get("relationship_score_r") or 0.0),
            "confidence": float(row.get("confidence") or 0.0),
        }
        for row in snapshot.get("pair_candidates", [])
        if float(row.get("relationship_score_r") or 0.0) >= pair_threshold
        and float(row.get("confidence") or 0.0) >= minimum_confidence
    ]
    self_rows = [
        row for row in snapshot.get("self_candidates", [])
        if float(row.get("self_score_r") or 0.0) >= self_threshold
        and float(row.get("confidence") or 0.0) >= minimum_confidence
    ]
    demand: dict[str, float] = {}
    self_scores: dict[str, float] = {}
    for row in snapshot.get("pair_candidates", []):
        demand[str(row["group_a"])] = max(
            demand.get(str(row["group_a"]), 0.0),
            float(row.get("active_cpus_a") or 0.0),
        )
        demand[str(row["group_b"])] = max(
            demand.get(str(row["group_b"]), 0.0),
            float(row.get("active_cpus_b") or 0.0),
        )
    for row in snapshot.get("self_candidates", []):
        group = str(row["group_name"])
        demand[group] = max(demand.get(group, 0.0), float(row.get("active_cpus") or 0.0))
        self_scores[group] = float(row.get("self_score_r") or 0.0)
    groups = sorted(
        {
            group
            for edge in pair_rows
            for group in (edge["group_a"], edge["group_b"])
        }
        | {str(row["group_name"]) for row in self_rows}
    )
    demand = {group: max(demand.get(group, 0.0), 0.01) for group in groups}
    capacities = {node: len(node_cpus[node]) for node in available_nodes}
    base = {
        "schema": SCHEMA,
        "realtime_ns": time.time_ns(),
        "phase": phase,
        "scaling_state": scaling_state,
        "available_nodes": list(available_nodes),
        "source_sequence_start": snapshot.get("sequence_start"),
        "source_sequence_end": snapshot.get("sequence_end"),
        "source_window_end_epoch_ms": snapshot.get("window_end_epoch_ms"),
        "quality": quality,
        "apply_allowed": False,
        "mode": "shadow",
    }
    if confidence < minimum_confidence:
        return {**base, "status": "insufficient_confidence", "groups": len(groups)}
    if not groups:
        return {**base, "status": "no_eligible_relationships", "groups": 0}
    if len(groups) > 18:
        return {**base, "status": "graph_too_large", "groups": len(groups)}

    assignment, loads, cut, internal, feasible = _partition(
        groups, demand, pair_rows, available_nodes, capacities
    )
    shards: dict[str, dict[int, float]] = {
        group: {assignment[group]: demand[group]} for group in groups
    }
    adjusted_loads = dict(loads)
    for node in available_nodes:
        overload = max(0.0, adjusted_loads[node] - capacities[node])
        if overload <= 0:
            continue
        for group in sorted(
            (item for item in groups if assignment[item] == node),
            key=lambda item: (-demand[item], item),
        ):
            for target in sorted(available_nodes, key=lambda item: adjusted_loads[item]):
                if target == node or overload <= 0:
                    continue
                available = max(0.0, capacities[target] - adjusted_loads[target])
                moved = min(overload, available, shards[group].get(node, 0.0))
                if moved <= 0:
                    continue
                shards[group][node] -= moved
                shards[group][target] = shards[group].get(target, 0.0) + moved
                adjusted_loads[node] -= moved
                adjusted_loads[target] += moved
                overload -= moved
            if overload <= 0:
                break
    feasible = all(adjusted_loads[node] <= capacities[node] + 1e-9 for node in available_nodes)
    cluster_cpus: dict[str, list[int]] = {group: [] for group in groups}
    cluster_cpus_by_node: dict[str, dict[str, list[int]]] = {
        group: {} for group in groups
    }
    cluster_feasible = True
    for node in available_nodes:
        cpus = sorted(node_cpus[node])
        clusters = [cpus[index:index + cluster_size] for index in range(0, len(cpus), cluster_size)]
        offset = 0
        for group in sorted(
            (item for item in groups if shards[item].get(node, 0.0) > 0),
            key=lambda item: (-shards[item].get(node, 0.0), item),
        ):
            needed = max(1, math.ceil(shards[group][node] / cluster_size))
            selected = clusters[offset:offset + needed]
            selected_cpus = [cpu for cluster in selected for cpu in cluster]
            cluster_cpus[group].extend(selected_cpus)
            cluster_cpus_by_node[group][str(node)] = selected_cpus
            offset += needed
            if len(selected) < needed:
                cluster_feasible = False
    return {
        **base,
        "status": "candidate" if feasible and cluster_feasible else "capacity_infeasible",
        "groups": len(groups),
        "eligible_edges": len(pair_rows),
        "group_demand_cpu_equiv": demand,
        "self_scores": {group: self_scores.get(group, 0.0) for group in groups},
        "assignment": assignment,
        "node_shards_cpu_equiv": {
            group: {str(node): value for node, value in sorted(values.items()) if value > 0}
            for group, values in shards.items()
        },
        "capacity_forced_split_groups": sorted(
            group for group, values in shards.items() if len([value for value in values.values() if value > 0]) > 1
        ),
        "cluster_cpus": cluster_cpus,
        "cluster_cpus_by_node": cluster_cpus_by_node,
        "node_load_cpu_equiv": {str(node): adjusted_loads[node] for node in available_nodes},
        "node_capacity_cpus": {str(node): capacities[node] for node in available_nodes},
        "cut_relationship_score": cut,
        "internal_relationship_score": internal,
        "capacity_feasible": feasible and cluster_feasible,
        "formula": "balanced capacity-constrained R_pair cut; R_self retained as compactness evidence",
    }


class FinePlacementShadow:
    def __init__(self) -> None:
        self.last_key: tuple[str, object, str] | None = None

    def poll(
        self,
        path: Path,
        *,
        phase: str,
        scaling_state: str,
        available_nodes: tuple[int, ...],
        node_cpus: dict[int, set[int]],
        pair_threshold: float,
        self_threshold: float,
        minimum_confidence: float,
        cluster_size: int,
    ) -> dict[str, Any] | None:
        try:
            snapshot = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        key = (phase, snapshot.get("sequence_end"), scaling_state)
        if key == self.last_key:
            return None
        self.last_key = key
        return build_fine_placement(
            snapshot,
            phase=phase,
            scaling_state=scaling_state,
            available_nodes=available_nodes,
            node_cpus=node_cpus,
            pair_threshold=pair_threshold,
            self_threshold=self_threshold,
            minimum_confidence=minimum_confidence,
            cluster_size=cluster_size,
        )


def replay_fine_placement_experiment(
    experiment: Path,
    output: Path,
    *,
    node_cpus: dict[int, set[int]],
    one_node_nodes: tuple[int, ...] = (0,),
    two_node_nodes: tuple[int, ...] = (0, 1),
    pair_threshold: float = 10.0,
    self_threshold: float = 10.0,
    minimum_confidence: float = 0.7,
    cluster_size: int = 4,
) -> dict[str, Any]:
    samples = []
    sample_path = experiment / "controller" / "samples.jsonl"
    if sample_path.is_file():
        samples = [
            json.loads(line)
            for line in sample_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    final_states: dict[str, str] = {}
    for row in samples:
        if row.get("phase"):
            final_states[str(row["phase"])] = str(row.get("actual_state", "one_node"))
    rows = []
    for path in sorted(experiment.glob("runs/*/r*/raw/live-candidates-latest.json")):
        phase = path.parts[-4]
        state = final_states.get(phase, "one_node")
        nodes = one_node_nodes if state == "one_node" else two_node_nodes
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        rows.append(build_fine_placement(
            snapshot,
            phase=phase,
            scaling_state=state,
            available_nodes=nodes,
            node_cpus=node_cpus,
            pair_threshold=pair_threshold,
            self_threshold=self_threshold,
            minimum_confidence=minimum_confidence,
            cluster_size=cluster_size,
        ))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    summary = {
        "schema": "prism-sampler.fine-placement-replay.v1",
        "experiment": str(experiment),
        "graphs": len(rows),
        "candidates": sum(row.get("status") == "candidate" for row in rows),
        "capacity_infeasible": sum(
            row.get("status") == "capacity_infeasible" for row in rows
        ),
        "insufficient_confidence": sum(
            row.get("status") == "insufficient_confidence" for row in rows
        ),
        "apply_allowed": any(bool(row.get("apply_allowed")) for row in rows),
        "output": str(output),
    }
    output.with_suffix(".summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return summary


def validate_fine_placement_log(path: Path, output: Path) -> dict[str, Any]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    candidates = [row for row in rows if row.get("status") == "candidate"]
    sequences = [
        int(row["source_sequence_end"])
        for row in rows if row.get("source_sequence_end") is not None
    ]
    report = {
        "schema": "prism-sampler.fine-placement-validation.v1",
        "path": str(path),
        "windows": len(rows),
        "candidate_windows": len(candidates),
        "warmup_rejections": sum(
            row.get("status") == "insufficient_confidence" for row in rows
        ),
        "capacity_infeasible": sum(
            row.get("status") == "capacity_infeasible" for row in rows
        ),
        "apply_allowed": any(bool(row.get("apply_allowed")) for row in rows),
        "sequences_strictly_increasing": all(
            current > previous for previous, current in zip(sequences, sequences[1:])
        ),
        "final_confidence": (
            float(rows[-1].get("quality", {}).get("confidence") or 0.0)
            if rows else 0.0
        ),
        "final_groups": int(rows[-1].get("groups") or 0) if rows else 0,
        "final_capacity_feasible": bool(rows[-1].get("capacity_feasible")) if rows else False,
    }
    report["passed"] = bool(
        report["windows"] > 0
        and report["candidate_windows"] > 0
        and report["capacity_infeasible"] == 0
        and not report["apply_allowed"]
        and report["sequences_strictly_increasing"]
        and report["final_confidence"] >= 0.7
        and report["final_groups"] > 0
        and report["final_capacity_feasible"]
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
