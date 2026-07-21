from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


POLICY_SCHEMA = "prism-sampler.policy.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _expand_cpu_list(value: str) -> set[int]:
    result: set[int] = set()
    for item in value.split(","):
        if not item:
            continue
        if "-" in item:
            start, end = item.split("-", 1)
            result.update(range(int(start), int(end) + 1))
        else:
            result.add(int(item))
    return result


def _topology(experiment: Path) -> dict[int, str]:
    for path in sorted(experiment.glob("runs/**/raw/capabilities.json")):
        value = json.loads(path.read_text())
        platform = value.get("platform") or {}
        rows = platform.get("numa_cpu_lists", {})
        if rows:
            return {int(node): str(cpus) for node, cpus in rows.items()}
    raise ValueError("no NUMA CPU topology was found in run capabilities")


def _node_pressure(experiment: Path, topology: dict[int, str]) -> dict[int, float]:
    import duckdb

    per_run: list[dict[int, float]] = []
    for db in sorted(experiment.glob("runs/**/dataset/telemetry.db3")):
        phase_path = db.parents[1] / "meta" / "phase.json"
        if not phase_path.is_file():
            continue
        phase = json.loads(phase_path.read_text())
        if phase.get("workload_clock") != "target_realtime":
            continue
        start = int(phase["workload_start_epoch_ns"]) / 1e9
        end = int(phase["workload_end_epoch_ns"]) / 1e9
        con = duckdb.connect(str(db), read_only=True)
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "system_pressure_samples" not in tables:
            con.close()
            continue
        rows = con.execute(
            "SELECT value FROM system_pressure_samples "
            "WHERE metric='proc_stat' AND epoch(ts)>=? AND epoch(ts)<? ORDER BY ts",
            [start, end],
        ).fetchall()
        con.close()
        samples: list[dict[int, tuple[int, int]]] = []
        for (text,) in rows:
            current = {}
            for line in str(text).splitlines():
                fields = line.split()
                if not fields or not re.fullmatch(r"cpu\d+", fields[0]):
                    continue
                cpu = int(fields[0][3:])
                values = [int(value) for value in fields[1:]]
                total = sum(values)
                idle = values[3] + (values[4] if len(values) > 4 else 0)
                current[cpu] = (total, idle)
            if current:
                samples.append(current)
        if len(samples) < 2:
            continue
        first, last = samples[0], samples[-1]
        pressure = {}
        for node, cpulist in topology.items():
            busy = total = 0
            for cpu in _expand_cpu_list(cpulist):
                if cpu not in first or cpu not in last:
                    continue
                total_delta = last[cpu][0] - first[cpu][0]
                idle_delta = last[cpu][1] - first[cpu][1]
                total += max(total_delta, 0)
                busy += max(total_delta - idle_delta, 0)
            if total:
                pressure[node] = busy / total
        if pressure:
            per_run.append(pressure)
    return {
        node: statistics.mean(run[node] for run in per_run if node in run)
        for node in topology
        if any(node in run for run in per_run)
    }


class DisjointSet:
    def __init__(self, names: set[str]):
        self.parent = {name: name for name in names}

    def find(self, name: str) -> str:
        while self.parent[name] != name:
            self.parent[name] = self.parent[self.parent[name]]
            name = self.parent[name]
        return name

    def members(self, root: str) -> set[str]:
        return {name for name in self.parent if self.find(name) == root}

    def merge(self, left: str, right: str) -> set[str]:
        a, b = self.find(left), self.find(right)
        if a != b:
            self.parent[b] = a
        return self.members(a)


def _group_demand(rows: pd.DataFrame) -> dict[str, float]:
    values: dict[str, list[float]] = {}
    for _, row in rows.iterrows():
        for suffix in ("a", "b"):
            group = str(row[f"group_{suffix}"])
            demand = float(row[f"active_cpus_{suffix}"]) + float(row[f"runqueue_cpus_{suffix}"])
            values.setdefault(group, []).append(demand)
    return {group: max(samples) for group, samples in values.items()}


def _communities(rows: pd.DataFrame, capacity: float, top_k: int) -> list[dict[str, Any]]:
    groups = set(rows["group_a"].astype(str)) | set(rows["group_b"].astype(str))
    demand = _group_demand(rows)
    dsu = DisjointSet(groups)
    candidates: list[dict[str, Any]] = []
    for _, edge in rows.sort_values("relationship_score_r", ascending=False).iterrows():
        left, right = str(edge["group_a"]), str(edge["group_b"])
        current = dsu.members(dsu.find(left)) | dsu.members(dsu.find(right))
        cpu_demand = sum(demand.get(group, 0.0) for group in current)
        if cpu_demand > capacity:
            continue
        members = sorted(dsu.merge(left, right))
        internal = rows[
            rows["group_a"].isin(members) & rows["group_b"].isin(members)
        ]
        candidates.append({
            "groups": members,
            "cpu_demand": cpu_demand,
            "edge_score_sum": float(internal["relationship_score_r"].sum()),
            "community_score": float(internal["relationship_score_r"].sum())
            / max(len(members) * (len(members) - 1) / 2, 1),
            "strongest_edge_r": float(internal["relationship_score_r"].max()),
            "evidence_edges": internal[[
                "group_a", "group_b", "relationship_score_r", "activity",
                "synchronization", "sharing", "stability",
            ]].to_dict("records"),
        })
    unique: dict[tuple[str, ...], dict[str, Any]] = {}
    for candidate in candidates:
        key = tuple(candidate["groups"])
        unique[key] = candidate
    return sorted(
        unique.values(), key=lambda row: (row["community_score"], row["strongest_edge_r"]),
        reverse=True,
    )[:top_k]


def _robust(rows: pd.DataFrame) -> pd.DataFrame:
    phases = max(int(rows["phase"].nunique()), 1)
    output = []
    for (group_a, group_b), values in rows.groupby(["group_a", "group_b"]):
        scores = [float(value) for value in values["relationship_score_r"]]
        presence = int(values["phase"].nunique()) / phases
        output.append({
            "phase": "robust",
            "group_a": group_a,
            "group_b": group_b,
            "relationship_score_r": statistics.median(scores) * presence,
            "phase_presence_ratio": presence,
            "active_cpus_a": float(values["active_cpus_a"].max()),
            "active_cpus_b": float(values["active_cpus_b"].max()),
            "runqueue_cpus_a": float(values["runqueue_cpus_a"].max()),
            "runqueue_cpus_b": float(values["runqueue_cpus_b"].max()),
            "activity": float(values["activity"].median()),
            "synchronization": float(values["synchronization"].median()),
            "sharing": float(values["sharing"].median()),
            "stability": float(values["stability"].median()),
        })
    return pd.DataFrame(output)


def _policy(
    scope: str,
    mode: str,
    community: dict[str, Any],
    topology: dict[int, str],
    target_node: int,
    evidence: Path,
    selection_reason: str,
) -> dict[str, Any]:
    remaining = sorted(node for node in topology if node != target_node)
    rules = [
        {"group": group, "comm_regex": _comm_regex(group), "nodes": [target_node]}
        for group in community["groups"]
    ]
    return {
        "schema": POLICY_SCHEMA,
        "status": "candidate_only",
        "apply_allowed": False,
        "expected_gain": None,
        "expected_gain_model": "unavailable",
        "scope": scope,
        "mode": mode,
        "target_nodes": [target_node],
        "target_cpu_list": topology[target_node],
        "other_nodes": remaining if mode == "limited" else None,
        "other_cpu_lists": [topology[node] for node in remaining] if mode == "limited" else None,
        "groups": community["groups"],
        "group_rules": rules,
        "cpu_demand": community["cpu_demand"],
        "capacity_limit": len(_expand_cpu_list(topology[target_node])) * 0.8,
        "relationship_evidence": {
            "edge_score_sum": community["edge_score_sum"],
            "community_score": community["community_score"],
            "strongest_edge_r": community["strongest_edge_r"],
            "edges": community["evidence_edges"],
        },
        "fallback": {"name": "one_node", "memory_policy": "system-default"},
        "selection_reason": selection_reason,
        "evidence": {"relation_candidates": str(evidence), "sha256": _sha256(evidence)},
    }


def generate_policies(experiment: Path, *, top_k: int = 5) -> dict[str, Any]:
    evidence = experiment / "summary" / "relation-candidates.csv"
    if not evidence.is_file():
        raise ValueError(f"relationship candidates do not exist: {evidence}")
    rows = pd.read_csv(evidence)
    if rows.empty:
        raise ValueError("relationship candidate table is empty")
    topology = _topology(experiment)
    pressures = _node_pressure(experiment, topology)
    target_node = min(pressures, key=lambda node: (pressures[node], node)) if pressures else min(topology)
    selection_reason = (
        f"lowest observed node CPU pressure ({pressures[target_node]:.6f})"
        if pressures else "lowest NUMA node ID; background-node pressure was unavailable"
    )
    capacity = len(_expand_cpu_list(topology[target_node])) * 0.8
    scopes = [(str(phase), values.copy()) for phase, values in rows.groupby("phase")]
    robust = _robust(rows)
    if not robust.empty:
        scopes.append(("robust", robust))
    policies = []
    for scope, values in scopes:
        for community in _communities(values, capacity, top_k):
            for mode in ("limited", "unlimited"):
                policies.append(_policy(
                    scope, mode, community, topology, target_node, evidence, selection_reason
                ))
    if not policies:
        raise ValueError("no relationship community fits the configured NUMA capacity")
    policies.sort(
        key=lambda row: (
            row["scope"] == "robust",
            row["relationship_evidence"]["community_score"],
            row["mode"] == "limited",
        ),
        reverse=True,
    )
    output = experiment / "policy"
    output.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema": "prism-sampler.policy-candidates.v1",
        "topology": {str(node): cpus for node, cpus in topology.items()},
        "capacity_headroom": 0.80,
        "node_cpu_pressure": {str(node): value for node, value in pressures.items()},
        "candidates": policies,
    }
    (output / "candidates.json").write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n")
    selected = policies[0]
    (output / "selected-policy.json").write_text(json.dumps(selected, indent=2, sort_keys=True) + "\n")
    (output / "selected-policy.toml").write_text(_toml(selected))
    render_yba(output / "selected-policy.json", output / "yba-profile.env", enable=False)
    (output / "explanation.md").write_text(_explanation(selected))
    return {"candidates": len(policies), "selected": str(output / "selected-policy.json")}


def _comm_regex(group: str) -> str:
    return "^" + re.escape(group).replace(r"\ ", "[[:space:]]") + "$"


def _toml(policy: dict[str, Any]) -> str:
    lines = [
        f'schema = "{policy["schema"]}"', f'status = "{policy["status"]}"',
        "apply_allowed = false", f'scope = "{policy["scope"]}"', f'mode = "{policy["mode"]}"',
        f'target_nodes = {json.dumps(policy["target_nodes"])}',
        f'target_cpu_list = "{policy["target_cpu_list"]}"',
        f'groups = {json.dumps(policy["groups"])}',
        f'cpu_demand = {policy["cpu_demand"]:.9f}',
        f'capacity_limit = {policy["capacity_limit"]:.9f}',
    ]
    for rule in policy["group_rules"]:
        lines.extend([
            "", "[[group_rules]]", f'name = "{rule["group"]}"',
            f'comm_regex = {json.dumps(rule["comm_regex"])}',
            f'nodes = {json.dumps(rule["nodes"])}',
        ])
    return "\n".join(lines) + "\n"


def render_yba(policy_path: Path, output: Path, *, enable: bool = False) -> Path:
    policy = json.loads(policy_path.read_text())
    validate_policy(policy)
    target_nodes = ",".join(str(node) for node in policy["target_nodes"])
    rules = " ".join(
        f'{index}:{rule["comm_regex"]}:{target_nodes}'
        for index, rule in enumerate(policy["group_rules"], 1)
    )
    default_nodes = ""
    if policy["mode"] == "limited":
        default_nodes = ",".join(str(node) for node in policy["other_nodes"])
    lines = [
        "# Generated by prism-sampler. Candidate-only unless explicitly enabled.",
        f"ENABLE_THREAD_CLUSTER={1 if enable else 0}",
        f"THREAD_CLUSTER_NODE_RULES='{rules}'",
        f"THREAD_CLUSTER_DEFAULT_NODES='{default_nodes}'",
        "THREAD_CLUSTER_STRICT=1",
        "THREAD_CLUSTER_REQUIRE_STABLE=1",
        "THREAD_CLUSTER_MIN_HIT_RATIO=0.95",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n")
    return output


def validate_policy(value: dict[str, Any] | Path) -> list[str]:
    policy = json.loads(value.read_text()) if isinstance(value, Path) else value
    errors = []
    if policy.get("schema") != POLICY_SCHEMA:
        errors.append("unsupported policy schema")
    if policy.get("apply_allowed") is not False:
        errors.append("candidate policy must set apply_allowed=false")
    if policy.get("expected_gain") is not None:
        errors.append("candidate policy must not claim expected gain")
    if policy.get("mode") not in {"limited", "unlimited"}:
        errors.append("mode must be limited or unlimited")
    if not policy.get("groups") or not policy.get("group_rules"):
        errors.append("policy has no thread groups")
    if float(policy.get("cpu_demand", math.inf)) > float(policy.get("capacity_limit", 0)):
        errors.append("community CPU demand exceeds capacity limit")
    if errors:
        raise ValueError("; ".join(errors))
    return errors


def _explanation(policy: dict[str, Any]) -> str:
    groups = ", ".join(policy["groups"])
    evidence = policy["relationship_evidence"]
    return (
        "# Prism Sampler Candidate Policy\n\n"
        f"- Scope: `{policy['scope']}`\n"
        f"- Mode: `{policy['mode']}`\n"
        f"- Groups: {groups}\n"
        f"- Target NUMA node: `{policy['target_nodes'][0]}`\n"
        f"- CPU demand: `{policy['cpu_demand']:.4f}` / `{policy['capacity_limit']:.4f}`\n"
        f"- Relationship edge score sum: `{evidence['edge_score_sum']:.4f}`\n\n"
        f"- Community edge density: `{evidence['community_score']:.4f}`\n\n"
        "This output is candidate-only. G has not been calibrated, expected throughput gain is unknown, "
        "and the generated YBA profile remains disabled by default.\n"
    )
