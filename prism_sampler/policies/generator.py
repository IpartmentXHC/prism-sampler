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


def _self_demand(rows: pd.DataFrame) -> dict[str, float]:
    return {
        str(row["group_name"]): float(row["active_cpus"]) + float(row["runqueue_cpus"])
        for _, row in rows.iterrows()
    }


def _self_evidence(rows: pd.DataFrame, members: list[str]) -> list[dict[str, Any]]:
    if rows.empty:
        return []
    selected = rows[rows["group_name"].astype(str).isin(members)]
    columns = [
        "group_name", "self_score_r", "activity", "synchronization", "sharing",
        "stability", "active_cpus", "runqueue_cpus", "thread_count",
    ]
    return selected[[column for column in columns if column in selected.columns]].to_dict("records")


def _communities(
    rows: pd.DataFrame, self_rows: pd.DataFrame, capacity: float, top_k: int
) -> list[dict[str, Any]]:
    groups = set(rows["group_a"].astype(str)) | set(rows["group_b"].astype(str))
    demand = _group_demand(rows)
    demand.update(_self_demand(self_rows))
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
        self_scores = _self_evidence(self_rows, members)
        self_score_sum = sum(float(row["self_score_r"]) for row in self_scores)
        pair_score_sum = float(internal["relationship_score_r"].sum())
        self_density = self_score_sum / max(len(members), 1)
        pair_density = pair_score_sum / max(len(members) * (len(members) - 1) / 2, 1)
        candidates.append({
            "action_type": "multi_group_colocation",
            "groups": members,
            "cpu_demand": cpu_demand,
            "self_score_sum": self_score_sum,
            "pair_score_sum": pair_score_sum,
            "self_score_density": self_density,
            "pair_score_density": pair_density,
            "community_static_score": self_density + pair_density,
            "strongest_edge_r": float(internal["relationship_score_r"].max()),
            "evidence_self": self_scores,
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
        unique.values(),
        key=lambda row: (row["community_static_score"], row["strongest_edge_r"]),
        reverse=True,
    )[:top_k]


def _singletons(rows: pd.DataFrame, capacity: float, top_k: int) -> list[dict[str, Any]]:
    if rows.empty:
        return []
    candidates = []
    for _, row in rows.sort_values("self_score_r", ascending=False).iterrows():
        score = float(row["self_score_r"])
        demand = float(row["active_cpus"]) + float(row["runqueue_cpus"])
        if score <= 0 or int(row.get("thread_count", 0)) < 2 or demand > capacity:
            continue
        evidence_self = _self_evidence(pd.DataFrame([row]), [str(row["group_name"])])
        candidates.append({
            "action_type": "singleton_colocation",
            "groups": [str(row["group_name"])],
            "cpu_demand": demand,
            "self_score_sum": score,
            "pair_score_sum": 0.0,
            "self_score_density": score,
            "pair_score_density": 0.0,
            "community_static_score": score,
            "strongest_edge_r": 0.0,
            "evidence_self": evidence_self,
            "evidence_edges": [],
        })
    return candidates[:top_k]


def _robust(rows: pd.DataFrame, phases: int) -> pd.DataFrame:
    phases = max(phases, 1)
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


def _robust_self(rows: pd.DataFrame, phases: int) -> pd.DataFrame:
    phases = max(phases, 1)
    output = []
    for group_name, values in rows.groupby("group_name"):
        present = values[values["self_score_r"] > 0]
        if present.empty:
            continue
        scores = [float(value) for value in present["self_score_r"]]
        presence = int(present["phase"].nunique()) / phases
        output.append({
            "phase": "robust",
            "group_name": group_name,
            "self_score_r": statistics.median(scores) * presence,
            "phase_presence_ratio": presence,
            "thread_count": int(present["thread_count"].max()),
            "active_cpus": float(present["active_cpus"].max()),
            "runqueue_cpus": float(present["runqueue_cpus"].max()),
            "activity": float(present["activity"].median()),
            "synchronization": float(present["synchronization"].median()),
            "sharing": float(present["sharing"].median()),
            "stability": float(present["stability"].median()),
        })
    return pd.DataFrame(output)


def _policy(
    scope: str,
    mode: str,
    community: dict[str, Any],
    topology: dict[int, str],
    target_node: int,
    evidence: list[Path],
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
        "action_type": community["action_type"],
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
            "self_score_sum": community["self_score_sum"],
            "pair_score_sum": community["pair_score_sum"],
            "self_score_density": community["self_score_density"],
            "pair_score_density": community["pair_score_density"],
            "community_static_score": community["community_static_score"],
            "strongest_edge_r": community["strongest_edge_r"],
            "self_scores": community["evidence_self"],
            "edges": community["evidence_edges"],
        },
        "fallback": {"name": "one_node", "memory_policy": "system-default"},
        "selection_reason": selection_reason,
        "evidence": [
            {"path": str(path), "sha256": _sha256(path)} for path in evidence
        ],
    }


def generate_policies(experiment: Path, *, top_k: int = 5) -> dict[str, Any]:
    pair_evidence = experiment / "summary" / "relation-candidates.csv"
    self_evidence = experiment / "summary" / "self-candidates.csv"
    rows = pd.read_csv(pair_evidence) if pair_evidence.is_file() else pd.DataFrame()
    self_rows = pd.read_csv(self_evidence) if self_evidence.is_file() else pd.DataFrame()
    if rows.empty and self_rows.empty:
        raise ValueError("pair and self relationship candidate tables are empty or missing")
    evidence = [path for path in (pair_evidence, self_evidence) if path.is_file()]
    topology = _topology(experiment)
    pressures = _node_pressure(experiment, topology)
    target_node = min(pressures, key=lambda node: (pressures[node], node)) if pressures else min(topology)
    selection_reason = (
        f"lowest observed node CPU pressure ({pressures[target_node]:.6f})"
        if pressures else "lowest NUMA node ID; background-node pressure was unavailable"
    )
    capacity = len(_expand_cpu_list(topology[target_node])) * 0.8
    phase_names = sorted(
        set(rows["phase"].astype(str) if not rows.empty else [])
        | set(self_rows["phase"].astype(str) if not self_rows.empty else [])
    )
    scopes = [
        (
            phase,
            rows[rows["phase"].astype(str).eq(phase)].copy() if not rows.empty else pd.DataFrame(),
            self_rows[self_rows["phase"].astype(str).eq(phase)].copy()
            if not self_rows.empty else pd.DataFrame(),
        )
        for phase in phase_names
    ]
    robust = _robust(rows, len(phase_names)) if not rows.empty else pd.DataFrame()
    robust_self = (
        _robust_self(self_rows, len(phase_names)) if not self_rows.empty else pd.DataFrame()
    )
    if not robust.empty or not robust_self.empty:
        scopes.append(("robust", robust, robust_self))
    policies = []
    for scope, values, self_values in scopes:
        communities = _singletons(self_values, capacity, top_k)
        if not values.empty:
            communities.extend(_communities(values, self_values, capacity, top_k))
        communities = sorted(
            communities,
            key=lambda row: row["community_static_score"],
            reverse=True,
        )[:top_k]
        for community in communities:
            for mode in ("limited", "unlimited"):
                policies.append(_policy(
                    scope, mode, community, topology, target_node, evidence, selection_reason
                ))
    if not policies:
        raise ValueError("no relationship community fits the configured NUMA capacity")
    policies.sort(
        key=lambda row: (
            row["scope"] == "robust",
            row["relationship_evidence"]["community_static_score"],
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
        f"- Action: `{policy['action_type']}`\n"
        f"- Groups: {groups}\n"
        f"- Target NUMA node: `{policy['target_nodes'][0]}`\n"
        f"- CPU demand: `{policy['cpu_demand']:.4f}` / `{policy['capacity_limit']:.4f}`\n"
        f"- Self score sum: `{evidence['self_score_sum']:.4f}`\n"
        f"- Pair score sum: `{evidence['pair_score_sum']:.4f}`\n"
        f"- Static candidate score: `{evidence['community_static_score']:.4f}`\n\n"
        "This output is candidate-only. G has not been calibrated, expected throughput gain is unknown, "
        "and the generated YBA profile remains disabled by default.\n"
    )
