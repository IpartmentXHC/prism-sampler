from __future__ import annotations

import json
import math
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

from .relations.analyzer import _normalize


PROFILE_PAIRS = {
    "self_limited": ("self_compact_limited", "self_spread_limited", "self"),
    "control_self": ("control_self_compact", "control_self_spread", "self"),
    "pair_limited": ("pair_together_limited", "pair_separate_limited", "pair"),
}


def _load_scales(summary: Path) -> dict[str, float]:
    return {key: float(value) for key, value in json.loads(
        (summary / "relation-scales.json").read_text(encoding="utf-8")
    ).items()}


def _run_scores(summary: Path, scales: dict[str, float]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    pairs = pd.read_csv(summary / "pair-features.csv")
    selves = pd.read_csv(summary / "self-features.csv")
    for _, row in pairs.iterrows():
        activity = _normalize(float(row.activity_raw), scales["pair_activity_log_p95"])
        sync = _normalize(float(row.sync_raw), scales["pair_sync_log_p95"])
        sharing = _normalize(float(row.sharing_raw), scales["pair_sharing_log_p95"])
        sharing *= float(row.active_overlap_ratio)
        stability = float(row.window_coverage)
        score = 100 * activity * stability * (0.7 * sync + 0.3 * sharing)
        rows.append({
            "candidate_type": "pair", "profile": row.profile, "load": row.phase,
            "round": int(row["round"]), "group_a": row.group_a,
            "group_b": row.group_b, "activity": activity,
            "synchronization": sync, "sharing": sharing,
            "stability": stability, "score_r": score,
        })
    for _, row in selves.iterrows():
        activity = _normalize(float(row.activity_raw), scales["self_activity_log_p95"])
        sync = _normalize(float(row.sync_raw), scales["self_sync_log_p95"])
        sharing = _normalize(float(row.sharing_raw), scales["self_sharing_log_p95"])
        stability = float(row.window_coverage)
        score = 100 * activity * stability * (0.7 * sync + 0.3 * sharing)
        rows.append({
            "candidate_type": "self", "profile": row.profile, "load": row.phase,
            "round": int(row["round"]), "group_a": row.group_name,
            "group_b": "", "activity": activity,
            "synchronization": sync, "sharing": sharing,
            "stability": stability, "score_r": score,
        })
    result = pd.DataFrame(rows)
    if not result.empty:
        result["rank"] = result.groupby(
            ["candidate_type", "profile", "load", "round"]
        )["score_r"].rank(method="min", ascending=False)
    return result


def _gate_a(scores: pd.DataFrame, baseline: str) -> dict[str, Any]:
    source = scores[scores.profile == baseline]
    checks = []
    for (kind, load), rows in source.groupby(["candidate_type", "load"]):
        top_sets = [set(zip(part.group_a, part.group_b)) for _, part in
                    rows[rows["rank"] <= 3].groupby("round")]
        jaccards = [len(a & b) / len(a | b) if a | b else 1.0
                    for a, b in combinations(top_sets, 2)]
        top = rows[rows["rank"] <= 3]
        means = top.groupby(["group_a", "group_b"], dropna=False).score_r.mean()
        stds = top.groupby(["group_a", "group_b"], dropna=False).score_r.std(ddof=1)
        cvs = (stds / means.replace(0, math.nan)).dropna()
        checks.append({
            "candidate_type": kind, "load": load,
            "rounds": int(rows["round"].nunique()),
            "top3_mean_jaccard": float(pd.Series(jaccards).mean()) if jaccards else None,
            "top3_max_score_cv": float(cvs.max()) if len(cvs) else None,
        })
    enough = bool(checks) and all(row["rounds"] >= 5 for row in checks)
    stable = enough and all(
        (row["top3_mean_jaccard"] or 0) >= 0.5 and
        (row["top3_max_score_cv"] is None or row["top3_max_score_cv"] <= 0.5)
        for row in checks
    )
    return {"passed": stable, "baseline_profile": baseline, "checks": checks,
            "criteria": {"rounds": 5, "top3_mean_jaccard_min": 0.5,
                         "top3_max_score_cv_max": 0.5}}


def _suite_kpi(experiment: Path) -> pd.DataFrame:
    path = experiment / "yba-suite" / "suite-summary.csv"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path).rename(columns={"load": "load"})
    return frame[frame.status == "ok"].copy() if "status" in frame else frame


def _paired_effects(kpi: pd.DataFrame, baseline: str) -> pd.DataFrame:
    output = []
    pairs = {**PROFILE_PAIRS}
    for profile in sorted(set(kpi.profile) - {baseline}):
        pairs[f"gain_{profile}"] = (profile, baseline, "g")
    for contrast, (action, reference, label_type) in pairs.items():
        left = kpi[kpi.profile == action]
        right = kpi[kpi.profile == reference]
        merged = left.merge(right, on=["load", "round"], suffixes=("_action", "_reference"))
        for _, row in merged.iterrows():
            output.append({
                "contrast": contrast, "label_type": label_type,
                "load": row["load"], "round": int(row["round"]),
                "action_profile": action, "reference_profile": reference,
                "action_throughput": row.throughput_action,
                "reference_throughput": row.throughput_reference,
                "effect_percent": 100 * (row.throughput_action / row.throughput_reference - 1),
                "action_p99_latency": row.get("p99_latency_action"),
                "reference_p99_latency": row.get("p99_latency_reference"),
            })
    return pd.DataFrame(output)


def _telemetry(experiment: Path, kpi: pd.DataFrame, baseline: str) -> pd.DataFrame:
    import duckdb

    output = []
    for db in sorted(experiment.glob("runs/*/*/r*/dataset/telemetry.db3")):
        run = db.parents[1]
        profile, load, round_name = run.parts[-3], run.parts[-2], run.parts[-1]
        round_number = int(round_name.removeprefix("r"))
        match = kpi[(kpi.profile == profile) & (kpi["load"] == load) &
                    (kpi["round"] == round_number)]
        row: dict[str, Any] = {"profile": profile, "load": load, "round": round_number}
        if not match.empty:
            for name in ("throughput", "p99_latency", "error_count", "timeout_count"):
                row[name] = match.iloc[0].get(name)
        con = duckdb.connect(str(db), read_only=True)
        tables = {value[0] for value in con.execute("show tables").fetchall()}
        phase = json.loads((run / "meta" / "phase.json").read_text(encoding="utf-8"))
        start = float(phase["workload_start_epoch_ns"]) / 1e9
        end = float(phase["workload_end_epoch_ns"]) / 1e9
        if "pmu_derived" in tables:
            for metric, value in con.execute(
                "select metric,avg(value) from pmu_derived "
                "where epoch(ts)>=? and epoch(ts)<? group by metric", [start, end]
            ).fetchall():
                row[f"pmu_{metric}"] = value
        if "numa_samples" in tables:
            values = con.execute(
                "select node,avg(value) from numa_samples where metric='resident' "
                "and epoch(ts)>=? and epoch(ts)<? group by node", [start, end]
            ).fetchall()
            total = sum(float(value or 0) for _, value in values)
            row["numa_dominant_resident_ratio"] = (
                max((float(value or 0) for _, value in values), default=0) / total if total else None
            )
        if "taskstats_view" in tables:
            value = con.execute(
                "select avg(run_share),avg(rq_share) from taskstats_view "
                "where epoch(ts)>=? and epoch(ts)<?", [start, end]
            ).fetchone()
            row["task_run_share_mean"], row["task_runqueue_share_mean"] = value
        con.close()
        output.append(row)
    result = pd.DataFrame(output)
    if result.empty:
        return result
    baseline_rows = result[result.profile == baseline][["load", "round", "throughput"]].rename(
        columns={"throughput": "baseline_throughput"}
    )
    result = result.merge(baseline_rows, on=["load", "round"], how="left")
    result["observed_gain_percent"] = 100 * (
        result.throughput / result.baseline_throughput - 1
    )
    return result


def calibrate_experiment(experiment: Path, *, baseline: str = "one_node") -> dict[str, Any]:
    summary = experiment / "summary"
    summary.mkdir(parents=True, exist_ok=True)
    scales = _load_scales(summary)
    scores = _run_scores(summary, scales)
    scores.to_csv(summary / "r-run-scores.csv", index=False)
    gate = _gate_a(scores, baseline)
    pd.DataFrame(gate["checks"]).to_csv(summary / "gate-a-checks.csv", index=False)
    proposal = {"schema": "prism-sampler.scale-proposal.v1", "current": scales,
                "gate_a": gate, "apply_allowed": False}
    (summary / "scale-proposal.json").write_text(
        json.dumps(proposal, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    kpi = _suite_kpi(experiment)
    if not kpi.empty:
        variability = kpi.groupby("load").throughput.agg(
            rounds="count", mean="mean", variance="var", stddev="std"
        ).reset_index()
        variability["cv"] = variability.stddev / variability["mean"]
    else:
        variability = pd.DataFrame()
    variability.to_csv(summary / "kpi-variability.csv", index=False)
    effects = _paired_effects(kpi, baseline) if not kpi.empty else pd.DataFrame()
    effects.to_csv(summary / "paired-effects.csv", index=False)
    g_data = _telemetry(experiment, kpi, baseline) if not kpi.empty else pd.DataFrame()
    g_data.to_csv(summary / "g-dataset.csv", index=False)
    model = {
        "schema": "prism-sampler.g-model-proposal.v1", "status": "proposal_only",
        "expected_gain": None, "apply_allowed": False,
        "label": "observed_gain_percent",
        "features": [name for name in g_data.columns if name.startswith(("pmu_", "numa_", "task_"))],
        "reason": "Matched causal cells and independent validation are required before fitting G.",
    }
    (summary / "g-model-proposal.json").write_text(
        json.dumps(model, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (summary / "weight-threshold-proposal.md").write_text(
        "# R Weight And Threshold Proposal\n\n"
        f"Gate A passed: **{gate['passed']}**. Keep the production 0.7/0.3 weights and "
        "thresholds frozen until Gate B paired effects are complete.\n",
        encoding="utf-8",
    )
    failed = [row for row in gate["checks"] if
              (row["top3_mean_jaccard"] or 0) < 0.5 or
              (row["top3_max_score_cv"] is not None and row["top3_max_score_cv"] > 0.5)]
    failed_text = ", ".join(
        f"{row['candidate_type']}/{row['load']} (CV={row['top3_max_score_cv']:.3f})"
        for row in failed
    ) or "none"
    (summary / "formula-review.md").write_text(
        "# Formula Review\n\n"
        f"- Gate A stability: **{'PASS' if gate['passed'] else 'NOT PASSED'}**\n"
        f"- Run-level R rows: {len(scores)}\n"
        f"- Matched effect rows: {len(effects)}\n"
        f"- G telemetry rows: {len(g_data)}\n"
        f"- Failed Gate A checks: {failed_text}\n"
        "- A stable Top-3 with unstable near-zero scores must be treated as weak-signal "
        "evidence, not as a server-throughput regression.\n"
        "- No formula, threshold, or policy was applied automatically.\n",
        encoding="utf-8",
    )
    return {"gate_a_passed": gate["passed"], "r_rows": len(scores),
            "paired_effect_rows": len(effects), "g_rows": len(g_data),
            "summary": str(summary)}
