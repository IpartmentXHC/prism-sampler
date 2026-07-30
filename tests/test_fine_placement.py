from __future__ import annotations

import json
from pathlib import Path

from prism_sampler.controller.fine_placement import (
    FinePlacementShadow,
    build_fine_placement,
    validate_fine_placement_log,
)


def snapshot(sequence: int = 3) -> dict[str, object]:
    return {
        "schema": "prism-sampler.live-candidates.v1",
        "sequence_start": 1,
        "sequence_end": sequence,
        "window_end_epoch_ms": 1000,
        "quality": {"confidence": 1.0, "stream_completeness": 1.0},
        "pair_candidates": [
            {
                "group_a": "A",
                "group_b": "B",
                "relationship_score_r": 80,
                "confidence": 1.0,
                "active_cpus_a": 4,
                "active_cpus_b": 4,
            },
            {
                "group_a": "B",
                "group_b": "C",
                "relationship_score_r": 5,
                "confidence": 1.0,
                "active_cpus_a": 4,
                "active_cpus_b": 4,
            },
        ],
        "self_candidates": [
            {
                "group_name": "A",
                "self_score_r": 30,
                "confidence": 1.0,
                "active_cpus": 4,
            },
            {
                "group_name": "C",
                "self_score_r": 20,
                "confidence": 1.0,
                "active_cpus": 4,
            },
        ],
    }


def test_builds_capacity_checked_balanced_shadow_partition() -> None:
    result = build_fine_placement(
        snapshot(),
        phase="opaque",
        scaling_state="two_node",
        available_nodes=(0, 1),
        node_cpus={0: set(range(8)), 1: set(range(8, 16))},
        pair_threshold=10,
        self_threshold=10,
        minimum_confidence=0.7,
        cluster_size=4,
    )

    assert result["status"] == "candidate"
    assert result["apply_allowed"] is False
    assert result["assignment"]["A"] == result["assignment"]["B"]
    assert result["assignment"]["C"] != result["assignment"]["A"]
    assert result["cut_relationship_score"] == 0
    assert result["capacity_feasible"] is True
    assert all(result["cluster_cpus"][group] for group in ("A", "B", "C"))


def test_rejects_low_confidence_graph() -> None:
    value = snapshot()
    value["quality"]["confidence"] = 0.5
    result = build_fine_placement(
        value,
        phase="opaque",
        scaling_state="one_node",
        available_nodes=(0,),
        node_cpus={0: set(range(8))},
        pair_threshold=10,
        self_threshold=10,
        minimum_confidence=0.7,
        cluster_size=4,
    )
    assert result["status"] == "insufficient_confidence"
    assert result["apply_allowed"] is False


def test_poll_deduplicates_sequence_and_state(tmp_path: Path) -> None:
    path = tmp_path / "latest.json"
    path.write_text(json.dumps(snapshot()), encoding="utf-8")
    shadow = FinePlacementShadow()
    arguments = {
        "phase": "opaque",
        "scaling_state": "one_node",
        "available_nodes": (0,),
        "node_cpus": {0: set(range(16))},
        "pair_threshold": 10,
        "self_threshold": 10,
        "minimum_confidence": 0.7,
        "cluster_size": 4,
    }
    assert shadow.poll(path, **arguments) is not None
    assert shadow.poll(path, **arguments) is None
    arguments["scaling_state"] = "two_node"
    arguments["available_nodes"] = (0, 1)
    arguments["node_cpus"] = {0: set(range(16)), 1: set(range(16, 32))}
    assert shadow.poll(path, **arguments) is not None


def test_capacity_forces_oversized_group_to_split_across_nodes() -> None:
    value = snapshot()
    value["pair_candidates"] = []
    value["self_candidates"] = [{
        "group_name": "A",
        "self_score_r": 80,
        "confidence": 1.0,
        "active_cpus": 12,
    }]
    result = build_fine_placement(
        value,
        phase="high",
        scaling_state="two_node",
        available_nodes=(0, 1),
        node_cpus={0: set(range(8)), 1: set(range(8, 16))},
        pair_threshold=10,
        self_threshold=10,
        minimum_confidence=0.7,
        cluster_size=4,
    )
    assert result["status"] == "candidate"
    assert result["capacity_forced_split_groups"] == ["A"]
    assert set(result["node_shards_cpu_equiv"]["A"]) == {"0", "1"}
    assert result["capacity_feasible"] is True


def test_validates_mature_shadow_log(tmp_path: Path) -> None:
    first = build_fine_placement(
        snapshot(3),
        phase="high",
        scaling_state="two_node",
        available_nodes=(0, 1),
        node_cpus={0: set(range(8)), 1: set(range(8, 16))},
        pair_threshold=10,
        self_threshold=10,
        minimum_confidence=0.7,
        cluster_size=4,
    )
    second = {**first, "source_sequence_end": 4}
    path = tmp_path / "fine.jsonl"
    path.write_text(
        json.dumps(first) + "\n" + json.dumps(second) + "\n",
        encoding="utf-8",
    )
    report = validate_fine_placement_log(path, tmp_path / "validation.json")
    assert report["passed"] is True
    assert report["candidate_windows"] == 2
