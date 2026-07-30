import json

from prism_sampler.place_calibration import render_place_suite, select_r_candidates


def test_candidates_and_rules_come_from_live_r_not_hardcoded_names(tmp_path):
    experiments = []
    for load in ("C2T2", "C4T6", "C5T16"):
        experiment = tmp_path / load
        raw = experiment / "runs" / load / "r1" / "raw"
        raw.mkdir(parents=True)
        snapshots = []
        for index in range(3):
            snapshots.append({
                "quality": {"confidence": 1.0},
                "self_candidates": [{
                    "group_name": f"worker-{load}", "self_score_r": 20 + index,
                    "active_cpus": 2, "confidence": 1.0,
                }],
                "pair_candidates": ([{
                    "group_a": f"noise-a-{load}", "group_b": f"noise-b-{load}",
                    "relationship_score_r": 0.001, "active_cpus_a": 1,
                    "active_cpus_b": 1, "confidence": 1.0,
                }] + ([{
                    "group_a": f"reader-{load}", "group_b": f"writer-{load}",
                    "relationship_score_r": 30 + index, "active_cpus_a": 2,
                    "active_cpus_b": 3, "confidence": 1.0,
                }] if index else [])),
            })
        (raw / "live-candidates.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in snapshots)
        )
        experiments.append(experiment)
    selected = select_r_candidates(experiments)
    assert selected["C2T2"]["pair"]["robust_score"] > 0
    output = tmp_path / "suite.env"
    render_place_suite("C2T2", selected["C2T2"], tmp_path / "scenario.env", output)
    value = output.read_text()
    assert "worker\\-C2T2" in value
    assert "reader\\-C2T2" in value
    assert "writer\\-C2T2" in value
    assert "ThreadPool" not in value
    assert "0-15,32-47" in value
