from prism_sampler.blackbox_runner import _schedule


def test_stage_b_schedule_is_balanced_randomized_and_reproducible():
    first = _schedule(7)
    second = _schedule(7)
    assert first == second
    assert len(first) == 22
    assert [row["load"] for row in first] != sorted(row["load"] for row in first)
    counts = {}
    for row in first:
        values = counts.setdefault(row["load"], {"expand": 0, "shrink": 0})
        values["expand"] += 1
        values["shrink"] += 1
    assert counts == {
        "C1T1": {"expand": 10, "shrink": 10},
        "C4T6": {"expand": 6, "shrink": 6},
        "C5T16": {"expand": 6, "shrink": 6},
    }
