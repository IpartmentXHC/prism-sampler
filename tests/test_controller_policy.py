from __future__ import annotations

from prism_sampler.controller.config import ControllerConfig
from prism_sampler.controller.models import MetricSample
from prism_sampler.controller.policy import BenefitPolicy, PressurePolicy, ScalingState


def sample(
    index: int,
    *,
    run: float,
    rq: float,
    active: bool = True,
    valid: bool = True,
) -> MetricSample:
    return MetricSample(
        realtime_ns=index * 10_000_000_000,
        monotonic_ns=index * 10_000_000_000,
        interval_seconds=10,
        workload_active=active,
        valid=valid,
        run_cpu_equiv=run if valid else None,
        rq_cpu_equiv=rq if valid else None,
        run_pressure=run / 32 if valid else None,
        rq_pressure=rq / 32 if valid else None,
        tids_observed=10,
    )


def test_expand_requires_three_consecutive_high_samples() -> None:
    policy = PressurePolicy(ControllerConfig())
    assert policy.evaluate(sample(1, run=31, rq=20)).action is None
    assert policy.evaluate(sample(2, run=31, rq=20)).action is None
    decision = policy.evaluate(sample(3, run=31, rq=20))
    assert decision.action == "expand"
    assert policy.state == ScalingState.TWO_NODE


def test_inactive_and_invalid_samples_reset_confirmation() -> None:
    policy = PressurePolicy(ControllerConfig())
    policy.evaluate(sample(1, run=31, rq=20))
    policy.evaluate(sample(2, run=31, rq=20, active=False))
    policy.evaluate(sample(3, run=31, rq=20))
    assert policy.evaluate(sample(4, run=31, rq=20)).action is None
    policy.evaluate(sample(5, run=31, rq=20, valid=False))
    assert policy.expand_matches == 0


def test_shrink_honors_low_pressure_duration_and_dwell() -> None:
    config = ControllerConfig(
        shrink_confirm_seconds=20,
        minimum_two_node_dwell_seconds=30,
        cooldown_seconds=0,
    )
    policy = PressurePolicy(config)
    policy.force_state(ScalingState.TWO_NODE, 0)
    assert policy.evaluate(sample(1, run=20, rq=1)).action is None
    assert policy.evaluate(sample(2, run=20, rq=1)).action is None
    decision = policy.evaluate(sample(3, run=20, rq=1))
    assert decision.action == "shrink"
    assert policy.state == ScalingState.ONE_NODE


def test_recent_shrink_rolls_back_after_two_high_samples() -> None:
    config = ControllerConfig(cooldown_seconds=120, rollback_confirm_samples=2)
    policy = PressurePolicy(config)
    policy.last_action = "shrink"
    policy.last_transition_ns = 0
    policy.evaluate(sample(1, run=31, rq=20))
    decision = policy.evaluate(sample(2, run=31, rq=20))
    assert decision.action == "rollback_expand"


def test_benefit_policy_uses_three_complete_kpi_windows() -> None:
    policy = BenefitPolicy(
        ControllerConfig(cooldown_seconds=0),
        [{
            "offered_threads": 80,
            "one_throughput_ops_s": 100,
            "two_throughput_ops_s": 180,
        }],
    )
    decisions = []
    for index in range(1, 4):
        value = sample(index, run=20, rq=1)
        value.kpi.update({
            "phase": "C5T16", "sequence": index, "complete": True,
            "offered_threads": 80, "throughput_ops_s": 100,
        })
        decisions.append(policy.evaluate(value))
    assert decisions[-1].action == "expand"
    assert decisions[-1].decision_source == "benefit"
    assert decisions[-1].expected_gain_pct == 80.0


def test_benefit_policy_holds_when_gain_is_below_gate() -> None:
    policy = BenefitPolicy(
        ControllerConfig(cooldown_seconds=0, minimum_expected_gain_pct=2),
        [{
            "offered_threads": 4,
            "one_throughput_ops_s": 100,
            "two_throughput_ops_s": 101,
        }],
    )
    decision = None
    for index in range(1, 4):
        value = sample(index, run=10, rq=0)
        value.kpi.update({
            "phase": "C2T2", "sequence": index, "complete": True,
            "offered_threads": 4,
        })
        decision = policy.evaluate(value)
    assert decision is not None and decision.action is None
    assert decision.reason == "expected_gain_below_gate"


def test_benefit_policy_matches_calibrated_pressure_before_offered_threads() -> None:
    policy = BenefitPolicy(
        ControllerConfig(cooldown_seconds=0),
        [
            {
                "offered_threads": 4,
                "one_throughput_ops_s": 100,
                "two_throughput_ops_s": 90,
                "one_run_cpu_equiv": 8,
                "one_rq_cpu_equiv": 1,
            },
            {
                "offered_threads": 80,
                "one_throughput_ops_s": 100,
                "two_throughput_ops_s": 180,
                "one_run_cpu_equiv": 28,
                "one_rq_cpu_equiv": 10,
            },
        ],
    )
    decision = None
    for index in range(1, 4):
        value = sample(index, run=28, rq=10)
        value.kpi.update({
            "phase": "opaque", "sequence": index, "complete": True,
            "offered_threads": 999,
        })
        decision = policy.evaluate(value)
    assert decision is not None and decision.action == "expand"
    assert decision.signature_threads == 80
    assert decision.signature_distance == 0


def test_benefit_policy_uses_three_window_pressure_median() -> None:
    policy = BenefitPolicy(
        ControllerConfig(cooldown_seconds=0),
        [{
            "offered_threads": 80,
            "one_throughput_ops_s": 100,
            "two_throughput_ops_s": 180,
            "one_run_cpu_equiv": 28,
            "one_rq_cpu_equiv": 10,
        }],
    )
    decision = None
    for index, (run, rq) in enumerate(((28, 10), (28, 10), (5, 0)), 1):
        value = sample(index, run=run, rq=rq)
        value.kpi.update({
            "phase": "opaque", "sequence": index, "complete": True,
            "offered_threads": 999,
        })
        decision = policy.evaluate(value)
    assert decision is not None and decision.action == "expand"
    assert decision.signature_distance == 0


def test_benefit_policy_rejects_pressure_outside_calibrated_distribution() -> None:
    policy = BenefitPolicy(
        ControllerConfig(cooldown_seconds=0, maximum_signature_distance=0.2),
        [{
            "offered_threads": 4,
            "one_throughput_ops_s": 100,
            "two_throughput_ops_s": 90,
            "one_run_cpu_equiv": 8,
            "one_rq_cpu_equiv": 1,
        }],
    )
    decision = None
    for index in range(1, 4):
        value = sample(index, run=15, rq=3)
        value.kpi.update({
            "phase": "C2T2", "sequence": index, "complete": True,
            "offered_threads": 4,
        })
        decision = policy.evaluate(value)
    assert decision is not None and decision.action is None
    assert decision.reason == "outside_calibrated_signatures"
    assert decision.decision_source == "pressure_safety"


def test_scripted_transitions_are_validated() -> None:
    ControllerConfig(
        initial_state="two_node",
        scripted_transitions=("90:one_node", "210:two_node"),
    ).validate()
