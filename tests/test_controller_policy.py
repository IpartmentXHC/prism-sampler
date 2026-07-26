from __future__ import annotations

from prism_sampler.controller.config import ControllerConfig
from prism_sampler.controller.models import MetricSample
from prism_sampler.controller.policy import PressurePolicy, ScalingState


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
