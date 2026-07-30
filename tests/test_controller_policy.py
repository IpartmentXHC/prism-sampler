from __future__ import annotations

from dataclasses import replace

import pytest

from prism_sampler.controller.config import ControllerConfig
from prism_sampler.controller.models import MetricSample
from prism_sampler.controller.dynamic_model import GScaleModel
from prism_sampler.controller.policy import (
    BenefitPolicy,
    ContinuousBenefitPolicy,
    PressurePolicy,
    ScalingState,
)


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


def dynamic_model() -> GScaleModel:
    return GScaleModel({
        "schema": "prism-sampler.pressure-g-scale.v1",
        "pressure_reference": {
            "reference_capacity_cpus": 32,
            "coefficients": {"run_coefficient": 1.1, "rq_coefficient": 0.6},
            "validation": {"leave_one_load_out_p95_absolute_error": 0.4},
        },
        "g_scale": {
            "feature_scales": {
                "pressure_ref": 0.2,
                "log_throughput": 0.2,
                "log_p99": 0.5,
            }
        },
        "anchors": [
            {
                "label": "low",
                "pressure_ref": 0.1,
                "one_throughput_ops_s": 100.0,
                "two_throughput_ops_s": 80.0,
                "one_p99_us": 10.0,
                "two_p99_us": 12.0,
            },
            {
                "label": "high",
                "pressure_ref": 1.5,
                "one_throughput_ops_s": 100.0,
                "two_throughput_ops_s": 180.0,
                "one_p99_us": 100.0,
                "two_p99_us": 50.0,
            },
        ],
    })


def dynamic_sample(
    index: int,
    *,
    run: float,
    rq: float,
    throughput: float,
    p99: float,
    phase: str = "opaque",
) -> MetricSample:
    value = sample(index, run=run, rq=rq)
    value.kpi.update({
        "phase": phase,
        "sequence": index,
        "complete": True,
        "throughput_ops_s": throughput,
        "max_client_p99_latency_us": p99,
        "offered_threads": 999,
    })
    return value


def test_continuous_policy_uses_p_ref_without_profile_identity() -> None:
    policy = ContinuousBenefitPolicy(
        ControllerConfig(
            cooldown_seconds=0,
            gain_uncertainty_multiplier=0,
        ),
        dynamic_model(),
    )
    decision = None
    for index in range(1, 4):
        decision = policy.evaluate(dynamic_sample(
            index, run=31, rq=17, throughput=100, p99=100,
            phase="does-not-identify-the-load",
        ))
    assert decision is not None and decision.action == "expand"
    assert decision.decision_source == "continuous_g_scale"
    assert decision.nearest_anchor == "high"
    assert decision.signature_threads is None


def test_continuous_policy_maps_two_node_pressure_and_shrinks_low_load() -> None:
    policy = ContinuousBenefitPolicy(
        ControllerConfig(
            cooldown_seconds=0,
            minimum_two_node_dwell_seconds=0,
            gain_uncertainty_multiplier=0,
        ),
        dynamic_model(),
    )
    policy.force_state(ScalingState.TWO_NODE, 0)
    decision = None
    for index in range(1, 4):
        decision = policy.evaluate(dynamic_sample(
            index, run=3, rq=0, throughput=80, p99=12
        ))
    assert decision is not None and decision.action == "shrink"
    assert decision.pressure_ref == pytest.approx(1.1 * 3 / 32)


def test_continuous_policy_retries_profitable_shrink_after_minimum_dwell() -> None:
    policy = ContinuousBenefitPolicy(
        ControllerConfig(
            cooldown_seconds=0,
            minimum_two_node_dwell_seconds=60,
            gain_uncertainty_multiplier=0,
        ),
        dynamic_model(),
    )
    policy.force_state(ScalingState.TWO_NODE, 0)

    decisions = [
        policy.evaluate(dynamic_sample(
            index, run=3, rq=0, throughput=80, p99=12
        ))
        for index in range(1, 7)
    ]

    assert decisions[2].reason == "minimum_two_node_dwell"
    assert decisions[5].action == "shrink"
    assert policy.state == ScalingState.ONE_NODE


def test_continuous_policy_initial_two_node_has_no_artificial_dwell() -> None:
    policy = ContinuousBenefitPolicy(
        ControllerConfig(
            cooldown_seconds=0,
            minimum_two_node_dwell_seconds=300,
            gain_uncertainty_multiplier=0,
        ),
        dynamic_model(),
    )
    policy.force_state(ScalingState.TWO_NODE)

    decision = None
    for index in range(1, 4):
        decision = policy.evaluate(dynamic_sample(
            index, run=3, rq=0, throughput=80, p99=12
        ))

    assert decision is not None and decision.action == "shrink"


def test_continuous_policy_retries_positive_gain_after_confidence_gate() -> None:
    config = ControllerConfig(
        cooldown_seconds=0,
        minimum_expected_gain_pct=100,
        gain_uncertainty_multiplier=0,
    )
    policy = ContinuousBenefitPolicy(config, dynamic_model())
    decision = None
    for index in range(1, 4):
        decision = policy.evaluate(dynamic_sample(
            index, run=25, rq=10, throughput=100, p99=100
        ))
    assert decision is not None
    assert decision.reason == "gain_lower_bound_below_gate"
    assert policy.pending_rebase

    policy.config = replace(config, minimum_expected_gain_pct=0)
    decision = policy.evaluate(dynamic_sample(
        4, run=25, rq=10, throughput=100, p99=100
    ))
    assert decision.action == "expand"


def test_continuous_policy_holds_until_sustained_pressure_rebase() -> None:
    policy = ContinuousBenefitPolicy(
        ControllerConfig(
            cooldown_seconds=0,
            pressure_change_confirm_samples=3,
            gain_uncertainty_multiplier=0,
        ),
        dynamic_model(),
    )
    for index in range(1, 4):
        policy.evaluate(dynamic_sample(
            index, run=3, rq=0, throughput=100, p99=10
        ))
    assert policy.pressure_baseline == pytest.approx(3 / 32)
    decisions = []
    for index in range(4, 8):
        decisions.append(policy.evaluate(dynamic_sample(
            index, run=25, rq=15, throughput=100, p99=100
        )))
    assert all(decision.action is None for decision in decisions[:3])
    assert decisions[3].action == "expand"
