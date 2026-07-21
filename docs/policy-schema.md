# Candidate Policy Schema

The canonical output is `policy/selected-policy.json`. It contains topology,
thread matching rules, CPU demand, capacity limits, R evidence, placement mode,
fallback, and evidence checksums.

The first version generates two modes:

- `limited`: the selected community occupies one NUMA node and remaining target
  threads use separate nodes.
- `unlimited`: the selected community is fixed while remaining threads keep the
  system scheduling envelope.

Community demand is `active_cpus + runqueue_cpus` and must remain below 80% of
node CPU capacity. Per-phase candidates use that phase's R graph. Robust edges
use `median(R_phase) * phase_presence_ratio`.

Until G is calibrated, every output has:

```json
{
  "status": "candidate_only",
  "expected_gain": null,
  "apply_allowed": false
}
```

The generated YBA environment is disabled by default. `policy render-yba
--enable` is an explicit experiment action, not an online scheduling decision.

