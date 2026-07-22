# Candidate Policy Schema

The canonical output is `policy/selected-policy.json`. It contains topology,
thread matching rules, CPU demand, capacity limits, R evidence, placement mode,
fallback, and evidence checksums.

Candidate actions are either `singleton_colocation`, driven by `R_self` for one
multi-threaded logical group, or `multi_group_colocation`, driven by both member
`R_self` node weights and `R_pair` edges. The static community score retains
self and pair components separately; it is a candidate rank, not a throughput
prediction.

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

`G(action)` will eventually estimate the net throughput gain of executing one
candidate. It must use action-induced locality changes, such as
`R_self * delta(intra-group locality)` and `R_pair * delta(inter-group locality)`,
rather than summing relationship scores that the current placement already
realizes. CPU, runqueue, LLC, bandwidth, NUMA, capacity, migration, and placement
mode supply the dynamic benefit and penalty terms. Adding `R_self` therefore
extends G's evidence vector but does not change G's role or justify a numeric
estimate.

The generated YBA environment is disabled by default. `policy render-yba
--enable` is an explicit experiment action, not an online scheduling decision.
