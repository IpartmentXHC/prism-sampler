# Pure System Black-Box Scheduling

## Online Contract

The black-box controller predicts the net benefit of a resource action from
system evidence only:

```text
eBPF/Prism + /proc + core/uncore PMU + NUMA + R
                         |
                    10 s windows
                         |
                     G_scale
                         |
             node mask + ClickHouse slots
```

YBA throughput, P99, errors, clients, threads, phase labels, and offered load
are not model features. YBA supplies offline action labels and final acceptance
results only. In black-box mode the controller also ignores YBA's workload
active marker and runs continuously while the database process identity is
valid.

`G_scale` is a separate-direction robust-scaled ridge model. Expansion and
shrinkage have separate coefficients. Each prediction records expected gain,
feature contributions, feature coverage, distance, and confidence. Features
with less than 50% coverage in training are excluded rather than imputed into
an unsupported online signal. A 2% equivalence band separates measurable
positive/negative actions from experimental noise.

The deployment objective is different from that noise band. Offline resource
selection chooses the smallest state whose throughput is at least 90% of the
best measured state. Consequently ONE remains acceptable while
`throughput(ONE) / throughput(TWO) >= 0.90`; expansion requires TWO to have a
repeatable advantage greater than `1 / 0.90 - 1 = 11.11%`. The online model
predicts this capacity advantage from `P_ref + B_ctx`; it does not observe the
throughput ratio in production.

## System Features

- `/proc`: running CPU equivalents, runqueue CPU equivalents, node utilization,
  CPU PSI, affinity, and target identity.
- Core PMU: IPC, cache refill, frontend/backend stalls, memory access, and
  remote access evidence.
- HiSilicon uncore: DDRC read/write and cross-SCCL traffic rates.
- NUMA: local-page ratio and page-distribution entropy.
- Prism live R: graph confidence plus aggregate `R_pair` and `R_self` strength.

No single feature is interpreted as throughput. The model estimates the
throughput effect of an action because those system windows were paired with
post-action YBA labels during offline calibration.

## Fine Placement

Node scaling and thread placement remain separate decisions:

1. `G_scale` selects ONE or TWO nodes and the calibrated global query slots.
2. The placement shadow builds a capacity-constrained partition from live R.
3. Candidate ranking uses `median(R) * window_presence_ratio`; thread-group
   names always come from Prism snapshots.
4. `G_place` is calibrated with matched actions: `R_self` compact versus split,
   and `R_pair` colocated versus separated, while node count, CPU count, slots,
   `max_threads`, and memory policy remain fixed.

Fine placement remains shadow-only until the matched action experiments show
repeatable benefit. Linux 6.6 execution uses `taskset`/`sched_setaffinity`; the
project does not depend on `sched_ext`.

## Gates

- Stage A replay: at least 90% valid feature coverage and 80% leave-one-load-out
  action-benefit direction accuracy, without application KPI features. The
  decision class is `predicted gain > 2%`; negative and equivalent gains both
  map to the controller's `hold` action while retaining their signed values for
  explanation.
- Stage B crossover: at least 60 valid actions; each pressure group has at least
  10 expansions and 10 shrinkages, with 3 pre-action windows, 20 seconds of
  settling, and 3 post-action label windows.
- Stage C placement: 30 matched effects across C2T2, C4T6, and C5T16, five
  randomized rounds per contrast.
- Stage D shadow: at least 30 model decision windows, confidence and feature
  coverage at least 0.8, three-window confirmation, no capacity overflow, no
  repeated recommendation within 60 seconds, and zero online KPI rows.
- Stage D active: runs only after all shadow gates pass. Final YBA throughput
  must reach the configured offline acceptance target. Resource-curve v1 uses
  at least 90% of static maximum throughput while minimizing node count; a
  stricter 98% oracle score may still be reported as a secondary maximum-
  throughput metric. Neither value is an online signal.

## Versioned Resource Curve

`prism-sampler resource-curve build` turns matched ONE/TWO anchors, the P_ref
mapping, and optional G evidence into a versioned calibration bundle. A bundle
is `active_eligible` only when every anchor has at least three matched rounds,
the pressure mapping passes leave-one-load-out MAE/P95 gates, and robust curve
residual exclusions stay below 10%. Legacy ONE configurations and under-
repeated cells remain in the audit files but cannot activate the policy.

Residual disagreement is not permission to delete a point. A suspect cell is
retested with two matched rounds; a repeated deviation becomes a `B_ctx`
branch or an uncertain hold region. `retest-proposal.csv` records the exact
missing measurements.

The controller cannot claim to observe throughput in production. Its claim is
limited to predicting action benefit from system pressure and relationship
evidence learned from offline supervised experiments.
