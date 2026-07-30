# Pressure-Aware NUMA Scaling v2

The controller chooses between two calibrated ClickHouse resource states in one
lifecycle:

```text
ONE = node0,       CPUs 0-31, slots S32
TWO = node0+node1, CPUs 0-63, slots S64
max_threads = M in both states
```

`M`, `S32`, and `S64` come from the 32/64 CPU x max_threads x global-slots
calibration matrix. `max_threads` is fixed online. Affinity and
`concurrent_threads_soft_limit_num` change as one ordered transaction: expansion
widens affinity before increasing slots; shrink reduces slots before narrowing
affinity. A partial failure restores the previous complete state.

Every five seconds the target-local controller reads execution and runqueue wait
deltas from `/proc/PID/task/TID/schedstat`, CPU PSI, NUMA pages, affinity,
ClickHouse QueryThread/GlobalThread metrics, and a YBA KPI window. The KPI stream
still uses non-overlapping ten-second windows and contains throughput,
max-client window P99, errors, timeouts, and completeness.
It is delivered over one persistent SSH channel with `(phase, sequence)` ACK and
deduplication.

Three complete windows are required. Their median run/rq CPU equivalents are
mapped to the ONE reference scale:

```text
P_ref(ONE) = (run + rq) / 32
P_ref(TWO) = a * run / 32 + b * rq / 32
```

`G_scale` is a Gaussian local regression over `P_ref`, log throughput, log P99,
and the current action/state. The uncertainty-adjusted alternative-state gain
must exceed the calibrated 1% gate. A positive but uncertain estimate remains
pending and is re-evaluated when later KPI windows replace transient evidence.
Pressure outside the calibrated distribution holds the current state, except
that sustained
`run_pressure >= 0.90` and `rq_pressure >= 0.50` remains a safety expansion
fallback. YBA inactive gaps reset confirmation.

After an action, 20 seconds are excluded for settling. The following three KPI
windows trigger rollback on throughput loss over 5%, P99 growth over 50%, or any
new error/timeout. Minimum dwell and cooldown are both 60 seconds.

`shadow` records decisions without changing resources. `active` applies
`taskset` to every target task, reloads ClickHouse slots, verifies both results,
and restores original affinity/config on exit. PID start times are checked
before every action. The controller does not use phase names as decisions.

PMU and NUMA placement remain explanatory evidence. The Prism online
`R_pair/R_self` graph now feeds a separate fine-placement shadow layer. It
filters low-confidence edges, solves a balanced capacity-constrained partition
over the currently active nodes, and emits primary node, four-CPU cluster, and
capacity-forced group-shard candidates. Every record sets
`apply_allowed=false`; the R layer cannot call an actuator and does not claim a
throughput gain before action-specific G is calibrated.

The actuator does not mount cpusets, reserve CPUs, power down nodes, migrate
pages, use `sched_ext`, or apply the proposed thread-group/CPU-cluster placement.

Controller artifacts additionally include:

```text
controller/fine-placement.jsonl  online, deduplicated R-graph shadow candidates
```

The one-shot workflow is:

```text
Gate A: 20 screening lifecycles, 90s per load
Gate B: 2 finalist strategies, 2 added rounds, 180s per load
Gate C: 12 scripted crossover probes, 300s each
Gate D: randomized-block ONE/TWO/dynamic, 3 lifecycles each
```

Run the post-Gate-A workflow with:

```bash
prism-sampler pressure-v2 execute-after-gate-a \
  --root /data/threadState/experiments/clickhouse/<v2-root> \
  --gate-a /data/threadState/experiments/clickhouse/<gate-a-root> \
  --base-config /path/to/clickhouse.env \
  --calibration-config /path/to/sampler-gate-b.toml
```

`resume-state.json` makes completed steps and experiments idempotent. Final
artifacts are under `summary/`, including the calibration matrix, selected
configuration, realtime KPI validation, G_v1 table, closed-loop comparison,
controller action validation, online graph index, and `report.md`.
