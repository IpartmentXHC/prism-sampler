# Pressure-Aware NUMA Scaling

The prototype changes the CPU affinity envelope of one ClickHouse lifecycle:

```text
one_node = node0
two_node = node0 + node1
```

Every ten seconds the target-local controller sums execution and runqueue wait
time deltas from `/proc/PID/task/TID/schedstat`. Dividing each sum by the sample
duration yields CPU equivalents. Pressure is normalized by the 32 CPUs in the
one-node envelope.

Expansion requires run pressure >= 0.90 and runqueue pressure >= 0.50 for three
consecutive active samples. Shrink requires <= 24 running CPU equivalents and
runqueue pressure <= 0.05 for 180 seconds, after a minimum 300-second two-node
residency. YBA phase gaps pause confirmation. A recent shrink rolls back after
two high-pressure samples.

`shadow` updates the policy state and journal without changing affinity.
`active` applies `taskset` to every target task, verifies the complete mask, and
restores the original per-thread masks on exit. PID start times are checked
before every action. The prototype does not mount cpusets, reserve CPUs, power
down nodes, migrate pages, or use phase names as policy inputs.

Set `target.controller_command_prefix` when the database rejects same-UID
affinity changes. Active startup fails if the initial one-node affinity cannot
be applied and verified, so an actuator failure cannot be recorded as a valid
active experiment.

PMU, NUMA placement, Prism R, throughput, and P99 remain explanatory evidence.
They are aligned with controller samples during postprocessing and are not part
of the first online decision gate.
