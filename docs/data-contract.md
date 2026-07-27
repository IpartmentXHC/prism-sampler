# Data Contract

Each YBA phase produces an independent run:

```text
runs/<phase>/r<round>/
  raw/       collector.db3, perf CSV, pressure JSONL, logs
  dataset/   telemetry.db3
  features/  activity, futex, VFS, pair/self features, R candidates
  meta/      phase context, capability and health records
```

`raw/collector.db3` and sidecars are immutable evidence. `telemetry.db3` is a
copy enriched with `pmu_samples`, `pmu_derived`, `numa_samples`,
`thread_placement_samples`, `system_pressure_samples`, and `phase_markers`.
YBA's authoritative phase and per-operation results are imported as
`yba_phase_kpi` and `yba_operation_kpi`; `meta/kpi.json` records source paths
and checksums.

PMU rows retain raw value, unit, scope, scale, time-enabled, and time-running.
Every local sampler row carries realtime and monotonic timestamps. At each
`phase_before`, the client hook measures `target - client` realtime offset over
a persistent SSH channel and records the lowest-RTT sample, RTT, and half-RTT
uncertainty. YBA's client timestamps are retained as
`client_workload_*_epoch_ns`; `workload_*_epoch_ns` is converted to target
realtime before analysis. Relationship analysis rejects phase metadata without
this explicit target-clock contract instead of assuming synchronized hosts.

DuckDB `TIMESTAMP` columns use naive UTC values so `epoch(ts)` remains aligned
with raw Unix timestamps regardless of the machine timezone. Raw DB3, perf CSV,
and JSONL files are never rewritten when derived telemetry is rebuilt.

Relationship analysis requires explicit target PIDs. It outputs every discovered
thread group in `group-activity.csv`. `relation-candidates.csv` contains
`R_pair` edges between distinct groups. `self-candidates.csv` contains
`R_self` node weights; intra-group VFS sharing requires at least two distinct
TIDs in the same logical group to access the same resource. Experiment
analysis is restricted to the exact YBA workload interval; collector attach and
flush samples remain available in telemetry but do not contribute to R.

An enabled pressure controller adds lifecycle-wide `controller/*.jsonl`
artifacts. Postprocessing slices them by target-clock workload bounds and adds
`controller_samples`, `controller_decisions`, and `controller_actions` to each
phase's `telemetry.db3`. These tables record observed and realized state only;
they do not contain a calibrated expected-gain G.

The static scores are:

```text
R_pair(i,j) = 100 * Activity_pair * Stability_pair
              * (0.7 * Sync_inter + 0.3 * Share_inter)
R_self(i)   = 100 * Activity_i * Stability_i
              * (0.7 * Sync_intra + 0.3 * Share_intra)
```

Synchronization contains attributed futex intensity, sharing contains selective
VFS intensity, and relationship-window coverage appears only in stability.
Pair and self features use separate experiment-wide P95 log scales.

The optional `online-relationship` profile additionally writes Prism live
snapshots and rolling shadow candidates under `raw/`. Online R uses separate
`*_live_v1` formula IDs, window-level repeatability, and explicit stream/TID
confidence. See [Live Relationship Analysis](live-relationships.md); live
output does not replace DB3 or authorize affinity changes.
