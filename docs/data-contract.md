# Data Contract

Each YBA phase produces an independent run:

```text
runs/<phase>/r<round>/
  raw/       collector.db3, perf CSV, pressure JSONL, logs
  dataset/   telemetry.db3
  features/  activity, futex, VFS, pair features, R candidates
  meta/      phase context, capability and health records
```

`raw/collector.db3` and sidecars are immutable evidence. `telemetry.db3` is a
copy enriched with `pmu_samples`, `pmu_derived`, `numa_samples`,
`thread_placement_samples`, `system_pressure_samples`, and `phase_markers`.

PMU rows retain raw value, unit, scope, scale, time-enabled, and time-running.
Every local sampler row carries realtime and monotonic timestamps. YBA context
records coordinator clocks; host offset is captured by preflight. Cross-host
alignment must use these markers rather than assuming synchronized wall clocks.

Relationship analysis requires explicit target PIDs. It outputs every discovered
thread group in `group-activity.csv`, while candidate pairs require both groups
to be active and at least one futex or VFS relationship window.

