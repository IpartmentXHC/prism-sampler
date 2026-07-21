# Architecture

## Boundaries

```text
YBA                 prism-sampler                     future controller
service/workload -> phase hook -> collectors -> R -> candidate policy -> online decisions
KPI/timeline         raw + telemetry DB3               affinity/rollback
```

YBA remains authoritative for service readiness and workload timing. Its
generic external hook blocks at `phase_before` until required collectors are
healthy. `phase_after` stops collectors with SIGINT and waits for DuckDB flush.
Attach and flush time are outside the measured YCSB phase.

The Rust `metric-collector` remains in the personal Prism fork pinned by
`collector.lock`. Prism Sampler consumes its CLI and DuckDB schema; it does not
copy the analysis UI or modify upstream ownership boundaries.

## Collection Lifecycle

1. Probe architecture, kernel, BTF, PMUs, topology, privileges, and clocks.
2. Validate target PID and `/proc/PID/stat` start time.
3. Start required plugins, then optional plugins, and publish health state.
4. Return from `phase_before`; YBA starts workload timing.
5. Measure the target/client clock offset and capture target-local realtime and
   monotonic timestamps throughout the phase.
6. Stop in reverse order, copy raw files, and preserve raw DB3 unchanged.
7. Build `telemetry.db3`, validate required tables, then analyze only the exact
   target-clock workload interval.

Hook and stop operations are idempotent. Required plugin failure prevents the
workload; optional failure is recorded in capabilities and health artifacts.

## Sampling Profiles

- `minimal`: Prism task statistics and phase markers.
- `relationship`: Prism futex/VFS/taskstats plus local placement snapshots.
- `policy`: relationship data, core/uncore perf, NUMA, PSI, runqueue evidence.
- `diagnostic`: policy profile plus opt-in ARM SPE.
