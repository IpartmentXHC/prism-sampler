# Architecture

## Boundaries

```text
YBA                 prism-sampler                     controller prototype
service/workload -> phase hook -> collectors -> R     system G -> affinity
offline KPI labels   raw + telemetry DB3              journal + safety rollback
```

YBA remains authoritative for service readiness and workload timing. Its
generic external hook blocks at `phase_before` until required collectors are
healthy. `phase_after` stops collectors with SIGINT and waits for DuckDB flush.
Attach and flush time are outside the measured YCSB phase.

The Rust `metric-collector` remains in the personal Prism fork pinned by
`collector.lock`. Prism Sampler consumes its CLI and DuckDB schema; it does not
copy the analysis UI or modify upstream ownership boundaries.

The pressure controller is a separate target-local process. YBA hooks only
start, mark, stop, and collect it. The collector never changes affinity, and
the pure black-box controller never reads workload identity, phase activity,
throughput, P99, or errors to make decisions. Those application KPIs are
imported after a run for supervision and acceptance only.

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
8. Generate relationship features and a candidate-only policy automatically;
   record an explicit skipped status when no eligible relationship exists.

Hook and stop operations are idempotent. Required plugin failure prevents the
workload; optional failure is recorded in capabilities and health artifacts.
The target-local snapshot agent runs through the configured privilege helper so
processes that restrict `/proc/PID/numa_maps` still produce NUMA evidence.

## Sampling Profiles

- `minimal`: Prism task statistics and phase markers.
- `relationship`: Prism futex/VFS/taskstats plus local placement snapshots.
- `policy`: relationship data, core/uncore perf, NUMA, PSI, runqueue evidence.
- `diagnostic`: policy profile plus opt-in ARM SPE.
