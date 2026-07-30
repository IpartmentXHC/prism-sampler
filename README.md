# Prism Sampler

Prism Sampler is a Kunpeng-first collection and NUMA policy research toolkit.
YBA owns database lifecycle, workload execution, throughput, and latency. This
repository owns eBPF/PMU/NUMA telemetry, relationship analysis, and guarded
candidate policy generation. It can consume Prism's live Unix-socket stream to
maintain shadow-only rolling `R_pair` and `R_self` candidates. The
`pressure-aware-numa-scaling` branch also
contains an experimental ClickHouse node-level pressure controller; it remains
disabled unless `--controller-mode shadow|active` is selected explicitly.

## Quick Start

Use the existing Prism virtual environment; no second environment is needed:

```bash
cd /data/threadState/prism-sampler
/data/threadState/prism/.venv/bin/pip install -e '.[excel,test]'
cp config/local.toml.example config/local.toml
prism-sampler platform probe --host kunpen183
prism-sampler preflight --config config/local.toml
```

Run a YBA multi-phase scenario with per-phase collection:

```bash
prism-sampler run \
  --config config/local.toml \
  --yba-config /path/to/base-dualhost.env \
  --scenario /path/to/scenario.env
```

Analyze an existing DB3 or experiment and generate candidate policies:

```bash
prism-sampler analyze db collector.db3 --pid 1234
prism-sampler analyze experiment /data/threadState/experiments/doris/run-id
prism-sampler policy generate /data/threadState/experiments/doris/run-id
```

Validate and run the pressure-aware NUMA prototype:

```bash
prism-sampler controller preflight --config config/local.toml
prism-sampler controller replay /path/to/experiment --config config/local.toml
prism-sampler run --config config/local.toml --yba-config /path/to/base.env \
  --scenario config/scenarios/clickhouse-pressure-lifecycle.env \
  --controller-mode shadow
```

`active` changes every target TID between node0 and node0+node1 with `taskset`.
It requires readable `/proc/PID/task/TID/schedstat`, stable PID identities, and
permission to change ClickHouse affinity. It does not migrate memory pages.

Generated policies are always `candidate_only`, have no claimed expected gain,
and render YBA profiles with `ENABLE_THREAD_CLUSTER=0` unless explicitly enabled.

Enable live relationship shadow analysis explicitly in the sampler config:

```toml
[sampling]
profile = "online-relationship"

[relations]
window_seconds = 60
stability_window_seconds = 10
emit_seconds = 10
```

This starts `prism-live-analyzer` next to the collector and writes live stream
health and candidates without invoking `taskset`.

See [Architecture](docs/architecture.md), [Data Contract](docs/data-contract.md),
[Policy Schema](docs/policy-schema.md), and
[Pressure Controller](docs/pressure-aware-numa-scaling.md).
See also [Live Relationship Analysis](docs/live-relationships.md) and
[Pure System Black-Box Scheduling](docs/system-blackbox-scheduling.md).
