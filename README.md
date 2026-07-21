# Prism Sampler

Prism Sampler is a Kunpeng-first collection and offline NUMA policy toolkit.
YBA owns database lifecycle, workload execution, throughput, and latency. This
repository owns eBPF/PMU/NUMA telemetry, relationship analysis, and guarded
candidate policy generation. It does not implement an online scheduler.

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

Generated policies are always `candidate_only`, have no claimed expected gain,
and render YBA profiles with `ENABLE_THREAD_CLUSTER=0` unless explicitly enabled.

See [Architecture](docs/architecture.md), [Data Contract](docs/data-contract.md),
and [Policy Schema](docs/policy-schema.md).

