# AffinityGraph Offline Placement Research

AffinityGraph placement research is an offline workflow. It does not authorize
affinity changes, deployment, remote access, or edits to the evaluator.

## Dataset contract

Export a completed local run with:

```bash
prism-sampler affinitygraph export-placement \
  --run RUN_DIRECTORY --output DATASET_DIRECTORY
```

`placement.duckdb` is the canonical fact source. It contains `manifest`,
`solve_windows`, `threads`, `groups`, `edges`, `topology_cpus`,
`topology_edges`, `plans`, `assignments`, `family_metrics`, `domains`,
`planned_masks`, `mask_actions`, `cpu_loads`, `action_batches`, and `bpf_health`.
Foreign thread references use `(tgid, tid, starttime)`; a TID is
never joined across windows without its start time. The exporter also writes
the curated CSV views and one `affinitygraph.snapshot.v1` JSON file for every
complete solve window.

The manifest records SHA256 for source runtime logs and any run-local TOML,
build manifest, or BPF object. Unframed historical plan/action records are not
treated as comparable windows. They are recorded only as
`legacy_unframed_canaries`, including the dynamic slot-cap concentration
check.

## Candidate comparison

Build `affinity-replay`, then compare reviewed strategy files:

```bash
make -C /data/threadState/affinitygraph build/affinity-replay
prism-sampler affinitygraph compare-placement \
  --dataset DATASET_DIRECTORY/placement.duckdb \
  --candidates /data/threadState/affinitygraph/strategies \
  --output comparison.json
```

The comparison runs every candidate five times per window. It first applies
the completeness, envelope, normalized CPU-mask, CPU/node overload, dynamic thread-slot,
active migration, determinism, and solve-P95 gates. Only passing candidates
can enter the Pareto frontier. The report gives metric deltas relative to
`legacy-v1`; it deliberately has no weighted total score or automatic winner.

Raw datasets, snapshots, CSV files, and comparison artifacts are experiment
data and must not be committed.

## Rollout boundary

Offline review can nominate at most three candidates for plan-mode dry runs.
Plan mode must produce zero actions. Only one manually approved candidate may
advance to smoke, and formal paired experiments require a separate approval
after smoke. Throughput and P99 are online causal acceptance measurements, not
offline training labels.
