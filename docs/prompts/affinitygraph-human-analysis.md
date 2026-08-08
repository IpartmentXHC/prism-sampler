# Fixed Prompt: AffinityGraph Human-Assisted Analysis

You are analyzing an AffinityGraph development dataset offline. Treat
`placement.duckdb` as the canonical fact source and CSV files only as curated
views. Open all inputs read-only. Do not modify the dataset, evaluator,
AffinityGraph runtime, BPF code, Supervisor, Actuator, experiment runner, or
remote systems. Do not access host 183, deploy, commit, push, invoke control
sockets, run active mode, or call `sched_setaffinity`.

Use `(tgid, tid, starttime)` as thread identity. Keep affinity, contention,
NUMA locality, capacity, and migration cost distinct. Do not interpret Prism
logical resource sharing as hardware cache sharing. Do not invent LLC, IRQ,
softirq, CPU capacity, application masks, or thread-local NUMA facts.

Produce one Markdown report containing:

1. Dataset hashes, complete-window count, BPF health/loss, and limitations.
2. The exact DuckDB SQL used for every conclusion.
3. Demand and confidence distributions by window and normalized thread group.
4. CPU/node thread-count and demand concentration, including dynamic slot-cap
   violations.
5. Relationship evidence using activity, sync, share, stability, score,
   handoff rate, shared-VFS seconds, overlap, observation count, coverage, and
   CV without relabeling them as global Prism totals.
6. Specific anomalous `(tgid, tid, starttime)` identities and groups.
7. Objective defects or unsupported causal claims, with contradictory
   evidence called out explicitly.

Do not select or apply a policy. End with falsifiable offline questions for the
strategy exploration pass.
