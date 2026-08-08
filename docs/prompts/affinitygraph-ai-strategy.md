# Fixed Prompt: AffinityGraph AI Offline Strategy Exploration

Work only on the supplied development `placement.duckdb`, its exported
snapshots, the fixed `affinity-replay` binary, and reviewed strategy schema.
All data access is read-only. You may create new candidate TOML files and one
candidate Markdown/JSON report in the designated output directory. You may
not change or replace the dataset, snapshots, evaluator, replay binary,
production solver, BPF, Supervisor, Actuator, runner, hooks, or deployment.
Do not access host 183 or any remote service. Do not invoke `affinityctl`,
active mode, control sockets, `taskset`, or `sched_setaffinity`. Do not commit
or push.

Candidate TOML may use only fields already accepted by
`affinitygraph.strategy.v1`: demand floor, dynamic/fixed CPU thread cap and
slack, count penalty, low-demand spread, same-CPU contention penalty,
group-aware spread penalty, tie-break policy, FM/LPT refinement counts, active
migration ratio/threshold, and fixed seed. Reject any idea that requires a new
metric or evaluator edit.

Evaluate every candidate on every complete development window with
`affinity-replay`. Repeat identical inputs to test determinism. First report
all hard-gate failures: complete singleton assignments inside the envelope,
zero CPU/node demand overload, thread slots no greater than
`ceil(eligible_threads / envelope_cpu_count) + 2`, active migration no greater
than 25%, identical output, and 128-CPU solve P95 below one second.

For candidates that pass, report per-window and aggregate max/P95 threads per
CPU, count CV/Gini, max CPU demand, relationship-weighted cross-CPU latency,
same-CPU edge weight, active/inactive migration, migration distance,
continuous-window churn, group locality/spread, and solve time. Compare each
metric with `legacy-v1`. Return the Pareto frontier; never collapse metrics to
a single weighted score and never claim an automatic optimum. Recommend at
most three candidates for human review and plan-mode dry run.
