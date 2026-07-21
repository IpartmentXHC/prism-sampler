# Repository Guidelines

## Project Structure

`prism_sampler/collectors/` owns collector lifecycle and health checks.
`orchestration/` integrates YBA without duplicating database adapters.
`relations/` extracts target-PID thread relationships and computes R.
`policies/` generates candidate-only NUMA placement files. Platform, system,
and sampling defaults live under `config/`; machine paths belong only in the
ignored `config/local.toml`.

## Development Commands

Use the existing environment rather than creating another venv:

```bash
/data/threadState/prism/.venv/bin/pip install -e '.[excel,test]'
/data/threadState/prism/.venv/bin/python -m unittest discover -s tests
git diff --check
```

Run `python -m compileall -q prism_sampler` after changing CLI or collection
modules. YBA hook changes are tested in the separate YBA repository.

## Design Rules

Use snake_case for Python and kebab-case for CLI commands. Keep raw artifacts
immutable; enrich a copied `telemetry.db3`. Every metric must retain units,
timestamps, scope, and PMU scaling fields. A missing optional collector is an
explicit capability error, never a numeric zero.

Do not hardcode Doris or ClickHouse thread groups. Discover target-PID `comm`
values and apply optional configuration rules. Keep logical futex/VFS sharing
separate from hardware cache sharing. Window persistence belongs only in
Stability, not Synchronization or Sharing.

## Safety And Data

Policies remain `candidate_only` until G, safety gates, and rollback are
validated. Never automatically apply affinity from generated output. Do not
commit DB3, Excel, CSV, JSONL, perf traces, credentials, host IDs, or experiment
directories. Commit only reusable code, sanitized fixtures, and capability
schemas.

