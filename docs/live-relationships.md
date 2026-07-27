# Live Relationship Analysis

Prism Sampler can consume Prism's `prism.live.v1` Unix-socket stream and build
a bounded, sliding thread-group relationship graph. This path is shadow-only:
it writes candidates and confidence metadata but never calls an affinity or
cpuset actuator.

## Lifecycle

Select the explicit sampling profile:

```toml
[sampling]
profile = "online-relationship"

[target]
live_analyzer_command = "/home/user/.local/bin/prism-live-analyzer"

[relations]
live_interval_ms = 1000
live_queue_capacity = 64
window_seconds = 60
stability_window_seconds = 10
emit_seconds = 10
minimum_evidence_windows = 3
record_snapshots = true
```

`CollectionSession` then:

1. Starts `metric-collector` with `--live-socket` while preserving DuckDB.
2. Waits for collector attach and starts `prism-live-analyzer` as the socket
   owner rather than as root.
3. Stops the analyzer before Prism, allowing a final candidate emission.
4. Copies DB3 and live artifacts into the same immutable `raw/` directory.

Existing sampling profiles do not enable the live analyzer.

The analyzer can also run directly:

```bash
prism-live-analyzer \
  --socket /run/prism/events.sock \
  --output-dir /tmp/prism-live \
  --pid 1234 \
  --window-seconds 60 \
  --emit-seconds 10
```

## Protocol Mapping

- `taskstats` values are cumulative totals. The analyzer computes monotonic
  per-TID run-time and runqueue deltas; first observations are mapping-only.
- `futex_wait` and `futex_wake` are per-snapshot aggregates. Wait time is
  attributed to each waker in proportion to successful wakes on the same
  futex key, preserving `waker -> waiter` direction.
- `vfs` rows are merged across operations by
  `(machine, fs_magic, device, inode, TID)`. Pair sharing uses the smaller
  group access time and discounts resources with high group degree. Self
  sharing requires two distinct TIDs in one group.
- TIDs are mapped from the latest taskstats `comm`. Optional regex rules merge
  names; unmatched names remain independent groups. No database thread name is
  hardcoded.

Logical VFS sharing is not hardware cache-line sharing. I/O-wait is validated
and retained by the Prism protocol but is not an R input.

## Online Scores

The online formulas have separate IDs because stability is measured across
live subwindows rather than repeated offline runs:

```text
R_pair_live = 100 * Activity * Stability
              * (0.7 * Synchronization + 0.3 * Sharing * ActiveOverlap)

R_self_live = 100 * Activity * Stability
              * (0.7 * Synchronization + 0.3 * Sharing)

Stability = duration_weighted_relationship_window_coverage
            * 1 / (1 + signal_CV)
```

Prism may publish every second, but stability is evaluated in independent
10-second analysis subwindows by default. Changing the transport interval does
not redefine relationship persistence.

Activity, synchronization, and sharing use the same log/P95 normalization
contract as offline analysis. A supplied offline `relation-scales.json` is
reported as `fixed_calibration`. Without one, the analyzer uses the current
window's P95 and marks every output `rolling_p95_uncalibrated`; such scores are
valid for shadow ranking but not for a fixed scheduling threshold.

## Confidence

R describes observed relationship strength. `confidence` independently
describes whether enough trustworthy live evidence supports that score:

```text
global_confidence = window_warmup
                    * stream_completeness
                    * TID_mapping_coverage

candidate_confidence = global_confidence
                       * min(evidence_windows / required_windows, 1)
```

Quality flags identify warmup, sequence gaps, producer or consumer queue drops,
low TID mapping, taskstats counter resets, and uncalibrated rolling scales.
Missing optional data is never converted to a successful zero-valued sample.

## Artifacts

Each enabled phase adds:

```text
raw/live-stream.jsonl              exact delivered Prism snapshots
raw/live-candidates.jsonl          periodic rolling candidates
raw/live-candidates-latest.json    atomic latest shadow decision input
raw/live-summary.json              counts and final quality state
raw/live-relations.log             analyzer stdout/stderr
```

DB3 remains authoritative for offline replay. A full candidate object contains
window boundaries, sequence range, scales, stream quality, directional
`R_pair`, `R_self`, and per-candidate confidence.

## Safety Boundary

The live analyzer imports no controller actuator and emits no taskset command.
Connecting candidates to placement requires a separately reviewed G model,
capacity constraints, minimum confidence, dwell time, and rollback policy.
