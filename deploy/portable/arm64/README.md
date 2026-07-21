# Prism ARM64 Portable Collector

This package contains only the files required for Prism data collection on an
ARM64 Linux server. Analysis and Excel export remain on the local workstation.

## Requirements

- `aarch64`/ARM64 CPU
- Linux kernel with eBPF, kprobes, ftrace, and BTF enabled
- readable `/sys/kernel/btf/vmlinux`
- root or working `sudo`
- system `glibc`, `libelf.so.1`, and `libz.so.1`

The package includes compatible `libstdc++.so.6` and `libgcc_s.so.1`.

## Verify The Machine

```bash
./prismctl preflight
./prismctl smoke --strict-6.6 --duration 20
```

`smoke` starts a temporary file-I/O workload, attaches Prism to it, stops the
collector with `SIGINT`, and verifies that a non-empty DB3 was produced.
On Linux 5.10 use `./prismctl smoke --best-effort`; subsystem load failures are
recorded in `collector_capabilities` instead of aborting the whole collector.

For passwordless sudo no extra configuration is needed. With askpass:

```bash
export SUDO_ASKPASS=/absolute/path/to/askpass.sh
./prismctl smoke --duration 20
```

## Collect One Process

Run until `Ctrl-C`:

```bash
./prismctl collect \
  --pids "$(pgrep -x doris_be | paste -sd, -)" \
  --platform-profile kunpeng \
  --best-effort \
  --output-dir "$PWD/data/doris" \
  --file doris.db3
```

Run for a fixed interval:

```bash
./prismctl collect \
  --process-name clickhouse \
  --duration 60 \
  --output-dir "$PWD/data/clickhouse" \
  --file clickhouse.db3
```

## Background Collection

```bash
./prismctl start \
  --pids 1234 \
  --output-dir "$PWD/data/run-1" \
  --file run-1.db3 \
  --state-dir "$PWD/data/run-1/state"

./prismctl status --state-dir "$PWD/data/run-1/state"
./prismctl stop --state-dir "$PWD/data/run-1/state"
```

Always stop with `prismctl stop` or `SIGINT` so DuckDB can close cleanly.

`manifest.json` records the source commit, build host ABI, build kernel, and
supported platform profiles. `capability-probe` reports BTF, tracefs, NUMA, and
available ARM/HiSilicon PMUs without loading eBPF programs.
