from __future__ import annotations

import json
import math
import shlex
import time
from pathlib import Path
from typing import Any

from ..config import CONFIG_ROOT, SamplerConfig, read_toml
from ..remote import Host
from .config import controller_config
from .models import MetricSample
from .policy import PressurePolicy


def controller_preflight(config: SamplerConfig) -> dict[str, Any]:
    controller = controller_config(config)
    host = Host(str(config.target["host"]))
    probe = host.run(
        "set -eu; command -v taskset; test -r /proc/self/schedstat; "
        "test \"$(cat /proc/sys/kernel/sched_schedstats)\" = 1; "
        "for path in /sys/devices/system/node/node*/cpulist; do "
        "printf '%s=%s\\n' \"$(basename \"$(dirname \"$path\")\")\" \"$(cat \"$path\")\"; done"
    )
    system = str(config.section("experiment").get("system", ""))
    system_path = CONFIG_ROOT / "systems" / f"{system}.toml"
    targets: list[dict[str, object]] = []
    if system_path.is_file():
        command = str(read_toml(system_path)["all_pid_command"])
        result = host.run(command, check=False)
        for token in result.stdout.split():
            if not token.isdigit():
                continue
            pid = int(token)
            details = host.run(
                f"printf '%s ' \"$(awk '/^Uid:/' /proc/{pid}/status | awk '{{print $2}}')\"; "
                f"awk '/Cpus_allowed_list/{{print $2}}' /proc/{pid}/status",
                check=False,
            ).stdout.split()
            if details:
                targets.append({
                    "pid": pid,
                    "owner_uid": int(details[0]),
                    "affinity": details[1] if len(details) > 1 else "",
                })
    return {
        "schema": "prism-sampler.controller-preflight.v1",
        "host": config.target["host"],
        "mode": controller.mode,
        "taskset": True,
        "schedstat": True,
        "topology": probe.stdout.strip().splitlines()[1:],
        "targets": targets,
        "cgroup_cpuset": False,
        "actuator": "taskset",
    }


def _finite(value: object) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def replay_experiment(experiment: Path, config: SamplerConfig) -> dict[str, Any]:
    import duckdb

    controller = controller_config(config, mode_override="shadow")
    rows = []
    for db in sorted(experiment.glob("runs/**/dataset/telemetry.db3")):
        run_dir = db.parents[1]
        phase_path = run_dir / "meta" / "phase.json"
        if not phase_path.is_file():
            continue
        phase = json.loads(phase_path.read_text(encoding="utf-8"))
        if phase.get("workload_start_epoch_ns") is None:
            continue
        pids = [int(row["pid"]) for row in phase.get("target_processes", [])]
        if not pids:
            continue
        start = int(phase["workload_start_epoch_ns"]) / 1e9
        end = int(phase["workload_end_epoch_ns"]) / 1e9
        placeholders = ",".join("?" for _ in pids)
        con = duckdb.connect(str(db), read_only=True)
        value = con.execute(
            f"SELECT sum(run_share*time_diff),sum(rq_share*time_diff) "
            f"FROM taskstats_view WHERE epoch(ts)>=? AND epoch(ts)<? "
            f"AND pid IN ({placeholders})",
            [start, end, *pids],
        ).fetchone()
        con.close()
        duration_ns = (end - start) * 1e9
        run_cpu = _finite(value[0] / duration_ns if value[0] is not None else None)
        rq_cpu = _finite(value[1] / duration_ns if value[1] is not None else None)
        policy = PressurePolicy(controller)
        decisions = []
        for index in range(controller.decision_window_samples):
            sample = MetricSample(
                realtime_ns=int((start + index * controller.sample_interval_seconds) * 1e9),
                monotonic_ns=int((index + 1) * controller.sample_interval_seconds * 1e9),
                interval_seconds=controller.sample_interval_seconds,
                workload_active=True,
                valid=run_cpu is not None and rq_cpu is not None,
                run_cpu_equiv=run_cpu,
                rq_cpu_equiv=rq_cpu,
                run_pressure=run_cpu / 32 if run_cpu is not None else None,
                rq_pressure=rq_cpu / 32 if rq_cpu is not None else None,
                tids_observed=0,
            )
            decisions.append(policy.evaluate(sample))
        rows.append({
            "run": str(run_dir.relative_to(experiment)),
            "profile": phase.get("profile", run_dir.parts[-4]),
            "phase": phase.get("phase", run_dir.parts[-3]),
            "round": phase.get("round", run_dir.name),
            "run_cpu_equiv": run_cpu,
            "rq_cpu_equiv": rq_cpu,
            "run_pressure": run_cpu / 32 if run_cpu is not None else None,
            "rq_pressure": rq_cpu / 32 if rq_cpu is not None else None,
            "recommended_state": policy.state.value,
            "action": next((item.action for item in decisions if item.action), None),
        })
    summary = experiment / "summary"
    summary.mkdir(parents=True, exist_ok=True)
    output = summary / "controller-replay.json"
    report = {
        "schema": "prism-sampler.controller-replay.v1",
        "experiment": str(experiment.resolve()),
        "rows": rows,
        "expanded": sum(row["recommended_state"] == "two_node" for row in rows),
    }
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report["output"] = str(output)
    return report
