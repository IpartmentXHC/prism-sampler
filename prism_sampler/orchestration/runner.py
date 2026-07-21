from __future__ import annotations

import csv
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config import SamplerConfig, validate_config
from ..artifacts import finalize_run, import_yba_kpi
from ..collectors.session import measure_clock_offset
from ..platform import probe
from ..remote import Host


def preflight(config: SamplerConfig) -> dict[str, Any]:
    missing = validate_config(config)
    if missing:
        raise ValueError("configuration is incomplete: " + ", ".join(missing))
    host = Host(config.target["host"])
    report = probe(host)
    runtime = str(config.collector.get("runtime_lib", ""))
    env = f"LD_LIBRARY_PATH={shlex.quote(runtime)} " if runtime else ""
    collector = shlex.quote(str(config.collector["binary"]))
    collector_help = host.run(f"{env}{collector} --help", check=False)
    agent = shlex.quote(str(config.target.get("agent_command", "prism-sampler-agent")))
    agent_check = host.run(f"{agent} --help >/dev/null", check=False)
    perf_check = host.run("command -v perf >/dev/null", check=False)
    clock = measure_clock_offset(host)
    result = {
        "schema": "prism-sampler.preflight.v1",
        "config_sha256": config.digest(),
        "platform": report.to_dict(),
        "checks": {
            "collector": collector_help.returncode == 0,
            "collector_capability_cli": "--platform-profile" in collector_help.stdout,
            "agent": agent_check.returncode == 0,
            "perf": perf_check.returncode == 0,
        },
    }
    result.update(clock)
    if not result["checks"]["collector"]:
        raise RuntimeError("metric-collector cannot run on the target")
    if "snapshot" in config.values["sampling_profiles"][config.sampling.get("profile", "policy")]["required"]:
        if not result["checks"]["agent"]:
            raise RuntimeError("target-local prism-sampler-agent is unavailable")
    return result


def run_yba(config: SamplerConfig, yba_config: Path, scenario: Path) -> int:
    check = preflight(config)
    yba_root = Path(config.section("yba")["root"])
    yba = yba_root / "bin" / "yba"
    if not yba.is_file():
        raise ValueError(f"YBA executable does not exist: {yba}")
    output_root = Path(config.section("experiment").get("output_root", "/data/threadState/experiments"))
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / "last-preflight.json"
    report_path.write_text(json.dumps(check, indent=2, sort_keys=True) + "\n")
    env = os.environ.copy()
    local_hook = (
        f"{shlex.quote(sys.executable)} -m prism_sampler.hooks "
        f"--config {shlex.quote(str(config.source))}"
    )
    client = config.section("client")
    remote_hook = str(client.get("hook_command", "prism-sampler-hook"))
    remote_config = str(client.get("sampler_config", ""))
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    experiment_name = "prism-sampler"
    run_id = f"{stamp}-{experiment_name}"
    system = str(config.section("experiment").get("system", "unknown"))
    experiment_root = output_root / system / run_id
    yba_output = experiment_root / f"yba-{run_id}"
    experiment_root.mkdir(parents=True, exist_ok=True)
    env.update({
        "ENABLE_METRICS": "0",
        "ENABLE_THREAD_CLUSTER": "0",
        "ENABLE_EXTERNAL_HOOK": "1",
        "EXTERNAL_HOOK_COMMAND": local_hook,
        "EXTERNAL_HOOK_REMOTE_COMMAND": remote_hook,
        "EXTERNAL_HOOK_REMOTE_CONFIG": remote_config,
        "EXTERNAL_HOOK_RUN_ID": run_id,
        "EXTERNAL_HOOK_SYSTEM": system,
        "PRISM_SAMPLER_CONFIG": str(config.source),
        "TIMESTAMP": stamp,
        "EXPERIMENT_NAME": experiment_name,
        "EXPERIMENT_DIR": str(yba_output),
    })
    returncode = subprocess.run(
        [str(yba), "scenario", "--config", str(yba_config), "--scenario", str(scenario)],
        cwd=yba_root,
        env=env,
        check=False,
    ).returncode
    client_output = str(client.get("output_root", "")).rstrip("/")
    client_host = str(client.get("host", ""))
    finalized_runs = 0
    if client_output and client_host:
        remote = f"{client_output}/{system}/{run_id}"
        if Host(client_host).run(f"test -d {shlex.quote(remote)}", check=False).returncode == 0:
            Host(client_host).copy_from(f"{remote}/runs", experiment_root, recursive=True)
            timeline = _timeline(yba_output / "scenario-timeline.csv")
            for db in sorted(experiment_root.glob("runs/**/raw/collector.db3")):
                run_dir = db.parents[1]
                phase_path = run_dir / "meta" / "phase.json"
                if phase_path.is_file():
                    phase = json.loads(phase_path.read_text())
                    bounds = timeline.get(str(phase.get("phase", "")))
                    if bounds:
                        phase.update(_target_workload_bounds(bounds, phase))
                        phase_path.write_text(json.dumps(phase, indent=2, sort_keys=True) + "\n")
                    finalize_run(run_dir, phase)
                    phase_dirs = sorted(yba_output.glob(f"phases/*-{phase.get('phase', '')}"))
                    if len(phase_dirs) != 1:
                        raise RuntimeError(
                            f"expected one YBA output directory for phase {phase.get('phase', '')}"
                        )
                    import_yba_kpi(run_dir, phase_dirs[0])
                    finalized_runs += 1
    if finalized_runs:
        _postprocess_experiment(experiment_root, finalized_runs, yba_returncode=returncode)
    return returncode


def _timeline(path: Path) -> dict[str, dict[str, int]]:
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8") as stream:
        return {
            row["phase"]: {
                "client_workload_start_epoch_ns": int(row["started_epoch_ns"]),
                "client_workload_end_epoch_ns": int(row["finished_epoch_ns"]),
            }
            for row in csv.DictReader(stream)
            if row.get("phase") and row.get("started_epoch_ns") and row.get("finished_epoch_ns")
        }


def _target_workload_bounds(
    bounds: dict[str, int], phase: dict[str, Any]
) -> dict[str, int | str]:
    if phase.get("target_clock_offset_ns") is None:
        raise ValueError("phase metadata has no target clock offset")
    offset = int(phase["target_clock_offset_ns"])
    start = int(bounds["client_workload_start_epoch_ns"])
    end = int(bounds["client_workload_end_epoch_ns"])
    return {
        **bounds,
        "workload_start_epoch_ns": start + offset,
        "workload_end_epoch_ns": end + offset,
        "workload_clock": "target_realtime",
    }


def _postprocess_experiment(
    experiment: Path, finalized_runs: int, *, yba_returncode: int
) -> dict[str, Any]:
    from ..policies import generate_policies
    from ..relations import analyze_experiment

    summary = experiment / "summary"
    summary.mkdir(parents=True, exist_ok=True)
    status: dict[str, Any] = {
        "schema": "prism-sampler.postprocess.v1",
        "finalized_runs": finalized_runs,
        "status": "complete",
    }
    try:
        analysis = analyze_experiment(experiment)
        status["analysis"] = analysis
        if analysis["errors"]:
            raise RuntimeError(f"relationship analysis failed for {analysis['errors']} run(s)")
        if analysis["runs"] != finalized_runs:
            raise RuntimeError(
                f"analyzed {analysis['runs']} of {finalized_runs} finalized run(s)"
            )
        if analysis["candidates"]:
            status["policy"] = generate_policies(experiment)
        else:
            status["policy"] = {
                "status": "skipped",
                "reason": "no eligible futex or VFS relationship candidates",
            }
    except Exception as exc:
        status["status"] = "failed"
        status["error"] = f"{type(exc).__name__}: {exc}"
        (summary / "postprocess.json").write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n"
        )
        if yba_returncode == 0:
            raise
        return status
    (summary / "postprocess.json").write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n"
    )
    return status
