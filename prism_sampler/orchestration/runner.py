from __future__ import annotations

import csv
import json
import os
import re
import shlex
import shutil
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


def sampling_requests(config: SamplerConfig, plugin: str) -> bool:
    profile_name = str(config.sampling.get("profile", "policy"))
    profile = config.values["sampling_profiles"][profile_name]
    requested = list(profile.get("required", [])) + list(profile.get("optional", []))
    return plugin in requested


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


def run_yba(
    config: SamplerConfig,
    yba_config: Path,
    scenario: Path,
    *,
    controller_mode: str | None = None,
    experiment_name: str | None = None,
) -> int:
    yba_config = yba_config.resolve()
    scenario = scenario.resolve()
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
    if controller_mode:
        local_hook += f" --controller-mode {shlex.quote(controller_mode)}"
    client = config.section("client")
    remote_hook = str(client.get("hook_command", "prism-sampler-hook"))
    remote_config = str(client.get("sampler_config", ""))
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    experiment_name = experiment_name or "prism-sampler"
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", experiment_name):
        raise ValueError(f"invalid experiment name: {experiment_name}")
    run_id = f"{stamp}-{experiment_name}"
    system = str(config.section("experiment").get("system", "unknown"))
    experiment_root = output_root / system / run_id
    yba_output = experiment_root / f"yba-{run_id}"
    experiment_root.mkdir(parents=True, exist_ok=True)
    env.update({
        "ENABLE_METRICS": "0",
        "ENABLE_REALTIME_KPI": "1",
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
        from ..controller.artifacts import import_controller_experiment

        import_controller_experiment(experiment_root, yba_output=yba_output)
        _postprocess_experiment(experiment_root, finalized_runs, yba_returncode=returncode)
    return returncode


def run_yba_suite(
    config: SamplerConfig,
    yba_config: Path,
    suite_config: Path,
    *,
    experiment_root: Path | None = None,
    resume: bool = False,
) -> int:
    check = preflight(config)
    yba_root = Path(config.section("yba")["root"])
    runner = yba_root / "tools" / "run-suite.sh"
    if not runner.is_file():
        raise ValueError(f"YBA suite runner does not exist: {runner}")
    output_root = Path(config.section("experiment").get("output_root", "/data/threadState/experiments"))
    system = str(config.section("experiment").get("system", "unknown"))
    if experiment_root is None:
        if resume:
            raise ValueError("--resume requires --experiment-root")
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        experiment_root = output_root / system / f"{stamp}-{suite_config.stem}"
    experiment_root = experiment_root.resolve()
    experiment_root.mkdir(parents=True, exist_ok=True)
    (experiment_root / "preflight.json").write_text(
        json.dumps(check, indent=2, sort_keys=True) + "\n"
    )
    suite_dir = experiment_root / "yba-suite"
    env = os.environ.copy()
    local_hook = (
        f"{shlex.quote(sys.executable)} -m prism_sampler.hooks "
        f"--config {shlex.quote(str(config.source))}"
    )
    client = config.section("client")
    env.update({
        "ENABLE_METRICS": "0",
        "ENABLE_REALTIME_KPI": "1",
        "ENABLE_EXTERNAL_HOOK": "1",
        "EXTERNAL_HOOK_COMMAND": local_hook,
        "EXTERNAL_HOOK_REMOTE_COMMAND": str(client.get("hook_command", "prism-sampler-hook")),
        "EXTERNAL_HOOK_REMOTE_CONFIG": str(client.get("sampler_config", "")),
        "EXTERNAL_HOOK_RUN_ID": experiment_root.name,
        "EXTERNAL_HOOK_SYSTEM": system,
        "PRISM_SAMPLER_CONFIG": str(config.source),
        "SUITE_DIR": str(suite_dir),
    })
    command = [str(runner), "--config", str(yba_config), "--suite", str(suite_config)]
    if resume:
        command.append("--resume")
    returncode = subprocess.run(command, cwd=yba_root, env=env, check=False).returncode
    if not sampling_requests(config, "prism"):
        return returncode
    finalized = _collect_suite_runs(config, experiment_root, suite_dir)
    if finalized:
        from ..relations import analyze_experiment

        analysis = analyze_experiment(experiment_root)
        status = {
            "schema": "prism-sampler.suite-postprocess.v1",
            "yba_returncode": returncode,
            "finalized_runs": finalized,
            "analysis": analysis,
            "status": "complete" if not analysis["errors"] else "failed",
        }
        summary = experiment_root / "summary"
        summary.mkdir(parents=True, exist_ok=True)
        (summary / "postprocess.json").write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n"
        )
        if analysis["errors"] and returncode == 0:
            raise RuntimeError(f"relationship analysis failed for {analysis['errors']} run(s)")
    return returncode


def _read_shell_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, raw = line.split("=", 1)
        parsed = shlex.split(raw)
        values[key] = parsed[0] if parsed else ""
    return values


def _collect_suite_runs(config: SamplerConfig, experiment: Path, suite_dir: Path) -> int:
    client = config.section("client")
    client_host = Host(str(client.get("host", "")))
    client_output = str(client.get("output_root", "")).rstrip("/")
    system = str(config.section("experiment").get("system", "unknown"))
    if not client_output:
        raise ValueError("client.output_root is required for Suite artifact collection")
    incoming = experiment / ".incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    finalized = 0
    for yba_cell in sorted((suite_dir / "runs").glob("*")):
        marker = yba_cell / ".complete"
        cell_meta = yba_cell / "meta" / "suite-run.env"
        if not marker.is_file() or not cell_meta.is_file():
            continue
        values = _read_shell_env(cell_meta)
        profile = values["profile"]
        label = values.get("load") or values.get("scenario")
        round_number = int(values["round"])
        timeline = _timeline(yba_cell / "scenario-timeline.csv")
        destination_labels = list(timeline) if len(timeline) > 1 else [label]
        if destination_labels and all(
            (
                experiment
                / "runs"
                / profile
                / destination_label
                / f"r{round_number}"
                / "dataset"
                / "telemetry.db3"
            ).is_file()
            for destination_label in destination_labels
        ):
            continue
        hook_id = f"{experiment.name}-{yba_cell.name}"
        remote = f"{client_output}/{system}/{hook_id}"
        local_cell = incoming / hook_id
        if not local_cell.exists():
            exists = client_host.run(f"test -d {shlex.quote(remote)}", check=False)
            if exists.returncode:
                raise RuntimeError(f"Prism Suite cell output is missing: {remote}")
            client_host.copy_from(remote, local_cell, recursive=True)
        prism_runs = sorted(local_cell.glob("runs/*/r*"))
        if not prism_runs:
            raise RuntimeError(f"expected Prism phases in Suite cell: {local_cell}")
        for source_run in prism_runs:
            phase_path = source_run / "meta" / "phase.json"
            phase = json.loads(phase_path.read_text())
            source_phase = str(phase.get("phase", ""))
            if not source_phase:
                raise RuntimeError(f"collected Prism phase has no phase label: {phase_path}")
            destination_label = source_phase if len(prism_runs) > 1 else label
            destination = experiment / "runs" / profile / destination_label / f"r{round_number}"
            if (destination / "dataset" / "telemetry.db3").is_file():
                continue
            phase["profile"] = profile
            phase["phase"] = destination_label
            phase["source_phase"] = source_phase
            phase["suite_target"] = label
            phase["round"] = round_number
            bounds = timeline.get(source_phase)
            if not bounds:
                raise RuntimeError(
                    f"Suite calibration requires timeline phase {source_phase}: {yba_cell}"
                )
            phase.update(_target_workload_bounds(bounds, phase))
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source_run, destination, dirs_exist_ok=True)
            destination_phase = destination / "meta" / "phase.json"
            destination_phase.write_text(json.dumps(phase, indent=2, sort_keys=True) + "\n")
            finalize_run(destination, phase)
            phase_dirs = sorted(yba_cell.glob(f"phases/*-{source_phase}"))
            if len(phase_dirs) != 1:
                raise RuntimeError(
                    f"expected one YBA phase directory for {source_phase}: {yba_cell}"
                )
            import_yba_kpi(destination, phase_dirs[0])
            finalized += 1
        client_host.run(f"rm -rf {shlex.quote(remote)}", check=False)
        shutil.rmtree(local_cell, ignore_errors=True)
    if incoming.exists() and not any(incoming.iterdir()):
        incoming.rmdir()
    return finalized


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
