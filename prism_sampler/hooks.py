from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .artifacts import validate_raw
from .collectors import CollectionSession, SessionContext
from .config import CONFIG_ROOT, SamplerConfig, load_config, read_toml
from .controller.config import controller_config
from .controller.integration import mark_controller, start_controller, stop_controller
from .controller.kpi_integration import start_kpi_forwarder, stop_kpi_forwarder
from .remote import Host


def _safe(value: object) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value)).strip("-")
    return text or "unknown"


def _system_name(config: SamplerConfig, context: dict[str, Any]) -> str:
    return str(
        context.get("system")
        or config.section("experiment").get("system")
        or "unknown"
    )


def _state_root(config: SamplerConfig) -> Path:
    root = Path(config.section("experiment").get("output_root", "/data/threadState/experiments"))
    path = root / ".prism-sampler-state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _state_path(config: SamplerConfig, session: str, phase: str, round_number: int) -> Path:
    return _state_root(config) / f"{_safe(session)}-{_safe(phase)}-r{round_number}.json"


def _discover_pids(config: SamplerConfig, context: dict[str, Any]) -> tuple[list[int], dict[int, int]]:
    rows = context.get("target_processes", [])
    pids = [int(row["pid"]) for row in rows if int(row.get("pid", 0)) > 0]
    starts = {int(row["pid"]): int(row["start_time"]) for row in rows if row.get("start_time")}
    if not pids:
        system_name = _system_name(config, context)
        system_path = CONFIG_ROOT / "systems" / f"{system_name}.toml"
        if not system_path.is_file():
            raise RuntimeError("hook context has no target PID and experiment.system is not configured")
        system = read_toml(system_path)
        host = Host(config.target["host"])
        output = host.run(system["all_pid_command"]).stdout
        pids = sorted({int(token) for token in output.split() if token.isdigit()})
        if not pids:
            raise RuntimeError(f"no target process found for system {system_name}")
        for pid in pids:
            value = host.run(f"awk '{{print $22}}' /proc/{pid}/stat").stdout.strip()
            starts[pid] = int(value)
    return pids, starts


def _identity(context: dict[str, Any]) -> tuple[str, str, int]:
    session = str(context.get("run_id") or context.get("session_id") or "yba-run")
    phase = str(context.get("phase") or context.get("label") or "phase")
    round_number = int(context.get("round") or 1)
    return session, phase, round_number


def _collect_phase(config: SamplerConfig, phase: str) -> bool:
    patterns = config.sampling.get("collect_phase_patterns", [])
    return not patterns or any(re.search(str(pattern), phase) for pattern in patterns)


def _run_dir(
    config: SamplerConfig,
    context: dict[str, Any],
    session: str,
    phase: str,
    round_number: int,
) -> Path:
    root = Path(config.section("experiment").get("output_root", "/data/threadState/experiments"))
    system = _safe(_system_name(config, context))
    return root / system / _safe(session) / "runs" / _safe(phase) / f"r{round_number}"


def _append_event(path: Path, event: str, context: dict[str, Any]) -> dict[str, Any]:
    phase = json.loads(path.read_text()) if path.is_file() else dict(context)
    phase.setdefault("events", []).append({
        "event": event,
        "realtime_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "yba_realtime_ns": context.get("realtime_ns"),
        "yba_monotonic_ns": context.get("monotonic_ns"),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(phase, indent=2, sort_keys=True) + "\n")
    return phase


def handle(
    event: str,
    context_path: Path,
    config_path: Path,
    *,
    controller_mode: str | None = None,
) -> dict[str, Any]:
    config = load_config(config_path)
    controller = controller_config(config, mode_override=controller_mode)
    context = json.loads(context_path.read_text(encoding="utf-8"))
    session, phase, round_number = _identity(context)
    collect_phase = _collect_phase(config, phase)
    if event in {"phase_before", "phase_after"} and not collect_phase:
        scaling = mark_controller(
            config,
            controller,
            session=session,
            phase=phase,
            active=event == "phase_before",
        )
        return {
            "event": event,
            "status": "skipped",
            "phase": phase,
            "controller": scaling,
        }
    run_dir = _run_dir(config, context, session, phase, round_number)
    phase_path = run_dir / "meta" / "phase.json"
    phase_context = _append_event(phase_path, event, context)
    if event == "server_ready":
        pids, starts = _discover_pids(config, context)
        scaling = start_controller(
            config,
            controller,
            session=session,
            system=_system_name(config, context),
            pids=pids,
            start_times=starts,
        )
        return {
            "event": event,
            "status": "recorded",
            "run_dir": str(run_dir),
            "controller": scaling,
        }
    if event == "phase_before":
        pids, starts = _discover_pids(config, context)
        phase_context["target_processes"] = [
            {"pid": pid, "start_time": starts.get(pid)} for pid in pids
        ]
        phase_path.write_text(json.dumps(phase_context, indent=2, sort_keys=True) + "\n")
        session_context = SessionContext(
            session, phase, round_number, tuple(pids), starts, run_dir
        )
        collector = CollectionSession(config, session_context)
        health = collector.start()
        for key in (
            "target_clock_offset_ns",
            "target_clock_uncertainty_ns",
            "target_clock_rtt_ns",
            "clock_sample_local_realtime_ns",
            "clock_sample_target_realtime_ns",
            "collector_ready_target_realtime_ns",
        ):
            if health.get(key) is not None:
                phase_context[key] = health[key]
        phase_context.setdefault("events", []).append({
            "event": "collector_ready",
            "realtime_ns": time.time_ns(),
            "monotonic_ns": time.monotonic_ns(),
            "target_realtime_ns": health.get("collector_ready_target_realtime_ns"),
        })
        phase_path.write_text(json.dumps(phase_context, indent=2, sort_keys=True) + "\n")
        state = {
            "config": str(config_path.resolve()),
            "context": {
                **asdict(session_context),
                "pids": list(session_context.pids),
                "pid_start_times": {str(k): v for k, v in starts.items()},
                "local_run_dir": str(run_dir),
            },
            "health": health,
        }
        _state_path(config, session, phase, round_number).write_text(
            json.dumps(state, indent=2, sort_keys=True, default=str) + "\n"
        )
        scaling = mark_controller(
            config,
            controller,
            session=session,
            phase=phase,
            active=True,
            relationship_candidates=(
                f"{collector.remote_dir}/live-candidates-latest.json"
                if controller.fine_placement_mode == "shadow" else ""
            ),
        )
        forwarding = start_kpi_forwarder(
            config,
            controller,
            session=session,
            yba_run_dir=Path(str(context["run_dir"])),
        )
        return {
            "event": event,
            "status": "ready",
            "run_dir": str(run_dir),
            "pids": pids,
            "controller": scaling,
            "kpi_forwarder": forwarding,
        }
    if event == "phase_after":
        scaling = mark_controller(
            config, controller, session=session, phase=phase, active=False
        )
        forwarding = stop_kpi_forwarder(Path(str(context["run_dir"])))
        state_path = _state_path(config, session, phase, round_number)
        if not state_path.is_file():
            raise RuntimeError(f"collection state is missing: {state_path}")
        state = json.loads(state_path.read_text())
        saved = state["context"]
        session_context = SessionContext(
            saved["session_id"], saved["phase"], int(saved["round"]),
            tuple(int(pid) for pid in saved["pids"]),
            {int(k): int(v) for k, v in saved["pid_start_times"].items()},
            Path(saved["local_run_dir"]),
        )
        collector = CollectionSession(config, session_context)
        collector.restore(state["health"])
        collector.stop(copy=True)
        phase_context = _append_event(phase_path, "collector_stopped", context)
        health = validate_raw(run_dir)
        state_path.unlink(missing_ok=True)
        return {
            "event": event,
            "status": "complete",
            "run_dir": str(run_dir),
            "health": health,
            "controller": scaling,
            "kpi_forwarder": forwarding,
        }
    if event in {"run_abort", "cleanup"}:
        scaling = stop_controller(
            config,
            controller,
            session=session,
            system=_system_name(config, context),
        )
        stopped = []
        for state_path in _state_root(config).glob(f"{_safe(session)}-*.json"):
            state = json.loads(state_path.read_text())
            saved = state["context"]
            session_context = SessionContext(
                saved["session_id"], saved["phase"], int(saved["round"]),
                tuple(int(pid) for pid in saved["pids"]),
                {int(k): int(v) for k, v in saved["pid_start_times"].items()},
                Path(saved["local_run_dir"]),
            )
            collector = CollectionSession(config, session_context)
            collector.restore(state["health"])
            collector.stop(copy=False)
            state_path.unlink(missing_ok=True)
            stopped.append(saved["phase"])
        return {
            "event": event,
            "status": "clean",
            "stopped": stopped,
            "controller": scaling,
        }
    raise ValueError(f"unsupported hook event: {event}")


def main() -> None:
    parser = argparse.ArgumentParser(description="YBA external hook for Prism Sampler")
    parser.add_argument("event", choices=["server_ready", "phase_before", "phase_after", "run_abort", "cleanup"])
    parser.add_argument("context", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--controller-mode", choices=["off", "shadow", "active"])
    args = parser.parse_args()
    config = args.config or Path(str(context_config(args.context)))
    print(json.dumps(handle(
        args.event,
        args.context,
        config,
        controller_mode=args.controller_mode,
    ), indent=2, sort_keys=True))


def context_config(context_path: Path) -> str:
    context = json.loads(context_path.read_text())
    value = context.get("sampler_config")
    if not value:
        raise ValueError("sampler config is missing; pass --config or PRISM_SAMPLER_CONFIG in YBA")
    return str(value)


if __name__ == "__main__":
    main()
