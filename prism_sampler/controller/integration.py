from __future__ import annotations

import json
import shlex
import shutil
import time
from pathlib import Path
from typing import Any

from ..config import SamplerConfig
from ..remote import Host
from .config import ControllerConfig
from .journal import write_json


def _safe(value: object) -> str:
    return "".join(character if character.isalnum() or character in "_.-" else "-" for character in str(value)).strip("-") or "unknown"


def remote_controller_dir(config: SamplerConfig, session: str) -> str:
    root = str(config.target.get("remote_root", "/tmp/prism-sampler")).rstrip("/")
    return f"{root}/controller/{_safe(session)}"


def local_controller_dir(config: SamplerConfig, system: str, session: str) -> Path:
    root = Path(config.section("experiment").get("output_root", "/data/threadState/experiments"))
    return root / _safe(system) / _safe(session) / "controller"


def _agent(config: ControllerConfig) -> str:
    return shlex.quote(config.agent_command)


def start_controller(
    config: SamplerConfig,
    controller: ControllerConfig,
    *,
    session: str,
    system: str,
    pids: list[int],
    start_times: dict[int, int],
) -> dict[str, Any]:
    if controller.mode == "off":
        return {"status": "disabled"}
    host = Host(str(config.target["host"]))
    remote = remote_controller_dir(config, session)
    existing = host.run(
        f"{_agent(controller)} status --run-dir {shlex.quote(remote)}", check=False
    )
    if existing.returncode == 0:
        try:
            status = json.loads(existing.stdout)
            if status.get("status") == "running":
                return {"status": "already_running", "remote_dir": remote}
        except json.JSONDecodeError:
            pass
    local = local_controller_dir(config, system, session)
    local.mkdir(parents=True, exist_ok=True)
    runtime = local / "runtime-config.json"
    runtime_value = {
        "schema": "prism-sampler.controller-launch.v1",
        "run_dir": remote,
        "controller": controller.to_dict(),
        "pids": pids,
        "start_times": {str(k): v for k, v in start_times.items()},
        "command_prefix": str(config.target.get("controller_command_prefix", "")),
    }
    if controller.benefit_signatures_file:
        signature_path = Path(controller.benefit_signatures_file)
        signature_value = json.loads(signature_path.read_text(encoding="utf-8"))
        runtime_value["benefit_signatures"] = signature_value.get(
            "signatures", signature_value
        )
    if controller.dynamic_model_file:
        model_path = Path(controller.dynamic_model_file)
        runtime_value["dynamic_model"] = json.loads(
            model_path.read_text(encoding="utf-8")
        )
    write_json(runtime, runtime_value)
    host.run(f"mkdir -p {shlex.quote(remote)}")
    remote_config = f"{remote}/runtime-config.json"
    host.copy_to(runtime, remote_config)
    host.start(
        f"{_agent(controller)} run --runtime-config {shlex.quote(remote_config)}",
        stdout=f"{remote}/controller.log",
        pidfile=f"{remote}/launcher.pid",
    )
    deadline = time.monotonic() + 15
    last = ""
    while time.monotonic() < deadline:
        result = host.run(
            f"{_agent(controller)} status --run-dir {shlex.quote(remote)}", check=False
        )
        last = result.stdout.strip() or result.stderr.strip()
        if result.returncode == 0:
            try:
                status = json.loads(result.stdout)
                if status.get("status") == "running":
                    return {"status": "running", "remote_dir": remote, "pids": pids}
            except json.JSONDecodeError:
                pass
        time.sleep(0.5)
    raise RuntimeError(f"controller failed to start: {last}")


def mark_controller(
    config: SamplerConfig,
    controller: ControllerConfig,
    *,
    session: str,
    phase: str,
    active: bool,
    relationship_candidates: str = "",
    telemetry_dir: str = "",
) -> dict[str, Any]:
    remote = remote_controller_dir(config, session)
    flag = "--active" if active else "--inactive"
    result = Host(str(config.target["host"])).run(
        f"{_agent(controller)} mark --run-dir {shlex.quote(remote)} "
        f"--phase {shlex.quote(phase)} {flag} "
        f"--relationship-candidates {shlex.quote(relationship_candidates)} "
        f"--telemetry-dir {shlex.quote(telemetry_dir)}",
        check=False,
    )
    if result.returncode:
        return {"status": "not_running", "detail": result.stderr.strip()}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"status": "invalid_response", "detail": result.stdout.strip()}


def stop_controller(
    config: SamplerConfig,
    controller: ControllerConfig,
    *,
    session: str,
    system: str,
) -> dict[str, Any]:
    remote = remote_controller_dir(config, session)
    host = Host(str(config.target["host"]))
    result = host.run(
        f"{_agent(controller)} stop --run-dir {shlex.quote(remote)} --timeout 120",
        check=False,
    )
    local = local_controller_dir(config, system, session)
    if host.run(f"test -d {shlex.quote(remote)}", check=False).returncode == 0:
        incoming = local.with_name(f".{local.name}.incoming")
        shutil.rmtree(incoming, ignore_errors=True)
        host.copy_from(remote, incoming, recursive=True)
        if incoming.exists():
            shutil.rmtree(local, ignore_errors=True)
            incoming.rename(local)
    try:
        value = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        value = {"detail": result.stdout.strip() or result.stderr.strip()}
    value["local_dir"] = str(local)
    return value
