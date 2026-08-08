from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Any

from .remote import Host
from .controller.metrics import format_cpu_list, parse_cpu_list


def _write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _masks(host: Host, pid: int) -> dict[str, str]:
    command = f"""
for task in /proc/{pid}/task/*; do
  test -r "$task/status" || continue
  tid=${{task##*/}}
  mask=$(awk '/^Cpus_allowed_list:/ {{print $2}}' "$task/status")
  printf '%s %s\n' "$tid" "$mask"
done
"""
    result = host.run(command, check=False)
    return {
        fields[0]: fields[1]
        for line in result.stdout.splitlines()
        if len(fields := line.split()) == 2
    }


def _ctl_command(ctl: str) -> str:
    library_path = os.environ.get("AFFINITYGRAPH_LIBRARY_PATH", "")
    prefix = (
        f"/lib/ld-linux-aarch64.so.1 --library-path {shlex.quote(library_path)} "
        if library_path else ""
    )
    return prefix + shlex.quote(ctl)


def _target_identity(
    host: Host,
    context: dict[str, Any],
    root: Path,
    target_exe: str,
) -> tuple[int, int]:
    identity_path = root / "target-identity.json"
    identities = [
        (int(row.get("pid", 0)), int(row.get("start_time", 0)), False)
        for row in context.get("target_processes", [])
        if int(row.get("pid", 0)) and int(row.get("start_time", 0))
    ]
    if identity_path.is_file():
        cached = json.loads(identity_path.read_text(encoding="utf-8"))
        identities.insert(0, (int(cached["pid"]), int(cached["start_time"]), True))

    validation_errors = []
    for pid, start_time, previously_validated in identities:
        expected_exe = (
            f'test "$(readlink -f /proc/{pid}/exe 2>/dev/null || true)" = '
            f"{shlex.quote(target_exe)} && "
            if target_exe and not previously_validated else ""
        )
        command = (
            f"test -r /proc/{pid}/stat && {expected_exe}"
            f"test \"$(awk '{{print $22}}' /proc/{pid}/stat)\" = {start_time}"
        )
        for attempt in range(8):
            probe = host.run(command, check=False)
            if probe.returncode == 0:
                _write(identity_path, {"pid": pid, "start_time": start_time})
                return pid, start_time
            validation_errors.append({
                "pid": pid,
                "returncode": probe.returncode,
                "stderr": probe.stderr.strip(),
            })
            if attempt < 7:
                time.sleep(0.25)

    if not target_exe:
        raise RuntimeError(f"expected one live ClickHouse identity, got {identities}")
    probe = (
        "for task in /proc/[0-9]*; do "
        "exe=$(readlink -f \"$task/exe\" 2>/dev/null || true); "
        f"test \"$exe\" = {shlex.quote(target_exe)} || continue; "
        "pid=${task##*/}; "
        "start=$(awk '{print $22}' \"$task/stat\" 2>/dev/null || true); "
        "test -n \"$start\" && printf '%s %s\\n' \"$pid\" \"$start\"; "
        "done"
    )
    command = (
        f"SUDO_ASKPASS={shlex.quote('/home/xhc/ExperScript/doris-bench/askpass.sh')} "
        f"sudo -A sh -c {shlex.quote(probe)}"
    )
    for attempt in range(8):
        candidates = host.run(command, check=False)
        if candidates.returncode == 0:
            break
        if attempt < 7:
            time.sleep(0.25)
    discovered = [
        (int(fields[0]), int(fields[1]))
        for line in candidates.stdout.splitlines()
        if len(fields := line.split()) == 2 and all(field.isdigit() for field in fields)
    ]
    if len(discovered) != 1:
        raise RuntimeError(
            f"expected one ClickHouse identity, got {discovered}; "
            f"scan_returncode={candidates.returncode}; "
            f"scan_stderr={candidates.stderr.strip()!r}; validations={validation_errors[-8:]}"
        )
    pid, start_time = discovered[0]
    _write(identity_path, {"pid": pid, "start_time": start_time})
    return pid, start_time


def _status(host: Host, ctl: str, socket: str, *, attempts: int = 40) -> dict[str, Any]:
    command = f"{_ctl_command(ctl)} status --socket {shlex.quote(socket)}"
    for _ in range(attempts):
        result = host.run(command, check=False)
        if result.returncode == 0 and result.stdout.strip().startswith("{"):
            return json.loads(result.stdout)
        time.sleep(0.25)
    raise RuntimeError(f"AffinityGraph control socket unavailable: {socket}")


def _active_ready(status: dict[str, Any], masks: dict[str, str]) -> bool:
    planned_masks = status.get("planned_masks") or {}
    assignments = status.get("planned_assignments") or {}
    if not status.get("policy_armed") or not status.get("active_effective"):
        return False
    if planned_masks:
        expected = len(planned_masks)
        if (
            int(status.get("active_cohort_threads", expected)) != expected
            or int(status.get("pinned_threads", expected)) != expected
        ):
            return False
        return all(
            format_cpu_list(parse_cpu_list(str(masks.get(str(tid), ""))))
            == format_cpu_list(parse_cpu_list(str(mask)))
            for tid, mask in planned_masks.items()
        )
    cohort_threads = int(status.get("active_cohort_threads", 0))
    return bool(
        assignments
        and cohort_threads > 0
        and len(assignments) == cohort_threads
        and int(status.get("pinned_threads", 0)) == cohort_threads
        and all(masks.get(str(tid)) == str(cpu) for tid, cpu in assignments.items())
    )


def _requires_active_readiness(
    treatment: str,
    event: str,
    phase: str,
    measurement_phases: set[str] | str,
    active_ready_phase: str,
) -> bool:
    phases = (
        {measurement_phases}
        if isinstance(measurement_phases, str) else measurement_phases
    )
    return bool(
        treatment == "active"
        and event == "phase_before"
        and (phase in phases or phase == active_ready_phase)
    )


def _measurement_phases() -> list[str]:
    raw = os.environ.get("AFFINITYGRAPH_MEASUREMENT_PHASES", "")
    phases = [value.strip() for value in raw.split(",") if value.strip()]
    if phases:
        return phases
    single = os.environ.get("AFFINITYGRAPH_MEASUREMENT_PHASE", "")
    return [single] if single else []


def _require_healthy(status: dict[str, Any], treatment: str) -> None:
    if (
        not status.get("bpf")
        or not status.get("bpf_health_valid", True)
        or status.get("collector_degraded")
        or status.get("fatal_error")
    ):
        raise RuntimeError("AffinityGraph required BPF is not healthy")
    if status.get("effective_mode") != treatment:
        raise RuntimeError(
            f"AffinityGraph mode mismatch: requested={treatment} "
            f"actual={status.get('effective_mode')}"
        )
    if status.get("bpf_window_ready") and float(status.get("bpf_window_loss_ratio", 0)) >= 0.01:
        raise RuntimeError("AffinityGraph 30-second BPF loss threshold exceeded")


def _relay(event: str, context_path: Path) -> dict[str, Any]:
    context = json.loads(context_path.read_text(encoding="utf-8"))
    root = Path(os.environ["AFFINITYGRAPH_HOOK_ROOT"])
    phase = str(context.get("phase") or context.get("label") or "server")
    request = root / f"request-{event}-{phase}.json"
    response = root / f"response-{event}-{phase}.json"
    response.unlink(missing_ok=True)
    _write(request, {"event": event, "context": context})
    deadline = time.monotonic() + float(
        os.environ.get("AFFINITYGRAPH_RELAY_TIMEOUT_SECONDS", "180")
    )
    while time.monotonic() < deadline:
        if response.is_file():
            return json.loads(response.read_text(encoding="utf-8"))
        time.sleep(0.25)
    raise RuntimeError(f"AffinityGraph hook relay timed out: {event}/{phase}")


def handle(event: str, context_path: Path) -> dict[str, Any]:
    context = json.loads(context_path.read_text(encoding="utf-8"))
    treatment = os.environ.get("AFFINITYGRAPH_TREATMENT", "baseline")
    root = Path(os.environ["AFFINITYGRAPH_HOOK_ROOT"])
    phase = str(context.get("phase") or context.get("label") or "server")
    measurement_phases = _measurement_phases()
    measurement_set = set(measurement_phases)
    measurement_phase = measurement_phases[0] if measurement_phases else ""
    final_measurement_phase = measurement_phases[-1] if measurement_phases else ""
    active_ready_phase = os.environ.get(
        "AFFINITYGRAPH_ACTIVE_READY_PHASE", measurement_phase
    )
    output = root / f"{event}-{phase}.json"
    value: dict[str, Any] = {"event": event, "treatment": treatment, "context": context}
    if treatment == "baseline":
        value["status"] = "not_applicable"
        _write(output, value)
        return value

    host = Host(str(context["server_host"]))
    target_exe = os.environ.get("AFFINITYGRAPH_TARGET_EXE", "")
    pid, start_time = _target_identity(host, context, root, target_exe)
    context["target_processes"] = [{"pid": pid, "start_time": start_time}]
    resolved_context = root / f"resolved-{event}-context.json"
    _write(resolved_context, context)
    prism_config = os.environ.get("AFFINITYGRAPH_PRISM_CONFIG")
    if prism_config:
        from .hooks import handle as prism_handle

        try:
            value["prism"] = prism_handle(
                event, resolved_context, Path(prism_config), controller_mode="off"
            )
        except Exception as error:
            value["prism"] = {
                "status": "warning",
                "error": f"{type(error).__name__}: {error}",
            }
    ctl = os.environ["AFFINITYGRAPH_CTL"]
    socket = os.environ["AFFINITYGRAPH_SOCKET"]
    value.update(pid=pid, runtime_status=_status(host, ctl, socket), masks=_masks(host, pid))
    status = value["runtime_status"]
    try:
        _require_healthy(status, treatment)
    except RuntimeError:
        _write(output, value)
        raise
    # active 需要先在有负载的 placement phase 中形成稳定 domain，再进入正式
    # warmup。measurement 开始前仍重复校验一次，防止两阶段之间 domain 失效。
    if _requires_active_readiness(
        treatment, event, phase, measurement_set, active_ready_phase
    ):
        timeout = float(os.environ.get("AFFINITYGRAPH_ACTIVE_READY_TIMEOUT_SECONDS", "60"))
        started = time.monotonic()
        attempts = 1
        while not _active_ready(status, value["masks"]):
            if time.monotonic() - started >= timeout:
                value.update(
                    active_measurement_ready=False,
                    active_ready_phase=phase,
                    active_ready_attempts=attempts,
                    active_ready_wait_seconds=time.monotonic() - started,
                )
                _write(output, value)
                if os.environ.get(
                    "AFFINITYGRAPH_ACTIVE_READINESS_REQUIRED", "true"
                ).lower() == "true":
                    raise RuntimeError(
                        "AffinityGraph active placement was not effective before measurement"
                    )
                break
            time.sleep(1)
            status = _status(host, ctl, socket)
            value["runtime_status"] = status
            value["masks"] = _masks(host, pid)
            attempts += 1
            try:
                _require_healthy(status, treatment)
            except RuntimeError:
                value.update(
                    active_measurement_ready=False,
                    active_ready_phase=phase,
                    active_ready_attempts=attempts,
                    active_ready_wait_seconds=time.monotonic() - started,
                )
                _write(output, value)
                raise
        if _active_ready(status, value["masks"]):
            value.update(
                active_measurement_ready=True,
                active_ready_phase=phase,
                active_ready_attempts=attempts,
                active_ready_wait_seconds=time.monotonic() - started,
            )
    if event == "phase_after" and phase == final_measurement_phase:
        command = f"{_ctl_command(ctl)} pause --socket {shlex.quote(socket)}"
        pause = host.run(command, check=False)
        value["pause_returncode"] = pause.returncode
        value["pause_stdout"] = pause.stdout.strip()
        value["pause_stderr"] = pause.stderr.strip()
        value["restored_masks"] = _masks(host, pid)
        value["post_pause_status"] = _status(host, ctl, socket)
        value["restored"] = bool(
            pause.returncode == 0
            and not value["post_pause_status"].get("planned_threads")
            and int(value["post_pause_status"].get("restore_failed", 0)) == 0
        )
        if not value["restored"]:
            _write(output, value)
            raise RuntimeError("AffinityGraph pause/restore failed")
    _write(output, value)
    if phase == measurement_phase and event == "phase_before":
        _write(root / "phase_before.json", value)
    if phase == final_measurement_phase and event == "phase_after":
        _write(root / f"{event}.json", value)
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="YBA lifecycle hook for AffinityGraph experiments")
    parser.add_argument("event", choices=["server_ready", "phase_before", "phase_after", "run_abort", "cleanup"])
    parser.add_argument("context", type=Path)
    args = parser.parse_args()
    if os.environ.get("AFFINITYGRAPH_RELAY") == "1":
        response = _relay(args.event, args.context)
        if response.get("stdout"):
            print(response["stdout"], end="")
        if response.get("stderr"):
            print(response["stderr"], end="", file=sys.stderr)
        raise SystemExit(int(response.get("returncode", 1)))
    if args.event in {"run_abort", "cleanup"}:
        context = json.loads(args.context.read_text(encoding="utf-8"))
        prism_config = os.environ.get("AFFINITYGRAPH_PRISM_CONFIG")
        if prism_config:
            from .hooks import handle as prism_handle

            try:
                value = prism_handle(
                    args.event, args.context, Path(prism_config), controller_mode="off"
                )
            except Exception as error:
                value = {
                    "event": args.event, "status": "warning",
                    "error": f"{type(error).__name__}: {error}",
                }
        else:
            value = {"event": args.event, "status": "clean"}
        if os.environ.get("AFFINITYGRAPH_TREATMENT", "baseline") != "baseline":
            host_name = str(
                context.get("server_host")
                or os.environ.get("AFFINITYGRAPH_SERVER_HOST", "")
            )
            if not host_name:
                raise RuntimeError("AffinityGraph cleanup context has no server host")
            host = Host(host_name)
            ctl = os.environ["AFFINITYGRAPH_CTL"]
            socket = os.environ["AFFINITYGRAPH_SOCKET"]
            pause = host.run(
                f"{_ctl_command(ctl)} pause --socket {shlex.quote(socket)}",
                check=False,
            )
            value["affinity_pause_returncode"] = pause.returncode
            value["affinity_pause_stdout"] = pause.stdout.strip()
    else:
        value = handle(args.event, args.context)
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
