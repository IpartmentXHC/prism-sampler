from __future__ import annotations

import json
import shutil
import shlex
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..config import SamplerConfig
from ..platform import PlatformReport, probe
from ..remote import Host
from ..sidecars import discover_uncore_events, select_core_events


def measure_clock_offset(host: Host, samples: int = 5) -> dict[str, int]:
    """Estimate target minus local realtime using the lowest-RTT SSH sample."""
    if host.is_local:
        now = time.time_ns()
        return {
            "target_clock_offset_ns": 0,
            "target_clock_uncertainty_ns": 0,
            "target_clock_rtt_ns": 0,
            "clock_sample_local_realtime_ns": now,
            "clock_sample_target_realtime_ns": now,
        }
    process = subprocess.Popen(
        ["ssh", "-o", "BatchMode=yes", host.ssh, "bash", "--noprofile", "--norc"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    assert process.stdin is not None and process.stdout is not None
    measurements = []
    try:
        process.stdin.write("printf 'prism-clock-ready\\n'\n")
        process.stdin.flush()
        if process.stdout.readline().strip() != "prism-clock-ready":
            raise RuntimeError("target clock probe did not become ready")
        for _ in range(samples):
            local_before = time.time_ns()
            process.stdin.write("date +%s%N\n")
            process.stdin.flush()
            target = int(process.stdout.readline().strip())
            local_after = time.time_ns()
            midpoint = (local_before + local_after) // 2
            measurements.append({
                "target_clock_offset_ns": target - midpoint,
                "target_clock_uncertainty_ns": (local_after - local_before) // 2,
                "target_clock_rtt_ns": local_after - local_before,
                "clock_sample_local_realtime_ns": midpoint,
                "clock_sample_target_realtime_ns": target,
            })
    finally:
        try:
            process.stdin.write("exit\n")
            process.stdin.flush()
        except BrokenPipeError:
            pass
        process.wait(timeout=10)
    return min(measurements, key=lambda row: row["target_clock_rtt_ns"])


@dataclass(frozen=True)
class SessionContext:
    session_id: str
    phase: str
    round: int
    pids: tuple[int, ...]
    pid_start_times: dict[int, int]
    local_run_dir: Path


@dataclass
class PluginStatus:
    name: str
    requested: bool
    required: bool
    available: bool = False
    started: bool = False
    healthy: bool = False
    error: str = ""


class CollectionSession:
    def __init__(self, config: SamplerConfig, context: SessionContext):
        self.config = config
        self.context = context
        self.host = Host(config.target["host"])
        self.profile = str(config.sampling.get("profile", "policy"))
        profile = config.values["sampling_profiles"][self.profile]
        self.required = set(profile.get("required", []))
        self.requested = list(dict.fromkeys(profile.get("required", []) + profile.get("optional", [])))
        self.status = {
            name: PluginStatus(name, True, name in self.required) for name in self.requested
        }
        root = config.target["remote_root"].rstrip("/")
        self.remote_dir = f"{root}/{context.session_id}/{context.phase}/r{context.round}"
        self.report: PlatformReport | None = None
        self.collector_cli_mode = "unknown"
        self.clock: dict[str, int] = {}
        self.collector_ready_target_realtime_ns: int | None = None

    def restore(self, health: dict[str, Any]) -> None:
        self.collector_cli_mode = str(health.get("collector_cli_mode", "unknown"))
        self.clock = {
            key: int(health[key])
            for key in (
                "target_clock_offset_ns",
                "target_clock_uncertainty_ns",
                "target_clock_rtt_ns",
                "clock_sample_local_realtime_ns",
                "clock_sample_target_realtime_ns",
            )
            if health.get(key) is not None
        }
        if health.get("collector_ready_target_realtime_ns") is not None:
            self.collector_ready_target_realtime_ns = int(
                health["collector_ready_target_realtime_ns"]
            )
        for row in health.get("plugins", []):
            name = str(row.get("name", ""))
            if name in self.status:
                for key in ("available", "started", "healthy", "error"):
                    if key in row:
                        setattr(self.status[name], key, row[key])

    @property
    def sudo(self) -> str:
        return str(self.config.target.get("sudo", "")).strip()

    def _prefix(self, command: str) -> str:
        return f"{self.sudo} {command}".strip()

    def _pidfile(self, plugin: str) -> str:
        return f"{self.remote_dir}/{plugin}.pid"

    def _log(self, plugin: str) -> str:
        return f"{self.remote_dir}/{plugin}.log"

    def _validate_pids(self) -> None:
        for pid in self.context.pids:
            result = self.host.run(
                f"awk '{{print $22}}' /proc/{pid}/stat 2>/dev/null", check=False
            )
            if result.returncode or not result.stdout.strip():
                raise RuntimeError(f"target PID is not running: {pid}")
            expected = self.context.pid_start_times.get(pid)
            actual = int(result.stdout.strip())
            if expected is not None and expected != actual:
                raise RuntimeError(
                    f"target PID start time changed: pid={pid} expected={expected} actual={actual}"
                )

    def _start_process(self, plugin: str, command: str) -> None:
        status = self.status[plugin]
        status.available = True
        try:
            self.host.start(command, stdout=self._log(plugin), pidfile=self._pidfile(plugin))
            time.sleep(0.5)
            alive = self.host.run(
                f"pid=$(cat {shlex.quote(self._pidfile(plugin))}); kill -0 \"$pid\"",
                check=False,
            ).returncode == 0
            if not alive:
                detail = self.host.run(
                    f"tail -80 {shlex.quote(self._log(plugin))}", check=False
                ).stdout.strip()
                raise RuntimeError(detail or "collector exited during startup")
            status.started = status.healthy = True
        except Exception as exc:
            status.error = str(exc)
            if status.required:
                raise

    def _start_prism(self) -> None:
        collector = self.config.collector
        binary = shlex.quote(str(collector["binary"]))
        runtime = str(collector.get("runtime_lib", ""))
        env = f"LD_LIBRARY_PATH={shlex.quote(runtime)} " if runtime else ""
        pids = ",".join(str(pid) for pid in self.context.pids)
        args = (
            f"--machine-id 1 --pids {pids} --backend duckdb "
            f"--duckdb-directory {shlex.quote(self.remote_dir)} --duckdb-file collector.db3"
        )
        help_text = self.host.run(f"env {env}{binary} --help", check=False).stdout
        if "--platform-profile" in help_text and "--subsystems" in help_text:
            self.collector_cli_mode = "capability-aware"
            strict = bool(self.report and self.report.kernel.startswith("6.6"))
            required = "taskstats,vfs,futex,iowait,net" if strict else "taskstats,futex,iowait"
            requested = "taskstats,vfs,futex,iowait,aio,mux,net,discovery"
            args += (
                f" --platform-profile "
                f"{'kunpeng' if self.report and self.report.profile == 'kunpeng' else 'generic-arm64'}"
                f" --subsystems {requested} --required-subsystems {required}"
            )
            if not strict:
                args += " --best-effort"
        else:
            self.collector_cli_mode = "legacy"
        self._start_process("prism", self._prefix(f"env {env}{binary} {args}"))
        time.sleep(float(self.config.collector.get("attach_wait_seconds", 12)))
        self._validate_pids()

    def _start_snapshot(self) -> None:
        agent = str(self.config.target.get("agent_command", "prism-sampler-agent"))
        args = " ".join(f"--pid {pid}" for pid in self.context.pids)
        interval = float(self.config.sampling.get("interval_seconds", 10))
        output = shlex.quote(f"{self.remote_dir}/system-pressure.jsonl")
        self._start_process(
            "snapshot", f"{shlex.quote(agent)} --output {output} {args} --interval {interval}"
        )

    def _start_perf_core(self) -> None:
        assert self.report is not None
        candidates = self.config.values["platform"].get("core_events", [])
        events = select_core_events(candidates, self.report.perf_events)
        status = self.status["perf-core"]
        if not events:
            status.error = "no configured core PMU events are available"
            return
        interval = int(float(self.config.sampling.get("interval_seconds", 10)) * 1000)
        pids = ",".join(str(pid) for pid in self.context.pids)
        event_arg = shlex.quote(",".join(events))
        output = shlex.quote(f"{self.remote_dir}/perf-core.csv")
        self.host.run(f"date +%s.%N > {self.remote_dir}/perf-core.csv.start")
        command = self._prefix(
            f"perf stat -I {interval} -x, -p {pids} -e {event_arg} -o {output} -- sleep 86400"
        )
        self._start_process("perf-core", command)

    def _start_perf_uncore(self) -> None:
        patterns = self.config.values["platform"].get("uncore_globs", [])
        event_names = self.config.values["platform"].get("uncore_event_names", [])
        events = discover_uncore_events(self.host, patterns, event_names)
        status = self.status["perf-uncore"]
        if not events:
            status.error = "no configured uncore PMU events are available"
            return
        interval = int(float(self.config.sampling.get("interval_seconds", 10)) * 1000)
        output = shlex.quote(f"{self.remote_dir}/perf-uncore.csv")
        self.host.run(f"date +%s.%N > {self.remote_dir}/perf-uncore.csv.start")
        command = self._prefix(
            f"perf stat -a -I {interval} -x, -e {shlex.quote(','.join(events))} "
            f"-o {output} -- sleep 86400"
        )
        self._start_process("perf-uncore", command)

    def _start_spe(self) -> None:
        assert self.report is not None
        status = self.status["arm-spe"]
        if "arm_spe_0" not in self.report.pmus:
            status.error = "arm_spe_0 is unavailable"
            return
        pids = ",".join(str(pid) for pid in self.context.pids)
        output = shlex.quote(f"{self.remote_dir}/arm-spe.data")
        self._start_process(
            "arm-spe", self._prefix(f"perf record -e arm_spe_0// -p {pids} -o {output} -- sleep 86400")
        )

    def start(self) -> dict[str, Any]:
        self.host.run(f"mkdir -p {shlex.quote(self.remote_dir)}")
        self._validate_pids()
        self.report = probe(self.host)
        starters = {
            "prism": self._start_prism,
            "snapshot": self._start_snapshot,
            "perf-core": self._start_perf_core,
            "perf-uncore": self._start_perf_uncore,
            "arm-spe": self._start_spe,
        }
        try:
            for name in self.requested:
                if name == "phase-marker":
                    self.status[name].available = self.status[name].started = self.status[name].healthy = True
                elif name in starters:
                    starters[name]()
            missing = [name for name in self.required if not self.status[name].healthy]
            if missing:
                raise RuntimeError("required collectors are unhealthy: " + ", ".join(missing))
            self.clock = measure_clock_offset(self.host)
            self.collector_ready_target_realtime_ns = (
                time.time_ns() + self.clock["target_clock_offset_ns"]
            )
        except Exception:
            self.stop(copy=False)
            raise
        health = self.health("running")
        self._write_remote_json("capabilities.json", health)
        return health

    def _write_remote_json(self, name: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, sort_keys=True)
        command = "python3 -c " + shlex.quote(
            "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(sys.argv[2]+'\\n')"
        )
        self.host.run(
            f"{command} {shlex.quote(self.remote_dir + '/' + name)} {shlex.quote(payload)}"
        )

    def health(self, state: str) -> dict[str, Any]:
        health = {
            "schema": "prism-sampler.health.v1",
            "session_id": self.context.session_id,
            "phase": self.context.phase,
            "round": self.context.round,
            "state": state,
            "realtime_ns": time.time_ns(),
            "monotonic_ns": time.monotonic_ns(),
            "profile": self.profile,
            "collector_cli_mode": self.collector_cli_mode,
            "pids": list(self.context.pids),
            "platform": self.report.to_dict() if self.report else None,
            "plugins": [asdict(self.status[name]) for name in self.requested],
        }
        health.update(self.clock)
        health["collector_ready_target_realtime_ns"] = (
            self.collector_ready_target_realtime_ns
        )
        return health

    def stop(self, *, copy: bool = True) -> dict[str, Any]:
        for name in reversed(self.requested):
            if name == "phase-marker":
                continue
            self.host.stop(
                self._pidfile(name),
                signal="INT",
                timeout_seconds=int(self.config.collector.get("stop_timeout_seconds", 30)),
                command_prefix=self.sudo,
            )
        expected = {
            "prism": "collector.db3",
            "snapshot": "system-pressure.jsonl",
            "perf-core": "perf-core.csv",
            "perf-uncore": "perf-uncore.csv",
            "arm-spe": "arm-spe.data",
        }
        for name, filename in expected.items():
            status = self.status.get(name)
            if not status or not status.started:
                continue
            exists = self.host.run(
                f"test -s {shlex.quote(self.remote_dir + '/' + filename)}", check=False
            ).returncode == 0
            if not exists:
                status.healthy = False
                status.error = f"expected artifact is missing or empty: {filename}"
        health = self.health("stopped")
        try:
            self._write_remote_json("health.json", health)
        except Exception:
            pass
        if copy:
            raw = self.context.local_run_dir / "raw"
            raw.mkdir(parents=True, exist_ok=True)
            self.host.copy_from(self.remote_dir, raw, recursive=True)
            nested = raw / Path(self.remote_dir).name
            if nested.is_dir():
                for source in nested.iterdir():
                    destination = raw / source.name
                    if destination.exists():
                        if destination.is_dir():
                            shutil.rmtree(destination)
                        else:
                            destination.unlink()
                    source.replace(destination)
                nested.rmdir()
        return health
