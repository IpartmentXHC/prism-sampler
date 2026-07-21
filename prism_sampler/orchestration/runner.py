from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ..config import SamplerConfig, validate_config
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
    collector_check = host.run(f"{env}{collector} --help >/dev/null", check=False)
    agent = shlex.quote(str(config.target.get("agent_command", "prism-sampler-agent")))
    agent_check = host.run(f"{agent} --help >/dev/null", check=False)
    perf_check = host.run("command -v perf >/dev/null", check=False)
    remote_ns = int(host.run("date +%s%N").stdout.strip())
    local_ns = time.time_ns()
    result = {
        "schema": "prism-sampler.preflight.v1",
        "config_sha256": config.digest(),
        "platform": report.to_dict(),
        "checks": {
            "collector": collector_check.returncode == 0,
            "agent": agent_check.returncode == 0,
            "perf": perf_check.returncode == 0,
        },
        "clock_offset_ns": remote_ns - local_ns,
    }
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
    hook = (
        f"{shlex.quote(sys.executable)} -m prism_sampler.hooks "
        f"--config {shlex.quote(str(config.source))}"
    )
    env.update({
        "ENABLE_METRICS": "0",
        "ENABLE_THREAD_CLUSTER": "0",
        "ENABLE_EXTERNAL_HOOK": "1",
        "EXTERNAL_HOOK_COMMAND": hook,
        "PRISM_SAMPLER_CONFIG": str(config.source),
    })
    return subprocess.run(
        [str(yba), "scenario", "--config", str(yba_config), "--scenario", str(scenario)],
        cwd=yba_root,
        env=env,
        check=False,
    ).returncode
