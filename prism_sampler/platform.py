from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .remote import Host


KUNPENG_PATTERNS = ("kunpeng", "huawei", "hisilicon", "0x48")


@dataclass(frozen=True)
class PlatformReport:
    host: str
    architecture: str
    kernel: str
    profile: str
    cpu_vendor: str
    cpu_model: str
    midr: str
    sockets: int
    cores: int
    logical_cpus: int
    numa_nodes: int
    btf: bool
    tracefs: bool
    bpffs: bool
    memlock: str
    pmus: tuple[str, ...]
    perf_events: tuple[str, ...]
    bpf_features: tuple[str, ...]
    numa_cpu_lists: dict[int, str]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["pmus"] = list(self.pmus)
        value["perf_events"] = list(self.perf_events)
        value["bpf_features"] = list(self.bpf_features)
        value["numa_cpu_lists"] = {str(key): item for key, item in self.numa_cpu_lists.items()}
        value["warnings"] = list(self.warnings)
        return value


def identify_profile(architecture: str, cpu_text: str) -> str:
    if architecture not in {"aarch64", "arm64"}:
        return "unknown"
    normalized = cpu_text.lower()
    if any(pattern in normalized for pattern in KUNPENG_PATTERNS):
        return "kunpeng"
    return "generic-arm64"


def parse_lscpu(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        for row in payload.get("lscpu", []):
            field = str(row.get("field", "")).rstrip(":")
            values[field] = str(row.get("data", ""))
        return values
    for line in text.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values


def _integer(values: dict[str, str], *keys: str, default: int = 0) -> int:
    for key in keys:
        if key in values:
            match = re.search(r"\d+", values[key])
            if match:
                return int(match.group())
    return default


def discover_perf_events(perf_list: str, sysfs_events: str = "") -> tuple[str, ...]:
    events: set[str] = set()
    for line in perf_list.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("List of"):
            continue
        token = stripped.split()[0].rstrip(":")
        if (
            token
            and ("[" in stripped or "/" in token)
            and not token.startswith("[")
            and not token.endswith(".")
            and re.match(r"^[A-Za-z0-9_./*=-]+$", token)
        ):
            events.add(token)
    for line in sysfs_events.splitlines():
        event = line.strip()
        if event:
            events.add(event)
    return tuple(sorted(events))


def probe(host: Host) -> PlatformReport:
    architecture = host.run("uname -m").stdout.strip()
    kernel = host.run("uname -r").stdout.strip()
    cpuinfo = host.run("cat /proc/cpuinfo", check=False).stdout
    lscpu_result = host.run("lscpu -J", check=False)
    if lscpu_result.returncode:
        lscpu_result = host.run("lscpu", check=False)
    topology = parse_lscpu(lscpu_result.stdout)
    midr = host.run(
        "cat /sys/devices/system/cpu/cpu0/regs/identification/midr_el1 2>/dev/null || true",
        check=False,
    ).stdout.strip()
    pmus = tuple(
        line for line in host.run(
            "find /sys/bus/event_source/devices -mindepth 1 -maxdepth 1 -printf '%f\\n' 2>/dev/null | sort",
            check=False,
        ).stdout.splitlines() if line
    )
    sysfs_events = host.run(
        "find /sys/bus/event_source/devices -path '*/events/*' -printf '%f\\n' 2>/dev/null | sort -u",
        check=False,
    ).stdout
    perf_list = host.run("perf list 2>/dev/null", check=False).stdout
    bpf_probe = host.run(
        "bpftool feature probe kernel unprivileged 2>/dev/null | "
        "grep -E 'program_type|map_type|is available' | head -200",
        check=False,
    ).stdout
    checks = host.run(
        "test -r /sys/kernel/btf/vmlinux; echo btf=$?; "
        "test -d /sys/kernel/tracing -o -d /sys/kernel/debug/tracing; echo tracefs=$?; "
        "test -d /sys/fs/bpf; echo bpffs=$?; ulimit -l",
        check=False,
    ).stdout.splitlines()
    node_cpu_text = host.run(
        "for n in /sys/devices/system/node/node[0-9]*; do "
        "test -r \"$n/cpulist\" && printf '%s:%s\\n' \"${n##*node}\" \"$(cat \"$n/cpulist\")\"; done",
        check=False,
    ).stdout
    node_cpu_lists = {}
    for line in node_cpu_text.splitlines():
        if ":" in line:
            node, cpus = line.split(":", 1)
            if node.isdigit():
                node_cpu_lists[int(node)] = cpus
    flags = {}
    memlock = "unknown"
    for line in checks:
        if "=" in line:
            key, value = line.split("=", 1)
            flags[key] = value == "0"
        elif line.strip():
            memlock = line.strip()
    vendor = topology.get("Vendor ID", topology.get("厂商 ID", ""))
    model = topology.get("Model name", topology.get("型号名称", ""))
    profile = identify_profile(architecture, "\n".join((vendor, model, cpuinfo, midr)))
    warnings = []
    if architecture not in {"aarch64", "arm64"}:
        warnings.append("collector bundle targets ARM64 but host architecture differs")
    if not flags.get("btf", False):
        warnings.append("kernel BTF is unavailable")
    if not flags.get("tracefs", False):
        warnings.append("tracefs is unavailable")
    if profile == "kunpeng" and not kernel.startswith("6.6"):
        warnings.append("Kunpeng full support is validated on Linux 6.6; use best-effort elsewhere")
    if not bpf_probe.strip():
        warnings.append("bpftool feature probe is unavailable; attach smoke remains authoritative")
    return PlatformReport(
        host=host.ssh,
        architecture=architecture,
        kernel=kernel,
        profile=profile,
        cpu_vendor=vendor,
        cpu_model=model,
        midr=midr,
        sockets=_integer(topology, "Socket(s)", "座"),
        cores=_integer(topology, "Core(s) per socket", "每个座的核数")
        * max(_integer(topology, "Socket(s)", "座", default=1), 1),
        logical_cpus=_integer(topology, "CPU(s)", "CPU"),
        numa_nodes=_integer(topology, "NUMA node(s)", "NUMA 节点"),
        btf=flags.get("btf", False),
        tracefs=flags.get("tracefs", False),
        bpffs=flags.get("bpffs", False),
        memlock=memlock,
        pmus=pmus,
        perf_events=discover_perf_events(perf_list, sysfs_events),
        bpf_features=tuple(line.strip() for line in bpf_probe.splitlines() if line.strip()),
        numa_cpu_lists=node_cpu_lists,
        warnings=tuple(warnings),
    )


def write_report(report: PlatformReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
