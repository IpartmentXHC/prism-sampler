from __future__ import annotations

import hashlib
import json
import platform
import shlex
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .remote import Host


REPO_ROOT = Path(__file__).resolve().parents[1]
PORTABLE_ROOT = REPO_ROOT / "deploy" / "portable" / "arm64"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True, capture_output=True, check=True
    ).stdout.strip()


def build_bundle(
    output: Path,
    *,
    collector: Path | None = None,
    source_host: str | None = None,
    source_root: str = "/home/xhc/prism-threads",
    runtime_libs: list[Path] | None = None,
) -> Path:
    if bool(collector) == bool(source_host):
        raise ValueError("provide exactly one of collector or source_host")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="prism-arm64-") as temporary:
        stage = Path(temporary) / "prism-collector-arm64"
        (stage / "bin").mkdir(parents=True)
        (stage / "lib").mkdir()
        target = stage / "bin" / "metric-collector"
        source_kernel = platform.release()
        build_glibc = subprocess.run(
            ["ldd", "--version"], text=True, capture_output=True
        ).stdout.splitlines()[0]
        if source_host:
            subprocess.run(
                ["scp", f"{source_host}:{source_root}/prism/target/release/metric-collector", str(target)],
                check=True,
            )
            source_kernel = Host(source_host).run("uname -r").stdout.strip()
            build_glibc = Host(source_host).run("ldd --version | head -1").stdout.strip()
            for name in ("libstdc++.so.6", "libgcc_s.so.1"):
                result = subprocess.run(
                    ["scp", f"{source_host}:{source_root}/runtime-lib/{name}", str(stage / "lib" / name)],
                    check=False,
                )
                if result.returncode:
                    (stage / "lib" / name).unlink(missing_ok=True)
        else:
            shutil.copy2(collector, target)
            for library in runtime_libs or []:
                shutil.copy2(library, stage / "lib" / library.name)
        target.chmod(0o755)
        file_output = subprocess.run(["file", str(target)], text=True, capture_output=True, check=True).stdout
        if "aarch64" not in file_output.lower() and "arm64" not in file_output.lower():
            raise ValueError(f"collector is not ARM64: {file_output.strip()}")
        for name in ("prismctl", "capability-probe", "README.md"):
            shutil.copy2(PORTABLE_ROOT / name, stage / name)
        package_root = stage / "python" / "prism_sampler"
        shutil.copytree(REPO_ROOT / "prism_sampler", package_root)
        agent = stage / "bin" / "prism-sampler-agent"
        agent.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "ROOT=$(cd \"$(dirname \"$0\")/..\" && pwd)\n"
            "export PYTHONPATH=\"$ROOT/python${PYTHONPATH:+:$PYTHONPATH}\"\n"
            "exec python3 -m prism_sampler.agent \"$@\"\n",
            encoding="utf-8",
        )
        agent.chmod(0o755)
        (stage / "prismctl").chmod(0o755)
        (stage / "capability-probe").chmod(0o755)
        manifest = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "git_commit": _git_commit(),
            "build_architecture": "aarch64",
            "build_kernel": source_kernel,
            "build_glibc": build_glibc,
            "collector_sha256": _sha256(target),
            "profiles": ["kunpeng", "generic-arm64"],
            "full_support_kernel": "6.6",
            "best_effort_kernel": "5.10",
            "agent": "bin/prism-sampler-agent",
        }
        (stage / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        checksums = []
        for path in sorted(item for item in stage.rglob("*") if item.is_file()):
            if path.name != "manifest.sha256":
                checksums.append(f"{_sha256(path)}  {path.relative_to(stage)}")
        (stage / "manifest.sha256").write_text("\n".join(checksums) + "\n", encoding="utf-8")
        with tarfile.open(output, "w:gz") as archive:
            archive.add(stage, arcname=stage.name)
    output.with_suffix(output.suffix + ".sha256").write_text(
        f"{_sha256(output)}  {output.name}\n", encoding="utf-8"
    )
    return output


def install_bundle(host: Host, bundle: Path, install_dir: str) -> None:
    remote_archive = f"/tmp/{bundle.name}"
    host.copy_to(bundle, remote_archive)
    expected = _sha256(bundle)
    actual = host.run(f"sha256sum {remote_archive}").stdout.split()[0]
    if actual != expected:
        raise RuntimeError("remote bundle checksum mismatch")
    install = shlex.quote(install_dir)
    archive = shlex.quote(remote_archive)
    host.run(
        f"mkdir -p {install} && tar -xzf {archive} -C {install} --strip-components=1 "
        f"&& chmod +x {install}/prismctl {install}/capability-probe "
        f"{install}/bin/metric-collector {install}/bin/prism-sampler-agent"
    )


def smoke_bundle(host: Host, install_dir: str, *, best_effort: bool = False, sudo: str = "") -> None:
    mode = "--best-effort" if best_effort else "--strict-6.6"
    env = f"PRISM_SUDO={json.dumps(sudo)} " if sudo else ""
    host.run(f"{env}{install_dir}/prismctl smoke {mode} --duration 20")
