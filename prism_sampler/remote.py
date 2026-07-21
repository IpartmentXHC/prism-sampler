from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class CommandResult:
    command: str
    returncode: int
    stdout: str
    stderr: str

    def check(self) -> "CommandResult":
        if self.returncode:
            detail = self.stderr.strip() or self.stdout.strip()
            raise RuntimeError(f"command failed ({self.returncode}): {self.command}\n{detail}")
        return self


class Host:
    def __init__(self, ssh: str | None = None):
        self.ssh = ssh or "local"

    @property
    def is_local(self) -> bool:
        return self.ssh in {"", "local", "localhost"}

    def run(
        self,
        command: str,
        *,
        check: bool = True,
        timeout: float | None = None,
        cwd: str | None = None,
    ) -> CommandResult:
        if cwd:
            command = f"cd {shlex.quote(cwd)} && {command}"
        argv: Sequence[str] = ["bash", "-lc", command]
        if not self.is_local:
            argv = ["ssh", "-o", "BatchMode=yes", self.ssh, "bash", "-lc", shlex.quote(command)]
        proc = subprocess.run(argv, text=True, capture_output=True, timeout=timeout)
        result = CommandResult(command, proc.returncode, proc.stdout, proc.stderr)
        return result.check() if check else result

    def copy_from(self, remote_path: str, local_path: Path, *, recursive: bool = False) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if self.is_local:
            args = ["cp"]
            if recursive:
                args.append("-a")
            args.extend([remote_path, str(local_path)])
        else:
            args = ["scp"]
            if recursive:
                args.append("-r")
            args.extend([f"{self.ssh}:{remote_path}", str(local_path)])
        subprocess.run(args, check=True)

    def copy_to(self, local_path: Path, remote_path: str) -> None:
        if self.is_local:
            subprocess.run(["cp", "-a", str(local_path), remote_path], check=True)
        else:
            subprocess.run(["scp", str(local_path), f"{self.ssh}:{remote_path}"], check=True)

    def start(self, command: str, *, stdout: str, pidfile: str) -> int:
        wrapped = (
            f"mkdir -p {shlex.quote(str(Path(stdout).parent))} {shlex.quote(str(Path(pidfile).parent))}; "
            f"nohup setsid bash -lc {shlex.quote(command)} >{shlex.quote(stdout)} 2>&1 </dev/null & "
            f"pid=$!; printf '%s\\n' \"$pid\" >{shlex.quote(pidfile)}; printf '%s\\n' \"$pid\""
        )
        return int(self.run(wrapped).stdout.strip().splitlines()[-1])

    def stop(
        self,
        pidfile: str,
        *,
        signal: str = "INT",
        timeout_seconds: int = 20,
        command_prefix: str = "",
    ) -> None:
        kill = f"{command_prefix} kill".strip()
        script = f"""
if test -r {shlex.quote(pidfile)}; then
  pid=$(cat {shlex.quote(pidfile)})
  {kill} -{signal} -- "-$pid" 2>/dev/null || {kill} -{signal} "$pid" 2>/dev/null || true
  n=0
  while {kill} -0 "$pid" 2>/dev/null && test "$n" -lt {timeout_seconds}; do
    sleep 1
    n=$((n + 1))
  done
  if {kill} -0 "$pid" 2>/dev/null; then
    {kill} -TERM -- "-$pid" 2>/dev/null || {kill} -TERM "$pid" 2>/dev/null || true
  fi
  rm -f {shlex.quote(pidfile)}
fi
"""
        self.run(script, check=False)
