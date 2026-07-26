from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from prism_sampler.controller.metrics import ProcMetricSource, process_start_time


def stat(pid: int, start: int) -> str:
    fields = ["S"] + ["0"] * 18 + [str(start)]
    return f"{pid} (target) " + " ".join(fields) + "\n"


def write_target(root: Path, pid: int, tid: int, start: int, run: int, rq: int) -> None:
    process = root / str(pid)
    task = process / "task" / str(tid)
    task.mkdir(parents=True, exist_ok=True)
    (process / "stat").write_text(stat(pid, start))
    (process / "numa_maps").write_text("1000 default N0=10 N1=2\n")
    (task / "schedstat").write_text(f"{run} {rq} 1\n")
    (task / "status").write_text("Cpus_allowed_list:\t0-31\n")


def write_system(root: Path) -> None:
    (root / "pressure").mkdir()
    (root / "pressure/cpu").write_text("some avg10=1.00 avg60=0.50 avg300=0.10 total=10\n")
    (root / "stat").write_text("cpu 1 0 0 9 0\ncpu0 1 0 0 9 0\n")


def test_schedstat_deltas_become_cpu_equivalents(tmp_path: Path) -> None:
    proc = tmp_path / "proc"
    sys = tmp_path / "sys"
    proc.mkdir()
    sys.mkdir()
    write_system(proc)
    write_target(proc, 10, 11, 99, 1_000, 2_000)
    source = ProcMetricSource([10], {10: 99}, proc_root=proc, sys_root=sys)
    with patch("prism_sampler.controller.metrics.time.monotonic_ns", return_value=1_000_000_000):
        assert not source.sample(workload_active=True, capacity_cpus=32).valid
    write_target(proc, 10, 11, 99, 11_000_001_000, 5_000_002_000)
    with patch("prism_sampler.controller.metrics.time.monotonic_ns", return_value=11_000_000_000):
        value = source.sample(workload_active=True, capacity_cpus=32)
    assert value.valid
    assert value.run_cpu_equiv == 1.1
    assert value.rq_cpu_equiv == 0.5
    assert value.numa_pages == {"0": 10, "1": 2}


def test_pid_start_time_change_invalidates_sample(tmp_path: Path) -> None:
    proc = tmp_path / "proc"
    sys = tmp_path / "sys"
    proc.mkdir()
    sys.mkdir()
    write_system(proc)
    write_target(proc, 10, 11, 100, 0, 0)
    source = ProcMetricSource([10], {10: 99}, proc_root=proc, sys_root=sys)
    value = source.sample(workload_active=True, capacity_cpus=32)
    assert not value.valid
    assert "identity changed" in str(value.error)


def test_process_start_time_handles_spaces_in_comm() -> None:
    value = stat(1, 123).replace("(target)", "(target worker)")
    assert process_start_time(value) == 123
