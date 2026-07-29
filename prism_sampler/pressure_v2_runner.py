from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from .config import load_config
from .deploy import build_bundle, install_bundle, install_client
from .orchestration.runner import run_yba, run_yba_suite
from .pressure_v2 import (
    analyze_calibration,
    analyze_closed_loop,
    analyze_combined_calibration,
    analyze_g,
    prepare_crossover_scenario,
    prepare_finalist_suite,
    render_controller_config,
    validate_controller_actions,
    validate_realtime_kpi,
    write_hardware_graph_reference,
    write_online_graph_index,
    write_pressure_v2_report,
)
from .remote import Host


def closed_loop_schedule(seed: int = 20260729) -> list[tuple[str, int]]:
    rng = random.Random(seed)
    rounds = {mode: 0 for mode in ("static_one", "static_two", "dynamic")}
    schedule = []
    for _ in range(3):
        block = list(rounds)
        rng.shuffle(block)
        for mode in block:
            rounds[mode] += 1
            schedule.append((mode, rounds[mode]))
    return schedule


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def environment(values: dict[str, str]):
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class PressureV2Runner:
    def __init__(
        self,
        root: Path,
        gate_a: Path,
        base_config: Path,
        calibration_config: Path,
    ):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.gate_a = gate_a.resolve()
        self.base_config = base_config.resolve()
        self.calibration_config = calibration_config.resolve()
        self.state_path = self.root / "resume-state.json"
        self.state = self._read_state()
        self.config_dir = self.root / "generated-config"
        self.summary = self.root / "summary"
        self.config_dir.mkdir(exist_ok=True)
        self.summary.mkdir(exist_ok=True)

    def _read_state(self) -> dict[str, Any]:
        if self.state_path.is_file():
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        else:
            value = {
                "schema": "prism-sampler.pressure-v2-resume.v1",
                "started_realtime_ns": time.time_ns(),
                "steps": {},
                "experiments": {},
            }
        value.setdefault("steps", {})
        experiments = value.setdefault("experiments", {})
        for name in (
            "live_smoke", "crossover", "closed_static_one", "closed_static_two", "dynamic"
        ):
            experiments.setdefault(name, [])
        return value

    def _save(self) -> None:
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self.state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, self.state_path)

    def _space_gate(self) -> None:
        free = shutil.disk_usage(self.root).free
        roots = self._data_roots()
        created = sum(
            path.stat().st_size
            for root in roots if root.exists()
            for path in root.rglob("*") if path.is_file()
        )
        if free < 80 * 1024**3:
            raise RuntimeError(f"local free space gate failed: {free / 1024**3:.1f}GiB")
        if created > 80 * 1024**3:
            raise RuntimeError(f"v2 data cap failed: {created / 1024**3:.1f}GiB")
        remote_free = int(
            Host("ubuntu197").run(
                "df -Pk /home/xhc | awk 'NR==2 {print $4 * 1024}'"
            ).stdout.strip()
        )
        if remote_free < 80 * 1024**3:
            raise RuntimeError(
                f"ubuntu197 free space gate failed: {remote_free / 1024**3:.1f}GiB"
            )

    def _data_roots(self) -> set[Path]:
        roots = {self.root, self.gate_a}
        for values in self.state.get("experiments", {}).values():
            if isinstance(values, list):
                roots.update(Path(value) for value in values)
        return roots

    def _workload_gate(self, additional_seconds: float = 0.0) -> None:
        workload_seconds = self._workload_seconds()
        if workload_seconds + additional_seconds > 8 * 3600:
            raise RuntimeError(
                f"workload occupancy gate failed: current={workload_seconds / 3600:.2f}h "
                f"next={additional_seconds / 3600:.2f}h"
            )

    @staticmethod
    def _scenario_seconds(path: Path) -> float:
        values = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("SCENARIO_PHASE_") and "_VALUE=" in line:
                values.append(float(line.split("=", 1)[1]))
        return sum(values)

    def _workload_seconds(self) -> float:
        roots = self._data_roots()
        summaries = {
            path.resolve()
            for root in roots if root.exists()
            for path in root.glob("**/phases/*/summary.csv")
        }
        total_ms = 0.0
        for path in summaries:
            with path.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            if len(rows) == 1:
                total_ms += float(rows[0].get("runtime_ms_max") or 0)
        return total_ms / 1000

    def step(self, name: str, action: Callable[[], Any]) -> Any:
        if self.state["steps"].get(name, {}).get("status") == "complete":
            return self.state["steps"][name].get("result")
        last_error = ""
        for attempt in (1, 2):
            self.state["steps"][name] = {
                "status": "running", "attempt": attempt, "started_realtime_ns": time.time_ns()
            }
            self._save()
            try:
                result = action()
                self.state["steps"][name] = {
                    "status": "complete",
                    "attempt": attempt,
                    "finished_realtime_ns": time.time_ns(),
                    "result": result,
                }
                self._save()
                return result
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self.state["steps"][name].update(status="failed", error=last_error)
                self._save()
        raise RuntimeError(f"step failed twice: {name}: {last_error}")

    def _deploy_remote_config(self, path: Path) -> None:
        Host("ubuntu197").copy_to(path, "/home/xhc/.config/prism-sampler/local.toml")

    def _new_experiment(self, before: set[Path], name: str) -> Path:
        matches = set(Path("/data/threadState/experiments/clickhouse").glob(f"*-{name}")) - before
        if len(matches) != 1:
            raise RuntimeError(f"expected one new experiment for {name}: {sorted(matches)}")
        return matches.pop()

    def _run_single(
        self,
        local_config: Path,
        remote_config: Path,
        scenario: Path,
        name: str,
        env: dict[str, str],
    ) -> str:
        self._space_gate()
        self._workload_gate(self._scenario_seconds(scenario))
        self._deploy_remote_config(remote_config)
        parent = Path("/data/threadState/experiments/clickhouse")
        before = set(parent.glob(f"*-{name}"))
        with environment(env):
            code = run_yba(load_config(local_config), self.base_config, scenario, experiment_name=name)
        if code:
            raise RuntimeError(f"YBA returned {code}: {name}")
        return str(self._new_experiment(before, name))

    def run(self) -> dict[str, Any]:
        gate_a_summary = self.summary / "gate-a"
        self.step("analyze_gate_a", lambda: self._analyze_gate_a(gate_a_summary))
        preliminary = gate_a_summary / "selected-config.json"
        gate_b_suite = self.config_dir / "gate-b.env"
        self.step("prepare_gate_b", lambda: prepare_finalist_suite(preliminary, gate_b_suite))
        gate_b_root = self.root / "gate-b"
        self.step("run_gate_b", lambda: self._run_gate_b(gate_b_root, gate_b_suite))
        combined = self.summary
        self.step("analyze_gate_b", lambda: analyze_combined_calibration(
            [self.gate_a / "yba-suite", gate_b_root / "yba-suite"],
            combined,
            preliminary,
        ))
        selected = combined / "selected-config.json"
        self.step("deploy_active_code", self._deploy_code)
        self.step("run_live_graph_smoke", lambda: self._run_live_graph_smoke(selected))
        self.step("run_crossover", lambda: self._run_crossovers(selected))
        self.step("run_randomized_closed_loop", lambda: self._run_randomized_closed_loop(
            selected
        ))
        crossover = [Path(path) for path in self.state["experiments"]["crossover"]]
        static_one = [
            Path(path) for path in self.state["experiments"]["closed_static_one"]
        ]
        static_two = [
            Path(path) for path in self.state["experiments"]["closed_static_two"]
        ]
        dynamic = [Path(path) for path in self.state["experiments"]["dynamic"]]
        self.step("analyze_g", lambda: analyze_g(crossover, selected, self.summary))
        self.step("analyze_closed_loop", lambda: analyze_closed_loop(
            {"one_node": static_one, "two_node": static_two}, dynamic, self.summary
        ))
        self.step("validate_realtime_kpi", lambda: validate_realtime_kpi(
            crossover + dynamic, self.summary, expected_phases=27
        ))
        self.step("validate_controller_actions", lambda: validate_controller_actions(
            crossover + dynamic, self.summary, expected_scripted_actions=24
        ))
        self.step("reference_hardware_graph", lambda: write_hardware_graph_reference(
            Path("/data/threadState/experiments/platform-calibration/kunpen183-20260722"),
            self.summary / "hardware-graph-reference.json",
        ))
        self.step("index_online_thread_graphs", lambda: write_online_graph_index(
            crossover + static_one + static_two + dynamic,
            self.summary / "online-thread-graph-index.csv",
            minimum_phase_graphs=57,
        ))
        self.step("verify_cleanup", self._verify_cleanup)
        self.step("write_workload_budget", self._write_workload_budget)
        self.step("write_report", lambda: str(write_pressure_v2_report(
            self.root, selected, self.summary
        )))
        self.step("write_manifest", lambda: self._manifest(selected))
        return self.state

    def _analyze_gate_a(self, output: Path) -> dict[str, Any]:
        result = analyze_calibration(self.gate_a / "yba-suite", output)
        with (output / "calibration-matrix.csv").open(
            newline="", encoding="utf-8"
        ) as stream:
            rows = list(csv.DictReader(stream))
        validation = {
            "schema": "prism-sampler.gate-a-validation.v1",
            "raw_rows": int(result["raw_rows"]),
            "matrix_rows": len(rows),
            "default_reference_rows": sum(
                str(row.get("default_reference", "")).lower() == "true" for row in rows
            ),
            "nonfinite_pressure_rows": sum(
                any(
                    not value or value.lower() == "nan"
                    for value in (
                        row.get("run_cpu_equiv_median", ""),
                        row.get("rq_cpu_equiv_median", ""),
                    )
                )
                for row in rows
            ),
            "errors": sum(int(row.get("errors") or 0) for row in rows),
            "timeouts": sum(int(row.get("timeouts") or 0) for row in rows),
        }
        validation["passed"] = bool(
            validation["raw_rows"] == 60
            and validation["matrix_rows"] == 60
            and validation["default_reference_rows"] == 6
            and validation["nonfinite_pressure_rows"] == 0
            and validation["errors"] == 0
            and validation["timeouts"] == 0
        )
        (output / "gate-a-validation.json").write_text(
            json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if not validation["passed"]:
            raise RuntimeError(f"Gate A validation failed: {validation}")
        return {**result, "validation": validation}

    def _run_gate_b(self, root: Path, suite: Path) -> dict[str, Any]:
        self._space_gate()
        self._workload_gate(8 * self._scenario_seconds(
            Path(__file__).resolve().parents[1]
            / "config/scenarios/clickhouse-v2-finalist.env"
        ))
        remote_config = self.calibration_config.with_name(
            self.calibration_config.stem + "-remote.toml"
        )
        if remote_config.is_file():
            self._deploy_remote_config(remote_config)
        code = run_yba_suite(
            load_config(self.calibration_config), self.base_config, suite,
            experiment_root=root, resume=(root / "yba-suite").exists(),
        )
        if code:
            raise RuntimeError(f"Gate B returned {code}")
        return {"root": str(root), "returncode": code}

    def _deploy_code(self) -> dict[str, Any]:
        bundle = self.root / "prism-v2-arm64.tar.gz"
        build_bundle(bundle, source_host="kunpen183", source_root="/home/xhc/prism-threads")
        server = Host("kunpen183")
        client = Host("ubuntu197")
        install_bundle(server, bundle, "/home/xhc/prism-sampler")
        install_client(client, "/home/xhc/.local/src/prism-sampler")
        server.run(
            "python3 -m compileall -q /home/xhc/prism-sampler/python/prism_sampler && "
            "/home/xhc/prism-sampler/bin/prism-numa-controller --help >/dev/null && "
            "/home/xhc/prism-sampler/bin/prism-live-analyzer --help >/dev/null"
        )
        client.run(
            "python3 -m compileall -q /home/xhc/.local/src/prism-sampler/prism_sampler && "
            "/home/xhc/.local/bin/prism-sampler-hook --help >/dev/null && "
            "/home/xhc/.local/bin/prism-kpi-forwarder --help >/dev/null"
        )
        return {
            "bundle": str(bundle),
            "server_python": server.run("python3 --version").stdout.strip(),
            "client_python": client.run("python3 --version").stdout.strip(),
        }

    def _configs(
        self,
        selected: Path,
        stem: str,
        *,
        mode: str,
        initial: str,
        transitions: list[str] | None = None,
    ) -> tuple[Path, Path]:
        local = self.config_dir / f"{stem}-local.toml"
        remote = self.config_dir / f"{stem}-remote.toml"
        render_controller_config(
            selected, local, target_host="kunpen183",
            output_root="/data/threadState/experiments", mode=mode,
            initial_state=initial, scripted_transitions=transitions,
        )
        render_controller_config(
            selected, remote, target_host="192.168.70.183",
            output_root="/home/xhc/.local/share/prism-sampler/experiments", mode=mode,
            initial_state=initial, scripted_transitions=transitions,
        )
        return local, remote

    def _selected_env(self, selected: Path, initial: str) -> dict[str, str]:
        value = json.loads(selected.read_text(encoding="utf-8"))
        slots = value["one_node_slots"] if initial == "one_node" else value["two_node_slots"]
        return {
            "CLICKHOUSE_MAX_THREADS": str(value["max_threads"]),
            "CLICKHOUSE_CONCURRENT_THREADS": str(slots),
            "CLICKHOUSE_CONCURRENT_RATIO": "0",
            "SERVER_CPU_NODES": "",
        }

    def _run_crossovers(self, selected: Path) -> dict[str, Any]:
        experiments = list(self.state["experiments"]["crossover"])
        for load in ("C2T2", "C4T6", "C5T16"):
            scenario = self.config_dir / f"crossover-{load}.env"
            prepare_crossover_scenario(load, scenario)
            for initial, transitions, order in (
                ("one_node", ["90:two_node", "210:one_node"], "one-two-one"),
                ("two_node", ["90:one_node", "210:two_node"], "two-one-two"),
            ):
                local, remote = self._configs(
                    selected, f"crossover-{load}-{order}", mode="active",
                    initial=initial, transitions=transitions,
                )
                for round_number in (1, 2):
                    name = f"pressure-v2-crossover-{load}-{order}-r{round_number}"
                    if any(Path(path).name.endswith(name) for path in experiments):
                        continue
                    path = self._run_single(
                        local, remote, scenario, name, self._selected_env(selected, initial)
                    )
                    experiments.append(path)
                    self.state["experiments"]["crossover"] = experiments
                    self._save()
        return {"experiments": experiments}

    def _run_live_graph_smoke(self, selected: Path) -> dict[str, Any]:
        existing = list(self.state["experiments"]["live_smoke"])
        if existing:
            return {"experiment": existing[0]}
        local, remote = self._configs(
            selected, "live-graph-smoke", mode="shadow", initial="one_node"
        )
        scenario = (
            Path(__file__).resolve().parents[1]
            / "config/scenarios/clickhouse-v2-high-smoke.env"
        )
        env = self._selected_env(selected, "one_node")
        env["SERVER_CPU_NODES"] = "0"
        path = Path(self._run_single(
            local, remote, scenario, "pressure-v2-live-graph-smoke", env
        ))
        kpis = [
            json.loads(row) for row in (path / "controller" / "kpi.jsonl").read_text(
                encoding="utf-8"
            ).splitlines() if row.strip()
        ]
        actions = [
            json.loads(line)
            for line in (path / "controller" / "actions.jsonl").read_text(
                encoding="utf-8"
            ).splitlines() if line.strip()
        ]
        summaries = sorted(path.glob("runs/**/raw/live-summary.json"))
        complete_kpis = [row for row in kpis if row.get("complete")]
        if len(complete_kpis) < 6:
            raise RuntimeError(
                f"live graph smoke has only {len(complete_kpis)} complete KPI windows"
            )
        if not any(
            row.get("status") == "shadow" and row.get("to_state") == "two_node"
            for row in actions
        ):
            raise RuntimeError("live graph smoke did not select shadow expansion")
        if len(summaries) != 1:
            raise RuntimeError(f"live graph smoke expected one live summary: {summaries}")
        live = json.loads(summaries[0].read_text(encoding="utf-8"))
        if int(live.get("snapshots") or 0) < 6 or int(live.get("emissions") or 0) < 1:
            raise RuntimeError(f"live graph smoke is incomplete: {live}")
        self.state["experiments"]["live_smoke"] = [str(path)]
        self._save()
        return {
            "experiment": str(path), "complete_kpi_windows": len(complete_kpis),
            "live": live,
        }

    def _run_randomized_closed_loop(self, selected: Path) -> dict[str, Any]:
        scenario = Path(__file__).resolve().parents[1] / "config/scenarios/clickhouse-v2-closed-loop.env"
        schedule = closed_loop_schedule()
        completed = {
            mode: list(self.state["experiments"][key])
            for mode, key in (
                ("static_one", "closed_static_one"),
                ("static_two", "closed_static_two"),
                ("dynamic", "dynamic"),
            )
        }
        for order, (mode, round_number) in enumerate(schedule, 1):
            name = f"pressure-v2-closed-{mode}-r{round_number}"
            if any(Path(path).name.endswith(name) for path in completed[mode]):
                continue
            active = mode == "dynamic"
            initial = "two_node" if mode == "static_two" else "one_node"
            local, remote = self._configs(
                selected, f"closed-{mode}-r{round_number}",
                mode="active" if active else "off", initial=initial,
            )
            env = self._selected_env(selected, initial)
            if not active:
                env["SERVER_CPU_NODES"] = "0,1" if initial == "two_node" else "0"
            path = self._run_single(
                local, remote, scenario, name, env
            )
            completed[mode].append(path)
            key = {
                "static_one": "closed_static_one",
                "static_two": "closed_static_two",
                "dynamic": "dynamic",
            }[mode]
            self.state["experiments"][key] = completed[mode]
            self.state.setdefault("closed_loop_schedule", []).append({
                "order": order,
                "mode": mode,
                "round": round_number,
                "experiment": path,
            })
            self._save()
        return {"schedule": self.state.get("closed_loop_schedule", []), **completed}

    def _verify_cleanup(self) -> dict[str, Any]:
        commands = {
            "kunpen183": (
                "test ! -e /home/xhc/clickhouse/etc/config.d/90-yba-experiment.xml && "
                "test -z \"$(pgrep -f '[m]etric-collector|[p]rism-numa-controller|"
                "clickhouse.*[s]erver.*config.xml' || true)\" && "
                "test ! -d /home/xhc/.local/share/yba/locks/clickhouse.suite.lock"
            ),
            "ubuntu197": (
                "test -z \"$(pgrep -f '[p]rism-kpi-forwarder|[y]ba scenario|"
                "[r]un-suite.sh' || true)\""
            ),
        }
        results = {}
        for host, command in commands.items():
            value = Host(host).run(command, check=False)
            results[host] = {
                "passed": value.returncode == 0,
                "returncode": value.returncode,
                "stdout": value.stdout.strip(),
                "stderr": value.stderr.strip(),
            }
        if not all(value["passed"] for value in results.values()):
            raise RuntimeError(f"post-experiment cleanup validation failed: {results}")
        return results

    def _write_workload_budget(self) -> dict[str, Any]:
        seconds = self._workload_seconds()
        value = {
            "schema": "prism-sampler.workload-budget.v1",
            "actual_workload_seconds": seconds,
            "actual_workload_hours": seconds / 3600,
            "target_minimum_hours": 6,
            "hard_maximum_hours": 8,
            "passed": 6 * 3600 <= seconds <= 8 * 3600,
        }
        (self.summary / "workload-budget.json").write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return value

    def _manifest(self, selected: Path) -> dict[str, Any]:
        repository = Path(__file__).resolve().parents[1]
        git_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
        git_dirty = bool(subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repository,
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip())
        yba_root = Path(load_config(self.calibration_config).section("yba")["root"])
        yba_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=yba_root,
            text=True, capture_output=True, check=True,
        ).stdout.strip()
        yba_dirty = bool(subprocess.run(
            ["git", "status", "--porcelain"], cwd=yba_root,
            text=True, capture_output=True, check=True,
        ).stdout.strip())
        preflight_path = self.gate_a / "preflight.json"
        value = {
            "schema": "prism-sampler.pressure-v2-manifest.v1",
            "git_commit": git_commit,
            "git_dirty": git_dirty,
            "yba_git_commit": yba_commit,
            "yba_git_dirty": yba_dirty,
            "platform_preflight": (
                json.loads(preflight_path.read_text(encoding="utf-8"))
                if preflight_path.is_file() else None
            ),
            "config_sha256": {
                "base": file_sha256(self.base_config),
                "calibration": file_sha256(self.calibration_config),
                "selected": file_sha256(selected),
            },
            "selected_config": json.loads(selected.read_text(encoding="utf-8")),
            "gate_a": str(self.gate_a),
            "root": str(self.root),
            "finished_realtime_ns": time.time_ns(),
            "actual_workload_seconds": self._workload_seconds(),
            "actual_workload_hours": self._workload_seconds() / 3600,
            "experiments": self.state["experiments"],
            "hardware_graph_reference": str(
                (self.summary / "hardware-graph-reference.json").resolve()
            ),
            "online_thread_graph_index": str(
                (self.summary / "online-thread-graph-index.csv").resolve()
            ),
            "graph_placement_applied": False,
            "limits": {
                "new_data_bytes": sum(
                    path.stat().st_size
                    for root in self._data_roots()
                    for path in root.rglob("*") if path.is_file()
                ),
                "data_limit_bytes": 80 * 1024**3,
                "minimum_free_bytes": 80 * 1024**3,
                "sched_ext": False,
                "page_migration": False,
                "cpu_cluster_placement": False,
            },
        }
        (self.root / "manifest.json").write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return value


def execute(
    root: Path, gate_a: Path, base_config: Path, calibration_config: Path
) -> dict[str, Any]:
    return PressureV2Runner(root, gate_a, base_config, calibration_config).run()
