from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .collectors import CollectionSession, SessionContext
from .artifacts import finalize_run
from .calibration import calibrate_experiment
from .config import load_config, read_toml
from .controller.commands import controller_preflight, replay_experiment
from .deploy import build_bundle, install_bundle, install_client, smoke_bundle
from .hooks import handle
from .orchestration import preflight, run_yba, run_yba_suite
from .platform import probe, write_report
from .policies import generate_policies, render_yba, validate_policy
from .relations import analyze_db, analyze_experiment
from .relations.analyzer import GroupRule
from .remote import Host


def _rules(path: Path | None) -> list[GroupRule]:
    if not path:
        return []
    values = read_toml(path)
    return [GroupRule(str(row["name"]), str(row["pattern"])) for row in values.get("group_rules", [])]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prism-sampler")
    commands = parser.add_subparsers(dest="command", required=True)

    platform = commands.add_parser("platform")
    platform_sub = platform.add_subparsers(dest="platform_command", required=True)
    platform_probe = platform_sub.add_parser("probe")
    platform_probe.add_argument("--host", default="local")
    platform_probe.add_argument("--output", type=Path)

    check = commands.add_parser("preflight")
    check.add_argument("--config", required=True, type=Path)

    run = commands.add_parser("run")
    run.add_argument("--config", required=True, type=Path)
    run.add_argument("--yba-config", required=True, type=Path)
    run.add_argument("--scenario", required=True, type=Path)
    run.add_argument("--controller-mode", choices=["off", "shadow", "active"])
    run.add_argument("--experiment-name")
    run_suite = commands.add_parser("run-suite")
    run_suite.add_argument("--config", required=True, type=Path)
    run_suite.add_argument("--yba-config", required=True, type=Path)
    run_suite.add_argument("--suite", required=True, type=Path)
    run_suite.add_argument("--experiment-root", type=Path)
    run_suite.add_argument("--resume", action="store_true")

    collect = commands.add_parser("collect")
    collect_sub = collect.add_subparsers(dest="collect_command", required=True)
    smoke = collect_sub.add_parser("smoke")
    smoke.add_argument("--config", required=True, type=Path)
    smoke.add_argument("--pid", action="append", required=True, type=int)
    smoke.add_argument("--duration", type=int, default=20)
    smoke.add_argument("--output", type=Path)

    analyze = commands.add_parser("analyze")
    analyze_sub = analyze.add_subparsers(dest="analyze_command", required=True)
    adb = analyze_sub.add_parser("db")
    adb.add_argument("db", type=Path)
    adb.add_argument("--pid", action="append", required=True, type=int)
    adb.add_argument("--output", type=Path)
    adb.add_argument("--group-rules", type=Path)
    adb.add_argument("--warmup", type=float, default=30)
    adb.add_argument("--tail", type=float, default=5)
    adb.add_argument("--start", type=float)
    adb.add_argument("--end", type=float)
    adb.add_argument("--window", type=int, default=10)
    aexp = analyze_sub.add_parser("experiment")
    aexp.add_argument("experiment", type=Path)
    aexp.add_argument("--window", type=int, default=10)

    calibrate = commands.add_parser("calibrate")
    calibrate_sub = calibrate.add_subparsers(dest="calibrate_command", required=True)
    cexp = calibrate_sub.add_parser("experiment")
    cexp.add_argument("experiment", type=Path)
    cexp.add_argument("--baseline", default="one_node")

    controller = commands.add_parser("controller")
    controller_sub = controller.add_subparsers(dest="controller_command", required=True)
    controller_check = controller_sub.add_parser("preflight")
    controller_check.add_argument("--config", required=True, type=Path)
    controller_replay = controller_sub.add_parser("replay")
    controller_replay.add_argument("experiment", type=Path)
    controller_replay.add_argument("--config", required=True, type=Path)

    policy = commands.add_parser("policy")
    policy_sub = policy.add_subparsers(dest="policy_command", required=True)
    generate = policy_sub.add_parser("generate")
    generate.add_argument("experiment", type=Path)
    generate.add_argument("--top-k", type=int, default=5)
    validate = policy_sub.add_parser("validate")
    validate.add_argument("policy", type=Path)
    render = policy_sub.add_parser("render-yba")
    render.add_argument("policy", type=Path)
    render.add_argument("--output", required=True, type=Path)
    render.add_argument("--enable", action="store_true")

    deploy = commands.add_parser("deploy")
    deploy_sub = deploy.add_subparsers(dest="deploy_command", required=True)
    build = deploy_sub.add_parser("build")
    build.add_argument("--output", required=True, type=Path)
    source = build.add_mutually_exclusive_group(required=True)
    source.add_argument("--collector", type=Path)
    source.add_argument("--source-host")
    build.add_argument("--source-root", default="/home/xhc/prism-threads")
    build.add_argument("--runtime-lib", action="append", type=Path)
    install = deploy_sub.add_parser("install")
    install.add_argument("--host", required=True)
    install.add_argument("--bundle", required=True, type=Path)
    install.add_argument("--install-dir", default=".local/prism-sampler")
    install_client_parser = deploy_sub.add_parser("install-client")
    install_client_parser.add_argument("--host", required=True)
    install_client_parser.add_argument("--install-dir", default="/home/xhc/.local/src/prism-sampler")
    install_client_parser.add_argument("--config", type=Path)
    dsmoke = deploy_sub.add_parser("smoke")
    dsmoke.add_argument("--host", required=True)
    dsmoke.add_argument("--install-dir", default=".local/prism-sampler")
    dsmoke.add_argument("--best-effort", action="store_true")
    dsmoke.add_argument("--sudo", default="")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "platform":
        report = probe(Host(args.host))
        if args.output:
            write_report(report, args.output)
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    elif args.command == "preflight":
        print(json.dumps(preflight(load_config(args.config)), indent=2, sort_keys=True))
    elif args.command == "run":
        raise SystemExit(run_yba(
            load_config(args.config),
            args.yba_config,
            args.scenario,
            controller_mode=args.controller_mode,
            experiment_name=args.experiment_name,
        ))
    elif args.command == "run-suite":
        raise SystemExit(run_yba_suite(
            load_config(args.config), args.yba_config, args.suite,
            experiment_root=args.experiment_root, resume=args.resume,
        ))
    elif args.command == "collect":
        config = load_config(args.config)
        host = Host(config.target["host"])
        starts = {
            pid: int(host.run(f"awk '{{print $22}}' /proc/{pid}/stat").stdout.strip())
            for pid in args.pid
        }
        output = args.output or Path(config.section("experiment").get("output_root", ".")) / "smoke"
        context = SessionContext("smoke", "smoke", 1, tuple(args.pid), starts, output)
        session = CollectionSession(config, context)
        phase_context = {
            "schema": "prism-sampler.smoke.v1", "run_id": "smoke", "phase": "smoke",
            "round": 1, "target_processes": [
                {"pid": pid, "start_time": starts[pid]} for pid in args.pid
            ],
            "events": [{
                "event": "phase_before", "realtime_ns": time.time_ns(),
                "monotonic_ns": time.monotonic_ns(),
            }],
        }
        (output / "meta").mkdir(parents=True, exist_ok=True)
        print(json.dumps(session.start(), indent=2, sort_keys=True))
        try:
            time.sleep(args.duration)
        finally:
            print(json.dumps(session.stop(), indent=2, sort_keys=True))
        phase_context["events"].append({
            "event": "phase_after", "realtime_ns": time.time_ns(),
            "monotonic_ns": time.monotonic_ns(),
        })
        (output / "meta" / "phase.json").write_text(
            json.dumps(phase_context, indent=2, sort_keys=True) + "\n"
        )
        print(json.dumps(finalize_run(output, phase_context), indent=2, sort_keys=True))
    elif args.command == "analyze" and args.analyze_command == "db":
        print(json.dumps(analyze_db(
            args.db, args.pid, output=args.output, rules=_rules(args.group_rules),
            warmup=args.warmup, tail=args.tail, start=args.start, end=args.end,
            window_seconds=args.window,
        ), indent=2, sort_keys=True))
    elif args.command == "analyze":
        print(json.dumps(analyze_experiment(args.experiment, window_seconds=args.window), indent=2, sort_keys=True))
    elif args.command == "calibrate":
        print(json.dumps(calibrate_experiment(
            args.experiment, baseline=args.baseline
        ), indent=2, sort_keys=True))
    elif args.command == "controller" and args.controller_command == "preflight":
        print(json.dumps(controller_preflight(load_config(args.config)), indent=2, sort_keys=True))
    elif args.command == "controller":
        print(json.dumps(replay_experiment(
            args.experiment, load_config(args.config)
        ), indent=2, sort_keys=True))
    elif args.command == "policy" and args.policy_command == "generate":
        print(json.dumps(generate_policies(args.experiment, top_k=args.top_k), indent=2, sort_keys=True))
    elif args.command == "policy" and args.policy_command == "validate":
        validate_policy(args.policy)
        print("valid")
    elif args.command == "policy":
        print(render_yba(args.policy, args.output, enable=args.enable))
    elif args.command == "deploy" and args.deploy_command == "build":
        print(build_bundle(args.output, collector=args.collector, source_host=args.source_host,
                           source_root=args.source_root, runtime_libs=args.runtime_lib))
    elif args.command == "deploy" and args.deploy_command == "install":
        install_bundle(Host(args.host), args.bundle, args.install_dir)
    elif args.command == "deploy" and args.deploy_command == "install-client":
        install_client(Host(args.host), args.install_dir, args.config)
    elif args.command == "deploy":
        smoke_bundle(Host(args.host), args.install_dir, best_effort=args.best_effort, sudo=args.sudo)


if __name__ == "__main__":
    main()
