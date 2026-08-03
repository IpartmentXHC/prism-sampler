from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .collectors import CollectionSession, SessionContext
from .artifacts import finalize_run
from .calibration import calibrate_experiment
from .blackbox_runner import execute_stage_b
from .blackbox_validation import execute_stage_d
from .config import load_config, read_toml
from .controller.commands import controller_preflight, replay_experiment
from .controller.blackbox_model import train_blackbox_model
from .controller.dynamic_model import (
    build_dynamic_model,
    replay_pressure_windows,
    validate_hidden_active,
    validate_hidden_shadow,
)
from .deploy import build_bundle, install_bundle, install_client, smoke_bundle
from .hooks import handle
from .orchestration import preflight, run_yba, run_yba_suite
from .platform import probe, write_report
from .place_calibration import execute_stage_c
from .resource_curve import (
    build_resource_curve,
    render_resource_curve,
    validate_resource_curve,
)
from .pressure_v2 import (
    analyze_calibration,
    analyze_combined_calibration,
    prepare_finalist_suite,
    analyze_g,
    analyze_closed_loop,
    prepare_static_suite,
    prepare_crossover_scenario,
    render_controller_config,
)
from .pressure_v2_runner import execute as execute_pressure_v2
from .policies import generate_policies, render_yba, validate_policy
from .relations import GroupRule, analyze_db, analyze_experiment
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

    pressure = commands.add_parser("pressure-v2")
    pressure_sub = pressure.add_subparsers(dest="pressure_command", required=True)
    pressure_calibration = pressure_sub.add_parser("analyze-calibration")
    pressure_calibration.add_argument("suite_dir", type=Path)
    pressure_calibration.add_argument("--output", required=True, type=Path)
    pressure_finalists = pressure_sub.add_parser("prepare-finalists")
    pressure_finalists.add_argument("selected", type=Path)
    pressure_finalists.add_argument("--output", required=True, type=Path)
    pressure_combined = pressure_sub.add_parser("analyze-combined")
    pressure_combined.add_argument("suite_dir", nargs="+", type=Path)
    pressure_combined.add_argument("--output", required=True, type=Path)
    pressure_combined.add_argument("--preliminary", type=Path)
    pressure_g = pressure_sub.add_parser("analyze-g")
    pressure_g.add_argument("experiment", nargs="+", type=Path)
    pressure_g.add_argument("--selected", required=True, type=Path)
    pressure_g.add_argument("--output", required=True, type=Path)
    pressure_closed = pressure_sub.add_parser("analyze-closed-loop")
    pressure_closed.add_argument("--static-suite", required=True, type=Path)
    pressure_closed.add_argument("--dynamic", nargs="+", required=True, type=Path)
    pressure_closed.add_argument("--output", required=True, type=Path)
    pressure_static = pressure_sub.add_parser("prepare-static-suite")
    pressure_static.add_argument("selected", type=Path)
    pressure_static.add_argument("--output", required=True, type=Path)
    pressure_crossover = pressure_sub.add_parser("prepare-crossover")
    pressure_crossover.add_argument("load", choices=["C1T1", "C2T2", "C4T6", "C5T16"])
    pressure_crossover.add_argument("--output", required=True, type=Path)
    pressure_runtime = pressure_sub.add_parser("render-controller-config")
    pressure_runtime.add_argument("selected", type=Path)
    pressure_runtime.add_argument("--output", required=True, type=Path)
    pressure_runtime.add_argument("--target-host", required=True)
    pressure_runtime.add_argument("--output-root", required=True)
    pressure_runtime.add_argument("--mode", choices=["off", "shadow", "active"], required=True)
    pressure_runtime.add_argument("--initial-state", choices=["one_node", "two_node"], default="one_node")
    pressure_runtime.add_argument("--transition", action="append", default=[])
    pressure_runtime.add_argument("--sampling-profile", default="pressure-v2")
    pressure_runtime.add_argument("--dynamic-model", type=Path)
    pressure_runtime.add_argument("--minimum-expected-gain-pct", type=float, default=2.0)
    pressure_runtime.add_argument("--controller-poll-seconds", type=float, default=10.0)
    pressure_execute = pressure_sub.add_parser("execute-after-gate-a")
    pressure_execute.add_argument("--root", required=True, type=Path)
    pressure_execute.add_argument("--gate-a", required=True, type=Path)
    pressure_execute.add_argument("--base-config", required=True, type=Path)
    pressure_execute.add_argument("--calibration-config", required=True, type=Path)
    pressure_dynamic = pressure_sub.add_parser("build-dynamic-model")
    pressure_dynamic.add_argument("--anchors", required=True, type=Path)
    pressure_dynamic.add_argument("--pressure-model", required=True, type=Path)
    pressure_dynamic.add_argument("--output", required=True, type=Path)
    pressure_replay = pressure_sub.add_parser("replay-dynamic")
    pressure_replay.add_argument("--windows", required=True, type=Path)
    pressure_replay.add_argument("--model", required=True, type=Path)
    pressure_replay.add_argument("--output", required=True, type=Path)
    pressure_replay.add_argument("--minimum-expected-gain-pct", type=float, default=2.0)
    pressure_replay.add_argument("--gain-uncertainty-multiplier", type=float, default=0.5)
    pressure_shadow = pressure_sub.add_parser("validate-hidden-shadow")
    pressure_shadow.add_argument("experiment", nargs="+", type=Path)
    pressure_shadow.add_argument("--model", required=True, type=Path)
    pressure_shadow.add_argument("--manifest", required=True, type=Path)
    pressure_shadow.add_argument("--output", required=True, type=Path)
    pressure_active = pressure_sub.add_parser("validate-hidden-active")
    pressure_active.add_argument("active", type=Path)
    pressure_active.add_argument("--static-one", required=True, type=Path)
    pressure_active.add_argument("--static-two", required=True, type=Path)
    pressure_active.add_argument("--manifest", required=True, type=Path)
    pressure_active.add_argument("--output", required=True, type=Path)
    pressure_active.add_argument("--equivalent-gain-pct", type=float, default=2.0)
    pressure_active.add_argument("--settling-seconds", type=float, default=20.0)
    pressure_blackbox = pressure_sub.add_parser("train-blackbox-g")
    pressure_blackbox.add_argument("experiment", nargs="+", type=Path)
    pressure_blackbox.add_argument("--output", required=True, type=Path)
    pressure_blackbox.add_argument("--alpha", type=float, default=0.1)
    pressure_stage_b = pressure_sub.add_parser("execute-blackbox-stage-b")
    pressure_stage_b.add_argument("--root", required=True, type=Path)
    pressure_stage_b.add_argument("--selected", required=True, type=Path)
    pressure_stage_b.add_argument("--base-config", required=True, type=Path)
    pressure_stage_b.add_argument("--seed", type=int, default=20260730)
    pressure_stage_c = pressure_sub.add_parser("execute-g-place-stage-c")
    pressure_stage_c.add_argument("--root", required=True, type=Path)
    pressure_stage_c.add_argument("--stage-b-state", required=True, type=Path)
    pressure_stage_c.add_argument("--selected", required=True, type=Path)
    pressure_stage_c.add_argument("--base-config", required=True, type=Path)
    pressure_stage_c.add_argument("--seed", type=int, default=20260730)
    pressure_stage_d = pressure_sub.add_parser("execute-blackbox-stage-d")
    pressure_stage_d.add_argument("--root", required=True, type=Path)
    pressure_stage_d.add_argument("--selected", required=True, type=Path)
    pressure_stage_d.add_argument("--model", required=True, type=Path)
    pressure_stage_d.add_argument("--stage-c-validation", required=True, type=Path)
    pressure_stage_d.add_argument("--base-config", required=True, type=Path)
    pressure_stage_d.add_argument("--scenario", required=True, type=Path)
    pressure_stage_d.add_argument("--manifest", required=True, type=Path)
    pressure_stage_d.add_argument("--anchors", required=True, type=Path)
    pressure_stage_d.add_argument("--skip-deploy", action="store_true")

    resource_curve = commands.add_parser("resource-curve")
    resource_curve_sub = resource_curve.add_subparsers(
        dest="resource_curve_command", required=True
    )
    resource_build = resource_curve_sub.add_parser("build")
    resource_build.add_argument("--anchors", required=True, type=Path)
    resource_build.add_argument("--pressure-anchors", required=True, type=Path)
    resource_build.add_argument("--pressure-model", required=True, type=Path)
    resource_build.add_argument("--g-model", type=Path)
    resource_build.add_argument("--output", required=True, type=Path)
    resource_build.add_argument("--system", default="clickhouse")
    resource_build.add_argument("--platform", default="kunpeng-920")
    resource_build.add_argument("--minimum-oracle-ratio", type=float, default=0.90)
    resource_build.add_argument("--measurement-equivalence-pct", type=float, default=2.0)
    resource_validate = resource_curve_sub.add_parser("validate")
    resource_validate.add_argument("bundle", type=Path)
    resource_render = resource_curve_sub.add_parser("render")
    resource_render.add_argument("bundle", type=Path)
    resource_render.add_argument("--output", required=True, type=Path)

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
    elif args.command == "pressure-v2" and args.pressure_command == "analyze-calibration":
        print(json.dumps(
            analyze_calibration(args.suite_dir, args.output),
            indent=2,
            sort_keys=True,
        ))
    elif args.command == "pressure-v2" and args.pressure_command == "prepare-finalists":
        print(json.dumps(
            prepare_finalist_suite(args.selected, args.output),
            indent=2,
            sort_keys=True,
        ))
    elif args.command == "pressure-v2" and args.pressure_command == "analyze-g":
        print(json.dumps(
            analyze_g(args.experiment, args.selected, args.output),
            indent=2,
            sort_keys=True,
        ))
    elif args.command == "pressure-v2" and args.pressure_command == "analyze-closed-loop":
        print(json.dumps(
            analyze_closed_loop(args.static_suite, args.dynamic, args.output),
            indent=2,
            sort_keys=True,
        ))
    elif args.command == "pressure-v2" and args.pressure_command == "prepare-static-suite":
        print(json.dumps(
            prepare_static_suite(args.selected, args.output), indent=2, sort_keys=True
        ))
    elif args.command == "pressure-v2" and args.pressure_command == "prepare-crossover":
        print(json.dumps(
            prepare_crossover_scenario(args.load, args.output), indent=2, sort_keys=True
        ))
    elif args.command == "pressure-v2" and args.pressure_command == "render-controller-config":
        print(json.dumps(render_controller_config(
            args.selected,
            args.output,
            target_host=args.target_host,
            output_root=args.output_root,
            mode=args.mode,
            initial_state=args.initial_state,
            scripted_transitions=args.transition,
            sampling_profile=args.sampling_profile,
            dynamic_model_path=args.dynamic_model,
            minimum_expected_gain_pct=args.minimum_expected_gain_pct,
            controller_poll_seconds=args.controller_poll_seconds,
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "execute-after-gate-a":
        print(json.dumps(execute_pressure_v2(
            args.root, args.gate_a, args.base_config, args.calibration_config
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "build-dynamic-model":
        print(json.dumps(build_dynamic_model(
            args.anchors, args.pressure_model, args.output
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "replay-dynamic":
        print(json.dumps(replay_pressure_windows(
            args.windows,
            args.model,
            args.output,
            minimum_expected_gain_pct=args.minimum_expected_gain_pct,
            gain_uncertainty_multiplier=args.gain_uncertainty_multiplier,
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "validate-hidden-shadow":
        print(json.dumps(validate_hidden_shadow(
            args.experiment, args.model, args.manifest, args.output
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "validate-hidden-active":
        print(json.dumps(validate_hidden_active(
            args.active,
            args.static_one,
            args.static_two,
            args.manifest,
            args.output,
            equivalent_gain_pct=args.equivalent_gain_pct,
            settling_seconds=args.settling_seconds,
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "train-blackbox-g":
        print(json.dumps(train_blackbox_model(
            args.experiment, args.output, alpha=args.alpha
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "execute-blackbox-stage-b":
        print(json.dumps(execute_stage_b(
            args.root, args.selected, args.base_config, seed=args.seed
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "execute-g-place-stage-c":
        print(json.dumps(execute_stage_c(
            args.root, args.stage_b_state, args.selected, args.base_config,
            seed=args.seed,
        ), indent=2, sort_keys=True))
    elif args.command == "pressure-v2" and args.pressure_command == "execute-blackbox-stage-d":
        print(json.dumps(execute_stage_d(
            args.root, args.selected, args.model, args.stage_c_validation,
            args.base_config, args.scenario, args.manifest, args.anchors,
            deploy=not args.skip_deploy,
        ), indent=2, sort_keys=True))
    elif args.command == "resource-curve" and args.resource_curve_command == "build":
        bundle = build_resource_curve(
            args.anchors,
            args.pressure_anchors,
            args.pressure_model,
            args.output,
            system=args.system,
            platform=args.platform,
            minimum_oracle_ratio=args.minimum_oracle_ratio,
            measurement_equivalence_pct=args.measurement_equivalence_pct,
            g_model_path=args.g_model,
        )
        try:
            render_resource_curve(
                args.output / "calibration-bundle.json",
                args.output / "resource-curve.png",
            )
        except ModuleNotFoundError as exc:
            if exc.name != "matplotlib":
                raise
        print(json.dumps(bundle, indent=2, sort_keys=True))
    elif args.command == "resource-curve" and args.resource_curve_command == "validate":
        print(json.dumps(validate_resource_curve(args.bundle), indent=2, sort_keys=True))
    elif args.command == "resource-curve":
        print(render_resource_curve(args.bundle, args.output))
    elif args.command == "pressure-v2":
        print(json.dumps(
            analyze_combined_calibration(
                args.suite_dir, args.output, args.preliminary
            ),
            indent=2,
            sort_keys=True,
        ))
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
