#!/usr/bin/env python3
"""Run configured RescueMesh sweeps and save raw data plus summaries."""

import argparse
import ast
import csv
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
import uuid
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


TERMS = ("CPU", "QUEUE", "SVC", "IO", "MEM", "REL", "DELAY")
DEFAULT_WEIGHTS = {
    "CPU": 0.24,
    "QUEUE": 0.24,
    "SVC": 0.20,
    "IO": 0.08,
    "MEM": 0.04,
    "REL": 0.10,
    "DELAY": 0.10,
}
CONFIG_TEMPLATE = {
    "nodes": "1@10.0.0.11:7001,2@10.0.0.12:7002,3@10.0.0.13:7003,4@10.0.0.14:7004,5@10.0.0.15:7005,6@10.0.0.16:7006",
    "working_directory": ".",
    "commands": {
        "measure_rtt": [],
        "restart_nodes": [],
        "clear_logs": [],
        "collect_logs": [],
        "client": ["cargo", "run", "--release", "--", "client"],
    },
    "conditions": {
        "idle": {"setup": [], "cleanup": []},
        "heavy_load": {"setup": [], "cleanup": []},
        "message_loss": {"setup": [], "cleanup": []},
    },
    "chosen_t_ok_ms": 100,
    "chosen_t_coord_ms": 500,
    "t_ok_values_ms": [5, 10, 25, 50, 100, 200],
    "t_coord_values_ms": [50, 100, 250, 500, 1000, 2000],
    "queue_cap_values": [5, 10, 20, 50, 100],
    "client_load_levels": [16, 32, 64, 128],
    "client_users": 32,
    "requests_per_user": 100,
    "client_timeout_ms": 10000,
    "sample_interval_ms": 500,
    "cpu_workers_values": [1, 2, 4, 8],
    "node_in_flight_values": [8, 16, 32, 64],
    "queue_capacity_values": [16, 32, 64, 128, 256],
}


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * p) - 1))
    return ordered[index]


def confidence_interval(values, seed):
    if not values:
        return (None, None)
    if len(values) == 1:
        return (values[0], values[0])
    rng = random.Random(seed)
    means = []
    for _ in range(2000):
        means.append(statistics.mean(rng.choices(values, k=len(values))))
    return percentile(means, 0.025), percentile(means, 0.975)


def normalized_weights(weights):
    total = sum(weights.values())
    if total <= 0:
        return {key: 0.0 for key in weights}
    return {key: value / total for key, value in weights.items()}


def score_cases():
    cases = []
    for policy in ("weighted", "queue", "cpu", "random"):
        cases.append(("score_policy", policy, {"RM_POLICY": policy}))

    for term in TERMS:
        weights = dict(DEFAULT_WEIGHTS)
        weights[term] = 0.0
        weights = normalized_weights(weights)
        env = {"RM_POLICY": "weighted"}
        env.update({f"RM_W_{key}": value for key, value in weights.items()})
        cases.append((f"ablate_{term.lower()}", f"remove_{term.lower()}", env))

    for group, fraction in (("100_0", 1.0), ("80_20", 0.8), ("60_40", 0.6), ("50_50", 0.5)):
        weights = dict(DEFAULT_WEIGHTS)
        machine_total = sum(weights[key] for key in ("CPU", "QUEUE", "SVC", "IO", "MEM"))
        communication_total = weights["REL"] + weights["DELAY"]
        env = {"RM_POLICY": "weighted"}
        for key in ("CPU", "QUEUE", "SVC", "IO", "MEM"):
            env[f"RM_W_{key}"] = weights[key] * fraction / machine_total
        for key in ("REL", "DELAY"):
            env[f"RM_W_{key}"] = weights[key] * (1.0 - fraction) / communication_total
        cases.append(("machine_communication_split", group, env))

    for group, included in (
        ("machine_only", {"CPU", "QUEUE", "SVC", "IO", "MEM"}),
        ("communication_only", {"REL", "DELAY"}),
    ):
        weights = {key: value if key in included else 0.0 for key, value in DEFAULT_WEIGHTS.items()}
        weights = normalized_weights(weights)
        env = {"RM_POLICY": "weighted"}
        env.update({f"RM_W_{key}": value for key, value in weights.items()})
        cases.append(("score_group_ablation", group, env))
    return cases


def cases_for_suite(suite, config):
    cases = []
    if suite in ("election", "all"):
        for value in config["t_ok_values_ms"]:
            cases.append(("t_ok", str(value), {
                "RM_T_OK_MS": value,
                "RM_T_COORD_MS": config["chosen_t_coord_ms"],
            }))
        for value in config["t_coord_values_ms"]:
            cases.append(("t_coord", str(value), {
                "RM_T_OK_MS": config["chosen_t_ok_ms"],
                "RM_T_COORD_MS": value,
            }))
    if suite in ("score", "all"):
        for kind, value, env in score_cases():
            cases.append((kind, value, env))
    if suite in ("queue", "all"):
        queue_settings = [("queue_cap", "RM_QUEUE_CAP", value) for value in config["queue_cap_values"]]
        for env_name, values, kind in (
            ("RM_CPU_WORKERS", config["cpu_workers_values"], "cpu_workers"),
            ("RM_NODE_MAX_IN_FLIGHT", config["node_in_flight_values"], "node_in_flight"),
            ("RM_REQUEST_QUEUE_CAPACITY", config["queue_capacity_values"], "queue_capacity"),
        ):
            queue_settings.extend((kind, env_name, value) for value in values)
        for kind, env_name, value in queue_settings:
            for load in config["client_load_levels"]:
                cases.append((f"{kind}_load_{load}", str(value), {
                    env_name: value,
                    "RM_CLIENT_MAX_IN_FLIGHT": load,
                    "RM_CLIENT_USERS": max(config["client_users"], load),
                }))
    return cases


def parse_client_metrics(text):
    line = next((line for line in text.splitlines() if line.startswith("CLIENT_METRICS,")), None)
    if line is None:
        raise RuntimeError("client command completed but emitted no CLIENT_METRICS line")
    values = dict(re.findall(r"([a-z][a-z0-9_]*)=([^,]+)", line))
    node_match = re.search(r"per_node=(\{[^}]*\})", line)
    try:
        per_node = ast.literal_eval(node_match.group(1)) if node_match else {}
    except (SyntaxError, ValueError) as error:
        raise RuntimeError(f"could not parse per_node client metric: {error}") from error
    result = {}
    for key in (
        "submitted", "succeeded", "failed", "timed_out", "mean_ms", "median_ms",
        "p95_ms", "p99_ms", "throughput_rps", "queue_wait_ms", "max_in_flight",
    ):
        if key in values:
            result[key] = float(values[key]) if key.endswith("_ms") or key == "throughput_rps" else int(values[key])
    result["per_node"] = {str(key): int(value) for key, value in per_node.items()}
    return result


def run_command(command, cwd, env, label, output_path=None):
    if not command:
        return ""
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if output_path:
        output_path.write_text(result.stdout, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(
            f"{label} exited with status {result.returncode}; see {output_path or 'command output'}"
        )
    return result.stdout


def env_for_run(config, output_dir, condition, case_name, case_value, overrides, run_number=None):
    env = os.environ.copy()
    env.update({str(key): str(value) for key, value in config.get("base_env", {}).items()})
    env.update({str(key): str(value) for key, value in overrides.items()})
    env.update({
        "RM_NODES": config["nodes"],
        "RM_CLIENT_USERS": str(overrides.get("RM_CLIENT_USERS", config["client_users"])),
        "RM_CLIENT_REQUESTS_PER_USER": str(config["requests_per_user"]),
        "RM_CLIENT_TIMEOUT_MS": str(config["client_timeout_ms"]),
        "RM_CLIENT_MAX_IN_FLIGHT": str(overrides.get(
            "RM_CLIENT_MAX_IN_FLIGHT",
            config.get("client_max_in_flight", 32),
        )),
        "RM_SAMPLE_MS": str(config["sample_interval_ms"]),
        "RM_SWEEP_OUTPUT": str(output_dir.resolve()),
        "RM_SWEEP_CONDITION": condition,
        "RM_SWEEP_CASE": case_name,
        "RM_SWEEP_VALUE": str(case_value),
    })
    if run_number is not None:
        env["RM_CLIENT_ID"] = f"sweep-{uuid.uuid4().hex}"
        env["RM_SWEEP_RUN"] = str(run_number)
    return env


def run_suite(args):
    config_path = Path(args.config).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    cases = cases_for_suite(args.suite, config)
    if not cases:
        raise RuntimeError(f"no cases configured for suite {args.suite}")
    workdir = (config_path.parent / config.get("working_directory", ".")).resolve()
    commands = config["commands"]
    for condition in args.conditions:
        if condition not in config.get("conditions", {}):
            raise RuntimeError(f"condition {condition!r} is not configured in {config_path}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(args.output).resolve() / f"{args.suite}-{stamp}"
    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    plan = [
        {"condition": condition, "case": kind, "value": value, "env": env}
        for condition in args.conditions
        for kind, value, env in cases
    ]
    (output_dir / "plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")

    if args.dry_run:
        print(
            f"Planned {len(plan)} settings, {len(plan) * args.repeats} measured client runs "
            f"plus {len(plan)} warm-ups; output: {output_dir}"
        )
        return

    for command_name in ("restart_nodes", "client", "collect_logs"):
        if not commands.get(command_name):
            raise RuntimeError(f"configure commands.{command_name} in {config_path}")
    for condition in args.conditions:
        if condition != "idle" and not config["conditions"][condition].get("setup"):
            raise RuntimeError(f"configure a safe conditions.{condition}.setup hook before running it")

    if commands.get("measure_rtt"):
        rtt_env = env_for_run(config, output_dir, "baseline", "rtt", "all", {})
        run_command(commands["measure_rtt"], workdir, rtt_env, "RTT baseline", output_dir / "rtt-baseline.log")

    raw_rows = []
    for condition in args.conditions:
        hooks = config["conditions"][condition]
        for case_index, (case_name, case_value, overrides) in enumerate(cases, start=1):
            case_dir = output_dir / condition / f"{case_index:03d}-{case_name}-{case_value}"
            case_dir.mkdir(parents=True)
            run_env = env_for_run(config, case_dir, condition, case_name, case_value, overrides)
            (case_dir / "setting.json").write_text(json.dumps({
                "condition": condition,
                "case": case_name,
                "value": case_value,
                "environment": {key: value for key, value in run_env.items() if key.startswith("RM_")},
            }, indent=2), encoding="utf-8")
            print(f"[{condition}] {case_name}={case_value} ({case_index}/{len(cases)})")
            try:
                run_command(hooks.get("setup", []), workdir, run_env, "condition setup", case_dir / "condition-setup.log")
                run_command(commands["restart_nodes"], workdir, run_env, "node restart", case_dir / "restart.log")
                warmup_env = env_for_run(config, case_dir, condition, case_name, case_value, overrides, "warmup")
                warmup_env["RM_CLIENT_REQUESTS_PER_USER"] = str(config.get("warmup_requests_per_user", 10))
                run_command(commands["client"], workdir, warmup_env, "warm-up client", case_dir / "warmup.log")
                run_command(commands.get("clear_logs", []), workdir, run_env, "clear logs", case_dir / "clear-logs.log")

                for repeat in range(1, args.repeats + 1):
                    one_env = env_for_run(config, case_dir, condition, case_name, case_value, overrides, repeat)
                    client_out = run_command(
                        commands["client"], workdir, one_env, f"client repeat {repeat}",
                        case_dir / f"client-{repeat:03d}.log",
                    )
                    metrics = parse_client_metrics(client_out)
                    raw_rows.append({
                        "condition": condition,
                        "case": case_name,
                        "value": case_value,
                        "repeat": repeat,
                        **{key: val for key, val in metrics.items() if key != "per_node"},
                        "per_node": json.dumps(metrics["per_node"], sort_keys=True),
                        "jain_fairness": jain_index(
                            list(metrics["per_node"].values()),
                            len(config["nodes"].split(",")),
                        ),
                    })
                log_text = run_command(
                    commands["collect_logs"], workdir, run_env, "collect node logs", case_dir / "nodes.log"
                )
                election_times, duplicate_count = parse_election_logs(log_text)
                (case_dir / "telemetry-summary.json").write_text(json.dumps({
                    "winner_election_events": len(election_times),
                    "winner_election_p50_ms": percentile(election_times, 0.50),
                    "winner_election_p95_ms": percentile(election_times, 0.95),
                    "winner_election_p99_ms": percentile(election_times, 0.99),
                    "duplicate_coordinator_events": duplicate_count,
                    "not_available_from_current_logs": [
                        "false-win rate / lowest-score winner",
                        "restarts per election",
                        "first-OK-to-COORDINATOR delay",
                        "messages per election",
                        "per-node score snapshots",
                    ],
                }, indent=2), encoding="utf-8")
            finally:
                run_command(hooks.get("cleanup", []), workdir, run_env, "condition cleanup", case_dir / "condition-cleanup.log")

    write_csv(output_dir / "runs.csv", raw_rows)
    summaries = summarize_rows(raw_rows)
    write_csv(output_dir / "summary.csv", summaries)
    (output_dir / "report.json").write_text(json.dumps({
        "suite": args.suite,
        "repeats_per_setting": args.repeats,
        "conditions": args.conditions,
        "settings": len(plan),
        "summaries": summaries,
        "limitations": [
            "The application currently uses a 300 ms placeholder instead of the real planner.",
            "The application logs do not expose restart counts, OK-to-COORDINATOR timing, messages per election, per-node election scores, or false-win verification.",
            "Sampled per-node CPU, memory, and I/O time series are not exported by the current application.",
            "The score bucket width is hard-coded and cannot be swept through environment settings.",
            "Load, packet loss, and failure injection require user-configured condition hooks.",
        ],
    }, indent=2), encoding="utf-8")
    write_svg_plots(output_dir, summaries)
    print(f"Finished. CSV, JSON report, raw logs, and plots: {output_dir}")


def parse_election_logs(text):
    elapsed_ms = []
    duplicates = 0
    for line in text.splitlines():
        if line.startswith("ELECTION,"):
            fields = line.strip().split(",")
            if len(fields) >= 7:
                try:
                    elapsed_ms.append(int(fields[6]) / 1000.0)
                except ValueError:
                    continue
        elif line.startswith("DUPCOORD,"):
            duplicates += 1
    return elapsed_ms, duplicates


def jain_index(values, node_count=None):
    if not values or sum(values) == 0:
        return None
    if node_count:
        values = values + [0] * max(0, node_count - len(values))
    return sum(values) ** 2 / (len(values) * sum(value * value for value in values))


def summarize_rows(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["condition"], row["case"], row["value"])].append(row)
    summaries = []
    for (condition, case, value), group in groups.items():
        seed = zlib.crc32(f"{condition}:{case}:{value}".encode("utf-8"))
        summary = {
            "condition": condition,
            "case": case,
            "value": value,
            "repeats": len(group),
        }
        for source, target in (
            ("mean_ms", "mean_latency_ms"),
            ("median_ms", "median_latency_ms"),
            ("p95_ms", "p95_latency_ms"),
            ("p99_ms", "p99_latency_ms"),
            ("throughput_rps", "throughput_rps"),
            ("failed", "failed_requests"),
            ("timed_out", "timed_out_requests"),
            ("jain_fairness", "jain_fairness"),
        ):
            values = [float(row[source]) for row in group if row.get(source) not in (None, "")]
            summary[target] = statistics.mean(values) if values else None
            ci_low, ci_high = confidence_interval(values, seed)
            summary[f"{target}_ci95_low"] = ci_low
            summary[f"{target}_ci95_high"] = ci_high
        summaries.append(summary)
    return summaries


def write_csv(path, rows):
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_svg_plots(output_dir, summaries):
    grouped = defaultdict(list)
    for row in summaries:
        grouped[(row["condition"], row["case"])].append(row)
    for (condition, case), rows in grouped.items():
        points = []
        for row in rows:
            try:
                x_value = float(row["value"])
                y_value = float(row["p95_latency_ms"])
            except (TypeError, ValueError):
                continue
            points.append((x_value, y_value))
        if not points:
            continue
        points.sort()
        width, height, pad = 800, 420, 60
        x_min, x_max = min(x for x, _ in points), max(x for x, _ in points)
        y_min, y_max = min(y for _, y in points), max(y for _, y in points)
        x_span, y_span = max(x_max - x_min, 1), max(y_max - y_min, 1)
        coords = [
            (pad + (x - x_min) / x_span * (width - 2 * pad),
             height - pad - (y - y_min) / y_span * (height - 2 * pad))
            for x, y in points
        ]
        polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
        labels = "\n".join(
            f'<text x="{x:.1f}" y="{y - 10:.1f}" font-size="12">{px:g}, {py:.2f} ms</text>'
            for (x, y), (px, py) in zip(coords, points)
        )
        svg = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
            f'<rect width="100%" height="100%" fill="white"/>'
            f'<text x="{width / 2}" y="25" text-anchor="middle" font-size="16">'
            f'{condition} / {case}: client p95 latency by setting</text>'
            f'<line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" stroke="black"/>'
            f'<line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height-pad}" stroke="black"/>'
            f'<polyline points="{polyline}" fill="none" stroke="#1769aa" stroke-width="2"/>'
            f'{labels}<text x="{width / 2}" y="{height - 12}" text-anchor="middle">setting</text>'
            f'<text x="15" y="{height / 2}" transform="rotate(-90 15 {height / 2})" '
            f'text-anchor="middle">p95 latency (ms)</text></svg>'
        )
        (output_dir / f"{condition}-{case}.svg").write_text(svg, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="sweep_config.json")
    parser.add_argument("--init-config", action="store_true", help="write an editable example config and exit")
    parser.add_argument("--suite", choices=("election", "score", "queue", "all"), default="election")
    parser.add_argument("--conditions", nargs="+", choices=("idle", "heavy_load", "message_loss"), default=["idle"])
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--output", default="sweep-results")
    parser.add_argument("--dry-run", action="store_true", help="save the plan without starting tests")
    args = parser.parse_args()

    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    if args.init_config:
        path = Path(args.config)
        if path.exists():
            parser.error(f"refusing to overwrite existing config: {path}")
        path.write_text(json.dumps(CONFIG_TEMPLATE, indent=2), encoding="utf-8")
        print(f"Created {path.resolve()}; edit its command hooks before running.")
        return 0
    try:
        run_suite(args)
    except (OSError, RuntimeError, KeyError, ValueError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
