#!/usr/bin/env python3
"""Run Phase 1 stress experiments via `cargo run -- client` and log CLIENT_METRICS.

Examples (from repo root):
  python stress/report/run_phase1_experiments.py --config stress/report/configs/e1_ramp.json
  python stress/report/run_phase1_experiments.py --config stress/report/configs/e3_policies.json
  python stress/report/run_phase1_experiments.py --config stress/report/configs/e1_ramp.json --dry-run

Requires RM_NODES in the environment (or stress/lab/nodes.env loaded by the shell).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

METRICS_RE = re.compile(r"CLIENT_METRICS,(.+)$")
KV_RE = re.compile(r"([^=,]+)=([^,]*)")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def parse_metrics_line(line: str) -> dict[str, str] | None:
    m = METRICS_RE.search(line.strip())
    if not m:
        return None
    body = m.group(1)
    per_node = ""
    if "per_node=" in body:
        head, _, rest = body.partition("per_node=")
        body = head.rstrip(",")
        per_node = rest.strip()
    row: dict[str, str] = {}
    for km in KV_RE.finditer(body):
        row[km.group(1).strip()] = km.group(2).strip()
    if per_node:
        row["per_node"] = per_node
    return row


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_rm_nodes() -> str:
    nodes = os.environ.get("RM_NODES", "").strip()
    if nodes:
        return nodes
    env_file = repo_root() / "stress" / "lab" / "nodes.env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("export RM_NODES="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                os.environ["RM_NODES"] = val
                return val
    raise SystemExit(
        "RM_NODES is not set. Run: source stress/lab/nodes.env\n"
        'or: export RM_NODES="1@IP:7001,2@IP:7002,3@IP:7003"'
    )


def run_client(
    *,
    users: int,
    requests_per_user: int,
    max_in_flight: int,
    timeout_ms: int,
    rate_ms: int,
    client_id: str,
    use_release: bool,
    extra_env: dict[str, str],
    log_path: Path,
    dry_run: bool,
) -> dict[str, str] | None:
    root = repo_root()
    env = os.environ.copy()
    env.update(extra_env)
    env["RM_CLIENT_ID"] = client_id
    env["RM_CLIENT_USERS"] = str(users)
    env["RM_CLIENT_REQUESTS_PER_USER"] = str(requests_per_user)
    env["RM_CLIENT_MAX_IN_FLIGHT"] = str(max_in_flight)
    env["RM_CLIENT_TIMEOUT_MS"] = str(timeout_ms)
    env["RM_CLIENT_RATE_MS"] = str(rate_ms)

    cmd = ["cargo", "run"]
    if use_release:
        cmd.append("--release")
    cmd += ["--", "client"]

    if dry_run:
        fake = {
            "submitted": str(users * requests_per_user),
            "succeeded": str(max(0, users * requests_per_user - 1)),
            "failed": "0",
            "timed_out": "1",
            "mean_ms": "50.00",
            "median_ms": "48.00",
            "p95_ms": "90.00",
            "p99_ms": "100.00",
            "throughput_rps": f"{max(users, 1) * 2:.2f}",
            "queue_wait_ms": "1.00",
            "max_in_flight": str(max_in_flight),
            "per_node": "{1: 1, 2: 1, 3: 1}",
        }
        log_path.write_text(
            f"[dry-run] {' '.join(cmd)}\nCLIENT_METRICS,"
            + ",".join(f"{k}={v}" for k, v in fake.items())
            + "\n",
            encoding="utf-8",
        )
        return fake

    print(f"  $ {' '.join(cmd)}  (users={users} id={client_id})")
    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=env,
        capture_output=True,
        text=True,
    )
    text = (proc.stdout or "") + "\n" + (proc.stderr or "")
    log_path.write_text(text, encoding="utf-8")
    metrics = None
    for line in text.splitlines():
        parsed = parse_metrics_line(line)
        if parsed:
            metrics = parsed
    if proc.returncode != 0 and metrics is None:
        print(f"  WARN: cargo exit {proc.returncode}; see {log_path}", file=sys.stderr)
    return metrics


def experiment_matrix(cfg: dict) -> list[dict]:
    """Expand config into concrete runs."""
    runs: list[dict] = []
    repeats = int(cfg.get("repeats", 5))
    clients_list = list(cfg.get("client_counts", [10]))
    policies = cfg.get("policies")
    condition = cfg.get("condition", "")

    if policies:
        for policy in policies:
            for users in clients_list:
                for rep in range(repeats):
                    runs.append(
                        {
                            "users": users,
                            "repeat": rep,
                            "policy": policy,
                            "condition": condition,
                        }
                    )
    else:
        for users in clients_list:
            for rep in range(repeats):
                runs.append(
                    {
                        "users": users,
                        "repeat": rep,
                        "policy": cfg.get("policy", ""),
                        "condition": condition,
                    }
                )
    return runs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True, help="JSON experiment config")
    ap.add_argument("--dry-run", action="store_true", help="Fake metrics (no cargo)")
    ap.add_argument(
        "--quick",
        action="store_true",
        help="Override to 2 repeats and first 3 client_counts (smoke the harness)",
    )
    args = ap.parse_args()

    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    if args.quick:
        cfg["repeats"] = min(int(cfg.get("repeats", 5)), 2)
        counts = list(cfg.get("client_counts", [1, 5, 10]))
        cfg["client_counts"] = counts[:3]
        if "policies" in cfg:
            cfg["policies"] = cfg["policies"][:2]

    nodes = ensure_rm_nodes()
    root = repo_root()
    out_dir = root / cfg.get("output_dir", f"stress/results/{cfg['name']}")
    out_dir.mkdir(parents=True, exist_ok=True)
    logs = out_dir / "logs"
    logs.mkdir(exist_ok=True)

    meta = {
        "name": cfg["name"],
        "description": cfg.get("description", ""),
        "table": cfg.get("table", ""),
        "dry_run": args.dry_run,
        "quick": args.quick,
        "config": cfg,
        "RM_NODES": nodes,
        "started_unix": time.time(),
        "git": subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(root),
            capture_output=True,
            text=True,
        ).stdout.strip()
        or "unknown",
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    fieldnames = [
        "experiment",
        "table",
        "condition",
        "policy",
        "clients",
        "repeat",
        "submitted",
        "succeeded",
        "failed",
        "timed_out",
        "error_rate",
        "mean_ms",
        "median_ms",
        "p95_ms",
        "p99_ms",
        "throughput_rps",
        "queue_wait_ms",
        "max_in_flight",
        "per_node",
        "log_file",
    ]
    runs_csv = out_dir / "runs.csv"
    matrix = experiment_matrix(cfg)
    print(f"=== {cfg['name']}: {len(matrix)} runs -> {out_dir} ===")
    if cfg.get("manual_failure") or cfg.get("manual_rejoin"):
        print("NOTE:", cfg.get("notes", "Manual failure/rejoin steps — see PHASE1_REPORT_GUIDE.md"))

    warmup = float(cfg.get("warmup_s", 0))
    pause = float(cfg.get("pause_between_repeats_s", 1))
    rpu = int(cfg.get("requests_per_user", 40))
    mif = int(cfg.get("max_in_flight", 64))
    timeout = int(cfg.get("timeout_ms", 8000))
    rate = int(cfg.get("rate_ms", 0))
    use_release = bool(cfg.get("use_release", True))

    with runs_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for i, spec in enumerate(matrix, 1):
            users = int(spec["users"])
            rep = int(spec["repeat"])
            policy = str(spec.get("policy") or "")
            condition = str(spec.get("condition") or "")
            tag = f"c{users}_r{rep}"
            if policy:
                tag = f"p{policy}_{tag}"
            if condition:
                tag = f"{condition}_{tag}"
            log_path = logs / f"{tag}.log"
            print(f"[{i}/{len(matrix)}] {tag}")

            if warmup > 0 and not args.dry_run:
                time.sleep(warmup)

            extra = {}
            if policy:
                extra["RM_POLICY"] = policy

            client_id = f"{cfg['name']}-{tag}-{os.getpid()}"
            metrics = run_client(
                users=users,
                requests_per_user=rpu,
                max_in_flight=mif,
                timeout_ms=timeout,
                rate_ms=rate,
                client_id=client_id,
                use_release=use_release,
                extra_env=extra,
                log_path=log_path,
                dry_run=args.dry_run,
            )

            row = {k: "" for k in fieldnames}
            row.update(
                {
                    "experiment": cfg["name"],
                    "table": str(cfg.get("table", "")),
                    "condition": condition,
                    "policy": policy,
                    "clients": str(users),
                    "repeat": str(rep),
                    "log_file": str(log_path.relative_to(out_dir)),
                }
            )
            if metrics:
                submitted = float(metrics.get("submitted", 0) or 0)
                succeeded = float(metrics.get("succeeded", 0) or 0)
                failed = float(metrics.get("failed", 0) or 0)
                timed_out = float(metrics.get("timed_out", 0) or 0)
                err_den = max(submitted, 1.0)
                row.update(
                    {
                        "submitted": metrics.get("submitted", ""),
                        "succeeded": metrics.get("succeeded", ""),
                        "failed": metrics.get("failed", ""),
                        "timed_out": metrics.get("timed_out", ""),
                        "error_rate": f"{(failed + timed_out) / err_den:.6f}",
                        "mean_ms": metrics.get("mean_ms", ""),
                        "median_ms": metrics.get("median_ms", ""),
                        "p95_ms": metrics.get("p95_ms", ""),
                        "p99_ms": metrics.get("p99_ms", ""),
                        "throughput_rps": metrics.get("throughput_rps", ""),
                        "queue_wait_ms": metrics.get("queue_wait_ms", ""),
                        "max_in_flight": metrics.get("max_in_flight", ""),
                        "per_node": metrics.get("per_node", ""),
                    }
                )
                print(
                    f"  OK thr={row['throughput_rps']} p95={row['p95_ms']} "
                    f"err={row['error_rate']} per_node={row['per_node']}"
                )
            else:
                print("  FAIL: no CLIENT_METRICS line", file=sys.stderr)
                row["error_rate"] = "1.0"
            writer.writerow(row)
            fh.flush()
            if pause > 0 and i < len(matrix) and not args.dry_run:
                time.sleep(pause)

    meta["finished_unix"] = time.time()
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Wrote {runs_csv}")
    print(
        "Next: python stress/report/build_report_tables.py "
        f"--input {out_dir.relative_to(root)} --out {out_dir.relative_to(root)}/report"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
