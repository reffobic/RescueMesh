#!/usr/bin/env python3
"""Phase 1 stress suite aligned to the project description (not tables-only).

Maps course requirements to runnable scenarios:

  e1  — increasing client load + repeats → latency/throughput curves (statistically meaningful)
  e2  — load imbalance (slow one node via RM_SIM_DELAY_MS) → win-share shift
  e3  — metric policy compare (RM_POLICY on nodes)
  e4  — 20s failure under load → failover_ms + success-during-mute
  e5  — rejoin/convergence under load
  all — run e1 then print instructions for e2–e5

Examples (repo root, nodes already up):
  python stress/report/run_phase1_suite.py --scenario e1 --dry-run
  python stress/report/run_phase1_suite.py --scenario e1
  python stress/report/run_phase1_suite.py --scenario e4 --clients 30 --duration-s 60
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT = Path(__file__).resolve().parent


def run(cmd: list[str], env: dict | None = None) -> int:
    print("+", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(ROOT), env=env or os.environ.copy())


def ensure_nodes_hint() -> None:
    if not os.environ.get("RM_NODES"):
        envf = ROOT / "stress" / "lab" / "nodes.env"
        if envf.exists():
            for line in envf.read_text(encoding="utf-8").splitlines():
                if line.startswith("export RM_NODES="):
                    os.environ["RM_NODES"] = line.split("=", 1)[1].strip().strip('"')
                    break
    print(f"RM_NODES={os.environ.get('RM_NODES', '(unset)')}")


def scenario_e1(args: argparse.Namespace) -> None:
    """Ramp load with repeats — project: stress + statistically meaningful data."""
    out = ROOT / "stress" / "results" / "e1_udp"
    out.mkdir(parents=True, exist_ok=True)
    clients_list = [1, 5, 10, 25, 50] if args.quick else [1, 5, 10, 25, 50, 100]
    if args.clients:
        clients_list = [args.clients]
    repeats = 2 if args.quick else args.repeats
    requests = 10 if args.quick else args.requests_per_client

    meta = {
        "scenario": "e1",
        "requirement": "Stress across increasing client loads; mean/variance latency & throughput",
        "clients_list": clients_list,
        "repeats": repeats,
        "requests_per_client": requests,
        "started_unix": time.time(),
        "dry_run": args.dry_run,
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("=== E1 baseline ramp (project stress bullet) ===")
    print(f"Target volume ~ {len(clients_list) * repeats * sum(clients_list) // len(clients_list) * requests} reqs (order of magnitude)")
    for clients in clients_list:
        for rep in range(repeats):
            csv_path = out / f"r{rep}_c{clients}.csv"
            cmd = [
                sys.executable,
                str(REPORT / "udp_load_client.py"),
                "--hosts",
                str(REPORT / "hosts.json"),
                "--clients",
                str(clients),
                "--requests",
                str(requests),
                "--timeout-s",
                str(args.timeout_s),
                "--out",
                str(csv_path),
            ]
            if args.dry_run:
                cmd.append("--dry-run")
            rc = run(cmd)
            if rc != 0:
                print(f"WARN: load client exit {rc}", file=sys.stderr)
            time.sleep(args.pause_s)

    run(
        [
            sys.executable,
            str(REPORT / "analyze_request_csv.py"),
            "--input",
            str(out),
            "--out",
            str(out / "report"),
        ]
    )
    print("E1 done -> stress/results/e1_udp/report/summary.md (Table A)")


def scenario_e2(args: argparse.Namespace) -> None:
    """Imbalance: operator must slow one node; we measure win share before/after."""
    out = ROOT / "stress" / "results" / "e2_imbalance"
    out.mkdir(parents=True, exist_ok=True)
    print("=== E2 load imbalance (project demo) ===")
    print(
        """
PREP (balanced):
  Start PC1–PC3 with default RM_SIM_DELAY_MS (300).

Then this script captures balanced win share.

AFTER balanced run, overload node 1:
  On PC1 only: export RM_SIM_DELAY_MS=2500
  Restart node 1, keep PC2/PC3 at 300.
  Re-run: python stress/report/run_phase1_suite.py --scenario e2 --tag overloaded
"""
    )
    tag = args.tag or "balanced"
    clients = args.clients or 30
    requests = args.requests_per_client
    for rep in range(args.repeats if not args.quick else 2):
        csv_path = out / f"{tag}_r{rep}_c{clients}.csv"
        cmd = [
            sys.executable,
            str(REPORT / "udp_load_client.py"),
            "--hosts",
            str(REPORT / "hosts.json"),
            "--clients",
            str(clients),
            "--requests",
            str(requests),
            "--out",
            str(csv_path),
        ]
        if args.dry_run:
            cmd.append("--dry-run")
        run(cmd)
        time.sleep(args.pause_s)
    run(
        [
            sys.executable,
            str(REPORT / "analyze_request_csv.py"),
            "--input",
            str(out),
            "--out",
            str(out / "report"),
        ]
    )
    print("Compare win_share.md balanced vs overloaded -> Table B")


def scenario_e3(args: argparse.Namespace) -> None:
    print("=== E3 policy comparison ===")
    print(
        """
For each policy in [weighted, queue, cpu, random]:
  1) On ALL cloud nodes: export RM_POLICY=<policy> && restart nodes
  2) Run:
       python stress/report/run_phase1_suite.py --scenario e2 --tag policy_<policy> --clients 30
  3) Record p95 + Jain from report into Table C

Election scoring is node-side; setting RM_POLICY only on the client is NOT sufficient.
"""
    )


def scenario_e4(args: argparse.Namespace) -> None:
    """20s failure under load — project demo + failover measurement."""
    out = ROOT / "stress" / "results" / "e4_fail20"
    out.mkdir(parents=True, exist_ok=True)
    clients = args.clients or 30
    duration = args.duration_s or 60.0
    mute_s = args.mute_s or 20.0
    fail_after = args.fail_after_s or 15.0

    print("=== E4 20s failure under load (project requirement) ===")
    print(
        f"""
OPERATOR STEPS (watch the clock):
  t=0     : this client starts sustained load ({clients} clients, {duration}s)
  t={fail_after:.0f}s  : on PC2 run Ctrl+C to stop the node  (silence ~{mute_s:.0f}s)
  t={fail_after + mute_s:.0f}s : restart PC2 node with same RM_NODES / RM_NODE_ID=2

We will record fail_at_unix ≈ now+{fail_after:.0f} for failover_ms analysis.
Pass: other nodes still answer during the silence window.
"""
    )
    fail_at = time.time() + fail_after
    (out / "fail_plan.json").write_text(
        json.dumps(
            {
                "fail_at_unix": fail_at,
                "failed_node": args.failed_node,
                "mute_s": mute_s,
                "clients": clients,
                "duration_s": duration,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Planned fail_at_unix={fail_at:.3f} (node {args.failed_node})")
    csv_path = out / f"fail_c{clients}.csv"
    cmd = [
        sys.executable,
        str(REPORT / "udp_load_client.py"),
        "--hosts",
        str(REPORT / "hosts.json"),
        "--clients",
        str(clients),
        "--duration-s",
        str(duration),
        "--rate-per-client",
        str(args.rate_per_client),
        "--timeout-s",
        str(args.timeout_s),
        "--out",
        str(csv_path),
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    run(cmd)

    revive_at = fail_at + mute_s
    run(
        [
            sys.executable,
            str(REPORT / "analyze_request_csv.py"),
            "--input",
            str(csv_path),
            "--out",
            str(out / "report"),
            "--fail-at-unix",
            str(fail_at),
            "--failed-node",
            str(args.failed_node),
            "--revive-at-unix",
            str(revive_at),
        ]
    )
    print("E4 done -> table_d_failure.md + failover.json (+ convergence if node rejoined in window)")


def scenario_e5(args: argparse.Namespace) -> None:
    """Dedicated rejoin run (can reuse e4 artifacts if present)."""
    print("=== E5 rejoin / convergence ===")
    print(
        """
1) Start sustained load (or reuse e4 CSV).
2) Kill node, wait, restart it.
3) Note wall times:
     revive_at_unix = time when cargo node process started again
4) Analyze:
     python stress/report/analyze_request_csv.py --input <csv> --out stress/results/e5_rejoin/report \\
         --fail-at-unix <t0> --failed-node 2 --revive-at-unix <t1>
5) Also confirm node log shows SYNC,... then ready / REJOINED.
"""
    )
    # If e4 report exists, copy convergence into e5 folder
    src = ROOT / "stress" / "results" / "e4_fail20" / "report"
    dst = ROOT / "stress" / "results" / "e5_rejoin" / "report"
    if (src / "table_e_rejoin.md").exists():
        dst.mkdir(parents=True, exist_ok=True)
        for name in ("table_e_rejoin.md", "convergence.json"):
            p = src / name
            if p.exists():
                (dst / name).write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"Copied e4 convergence artifacts -> {dst}")
    else:
        print("No e4 convergence yet — run --scenario e4 with node restart inside the window.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--scenario",
        choices=["e1", "e2", "e3", "e4", "e5", "all", "check"],
        required=True,
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--quick", action="store_true", help="Fewer repeats/loads for harness debug")
    ap.add_argument("--clients", type=int, default=0)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--requests-per-client", type=int, default=40)
    ap.add_argument("--timeout-s", type=float, default=5.0)
    ap.add_argument("--pause-s", type=float, default=1.0)
    ap.add_argument("--duration-s", type=float, default=0.0)
    ap.add_argument("--rate-per-client", type=float, default=2.0)
    ap.add_argument("--mute-s", type=float, default=20.0)
    ap.add_argument("--fail-after-s", type=float, default=15.0)
    ap.add_argument("--failed-node", default="2")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    ensure_nodes_hint()
    if args.scenario == "e1":
        scenario_e1(args)
    elif args.scenario == "e2":
        scenario_e2(args)
    elif args.scenario == "e3":
        scenario_e3(args)
    elif args.scenario == "e4":
        scenario_e4(args)
    elif args.scenario == "e5":
        scenario_e5(args)
    elif args.scenario == "all":
        scenario_e1(args)
        print("\nNext: e2 (imbalance), e4 (20s failure), election log parse, then --scenario check\n")
        scenario_e3(args)
    elif args.scenario == "check":
        return run([sys.executable, str(REPORT / "check_requirements.py")])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
