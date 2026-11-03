#!/usr/bin/env python3
"""Check Phase 1 project stress/demo requirements against collected artifacts.

Exit 0 if all required artifacts present and basic pass heuristics hold.
Exit 1 with a checklist of gaps.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def exists_nonempty(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--results-root",
        default="stress/results",
        help="Folder containing e1_*/e4_*/election_* outputs",
    )
    args = ap.parse_args()
    root = Path(args.results_root)

    checks: list[tuple[str, bool, str]] = []

    # --- Project §7 pass conditions mapped to artifacts ---
    e1_summary = None
    for cand in [
        root / "e1_udp" / "report" / "summary.md",
        root / "e1_ramp_live" / "report" / "summary.md",
    ]:
        if exists_nonempty(cand):
            e1_summary = cand
            break
    checks.append(
        (
            "Stress ramp tables (latency/throughput mean+/-s)",
            e1_summary is not None,
            "Run: python stress/report/run_phase1_suite.py --scenario e1",
        )
    )

    win = None
    for cand in [
        root / "e1_udp" / "report" / "win_share.md",
        root / "e2_imbalance" / "report" / "win_share.md",
    ]:
        if exists_nonempty(cand):
            win = cand
            break
    checks.append(
        (
            "Load distribution / win share (election evidence)",
            win is not None,
            "Need per-request CSV with processed_by (udp_load_client)",
        )
    )

    e2 = root / "e2_imbalance" / "report" / "win_share.md"
    checks.append(
        (
            "Load imbalance experiment (demo + Table B)",
            exists_nonempty(e2),
            "Run scenario e2 with one node slowed (RM_SIM_DELAY_MS)",
        )
    )

    e4 = root / "e4_fail20" / "report" / "table_d_failure.md"
    fo = root / "e4_fail20" / "report" / "failover.json"
    fail_ok = False
    if exists_nonempty(fo):
        data = json.loads(fo.read_text(encoding="utf-8"))
        rate = data.get("success_rate_during_20s")
        fail_ok = rate is not None and rate > 0
    checks.append(
        (
            "20s failure: service continues (success during mute > 0)",
            exists_nonempty(e4) and fail_ok,
            "Run scenario e4; Ctrl+C node 2 for 20s; pass fail-at-unix to analyzer",
        )
    )

    e5 = root / "e5_rejoin" / "report" / "table_e_rejoin.md"
    checks.append(
        (
            "Rejoin / convergence time recorded",
            exists_nonempty(e5),
            "Run scenario e5; restart muted node; pass revive-at-unix",
        )
    )

    elec = root / "election_overhead" / "election_overhead.md"
    checks.append(
        (
            "Election duration / overhead from node logs",
            exists_nonempty(elec),
            "tee nodes; bash stress/report/run_election_overhead.sh (see DEMO_PHASE1.md)",
        )
    )

    # Volume heuristic
    total_ok = 0
    for csv in root.rglob("*.csv"):
        if csv.name in {"summary.csv", "win_share.csv", "elections.csv", "runs.csv"}:
            continue
        try:
            text = csv.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if "latency_ms" not in text.splitlines()[0:1][0] if text else "":
            # header check
            pass
        total_ok += sum(1 for line in text.splitlines()[1:] if ",1," in line or line.endswith(",1"))
    # weaker heuristic — count ok=1 fields
    ok_count = 0
    for csv in root.rglob("*.csv"):
        if "r" not in csv.name and "fail" not in csv.name and "c" not in csv.name:
            continue
        try:
            lines = csv.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        if not lines or "ok" not in lines[0]:
            continue
        idx = lines[0].split(",").index("ok")
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) > idx and parts[idx] == "1":
                ok_count += 1
    checks.append(
        (
            f"Volume toward tens of thousands (found ok~{ok_count})",
            ok_count >= 2000,
            "Increase --clients/--requests or repeats in e1 suite",
        )
    )

    print("Phase 1 stress requirement checklist")
    print("=" * 60)
    failed = 0
    for name, ok, hint in checks:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"[{mark}] {name}")
        if not ok:
            print(f"       -> {hint}")
    print("=" * 60)
    if failed:
        print(f"{failed} gap(s) remain before the stress bullet is fully evidenced.")
        return 1
    print("All checklist items present. Still write design interpretation in the report.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
