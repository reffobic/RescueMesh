#!/usr/bin/env python3
"""Merge election_overhead_n1/n2/n3 (or any dirs with elections.csv) into one report.

Usage (after Option A on each cloud PC + git pull on PC4):
  python3 stress/report/merge_election_overhead.py \\
      --inputs stress/results/election_overhead_n1 \\
               stress/results/election_overhead_n2 \\
               stress/results/election_overhead_n3 \\
      --out stress/results/election_overhead
"""
from __future__ import annotations

import argparse
import csv
import math
import statistics
from pathlib import Path


def percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    ys = sorted(xs)
    if len(ys) == 1:
        return ys[0]
    k = (len(ys) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ys[int(k)]
    return ys[f] * (c - k) + ys[c] * (k - f)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--inputs",
        nargs="+",
        default=[
            "stress/results/election_overhead_n1",
            "stress/results/election_overhead_n2",
            "stress/results/election_overhead_n3",
        ],
    )
    ap.add_argument("--out", default="stress/results/election_overhead")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    elections: list[dict] = []
    used: list[str] = []
    missing: list[str] = []
    for raw in args.inputs:
        d = Path(raw)
        csv_path = d / "elections.csv"
        if not csv_path.exists():
            missing.append(str(d))
            continue
        with csv_path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        elections.extend(rows)
        used.append(f"{d} ({len(rows)} rows)")

    if not elections:
        raise SystemExit(
            "No elections.csv found under inputs. Run Option A on PC1/2/3 first.\n"
            f"Missing: {missing}"
        )

    # Dedupe identical election lines (same req/attempt/winner/elapsed from overlapping views)
    seen = set()
    unique = []
    for e in elections:
        key = (
            e.get("req_id", ""),
            e.get("attempt", ""),
            e.get("winner", ""),
            e.get("elapsed_us", ""),
            e.get("strategy", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)
    elections = unique

    us: list[float] = []
    for e in elections:
        try:
            us.append(float(e["elapsed_us"]))
        except (KeyError, ValueError):
            continue

    win_counts: dict[str, int] = {}
    for e in elections:
        w = str(e.get("winner", ""))
        win_counts[w] = win_counts.get(w, 0) + 1

    fields = list(elections[0].keys())
    with (out / "elections.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(elections)

    md = [
        "# Election overhead (merged n1+n2+n3)",
        "",
        "## Sources",
        "",
    ]
    for u in used:
        md.append(f"- {u}")
    if missing:
        md.append("")
        md.append("## Missing inputs")
        md.append("")
        for m in missing:
            md.append(f"- {m} (skipped)")
    md += [
        "",
        f"- Merged unique election samples: **{len(elections)}**",
        "",
        "## Election duration (us)",
        "",
        "| n | mean | p50 | p95 | p99 |",
        "|--:|-----:|----:|----:|----:|",
    ]
    if us:
        md.append(
            f"| {len(us)} | {statistics.mean(us):.1f} | {percentile(us, 50):.1f} | "
            f"{percentile(us, 95):.1f} | {percentile(us, 99):.1f} |"
        )
    else:
        md.append("| 0 | | | | |")

    md += ["", "## Winner counts", "", "| Node | Elections won |", "|-----:|--------------:|"]
    for nid, cnt in sorted(win_counts.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]):
        md.append(f"| {nid} | {cnt} |")
    md += [
        "",
        "Pass: election duration + winner diversity reported under load.",
        "",
    ]
    (out / "election_overhead.md").write_text("\n".join(md), encoding="utf-8")
    print(f"Merged {len(elections)} unique elections -> {out / 'election_overhead.md'}")
    if missing:
        print("WARNING missing:", ", ".join(missing))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
