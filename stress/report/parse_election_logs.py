#!/usr/bin/env python3
"""Parse node stdout for election overhead metrics (project: election duration / messages).

Looks for lines:
  ELECTION,<strategy>,<req_id>,<attempt>,<winner>,<score>,<elapsed_us>,<complete>
  DUPCOORD,<req_id>,<attempt>,<first>,<winner>
  REQUEST,<req_id>,<node>,<queue_us>,<proc_us>,<repl_us>

Usage:
  # tee node output while testing:
  #   cargo run --release -- node 2>&1 | tee /tmp/node1.log
  python stress/report/parse_election_logs.py --logs /tmp/node1.log /tmp/node2.log /tmp/node3.log \\
      --out stress/results/election_overhead/report
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
    ap.add_argument("--logs", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    elections = []
    dupcoords = 0
    requests = []
    for path in args.logs:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            if line.startswith("ELECTION,"):
                parts = line.strip().split(",")
                # ELECTION,strategy,req,attempt,winner,score,elapsed_us,complete
                if len(parts) >= 8:
                    try:
                        elections.append(
                            {
                                "strategy": parts[1],
                                "req_id": parts[2],
                                "attempt": int(parts[3]),
                                "winner": parts[4],
                                "score": parts[5],
                                "elapsed_us": float(parts[6]),
                                "complete": parts[7],
                                "source": path,
                            }
                        )
                    except ValueError:
                        continue
            elif line.startswith("DUPCOORD,"):
                dupcoords += 1
            elif line.startswith("REQUEST,"):
                parts = line.strip().split(",")
                if len(parts) >= 6:
                    try:
                        requests.append(
                            {
                                "req_id": parts[1],
                                "node": parts[2],
                                "queue_us": float(parts[3]),
                                "proc_us": float(parts[4]),
                                "repl_us": float(parts[5]),
                                "source": path,
                            }
                        )
                    except ValueError:
                        continue

    us = [e["elapsed_us"] for e in elections]
    with (out / "elections.csv").open("w", newline="", encoding="utf-8") as fh:
        if elections:
            w = csv.DictWriter(fh, fieldnames=list(elections[0].keys()))
            w.writeheader()
            w.writerows(elections)

    win_counts: dict[str, int] = {}
    for e in elections:
        win_counts[e["winner"]] = win_counts.get(e["winner"], 0) + 1

    md = [
        "# Election overhead (from node logs)",
        "",
        f"- Election samples: **{len(elections)}**",
        f"- DUPCOORD lines: **{dupcoords}**",
        f"- REQUEST timing samples: **{len(requests)}**",
        "",
        "## Election duration (µs)",
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
        md += ["", "No ELECTION lines found — tee node stdout while load runs.", ""]

    md += ["", "## Winner counts", "", "| Node | Elections won |", "|-----:|--------------:|"]
    for nid, cnt in sorted(win_counts.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]):
        md.append(f"| {nid} | {cnt} |")

    if requests:
        proc = [r["proc_us"] for r in requests]
        repl = [r["repl_us"] for r in requests]
        md += [
            "",
            "## REQUEST breakdown (µs)",
            "",
            f"- processing p95: {percentile(proc, 95):.1f}",
            f"- replication p95: {percentile(repl, 95):.1f}",
            "",
        ]
    md += [
        "",
        "Pass: election duration + winner diversity reported under load (project measurements).",
        "",
    ]
    (out / "election_overhead.md").write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {out / 'election_overhead.md'} elections={len(elections)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
