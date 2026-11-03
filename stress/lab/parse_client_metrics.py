#!/usr/bin/env python3
"""Parse CLIENT_METRICS lines from RescueMesh client logs into CSV.

Usage:
  python3 stress/lab/parse_client_metrics.py stress/results/lab_ramp \\
      --out stress/results/lab_ramp/report.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

LINE_RE = re.compile(r"CLIENT_METRICS,(.+)$")
KV_RE = re.compile(r"([^=,]+)=([^,]*)")


def parse_line(line: str) -> dict[str, str] | None:
    m = LINE_RE.search(line.strip())
    if not m:
        return None
    body = m.group(1)
    # per_node={...} may contain commas — peel it off first
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


def collect(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    files = sorted(path.rglob("*")) if path.is_dir() else [path]
    for f in files:
        if not f.is_file():
            continue
        if f.suffix not in {".log", ".metrics", ".txt", ".out", ""} and "metrics" not in f.name:
            # still scan .log/.metrics; also any text that might hold the line
            if f.suffix not in {".log", ".metrics", ".txt", ".out"}:
                continue
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            print(f"skip {f}: {e}", file=sys.stderr)
            continue
        for i, line in enumerate(text.splitlines(), 1):
            row = parse_line(line)
            if row:
                row["source_file"] = str(f)
                row["source_line"] = str(i)
                rows.append(row)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path", type=Path, help="Directory or file with CLIENT_METRICS")
    ap.add_argument("--out", type=Path, default=None, help="CSV output path")
    args = ap.parse_args()
    if not args.path.exists():
        print(f"path not found: {args.path}", file=sys.stderr)
        return 1

    rows = collect(args.path)
    if not rows:
        print("No CLIENT_METRICS lines found.", file=sys.stderr)
        return 2

    preferred = [
        "submitted",
        "succeeded",
        "failed",
        "timed_out",
        "mean_ms",
        "median_ms",
        "p95_ms",
        "p99_ms",
        "throughput_rps",
        "queue_wait_ms",
        "max_in_flight",
        "per_node",
        "source_file",
        "source_line",
    ]
    keys: list[str] = []
    for k in preferred:
        if any(k in r for r in rows):
            keys.append(k)
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)

    out = args.out
    if out is None:
        out = args.path / "report.csv" if args.path.is_dir() else args.path.with_suffix(".csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} row(s) -> {out}")
    # Quick console summary
    try:
        ok = sum(int(float(r.get("succeeded", 0))) for r in rows)
        to = sum(int(float(r.get("timed_out", 0))) for r in rows)
        print(f"Totals: succeeded={ok} timed_out={to}")
    except ValueError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
