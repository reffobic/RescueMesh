#!/usr/bin/env python3
"""Analyze per-request CSVs from udp_load_client.py into Phase 1 metrics + tables.

Computes:
  - latency p50/p95/p99, throughput, error rate (mean+/-stdev over repeats)
  - win share per node
  - failover_ms estimate from a sustained run (gap around fail_at_unix)
  - convergence proxy (first success processed_by failed node after revive_at_unix)

Examples:
  python stress/report/analyze_request_csv.py --input stress/results/e1_udp --out stress/results/e1_udp/report
  python stress/report/analyze_request_csv.py --input stress/results/e4_fail20/fail_c30.csv \\
      --fail-at-unix 1710000010 --failed-node 2 --revive-at-unix 1710000030
"""
from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


def percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    ys = sorted(xs)
    k = (len(ys) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ys[int(k)]
    return ys[f] * (c - k) + ys[c] * (k - f)


def mean_std(xs: list[float]) -> tuple[float, float]:
    if not xs:
        return float("nan"), float("nan")
    if len(xs) == 1:
        return xs[0], 0.0
    return statistics.mean(xs), statistics.stdev(xs)


def fmt_pm(m: float, s: float, d: int = 2) -> str:
    if math.isnan(m):
        return ""
    return f"{m:.{d}f}+/-{s:.{d}f}"


def load_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def summarize_file(rows: list[dict], clients_hint: int | None = None) -> dict:
    ok_rows = [r for r in rows if str(r.get("ok")) in {"1", "True", "true"}]
    lat = [float(r["latency_ms"]) for r in ok_rows]
    if rows:
        wall = max(float(r["ts_recv"]) for r in rows) - min(float(r["ts_send"]) for r in rows)
    else:
        wall = 0.0
    wall = max(wall, 1e-9)
    clients = clients_hint
    if clients is None:
        clients = len({r.get("client_id") for r in rows}) or 1
    wins: dict[str, int] = defaultdict(int)
    for r in ok_rows:
        pb = str(r.get("processed_by") or "").strip()
        if pb != "":
            wins[pb] += 1
    return {
        "clients": clients,
        "n": len(rows),
        "ok": len(ok_rows),
        "error_rate": 1.0 - (len(ok_rows) / max(len(rows), 1)),
        "throughput_rps": len(ok_rows) / wall,
        "lat_mean": statistics.mean(lat) if lat else float("nan"),
        "lat_p50": percentile(lat, 50),
        "lat_p95": percentile(lat, 95),
        "lat_p99": percentile(lat, 99),
        "wins": dict(wins),
    }


def parse_name_hints(stem: str) -> tuple[int | None, int | None]:
    repeat, clients = None, None
    for part in stem.replace("-", "_").split("_"):
        if part.startswith("r") and part[1:].isdigit():
            repeat = int(part[1:])
        if part.startswith("c") and part[1:].isdigit():
            clients = int(part[1:])
    return repeat, clients


def failover_ms(
    rows: list[dict],
    fail_at: float,
    failed_node: str,
    window_pre: float = 2.0,
) -> dict:
    """Estimate failover: last ok that needed failed node before fail -> first ok by others after."""
    before = [
        r
        for r in rows
        if float(r["ts_recv"]) < fail_at
        and str(r.get("ok")) == "1"
        and str(r.get("processed_by")) == failed_node
    ]
    after_others = [
        r
        for r in rows
        if float(r["ts_recv"]) >= fail_at
        and str(r.get("ok")) == "1"
        and str(r.get("processed_by")) != failed_node
        and str(r.get("processed_by") or "") != ""
    ]
    # success rate in [fail_at, fail_at+20]
    during = [r for r in rows if fail_at <= float(r["ts_send"]) < fail_at + 20.0]
    during_ok = [r for r in during if str(r.get("ok")) == "1"]
    last_before = max((float(r["ts_recv"]) for r in before), default=fail_at - window_pre)
    first_after = min((float(r["ts_recv"]) for r in after_others), default=float("nan"))
    fo = (first_after - fail_at) * 1000.0 if not math.isnan(first_after) else float("nan")
    return {
        "fail_at_unix": fail_at,
        "failed_node": failed_node,
        "failover_ms": fo,
        "success_rate_during_20s": (len(during_ok) / max(len(during), 1)) if during else float("nan"),
        "n_during": len(during),
        "ok_during": len(during_ok),
        "last_success_on_failed_before": last_before,
        "first_success_on_other_after": first_after,
    }


def convergence_ms(rows: list[dict], revive_at: float, node: str) -> dict:
    after = [
        r
        for r in rows
        if float(r["ts_recv"]) >= revive_at
        and str(r.get("ok")) == "1"
        and str(r.get("processed_by")) == node
    ]
    first = min((float(r["ts_recv"]) for r in after), default=float("nan"))
    conv = (first - revive_at) * 1000.0 if not math.isnan(first) else float("nan")
    return {
        "revive_at_unix": revive_at,
        "node": node,
        "convergence_ms": conv,
        "first_win_after_revive": first,
    }


def write_table_a(agg: list[dict], path: Path, title: str) -> None:
    lines = [
        f"# {title}",
        "",
        "From per-request CSVs (udp_load_client). mean +/- sample stdev over repeats.",
        "",
        "| Clients | Repeats | Throughput (req/s) mean+/-s | Error rate mean+/-s | p50 ms | p95 ms | p99 ms |",
        "|--------:|--------:|----------------------------:|--------------------:|-------:|-------:|-------:|",
    ]
    for r in agg:
        lines.append(
            f"| {r['clients']} | {r['repeats']} | "
            f"{fmt_pm(r['thr_m'], r['thr_s'])} | "
            f"{fmt_pm(r['err_m'], r['err_s'], 3)} | "
            f"{fmt_pm(r['p50_m'], r['p50_s'], 1)} | "
            f"{fmt_pm(r['p95_m'], r['p95_s'], 1)} | "
            f"{fmt_pm(r['p99_m'], r['p99_s'], 1)} |"
        )
    lines += [
        "",
        "## Pass check (project)",
        "- Throughput rises then flattens under saturation.",
        "- Error rate stays low at moderate load.",
        "- p95/p99 reported (not mean alone).",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="Folder of CSVs or one CSV file")
    ap.add_argument("--out", required=True)
    ap.add_argument("--fail-at-unix", type=float, default=None)
    ap.add_argument("--failed-node", default="2")
    ap.add_argument("--revive-at-unix", type=float, default=None)
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    files = [inp] if inp.is_file() else sorted(inp.rglob("*.csv"))
    files = [f for f in files if f.name not in {"summary.csv", "win_share.csv"} and "report" not in f.parts]
    if not files:
        raise SystemExit(f"no CSV under {inp}")

    by_clients: dict[int, list[dict]] = defaultdict(list)
    all_rows: list[dict] = []
    for f in files:
        rows = load_csv(f)
        all_rows.extend(rows)
        _rep, clients = parse_name_hints(f.stem)
        summary = summarize_file(rows, clients)
        if clients is None:
            clients = int(summary["clients"])
        summary["source"] = str(f)
        by_clients[clients].append(summary)

    agg = []
    for clients in sorted(by_clients):
        g = by_clients[clients]
        thr = [x["throughput_rps"] for x in g]
        err = [x["error_rate"] for x in g]
        p50 = [x["lat_p50"] for x in g]
        p95 = [x["lat_p95"] for x in g]
        p99 = [x["lat_p99"] for x in g]
        thr_m, thr_s = mean_std(thr)
        err_m, err_s = mean_std(err)
        p50_m, p50_s = mean_std(p50)
        p95_m, p95_s = mean_std(p95)
        p99_m, p99_s = mean_std(p99)
        agg.append(
            {
                "clients": clients,
                "repeats": len(g),
                "thr_m": thr_m,
                "thr_s": thr_s,
                "err_m": err_m,
                "err_s": err_s,
                "p50_m": p50_m,
                "p50_s": p50_s,
                "p95_m": p95_m,
                "p95_s": p95_s,
                "p99_m": p99_m,
                "p99_s": p99_s,
            }
        )

    write_table_a(agg, out / "summary.md", f"Phase 1 stress: `{inp.name}`")
    with (out / "summary.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(agg[0].keys()) if agg else ["clients"])
        w.writeheader()
        for r in agg:
            w.writerow(r)

    # win share overall and by clients
    wins_by_c: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for clients, glist in by_clients.items():
        for s in glist:
            for nid, cnt in s["wins"].items():
                wins_by_c[clients][nid] += cnt
    lines = [
        "# Win share (processed_by_node)",
        "",
        "| Clients | " + " | ".join(f"N{n}" for n in sorted({n for d in wins_by_c.values() for n in d}, key=lambda x: int(x) if x.isdigit() else x)) + " |",
    ]
    # rebuild cleanly
    nodes = sorted({n for d in wins_by_c.values() for n in d}, key=lambda x: int(x) if str(x).isdigit() else str(x))
    lines = [
        "# Win share (processed_by_node)",
        "",
        "| Clients | " + " | ".join(f"N{n} share" for n in nodes) + " |",
        "|--------:|" + "|".join(["-------:"] * len(nodes)) + "|",
    ]
    for clients in sorted(wins_by_c):
        tot = sum(wins_by_c[clients].values()) or 1
        cells = [str(clients)] + [f"{wins_by_c[clients].get(n, 0) / tot:.3f}" for n in nodes]
        lines.append("| " + " | ".join(cells) + " |")
    lines += ["", "Pass: under uneven load, hot node share drops (Table B).", ""]
    (out / "win_share.md").write_text("\n".join(lines), encoding="utf-8")

    if args.fail_at_unix is not None:
        fo = failover_ms(all_rows, args.fail_at_unix, str(args.failed_node))
        with (out / "failover.json").open("w", encoding="utf-8") as fh:
            import json

            json.dump(fo, fh, indent=2)
        (out / "table_d_failure.md").write_text(
            "\n".join(
                [
                    "# Table D — Failure under load",
                    "",
                    f"| Clients | Mute s | Success rate during mute | failover_ms |",
                    f"|--------:|-------:|-------------------------:|------------:|",
                    f"| (see run) | 20 | {fo['success_rate_during_20s']:.3f} | {fo['failover_ms']:.1f} |",
                    "",
                    f"failed_node={fo['failed_node']} fail_at={fo['fail_at_unix']}",
                    "",
                    "Pass: during silence, other nodes still answer (success_rate > 0).",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print(f"failover_ms={fo['failover_ms']:.1f} during_ok_rate={fo['success_rate_during_20s']:.3f}")

    if args.revive_at_unix is not None:
        cv = convergence_ms(all_rows, args.revive_at_unix, str(args.failed_node))
        import json

        (out / "convergence.json").write_text(json.dumps(cv, indent=2), encoding="utf-8")
        (out / "table_e_rejoin.md").write_text(
            "\n".join(
                [
                    "# Table E — Rejoin / convergence",
                    "",
                    f"| Node | convergence_ms |",
                    f"|-----:|---------------:|",
                    f"| {cv['node']} | {cv['convergence_ms']:.1f} |",
                    "",
                    "Pass: node appears in processed_by again only after revive/sync.",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print(f"convergence_ms={cv['convergence_ms']:.1f}")

    total_ok = sum(1 for r in all_rows if str(r.get("ok")) == "1")
    print(f"Wrote {out}  total_ok_requests={total_ok}")
    if total_ok < 1000:
        print("NOTE: project asks for large volume where feasible; consider more clients/requests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
