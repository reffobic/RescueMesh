#!/usr/bin/env python3
"""Build Phase 1 report tables (mean ± sample stdev) from experiment runs.csv.

Examples:
  python stress/report/build_report_tables.py \\
      --input stress/results/e1_ramp_live --out stress/results/e1_ramp_live/report

  # Also fills blank Markdown skeletons:
  python stress/report/build_report_tables.py --input stress/results/e1_ramp_live \\
      --out stress/results/e1_ramp_live/report --templates
"""
from __future__ import annotations

import argparse
import ast
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


def fnum(x: str | float | None) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def mean_std(vals: list[float]) -> tuple[float, float]:
    if not vals:
        return float("nan"), float("nan")
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.mean(vals), statistics.stdev(vals)


def fmt_pm(mean: float, std: float, digits: int = 2) -> str:
    if math.isnan(mean):
        return ""
    return f"{mean:.{digits}f}±{std:.{digits}f}"


def parse_per_node(s: str) -> dict[str, float]:
    """Parse Rust Debug map like `{1: 4, 2: 3, 3: 2}` into shares later."""
    s = (s or "").strip()
    if not s:
        return {}
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, dict):
            return {str(k): float(v) for k, v in obj.items()}
    except (SyntaxError, ValueError):
        pass
    # fallback: 1: 4, 2: 3
    out: dict[str, float] = {}
    for part in s.replace("{", "").replace("}", "").split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        try:
            out[k.strip()] = float(v.strip())
        except ValueError:
            continue
    return out


def load_runs(input_dir: Path) -> list[dict[str, str]]:
    path = input_dir / "runs.csv"
    if not path.exists():
        raise SystemExit(f"Missing {path} — run run_phase1_experiments.py first")
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def aggregate_by_clients(rows: list[dict[str, str]]) -> list[dict]:
    groups: dict[int, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        c = fnum(r.get("clients"))
        if c is None:
            continue
        groups[int(c)].append(r)

    out = []
    for clients in sorted(groups):
        g = groups[clients]
        thr = [fnum(r["throughput_rps"]) for r in g if fnum(r.get("throughput_rps")) is not None]
        err = [fnum(r["error_rate"]) for r in g if fnum(r.get("error_rate")) is not None]
        p50 = [fnum(r["median_ms"]) for r in g if fnum(r.get("median_ms")) is not None]
        p95 = [fnum(r["p95_ms"]) for r in g if fnum(r.get("p95_ms")) is not None]
        p99 = [fnum(r["p99_ms"]) for r in g if fnum(r.get("p99_ms")) is not None]
        thr_m, thr_s = mean_std([x for x in thr if x is not None])  # type: ignore
        err_m, err_s = mean_std([x for x in err if x is not None])  # type: ignore
        p50_m, p50_s = mean_std([x for x in p50 if x is not None])  # type: ignore
        p95_m, p95_s = mean_std([x for x in p95 if x is not None])  # type: ignore
        p99_m, p99_s = mean_std([x for x in p99 if x is not None])  # type: ignore
        out.append(
            {
                "clients": clients,
                "repeats": len(g),
                "throughput_rps_mean": thr_m,
                "throughput_rps_std": thr_s,
                "error_rate_mean": err_m,
                "error_rate_std": err_s,
                "p50_mean": p50_m,
                "p50_std": p50_s,
                "p95_mean": p95_m,
                "p95_std": p95_s,
                "p99_mean": p99_m,
                "p99_std": p99_s,
            }
        )
    return out


def aggregate_by_policy(rows: list[dict[str, str]]) -> list[dict]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        p = (r.get("policy") or "").strip() or "default"
        groups[p].append(r)
    out = []
    for policy in sorted(groups):
        g = groups[policy]
        thr = [fnum(r["throughput_rps"]) for r in g if fnum(r.get("throughput_rps")) is not None]
        p95 = [fnum(r["p95_ms"]) for r in g if fnum(r.get("p95_ms")) is not None]
        thr_m, thr_s = mean_std([x for x in thr if x is not None])  # type: ignore
        p95_m, p95_s = mean_std([x for x in p95 if x is not None])  # type: ignore
        # Jain fairness over win counts summed across repeats
        totals: dict[str, float] = defaultdict(float)
        for r in g:
            for nid, cnt in parse_per_node(r.get("per_node", "")).items():
                totals[nid] += cnt
        jain = jain_index(list(totals.values())) if totals else float("nan")
        out.append(
            {
                "policy": policy,
                "repeats": len(g),
                "p95_mean": p95_m,
                "p95_std": p95_s,
                "throughput_rps_mean": thr_m,
                "throughput_rps_std": thr_s,
                "jain_fairness": jain,
                "win_totals": dict(totals),
            }
        )
    return out


def jain_index(xs: list[float]) -> float:
    if not xs:
        return float("nan")
    s = sum(xs)
    q = sum(x * x for x in xs)
    if q <= 0:
        return float("nan")
    return (s * s) / (len(xs) * q)


def win_share_by_clients(rows: list[dict[str, str]]) -> list[dict]:
    # Sum wins per node per client count, then share
    by_c: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in rows:
        c = fnum(r.get("clients"))
        if c is None:
            continue
        for nid, cnt in parse_per_node(r.get("per_node", "")).items():
            by_c[int(c)][nid] += cnt
    out = []
    for clients in sorted(by_c):
        totals = by_c[clients]
        s = sum(totals.values()) or 1.0
        row = {"clients": clients}
        for nid in sorted(totals, key=lambda x: int(x) if x.isdigit() else x):
            row[f"node_{nid}_share"] = totals[nid] / s
            row[f"node_{nid}_wins"] = totals[nid]
        out.append(row)
    return out


def write_table_a(agg: list[dict], path: Path, title: str) -> None:
    lines = [
        f"# {title}",
        "",
        "Statistically aggregated over repeats (mean ± sample stdev).",
        "",
        "| Clients | Repeats | Throughput (req/s) mean±σ | Error rate mean±σ | p50 ms | p95 ms | p99 ms |",
        "|--------:|--------:|---------------------------:|------------------:|-------:|-------:|-------:|",
    ]
    for r in agg:
        lines.append(
            f"| {r['clients']} | {r['repeats']} | "
            f"{fmt_pm(r['throughput_rps_mean'], r['throughput_rps_std'])} | "
            f"{fmt_pm(r['error_rate_mean'], r['error_rate_std'], 3)} | "
            f"{fmt_pm(r['p50_mean'], r['p50_std'], 1)} | "
            f"{fmt_pm(r['p95_mean'], r['p95_std'], 1)} | "
            f"{fmt_pm(r['p99_mean'], r['p99_std'], 1)} |"
        )
    lines += [
        "",
        "## How to read",
        "- **Throughput** should rise then flatten (saturation).",
        "- **p95/p99** matter more than the mean for RescueMesh demos.",
        "- Connect jumps to timeouts, plan cost, election, or replication quorum.",
        "",
        "**Design link:** _______________________________________________",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def write_table_c(agg: list[dict], path: Path) -> None:
    lines = [
        "# Table C — Metric policy comparison (E3)",
        "",
        "| Policy | p95 ms | Throughput | Jain fairness (wins) | Election p95 µs |",
        "|--------|-------:|-----------:|---------------------:|----------------:|",
    ]
    for r in agg:
        lines.append(
            f"| {r['policy']} | "
            f"{fmt_pm(r['p95_mean'], r['p95_std'], 1)} | "
            f"{fmt_pm(r['throughput_rps_mean'], r['throughput_rps_std'])} | "
            f"{r['jain_fairness']:.3f} | |"
        )
    lines += [
        "",
        "Election p95 µs: fill from node logs / election instrumentation if available.",
        "",
        "**Design link:** _______________________________________________",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def write_win_share_md(shares: list[dict], path: Path) -> None:
    if not shares:
        path.write_text("# Win share\n\n(no per_node data)\n", encoding="utf-8")
        return
    # union of node columns
    nodes: set[str] = set()
    for r in shares:
        for k in r:
            if k.startswith("node_") and k.endswith("_share"):
                nodes.add(k[len("node_") : -len("_share")])
    node_list = sorted(nodes, key=lambda x: int(x) if x.isdigit() else x)
    header = "| Clients | " + " | ".join(f"N{n} share" for n in node_list) + " |"
    sep = "|--------:|" + "|".join(["-------:"] * len(node_list)) + "|"
    lines = ["# Load distribution (processed_by / per_node)", "", header, sep]
    for r in shares:
        cells = [str(r["clients"])]
        for n in node_list:
            v = r.get(f"node_{n}_share")
            cells.append(f"{v:.3f}" if isinstance(v, float) else "")
        lines.append("| " + " | ".join(cells) + " |")
    lines += ["", "**Design link:** load-aware election should spread work under imbalance.", ""]
    path.write_text("\n".join(lines), encoding="utf-8")


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def copy_blank_templates(out: Path) -> None:
    src = Path(__file__).resolve().parent / "templates" / "report_tables.md"
    if src.exists():
        (out / "report_tables_blank.md").write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="Experiment folder containing runs.csv")
    ap.add_argument("--out", required=True, help="Report output folder")
    ap.add_argument("--templates", action="store_true", help="Also copy blank Table A–E skeletons")
    args = ap.parse_args()

    input_dir = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_runs(input_dir)
    name = rows[0].get("experiment", input_dir.name) if rows else input_dir.name
    table = (rows[0].get("table") if rows else "") or ""

    by_clients = aggregate_by_clients(rows)
    write_csv(by_clients, out_dir / "summary.csv")
    write_csv(rows, out_dir / "summary_by_repeat.csv")
    write_table_a(by_clients, out_dir / "summary.md", f"Stress report: `{name}` (Table A style)")

    shares = win_share_by_clients(rows)
    write_csv(shares, out_dir / "win_share.csv")
    write_win_share_md(shares, out_dir / "win_share.md")

    if any((r.get("policy") or "").strip() for r in rows) or table == "C":
        by_pol = aggregate_by_policy(rows)
        # flatten win_totals for csv
        flat = []
        for r in by_pol:
            d = {k: v for k, v in r.items() if k != "win_totals"}
            for nid, cnt in (r.get("win_totals") or {}).items():
                d[f"wins_node_{nid}"] = cnt
            flat.append(d)
        write_csv(flat, out_dir / "summary_by_policy.csv")
        write_table_c(by_pol, out_dir / "table_c_policies.md")

    if args.templates:
        copy_blank_templates(out_dir)

    # Optional plots
    try:
        import matplotlib.pyplot as plt  # type: ignore

        if by_clients:
            xs = [r["clients"] for r in by_clients]
            plt.figure(figsize=(8, 5))
            for key, label in [("p50_mean", "p50"), ("p95_mean", "p95"), ("p99_mean", "p99")]:
                ys = [r[key] for r in by_clients]
                es = [r[key.replace("_mean", "_std")] for r in by_clients]
                plt.errorbar(xs, ys, yerr=es, marker="o", label=label)
            plt.xlabel("Concurrent clients (RM_CLIENT_USERS)")
            plt.ylabel("Latency (ms)")
            plt.title("Request latency vs load")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(out_dir / "fig_latency.png")
            plt.close()

            plt.figure(figsize=(8, 5))
            plt.errorbar(
                xs,
                [r["throughput_rps_mean"] for r in by_clients],
                yerr=[r["throughput_rps_std"] for r in by_clients],
                marker="o",
                color="green",
            )
            plt.xlabel("Concurrent clients")
            plt.ylabel("Throughput (req/s)")
            plt.title("Throughput vs load")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(out_dir / "fig_throughput.png")
            plt.close()
            print("Wrote fig_latency.png fig_throughput.png")
    except ImportError:
        print("matplotlib not installed — skipped plots (tables still written)")

    print(f"Wrote report to {out_dir}")
    print(f"  - summary.md  (paste Table A)")
    print(f"  - summary.csv")
    print(f"  - win_share.md / win_share.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
