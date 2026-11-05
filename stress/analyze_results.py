#!/usr/bin/env python3
"""
Analyze load_client CSV folders → summary tables + plots.

Expects:
  input_dir/
    meta.json (optional)
    r{repeat}_c{clients}.csv  OR  **/*.csv

Writes:
  out_dir/summary.csv
  out_dir/summary.md
  out_dir/fig_latency.png
  out_dir/fig_throughput.png
  out_dir/fig_errors.png
  out_dir/fig_winshare.png (if processed_by present)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def percentile(x: np.ndarray, p: float) -> float:
    if len(x) == 0:
        return float("nan")
    return float(np.percentile(x, p))


def load_runs(input_dir: Path) -> pd.DataFrame:
    frames = []
    for csv in sorted(input_dir.rglob("*.csv")):
        if csv.name.startswith("summary"):
            continue
        df = pd.read_csv(csv)
        # Parse filename hints: r0_c25.csv
        stem = csv.stem
        repeat, clients = None, None
        parts = stem.replace("-", "_").split("_")
        for i, p in enumerate(parts):
            if p.startswith("r") and p[1:].isdigit():
                repeat = int(p[1:])
            if p.startswith("c") and p[1:].isdigit():
                clients = int(p[1:])
        df["source"] = str(csv.relative_to(input_dir))
        df["repeat"] = repeat if repeat is not None else 0
        if clients is not None:
            df["clients"] = clients
        elif "clients" not in df.columns:
            df["clients"] = df["client_id"].nunique() if "client_id" in df.columns else 1
        frames.append(df)
    if not frames:
        raise SystemExit(f"No CSV files under {input_dir}")
    return pd.concat(frames, ignore_index=True)


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (clients, repeat), g in df.groupby(["clients", "repeat"]):
        ok = g[g["ok"] == 1]
        lat = ok["latency_ms"].to_numpy() if len(ok) else np.array([])
        wall = float(g["ts_recv"].max() - g["ts_send"].min()) if len(g) else 1.0
        wall = max(wall, 1e-9)
        rows.append(
            {
                "clients": clients,
                "repeat": repeat,
                "n": len(g),
                "ok": int(ok.shape[0]),
                "error_rate": 1.0 - ok.shape[0] / max(len(g), 1),
                "throughput_rps": ok.shape[0] / wall,
                "lat_mean": float(np.mean(lat)) if len(lat) else np.nan,
                "lat_median": percentile(lat, 50),
                "lat_p95": percentile(lat, 95),
                "lat_p99": percentile(lat, 99),
                "under_repl_rate": float(ok["under_replicated"].mean()) if len(ok) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def aggregate_repeats(summary: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "error_rate",
        "throughput_rps",
        "lat_mean",
        "lat_median",
        "lat_p95",
        "lat_p99",
        "under_repl_rate",
    ]
    rows = []
    for clients, g in summary.groupby("clients"):
        row = {"clients": clients, "repeats": len(g)}
        for m in metrics:
            row[f"{m}_mean"] = g[m].mean()
            row[f"{m}_std"] = g[m].std(ddof=1) if len(g) > 1 else 0.0
        rows.append(row)
    return pd.DataFrame(rows).sort_values("clients")


def win_share(df: pd.DataFrame) -> pd.DataFrame:
    ok = df[df["ok"] == 1].copy()
    if ok.empty or ok["processed_by"].astype(str).eq("").all():
        return pd.DataFrame()
    ok["processed_by"] = ok["processed_by"].astype(str)
    counts = ok.groupby(["clients", "processed_by"]).size().reset_index(name="wins")
    totals = counts.groupby("clients")["wins"].transform("sum")
    counts["share"] = counts["wins"] / totals
    return counts


def plot_latency(agg: pd.DataFrame, out: Path) -> None:
    plt.figure(figsize=(8, 5))
    x = agg["clients"]
    for key, label in [
        ("lat_median_mean", "p50"),
        ("lat_p95_mean", "p95"),
        ("lat_p99_mean", "p99"),
    ]:
        y = agg[key]
        e = agg[key.replace("_mean", "_std")] if key.replace("_mean", "_std") in agg else None
        plt.errorbar(x, y, yerr=e, marker="o", label=label)
    plt.xlabel("Concurrent clients")
    plt.ylabel("Latency (ms)")
    plt.title("Request latency vs load")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out)
    plt.close()


def plot_throughput(agg: pd.DataFrame, out: Path) -> None:
    plt.figure(figsize=(8, 5))
    plt.errorbar(
        agg["clients"],
        agg["throughput_rps_mean"],
        yerr=agg["throughput_rps_std"],
        marker="o",
        color="green",
    )
    plt.xlabel("Concurrent clients")
    plt.ylabel("Throughput (successful req/s)")
    plt.title("Throughput vs load")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out)
    plt.close()


def plot_errors(agg: pd.DataFrame, out: Path) -> None:
    plt.figure(figsize=(8, 5))
    plt.errorbar(
        agg["clients"],
        agg["error_rate_mean"],
        yerr=agg["error_rate_std"],
        marker="o",
        color="red",
    )
    plt.xlabel("Concurrent clients")
    plt.ylabel("Error rate")
    plt.title("Error rate vs load")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out)
    plt.close()


def plot_winshare(shares: pd.DataFrame, out: Path) -> None:
    if shares.empty:
        return
    pivot = shares.pivot_table(index="clients", columns="processed_by", values="share", fill_value=0)
    pivot.plot(kind="bar", stacked=True, figsize=(8, 5))
    plt.ylabel("Win share")
    plt.xlabel("Concurrent clients")
    plt.title("Load distribution (processed_by_node)")
    plt.tight_layout()
    plt.savefig(out)
    plt.close()


def write_md(agg: pd.DataFrame, out: Path, input_dir: Path) -> None:
    lines = [
        f"# Stress report: `{input_dir.name}`",
        "",
        "Statistically aggregated over repeats (mean ± sample stdev).",
        "",
        "| clients | repeats | thr_rps | err | p50 ms | p95 ms | p99 ms |",
        "|--------:|--------:|--------:|----:|-------:|-------:|-------:|",
    ]
    for _, r in agg.iterrows():
        lines.append(
            f"| {int(r['clients'])} | {int(r['repeats'])} | "
            f"{r['throughput_rps_mean']:.2f}±{r['throughput_rps_std']:.2f} | "
            f"{r['error_rate_mean']:.3f}±{r['error_rate_std']:.3f} | "
            f"{r['lat_median_mean']:.1f}±{r['lat_median_std']:.1f} | "
            f"{r['lat_p95_mean']:.1f}±{r['lat_p95_std']:.1f} | "
            f"{r['lat_p99_mean']:.1f}±{r['lat_p99_std']:.1f} |"
        )
    lines += [
        "",
        "## How to read",
        "- **Throughput** should rise then flatten (saturation).",
        "- **p95/p99** matter more than the mean for RescueMesh demos.",
        "- Connect jumps to timeouts, plan cost, election, or replication quorum.",
        "",
    ]
    out.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Folder with per-run CSVs")
    ap.add_argument("--out", required=True, help="Output report folder")
    args = ap.parse_args()
    input_dir = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_runs(input_dir)
    summary = summarize(df)
    summary.to_csv(out_dir / "summary_by_repeat.csv", index=False)
    agg = aggregate_repeats(summary)
    agg.to_csv(out_dir / "summary.csv", index=False)
    write_md(agg, out_dir / "summary.md", input_dir)

    plot_latency(agg, out_dir / "fig_latency.png")
    plot_throughput(agg, out_dir / "fig_throughput.png")
    plot_errors(agg, out_dir / "fig_errors.png")
    shares = win_share(df)
    if not shares.empty:
        shares.to_csv(out_dir / "win_share.csv", index=False)
        plot_winshare(shares, out_dir / "fig_winshare.png")

    print(f"Wrote report to {out_dir}")
    print(agg.to_string(index=False))


if __name__ == "__main__":
    main()
