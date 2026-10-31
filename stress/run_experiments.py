#!/usr/bin/env python3
"""
Drive Phase-1 stress experiment configs (YAML).

Example:
  python run_experiments.py --config configs/e1_ramp.yaml --dry-run
  python run_experiments.py --config configs/e1_ramp.yaml
  python analyze_results.py --input results/e1_ramp --out results/e1_ramp/report
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import yaml

from load_client import run_load


def resolve(base: Path, p: str | Path) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (base / path)


def run_one_config(cfg: dict, base: Path, dry_run: bool) -> None:
    name = cfg["name"]
    out_dir = resolve(base, cfg.get("output_dir", f"results/{name}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    hosts = resolve(base, cfg.get("hosts", "configs/hosts.json"))
    repeats = int(cfg.get("repeats", 5))
    clients_list = list(cfg.get("client_counts", [10]))
    rpc = int(cfg.get("requests_per_client", 20))
    timeout = float(cfg.get("timeout_s", 5.0))
    warmup = float(cfg.get("warmup_s", 0))
    pause = float(cfg.get("pause_between_repeats_s", 1))
    send_ack = not cfg.get("no_ack", False)

    meta = {
        "name": name,
        "description": cfg.get("description", ""),
        "dry_run": dry_run,
        "config": cfg,
        "started_unix": time.time(),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # Optional imbalance prep
    imb = cfg.get("imbalance")
    if imb and not dry_run:
        inj = base / "fault_inject.py"
        cmd = [
            sys.executable,
            str(inj),
            "--hosts",
            str(hosts),
            "--sim-load",
            str(imb["node_id"]),
            "--extra-queue",
            str(imb.get("extra_queue", 10)),
            "--cpu",
            str(imb.get("cpu", 0.9)),
            "--print-only",  # safe default until AdminSimLoad exists
        ]
        print("[run] imbalance hook:", " ".join(cmd))
        subprocess.run(cmd, check=False)

    failure = cfg.get("failure")

    for clients in clients_list:
        for rep in range(repeats):
            if warmup > 0:
                print(f"[run] warmup {warmup}s ...")
                time.sleep(warmup)

            # Schedule mute mid-run for failure experiments (best-effort background)
            mute_proc = None
            if failure and not dry_run:
                nid = int(failure["node_id"])
                start_after = float(failure.get("start_after_s", 5))
                mute_secs_list = failure.get("mute_secs", [10])
                # Use first mute length for this simple driver; advanced: nest loops
                secs = int(mute_secs_list[0]) if isinstance(mute_secs_list, list) else int(mute_secs_list)

                def _mute():
                    time.sleep(start_after)
                    cmd = [
                        sys.executable,
                        str(base / "fault_inject.py"),
                        "--hosts",
                        str(hosts),
                        "--mute",
                        str(nid),
                        "--secs",
                        str(secs),
                        "--print-only",
                    ]
                    print("[run] failure inject:", " ".join(cmd))
                    subprocess.run(cmd, check=False)

                import threading

                mute_proc = threading.Thread(target=_mute, daemon=True)
                mute_proc.start()

            out_csv = out_dir / f"r{rep}_c{clients}.csv"
            print(f"[run] {name} clients={clients} repeat={rep} -> {out_csv.name}")
            run_load(
                hosts,
                clients,
                rpc,
                timeout,
                send_ack,
                dry_run,
                out_csv,
            )
            if mute_proc:
                mute_proc.join(timeout=1)
            time.sleep(pause)

    # Nested mute_secs × clients for e4-style (optional explicit loop)
    if failure and isinstance(failure.get("mute_secs"), list) and len(failure["mute_secs"]) > 1:
        nest = out_dir / "by_mute_secs"
        nest.mkdir(exist_ok=True)
        print(
            "[run] NOTE: multi mute_secs lengths — re-run manually per length or extend driver; "
            f"primary runs used mute_secs[0]={failure['mute_secs'][0]}"
        )

    print(f"[run] done. Analyze with:\n  python analyze_results.py --input {out_dir} --out {out_dir / 'report'}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    base = Path(__file__).resolve().parent
    cfg_path = resolve(base, args.config)
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    dry = args.dry_run or bool(cfg.get("force_dry_run", False))
    run_one_config(cfg, base, dry)


if __name__ == "__main__":
    main()
