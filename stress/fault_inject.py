#!/usr/bin/env python3
"""
Fault / imbalance injection helpers.

Modes:
  --mute-udp     Send AdminMute JSON to admin port (requires node support)
  --sim-load     Send AdminSimLoad JSON (extra_queue, cpu) for E2
  --print-only   Print what operator should do (SSH / env) — always safe

Example:
  python fault_inject.py --hosts configs/hosts.json --mute 1 --secs 10
  python fault_inject.py --sim-load 0 --extra-queue 15 --cpu 0.95
"""
from __future__ import annotations

import argparse
import json
import socket
import time
from pathlib import Path


def load_hosts(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def send_udp(host: str, port: int, obj: dict) -> None:
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.sendto(data, (host, port))
    finally:
        sock.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", default="configs/hosts.json")
    ap.add_argument("--mute", type=int, default=None, help="node id to mute")
    ap.add_argument("--secs", type=int, default=10)
    ap.add_argument("--sim-load", type=int, default=None, help="node id")
    ap.add_argument("--extra-queue", type=int, default=10)
    ap.add_argument("--cpu", type=float, default=0.9)
    ap.add_argument("--print-only", action="store_true")
    args = ap.parse_args()

    base = Path(__file__).resolve().parent
    hosts_path = Path(args.hosts)
    if not hosts_path.is_absolute():
        hosts_path = base / hosts_path
    hosts = load_hosts(hosts_path)
    peers = {p["id"]: p for p in hosts["peers"]}
    admin = {int(k): int(v) for k, v in hosts.get("admin_ports", {}).items()}

    if args.mute is not None:
        nid = args.mute
        peer = peers[nid]
        msg = {"AdminMute": {"secs": args.secs}}
        print(f"[fault] mute node {nid} for {args.secs}s via AdminMute")
        if args.print_only:
            print("MANUAL:", json.dumps(msg), "->", peer["host"], admin.get(nid, peer["port"]))
            print("ALT: call Node::mute_for on that process / SIGSTOP for secs then SIGCONT + catch-up")
        else:
            port = admin.get(nid, peer["port"])
            send_udp(peer["host"], port, msg)
            print(f"[fault] sent to {peer['host']}:{port}")

    if args.sim_load is not None:
        nid = args.sim_load
        peer = peers[nid]
        msg = {
            "AdminSimLoad": {
                "extra_queue": args.extra_queue,
                "cpu": args.cpu,
            }
        }
        print(f"[fault] sim_load node {nid} queue+={args.extra_queue} cpu={args.cpu}")
        if args.print_only:
            print("MANUAL: metrics.sim_load(", args.extra_queue, ",", args.cpu, ")")
            print("OR set env burn on that host only")
        else:
            port = admin.get(nid, peer["port"])
            send_udp(peer["host"], port, msg)

    if args.mute is None and args.sim_load is None:
        print("Nothing to do; pass --mute ID or --sim-load ID")


if __name__ == "__main__":
    main()
