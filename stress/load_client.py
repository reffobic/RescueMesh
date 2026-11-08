#!/usr/bin/env python3
"""
Concurrent RescueMesh load client (JSON Msg over UDP).

Emits one CSV row per attempt:
  ts_send,ts_recv,client_id,request_id,ok,latency_ms,processed_by,under_replicated,error

Usage:
  python load_client.py --hosts configs/hosts.json --clients 20 --requests 50 --out results/run.csv
  python load_client.py --dry-run --clients 5 --requests 10 --out results/dry.csv
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Any


CATEGORIES = [
    "BlockedRoad",
    "PowerOutage",
    "LabIncident",
    "LocalizedFlooding",
    "NetworkOutage",
    "MedicalTechnicalAssistance",
]

_print_lock = threading.Lock()


def load_hosts(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def peer_addrs(hosts: dict[str, Any]) -> list[tuple[str, int]]:
    return [(p["host"], int(p["port"])) for p in hosts["peers"]]


def encode_request(req: dict[str, Any], reply_ip: str, reply_port: int) -> bytes:
    """Build serde-json-ish Request message. Adjust if Rust SocketAddr shape differs."""
    msg = {
        "Request": {
            "req": req,
            "reply_to": f"{reply_ip}:{reply_port}",
        }
    }
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def encode_ack(request_id: str) -> bytes:
    return json.dumps({"Ack": {"request_id": request_id}}, separators=(",", ":")).encode("utf-8")


def parse_reply(data: bytes) -> tuple[bool, dict[str, Any] | None, bool, str]:
    try:
        obj = json.loads(data.decode("utf-8"))
    except Exception as e:
        return False, None, False, f"json:{e}"
    if "Reply" in obj:
        reply = obj["Reply"]
        plan = reply.get("plan") or {}
        under = bool(reply.get("under_replicated", False))
        return True, plan, under, ""
    if "Busy" in obj:
        return False, None, False, "busy"
    return False, None, False, f"unexpected:{list(obj)[:3]}"


def guess_reply_ip(peers: list[tuple[str, int]]) -> str:
    """Local IP that can reach the first peer (same trick as cargo client)."""
    if not peers:
        return "0.0.0.0"
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(peers[0])
        ip = probe.getsockname()[0]
        probe.close()
        return ip
    except OSError:
        return "0.0.0.0"


def one_request(
    client_id: str,
    peers: list[tuple[str, int]],
    multicast: bool,
    timeout_s: float,
    send_ack: bool,
    dry_run: bool,
) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)
    req = {
        "request_id": request_id,
        "incident_id": f"inc-{random.randint(1, 10_000)}",
        "reporter_id": client_id,
        "category": random.choice(CATEGORIES),
        "location_zone": random.randint(0, 19),
        "severity": random.randint(1, 5),
        "timestamp_ms": now_ms,
        "status": "Open",
        "ttl_secs": 300,
    }

    ts_send = time.time()
    if dry_run:
        # Synthetic latency model for pipeline testing without servers.
        time.sleep(random.uniform(0.02, 0.08))
        ok = random.random() > 0.02
        return {
            "ts_send": ts_send,
            "ts_recv": time.time(),
            "client_id": client_id,
            "request_id": request_id,
            "ok": int(ok),
            "latency_ms": random.uniform(30, 120) if ok else timeout_s * 1000,
            "processed_by": random.randint(1, 3) if ok else "",
            "under_replicated": 0,
            "error": "" if ok else "dry_timeout",
        }

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("0.0.0.0", 0))
        local_port = sock.getsockname()[1]
        reply_ip = guess_reply_ip(peers)
        payload = encode_request(req, reply_ip, local_port)

        targets = peers if multicast else [random.choice(peers)]
        for addr in targets:
            try:
                sock.sendto(payload, addr)
            except OSError:
                continue

        deadline = time.time() + timeout_s
        last_err = "timeout"
        resends = 0
        while time.time() < deadline:
            remaining = deadline - time.time()
            sock.settimeout(max(0.05, min(1.0, remaining)))
            try:
                data, src = sock.recvfrom(65535)
            except socket.timeout:
                if resends < 4 and time.time() < deadline:
                    resends += 1
                    for addr in targets:
                        try:
                            sock.sendto(payload, addr)
                        except OSError:
                            continue
                continue
            ok, plan, under, err = parse_reply(data)
            if not ok:
                last_err = err
                continue
            assert plan is not None
            if plan.get("request_id") != request_id:
                continue
            if send_ack:
                try:
                    sock.sendto(encode_ack(request_id), src)
                except OSError:
                    pass
            ts_recv = time.time()
            return {
                "ts_send": ts_send,
                "ts_recv": ts_recv,
                "client_id": client_id,
                "request_id": request_id,
                "ok": 1,
                "latency_ms": (ts_recv - ts_send) * 1000.0,
                "processed_by": plan.get("processed_by_node", ""),
                "under_replicated": int(under),
                "error": "",
            }
        return {
            "ts_send": ts_send,
            "ts_recv": time.time(),
            "client_id": client_id,
            "request_id": request_id,
            "ok": 0,
            "latency_ms": (time.time() - ts_send) * 1000.0,
            "processed_by": "",
            "under_replicated": 0,
            "error": last_err,
        }
    except Exception as e:
        return {
            "ts_send": ts_send,
            "ts_recv": time.time(),
            "client_id": client_id,
            "request_id": request_id,
            "ok": 0,
            "latency_ms": (time.time() - ts_send) * 1000.0,
            "processed_by": "",
            "under_replicated": 0,
            "error": str(e),
        }
    finally:
        sock.close()


def client_worker(
    cid: int,
    n_req: int,
    peers: list[tuple[str, int]],
    multicast: bool,
    timeout_s: float,
    send_ack: bool,
    dry_run: bool,
) -> list[dict[str, Any]]:
    client_id = f"c{cid}"
    rows = []
    for _ in range(n_req):
        rows.append(
            one_request(client_id, peers, multicast, timeout_s, send_ack, dry_run)
        )
    return rows


CSV_HEADER = (
    "ts_send,ts_recv,client_id,request_id,ok,latency_ms,processed_by,under_replicated,error\n"
)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(CSV_HEADER)
        for r in rows:
            f.write(
                f"{r['ts_send']},{r['ts_recv']},{r['client_id']},{r['request_id']},"
                f"{r['ok']},{r['latency_ms']:.3f},{r['processed_by']},{r['under_replicated']},"
                f"{r['error']}\n"
            )


def run_load(
    hosts_path: Path,
    n_clients: int,
    requests_per_client: int,
    timeout_s: float,
    send_ack: bool,
    dry_run: bool,
    out_csv: Path,
) -> dict[str, Any]:
    hosts = (
        {"peers": [{"host": "127.0.0.1", "port": 9000}], "multicast": True}
        if dry_run
        else load_hosts(hosts_path)
    )
    peers = peer_addrs(hosts)
    multicast = bool(hosts.get("multicast", True))
    t_wall0 = time.time()
    rows: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_clients) as ex:
        futs = [
            ex.submit(
                client_worker,
                i,
                requests_per_client,
                peers,
                multicast,
                timeout_s,
                send_ack,
                dry_run,
            )
            for i in range(n_clients)
        ]
        for fut in concurrent.futures.as_completed(futs):
            rows.extend(fut.result())
    t_wall1 = time.time()
    write_csv(out_csv, rows)
    ok = sum(1 for r in rows if r["ok"])
    wall = max(t_wall1 - t_wall0, 1e-9)
    summary = {
        "clients": n_clients,
        "attempts": len(rows),
        "ok": ok,
        "error_rate": 1.0 - ok / max(len(rows), 1),
        "throughput_rps": ok / wall,
        "wall_s": wall,
        "out_csv": str(out_csv),
    }
    with _print_lock:
        print(json.dumps(summary))
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", default="configs/hosts.json")
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--requests", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--no-ack", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    base = Path(__file__).resolve().parent
    hosts = Path(args.hosts)
    if not hosts.is_absolute():
        hosts = base / hosts
    run_load(
        hosts,
        args.clients,
        args.requests,
        args.timeout,
        not args.no_ack,
        args.dry_run,
        Path(args.out),
    )


if __name__ == "__main__":
    main()
