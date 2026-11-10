#!/usr/bin/env python3
"""Per-request UDP load client for Phase 1 stress (statistically meaningful CSVs).

Emits one CSV row per attempt:
  ts_send,ts_recv,client_id,request_id,ok,latency_ms,processed_by,under_replicated,error

This is what failover_ms / win-share / p95 need — batch CLIENT_METRICS alone is not enough.

Examples:
  python stress/report/udp_load_client.py --hosts stress/report/hosts.json \\
      --clients 20 --requests 50 --out stress/results/e1/r0_c20.csv

  # Sustained window for 20s failure test (operator kills a node mid-run):
  python stress/report/udp_load_client.py --hosts stress/report/hosts.json \\
      --clients 30 --duration-s 60 --rate-per-client 5 \\
      --out stress/results/e4/fail20_c30.csv
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import random
import socket
import threading
import time
import uuid
from pathlib import Path

CATEGORIES = [
    "BlockedRoad",
    "PowerOutage",
    "LabIncident",
    "LocalizedFlooding",
    "NetworkOutage",
    "MedicalTechnicalAssistance",
]


def load_peers(hosts_path: Path, nodes_env: str | None) -> list[tuple[int, str, int]]:
    if nodes_env:
        out = []
        for part in nodes_env.split(","):
            nid, addr = part.strip().split("@", 1)
            host, port = addr.rsplit(":", 1)
            out.append((int(nid), host, int(port)))
        return out
    data = json.loads(hosts_path.read_text(encoding="utf-8"))
    return [(int(p["id"]), p["host"], int(p["port"])) for p in data["peers"]]


def encode_request(req: dict, reply_ip: str, reply_port: int) -> bytes:
    # serde SocketAddr as string "ip:port"
    msg = {"Request": {"req": req, "reply_to": f"{reply_ip}:{reply_port}"}}
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def encode_ack(request_id: str) -> bytes:
    return json.dumps({"Ack": {"request_id": request_id}}, separators=(",", ":")).encode("utf-8")


def parse_reply(data: bytes) -> tuple[bool, dict | None, bool, str]:
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


def one_request(
    client_id: str,
    peers: list[tuple[int, str, int]],
    timeout_s: float,
    send_ack: bool,
    dry_run: bool,
) -> dict:
    request_id = str(uuid.uuid4())
    req = {
        "request_id": request_id,
        "incident_id": f"inc-{random.randint(1, 10_000)}",
        "reporter_id": client_id,
        "category": random.choice(CATEGORIES),
        "location_zone": random.randint(0, 19),
        "severity": random.randint(1, 5),
        "timestamp_ms": int(time.time() * 1000),
        "status": "Open",
        "ttl_secs": 300,
    }
    ts_send = time.time()
    if dry_run:
        time.sleep(random.uniform(0.02, 0.08))
        ok = random.random() > 0.02
        return {
            "ts_send": ts_send,
            "ts_recv": time.time(),
            "client_id": client_id,
            "request_id": request_id,
            "ok": 1 if ok else 0,
            "latency_ms": (time.time() - ts_send) * 1000,
            "processed_by": random.randint(1, 3) if ok else "",
            "under_replicated": 0,
            "error": "" if ok else "dry_fail",
        }

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout_s)
    try:
        sock.bind(("0.0.0.0", 0))
        local_port = sock.getsockname()[1]
        # guess reply IP via connect trick to first peer
        reply_ip = "0.0.0.0"
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.connect((peers[0][1], peers[0][2]))
            reply_ip = probe.getsockname()[0]
            probe.close()
        except OSError:
            pass
        payload = encode_request(req, reply_ip, local_port)
        for _nid, host, port in peers:
            try:
                sock.sendto(payload, (host, port))
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
                    for _nid, host, port in peers:
                        try:
                            sock.sendto(payload, (host, port))
                        except OSError:
                            continue
                continue
            ok, plan, under, err = parse_reply(data)
            if not ok:
                last_err = err
                if err == "busy":
                    continue
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
                "latency_ms": (ts_recv - ts_send) * 1000,
                "processed_by": plan.get("processed_by_node", ""),
                "under_replicated": 1 if under else 0,
                "error": "",
            }
        return {
            "ts_send": ts_send,
            "ts_recv": time.time(),
            "client_id": client_id,
            "request_id": request_id,
            "ok": 0,
            "latency_ms": (time.time() - ts_send) * 1000,
            "processed_by": "",
            "under_replicated": 0,
            "error": last_err,
        }
    finally:
        sock.close()


def run_fixed_requests(
    peers: list[tuple[int, str, int]],
    clients: int,
    requests: int,
    timeout_s: float,
    send_ack: bool,
    dry_run: bool,
    out_path: Path,
) -> None:
    rows: list[dict] = []
    lock = threading.Lock()

    def worker(cid: int) -> None:
        client_id = f"c{cid}"
        local = []
        for _ in range(requests):
            local.append(one_request(client_id, peers, timeout_s, send_ack, dry_run))
        with lock:
            rows.extend(local)

    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as ex:
        futs = [ex.submit(worker, i) for i in range(clients)]
        for f in concurrent.futures.as_completed(futs):
            f.result()
    _write_csv(out_path, rows)


def run_duration(
    peers: list[tuple[int, str, int]],
    clients: int,
    duration_s: float,
    rate_per_client: float,
    timeout_s: float,
    send_ack: bool,
    dry_run: bool,
    out_path: Path,
) -> None:
    """Open-loop for failure/rejoin windows: each client aims for rate_per_client req/s."""
    stop_at = time.time() + duration_s
    rows: list[dict] = []
    lock = threading.Lock()
    interval = 1.0 / max(rate_per_client, 0.01)

    def worker(cid: int) -> None:
        client_id = f"c{cid}"
        local = []
        while time.time() < stop_at:
            t0 = time.time()
            local.append(one_request(client_id, peers, timeout_s, send_ack, dry_run))
            sleep_for = interval - (time.time() - t0)
            if sleep_for > 0:
                time.sleep(sleep_for)
        with lock:
            rows.extend(local)

    print(
        f"[udp_load] duration={duration_s}s clients={clients} "
        f"target_rps~={clients * rate_per_client:.1f} -> {out_path}"
    )
    print(
        "[udp_load] FAILURE TEST: when prompted, Ctrl+C the chosen cloud node, wait mute_s, restart it."
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as ex:
        futs = [ex.submit(worker, i) for i in range(clients)]
        for f in concurrent.futures.as_completed(futs):
            f.result()
    _write_csv(out_path, rows)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "ts_send",
        "ts_recv",
        "client_id",
        "request_id",
        "ok",
        "latency_ms",
        "processed_by",
        "under_replicated",
        "error",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    ok = sum(1 for r in rows if int(r["ok"]) == 1)
    print(f"[udp_load] wrote {len(rows)} rows ok={ok} -> {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hosts", default="stress/report/hosts.json")
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--requests", type=int, default=20, help="Per client (fixed mode)")
    ap.add_argument("--duration-s", type=float, default=0, help="If >0, sustained mode")
    ap.add_argument("--rate-per-client", type=float, default=2.0)
    ap.add_argument("--timeout-s", type=float, default=5.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--no-ack", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    hosts = Path(args.hosts)
    peers = load_peers(hosts, os.environ.get("RM_NODES"))
    if not peers:
        raise SystemExit("no peers")
    out = Path(args.out)
    if args.duration_s > 0:
        run_duration(
            peers,
            args.clients,
            args.duration_s,
            args.rate_per_client,
            args.timeout_s,
            not args.no_ack,
            args.dry_run,
            out,
        )
    else:
        run_fixed_requests(
            peers,
            args.clients,
            args.requests,
            args.timeout_s,
            not args.no_ack,
            args.dry_run,
            out,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
