#!/usr/bin/env python3
"""Check that lab cloud IPs respond to ICMP (optional) and UDP ports are reachable.

Usage:
  python3 stress/lab/check_connectivity.py
  python3 stress/lab/check_connectivity.py --nodes "1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
"""
from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import time


DEFAULT_NODES = "1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"


def parse_nodes(s: str) -> list[tuple[str, str, int]]:
    out = []
    for part in s.split(","):
        part = part.strip()
        nid, addr = part.split("@", 1)
        host, port = addr.rsplit(":", 1)
        out.append((nid, host, int(port)))
    return out


def ping(host: str, timeout_s: float = 2.0) -> bool:
    # Linux ping; ignore failure if ICMP blocked
    try:
        r = subprocess.run(
            ["ping", "-c", "1", "-W", str(max(1, int(timeout_s))), host],
            capture_output=True,
            text=True,
            timeout=timeout_s + 2,
        )
        return r.returncode == 0
    except Exception:
        return False


def udp_probe(host: str, port: int, timeout_s: float = 1.0) -> str:
    """Send a tiny datagram. 'sent' means OS accepted send; reply is bonus if node is up."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout_s)
    try:
        sock.sendto(b"{}", (host, port))
        try:
            data, _ = sock.recvfrom(4096)
            return f"udp_sent+reply({len(data)}B)"
        except socket.timeout:
            return "udp_sent_no_reply (OK if node ignores unknown JSON; firewall may still be open)"
    except OSError as e:
        return f"udp_FAIL: {e}"
    finally:
        sock.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", default=DEFAULT_NODES)
    args = ap.parse_args()
    nodes = parse_nodes(args.nodes)
    print(f"Local time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        hostname = socket.gethostname()
        print(f"Hostname: {hostname}")
    except Exception:
        pass

    ok = True
    for nid, host, port in nodes:
        pong = ping(host)
        udp = udp_probe(host, port)
        print(f"node {nid} {host}:{port}  ping={'OK' if pong else 'no/blocked'}  {udp}")
        if udp.startswith("udp_FAIL"):
            ok = False

    print()
    if ok:
        print("PASS: UDP send to all node ports succeeded from this machine.")
        print("Next: start nodes, then run: cargo run --release -- client")
        return 0
    print("FAIL: fix network/firewall/IPs, then re-run.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
