#!/usr/bin/env python3
"""Compare this machine's IPv4 addresses to the lab map.

Usage:
  python3 stress/lab/verify_ips.py
  python3 stress/lab/verify_ips.py --expect 10.40.32.242
"""
from __future__ import annotations

import argparse
import socket
import subprocess
import sys

LAB = {
    "10.40.32.242": "PC1 cloud node 1 (:7001)",
    "10.40.2.29": "PC2 cloud node 2 (:7002)",
    "10.40.0.73": "PC3 cloud node 3 (:7003)",
    "10.40.1.240": "PC4 client",
    "10.40.39.134": "PC5 client",
    "10.40.5.45": "PC6 client",
}


def local_ipv4s() -> list[str]:
    found: set[str] = set()
    # hostname -I (Linux)
    try:
        r = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=3)
        if r.returncode == 0:
            for tok in r.stdout.split():
                if tok.count(".") == 3:
                    found.add(tok.strip())
    except Exception:
        pass
    # fallback: connect trick for primary outbound IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        found.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    # getaddrinfo on hostname
    try:
        host = socket.gethostname()
        for info in socket.getaddrinfo(host, None, socket.AF_INET):
            found.add(info[4][0])
    except Exception:
        pass
    return sorted(found)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--expect", help="Exact IP this PC should have")
    args = ap.parse_args()

    ips = local_ipv4s()
    print(f"Local IPv4(s): {', '.join(ips) if ips else '(none detected)'}")
    print("Lab map matches:")
    matched = []
    for ip in ips:
        if ip in LAB:
            print(f"  [OK] {ip} -> {LAB[ip]}")
            matched.append(ip)
        else:
            print(f"  [..] {ip} (not in default lab table — update RM_NODES if this is your cloud/client IP)")

    if args.expect:
        if args.expect in ips:
            print(f"PASS: expected IP {args.expect} is on this machine.")
            return 0
        print(f"FAIL: expected {args.expect} but local IPs are {ips}")
        return 1

    if matched:
        print("PASS: at least one lab IP is present on this host.")
        return 0
    print("WARN: no lab-table IP found. Run `hostname -I` and update stress/lab/nodes.env on ALL PCs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
