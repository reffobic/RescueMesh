#!/usr/bin/env bash
# One-request smoke test. Exit 0 only if CLIENT_METRICS shows succeeded>=1.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/stress/lab/nodes.env"

export RM_CLIENT_ID="${RM_CLIENT_ID:-smoke-$(hostname)-$$}"
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_MAX_IN_FLIGHT=1
export RM_CLIENT_TIMEOUT_MS="${RM_CLIENT_TIMEOUT_MS:-5000}"

cd "$ROOT"
echo "Smoke: RM_NODES=$RM_NODES client=$RM_CLIENT_ID"
OUT="$(mktemp)"
set +e
cargo run --release -- client 2>&1 | tee "$OUT"
rc=${PIPESTATUS[0]}
set -e

if grep -qE 'CLIENT_METRICS,.*succeeded=[1-9]' "$OUT"; then
  echo "SMOKE PASS"
  rm -f "$OUT"
  exit 0
fi
echo "SMOKE FAIL (no succeeded>=1). See output above." >&2
rm -f "$OUT"
exit 1
