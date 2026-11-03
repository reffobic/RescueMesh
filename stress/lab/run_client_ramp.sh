#!/usr/bin/env bash
# Ramp simulated users and capture CLIENT_METRICS from cargo client.
# Usage:
#   source stress/lab/nodes.env
#   bash stress/lab/run_client_ramp.sh [output_dir]
#
# Env overrides:
#   RAMP_USERS="1 5 10 20 40"   # default
#   RM_CLIENT_REQUESTS_PER_USER  # default 20
#   RM_CLIENT_MAX_IN_FLIGHT      # default 32
#   RM_CLIENT_TIMEOUT_MS         # default 8000
#   RAMP_PAUSE_S                 # default 2
#   USE_RELEASE=0                # set 0 for debug cargo run
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${1:-$ROOT/stress/results/lab_ramp}"
mkdir -p "$OUT_DIR"

if [[ -z "${RM_NODES:-}" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/stress/lab/nodes.env"
fi

USERS_LIST="${RAMP_USERS:-1 5 10 20 40}"
export RM_CLIENT_REQUESTS_PER_USER="${RM_CLIENT_REQUESTS_PER_USER:-20}"
export RM_CLIENT_MAX_IN_FLIGHT="${RM_CLIENT_MAX_IN_FLIGHT:-32}"
export RM_CLIENT_TIMEOUT_MS="${RM_CLIENT_TIMEOUT_MS:-8000}"
PAUSE_S="${RAMP_PAUSE_S:-2}"
USE_RELEASE="${USE_RELEASE:-1}"

HOST_TAG="$(hostname 2>/dev/null || echo host)"
export RM_CLIENT_ID="${RM_CLIENT_ID:-ramp-${HOST_TAG}-$$}"

BIN=(cargo run)
if [[ "$USE_RELEASE" == "1" ]]; then
  BIN=(cargo run --release)
fi

echo "=== RescueMesh client ramp ==="
echo "cwd=$ROOT"
echo "RM_NODES=$RM_NODES"
echo "RM_CLIENT_ID=$RM_CLIENT_ID"
echo "users=$USERS_LIST req_per_user=$RM_CLIENT_REQUESTS_PER_USER out=$OUT_DIR"
echo

cd "$ROOT"
META="$OUT_DIR/meta.txt"
{
  echo "started=$(date -Iseconds 2>/dev/null || date)"
  echo "host=$HOST_TAG"
  echo "RM_NODES=$RM_NODES"
  echo "RM_CLIENT_ID=$RM_CLIENT_ID"
  echo "RM_CLIENT_REQUESTS_PER_USER=$RM_CLIENT_REQUESTS_PER_USER"
  echo "RM_CLIENT_MAX_IN_FLIGHT=$RM_CLIENT_MAX_IN_FLIGHT"
  echo "RM_CLIENT_TIMEOUT_MS=$RM_CLIENT_TIMEOUT_MS"
  echo "git=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "users=$USERS_LIST"
} >"$META"

for u in $USERS_LIST; do
  export RM_CLIENT_USERS="$u"
  stamp="$(date +%Y%m%d_%H%M%S 2>/dev/null || echo run)"
  raw="$OUT_DIR/u${u}_${stamp}.log"
  metrics="$OUT_DIR/u${u}_${stamp}.metrics"
  echo "--- users=$u → $raw ---"
  set +e
  "${BIN[@]}" -- client 2>&1 | tee "$raw"
  rc=${PIPESTATUS[0]}
  set -e
  grep -E '^CLIENT_METRICS,' "$raw" | tee "$metrics" || true
  if [[ $rc -ne 0 ]]; then
    echo "WARN: client exit code $rc for users=$u" | tee -a "$OUT_DIR/errors.txt"
  fi
  sleep "$PAUSE_S"
done

echo
echo "Done. Parse with:"
echo "  python3 stress/lab/parse_client_metrics.py $OUT_DIR --out $OUT_DIR/report.csv"
