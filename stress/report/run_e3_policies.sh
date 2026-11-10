#!/usr/bin/env bash
# Optional E3 — metric policy comparison (Table C).
#
# IMPORTANT: Set RM_POLICY on ALL cloud nodes and restart them BEFORE each
# PC4 run below. Client-only RM_POLICY is not enough for a fair test.
#
# On PC1/PC2/PC3 for each policy P in weighted|queue|cpu|random:
#   export RM_POLICY=P
#   export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
#   export RM_NODE_ID=...
#   cargo run --release -- node
#
# Then on PC4 (this script runs one tag; pass policy name):
#   bash stress/report/run_e3_policies.sh weighted
#   bash stress/report/run_e3_policies.sh queue
#   bash stress/report/run_e3_policies.sh cpu
#   bash stress/report/run_e3_policies.sh random
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
POLICY="${1:-}"
if [[ -z "$POLICY" ]]; then
  echo "usage: $0 <weighted|queue|cpu|random>"
  echo "Restart ALL nodes with RM_POLICY=<same> before running."
  exit 2
fi
# shellcheck disable=SC1091
source "$ROOT/stress/lab/nodes.env" 2>/dev/null || true

echo "=== E3 policy=${POLICY} (nodes must already use RM_POLICY=${POLICY}) ==="
python3 "$ROOT/stress/report/run_phase1_suite.py" \
  --scenario e2 \
  --tag "policy_${POLICY}" \
  --clients 30

echo "Analyze all e2_imbalance CSVs (includes this policy tag):"
python3 "$ROOT/stress/report/analyze_request_csv.py" \
  --input "$ROOT/stress/results/e2_imbalance" \
  --out "$ROOT/stress/results/e2_imbalance/report"

echo "Done. Compare win_share / p95 across policy_* tags for Table C."
