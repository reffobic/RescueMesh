#!/usr/bin/env bash
# Collect election frequency/duration for the scale/stress report.
#
# ON EACH CLOUD PC (PC1/2/3) start the node with tee FIRST:
#   export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
#   export RM_NODE_ID=1   # 2 or 3 on other PCs
#   cargo run --release -- node 2>&1 | tee /tmp/node${RM_NODE_ID}.log
#
# Then ON PC4 run this script (generates load so ELECTION lines appear):
#   bash stress/report/run_election_overhead.sh
#
# Copy logs to PC4 if needed:
#   scp cse1@pc1:/tmp/node1.log /tmp/
#   scp cse1@pc2:/tmp/node2.log /tmp/
#   scp cse1@pc3:/tmp/node3.log /tmp/
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/stress/lab/nodes.env" 2>/dev/null || true

CLIENTS="${CLIENTS:-25}"
REQUESTS="${REQUESTS:-40}"
OUT="${OUT:-$ROOT/stress/results/election_overhead}"
LOG1="${LOG1:-/tmp/node1.log}"
LOG2="${LOG2:-/tmp/node2.log}"
LOG3="${LOG3:-/tmp/node3.log}"

echo "=== Election overhead: short load (${CLIENTS} clients x ${REQUESTS} req) ==="
python3 "$ROOT/stress/report/udp_load_client.py" \
  --hosts "$ROOT/stress/report/hosts.json" \
  --clients "$CLIENTS" \
  --requests "$REQUESTS" \
  --out "$OUT/load_for_elections.csv"

missing=0
for f in "$LOG1" "$LOG2" "$LOG3"; do
  if [[ ! -f "$f" ]]; then
    echo "MISSING log: $f  (tee nodes on PC1-3, or scp logs here)"
    missing=1
  fi
done
if [[ "$missing" -eq 1 ]]; then
  echo "Load finished. Fix logs, then re-run parse only:"
  echo "  python3 stress/report/parse_election_logs.py --logs $LOG1 $LOG2 $LOG3 --out $OUT"
  exit 2
fi

python3 "$ROOT/stress/report/parse_election_logs.py" \
  --logs "$LOG1" "$LOG2" "$LOG3" \
  --out "$OUT"

echo "Done -> $OUT/election_overhead.md"
