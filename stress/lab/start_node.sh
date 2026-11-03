#!/usr/bin/env bash
# Start a cloud node. Pass node id 1|2|3.
#   bash stress/lab/start_node.sh 1
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/stress/lab/nodes.env"

NID="${1:-}"
if [[ -z "$NID" ]]; then
  echo "usage: $0 <RM_NODE_ID 1|2|3>" >&2
  exit 2
fi
export RM_NODE_ID="$NID"
echo "Starting node id=$RM_NODE_ID RM_NODES=$RM_NODES"
cd "$ROOT"
exec cargo run --release -- node
