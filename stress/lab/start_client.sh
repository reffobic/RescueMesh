#!/usr/bin/env bash
# Start a lab client against RM_NODES.
#   bash stress/lab/start_client.sh [client_id]
# Optional env: RM_CLIENT_USERS, RM_CLIENT_REQUESTS_PER_USER, etc.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/stress/lab/nodes.env"

export RM_CLIENT_ID="${1:-${RM_CLIENT_ID:-client-$(hostname)-$$}}"
export RM_CLIENT_USERS="${RM_CLIENT_USERS:-1}"
export RM_CLIENT_REQUESTS_PER_USER="${RM_CLIENT_REQUESTS_PER_USER:-1}"
export RM_CLIENT_MAX_IN_FLIGHT="${RM_CLIENT_MAX_IN_FLIGHT:-32}"
export RM_CLIENT_TIMEOUT_MS="${RM_CLIENT_TIMEOUT_MS:-5000}"

echo "Client id=$RM_CLIENT_ID users=$RM_CLIENT_USERS req/user=$RM_CLIENT_REQUESTS_PER_USER"
echo "RM_NODES=$RM_NODES"
cd "$ROOT"
exec cargo run --release -- client
