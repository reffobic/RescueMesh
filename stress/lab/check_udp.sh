#!/usr/bin/env bash
# Quick lab connectivity helper
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/stress/lab/nodes.env"
python3 "$ROOT/stress/lab/check_connectivity.py" --nodes "$RM_NODES"
