#!/usr/bin/env bash
# Shared helpers for fault-injection scripts. Nodes are containers kv1..kvN.
set -euo pipefail
FAULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$FAULTS_DIR/.." && pwd)"
NODES="${NODES:-3}"

compose() {
  if [[ "$NODES" == "5" ]]; then
    docker compose -f "$ROOT/docker-compose.yml" -f "$ROOT/deploy/docker-compose.5node.yml" "$@"
  else
    docker compose -f "$ROOT/docker-compose.yml" "$@"
  fi
}

all_nodes() { for i in $(seq 1 "$NODES"); do echo "kv$i"; done; }
node_ip() { echo "172.28.0.1${1#kv}"; }
log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# Run kvctl from inside the tools container.
kvctl() { compose run --rm -T tools kvctl "$@" 2>/dev/null; }

# Prints the container name leading a range (default range 1), or nothing.
leader_of() {
  local id
  for _ in 1 2 3 4 5; do
    id="$(kvctl leader "${1:-1}" | tail -1)"
    if [[ -n "$id" && "$id" != "0" ]]; then echo "kv$id"; return 0; fi
    sleep 0.5
  done
  return 1
}
