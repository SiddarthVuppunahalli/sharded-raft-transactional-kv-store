#!/usr/bin/env bash
# Hard-crash a node (SIGKILL, no graceful shutdown).
#   faults/crash.sh kv2            crash a specific node
#   faults/crash.sh leader [range] crash the current leader of a range
source "$(dirname "$0")/lib.sh"
target="${1:?usage: crash.sh <kvN|leader> [range]}"
if [[ "$target" == "leader" ]]; then
  target="$(leader_of "${2:-1}")" || { log "no leader found"; exit 1; }
fi
log "crashing $target (SIGKILL)"
docker kill -s KILL "$target" >/dev/null
echo "$target"
