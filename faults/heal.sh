#!/usr/bin/env bash
# Remove all injected network faults (partitions + netem) from nodes.
#   faults/heal.sh            heal every running node
#   faults/heal.sh kv2        heal one node
source "$(dirname "$0")/lib.sh"
targets="${1:-$(all_nodes)}"
for n in $targets; do
  if [[ "$(docker inspect -f '{{.State.Running}}' "$n" 2>/dev/null)" != "true" ]]; then continue; fi
  docker exec "$n" iptables -F INPUT >/dev/null 2>&1 || true
  docker exec "$n" iptables -F OUTPUT >/dev/null 2>&1 || true
  docker exec "$n" tc qdisc del dev eth0 root >/dev/null 2>&1 || true
  log "healed $n"
done
