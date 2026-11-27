#!/usr/bin/env bash
# Network partition between groups of nodes using iptables inside the
# containers. Client traffic (from the tools container) is not blocked.
#
#   faults/partition.sh isolate kv1          kv1 cannot talk to any other node
#   faults/partition.sh isolate leader [r]   isolate the current leader of range r
#   faults/partition.sh split "kv1 kv2" "kv3"   two-sided partition
#   faults/partition.sh oneway kv1 kv2       drop packets kv1 -> kv2 only
source "$(dirname "$0")/lib.sh"

block() { # block traffic from $1 to $2 (both directions of the TCP flow die)
  docker exec "$1" iptables -A OUTPUT -d "$(node_ip "$2")" -j DROP
  docker exec "$1" iptables -A INPUT -s "$(node_ip "$2")" -j DROP
}

mode="${1:?usage: partition.sh isolate <kvN|leader> | split \"<a..>\" \"<b..>\" | oneway <from> <to>}"
case "$mode" in
  isolate)
    target="${2:?node}"
    if [[ "$target" == "leader" ]]; then
      target="$(leader_of "${3:-1}")" || { log "no leader found"; exit 1; }
    fi
    log "isolating $target from all other nodes"
    for n in $(all_nodes); do
      [[ "$n" == "$target" ]] && continue
      block "$target" "$n"; block "$n" "$target"
    done
    echo "$target"
    ;;
  split)
    log "partition: [$2] | [$3]"
    for a in $2; do for b in $3; do block "$a" "$b"; block "$b" "$a"; done; done
    ;;
  oneway)
    log "dropping packets $2 -> $3"
    docker exec "$2" iptables -A OUTPUT -d "$(node_ip "$3")" -j DROP
    ;;
  *) echo "unknown mode $mode" >&2; exit 1 ;;
esac
