#!/usr/bin/env bash
# Add latency / jitter / loss to a node's network interface with tc netem.
#   faults/delay.sh kv2 100ms            100ms delay on kv2
#   faults/delay.sh all 50ms 10ms 1%     50ms +-10ms and 1% loss on every node
#   faults/delay.sh wan                  realistic cross-region topology
source "$(dirname "$0")/lib.sh"
target="${1:?usage: delay.sh <kvN|all|wan> [delay] [jitter] [loss]}"
delay="${2:-100ms}"; jitter="${3:-0ms}"; loss="${4:-0%}"

netem() { # node delay jitter loss
  local args=(delay "$2")
  [[ "$3" != "0ms" && "$3" != "0" ]] && args+=("$3" distribution normal)
  [[ "$4" != "0%" && "$4" != "0" ]] && args+=(loss "$4")
  docker exec "$1" tc qdisc replace dev eth0 root netem "${args[@]}"
}

case "$target" in
  all) for n in $(all_nodes); do log "netem $n: $delay +-$jitter loss $loss"; netem "$n" "$delay" "$jitter" "$loss"; done ;;
  wan)
    # Approximates three regions: kv1 local, kv2 ~40ms away, kv3 ~80ms away.
    # One-way egress delay per node; RTT between two nodes is the sum.
    log "WAN topology: kv1=5ms kv2=20ms kv3=40ms one-way (+-20% jitter, 0.1% loss)"
    netem kv1 5ms 1ms 0.1%
    netem kv2 20ms 4ms 0.1%
    netem kv3 40ms 8ms 0.1%
    if [[ "$NODES" == "5" ]]; then netem kv4 20ms 4ms 0.1%; netem kv5 40ms 8ms 0.1%; fi
    ;;
  *) log "netem $target: $delay +-$jitter loss $loss"; netem "$target" "$delay" "$jitter" "$loss" ;;
esac
