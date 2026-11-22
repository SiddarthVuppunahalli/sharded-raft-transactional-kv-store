#!/usr/bin/env bash
# Random fault schedule for running alongside a workload ("nemesis").
# Every INTERVAL seconds picks one fault (crash+restart, isolate+heal,
# delay+heal) for a random node, then heals. Stops after DURATION seconds.
#   DURATION=60 INTERVAL=8 faults/chaos.sh
source "$(dirname "$0")/lib.sh"
DURATION="${DURATION:-60}"; INTERVAL="${INTERVAL:-8}"
end=$((SECONDS + DURATION))
trap '"$FAULTS_DIR/heal.sh" >/dev/null 2>&1; "$FAULTS_DIR/restart.sh" all >/dev/null 2>&1' EXIT
while (( SECONDS < end )); do
  node="kv$(( RANDOM % NODES + 1 ))"
  case $(( RANDOM % 3 )) in
    0) "$FAULTS_DIR/crash.sh" "$node" >/dev/null; sleep $(( INTERVAL / 2 )); "$FAULTS_DIR/restart.sh" "$node" ;;
    1) "$FAULTS_DIR/partition.sh" isolate "$node" >/dev/null; sleep $(( INTERVAL / 2 )); "$FAULTS_DIR/heal.sh" "$node" ;;
    2) "$FAULTS_DIR/delay.sh" "$node" 200ms 50ms 5%; sleep $(( INTERVAL / 2 )); "$FAULTS_DIR/heal.sh" "$node" ;;
  esac
  sleep $(( INTERVAL / 2 ))
done
