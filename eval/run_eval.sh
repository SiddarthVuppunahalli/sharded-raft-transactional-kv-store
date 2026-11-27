#!/usr/bin/env bash
# End-to-end evaluation: builds the cluster, runs every scenario, injects
# faults during active workloads, and writes results to results/<timestamp>/.
#
#   eval/run_eval.sh                 full suite (~10 min)
#   QUICK=1 eval/run_eval.sh         shorter runs (~4 min)
#   NODES=5 eval/run_eval.sh         5-node cluster
#   SCENARIOS="baseline failover" eval/run_eval.sh   subset
#
# Scenarios: baseline scaling failover partition delay linearizability bank router
# ("router" runs workloads through the Part 2 metadata router over HTTP; it is
# not in the default list because it needs the 2_SAM_Branch code.)
set -euo pipefail
source "$(dirname "$0")/../faults/lib.sh"

if [[ "$NODES" == "5" ]]; then
  export KV_PEERS="1=kv1:7000,2=kv2:7000,3=kv3:7000,4=kv4:7000,5=kv5:7000"
  export KV_ADDRS="kv1:7000,kv2:7000,kv3:7000,kv4:7000,kv5:7000"
fi
D="${DURATION:-30}"; [[ -n "${QUICK:-}" ]] && D=15
FAILOVER_TRIALS="${FAILOVER_TRIALS:-5}"; [[ -n "${QUICK:-}" ]] && FAILOVER_TRIALS=3
SCENARIOS="${SCENARIOS:-baseline scaling failover partition delay linearizability bank}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT/results/$STAMP"; mkdir -p "$OUT"
SUMMARY="/results/$STAMP/summary.md"   # path inside the tools container
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

bench() { # label, extra kvbench args...
  local label="$1"; shift
  compose run --rm -T tools kvbench --label "$label" --out "/results/$STAMP/$label.json" \
    --summary-md "$SUMMARY" "$@" 2>&1 | grep -v -E "Container .* (Creating|Created)" | tee "$OUT/$label.log"
}

# Fresh containers per scenario: clears injected faults and in-memory state
# so scenarios are independent and the (in-memory) Raft log stays small.
reset_cluster() {
  compose up -d --force-recreate --no-build $(all_nodes) >/dev/null 2>&1
  compose run --rm -T tools kvctl --timeout 30s wait-ready >/dev/null 2>&1
}

log "building and starting $NODES-node cluster"
compose build -q >/dev/null 2>&1
reset_cluster
{
  echo "# Evaluation results ($STAMP)"
  echo
  echo "- Nodes: $NODES, ranges: split at '${KV_SPLITS:-m}', replication factor $NODES"
  echo "- Run duration per scenario: ${D}s"
  echo "- Host: $(uname -sm), Docker $(docker version --format '{{.Server.Version}}')"
  echo
  echo "## Workload results"
  echo
  echo "| Scenario | Workload | Ops/s | OK | Errors | Read p50 / p99 (ms) | Write p50 / p99 (ms) | Max unavailability (ms) | Correctness |"
  echo "|---|---|---:|---:|---:|---:|---:|---:|---|"
} > "$OUT/summary.md"

for s in $SCENARIOS; do
  case "$s" in
  baseline)
    log "scenario: baseline (read-heavy, mixed, write-heavy)"
    reset_cluster
    bench baseline-read95  --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.95
    reset_cluster
    bench baseline-mixed50 --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
    reset_cluster
    bench baseline-write95 --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.05
    ;;
  scaling)
    log "scenario: throughput vs concurrency"
    for c in 1 4 16 64; do
      reset_cluster
      bench "scaling-c$c" --workload kv --duration "$(( D / 2 ))s" --concurrency "$c" --read-ratio 0.5
    done
    ;;
  failover)
    log "scenario: leader crash during active workload"
    reset_cluster
    ( sleep $(( D / 3 )); "$FAULTS_DIR/crash.sh" leader 1 >/dev/null
      sleep $(( D / 3 )); "$FAULTS_DIR/restart.sh" all ) &
    bench failover-leader-crash --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
    wait
    reset_cluster
    log "scenario: follower crash during active workload"
    ( sleep $(( D / 3 )); leader="$(leader_of 1)"
      for n in $(all_nodes); do [[ "$n" != "$leader" ]] && { "$FAULTS_DIR/crash.sh" "$n" >/dev/null; break; }; done
      sleep $(( D / 3 )); "$FAULTS_DIR/restart.sh" all ) &
    bench failover-follower-crash --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
    wait
    log "measuring leader failover time over $FAILOVER_TRIALS trials"
    echo "trial,fault,old_leader,new_leader,detect_to_leader_ms,fault_to_leader_ms,fault_to_first_write_ms" > "$OUT/failover.csv"
    for t in $(seq 1 "$FAILOVER_TRIALS"); do
      for fault in crash isolate; do
        reset_cluster
        compose run --rm -T tools kvctl --timeout 30s failover-probe 1 > "$OUT/probe.json" 2>/dev/null &
        probe=$!
        sleep 2
        t_fault="$(now_ms)"
        if [[ "$fault" == crash ]]; then "$FAULTS_DIR/crash.sh" leader 1 >/dev/null
        else "$FAULTS_DIR/partition.sh" isolate leader 1 >/dev/null; fi
        wait "$probe" || true
        python3 - "$OUT/probe.json" "$t_fault" "$t" "$fault" >> "$OUT/failover.csv" <<'PY'
import json, sys
line = [l for l in open(sys.argv[1]) if l.startswith("{")][-1]
p = json.loads(line); tf = int(sys.argv[2])
print(f'{sys.argv[3]},{sys.argv[4]},{p["old_leader"]},{p["new_leader"]},{p["election_ms"]},{p["t_leader_unix_ms"]-tf},{p["t_write_unix_ms"]-tf}')
PY
        tail -1 "$OUT/failover.csv"
      done
    done
    rm -f "$OUT/probe.json"
    ;;
  partition)
    log "scenario: leader isolated by network partition during workload"
    reset_cluster
    ( sleep $(( D / 3 )); "$FAULTS_DIR/partition.sh" isolate leader 1 >/dev/null
      sleep $(( D / 3 )); "$FAULTS_DIR/heal.sh" >/dev/null ) &
    bench partition-leader-isolated --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
    wait
    reset_cluster
    log "scenario: minority/majority split during workload"
    ( sleep $(( D / 3 )); "$FAULTS_DIR/partition.sh" split "kv1" "$(all_nodes | grep -v '^kv1$' | tr '\n' ' ')" >/dev/null
      sleep $(( D / 3 )); "$FAULTS_DIR/heal.sh" >/dev/null ) &
    bench partition-split-kv1 --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
    wait
    ;;
  delay)
    for spec in "lan:all 2ms 1ms" "wan:wan" "lossy:all 10ms 5ms 2%"; do
      name="${spec%%:*}"; args="${spec#*:}"
      reset_cluster
      log "scenario: network profile $name ($args)"
      # shellcheck disable=SC2086
      "$FAULTS_DIR/delay.sh" $args >/dev/null
      bench "delay-$name" --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
      "$FAULTS_DIR/heal.sh" >/dev/null
    done
    ;;
  linearizability)
    reset_cluster
    log "scenario: linearizability (no faults)"
    bench linearizability-clean --workload linear --duration "${D}s" --concurrency 8 --keys 8 --read-ratio 0.5 \
      --visualize "/results/$STAMP/linearizability-clean.html"
    reset_cluster
    log "scenario: linearizability under random crashes / partitions / delays"
    ( DURATION=$(( D * 2 - 5 )) INTERVAL=6 "$FAULTS_DIR/chaos.sh" > "$OUT/chaos.log" 2>&1 ) &
    bench linearizability-chaos --workload linear --duration "$(( D * 2 ))s" --concurrency 8 --keys 8 --read-ratio 0.5 \
      --visualize "/results/$STAMP/linearizability-chaos.html"
    wait
    ;;
  bank)
    reset_cluster
    log "scenario: cross-range bank transfers (transaction atomicity)"
    bench txn-bank-clean --workload bank --duration "${D}s" --concurrency 8 --accounts 10
    reset_cluster
    ( DURATION=$(( D - 5 )) INTERVAL=6 "$FAULTS_DIR/chaos.sh" > "$OUT/chaos-bank.log" 2>&1 ) &
    bench txn-bank-chaos --workload bank --duration "${D}s" --concurrency 8 --accounts 10
    wait
    ;;
  router)
    reset_router() { reset_cluster; "$ROOT/deploy/router.sh" up >/dev/null; }
    R="--http-url http://metadata:8080"
    reset_router
    log "scenario: mixed workload through the metadata router"
    # shellcheck disable=SC2086
    bench router-mixed50 $R --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5
    reset_router
    log "scenario: leader crash through the metadata router"
    ( sleep $(( D / 3 )); "$FAULTS_DIR/crash.sh" leader 1 >/dev/null
      sleep $(( D / 3 )); "$FAULTS_DIR/restart.sh" all ) &
    # shellcheck disable=SC2086
    bench router-failover-leader-crash $R --workload kv --duration "${D}s" --concurrency 16 --read-ratio 0.5 --op-timeout 8s
    wait
    reset_router
    log "scenario: linearizability through the router under random faults"
    ( DURATION=$(( D * 2 - 5 )) INTERVAL=6 "$FAULTS_DIR/chaos.sh" > "$OUT/chaos-router.log" 2>&1 ) &
    # shellcheck disable=SC2086
    bench router-linearizability-chaos $R --workload linear --duration "$(( D * 2 ))s" --concurrency 8 --keys 8 --read-ratio 0.5 --op-timeout 8s
    wait
    "$ROOT/deploy/router.sh" down >/dev/null
    ;;
  *) log "unknown scenario $s" ;;
  esac
done

reset_cluster
if [[ -f "$OUT/failover.csv" ]]; then
  {
    echo
    echo "## Leader failover time (range 1)"
    echo
    python3 - "$OUT/failover.csv" <<'PY'
import csv, sys, statistics as st
rows = list(csv.DictReader(open(sys.argv[1])))
print("| Fault | Trials | Fault → new leader, mean (min–max) ms | Fault → first write, mean (min–max) ms |")
print("|---|---:|---:|---:|")
for f in ("crash", "isolate"):
    r = [x for x in rows if x["fault"] == f]
    if not r: continue
    a = [int(x["fault_to_leader_ms"]) for x in r]; b = [int(x["fault_to_first_write_ms"]) for x in r]
    print(f"| leader {f} | {len(r)} | {st.mean(a):.0f} ({min(a)}–{max(a)}) | {st.mean(b):.0f} ({min(b)}–{max(b)}) |")
PY
  } >> "$OUT/summary.md"
fi
python3 "$ROOT/eval/report.py" "$OUT" >/dev/null 2>&1 && log "HTML report: results/$STAMP/report.html" || true
log "done — results in results/$STAMP/"
cat "$OUT/summary.md"
