#!/usr/bin/env bash
# Start the Part 2 metadata router against the running KV cluster and
# register the range map (range ids, bounds, replicas and current leaders).
#
#   deploy/router.sh up        extract router code, start it, register ranges
#   deploy/router.sh register  re-register ranges with current leaders
#   deploy/router.sh down      stop the router
#
# ROUTER_REF picks the git ref holding 2-SharingAndMetadata
# (default origin/feature/2_SAM_Branch). Router API: http://localhost:18080
set -euo pipefail
source "$(dirname "$0")/../faults/lib.sh"
ROUTER_REF="${ROUTER_REF:-origin/feature/2_SAM_Branch}"
SPLITS="${KV_SPLITS:-m}"
URL="http://localhost:18080"
if [[ "$NODES" == "5" ]]; then
  export RANGE_NODES="1=kv1:8080,2=kv2:8080,3=kv3:8080,4=kv4:8080,5=kv5:8080"
  compose() { docker compose -f "$ROOT/docker-compose.yml" -f "$ROOT/deploy/docker-compose.5node.yml" "$@"; }
fi
rcompose() { compose -f "$ROOT/deploy/docker-compose.router.yml" "$@"; }

extract() {
  log "extracting router code from $ROUTER_REF"
  rm -rf "$ROOT/.router" && mkdir -p "$ROOT/.router"
  git -C "$ROOT" fetch -q origin 2>/dev/null || true
  git -C "$ROOT" archive "$ROUTER_REF" 2-SharingAndMetadata | tar -x -C "$ROOT/.router"
}

register() {
  local leaders version body
  leaders="$(kvctl leaders | tail -1)"
  version="$(curl -sf "$URL/api/metadata/ranges")"
  body="$(python3 - "$SPLITS" "$NODES" "$leaders" "$version" <<'PY'
import json, sys
splits = [s for s in sys.argv[1].split(",") if s]
nodes = list(range(1, int(sys.argv[2]) + 1))
leaders = {int(k): v for k, v in json.loads(sys.argv[3] or "{}").items()}
current = json.loads(sys.argv[4])
bounds = [None] + sorted(splits) + [None]
ranges = []
for i in range(len(bounds) - 1):
    rid = i + 1
    ranges.append({"id": rid, "startKey": bounds[i], "endKey": bounds[i + 1],
                   "replicaIds": nodes, "leaderId": leaders.get(rid, nodes[0])})
cmd = {"type": "replace_ranges", "expectedVersion": current["version"], "ranges": ranges}
if current["ranges"]:
    cmd["removeRangeIds"] = [r["id"] for r in current["ranges"]]
print(json.dumps(cmd))
PY
)"
  curl -sf -X POST -H 'Content-Type: application/json' -d "$body" "$URL/api/metadata/commands" >/dev/null
  log "registered ranges: $(curl -sf "$URL/api/metadata/ranges")"
}

case "${1:-up}" in
  up)
    extract
    rcompose up -d --force-recreate metadata >/dev/null 2>&1
    for _ in $(seq 1 50); do curl -sf "$URL/healthz" >/dev/null 2>&1 && break; sleep 0.2; done
    curl -sf "$URL/healthz" >/dev/null || { docker logs metadata 2>&1 | tail -5; log "router failed to start"; exit 1; }
    # The metadata service answers 503 until its own Raft group has a leader.
    for _ in $(seq 1 100); do curl -sf "$URL/api/metadata/ranges" >/dev/null 2>&1 && break; sleep 0.2; done
    compose run --rm -T tools kvctl --timeout 30s wait-ready >/dev/null 2>&1
    registered=""
    for attempt in 1 2 3 4 5 6 7 8 9 10; do
      if register; then registered=1; break; fi
      log "register failed (attempt $attempt), retrying"; sleep 1
    done
    [[ -n "$registered" ]] || { log "could not register ranges with the router"; exit 1; }
    log "router ready at $URL (inside the cluster network: http://metadata:8080)"
    ;;
  register) register ;;
  down) rcompose rm -sf metadata >/dev/null 2>&1; log "router stopped" ;;
  *) echo "usage: router.sh up|register|down" >&2; exit 1 ;;
esac
