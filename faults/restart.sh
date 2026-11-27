#!/usr/bin/env bash
# Restart a crashed node (or all nodes with "all").
source "$(dirname "$0")/lib.sh"
target="${1:?usage: restart.sh <kvN|all>}"
[[ "$target" == "all" ]] && targets="$(all_nodes)" || targets="$target"
for n in $targets; do
  log "starting $n"
  docker start "$n" >/dev/null
done
