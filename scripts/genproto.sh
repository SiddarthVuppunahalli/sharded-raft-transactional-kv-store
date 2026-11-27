#!/usr/bin/env bash
# Regenerates api/kvpb from proto/kv/v1/kv.proto inside Docker, so nobody
# needs protoc or Go plugins installed locally.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
docker run --rm -v "$ROOT":/src -w /src golang:1.25 bash -c '
  set -e
  apt-get update -qq >/dev/null && apt-get install -y -qq protobuf-compiler >/dev/null
  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.0
  protoc -I proto \
    --go_out=api/kvpb --go_opt=paths=source_relative --go_opt=Mkv/v1/kv.proto=github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb \
    --go-grpc_out=api/kvpb --go-grpc_opt=paths=source_relative --go-grpc_opt=Mkv/v1/kv.proto=github.com/SiddarthVuppunahalli/sharded-raft-transactional-kv-store/api/kvpb \
    kv/v1/kv.proto
  mv api/kvpb/kv/v1/*.go api/kvpb/ && rm -rf api/kvpb/kv
'
echo "generated: $(ls "$ROOT"/api/kvpb)"
