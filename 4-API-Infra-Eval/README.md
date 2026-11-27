# Part 4: API, Infrastructure & Evaluation (Samhitha)

This part covers the gRPC APIs, the Docker Compose cluster, fault injection, and the evaluation suite. You don't need Go or protoc on your machine, only Docker.

| Checklist item | Status | Where |
|---|---|---|
| Typed gRPC/Protobuf APIs for client requests, inter-node Raft transport and transaction coordination | Done | [`proto/kv/v1/kv.proto`](../proto/kv/v1/kv.proto), generated code in [`api/kvpb`](../api/kvpb), Raft transport in [`transport/`](../transport), node server in [`server/`](../server), client library in [`client/`](../client) |
| Docker Compose deployment for reproducible multi-node clusters with realistic network topologies | Done | [`docker-compose.yml`](../docker-compose.yml), [`deploy/docker-compose.5node.yml`](../deploy/docker-compose.5node.yml), [`Dockerfile`](../Dockerfile), WAN profile in [`faults/delay.sh`](../faults/delay.sh) |
| Scripts that simulate node crashes, network partitions and delayed messages during active workloads | Done | [`faults/`](../faults) |
| Test suite for throughput, read/write latency, failover time, and transaction correctness under failure | Done (the bank/txn check activates once Part 3 registers `Txn`) | [`cmd/kvbench`](../cmd/kvbench), [`eval/run_eval.sh`](../eval/run_eval.sh), [`eval/report.py`](../eval/report.py), [`server/cluster_test.go`](../server/cluster_test.go) |

## Quick start

```bash
docker compose up -d --build                         # 3 nodes: kv1, kv2, kv3
docker compose run --rm tools kvctl wait-ready
docker compose run --rm tools kvctl put apple red
docker compose run --rm tools kvctl get apple
docker compose run --rm tools kvctl status           # ranges, leaders, terms, indexes
```

Run the full evaluation. Results go to `results/<timestamp>/`, including `summary.md`, `report.html`, and one JSON/log file per scenario:

```bash
eval/run_eval.sh               # full suite, ~15 min
QUICK=1 eval/run_eval.sh       # ~8 min
NODES=5 eval/run_eval.sh       # 5-node cluster
SCENARIOS="failover partition" eval/run_eval.sh
```

Run the Go tests (Docker only, no local Go needed):

```bash
docker run --rm -v "$PWD":/src -w /src golang:1.25 go test -race ./...
```

Regenerate protobuf code after editing `kv.proto`:

```bash
scripts/genproto.sh
```

## Architecture of this layer

```
client (kvctl / kvbench / client pkg)
   │  KV.Get/Put/Delete/Txn, Admin.Status       ← proto/kv/v1/kv.proto
   ▼
kvnode (server/)  ── one multiraft.Group per range ── StateMachine (in-memory now, Pebble/MVCC later)
   │  RaftTransport.Send(batch of raftpb.Message tagged with range_id)
   ▼
peer kvnodes (transport/)
```

- **One connection per peer carries the traffic for every range.** Each Raft message is tagged with `range_id`. Sends are batched and asynchronous. If a peer is unreachable or its queue is full, messages are dropped (Raft retransmits) and the peer is reported with `ReportUnreachable`, so a dead node never blocks the Raft loop.
- **Reads go through the Raft log.** That makes them linearizable even when a follower serves them. One effect: read throughput is about the same as write throughput. Adding ReadIndex/lease reads would be a good optimisation later.
- **Errors map to gRPC codes.** No leader or a stopped node returns `UNAVAILABLE` (retryable). A commit timeout returns `DEADLINE_EXCEEDED`: the outcome is unknown and the write may still commit, and the linearizability checker models it that way.
- **The client is range-aware.** It learns the range map and leaders from `Admin.Status`, sends each request to the range leader, and fails over to other nodes on retryable errors.

## How the other parts plug in

| Part | Hook | Notes |
|---|---|---|
| **1. Consensus & storage (Mukesh)** | `server.Config.StateMachine` (`Apply(rangeID, index, cmd)`), and pass a Pebble `raft.Storage` in `NewNode` | Today: `raft.NewMemoryStorage()` plus `MemoryStateMachine`. I added `Group.Step`, `Status`, `IsLeader`, `Leader`, `ReportUnreachable` and `Manager.Groups` to `multiraft` so the network transport can drive the groups, plus `Command.ReqID` so a node can match committed entries to waiting requests. |
| **2. Sharding & metadata (Pranav)** | `RangeDescriptor`, `RoutingError` (`NOT_LEADER`, `KEY_OUT_OF_RANGE`, …) and `Timestamp` messages in the proto. The node currently routes with `multiraft.Manager.Lookup` over static splits (`KV_SPLITS`). | Replace the static splits with the metadata service, and fill `leader_hint` / `generation`. The client already refreshes its range map from `Admin.Status` after failures. |
| **3. Transactions (Siddarth)** | Implement `server.TxnHandler` (client `Txn` RPC) and `kvpb.TxnParticipantServer` (`Prepare`, `Commit`, `Abort`, `QueryStatus`), then set them in `server.Config{Txn: ..., Participant: ...}` | Until then, `Txn` returns `UNIMPLEMENTED` and the bank scenario reports `SKIPPED`. Once registered, `eval/run_eval.sh` checks the money-conservation invariant with and without faults, with no further changes needed. |

## Fault injection (`faults/`)

All scripts act on the containers `kv1`…`kvN` (set `NODES=5` for the 5-node cluster).

| Script | What it does |
|---|---|
| `crash.sh kv2` / `crash.sh leader [range]` | SIGKILL a node, or the current leader of a range |
| `restart.sh kv2` / `restart.sh all` | Bring crashed nodes back |
| `partition.sh isolate kv1` / `isolate leader [range]` | Cut one node off from all its peers (iptables) |
| `partition.sh split "kv1 kv2" "kv3"` | Two-sided partition |
| `partition.sh oneway kv1 kv2` | Asymmetric packet loss kv1 → kv2 |
| `delay.sh kv2 100ms [jitter] [loss%]` / `delay.sh all …` | tc netem latency, jitter and loss |
| `delay.sh wan` | Three-region topology (5 / 20 / 40 ms one-way, jitter, 0.1% loss) |
| `heal.sh [kvN]` | Remove all partitions and netem rules |
| `chaos.sh` | Random crash / partition / delay schedule for `DURATION` seconds (Jepsen-style nemesis) |

Client traffic from the `tools` container is never blocked by partitions, so you can watch a partitioned cluster from outside.

## Evaluation suite

| Scenario | What it measures |
|---|---|
| `baseline` | Throughput and p50/p95/p99 latency for read-heavy (95/5), mixed (50/50) and write-heavy (5/95) workloads |
| `scaling` | Throughput and latency as clients go from 1 to 64 |
| `failover` | Leader crash and follower crash during load (throughput timeline, max unavailability window), plus repeated trials of `kvctl failover-probe` for fault → new leader → first successful write |
| `partition` | Leader isolated, and a minority/majority split, during load |
| `delay` | LAN, WAN-topology and lossy-network profiles |
| `linearizability` | Unique-value read/write history checked with [porcupine](https://github.com/anishathalye/porcupine), clean and under `chaos.sh`. An HTML visualisation of the history is saved too. |
| `bank` | Cross-range transfers through `Txn`; an auditor checks the total balance never changes (atomicity + snapshot isolation), clean and under chaos |

**Max unavailability** is the longest interval in which no client operation succeeded. During a leader crash this is the client-visible failover time.

## Sample results (3 nodes, QUICK run on an M-series Mac with Docker Desktop)

Full data: [`sample-results/summary.md`](sample-results/summary.md). Download [`sample-results/report.html`](sample-results/report.html) and open it for charts.

- **Throughput:** about 26k ops/s with 16 clients and about 49k ops/s with 64 clients. p50 latency is 0.6 ms and p99 is 1.4 ms on the local Docker network.
- **Leader failover:** after a leader crash, a new leader is elected in 1.6 s on average (1.3–1.9 s) and writes resume within 1.7 s. With an isolated leader the figures are similar (1.5 s / 1.8 s). Clients see about 2 s with no successful operations. Zero failed client operations: the client retries through the outage.
- **Follower crash:** also shows about a 2 s gap. Each node leads some range, so the "follower" of range 1 that gets crashed can be the leader of range 2.
- **Network profiles:** 2 ms LAN takes p50 from 0.6 ms to 12 ms. The 3-region WAN gives p50 104 ms; 2% loss gives p99 300 ms.
- **Linearizability:** porcupine verified about 234k operations without faults and about 241k operations under random crashes, partitions and delays. No violations found.
- **Transactions:** `SKIPPED` until Part 3 registers the `Txn` handler.

## Durable mode (Pebble) and the HTTP router

**Pebble is now the default.** The Docker cluster stores each range in Mukesh's Pebble storage under `/data` (`KV_DATA_DIR`; set it to an empty string for in-memory). A crashed container restarts from disk, and the `TestPebbleNodeRecoversAfterRestart` integration test covers this.

Quick run on Pebble ([`sample-results-pebble/summary.md`](sample-results-pebble/summary.md)):

| | In-memory | Pebble |
|---|---:|---:|
| Read-heavy throughput (16 clients) | ~26,300 ops/s | ~1,330 ops/s |
| Write-heavy p50 latency | 0.6 ms | 35 ms |
| Leader crash → first write | ~1.7 s | ~1.9 s |
| Linearizability under chaos | OK | OK |

The throughput drop comes from durability. `PebbleStorage` calls `fsync` (`pebble.Sync`) for every log entry, hard-state update and applied write. Writing each Raft `Ready` batch with one synced Pebble batch would win most of it back.

## Integration with the metadata router (Part 2)

The nodes serve the HTTP range API agreed with Pranav and Mukesh, and Pranav's router runs next to the cluster:

```bash
docker compose up -d --build
deploy/router.sh up              # starts Part 2's router from its branch and registers the ranges
docker compose run --rm tools kvctl --http-url http://metadata:8080 put apple red
docker compose run --rm tools kvctl --http-url http://metadata:8080 get apple
curl -X PUT --data red 'http://localhost:18080/api/kv?key=apple'   # same API from your Mac
SCENARIOS=router QUICK=1 eval/run_eval.sh                          # evaluate through the router
```

### HTTP range API (served by every node on :8080; host ports 8081–8083)

| Endpoint | Request | Response |
|---|---|---|
| `POST /_range/execute` | `{"operation":"get"\|"put"\|"delete","rangeId":1,"key":"apple","value":"red","metadataVersion":1}` | `{"rangeId":1,"operation":"put","key":"apple","value":"red","found":true,"committed":true,"leaderId":2,"leaderNodeId":2,"metadataVersion":1,"raftIndex":12}`. A GET miss returns `found:false` and `value:null`; PUT always returns `found:true`. |
| `GET /_range/stats?rangeId=1` | | `{"rangeId":1,"keyCount":100,"sizeBytes":2048,"logEntries":245,"commitIndex":245,"appliedIndex":245,"leaderNodeId":2,"replicaCount":3}` |
| `GET /_range/metadata?rangeId=1` | | `{"rangeId":1,"startKey":null,"endKey":"m","leaderNodeId":2,"metadataVersion":1,"replicas":[{"nodeId":1,"apiUrl":"http://kv1:8080","role":"follower","healthy":true}, …]}`. Followers return the leader's view, since only the leader knows replica health. |

Errors return `{"error": …}`, which the router acts on:

| Error | HTTP status | Meaning |
|---|---|---|
| `not_leader` | 409 | This node isn't the range leader; `leaderId` says which node is. |
| `stale_metadata` | 409 | Unknown range, or the key is outside it; refresh the range map. |
| `no_leader` | 503 | The range has no elected leader or no quorum. |
| `timeout` | 504 | The outcome is unknown; the write may still commit. |

**Leader reporting.** With `KV_METADATA_URL`, a node that becomes leader of a range proposes `set_leader` to the metadata service. Without it, after a leader crash the router kept sending to the dead node, which can never reply `not_leader`, so every request failed with 503 until someone fixed the map by hand.

### Results through the router (Pebble, 3 nodes, QUICK run)

From [`sample-results-router/summary.md`](sample-results-router/summary.md):

| Scenario | Ops/s | Failed ops | p50 / p99 (ms) | Max unavailability | Correctness |
|---|---:|---:|---:|---:|---|
| Mixed 50/50 | 1,301 | 0 | 11 / 18 | 15 ms | |
| Leader crash during load | 1,171 | 0 | 11 / 20 | 0.9 s | |
| Random crashes, partitions and delays | 848 | 0 | 8 / 13 | 2.0 s | Linearizable (25,460 ops) |

## Known limitations (depend on the other parts)

- **Raft log snapshots/compaction aren't implemented.** The log grows without limit, so the eval runner starts fresh containers for each scenario.
- **Ranges are static, from `KV_SPLITS`.** Range splits and replica movement come from Part 2.
