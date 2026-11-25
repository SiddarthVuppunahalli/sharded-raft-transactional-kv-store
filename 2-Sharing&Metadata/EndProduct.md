# End Product API Reference

This document describes the API clients and project services should use when the full system is complete. It keeps the current metadata and key-value routes and proposes the transaction and internal service routes required by the project architecture.

**Implementation status:** health, metadata range lookup/update, and the router's client-facing key-value routes exist in the current Python prototype. The range Raft receiver, MVCC transaction API, 2PC coordinator, and timestamp allocator are still planned. Transaction and internal routes below are the target contract, not claims that those routes already work.

## Service addresses

| Service | Default address | Used by |
| --- | --- | --- |
| Client and metadata API | `http://127.0.0.1:8080` | Applications and administrators |
| Browser frontend | `http://127.0.0.1:5173` | People viewing the range map |
| Range Raft node | Configured per node | Router and transaction coordinator |
| Metadata Raft peer transport | Configured per metadata replica | Metadata replicas only |

The frontend is a separate process. It serves the browser files and proxies `/api/*` requests to the client and metadata API.

## Conventions

- All endpoints use HTTP. Send JSON with `Content-Type: application/json` when the request body is JSON.
- Keys in query parameters use **unpadded base64url**. The UTF-8 key `apple` is `YXBwbGU`.
- Values for direct key-value reads and writes are raw bytes with `Content-Type: application/octet-stream`.
- Byte strings inside JSON, including range boundaries, use standard base64. For example, `m` is `bQ==`.
- A range is a half-open interval `[startKey, endKey)`. A null start is the beginning of the keyspace; a null end is the end.
- Direct single-key reads and writes are linearizable while the target range has a Raft majority. Transactions use the snapshot behavior described below.

## Client API

### `GET /healthz`

Checks whether the API process is serving requests.

```sh
curl -i http://127.0.0.1:8080/healthz
```

Returns `200 OK` and plain text `ok`.

### `GET /api/kv?key={base64url-key}`

Reads a key. The router looks up the key's range and sends the read to that range's leader.

```sh
curl -i 'http://127.0.0.1:8080/api/kv?key=YXBwbGU'
```

On success, returns the value bytes with `Content-Type: application/octet-stream`. Returns `404` if the key does not exist, `400` for a missing or malformed key, `503` if metadata or the range is unavailable, and `504` if the operation times out.

### `PUT /api/kv?key={base64url-key}`

Writes or replaces a value. The request body is the raw value, up to 1 MiB.

```sh
curl -i -X PUT \
  'http://127.0.0.1:8080/api/kv?key=YXBwbGU' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'red apple'
```

Returns `204 No Content` after the range leader reports the write committed. Uses the same key validation and availability errors as `GET /api/kv`.

### `POST /api/transactions`

Begins a transaction and allocates its snapshot timestamp. The final contract uses snapshot isolation: reads see a consistent snapshot and the transaction's own writes; conflicting concurrent writes to the same key cause commit to fail.

Request:

```json
{
  "isolation": "snapshot"
}
```

The `isolation` field may be omitted; `snapshot` is the default and only supported mode in this project.

Example:

```sh
curl -i -X POST http://127.0.0.1:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"isolation":"snapshot"}'
```

Returns `201 Created`:

```json
{
  "transactionId": "txn-7f20c1",
  "readTimestamp": 1042,
  "status": "active"
}
```

### `GET /api/transactions/{transactionId}`

Gets the transaction state. Use this after a lost commit response to learn whether the coordinator recorded a final decision.

```sh
curl -i http://127.0.0.1:8080/api/transactions/txn-7f20c1
```

Example response:

```json
{
  "transactionId": "txn-7f20c1",
  "status": "committed",
  "readTimestamp": 1042,
  "commitTimestamp": 1048
}
```

`status` is `active`, `prepared`, `committed`, or `aborted`.

### `GET /api/transactions/{transactionId}/kv?key={base64url-key}`

Reads a key at the transaction's snapshot timestamp, including any earlier write to that key made by the same transaction.

```sh
curl -i 'http://127.0.0.1:8080/api/transactions/txn-7f20c1/kv?key=YXBwbGU'
```

Returns raw value bytes with `Content-Type: application/octet-stream`. Returns `404` when the key was absent at the snapshot and has no transaction-local write.

### `PUT /api/transactions/{transactionId}/kv?key={base64url-key}`

Stages a value in the transaction. The value is not visible to other transactions until commit.

```sh
curl -i -X PUT \
  'http://127.0.0.1:8080/api/transactions/txn-7f20c1/kv?key=YXBwbGU' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'green apple'
```

Returns `204 No Content` when the write is staged. Returns `404` for an unknown transaction and `409` if the transaction is no longer active.

### `POST /api/transactions/{transactionId}/commit`

Commits all staged writes atomically, including writes routed to different ranges. The coordinator runs 2PC: it asks every participant to prepare, records one durable decision in the replicated metadata service, then tells participants to commit. Repeating the call after a committed response returns the same decision.

```sh
curl -i -X POST http://127.0.0.1:8080/api/transactions/txn-7f20c1/commit
```

Returns `200 OK`:

```json
{
  "transactionId": "txn-7f20c1",
  "status": "committed",
  "commitTimestamp": 1048
}
```

Returns `409 Conflict` if a write conflict prevents commit. The coordinator records an abort decision and the transaction status becomes `aborted`. Returns `503` if the coordinator cannot reach the required range or metadata majorities; a client can query the transaction status before retrying.

### `POST /api/transactions/{transactionId}/abort`

Aborts an active transaction and discards its staged writes.

```sh
curl -i -X POST http://127.0.0.1:8080/api/transactions/txn-7f20c1/abort
```

Returns `200 OK`:

```json
{
  "transactionId": "txn-7f20c1",
  "status": "aborted"
}
```

### Cross-range transaction example

Assume the map is split at `m`: `apple` routes to range 1 and `zebra` routes to range 2. Both writes are staged under one transaction and become visible together only after the coordinator commits.

```sh
# Begin and use the returned transactionId in the following calls.
curl -X POST http://127.0.0.1:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"isolation":"snapshot"}'

curl -X PUT \
  'http://127.0.0.1:8080/api/transactions/txn-7f20c1/kv?key=YXBwbGU' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '{"quantity":9}'

curl -X PUT \
  'http://127.0.0.1:8080/api/transactions/txn-7f20c1/kv?key=emVicmE' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '{"quantity":4}'

curl -X POST http://127.0.0.1:8080/api/transactions/txn-7f20c1/commit
```

If either range rejects prepare, neither write is committed. The coordinator durably records an abort decision and releases any participant that already prepared.

## Metadata administration API

These routes are for cluster setup and administration. Normal application clients should use `/api/kv` or the transaction routes instead of routing themselves.

### `GET /api/metadata/ranges`

Returns the committed range map after a linearizable metadata read.

```sh
curl -i http://127.0.0.1:8080/api/metadata/ranges
```

Example response:

```json
{
  "version": 3,
  "ranges": [
    {
      "id": 1,
      "startKey": null,
      "endKey": "bQ==",
      "replicaIds": [1, 2, 3],
      "leaderId": 1
    },
    {
      "id": 2,
      "startKey": "bQ==",
      "endKey": null,
      "replicaIds": [1, 2, 3],
      "leaderId": 2
    }
  ]
}
```

### `GET /api/metadata/lookup?key={base64url-key}`

Finds the descriptor for one key.

```sh
curl -i 'http://127.0.0.1:8080/api/metadata/lookup?key=YXBwbGU'
```

Returns the map version and descriptor:

```json
{
  "version": 3,
  "range": {
    "id": 1,
    "startKey": null,
    "endKey": "bQ==",
    "replicaIds": [1, 2, 3],
    "leaderId": 1
  }
}
```

### `POST /api/metadata/commands`

Applies an atomic metadata update through the metadata Raft group. Every command must include `expectedVersion` from the latest snapshot; a stale version returns `409 Conflict`.

Initialize the map with one range:

```sh
curl -i -X POST http://127.0.0.1:8080/api/metadata/commands \
  -H 'Content-Type: application/json' \
  -d '{"type":"replace_ranges","expectedVersion":0,"removeRangeIds":[],"ranges":[{"id":1,"startKey":null,"endKey":null,"replicaIds":[1,2,3],"leaderId":1}]}'
```

Split range 1 at the key byte `m` (`bQ==`). The range service must finish preparing the replacement ranges before the administrator publishes this map change.

```json
{
  "type": "replace_ranges",
  "expectedVersion": 1,
  "removeRangeIds": [1],
  "ranges": [
    {"id": 1, "startKey": null, "endKey": "bQ==", "replicaIds": [1, 2, 3], "leaderId": 1},
    {"id": 2, "startKey": "bQ==", "endKey": null, "replicaIds": [1, 2, 3], "leaderId": 2}
  ]
}
```

Update a range leader:

```json
{
  "type": "set_leader",
  "expectedVersion": 2,
  "rangeId": 1,
  "leaderId": 2
}
```

Set `leaderId` to `0` if the leader is unknown. Update replicas after the range service confirms the move is safe:

```json
{
  "type": "set_replicas",
  "expectedVersion": 3,
  "rangeId": 1,
  "replicaIds": [1, 2, 4],
  "leaderId": 2
}
```

A successful command returns the updated range snapshot. Returns `400` for an invalid map or command and `503` if the metadata Raft group cannot commit.

## Internal service APIs

These endpoints are for trusted project services communicating inside the cluster. They are not application-client APIs.

### `POST /_range/execute`

The router sends a range operation to the descriptor's leader. The range service must verify that it hosts the requested range and is the current leader. Reads pass a Raft read barrier; writes return success only after their log entry commits and applies.

Example read request:

```json
{
  "operation": "get",
  "rangeId": 1,
  "key": "YXBwbGU=",
  "value": "",
  "metadataVersion": 3
}
```

Found read response:

```json
{
  "found": true,
  "value": "cmVkIGFwcGxl"
}
```

Missing key response: `{"found":false}`. A non-leader returns `409` with `{"error":"not_leader","leaderId":2}`. A request for a removed or moved range returns `409` with `{"error":"stale_metadata"}`. A range without quorum returns `503`.

Example write request:

```json
{
  "operation": "put",
  "rangeId": 1,
  "key": "YXBwbGU=",
  "value": "Z3JlZW4gYXBwbGU=",
  "metadataVersion": 3
}
```

Successful write response: `{"found":true}`.

### Range participant transaction routes

The coordinator calls each participating range leader during 2PC.

`POST /_transactions/prepare` includes the transaction ID, read timestamp, proposed commit timestamp, and that range's staged writes:

```json
{
  "transactionId": "txn-7f20c1",
  "readTimestamp": 1042,
  "commitTimestamp": 1048,
  "writes": [
    {"key": "YXBwbGU=", "value": "Z3JlZW4gYXBwbGU="}
  ]
}
```

Prepared response: `{"transactionId":"txn-7f20c1","prepared":true}`. A conflict returns `409` and `prepared:false`.

`POST /_transactions/commit` applies a previously prepared participant:

```json
{"transactionId":"txn-7f20c1","commitTimestamp":1048}
```

`POST /_transactions/abort` releases a prepared participant and discards its staged writes:

```json
{"transactionId":"txn-7f20c1"}
```

Commit and abort are idempotent for a transaction with the same recorded decision. A participant must not commit unless the coordinator has durably recorded the commit decision.

### Metadata timestamp and decision routes

The transaction service obtains ordered timestamps from the replicated metadata service using `POST /internal/timestamps/next`:

```json
{}
```

Response: `{"timestamp":1048}`. A timestamp is returned only after its allocation commits.

After all participants prepare, the coordinator durably records its final decision through `PUT /internal/transactions/{transactionId}/decision`:

```json
{
  "decision": "commit",
  "commitTimestamp": 1048
}
```

For abort, set `decision` to `abort`; omit `commitTimestamp`. The decision cannot change once recorded. The coordinator recovers incomplete work using `GET /internal/transactions/{transactionId}/decision`, which returns the durable decision or `404` if none has been recorded.

### Metadata Raft peer transport

`POST /_raft/vote` and `POST /_raft/append` are internal consensus messages exchanged between metadata replicas. They are implementation-level routes and are not part of the application API contract. Range Raft peers use their range service's configured consensus transport.

## Common errors

| HTTP status | Meaning |
| --- | --- |
| `400 Bad Request` | Missing, malformed, or invalid input |
| `404 Not Found` | Key, transaction, or metadata item does not exist |
| `409 Conflict` | Stale metadata version, non-leader response, or transaction write conflict |
| `413 Payload Too Large` | Request value exceeds the endpoint's size limit |
| `503 Service Unavailable` | Required Raft majority, coordinator, or range service is unavailable |
| `504 Gateway Timeout` | The operation exceeded its deadline |
