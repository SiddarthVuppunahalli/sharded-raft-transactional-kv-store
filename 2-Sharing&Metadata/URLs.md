# Backend URL Reference

The backend is a Python service. Its HTTP API listens on `http://127.0.0.1:8080` by default. The browser frontend is a separate Python development server at `http://127.0.0.1:5173`; it serves `frontend/` and forwards `/api` calls to the backend.

For the proposed full-project API, including transaction and internal service calls that are not implemented yet, see [EndProduct.md](EndProduct.md).

## Run locally

Run the commands below inside `2-Sharing&Metadata`. From the repository root, enter this folder in each of two terminals:

```powershell
Set-Location -LiteralPath '.\2-Sharing&Metadata'
```

Backend:

```powershell
py -m backend.cmd.metadata
```

Frontend:

```powershell
py frontend/dev_server.py
```

The default configuration runs a one-node metadata Raft group and stores its Raft log and applied range map in `data/metadata/metadata.sqlite3`. One node is useful for local development; it does not provide failure tolerance.

The database path is relative to `2-Sharing&Metadata`. Relative `METADATA_DATA_DIR` values also resolve from this folder; absolute values are used as supplied.

To run three metadata replicas, open three backend terminals and use the matching settings below. Keep the peer lists identical in every terminal; change the node ID, listen addresses, and data directory for each process.

```powershell
# Terminal 1
$env:METADATA_NODE_ID="1"
$env:METADATA_HTTP_ADDR="127.0.0.1:8080"
$env:METADATA_RAFT_ADDR="127.0.0.1:9090"
$env:METADATA_DATA_DIR="./data/metadata-1"
$env:METADATA_PEERS="1=127.0.0.1:9090,2=127.0.0.1:9091,3=127.0.0.1:9092"
$env:METADATA_HTTP_PEERS="1=127.0.0.1:8080,2=127.0.0.1:8081,3=127.0.0.1:8082"
py -m backend.cmd.metadata
```

For terminal 2, use node ID `2`, HTTP port `8081`, Raft port `9091`, and `./data/metadata-2`. For terminal 3, use node ID `3`, HTTP port `8082`, Raft port `9092`, and `./data/metadata-3`. Keep both peer lists the same in all three terminals. The frontend can proxy to any replica by setting `$env:BACKEND_URL="http://127.0.0.1:8081"` before starting it.

## `GET /healthz`

Checks that this backend process is serving HTTP.

Inputs: none.

Returns `200 OK` with `ok` as plain text.

## `GET /api/metadata/ranges`

Returns the committed range-map snapshot after a majority-backed Raft read barrier. A follower forwards the request to its known leader. A one-node group responds only while its single node is available; a multi-node group needs a majority.

Inputs: none.

Example response:

```json
{
  "version": 1,
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

Range boundaries are key bytes encoded as standard base64. A null or empty start means the beginning of the keyspace; a null or empty end means the end. Ranges use half-open intervals: `[startKey, endKey)`.

## `GET /api/metadata/lookup?key={base64url-key}`

Finds the range that contains a key after a majority-backed read barrier.

Inputs: `key`, the raw key bytes encoded as unpadded base64url. For example, UTF-8 `apple` is `YXBwbGU`.

Example response:

```json
{
  "version": 1,
  "range": {
    "id": 1,
    "startKey": null,
    "endKey": "bQ==",
    "replicaIds": [1, 2, 3],
    "leaderId": 1
  }
}
```

Returns `400` for a missing or invalid key and `404` if the range map has no matching range.

## `POST /api/metadata/commands`

Proposes one metadata update to the metadata Raft group. It returns after the entry commits and is applied. Include `expectedVersion` from the latest range snapshot; stale versions return `409 Conflict`.

### Replace ranges

Use this atomic command to initialize the map, split or merge ranges, or replace descriptors. The final ranges must cover the entire keyspace with no gaps or overlaps.

```json
{
  "type": "replace_ranges",
  "expectedVersion": 0,
  "removeRangeIds": [],
  "ranges": [
    {
      "id": 1,
      "startKey": null,
      "endKey": null,
      "replicaIds": [1, 2, 3],
      "leaderId": 1
    }
  ]
}
```

### Update a range leader

```json
{
  "type": "set_leader",
  "expectedVersion": 1,
  "rangeId": 1,
  "leaderId": 2
}
```

Use `leaderId: 0` when the current range leader is unknown.

### Update replica membership

```json
{
  "type": "set_replicas",
  "expectedVersion": 2,
  "rangeId": 1,
  "replicaIds": [1, 2, 4],
  "leaderId": 2
}
```

Successful response: the updated range snapshot in the same format as `GET /api/metadata/ranges`. Returns `400` for malformed commands, `409` for a stale version, and `503` if a leader or Raft majority is unavailable.

## `GET /api/kv?key={base64url-key}`

Looks up the range, then sends a read to the configured range leader.

Input: unpadded base64url `key` query parameter.

Returns the value as raw bytes with `Content-Type: application/octet-stream`. Returns `404` if the key is absent, `400` for an invalid key, and `503` if the metadata or range leader is unavailable.

## `PUT /api/kv?key={base64url-key}`

Looks up the range and sends the request body to its configured leader.

Inputs: unpadded base64url `key` query parameter and raw value bytes in the request body (up to 1 MiB).

Returns `204 No Content` on success. Returns `400` for an invalid key, `503` if a leader is unavailable, and may return `504` if the range request times out.

The Python router sends JSON to `POST /_range/execute` on the selected address from `RANGE_NODES`. Its request has `operation`, `rangeId`, `key` and `value` fields; keys and values are standard-base64 byte strings. The response uses `found` and `value` fields. The receiving range service is still to be implemented, so `/api/kv` returns `503` until that service is available.

## Internal metadata Raft RPC

Metadata replicas exchange JSON over HTTP at the configured `METADATA_PEERS` addresses. The internal routes `POST /_raft/vote` and `POST /_raft/append` carry election and log-replication messages. Client applications should use the HTTP API above.

## Frontend

The static browser application is in `frontend/`. Start it with `py frontend/dev_server.py`. It serves the page separately from the backend and proxies `/api/*` to `BACKEND_URL` (default `http://127.0.0.1:8080`).

## MVP validation and error handling

Run `py -B -m unittest discover -s tests -v` from this folder. Routing integration uses a test-only HTTP receiver, not the missing range Raft implementation. PUT bodies are read using Content-Length, with a 1 MiB limit. Requests to range replicas share a five-second routing budget and at most three attempts. Malformed upstream responses return 502; unavailable replicas return 503; timeouts return 504. The frontend proxy preserves these statuses and returns JSON when the backend is unavailable or times out.
