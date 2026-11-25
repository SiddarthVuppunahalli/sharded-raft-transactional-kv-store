# Pranav MVP TODO

This checklist is limited to Pranav's Sharing & Metadata MVP: keep a durable range map and route basic single-key reads and writes using it.

## Implemented

- [x] Store and validate range descriptors with complete, non-overlapping keyspace coverage.
- [x] Replicate metadata commands through a metadata Raft group and persist its log and applied map in SQLite.
- [x] Expose range-list, key-lookup, and version-checked metadata update endpoints.
- [x] Route `GET /api/kv` and `PUT /api/kv` to the range leader recorded in metadata.
- [x] Refresh stale routing information, update a changed leader, and apply bounded retries.
- [x] Keep the browser metadata viewer separate from the Python backend.

## Remaining MVP work

- [ ] Add Python tests for range validation, metadata updates, routing retries, and persistence after restart.
- [ ] Verify metadata replication and leader failover with three running metadata nodes.
- [ ] Complete an end-to-end `GET` and `PUT` through the router when the range service is available.
- [ ] Confirm routing errors and timeouts are shown clearly in the client and frontend.

## Dependency outside Pranav's ownership

The range Raft service must provide `POST /_range/execute` and report its current leader. The router contract is documented in [EndProduct.md](EndProduct.md); the receiver belongs with the range Raft service.

## Deferred beyond this MVP

Transaction/MVCC APIs, the 2PC coordinator, timestamp allocation, automatic range splitting, and automatic replica balancing are specified or described in [EndProduct.md](EndProduct.md), but are not part of this MVP checklist.
