# Sharing & Metadata TODO

Owner: Pranav

## 1. Metadata Service

- [x] Define the range descriptor: range ID, start key, end key, replicas, and current leader.
- [x] Define metadata commands for creating, updating, and replacing/removing range descriptors atomically.
- [x] Build the replicated metadata service using its own Raft group.
- [x] Store the key-range map in durable SQLite storage through the metadata Raft state machine.
- [x] Implement metadata state-machine startup loading and validate the loaded range map.
- [x] Validate that ranges do not overlap and that key lookups are unambiguous.
- [x] Track the current Raft leader and replica list for every range.
- [x] Add metadata lookup and update operations for the router and other services.
- [x] Return clear errors when metadata is unavailable, invalid, or stale.

## 2. Request Routing

- [x] Implement key-to-range lookup using the metadata range map.
- [x] Route client requests to the correct range leader through the backend `Router` API.
- [x] Handle a `NotLeader` response by updating the leader in metadata and retrying.
- [x] Refresh stale metadata before retrying a request.
- [x] Add request deadlines and a maximum retry limit.
- [x] Return a clear error when the target range has no available leader.
- [x] Prevent requests from being sent to nodes that are not replicas of the target range.
- [x] Add routing tests using multiple ranges and different leaders.
- [x] Connect the router to the Python HTTP range-client contract.
- [ ] Implement the `RangeKVTransport` receiver in each range Raft service.

## 3. Range Splits

- [ ] Define the condition that triggers a split, such as range size or key count.
- [ ] Select and validate a split key.
- [ ] Create new range descriptors for the two resulting ranges.
- [ ] Assign replicas and create Raft groups for the new ranges.
- [ ] Ensure data is available in the correct new range after the split.
- [ ] Update the metadata map atomically after the split is ready.
- [ ] Mark the original range as replaced and stop routing new requests to it.
- [ ] Safely pause or retry requests while a split is in progress.
- [ ] Add tests proving that keys on both sides of the split route correctly.

## 4. Replica Movement and Shard Balancing

- [ ] Define metadata commands for adding, removing, and replacing range replicas.
- [ ] Add a replica to a target node and wait for it to catch up.
- [ ] Update the range metadata after the new replica is ready.
- [ ] Remove an old replica only when the range still has a safe quorum.
- [ ] Track which ranges are assigned to each physical node.
- [ ] Implement a simple policy for balancing ranges across nodes.
- [ ] Handle replica movement while leaders change or nodes fail.
- [ ] Add tests for replica movement and continued request routing.

## 5. Monotonic Timestamp Allocator

- [ ] Add a timestamp counter to the replicated metadata service.
- [ ] Allocate a new timestamp through the metadata Raft group.
- [ ] Guarantee that returned timestamps are monotonically increasing.
- [ ] Persist the latest timestamp before returning it to a caller.
- [ ] Restore the timestamp counter correctly after a metadata service restart.
- [ ] Expose timestamp allocation to the transaction service.
- [ ] Add tests for concurrent timestamp requests and restart recovery.

## 6. Documentation and Handoff

- [ ] Document the range descriptor and metadata state format.
- [x] Document how request routing handles stale metadata and leader changes.
- [ ] Document the range split workflow.
- [ ] Document the replica movement and balancing workflow.
- [ ] Document the timestamp allocator contract for the transaction service.
- [x] Add the Sharing & Metadata endpoints, inputs, outputs, and examples to `URLs.md`.
