# Primary MVP Checklist:

-> Build the replicated metadata service that stores the key-range map and tracks current Raft group leaders.

-> Implement the routing logic that intercepts client API requests and forwards them to the correct range leader based on the metadata map.

-> Implement the logic for range splits (when a shard gets too large) and replica movement (balancing shards across physical nodes).

-> Build the monotonic timestamp allocator within the metadata service to feed into the transaction layer.