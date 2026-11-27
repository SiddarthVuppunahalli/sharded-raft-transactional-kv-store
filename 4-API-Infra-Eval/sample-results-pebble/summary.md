# Evaluation results (20261127-021617)

- Nodes: 3, ranges: split at 'm', replication factor 3
- Run duration per scenario: 15s
- Host: Darwin arm64, Docker 29.9.2

## Workload results

| Scenario | Workload | Ops/s | OK | Errors | Read p50 / p99 (ms) | Write p50 / p99 (ms) | Max unavailability (ms) | Correctness |
|---|---|---:|---:|---:|---:|---:|---:|---|
| baseline-read95 | kv | 1333 | 20001 | 0 | 11.6 / 21.4 | 12.6 / 20.9 | 44 | n/a |
| baseline-mixed50 | kv | 752 | 11300 | 0 | 18.9 / 44.8 | 20.1 / 44.6 | 19 | n/a |
| baseline-write95 | kv | 462 | 6940 | 0 | 32.9 / 49.5 | 34.7 / 59.8 | 36 | n/a |
| failover-leader-crash | kv | 488 | 7330 | 0 | 26.5 / 77.5 | 27.7 / 86.7 | 1915 | n/a |
| failover-follower-crash | kv | 690 | 10357 | 0 | 20.7 / 52.7 | 21.8 / 56.2 | 25 | n/a |
| linearizability-clean | linear | 398 | 5975 | 0 | 18.8 / 35.3 | 20.3 / 36.3 | 30 | OK |
| linearizability-chaos | linear | 347 | 10690 | 0 | 14.2 / 209.9 | 15.0 / 172.2 | 2042 | OK |

## Leader failover time (range 1)

| Fault | Trials | Fault → new leader, mean (min–max) ms | Fault → first write, mean (min–max) ms |
|---|---:|---:|---:|
| leader crash | 3 | 1883 (1742–2126) | 1889 (1749–2132) |
| leader isolate | 3 | 1939 (1751–2175) | 2950 (2761–3186) |
