# Evaluation results (20261121-155008)

- Nodes: 3, ranges: split at 'm', replication factor 3
- Run duration per scenario: 15s
- Host: Darwin arm64, Docker 29.9.2

## Workload results

| Scenario | Workload | Ops/s | OK | Errors | Read p50 / p99 (ms) | Write p50 / p99 (ms) | Max unavailability (ms) | Correctness |
|---|---|---:|---:|---:|---:|---:|---:|---|
| baseline-read95 | kv | 26305 | 394580 | 0 | 0.6 / 1.3 | 0.6 / 1.3 | 32 | n/a |
| baseline-mixed50 | kv | 25834 | 387519 | 0 | 0.6 / 1.5 | 0.6 / 1.4 | 34 | n/a |
| baseline-write95 | kv | 25593 | 383918 | 0 | 0.6 / 1.4 | 0.6 / 1.4 | 37 | n/a |
| scaling-c1 | kv | 3317 | 23219 | 0 | 0.3 / 0.6 | 0.3 / 0.6 | 3 | n/a |
| scaling-c4 | kv | 9189 | 64328 | 0 | 0.4 / 0.8 | 0.4 / 0.8 | 2 | n/a |
| scaling-c16 | kv | 24812 | 173702 | 0 | 0.6 / 1.5 | 0.6 / 1.5 | 10 | n/a |
| scaling-c64 | kv | 49197 | 344422 | 0 | 1.2 / 3.2 | 1.2 / 3.2 | 19 | n/a |
| failover-leader-crash | kv | 23393 | 350898 | 0 | 0.5 / 1.4 | 0.6 / 1.4 | 2032 | n/a |
| failover-follower-crash | kv | 23429 | 351446 | 0 | 0.5 / 1.4 | 0.6 / 1.4 | 2022 | n/a |
| partition-leader-isolated | kv | 20417 | 306268 | 0 | 0.6 / 1.5 | 0.6 / 1.5 | 2001 | n/a |
| partition-split-kv1 | kv | 19628 | 294446 | 0 | 0.6 / 1.5 | 0.6 / 1.5 | 2000 | n/a |
| delay-lan | kv | 1336 | 20054 | 0 | 11.8 / 18.0 | 11.9 / 18.5 | 17 | n/a |
| delay-wan | kv | 138 | 2090 | 0 | 103.8 / 231.7 | 104.5 / 213.8 | 154 | n/a |
| delay-lossy | kv | 180 | 2708 | 0 | 74.1 / 296.4 | 73.9 / 303.7 | 275 | n/a |
| linearizability-clean | linear | 15599 | 233985 | 0 | 0.5 / 1.0 | 0.5 / 1.0 | 13 | OK |
| linearizability-chaos | linear | 8032 | 240972 | 12 | 0.6 / 1.3 | 0.6 / 1.4 | 5413 | OK |
| txn-bank-clean | bank | 0 | 0 | 0 | 0.0 / 0.0 | 0.0 / 0.0 | 0 | SKIPPED |
| txn-bank-chaos | bank | 0 | 0 | 0 | 0.0 / 0.0 | 0.0 / 0.0 | 0 | SKIPPED |

## Leader failover time (range 1)

| Fault | Trials | Fault → new leader, mean (min–max) ms | Fault → first write, mean (min–max) ms |
|---|---:|---:|---:|
| leader crash | 3 | 1575 (1279–1889) | 1744 (1282–2060) |
| leader isolate | 3 | 1508 (1291–1811) | 1844 (1292–2428) |
