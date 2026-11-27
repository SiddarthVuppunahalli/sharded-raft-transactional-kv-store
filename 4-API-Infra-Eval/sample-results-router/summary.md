# Evaluation results (20261127-023930)

- Nodes: 3, ranges: split at 'm', replication factor 3
- Run duration per scenario: 15s
- Host: Darwin arm64, Docker 29.9.2

## Workload results

| Scenario | Workload | Ops/s | OK | Errors | Read p50 / p99 (ms) | Write p50 / p99 (ms) | Max unavailability (ms) | Correctness |
|---|---|---:|---:|---:|---:|---:|---:|---|
| router-mixed50 | kv | 1301 | 19527 | 0 | 10.8 / 17.9 | 11.5 / 18.9 | 15 | n/a |
| router-failover-leader-crash | kv | 1171 | 17724 | 0 | 10.6 / 19.6 | 11.2 / 19.2 | 921 | n/a |
| router-linearizability-chaos | linear | 848 | 25460 | 0 | 7.8 / 12.9 | 8.2 / 13.7 | 2025 | OK |
