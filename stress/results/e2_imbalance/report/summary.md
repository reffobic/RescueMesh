# Phase 1 stress: `e2_imbalance`

From per-request CSVs (udp_load_client). mean +/- sample stdev over repeats.

| Clients | Repeats | Throughput (req/s) mean+/-s | Error rate mean+/-s | p50 ms | p95 ms | p99 ms |
|--------:|--------:|----------------------------:|--------------------:|-------:|-------:|-------:|
| 30 | 15 | 45.28+/-16.35 | 0.000+/-0.000 | 479.2+/-41.4 | 1221.9+/-1028.6 | 1396.3+/-1082.1 |

## Pass check (project)
- Throughput rises then flattens under saturation.
- Error rate stays low at moderate load.
- p95/p99 reported (not mean alone).
