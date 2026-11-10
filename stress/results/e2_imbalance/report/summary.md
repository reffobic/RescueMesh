# Phase 1 stress: `e2_imbalance`

From per-request CSVs (udp_load_client). mean +/- sample stdev over repeats.

| Clients | Repeats | Throughput (req/s) mean+/-s | Error rate mean+/-s | p50 ms | p95 ms | p99 ms |
|--------:|--------:|----------------------------:|--------------------:|-------:|-------:|-------:|
| 30 | 30 | 58.75+/-3.29 | 0.000+/-0.000 | 508.5+/-1.6 | 522.6+/-16.9 | 617.4+/-401.9 |

## Pass check (project)
- Throughput rises then flattens under saturation.
- Error rate stays low at moderate load.
- p95/p99 reported (not mean alone).
