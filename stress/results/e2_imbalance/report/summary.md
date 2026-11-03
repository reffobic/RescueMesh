# Phase 1 stress: `e2_imbalance`

From per-request CSVs (udp_load_client). mean +/- sample stdev over repeats.

| Clients | Repeats | Throughput (req/s) mean+/-s | Error rate mean+/-s | p50 ms | p95 ms | p99 ms |
|--------:|--------:|----------------------------:|--------------------:|-------:|-------:|-------:|
| 30 | 10 | 56.12+/-4.78 | 0.000+/-0.000 | 507.5+/-2.2 | 519.2+/-3.0 | 762.5+/-694.5 |

## Pass check (project)
- Throughput rises then flattens under saturation.
- Error rate stays low at moderate load.
- p95/p99 reported (not mean alone).
