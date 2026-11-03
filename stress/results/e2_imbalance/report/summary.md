# Phase 1 stress: `e2_imbalance`

From per-request CSVs (udp_load_client). mean +/- sample stdev over repeats.

| Clients | Repeats | Throughput (req/s) mean+/-s | Error rate mean+/-s | p50 ms | p95 ms | p99 ms |
|--------:|--------:|----------------------------:|--------------------:|-------:|-------:|-------:|
| 30 | 5 | 60.37+/-0.73 | 0.000+/-0.000 | 509.0+/-0.9 | 517.4+/-0.8 | 520.4+/-1.1 |

## Pass check (project)
- Throughput rises then flattens under saturation.
- Error rate stays low at moderate load.
- p95/p99 reported (not mean alone).
