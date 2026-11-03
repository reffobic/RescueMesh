# Phase 1 stress: `fail_c30.csv`

From per-request CSVs (udp_load_client). mean +/- sample stdev over repeats.

| Clients | Repeats | Throughput (req/s) mean+/-s | Error rate mean+/-s | p50 ms | p95 ms | p99 ms |
|--------:|--------:|----------------------------:|--------------------:|-------:|-------:|-------:|
| 30 | 1 | 55.84+/-0.00 | 0.000+/-0.000 | 507.0+/-0.0 | 535.0+/-0.0 | 1135.0+/-0.0 |

## Pass check (project)
- Throughput rises then flattens under saturation.
- Error rate stays low at moderate load.
- p95/p99 reported (not mean alone).
