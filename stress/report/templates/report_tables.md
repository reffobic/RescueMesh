# Report table skeletons (paste into Phase 1 design/report)

Fill these from `stress/results/*/report/` after running experiments.

## Table A — Latency & throughput vs concurrent clients (E1)

| Clients | Repeats | Throughput (req/s) mean±σ | Error rate mean±σ | p50 ms | p95 ms | p99 ms |
|--------:|--------:|---------------------------:|------------------:|-------:|-------:|-------:|
| 1 | 5 | | | | | |
| 5 | 5 | | | | | |
| 10 | 5 | | | | | |
| 25 | 5 | | | | | |
| 50 | 5 | | | | | |
| 100 | 5 | | | | | |

**Design link:** _______________________________________________

## Table B — Load imbalance (E2)

| Condition | Win share N1 | N2 | N3 | p95 ms |
|-----------|-------------:|---:|---:|-------:|
| Balanced | | | | |
| Node overloaded | | | | |

**Design link:** load-aware Bully uses queue/CPU → work leaves hot node.

## Table C — Metric policy comparison (E3)

| Policy | p95 ms | Throughput | Jain fairness (wins) | Election p95 µs |
|--------|-------:|-----------:|---------------------:|----------------:|
| weighted | | | | |
| queue | | | | |
| cpu | | | | |
| random | | | | |

**Design link:** _______________________________________________

## Table D — Failure under load (E4)

| Clients | Mute s | Success rate during mute | failover_ms mean±σ |
|--------:|-------:|-------------------------:|-------------------:|
| 20 | 5 | | |
| 20 | 20 | | |
| 50 | 10 | | |
| 100 | 10 | | |

## Table E — Rejoin / convergence (E5)

| Clients | Mute s | convergence_ms mean±σ | Plans consistent? |
|--------:|-------:|----------------------:|-------------------|
| 30 | 10 | | Y/N |
| 60 | 10 | | Y/N |

## Caption checklist

Every figure/table lists: git commit, date, machine CPU/RAM, `RM_POLICY` / planner knobs, peer addresses (`RM_NODES`), repeats.
