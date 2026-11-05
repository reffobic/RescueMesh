# Election overhead (merged n1+n2+n3)

## Sources

- stress/results/election_overhead_n1 (769 rows)
- stress/results/election_overhead_n2 (91 rows)
- stress/results/election_overhead_n3 (140 rows)

- Merged unique election samples: **1000**

## Election duration (us)

| n | mean | p50 | p95 | p99 |
|--:|-----:|----:|----:|----:|
| 1000 | 100885.5 | 100870.5 | 101792.0 | 101885.0 |

## Winner counts

| Node | Elections won |
|-----:|--------------:|
| 1 | 769 |
| 2 | 91 |
| 3 | 140 |

Pass: election duration + winner diversity reported under load.
