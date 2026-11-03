# Election overhead (merged n1+n2+n3)

## Sources

- stress\results\election_overhead_n1 (2404 rows)
- stress\results\election_overhead_n2 (1119 rows)
- stress\results\election_overhead_n3 (234 rows)

- Merged unique election samples: **3757**

## Election duration (us)

| n | mean | p50 | p95 | p99 |
|--:|-----:|----:|----:|----:|
| 3757 | 100968.7 | 100968.0 | 101764.0 | 102057.9 |

## Winner counts

| Node | Elections won |
|-----:|--------------:|
| 1 | 2404 |
| 2 | 1119 |
| 3 | 234 |

Pass: election duration + winner diversity reported under load.
