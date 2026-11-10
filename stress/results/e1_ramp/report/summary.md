# Stress report: `e1_ramp`

Statistically aggregated over repeats (mean ± sample stdev).

| clients | repeats | thr_rps | err | p50 ms | p95 ms | p99 ms |
|--------:|--------:|--------:|----:|-------:|-------:|-------:|
| 1 | 5 | 19.62±0.68 | 0.020±0.033 | 78.8±5.5 | 113.6±4.6 | 117.2±2.7 |
| 5 | 5 | 92.68±4.41 | 0.016±0.005 | 75.9±2.9 | 115.4±1.0 | 118.7±0.7 |
| 10 | 5 | 173.18±4.52 | 0.024±0.005 | 76.4±1.6 | 115.9±0.9 | 118.9±0.2 |
| 25 | 5 | 444.59±9.43 | 0.017±0.002 | 75.2±0.9 | 116.1±0.8 | 119.3±0.4 |
| 50 | 5 | 858.56±30.87 | 0.018±0.002 | 75.3±1.2 | 115.6±0.3 | 119.2±0.2 |
| 100 | 5 | 1683.76±34.86 | 0.021±0.003 | 75.4±0.5 | 115.5±0.4 | 119.0±0.1 |

## How to read
- **Throughput** should rise then flatten (saturation).
- **p95/p99** matter more than the mean for RescueMesh demos.
- Connect jumps to timeouts, plan cost, election, or replication quorum.
