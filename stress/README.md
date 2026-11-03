# RescueMesh stress / multi-PC lab

Branch: **`stress-testing-lab`** (based on `incident_proc_coord`)

## Guides

| Doc | Purpose |
|-----|---------|
| **[MULTI_PC_LAB_GUIDE.md](MULTI_PC_LAB_GUIDE.md)** | Bring up 6 PCs, IPs, firewall, smoke |
| **[PHASE1_REPORT_GUIDE.md](PHASE1_REPORT_GUIDE.md)** | **Project-required stress tests** (E1–E5, failover, election overhead) |
| **[DEMO_PHASE1.md](DEMO_PHASE1.md)** | Election overhead + live demo + E3 + coverage/thresholds |
| **[MISSING_PHASE1.md](MISSING_PHASE1.md)** | **What’s left** — full commands only for gaps |

## What the harness actually tests

Aligned to the Phase 1 project description:

1. **Increasing client load** with repeats → latency p50/p95/p99 + throughput mean±σ  
2. **Load-aware election evidence** → per-node win share; imbalance via `RM_SIM_DELAY_MS`  
3. **≤20s failure** under load → success continues; **failover_ms**  
4. **Rejoin / convergence** → node wins again after `SYNC`  
5. **Election overhead** from teed `ELECTION,` node logs  
6. **`check_requirements.py`** PASS/FAIL checklist  

## Quick start (nodes already up)

```bash
source stress/lab/nodes.env

# Offline harness check
python3 stress/report/run_phase1_suite.py --scenario e1 --dry-run --quick

# Live campaign
python3 stress/report/run_phase1_suite.py --scenario e1
python3 stress/report/run_phase1_suite.py --scenario e4 --clients 30 --duration-s 60
python3 stress/report/parse_election_logs.py --logs /tmp/node1.log /tmp/node2.log /tmp/node3.log \
  --out stress/results/election_overhead
python3 stress/report/run_phase1_suite.py --scenario check
```

## Lab bring-up

See `MULTI_PC_LAB_GUIDE.md`. Short path:

```bash
bash stress/lab/start_node.sh 1   # cloud: 1|2|3
bash stress/lab/smoke_test.sh     # client
```
