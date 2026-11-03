# Phase 1 stress harness — project-description tests

This is **not** “fill empty tables.” It runs the experiments the term project requires, then builds report tables from the data.

Official intent (Phase 1): *Stress the system across increasing client loads and collect statistically meaningful performance data* — plus demos for **load-aware election**, **≤20s failure**, and **rejoin/convergence**.

| Project need | Harness scenario | Primary outputs |
|--------------|------------------|-----------------|
| Ramp load + mean/variance latency & throughput | `e1` | `e1_udp/report/summary.md` (Table A) |
| Different peers win / load metrics | `e1` win share + `e2` | `win_share.md` (Tables A/B) |
| Justify metrics / policies | `e3` | Table C |
| 20s silence; others still answer | `e4` | `failover.json`, `table_d_failure.md` |
| Rejoin sync then work again | `e5` / e4 revive | `convergence.json`, `table_e_rejoin.md` |
| Election duration / overhead | `parse_election_logs.py` | `election_overhead.md` |
| Checklist | `--scenario check` | PASS/FAIL gaps |

---

## 0. Bring up the cloud (required before any scenario)

Follow `MULTI_PC_LAB_GUIDE.md`. Minimum:

```bash
# Each cloud PC (correct RM_NODE_ID!)
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1   # 2 on PC2, 3 on PC3
cargo run --release -- node 2>&1 | tee /tmp/node${RM_NODE_ID}.log
```

Update `stress/report/hosts.json` if `hostname -I` shows different `10.40.*` addresses.

On the **coordinator client PC**:

```bash
cd ~/Projects/RescueMesh   # or your path
source stress/lab/nodes.env
python3 stress/lab/check_connectivity.py
```

---

## 1. E1 — Increasing load (core stress bullet)

**What it tests:** concurrent clients ↑; each point repeated; p50/p95/p99 + throughput + error rate with **mean ± σ**.

```bash
source stress/lab/nodes.env

# Harness debug (no cluster needed):
python3 stress/report/run_phase1_suite.py --scenario e1 --dry-run --quick

# Live (nodes up):
python3 stress/report/run_phase1_suite.py --scenario e1
# Optional denser: --repeats 5 --requests-per-client 40
```

**Pass:** `stress/results/e1_udp/report/summary.md` filled; throughput trend makes sense; moderate-load errors low; **win_share.md** shows work not stuck on one node forever.

Volume: aim for thousands–tens of thousands of successful rows across the campaign (raise `--requests-per-client` / repeats if needed).

---

## 2. E2 — Load imbalance (demo + Table B)

**What it tests:** artificially load/slow one server → election sends work elsewhere.

1. Run balanced:

```bash
python3 stress/report/run_phase1_suite.py --scenario e2 --tag balanced --clients 30
```

2. On **PC1 only**, restart the node slower:

```bash
export RM_SIM_DELAY_MS=2500   # others stay at default ~300
export RM_NODES=...
export RM_NODE_ID=1
cargo run --release -- node
```

3. Capture overloaded:

```bash
python3 stress/report/run_phase1_suite.py --scenario e2 --tag overloaded --clients 30
```

**Pass:** node 1’s share in `win_share.md` **drops** vs balanced.

---

## 3. E3 — Policy comparison (justify metrics)

```bash
python3 stress/report/run_phase1_suite.py --scenario e3
# follow printed steps: set RM_POLICY on ALL nodes, restart, re-run load
```

Policies: `weighted` | `queue` | `cpu` | `random`.

**Pass:** Table C + short write-up tying numbers to metric choice (random as strawman).

---

## 4. E4 — ≤20s failure under load (required demo)

**What it tests:** silence one node up to 20s; **peers continue**; measure **failover_ms**.

```bash
python3 stress/report/run_phase1_suite.py --scenario e4 --clients 30 --duration-s 60 --mute-s 20 --fail-after-s 15
```

When the script prints the plan:

1. At **t ≈ fail_after**, on PC2: `Ctrl+C` the node.  
2. Wait **20s**.  
3. Restart PC2 (`RM_NODE_ID=2`).  
4. Let the client finish.

**Pass:** `table_d_failure.md` shows **success_rate_during_mute > 0**; `failover_ms` finite.

---

## 5. E5 — Rejoin / convergence

After E4 (node restarted inside the window):

```bash
python3 stress/report/run_phase1_suite.py --scenario e5
```

Confirm on the revived node log: `SYNC,<id>,<peer>,…` then ready.  
**Pass:** `convergence_ms` in `table_e_rejoin.md`; node reappears in `processed_by`.

---

## 6. Election overhead (measurements section)

While E1/E4 run, nodes should `tee` stdout. Then:

```bash
python3 stress/report/parse_election_logs.py \
  --logs /tmp/node1.log /tmp/node2.log /tmp/node3.log \
  --out stress/results/election_overhead
```

**Pass:** election duration p50/p95 and winner counts in `election_overhead.md`.

---

## 7. Requirement checklist

```bash
python3 stress/report/run_phase1_suite.py --scenario check
# or: python3 stress/report/check_requirements.py
```

Fix every `FAIL` before claiming the stress bullet done.

---

## 8. Tool map

| Script | Role |
|--------|------|
| `run_phase1_suite.py` | Orchestrates e1–e5 / check |
| `udp_load_client.py` | Per-request CSV (needed for failover & p95) |
| `analyze_request_csv.py` | Tables A/D/E + win share + failover_ms |
| `parse_election_logs.py` | Election duration from `ELECTION,` lines |
| `check_requirements.py` | Project checklist PASS/FAIL |
| `run_phase1_experiments.py` | Optional cargo-client batch ramp (CLIENT_METRICS) |
| `build_report_tables.py` | Aggregate cargo `runs.csv` if you use that path |

Prefer **`udp_load_client` + suite** for grade evidence; cargo `CLIENT_METRICS` alone cannot compute failover_ms.

---

## 9. Caption under every table/figure

```text
RescueMesh git=$(git rev-parse --short HEAD)  RM_NODES=...  RM_POLICY=...  RM_SIM_DELAY_MS=...  repeats=...  date=...
```
