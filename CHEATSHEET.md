# RescueMesh cheat sheet (cargo client only)

Substitute: `N` = `RM_CLIENT_USERS`, `R` = `RM_CLIENT_REQUESTS_PER_USER`, `T` = timeout ms, `S`/`X` = sleep seconds.

`main` now includes the full node/client binary (Service 1 planner, load-aware election, fault tolerance, failure injection, and demo GUI). Keep nodes up on PC1–3; run clients on PC4–6.

Watch client stdout for a line like:

```text
CLIENT_METRICS,submitted=…,succeeded=…,failed=…,timed_out=…,mean_ms=…,p95_ms=…,throughput_rps=…,per_node={1:…,2:…,3:…}
```

---

## Setup (every PC)

```bash
git clone https://github.com/reffobic/RescueMesh.git
cd RescueMesh
git fetch origin
git checkout main
```

```bash
hostname -I
```

```bash
cd RescueMesh
cargo build --release
```

```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
```

```bash
# cloud hosts only — that PC’s port
sudo ufw allow 7001/udp   # PC1
sudo ufw allow 7002/udp   # PC2
sudo ufw allow 7003/udp   # PC3
```

---

## Start cloud hosts (PC1–PC3)

```bash
cd RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"

# PC1
export RM_NODE_ID=1
unset RM_SIM_DELAY_MS
cargo run --release -- node

# PC2
export RM_NODE_ID=2
unset RM_SIM_DELAY_MS
cargo run --release -- node

# PC3
export RM_NODE_ID=3
unset RM_SIM_DELAY_MS
cargo run --release -- node
```

Expect: listen on 7001/7002/7003, peer heartbeats, optional `SYNC,<id>,<peer>,<plans>`.

---

## Client template (PC4+)

```bash
cd RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_CLIENT_ID=client-$(hostname)
export RM_CLIENT_USERS=N
export RM_CLIENT_REQUESTS_PER_USER=R
export RM_CLIENT_TIMEOUT_MS=T          # default 5000
export RM_CLIENT_MAX_IN_FLIGHT=32
cargo run --release -- client
```

---

## Scenario A — Smoke (1 request)

```bash
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_TIMEOUT_MS=5000
cargo run --release -- client
```

**Expect:** `submitted=1`, `succeeded=1`, `timed_out=0`, `per_node` has one of `{1,2,3}`.

**Justify:** Proves UDP path + election/reply work before any stress. If this fails, fix `RM_NODES` / firewall / `RM_NODE_ID` first.

---

## Scenario B — Light load

```bash
export RM_CLIENT_USERS=5
export RM_CLIENT_REQUESTS_PER_USER=10
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

**Expect:** `submitted=50`, high `succeeded`, modest `mean_ms` / `p95_ms`, `throughput_rps` clearly above smoke, `per_node` spread across more than one node.

**Justify:** Low concurrency should not saturate the mesh; wins should not stick to a single node forever (load-aware / multicast election). Lab E1 at 5 clients: ~10 rps, p50 ~510 ms, error ~0 (UDP suite).

---

## Scenario C — Heavy / ramp step

```bash
# step 1
export RM_CLIENT_USERS=10
export RM_CLIENT_REQUESTS_PER_USER=20
cargo run --release -- client

# step 2 (same PC or second client PC)
export RM_CLIENT_USERS=25
export RM_CLIENT_REQUESTS_PER_USER=40
cargo run --release -- client

# step 3
export RM_CLIENT_USERS=50
export RM_CLIENT_REQUESTS_PER_USER=40
cargo run --release -- client
```

**Expect:** As `USERS` rises, `throughput_rps` rises then **flattens**; `p95_ms` / `p99_ms` grow; timeouts may appear at the top step.

**Justify:** Classic stress curve — more parallel work until CPU/UDP/election saturate. Lab E1 (`e1_udp`): ~1.8 → ~20 → ~50 → ~76 rps from 1→10→25→50 clients; at 100 clients throughput ~77 rps (flat) while p50 jumps ~510→1290 ms. That flattening + latency growth is the Phase 1 “statistically meaningful load” story.

---

## Scenario D — Timeline (burst → wait → burst)

```bash
export RM_CLIENT_USERS=10
export RM_CLIENT_REQUESTS_PER_USER=20
cargo run --release -- client
sleep 5
cargo run --release -- client
```

**Expect:** Two `CLIENT_METRICS` lines; second run similar success if cluster stayed healthy.

**Justify:** Shows the system is stable across phases (no sticky failure after a quiet period). Use different `USERS`/`REQUESTS` on the second burst if the professor asks for a light→heavy timeline.

---

## Scenario E — Load imbalance (slow one node)

**PC1** — restart node 1 slower:

```bash
# Ctrl+C node 1, then:
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1
export RM_SIM_DELAY_MS=2500
cargo run --release -- node
```

**PC2/PC3** — leave normal (`unset RM_SIM_DELAY_MS`).

**PC4:**

```bash
export RM_CLIENT_USERS=30
export RM_CLIENT_REQUESTS_PER_USER=20
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

**Expect:** `per_node` share for **node 1 drops** vs a balanced run (same client settings, all `RM_SIM_DELAY_MS` unset/300).

**Justify:** Load-aware election should prefer healthier peers; artificial delay makes node 1 a bad winner. Compare two `CLIENT_METRICS` `per_node={…}` maps (balanced vs overloaded).

---

## Scenario F — Failure under load (~20s mute)

**PC4** — start a longer client (large N×R or repeat):

```bash
export RM_CLIENT_USERS=30
export RM_CLIENT_REQUESTS_PER_USER=40
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

**PC2** — after ~15s of load: `Ctrl+C` the node (silence ~20s), then restart:

```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=2
unset RM_SIM_DELAY_MS
cargo run --release -- node
```

**Expect during outage:** client still gets many successes; `per_node` loses `2` temporarily. After restart: `SYNC,2,…` on PC2; node 2 reappears in later `per_node`.

**Justify:** Project ≤20s failure requirement — mesh must keep answering via other nodes. Lab E4: `failover_ms≈286`, `success_rate_during_20s=1.0` (1170/1170). Lab E5: `convergence_ms≈14s` after revive before node 2 wins again.

---

## Scenario G — Multi-client PCs (optional)

On **PC4, PC5, PC6** at once:

```bash
export RM_CLIENT_USERS=20
export RM_CLIENT_REQUESTS_PER_USER=20
cargo run --release -- client
```

**Expect:** Aggregate load ≈ 3× one PC; nodes stay up; higher latency possible.

**Justify:** Real multi-client stress closer to the 6-PC lab map; shows the cloud handles concurrent reporters, not only one harness process.

---

## Phase 1 stress suite (PC4 — formal testing)

Nodes must already be up (PC1–3). Run from repo root on **`stress-testing-lab`**.

```bash
cd RescueMesh
source stress/lab/nodes.env
# or:
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
```

### E1 — Load ramp (latency / throughput)

```bash
python3 stress/report/run_phase1_suite.py --scenario e1 --dry-run --quick   # harness check only
python3 stress/report/run_phase1_suite.py --scenario e1
```

**Outputs:** `stress/results/e1_udp/` + `report/summary.md`  
**Justify:** Increasing clients + repeats → mean/σ latency & throughput (Phase 1 core stress).

### E2 — Imbalance (balanced then overloaded)

```bash
# balanced — all nodes normal delay
python3 stress/report/run_phase1_suite.py --scenario e2 --tag balanced --clients 30

# then on PC1: Ctrl+C, restart with RM_SIM_DELAY_MS=2500 (see Scenario E), then:
python3 stress/report/run_phase1_suite.py --scenario e2 --tag overloaded --clients 30
```

**Outputs:** `stress/results/e2_imbalance/` win-share reports  
**Justify:** Slow node should win less work (load-aware election).

### E3 — Policy compare (restart nodes with RM_POLICY each time)

```bash
python3 stress/report/run_phase1_suite.py --scenario e3
# follow printed steps; for each policy on ALL nodes:
#   export RM_POLICY=weighted|queue|cpu|random
#   restart cargo node, then re-run e2-style capture as instructed
```

**Justify:** Shows metric/policy choice changes win share / behavior.

### E4 — ~20s failure under load

```bash
python3 stress/report/run_phase1_suite.py --scenario e4 --clients 30 --duration-s 60 --mute-s 20 --fail-after-s 15
```

When prompted (~15s): **Ctrl+C PC2**, wait ~20s, restart node 2 (same `RM_NODES` / `RM_NODE_ID=2`).

**Outputs:** `stress/results/e4_fail20/report/failover.json`  
**Justify:** Lab example `failover_ms≈286`, `success_rate_during_20s=1.0`.

### E5 — Rejoin / convergence

```bash
python3 stress/report/run_phase1_suite.py --scenario e5
```

Or full path: re-run E4 with PC2 restart inside the window, then E5. Confirm PC2 log `SYNC,2,…`.

**Outputs:** `convergence.json` / `table_e_rejoin.md`  
**Justify:** Lab example `convergence_ms≈14s` before node 2 wins again.

### Election overhead (tee logs on PC1–3, parse on PC4)

```bash
# on each cloud PC while running nodes (example PC1):
cargo run --release -- node 2>&1 | tee /tmp/node1.log

# on PC4 after collecting /tmp/node1.log node2.log node3.log:
python3 stress/report/parse_election_logs.py \
  --logs /tmp/node1.log /tmp/node2.log /tmp/node3.log \
  --out stress/results/election_overhead
```

**Justify:** Election duration distribution / winner stats for the report.

### Requirements check

```bash
python3 stress/report/run_phase1_suite.py --scenario check
# or: python3 stress/report/check_requirements.py
```

### All-in-one tip

```bash
python3 stress/report/run_phase1_suite.py --scenario all
```

Runs E1 then prints next steps for E2–E5.

---

## Quick result board (from live lab CSVs)

| Situation | What “good” looks like | Why |
|-----------|------------------------|-----|
| Smoke | `succeeded ≈ submitted` | Path + cluster OK |
| Ramp | throughput ↑ then flat; p95 ↑ | Saturation under stress |
| Imbalance | fewer wins on slow node | Load-aware placement |
| Kill node 2 ~20s | successes continue on 1/3 | Failover / no single point of failure |
| Restart node 2 | `SYNC,2,…` then wins again | Rejoin / convergence |

---

## Smoke before the professor

```bash
# PC1–3 nodes already up
cd RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
cargo run --release -- client
```
