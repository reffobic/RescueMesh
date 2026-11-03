# Phase 1 — what’s still missing (full commands)

**Already done (do not redo unless you want a rerun):**

| Item | Evidence |
|------|----------|
| E1 scale/stress ramp | `stress/results/e1_udp/` — 38.2k ok, thr saturates ~77 rps, latency jumps at 100 clients |
| E2 imbalance | `balanced` + `overloaded` — N1 win share ~37% → ~0.9% |
| E4 failure | `e4_fail20/` — 100% success during 20s mute, failover ~0.3–0.4 s |
| E5 recovery | `convergence_ms` ~7 s, N2 wins again after restart |

**Still missing for a complete Phase 1 package:**

| # | Gap | Required? | Section below |
|---|-----|-----------|---------------|
| 1 | **Election overhead** report (`election_overhead.md`) | **Yes** (scale/stress measurements) | §1 |
| 2 | **Live demo polish** (winners + `SYNC` on screen) | **Yes** for demo day | §2 |
| 3 | **E3** full policies (weighted/cpu/random; queue may exist) | Optional | §3 |
| 4 | **Peer discovery** (zone/capability directory + P2P) | **No — Phase 2** | §4 |

Shared peers (fix if `hostname -I` differs):

```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
```

---

## 1. Election overhead (REQUIRED)

### Why
Report needs election **frequency / duration** under load. You have latency/throughput/failover; you still need parsed `ELECTION,` lines.

### PC1 — start node with log

```bash
cd ~/Projects/RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1
unset RM_SIM_DELAY_MS
cargo run --release -- node 2>&1 | tee /tmp/node1.log
```

### PC2

```bash
cd ~/Projects/RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=2
unset RM_SIM_DELAY_MS
cargo run --release -- node 2>&1 | tee /tmp/node2.log
```

### PC3

```bash
cd ~/Projects/RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=3
unset RM_SIM_DELAY_MS
cargo run --release -- node 2>&1 | tee /tmp/node3.log
```

### PC4 — generate elections, then parse

```bash
cd ~/Projects/RescueMesh
source stress/lab/nodes.env

# If /tmp/node*.log are only on the cloud PCs, copy them here first:
# scp USER@10.40.32.242:/tmp/node1.log /tmp/
# scp USER@10.40.2.29:/tmp/node2.log /tmp/
# scp USER@10.40.0.73:/tmp/node3.log /tmp/

bash stress/report/run_election_overhead.sh
```

Manual equivalent on PC4:

```bash
mkdir -p stress/results/election_overhead
python3 stress/report/udp_load_client.py \
  --hosts stress/report/hosts.json \
  --clients 25 --requests 40 \
  --out stress/results/election_overhead/load_for_elections.csv

python3 stress/report/parse_election_logs.py \
  --logs /tmp/node1.log /tmp/node2.log /tmp/node3.log \
  --out stress/results/election_overhead

cat stress/results/election_overhead/election_overhead.md
```

**Done when:** `stress/results/election_overhead/election_overhead.md` exists and shows election count + p50/p95 µs + winner counts.

### If you cannot copy `/tmp` logs to PC4 — Option A + merge

Parse **on each cloud PC**, push, then **merge on PC4** into one report.

#### Step A1 — PC1 (log already at `/tmp/node1.log`)

Use `--out stress/results/election_overhead_n1` (a **folder**).  
Do **not** use `--out stress/results` — that drops loose `election_overhead.md` / `elections.csv` in the results root.

```bash
cd ~/Projects/RescueMesh
python3 stress/report/parse_election_logs.py \
  --logs /tmp/node1.log \
  --out stress/results/election_overhead_n1
git add stress/results/election_overhead_n1
git commit -m "Election overhead from node 1"
git push origin stress-testing-lab
```

#### Step A2 — PC2

```bash
cd ~/Projects/RescueMesh
python3 stress/report/parse_election_logs.py \
  --logs /tmp/node2.log \
  --out stress/results/election_overhead_n2
git add stress/results/election_overhead_n2
git commit -m "Election overhead from node 2"
git push origin stress-testing-lab
```

#### Step A3 — PC3

```bash
cd ~/Projects/RescueMesh
python3 stress/report/parse_election_logs.py \
  --logs /tmp/node3.log \
  --out stress/results/election_overhead_n3
git add stress/results/election_overhead_n3
git commit -m "Election overhead from node 3"
git push origin stress-testing-lab
```

#### Step A4 — PC4 merge all three

```bash
cd ~/Projects/RescueMesh
git pull origin stress-testing-lab

python3 stress/report/merge_election_overhead.py \
  --inputs stress/results/election_overhead_n1 \
           stress/results/election_overhead_n2 \
           stress/results/election_overhead_n3 \
  --out stress/results/election_overhead

cat stress/results/election_overhead/election_overhead.md
```

(Defaults are already `n1 n2 n3` → `election_overhead`, so this also works:)

```bash
python3 stress/report/merge_election_overhead.py
cat stress/results/election_overhead/election_overhead.md
```

**Done when:** `stress/results/election_overhead/election_overhead.md` exists (merged unique elections + p50/p95 + winner counts). Missing `n1`/`n2`/`n3` folders are skipped with a warning — pull/push until all three are present for a full merge.

---

## 2. Live demo polish (REQUIRED for demo — no new code)

Keep PC1–3 teeing logs (same as §1). On **PC4** during the talk:

```bash
cd ~/Projects/RescueMesh
source stress/lab/nodes.env
python3 stress/report/udp_load_client.py \
  --hosts stress/report/hosts.json \
  --clients 20 --requests 30 \
  --out /tmp/demo_live.csv
```

**What to point at on screen**

| Say | Show |
|-----|------|
| Different peers elected as load changes | PC1–3 terminals: `ELECTION,...winner=...` changes |
| Load imbalance | (Pre-done) or quick: restart PC1 with `RM_SIM_DELAY_MS=2500`, run same client, N1 stops winning |
| Failure | Ctrl+C PC2 mid-load; PC4 still gets oks |
| Recovery | Restart PC2; show **`SYNC,2,...`** then `winner=2` again |
| Stress numbers | Open `stress/results/e1_udp/report/summary.md` + `election_overhead.md` |

Imbalance one-liner on **PC1** if you redo live:

```bash
# Ctrl+C node 1, then:
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1
export RM_SIM_DELAY_MS=2500
cargo run --release -- node 2>&1 | tee /tmp/node1.log
```

Restore after demo:

```bash
unset RM_SIM_DELAY_MS
export RM_NODE_ID=1
cargo run --release -- node 2>&1 | tee /tmp/node1.log
```

---

## 3. Optional E3 — metric policies (only if write-up needs Table C)

You may already have `policy_queue` data. Still missing for a full table: **weighted**, **cpu**, **random** (and re-check queue under same conditions).

### For each policy `P` in `weighted` `queue` `cpu` `random`

**PC1, PC2, and PC3** — same `P`, restart:

```bash
cd ~/Projects/RescueMesh
export RM_POLICY=P          # replace P: weighted | queue | cpu | random
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1         # 2 on PC2, 3 on PC3
cargo run --release -- node
```

**PC4** after all three nodes are up with that policy:

```bash
cd ~/Projects/RescueMesh
source stress/lab/nodes.env
bash stress/report/run_e3_policies.sh P    # same P
```

Example full sequence on PC4 (after each node restart round):

```bash
bash stress/report/run_e3_policies.sh weighted
# restart all nodes with RM_POLICY=queue, then:
bash stress/report/run_e3_policies.sh queue
# restart all nodes with RM_POLICY=cpu, then:
bash stress/report/run_e3_policies.sh cpu
# restart all nodes with RM_POLICY=random, then:
bash stress/report/run_e3_policies.sh random
```

**Done when:** `stress/results/e2_imbalance/policy_*.csv` exist for each policy; compare p95 / win share in the report.

---

## 4. Peer discovery — NOT missing for Phase 1

**Do not implement for this deadline.**

Phase 2 graded demo:

1. Many clients **register** with Discovery/Presence  
2. **Query** by zone / capability  
3. Directory **consistent** on all cloud peers  
4. Then **P2P** between users  

Phase 1 already covers cloud **membership** (heartbeats, `alive`, `SYNC`). That is enough to mention in the report as “cloud peer liveness”; full discovery waits until Phase 2.

---

## 5. Final checklist after §1 (and §2 for demo day)

```bash
cd ~/Projects/RescueMesh
python3 stress/report/run_phase1_suite.py --scenario check
```

| Project demo / stress need | After §1–§2 |
|----------------------------|-------------|
| Normal cloud operation | Covered |
| Load imbalance | Covered |
| Server failure | Covered |
| Server recovery | Covered |
| Scale/stress (+ election overhead) | Covered once `election_overhead.md` exists |
| Peer discovery | Phase 2 — skip |

**Stress-until-threshold:** already met by E1 (throughput flat ~77 rps, p50 doubles at 100 clients). No need for more ramp unless you want an optional “queue full at 500 users” anecdote.

---

## Quick order today

1. §1 election overhead (PC1–3 tee + PC4 script) → **~5–10 min**  
2. Practice §2 live demo once → **~5 min**  
3. §3 only if advisor/report needs policy table → **~30–40 min**  
4. `run_phase1_suite.py --scenario check`  
5. Commit/push results you care about (`election_overhead/`, not huge accidental dupes)
