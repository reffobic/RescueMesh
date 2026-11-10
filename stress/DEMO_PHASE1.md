# Phase 1 demo + remaining tests (election overhead, polish, optional E3)

## A. Do you already stress “enough” (thresholds)?

From **current** `stress/results` (E1/E2/E4/E5):

| Threshold / signal | Your data | Enough? |
|--------------------|-----------|---------|
| Throughput rises then **flattens** | ~50 rps @25c → **~76 rps @50c ≈ ~77 @100c** | **Yes — saturation** |
| Latency **jumps** at high load | p50 ~510 ms → **~1290 ms @100 clients** | **Yes — soft threshold** |
| Large automated volume | **38,200** E1 + 12k E2 + ~3k E4, **0% errors** in those runs | **Yes** |
| Failure under load | E4: **100%** success during 20s mute; failover **~0.3–0.4 s** | **Yes** |
| Convergence | E5: **~7.4 s** to N2 winning again | **Yes** |
| Imbalance response | E2: N1 share **37% → 0.9%** | **Yes** |
| Election overhead artifact | `election_overhead/` **missing** | **Do section B** |
| Hard error cliff (queue full) | Seen with cargo **500 users** (optional anecdote) | Optional; not required |
| Peer discovery (zone/capability) | Not in Phase 1 codebase/results | **Phase 2** |

**Verdict:** You do **not** need to keep raising clients until everything fails. You already hit the **performance threshold** (throughput flat + latency blow-up). Remaining Phase 1 gap is mainly **election overhead logs** (+ optional E3 + live demo polish).

---

## B. Election overhead — exact commands

### B1. PC1 / PC2 / PC3 — restart nodes with tee

```bash
cd ~/Projects/RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1    # 2 on PC2, 3 on PC3
unset RM_SIM_DELAY_MS
cargo run --release -- node 2>&1 | tee /tmp/node${RM_NODE_ID}.log
```

### B2. PC4 — short load + parse

```bash
cd ~/Projects/RescueMesh
source stress/lab/nodes.env

# If logs are only on the cloud PCs, copy them first:
# scp user@pc1:/tmp/node1.log /tmp/
# scp user@pc2:/tmp/node2.log /tmp/
# scp user@pc3:/tmp/node3.log /tmp/

bash stress/report/run_election_overhead.sh
# or manually:
# python3 stress/report/udp_load_client.py --hosts stress/report/hosts.json \
#   --clients 25 --requests 40 --out stress/results/election_overhead/load_for_elections.csv
# python3 stress/report/parse_election_logs.py \
#   --logs /tmp/node1.log /tmp/node2.log /tmp/node3.log \
#   --out stress/results/election_overhead
```

**Pass:** `stress/results/election_overhead/election_overhead.md` has election count + p50/p95 duration + winner counts.

---

## C. Live demo polish (no new binary — talk through the screen)

| Demo line (project) | What to show live |
|---------------------|-------------------|
| Normal operation | PC4: small load; PC1–3 logs show **different** `ELECTION,...winner=...` / changing `processed_by` |
| Load imbalance | Restart PC1 with `RM_SIM_DELAY_MS=2500`; show N1 almost stops winning |
| Server failure | Ctrl+C PC2 under load; clients still get replies from 1/3 |
| Server recovery | Restart PC2; show **`SYNC,2,...`** then winners include 2 again |
| Scale/stress | Open `e1_udp/report/summary.md` + election_overhead.md |

Suggested 2-minute live load during demo:

```bash
# PC4
python3 stress/report/udp_load_client.py --hosts stress/report/hosts.json \
  --clients 20 --requests 30 --out /tmp/demo_live.csv
```

---

## D. Optional E3 — weighted / cpu / random / queue

On **all three nodes**, for each policy, restart with:

```bash
export RM_POLICY=weighted   # then queue, cpu, random
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1         # 2 / 3 elsewhere
cargo run --release -- node
```

On **PC4** after each restart:

```bash
bash stress/report/run_e3_policies.sh weighted
bash stress/report/run_e3_policies.sh queue
bash stress/report/run_e3_policies.sh cpu
bash stress/report/run_e3_policies.sh random
```

Compare p95 / win share across `policy_*` CSVs → Table C. Skip if report already justifies `weighted` from E2.

---

## E. Why peer discovery is Phase 2 (graded demo)

Phase 1 deliverable = cloud **incident processing + election + FT + stress**.

Phase 2 graded “Peer discovery” means:

1. Many **clients/users register online** with the Discovery/Presence service  
2. **Query by zone / capability**  
3. **Directory consistent** on all cloud replicas  
4. Then **direct P2P** between those users  

Your Phase 1 heartbeats + `RM_NODES` + `SYNC` are **cloud membership**, not that demo. Do **not** block Phase 1 on building Discovery.

---

## F. Coverage after you finish section B (+ optional D)

| Requirement | After election logs | Notes |
|-------------|---------------------|-------|
| Normal cloud operation | Yes | E1 + live winners |
| Load imbalance | Yes | E2 done |
| Server failure | Yes | E4 done |
| Server recovery | Yes | E5/SYNC done |
| Peer discovery | Phase 2 | Not required now |
| Scale/stress | Yes | E1 thresholds + failover/convergence + **election overhead** |

Final check:

```bash
python3 stress/report/run_phase1_suite.py --scenario check
```
