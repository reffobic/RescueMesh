# RescueMesh Phase 1 — Stress Testing & Performance Plan

**Course requirement (verbatim intent):** *Stress the system across increasing client loads and collect statistically meaningful performance data.*

**Also required by the term project (measurements / demos):**

| Source | What you must show / measure |
|--------|------------------------------|
| Phase 1 bullet | Increasing client load + stats |
| Scale/stress objective | Latency, throughput, election overhead, failure-recovery, convergence |
| Measurements section | mean/median **and** p95/p99; throughput vs clients; election frequency/duration/messages; load distribution fairness; before/during/after failure; recovery/convergence time; CPU/mem/net/queue where relevant |
| Demo: Normal | Multiple requests; **different peers elected** as load changes |
| Demo: Load imbalance | Artificially load one server; election responds |
| Demo: Server failure | Elected server nonresponsive during work; peers continue |
| FT section | Measure **failover time** and **recovery/convergence time under different loads** |
| Analysis rule | Tens of thousands of ops where feasible; **not** demo screenshots alone; connect numbers to design decisions |

This folder is the **evaluation harness**. It assumes the cloud speaks **JSON `Msg` over UDP** (as on `distributed-election`). Adapt ports/paths when RPC changes.

---

## 1. Goals (what “pass” means)

You pass the stress bullet when the report contains:

1. **Ramp curves** — latency (p50/p95/p99) and throughput vs concurrent clients (e.g. 1 → 10 → 50 → 100 → 200…).
2. **Repeats** — each load point run **≥ 5 times** (ideally 10); report mean ± stdev or 95% CI, not one lucky run.
3. **Volume** — aim for **≥ 10k–50k** successful requests across the campaign (project: “tens of thousands … where feasible”).
4. **Election evidence** — CSV of winners + scores; plot **fraction of work per node**; imbalance experiment.
5. **Failure evidence** — timeline before / during / after 1–20s silence; failover_ms and convergence_ms vs load.
6. **Interpretation** — each plot answers “so what for our design?” (e.g. why weighted metrics beat queue-only).

---

## 2. Experiment matrix (run these)

### E1 — Baseline ramp (no faults)

| Parameter | Value |
|-----------|--------|
| Clients (concurrent) | 1, 5, 10, 25, 50, 100, (200 if stable) |
| Requests per client | enough so total ≈ 2000+ per load point |
| Repeats | 5–10 |
| Faults | none |
| Metrics | latency p50/p95/p99, throughput (req/s), error rate, per-node win share |

**Pass:** Smooth-ish curves; error rate &lt; 1% at moderate load; report saturation point.

### E2 — Load imbalance (demo + data)

| Parameter | Value |
|-----------|--------|
| Artificial load | Node 0: `sim_load` high queue/CPU (or burn CPU) |
| Clients | fixed moderate (e.g. 30) |
| Repeats | 5 |

**Pass:** Node 0 wins **significantly fewer** elections than before; table + bar chart.

### E3 — Metric policy comparison (justify metrics)

| Policy | `RM_POLICY` |
|--------|-------------|
| Weighted (default) | `weighted` / unset |
| Queue only | `queue` |
| CPU only | `cpu` |
| Random (baseline) | `random` |

Same load for each; compare p95 latency, fairness (Jain index or win entropy), election duration.

**Pass:** Written justification: which policy is best under which stress; random as strawman.

### E4 — Failure during load (demo + failover)

| Parameter | Value |
|-----------|--------|
| Steady clients | 20, 50, 100 |
| Inject | mute/fail one node for 5s, 10s, 20s |
| Timing | fail while traffic is on; preferably while that node is often winning |

**Metrics:**

- `failover_ms` = time from last success that needed the failed node → first success completed by remaining peers (or: gap in successful replies attributable to fail start).
- Success rate during silence window.
- Election re-attempts during window.

**Pass:** During silence, **service continues**; failover_ms reported vs load; no permanent hang.

### E5 — Rejoin / convergence under load

After E4 revive:

- `convergence_ms` = Failed end → node reports Active / accepts client work after sync.
- `entries_synced` if logged.
- Consistency check: sample `request_id`s present on all live nodes.

**Pass:** convergence_ms vs load table; node does **not** serve clients before catch-up.

### E6 — Long soak (optional but strong)

30–60 minutes at 50% of max stable load; watch memory leaks / error creep.

---

## 3. Metrics definitions (use exactly these names in CSV)

| Metric | Definition | Unit |
|--------|------------|------|
| `latency_ms` | Client send Request → receive Reply | ms |
| `throughput_rps` | Successful replies / wall time | 1/s |
| `error_rate` | timeouts+errors / attempts | 0–1 |
| `election_us` | From node CSV `ELECTION,...` elapsed | µs |
| `election_count` | Elections observed in window | count |
| `win_share_node_i` | Wins by node i / total wins | 0–1 |
| `failover_ms` | See E4 | ms |
| `convergence_ms` | See E5 | ms |
| `under_replicated_rate` | Replies flagged under_replicated / successes | 0–1 |
| `queue_len` / `cpu` | From node stats logs | various |

**Statistically meaningful:** for each (experiment, load) cell report **n, mean, stdev, median, p95, p99**.

---

## 4. How results should look (report figures)

### Figure set (minimum)

1. **Latency vs clients** — lines for p50, p95, p99 (± error bars from repeats).
2. **Throughput vs clients** — rises then flattens (saturation).
3. **Error rate vs clients**.
4. **Win share stacked bar** — normal vs imbalance (E2).
5. **Policy comparison** — grouped bars p95 + fairness (E3).
6. **Failover & convergence vs load** — two lines (E4/E5).
7. **Timeline strip** (one failure run) — success rate in 1s buckets before/during/after mute.

### Table templates

See `templates/`. Every table caption must state: load, repeats, duration, git commit, machine specs.

### Example interpretation bullets (copy style)

- “p99 jumps after 100 clients → queue_cap / exec_timeout retune.”
- “Under CPU burn on node 0, win_share drops from 0.34 to 0.08 → load-aware Bully reacts.”
- “failover_ms ≈ election timeout + one plan; grows slowly with load because queues are longer.”

---

## 5. Instructions per component (pass stress by design)

### 5.1 Platform / RPC (Mohamed)

| Must provide | Why stress needs it |
|--------------|---------------------|
| Stable 3-node config (`hosts.json`) | Harness targets fixed peers |
| Multicast or client fan-out to all peers | Project load-balance model |
| JSON `Msg` framing stable | Scripts encode/decode |
| Timeouts + retries with **same request_id** | No double-charge latency wrongly |
| Structured logs: ISO time, `request_id`, node_id | Join client+server CSVs |
| `/stats` or periodic `STATS,...` line | Queue/CPU without scraping println noise |

**Optimal:** bound UDP buffers; don’t block reactor on planner CPU — offload heavy work to `spawn_blocking` so heartbeats/election stay live under stress.

### 5.2 Load-aware election (Farida)

| Must provide | Why |
|--------------|-----|
| Keep `ELECTION,strategy,req_id,attempt,winner,score,elapsed_us,complete` CSV | Election overhead plots |
| `sim_load(extra_queue, cpu)` or env burn | E2 imbalance demo |
| `RM_POLICY` queue/cpu/random/weighted | E3 justify metrics |
| Exclude Failed/muted nodes | E4 correctness |
| Fast rounds at n=3 (`RM_T_OK_MS`, `RM_T_COORD_MS` tunable) | Tail latency under load |

**Optimal:** snapshot scores once per round (already); avoid electing on every tiny metric blip; log message counts if possible (`msgs_sent` delta).

**Pass stress when:** E2 shows shift; E3 has ≥3 policies compared; election_us p95 stays ≪ plan latency.

### 5.3 Planner / concurrency (Ismaiel)

| Must provide | Why |
|--------------|-----|
| Tunable plan cost (`RM_PLAN_MS` or CPU loops) | Create real queues for load balancing |
| Idempotent execute by `request_id` | Retries during E4 don’t corrupt stats |
| Bounded in-flight / queue metric update | Metrics match reality |
| No global lock across network I/O | Throughput scales |

**Optimal:** default plan cost ~50–300ms so election overhead is visible but cluster isn’t dead; document parallelization (async tasks + blocking pool).

**Pass stress when:** throughput scales with clients until CPU/queue saturation; Cached path returns fast on retry.

### 5.4 Replication / state (Hoda)

| Must provide | Why |
|--------------|-----|
| Majority replicate before success (or document interim) | under_replicated_rate low |
| Durable or at least peer catch-up API | E5 convergence |
| Logs: `REPLICATE,req_id,ok,elapsed_ms` and `CATCHUP,node,entries,ms` | Convergence plots |
| Recovering rejects client work | Fair failover measurement |

**Optimal:** batch or pipeline ACKs; don’t hold client Reply until unnecessary disk fsync if you measure separately — but for grade, prefer **correct then fast**.

**Pass stress when:** during E4, remaining nodes still commit; after rejoin, convergence_ms finite and plans match.

### 5.5 Fault injector + harness (Haya) — this folder

| Must provide | Why |
|--------------|-----|
| Scripted mute 1–20s | E4/E5 automation |
| Load ramp runner + repeats | E1 |
| `analyze_results.py` tables/plots | Report |
| Design log link: plot → decision | Grading emphasis |

**Pass stress when:** full matrix green checklist in §8.

---

## 6. How to implement for optimal stress results (engineering checklist)

1. **Fix compile / wire `main`** on election branch before measuring.
2. **One log format** everywhere (CSV lines to stdout or `results/raw/`).
3. **Clock sync** not required if all latency is client-side; server election_us is local.
4. **Warm-up** 5–10s discarded before each measured window.
5. **Cool-down** between repeats; clear in-memory only if testing recovery cold.
6. **Pin experiment metadata** in `run_meta.json`: commit, policies, plan_ms, peer list.
7. **Don’t change code mid-campaign** without new commit hash in meta.
8. **Separate CPU burn from plan cost** so imbalance (E2) isn’t confused with global slowdown.

---

## 7. How to run (quick start)

```bash
# From RescueMesh/
pip install -r stress/requirements.txt

# 1) Start three nodes (your binary once wired), e.g. ports 9000,9001,9002
# 2) Baseline ramp
python stress/run_experiments.py --config stress/configs/e1_ramp.yaml

# 3) Analyze
python stress/analyze_results.py --input stress/results/e1_ramp --out stress/results/e1_ramp/report
```

Dry-run without servers (synthetic):

```bash
python stress/run_experiments.py --config stress/configs/e1_ramp.yaml --dry-run
```

---

## 8. Final acceptance checklist

- [ ] E1 ramp plots + tables with repeats  
- [ ] E2 imbalance demo numbers  
- [ ] E3 ≥3 policies compared + written justification  
- [ ] E4 failure under load: continue service + failover_ms  
- [ ] E5 convergence_ms + consistency spot-check  
- [ ] ≥10k requests total in raw data (or justified lower bound)  
- [ ] p50/p95/p99 everywhere latency is claimed  
- [ ] Each figure has a design-decision sentence  
- [ ] Git commit hashes recorded  

---

## 9. File map

| File | Role |
|------|------|
| `load_client.py` | Concurrent UDP clients, latency CSV |
| `run_experiments.py` | Matrix driver (ramp, imbalance, failure) |
| `analyze_results.py` | Stats + matplotlib figures |
| `fault_inject.py` | Tell a node to mute (control msg or SSH hook) |
| `configs/*.yaml` | Experiment parameters |
| `templates/*` | Report table skeletons |
| `PROTOCOL.md` | Wire format assumed by scripts |
