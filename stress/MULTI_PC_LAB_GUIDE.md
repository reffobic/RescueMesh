# RescueMesh multi-PC lab guide + stress testing

Branch base: **`incident_proc_coord`**  
Stress branch: **`stress-testing-lab`**

This guide makes sure **UDP between the 6 PCs works**, nodes form a cluster, clients get replies, then you ramp load for Phase 1 stress data.

---

## 0. Lab map

| Role | PC | IP | Port / notes |
|------|-----|-----|----------------|
| Cloud node **1** | PC1 | `10.40.32.242` | UDP **7001** |
| Cloud node **2** | PC2 | `10.40.2.29` | UDP **7002** |
| Cloud node **3** | PC3 | `10.40.0.73` | UDP **7003** |
| Client | PC4 | `10.40.1.240` | ephemeral UDP out |
| Client | PC5 | `10.40.39.134` | ephemeral UDP out |
| Client | PC6 | `10.40.5.45` | ephemeral UDP out |

**Same peer string everywhere:**

```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
```

Or: `source stress/lab/nodes.env`

### Critical: set `RM_NODE_ID` on cloud PCs

`cargo run -- node` alone is **not enough**. Each cloud PC must set its own id:

| PC | Must set |
|----|----------|
| PC1 | `export RM_NODE_ID=1` |
| PC2 | `export RM_NODE_ID=2` |
| PC3 | `export RM_NODE_ID=3` |

If two PCs both use default `RM_NODE_ID=1`, the cluster breaks.

---

## 1. One-time setup on EVERY PC

### 1.1 Get code

```bash
git clone https://github.com/reffobic/RescueMesh.git
cd RescueMesh
git fetch origin
git checkout stress-testing-lab   # or: incident_proc_coord then merge stress docs
# If branch not pushed yet:
# git checkout incident_proc_coord
```

### 1.2 Confirm your IP

```bash
hostname -I
# or: ip -4 addr | grep inet
```

Match the table above. If your IP differs, **update `RM_NODES` on all machines** to the real addresses.

### 1.3 Install Rust (if needed)

```bash
rustc --version
cargo --version
# https://rustup.rs if missing
```

### 1.4 Build once (same on all PCs)

```bash
cd RescueMesh
cargo build --release
```

Use `target/release/rescue_mesh` for less JIT noise during stress, or `cargo run --release -- …`.

### 1.5 Firewall (UDP must be open)

On **each cloud PC**, allow inbound UDP on **its** port:

```bash
# Ubuntu ufw examples
sudo ufw allow 7001/udp   # only on PC1
sudo ufw allow 7002/udp   # only on PC2
sudo ufw allow 7003/udp   # only on PC3
sudo ufw status
```

Clients need **outbound** UDP (usually open). If replies fail, also allow ephemeral UDP inbound on clients, or disable host firewall temporarily for the lab.

### 1.6 Connectivity check (before cargo)

From **each** PC, test reachability of the three node ports:

```bash
bash stress/lab/check_udp.sh
# or Python:
python3 stress/lab/check_connectivity.py
```

Manual checks:

```bash
# ICMP (optional — some labs block ping)
ping -c 2 10.40.32.242
ping -c 2 10.40.2.29
ping -c 2 10.40.0.73

# Port open from another machine (install nmap if available)
nc -u -v -z 10.40.32.242 7001
```

**Pass criteria:** no “destination unreachable” for the three cloud IPs; after nodes start, client gets `CLIENT_METRICS` with `succeeded > 0`.

Also confirm this PC’s address matches the lab map:

```bash
hostname -I
python3 stress/lab/verify_ips.py
# Cloud example: python3 stress/lab/verify_ips.py --expect 10.40.32.242
```

---

## 2. Start cloud nodes (PC1–PC3)

Open a terminal **on each cloud PC**. Keep them running.

### Wrong vs right start (read this once)

**Wrong** — looks like it works but never joins the lab cluster:

```bash
cargo build --release
cargo run --release --          # no mode, no env → node 1 on 127.0.0.1 only
```

Typical bad output:

```text
RescueMesh node 1 listening; ready=false alive=[1] ...
[node 1] warmup: no peer heard, serving alone
```

That means `RM_NODES` fell back to `1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003` and `RM_NODE_ID` defaulted to `1`. Peers on other PCs are invisible.

**Right** — always export peers + this PC’s id, then pass `node`:

```bash
hostname -I                     # match IP → node id in the table below
source stress/lab/nodes.env     # sets RM_NODES to the three lab IPs
export RM_NODE_ID=1             # 1 / 2 / 3 for PC1 / PC2 / PC3
cargo run --release -- node
# or: bash stress/lab/start_node.sh 1
```

Dead-code compiler **warnings** (`sim_load`, `FaultToleranceManager`, …) are normal on this branch — ignore them if `Finished release` appears.

### PC1 — `10.40.32.242`

```bash
cd RescueMesh
source stress/lab/nodes.env
export RM_NODE_ID=1
cargo run --release -- node
```

Expect something like: `RescueMesh node 1 listening; …`  
After PC2 and PC3 are up with the same `RM_NODES`, `alive` should list more than `[1]` (not stuck on `serving alone` forever).

### PC2 — `10.40.2.29`

```bash
cd RescueMesh
source stress/lab/nodes.env
export RM_NODE_ID=2
cargo run --release -- node
```

### PC3 — `10.40.0.73`

```bash
cd RescueMesh
source stress/lab/nodes.env
export RM_NODE_ID=3
cargo run --release -- node
```

### Warmup / peer health

Watch logs for warmup / peer heard (cluster forming). After ~1–2 s all three should see each other via heartbeats.

**Quick peer test from PC1:**

```bash
# In another terminal on any PC with code:
source stress/lab/nodes.env
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_MAX_IN_FLIGHT=1
cargo run --release -- client
```

You should see a `CLIENT_METRICS,...` line with `succeeded=1` (or close).

---

## 3. Client PCs (PC4–PC6) — commands in order

Start **PC1–PC3 nodes first**. Do **not** set `RM_NODE_ID` on clients (only `RM_NODES`).

**Pass (smoke):** `CLIENT_METRICS,...succeeded=1...` (and not all `timed_out`).  
**Order:** smoke on PC4 → then smoke PC5/PC6 → then run the load block on PC4/5/6 together for multi-PC stress.

### PC4 — `10.40.1.240`

**Smoke:**

```bash
cd ~/Projects/RescueMesh
hostname -I
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_CLIENT_ID=pc4
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_MAX_IN_FLIGHT=1
export RM_CLIENT_TIMEOUT_MS=5000
cargo run --release -- client
```

**Load:**

```bash
export RM_CLIENT_USERS=15
export RM_CLIENT_REQUESTS_PER_USER=20
export RM_CLIENT_MAX_IN_FLIGHT=32
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

### PC5 — `10.40.39.134`

**Smoke:**

```bash
cd ~/Projects/RescueMesh
hostname -I
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_CLIENT_ID=pc5
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_MAX_IN_FLIGHT=1
export RM_CLIENT_TIMEOUT_MS=5000
cargo run --release -- client
```

**Load** (keep `RM_CLIENT_ID=pc5`):

```bash
export RM_CLIENT_USERS=15
export RM_CLIENT_REQUESTS_PER_USER=20
export RM_CLIENT_MAX_IN_FLIGHT=32
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

### PC6 — `10.40.5.45`

**Smoke:**

```bash
cd ~/Projects/RescueMesh
hostname -I
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_CLIENT_ID=pc6
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_MAX_IN_FLIGHT=1
export RM_CLIENT_TIMEOUT_MS=5000
cargo run --release -- client
```

**Load** (keep `RM_CLIENT_ID=pc6`):

```bash
export RM_CLIENT_USERS=15
export RM_CLIENT_REQUESTS_PER_USER=20
export RM_CLIENT_MAX_IN_FLIGHT=32
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

Total offered load when all three run the load block ≈ `3 × 15 × 20` submissions.

---

## 4. Automated stress ramp + Phase 1 report tables

### 4a. Quick ramp (smoke / informal)

On **one coordinator client PC** (e.g. PC4):

```bash
source stress/lab/nodes.env
mkdir -p stress/results/lab_ramp
bash stress/lab/run_client_ramp.sh stress/results/lab_ramp
python3 stress/lab/parse_client_metrics.py stress/results/lab_ramp --out stress/results/lab_ramp/report.csv
```

### 4b. Phase 1 project stress suite (required evidence — not tables-only)

Full procedures: **`stress/PHASE1_REPORT_GUIDE.md`**.

This suite exercises the **project description**: load ramp + stats, win share, 20s failure/failover, rejoin, election logs.

```bash
source stress/lab/nodes.env
# Offline harness check
python3 stress/report/run_phase1_suite.py --scenario e1 --dry-run --quick

# Live E1 ramp (per-request CSV → Table A + win share)
python3 stress/report/run_phase1_suite.py --scenario e1

# 20s failure under load (Ctrl+C node 2 when prompted)
python3 stress/report/run_phase1_suite.py --scenario e4 --clients 30 --duration-s 60

# Checklist vs project requirements
python3 stress/report/run_phase1_suite.py --scenario check
```

Also run E2 imbalance (`RM_SIM_DELAY_MS` on one node) and `parse_election_logs.py` on teed node logs — see the Phase 1 guide.

### What “working properly” looks like

| Check | Good sign |
|-------|-----------|
| Nodes start | `node N listening` on PC1–3 |
| Heartbeats | No permanent “serving alone” on all three after warmup |
| Smoke client | `succeeded≥1` |
| Ramp | throughput rises then flattens; p95 grows under heavy load |
| Load balance | `per_node` not always `{1: all}` — work spreads (unless you forced imbalance) |
| One node down | Stop PC2 node; clients still get successes from 1 and 3 (may need retries/timeouts) |

---

## 5. Failure / rejoin quick test (manual)

1. Start all three nodes + steady client (`USERS=10`, loop or long ramp).  
2. On PC2: `Ctrl+C` the node **or** use mute if exposed.  
3. Confirm clients still print successes (may see more timeouts briefly).  
4. Restart PC2 node with same `RM_NODES` / `RM_NODE_ID=2`.  
5. Confirm it syncs / rejoins and starts winning some work again.

Record wall-clock: fail start → first success without node 2; revive → node 2 appears in `per_node` again.

---

## 6. Env cheat sheet

| Variable | Used by | Meaning |
|----------|---------|---------|
| `RM_NODES` | node + client | `id@ip:port,…` peer list |
| `RM_NODE_ID` | **node only** | This process’s id (1/2/3) |
| `RM_CLIENT_ID` | client | Unique name (avoid colliding request ids) |
| `RM_CLIENT_USERS` | client | Simulated concurrent users |
| `RM_CLIENT_REQUESTS_PER_USER` | client | Requests each user sends |
| `RM_CLIENT_MAX_IN_FLIGHT` | client | Cap on parallel in-flight requests |
| `RM_CLIENT_TIMEOUT_MS` | client | Per-request timeout |
| `RM_CLIENT_RATE_MS` | client | Delay between a user’s requests (0 = ASAP) |

Election/planner knobs (optional): see `election::Config::from_env` / planner (`RM_*` in source).

---

## 7. Common failures

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `alive=[1]` / `warmup: no peer heard, serving alone` | Ran `cargo run --release --` **without** `RM_NODES` / `RM_NODE_ID` (defaults to localhost) | `Ctrl+C`, `source stress/lab/nodes.env`, set `RM_NODE_ID`, run `cargo run --release -- node` |
| Client all timeouts | Firewall / wrong IP / nodes not running | `hostname -I`, ufw, verify `RM_NODES` |
| Only one node “wins” forever | Others not reachable or wrong `RM_NODE_ID` | Fix IDs; check peer logs |
| `bind` errors | Port in use / binding wrong IP | Kill old process; code falls back to `0.0.0.0` |
| Duplicate node id | Two PCs with `RM_NODE_ID=1` | Set 1/2/3 correctly |
| Many `warning: … never used` at build | Unused FT/election stubs on this branch | Safe to ignore if build finishes |
| Build fails | Old Rust | `rustup update` |
| Windows clients | `source` / bash | Use Git Bash, WSL, or set env in PowerShell (see below) |
| `stress/lab/nodes.env: No such file` | Wrong branch / not pulled | `git fetch && git checkout stress-testing-lab` (after push) |

### PowerShell (Windows) equivalent

```powershell
$env:RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
$env:RM_NODE_ID="1"   # cloud only
cargo run --release -- node
```

```powershell
$env:RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
$env:RM_CLIENT_USERS="10"
$env:RM_CLIENT_REQUESTS_PER_USER="10"
cargo run --release -- client
```

---

## 8. Exact commands (copy-paste)

### Cloud

**PC1 (10.40.32.242):**
```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=1
cargo run --release -- node
```

**PC2 (10.40.2.29):**
```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=2
cargo run --release -- node
```

**PC3 (10.40.0.73):**
```bash
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_NODE_ID=3
cargo run --release -- node
```

### Clients (see §3 for full smoke → load per PC)

**PC4 (`10.40.1.240`) smoke:**
```bash
cd ~/Projects/RescueMesh
hostname -I
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
export RM_CLIENT_ID=pc4
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
export RM_CLIENT_MAX_IN_FLIGHT=1
export RM_CLIENT_TIMEOUT_MS=5000
cargo run --release -- client
```

**PC5:** same as PC4 with `export RM_CLIENT_ID=pc5`  
**PC6:** same as PC4 with `export RM_CLIENT_ID=pc6`

**Load (any client PC, after smoke):**
```bash
export RM_CLIENT_USERS=15
export RM_CLIENT_REQUESTS_PER_USER=20
export RM_CLIENT_MAX_IN_FLIGHT=32
export RM_CLIENT_TIMEOUT_MS=8000
cargo run --release -- client
```

---

## 9. Checklist before measuring “for real”

- [ ] All 6 IPs match `hostname -I`  
- [ ] UDP 7001/7002/7003 open on cloud PCs  
- [ ] PC1–3 running with correct `RM_NODE_ID`  
- [ ] Smoke client from PC4, PC5, PC6 each succeeds  
- [ ] Ramp saved under `stress/results/`  
- [ ] Note git commit: `git rev-parse --short HEAD`  
- [ ] Report p50/p95/p99 + throughput + per_node share  

---

## 10. Files in this folder

| File | Purpose |
|------|---------|
| `README.md` | Short entry point |
| `lab/nodes.env` | Shared `RM_NODES` |
| `lab/verify_ips.py` | Match `hostname -I` to lab map |
| `lab/check_connectivity.py` | Ping/UDP sanity to node ports |
| `lab/check_udp.sh` | Bash wrapper for connectivity check |
| `lab/start_node.sh` | `start_node.sh 1\|2\|3` |
| `lab/start_client.sh` | Smoke/load client helper |
| `lab/smoke_test.sh` | Exit 0 only if `succeeded≥1` |
| `lab/run_client_ramp.sh` | Increasing users → metrics files |
| `lab/parse_client_metrics.py` | Aggregate `CLIENT_METRICS` → CSV |
| `lab/Start-Node.ps1` | Windows node starter |
| `lab/Start-Client.ps1` | Windows client starter |
| `lab/Run-ClientRamp.ps1` | Windows ramp |
| `PHASE1_REPORT_GUIDE.md` | Project-required stress tests + tables |
| `report/run_phase1_suite.py` | E1–E5 / check orchestrator |
| `report/udp_load_client.py` | Per-request CSV load (failover/p95) |
| `report/analyze_request_csv.py` | Tables + failover_ms / convergence |
| `report/parse_election_logs.py` | Election duration from node logs |
| `report/check_requirements.py` | PASS/FAIL vs project checklist |
| `report/run_phase1_experiments.py` | Optional cargo-client batch ramp |
| `report/build_report_tables.py` | Aggregate cargo `runs.csv` |
| `report/configs/*.json` | Extra cargo-client configs |
| `report/templates/report_tables.md` | Blank Table A–E skeletons |
| `MULTI_PC_LAB_GUIDE.md` | This document |
