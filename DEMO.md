# RescueMesh demo (professor prompts)

Use this as a live cheat-sheet. The professor may change numbers on the spot — substitute **N** (clients), **R** (requests per client), **T** (timeout seconds), **S** / **W** / **X** (sleep / warm / wait seconds), **K** (node id 0/1/2).

Every recipe has **dry-run** (no mesh) and **live** (UDP to peers in `stress/configs/hosts.json`).

Commands assume you are in the `stress/` directory unless noted.

This demo uses **only existing tools**: `load_client.py`, `fault_inject.py`, `run_experiments.py`, `analyze_results.py`, shell `sleep`, and the YAML configs. There is no per-request delay flag — timelines are built with `sleep` between bursts and with YAML fields like `warmup_s` / `start_after_s` / `mute_secs`.

---

## 0. One-time setup

```bash
cd RescueMesh
cargo build
cd stress
pip install -r requirements.txt
```

Edit `configs/hosts.json` so peer IPs/ports match your lab (defaults: `127.0.0.1:9000–9002`, admin `9100–9102`).

**Live only:** start three node processes listening on those ports first (`RM_NODES=... RM_NODE_ID=1 cargo run --release -- node`, etc.). Dry-run needs no nodes.

**Console summary** (`load_client`): JSON with `clients`, `attempts`, `ok`, `error_rate`, `throughput_rps`, `wall_s`, `out_csv`.

**CSV columns:** `ts_send,ts_recv,client_id,request_id,ok,latency_ms,processed_by,under_replicated,error`

---

## Knobs that exist today

| Goal | How |
|------|-----|
| Concurrent clients | `load_client.py --clients N` |
| Requests per client (total ≈ N×R) | `--requests R` |
| Per-request reply wait | `--timeout T` |
| No mesh | `--dry-run` |
| Pause before / between bursts | shell `sleep S` |
| Mute node K for S seconds | `fault_inject.py --mute K --secs S` |
| Overload node K | `fault_inject.py --sim-load K --extra-queue Q --cpu 0.95` |
| Preview inject (no send) | `fault_inject.py … --print-only` |
| Packaged ramp / fail timelines | YAML: `warmup_s`, `pause_between_repeats_s`, `failure.start_after_s`, `failure.mute_secs` via `run_experiments.py` |

---

## Prompt recipes

### 1. “Send N clients × R requests”

**Dry-run**

```bash
python load_client.py --dry-run --clients N --requests R --out results/demo_burst.csv
```

**Live**

```bash
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_burst.csv
```

**Expect:** `attempts` = N×R; JSON on stdout; CSV written. Live: mostly `ok=1` and `processed_by` set if healthy; dry-run: synthetic latency, ~2% `dry_timeout` by design.

Example: N=5, R=10 → 50 attempts.

---

### 2. “Wait S seconds, then send” (simple timeline)

**Dry-run**

```bash
sleep S
python load_client.py --dry-run --clients N --requests R --out results/demo_after_wait.csv
```

**Live**

```bash
sleep S
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_after_wait.csv
```

**Expect:** Same as recipe 1 after the pause (useful for “let the cluster settle”).

---

### 3. “Burst, wait S seconds, burst again” (multi-phase timeline)

**Dry-run**

```bash
python load_client.py --dry-run --clients N --requests R --out results/demo_a.csv
sleep S
python load_client.py --dry-run --clients N --requests R --out results/demo_b.csv
```

**Live**

```bash
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_a.csv
sleep S
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_b.csv
```

**Expect:** Two summaries / two CSVs. On live, compare `throughput_rps` and `processed_by` between phases.

To stretch a longer timeline, chain more bursts:

```bash
python load_client.py --dry-run --clients N --requests R --out results/demo_t0.csv
sleep S
python load_client.py --dry-run --clients N --requests R --out results/demo_t1.csv
sleep S
python load_client.py --dry-run --clients N --requests R --out results/demo_t2.csv
```

(Same pattern for live: drop `--dry-run`, add `--hosts configs/hosts.json`.)

---

### 4. “Use timeout T seconds”

**Dry-run**

```bash
python load_client.py --dry-run --clients N --requests R --timeout T --out results/demo_timeout.csv
```

**Live**

```bash
python load_client.py --hosts configs/hosts.json --clients N --requests R --timeout T --out results/demo_timeout.csv
```

**Expect:** Live — smaller T → more `error=timeout` if nodes are slow/down. Dry-run — synthetic ~2% failures; timeout mainly scales failed latency.

---

### 5. “Warm up W seconds, then load”

**Dry-run**

```bash
sleep W
python load_client.py --dry-run --clients N --requests R --out results/demo_warm.csv
```

**Live**

```bash
sleep W
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_warm.csv
```

**Expect:** Same as recipe 1 after warm-up. Packaged configs also use `warmup_s` inside `run_experiments.py` (see recipe 10).

---

### 6. “Mute / silence node K for S seconds”

**Dry-run (safe print only)**

```bash
python fault_inject.py --hosts configs/hosts.json --mute K --secs S --print-only
```

**Live**

```bash
python fault_inject.py --hosts configs/hosts.json --mute K --secs S
```

Then send load while muted (**live**):

```bash
python load_client.py --hosts configs/hosts.json --clients N --requests R --timeout T --out results/demo_mute.csv
```

**Dry-run load after print-only** (narrate only — mute does not hit a mesh):

```bash
python load_client.py --dry-run --clients N --requests R --out results/demo_mute_narrate.csv
```

**Expect:** Print-only shows `AdminMute` JSON and target host/admin port. Live mute + load: timeouts or winners ≠ K during the window; recovery after S seconds.

---

### 7. “Overload node K”

**Dry-run (print-only)**

```bash
python fault_inject.py --hosts configs/hosts.json --sim-load K --extra-queue 15 --cpu 0.95 --print-only
```

**Live**

```bash
python fault_inject.py --hosts configs/hosts.json --sim-load K --extra-queue 15 --cpu 0.95
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_imbalance.csv
```

**Expect:** If placement respects load, `processed_by` share for K drops vs a baseline run without sim-load.

---

### 8. “Start load, then after X seconds mute node K”

Two terminals.

**Live — Terminal A (load):**

```bash
python load_client.py --hosts configs/hosts.json --clients N --requests R --timeout T --out results/demo_midfail.csv
```

**Live — Terminal B (wait, then mute):**

```bash
sleep X
python fault_inject.py --hosts configs/hosts.json --mute K --secs S
```

**Dry-run stand-in**

```bash
# Terminal A
python load_client.py --dry-run --clients N --requests R --out results/demo_midfail.csv

# Terminal B — show what would be sent
sleep X
python fault_inject.py --hosts configs/hosts.json --mute K --secs S --print-only
```

**Expect (live):** early successes; during mute, errors or shift away from K.

Packaged variant (mute scheduled by YAML `start_after_s` / `mute_secs`):

**Dry-run**

```bash
python run_experiments.py --config configs/e4_failure.yaml --dry-run
```

**Live**

```bash
python run_experiments.py --config configs/e4_failure.yaml
```

---

### 9. “Different request volume each phase”

Example timeline: light → wait → heavy.

**Dry-run**

```bash
python load_client.py --dry-run --clients 5 --requests 10 --out results/demo_phase1.csv
sleep S
python load_client.py --dry-run --clients 25 --requests 40 --out results/demo_phase2.csv
```

**Live**

```bash
python load_client.py --hosts configs/hosts.json --clients 5 --requests 10 --out results/demo_phase1.csv
sleep S
python load_client.py --hosts configs/hosts.json --clients 25 --requests 40 --out results/demo_phase2.csv
```

**Expect:** Second summary shows higher `attempts` and usually higher `throughput_rps` under load.

---

### 10. “Ramp clients like E1”

**Dry-run**

```bash
python run_experiments.py --config configs/e1_ramp.yaml --dry-run
```

**Live**

```bash
python run_experiments.py --config configs/e1_ramp.yaml
```

**Expect:** Files under `results/e1_ramp/` (`meta.json`, `r{repeat}_c{clients}.csv`). This **overwrites** that folder — for a throwaway demo, temporarily set `output_dir: results/demo_e1` in the YAML (or copy the folder first).

Config already encodes timeline pieces: `warmup_s`, `client_counts`, `repeats`, `pause_between_repeats_s`.

Analyze:

```bash
python analyze_results.py --input results/e1_ramp --out results/e1_ramp/report
```

**Expect:** `report/summary.csv` plus latency/throughput/error figures.

---

### 11. “Show measured failover / election examples”

No new traffic required:

| Artifact | Talking point |
|----------|----------------|
| `results/e1_ramp/report/summary.csv` | Throughput rises with clients (~20 → ~1680 rps in sample); latency ~75 ms; error_rate ~0.02 |
| `results/e4_fail20/report/failover.json` | `failover_ms` in hundreds of ms; high success during failure window if mesh recovered |
| `results/election_overhead/elections.csv` | Bully rows with `elapsed_us` ~1e5 (~100 ms class) in sample |

---

## Ballpark sanity (checked-in E1)

| clients | throughput_rps (mean) | lat_mean (ms) | error_rate |
|--------:|----------------------:|--------------:|-----------:|
| 1 | ~20 | ~77 | ~0.02 |
| 10 | ~173 | ~76 | ~0.02 |
| 100 | ~1684 | ~75 | ~0.02 |

Live numbers depend on hardware and node binary.

---

## Troubleshooting

| Symptom | Check |
|---------|--------|
| Wrong config / missing file | `cd` into `stress/` first |
| Live all timeouts | Nodes up? `hosts.json` IPs/ports? Firewall? |
| Mute/sim-load no effect | Admin port listening? Try `--print-only` |
| Overwrote golden CSVs | Use `output_dir: results/demo_*` or restore from git |
| Want inter-request delay | Not supported by current `load_client` — use smaller bursts + `sleep` between them |
| `cargo run` useless alone | Expected stub on this branch — dry-run or course node binary for live |

---

## Suggested smoke before the professor arrives

```bash
cd stress
python load_client.py --dry-run --clients 3 --requests 5 --out results/demo_smoke.csv
sleep 1
python load_client.py --dry-run --clients 3 --requests 5 --out results/demo_smoke2.csv
python fault_inject.py --hosts configs/hosts.json --mute 1 --secs 5 --print-only
```

**Expect:** two JSON summaries with `attempts: 15` each; mute MANUAL line for `127.0.0.1:9101`.
