# RescueMesh

Distributed rescue-coordination mesh (Rust). `main` unifies the full node/client runtime, Service 1 incident planner and snapshot engine, load-aware bully election, fault tolerance & quorum replication, periodic failure injection (`RM_FAIL_INJECT=1`), the demo GUI, and the Phase 1 stress harnesses and measured CSV results.

Live walkthroughs: see [`DEMO.md`](DEMO.md), [`CHEATSHEET.md`](CHEATSHEET.md), and [`stress/README.md`](stress/README.md).

## Layout

| Path | Contents |
|------|----------|
| `src/` | Node (`node.rs`), client (`client.rs`), load-aware election (`election.rs`), Service 1 planner & snapshot (`planner.rs`, `snapshot.rs`), fault tolerance (`fault_tolerance.rs`), types (`types.rs`) |
| `gui/` | Tkinter live cluster & failure-injection demo GUI (`gui/gui.py`) |
| `docs/` | Technical documentation, architecture evidence, parallelization notes |
| `stress/` | Multi-PC lab launchers (`lab/`), Phase 1 report & experiment suites (`report/`), and legacy harness/YAML configs |
| `stress/results/` | Detailed CSV (and JSON/PNG/Markdown) experiment outputs |

## Build & run

```bash
cargo build
# example (set peers + this node’s id as required by your lab)
RM_NODES=... RM_NODE_ID=1 cargo run -- node
RM_NODES=... cargo run -- client
```

## Stress harness

Automates load, imbalance, failure, and rejoin experiments. Scripts: `stress/load_client.py`, `fault_inject.py`, `run_experiments.py`, `analyze_results.py`.

Configs: `stress/configs/e1_ramp.yaml`, `e2_imbalance.yaml`, `e3_policies.yaml`, `e4_failure.yaml`, `e5_rejoin.yaml`. Hosts: `stress/configs/hosts.json`.

```bash
cd stress
pip install -r requirements.txt

# dry-run (no live cluster)
python run_experiments.py --config configs/e1_ramp.yaml --dry-run

# live cluster
python run_experiments.py --config configs/e1_ramp.yaml

# summarize a result directory
python analyze_results.py --input results/e1_ramp --out results/e1_ramp/report
```

### Results

| Directory | Experiment |
|-----------|------------|
| `stress/results/e1_ramp`, `e1_ramp_live`, `e1_udp` | Load ramp / UDP load |
| `stress/results/e2_imbalance` | Workload imbalance / policies |
| `stress/results/e4_fail20` | Failure / failover |
| `stress/results/e5_rejoin` | Rejoin / convergence |
| `stress/results/election_overhead*` | Election timing CSVs |
