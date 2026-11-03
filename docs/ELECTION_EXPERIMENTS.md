# Running RescueMesh experiments

`scripts/sweep_experiments.py` is a configurable controller for repeatable election, score, and queue sweeps. It runs on the client/control machine. It does not assume how the six Linux nodes are launched: configure command hooks in a JSON file to restart the nodes, clear/collect their logs, and optionally set up/clean up load or packet-loss conditions.

## Prepare the cluster

1. Make sure all six nodes are ready and the configured `RM_NODES` value lists all six addresses.
2. Measure the idle RTT between every pair of nodes and retain the samples and percentiles with the experiment results.
3. Check that the real planner is in use. The current `Node::execute` implementation is still a 300 ms placeholder, so score-function and queue conclusions are not representative until that is replaced.
4. Ensure the node logs are accessible to the control machine. Election settings are read at node startup, so the restart hook must apply the supplied environment values to every node before restarting it.

## Configure and run

Use Python 3 on the control machine:

```sh
python3 scripts/sweep_experiments.py --init-config --config sweep_config.json
```

Edit `sweep_config.json`. In particular:

- Set `nodes` and `working_directory`.
- Optionally set `commands.measure_rtt` to a command that measures and prints pairwise RTT samples; its output is saved as `rtt-baseline.log`.
- Set `commands.restart_nodes` to stop/start all six nodes while applying `RM_*` environment variables from the runner. This hook receives the sweep configuration in its environment; remote SSH commands must explicitly forward the relevant variables.
- Set `commands.clear_logs` to clear or rotate logs after the warm-up run.
- Set `commands.collect_logs` to print the measured node logs to stdout. The runner saves that output as `nodes.log`.
- Configure `conditions.heavy_load` and `conditions.message_loss` hooks before selecting those conditions. Hooks are commands run on the control machine; use an authorized test setup and always provide cleanup hooks.
- Set the workload and sweep values to match your target traffic.

First inspect the proposed matrix and its size without starting any process:

```sh
python3 scripts/sweep_experiments.py --config sweep_config.json --suite election --conditions idle --dry-run
```

Then run the initial `t_ok` and `t_coord` tests:

```sh
python3 scripts/sweep_experiments.py --config sweep_config.json --suite election --conditions idle --repeats 30
```

After validating the hooks, add `heavy_load` and `message_loss` to `--conditions`. Score and queue tests can be run separately with `--suite score` and `--suite queue`; `--suite all` runs the full matrix. The score suite includes policy comparisons, individual term ablations, machine/communication ablations, and machine/communication weight splits. The queue suite varies queue cap, CPU workers, node in-flight limit, and request queue capacity. All settings are recorded with each run.

Each result directory contains the effective configuration, plan, raw client and node logs, `runs.csv`, `summary.csv`, `report.json`, and p95-latency SVG plots. The 95% intervals in the summary are bootstrap intervals across repeated run-level measurements. Use the raw logs and per-run CSV as the source data.

## Current measurement limits

The runner records client latency, throughput, failures/timeouts, node work shares, Jain fairness, election durations present in `ELECTION` log lines, and duplicate-coordinator events present in `DUPCOORD` lines. Current application logging does **not** provide per-election restart counts, time from first `OK` to `COORDINATOR`, messages per election, each node's election score, or enough information to verify whether the winner had the lowest score. Those fields require application telemetry before the corresponding checklist metrics can be reported.

The score-bucket width is currently a compile-time constant, so the runner cannot sweep it. Also, real load, network loss, and “kill the best node immediately after `OK`” behavior are not inferred or injected automatically: condition hooks must implement the first two, and the precise failure timing needs protocol instrumentation before it can be automated reliably.
