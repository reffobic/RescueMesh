# RescueMesh stress / multi-PC lab

Branch: **`stress-testing-lab`** (based on `incident_proc_coord`)

## Start here

Read **[MULTI_PC_LAB_GUIDE.md](MULTI_PC_LAB_GUIDE.md)** for the full 6-PC procedure (IPs, firewall, `RM_NODE_ID`, ramp, failure test).

## Quick path

```bash
# Every PC
git checkout stress-testing-lab
cargo build --release
source stress/lab/nodes.env
python3 stress/lab/verify_ips.py
python3 stress/lab/check_connectivity.py

# Cloud PC1/2/3
bash stress/lab/start_node.sh 1   # or 2 / 3

# Client PC4/5/6 — smoke then ramp
bash stress/lab/smoke_test.sh
bash stress/lab/run_client_ramp.sh stress/results/lab_ramp
python3 stress/lab/parse_client_metrics.py stress/results/lab_ramp --out stress/results/lab_ramp/report.csv
```

Windows PowerShell: `Start-Node.ps1`, `Start-Client.ps1`, `Run-ClientRamp.ps1` under `lab/`.
