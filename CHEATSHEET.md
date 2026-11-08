# RescueMesh cheat sheet (commands only)

Substitute: `N` clients, `R` requests/client, `T` timeout sec, `S`/`W`/`X` sleep sec, `K` node id (0/1/2).

Run stress commands from `stress/` unless noted.

---

## Setup (every PC)

```bash
# extract / get code
git clone https://github.com/reffobic/RescueMesh.git
cd RescueMesh
git fetch origin
git checkout stress-testing   # or stress-testing-lab for full lab suite
```

```bash
# confirm this machine IP
hostname -I
```

```bash
# build (hosts and clients)
cd RescueMesh
cargo build --release
```

```bash
# stress Python deps (client / driver PCs)
cd RescueMesh/stress
pip install -r requirements.txt
```

```bash
# shared peer string (ALL PCs — update IPs if lab differs)
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
```

```bash
# firewall on cloud hosts only (matching that PC’s port)
sudo ufw allow 7001/udp   # node 1
sudo ufw allow 7002/udp   # node 2
sudo ufw allow 7003/udp   # node 3
```

---

## Cloud hosts (nodes PC1–PC3)

```bash
cd RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"

# PC1
export RM_NODE_ID=1
cargo run --release -- node

# PC2
export RM_NODE_ID=2
cargo run --release -- node

# PC3
export RM_NODE_ID=3
cargo run --release -- node
```

```bash
# optional helper (stress-testing-lab branch)
bash stress/lab/start_node.sh 1   # or 2 / 3
```

---

## Clients (PC4–PC6)

```bash
cd RescueMesh
export RM_NODES="1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"

# cargo client (lab binary)
export RM_CLIENT_ID=client-$(hostname)
export RM_CLIENT_USERS=1
export RM_CLIENT_REQUESTS_PER_USER=1
cargo run --release -- client
```

```bash
# optional helper (stress-testing-lab branch)
bash stress/lab/start_client.sh
```

```bash
# Python load client (this branch harness) — from stress/
cd RescueMesh/stress
# edit configs/hosts.json to lab node IPs/ports if not 127.0.0.1
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_burst.csv
```

```bash
# local-only harness hosts (default file; not the 6-PC lab map)
# stress/configs/hosts.json → 127.0.0.1:9000–9002
```

---

## Load

```bash
# dry-run
python load_client.py --dry-run --clients N --requests R --out results/demo_burst.csv

# live
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_burst.csv
```

```bash
# with timeout
python load_client.py --dry-run --clients N --requests R --timeout T --out results/demo_timeout.csv
python load_client.py --hosts configs/hosts.json --clients N --requests R --timeout T --out results/demo_timeout.csv
```

---

## Timeline (sleep between bursts)

```bash
# wait then send
sleep S
python load_client.py --dry-run --clients N --requests R --out results/demo_after_wait.csv

sleep S
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_after_wait.csv
```

```bash
# burst → wait → burst
python load_client.py --dry-run --clients N --requests R --out results/demo_a.csv
sleep S
python load_client.py --dry-run --clients N --requests R --out results/demo_b.csv

python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_a.csv
sleep S
python load_client.py --hosts configs/hosts.json --clients N --requests R --out results/demo_b.csv
```

```bash
# light → wait → heavy
python load_client.py --dry-run --clients 5 --requests 10 --out results/demo_phase1.csv
sleep S
python load_client.py --dry-run --clients 25 --requests 40 --out results/demo_phase2.csv

python load_client.py --hosts configs/hosts.json --clients 5 --requests 10 --out results/demo_phase1.csv
sleep S
python load_client.py --hosts configs/hosts.json --clients 25 --requests 40 --out results/demo_phase2.csv
```

---

## Fault inject

```bash
# mute (print-only / live)
python fault_inject.py --hosts configs/hosts.json --mute K --secs S --print-only
python fault_inject.py --hosts configs/hosts.json --mute K --secs S
```

```bash
# overload (print-only / live)
python fault_inject.py --hosts configs/hosts.json --sim-load K --extra-queue 15 --cpu 0.95 --print-only
python fault_inject.py --hosts configs/hosts.json --sim-load K --extra-queue 15 --cpu 0.95
```

```bash
# load then mute after X sec (two terminals, live)
# terminal A:
python load_client.py --hosts configs/hosts.json --clients N --requests R --timeout T --out results/demo_midfail.csv
# terminal B:
sleep X
python fault_inject.py --hosts configs/hosts.json --mute K --secs S
```

---

## Packaged experiments

```bash
python run_experiments.py --config configs/e1_ramp.yaml --dry-run
python run_experiments.py --config configs/e1_ramp.yaml

python run_experiments.py --config configs/e2_imbalance.yaml --dry-run
python run_experiments.py --config configs/e2_imbalance.yaml

python run_experiments.py --config configs/e4_failure.yaml --dry-run
python run_experiments.py --config configs/e4_failure.yaml

python run_experiments.py --config configs/e5_rejoin.yaml --dry-run
python run_experiments.py --config configs/e5_rejoin.yaml
```

```bash
python analyze_results.py --input results/e1_ramp --out results/e1_ramp/report
```

---

## Smoke

```bash
cd stress
python load_client.py --dry-run --clients 3 --requests 5 --out results/demo_smoke.csv
sleep 1
python load_client.py --dry-run --clients 3 --requests 5 --out results/demo_smoke2.csv
python fault_inject.py --hosts configs/hosts.json --mute 1 --secs 5 --print-only
```
