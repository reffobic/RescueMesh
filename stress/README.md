# Stress harness (Phase 1)

Branch purpose: automate **increasing client load** experiments and produce **statistically meaningful** latency/throughput/failover plots for the RescueMesh report.

See `REQUIREMENTS_STRESS.md` for the full plan and `../docs/Full_System_Diagram.drawio` for the system picture.

## Quick start

```bash
pip install -r requirements.txt
python run_experiments.py --config configs/e1_ramp.yaml --dry-run
python analyze_results.py --input results/e1_ramp --out results/e1_ramp/report
```

Against a live 3-node cloud (after nodes listen on ports in `configs/hosts.json`):

```bash
python run_experiments.py --config configs/e1_ramp.yaml
```
