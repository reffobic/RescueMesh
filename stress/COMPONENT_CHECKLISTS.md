# Per-component “pass the stress tests” checklists

Use with `REQUIREMENTS_STRESS.md`. Check boxes before claiming Phase 1 stress done.

---

## Platform / RPC

- [ ] 3 nodes start from one config; ports match `configs/hosts.json`
- [ ] Client can multicast `Request` to all peers
- [ ] Reply reaches client `reply_to` port
- [ ] Timeout/retry uses **same** `request_id`
- [ ] Heavy plan work does not block heartbeats (spawn_blocking / separate pool)
- [ ] Logs include timestamps suitable for joining with client CSV

**Stress risk if missing:** harness timeouts → fake high error_rate; can’t trust throughput.

---

## Election (load-aware Bully)

- [ ] Emits `ELECTION,...` CSV lines
- [ ] `sim_load` or equivalent for E2 demo
- [ ] `RM_POLICY` supports weighted/queue/cpu/random
- [ ] Muted/Failed nodes never win
- [ ] Election duration ≪ plan time at n=3 under moderate load

**Stress risk if missing:** no win-share plots; cannot justify metrics; imbalance demo fails.

---

## Planner / concurrency

- [ ] Tunable CPU/time cost for plans
- [ ] Idempotent: second execute with same id is Cached
- [ ] Concurrent requests don’t deadlock
- [ ] Parallelization model written for report

**Stress risk if missing:** no saturation curve; load balancing has nothing to measure.

---

## Replication / recovery

- [ ] Majority ack before success (or documented interim + under_replicated flag)
- [ ] Catch-up before Active after mute
- [ ] `CATCHUP` / `RECOVERED` logs with ms
- [ ] Consistency spot-check script or manual table for E5

**Stress risk if missing:** E4/E5 fail course FT measurement bullets.

---

## Fault injector / eval

- [ ] Mute 1–20s automated or documented one-command
- [ ] E1–E5 configs run end-to-end
- [ ] `analyze_results.py` figures in report
- [ ] ≥5 repeats; p95/p99 shown
- [ ] Each figure has a design-decision sentence

---

## Demo scenario mapping

| Demo (project PDF) | Experiment | Owner proof |
|--------------------|------------|-------------|
| Normal cloud operation | E1 + win_share | Election CSV + fig_winshare |
| Load imbalance | E2 | Before/after win shares |
| Server failure during work | E4 | Timeline + failover_ms |
| (implied) rejoin | E5 | convergence_ms |
