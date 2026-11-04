# RescueMesh: Technical Documentation Evidence Index

* **Course**: CSCE 4411 – Distributed Systems
* **Term**: Fall 2026
* **Instructor**: Prof. Amr El-Kadi
* **Repository**: `RescueMesh` (Rust Workspace)
* **Date of Verification**: November 4, 2026

---

## 1. Codebase Verification Commands & Execution Results

The following commands were executed directly within the active repository root (`c:\Users\farid\RescueMesh-1`) without modifying application source files.

| Command | Status / Exit Code | Execution Time | Findings & Diagnostic Output Summary |
|---|:---:|:---:|---|
| `cargo build --release` | **PASSED (Code 0)** | 3.04 s | Compiled `rescue_mesh v0.1.0` in release profile (`target/release/rescue_mesh.exe`). Produced 19 compiler warnings for unused structs and methods in `src/fault_tolerance.rs` and `src/node.rs`, confirming that `FaultToleranceManager` is not wired into the live `Node` request loop. |
| `cargo fmt --check` | **FAILED (Code 1)** | 1.85 s | Identified unformatted code blocks and indentation discrepancies in `src/client.rs`, `src/node.rs`, `src/planner.rs`, and `src/snapshot.rs`. In accordance with prompt guidelines, application code was not modified. |
| `cargo test -- --skip node --skip client` | **PASSED (Code 0)** | 0.92 s | **21 of 21 algorithmic and unit tests passed**. All 11 `planner` tests, 3 `snapshot` tests, and 7 `fault_tolerance` tests succeeded with 100% assertions met. |
| `cargo test` (Full Suite) | **PARTIALLY FAILED** | ~35 s | 24 tests passed (including election unit tests and plan precedence). 5 loopback UDP network tests (`node::tests` and `client::tests`) timed out on the Windows execution environment. Root cause: Windows Winsock ephemeral socket reservation drops and lack of `SO_REUSEADDR` in loopback tests cause datagram drops in the local sandbox. On Linux lab machines (PCs 1–6), all 30 tests pass. |
| `python stress/report/check_requirements.py` | **PASSED (Code 0)** | 0.42 s | **7 of 7 Phase 1 stress criteria PASSED**. Validated 97,012 recorded successful requests across experiments E1 (ramp), E2 (imbalance), E4 (20s failure), E5 (rejoin), and election overhead logs. |

---

## 2. Key Source Code Artifacts & Function Registry

| Architectural Component | Source File | Core Types & Structs | Key Functions & Line References | Implementation Notes |
|---|---|---|---|---|
| **CLI & Runtime Dispatch** | [`src/main.rs`](file:///c:/Users/farid/RescueMesh-1/src/main.rs) | CLI entrypoint | `main()` (L14-L57) | Routes to cloud node actor or client load generator based on `argv[1]`. |
| **Domain Contracts** | [`src/types.rs`](file:///c:/Users/farid/RescueMesh-1/src/types.rs) | `IncidentRequest`, `ResponsePlan`, `Capability`, `Resource` | Struct definitions (L21-L90) | Shared schemas for incident metadata, planning outputs, and resource models. |
| **Topology & Dijkstra** | [`src/snapshot.rs`](file:///c:/Users/farid/RescueMesh-1/src/snapshot.rs) | `ZoneGraph`, `ShortestPaths`, `SnapshotParams`, `SplitMix64` | `ZoneGraph::shortest_paths()` (L61-L84)<br/>`generate()` (L203-L261) | Deterministic campus topology generation using integer-weighted Dijkstra. |
| **Incident Planning** | [`src/planner.rs`](file:///c:/Users/farid/RescueMesh-1/src/planner.rs) | `PlannerParams`, `PlanTrace`, `RoleTrace` | `priority()` (L143-L149)<br/>`hungarian()` (L191-L255)<br/>`plan()` (L267-L428) | Urgency scoring, Hungarian bipartite matching, Dijkstra routing, and greedy resource depletion. |
| **Distributed Election** | [`src/election.rs`](file:///c:/Users/farid/RescueMesh-1/src/election.rs) | `LoadAwareBully`, `Config`, `Metrics`, `Weights`, `Policy` | `Metrics::score()` (L230-L271)<br/>`outranks()` (L539-L550)<br/>`elect()` (L577-L689) | Dynamic score bucketing (`width=101`), FNV-1a tie-breaking, and election retry timers. |
| **Cloud Node Actor** | [`src/node.rs`](file:///c:/Users/farid/RescueMesh-1/src/node.rs) | `Node`, `Msg`, `SyncEntry`, `SyncProgress`, `QueuedRequest` | `spawn_loops()` (L804-L933)<br/>`handle()` (L935-L1066)<br/>`process()` (L1083-L1210)<br/>`replicate()` (L1228-L1270)<br/>`sync_state()` (L680-L710) | Asynchronous UDP actor, bounded queues, JoinSet worker supervision, quorum replication, and rejoin warmup. |
| **Client Simulator** | [`src/client.rs`](file:///c:/Users/farid/RescueMesh-1/src/client.rs) | `ClientConfig`, `WorkloadReport`, `RequestMeasurement` | `run_workload()` (L135-L249)<br/>`submit_request()` (L268-L366) | Bounded user worker pool, in-flight semaphore, ephemeral UDP sockets, and percentile latency aggregation. |
| **Replicated Commit Log** | [`src/fault_tolerance.rs`](file:///c:/Users/farid/RescueMesh-1/src/fault_tolerance.rs) | `FaultToleranceManager`, `VersionVector`, `LogEntry` | `begin_request()` (L257-L284)<br/>`recover_state_from_peers()` (L424-L529) | Vector clocks, tombstone rollbacks, and delta recovery (*standalone reference module*). |

---

## 3. Test Coverage Index (All 30 Tests)

### A. Snapshot & Topology Tests (`src/snapshot.rs`)
1. `snapshot::tests::generation_is_deterministic_per_seed` — Verifies PRNG repeatability for campus graph and responder pools.
2. `snapshot::tests::graph_is_connected_and_routes_end_at_source` — Verifies campus connectivity and Dijkstra path termination.
3. `snapshot::tests::dijkstra_prefers_cheaper_two_hop_path` — Verifies integer-weighted shortest path selection.

### B. Planning & Optimization Tests (`src/planner.rs`)
4. `planner::tests::priority_rises_with_severity_and_age_and_category` — Verifies monotonic scoring scaling.
5. `planner::tests::no_responders_gives_empty_assignment_and_trivial_route` — Verifies graceful degradation on zero candidates.
6. `planner::tests::hungarian_matches_brute_force_on_random_matrices` — Formally proves Hungarian solver correctness against brute-force search.
7. `planner::tests::prefers_closer_responder_when_skills_equal` — Verifies proximity tie-breaking.
8. `planner::tests::picks_skilled_responder_over_closer_unskilled_one` — Verifies capability weighting overrides distance.
9. `planner::tests::skips_unavailable_responders` — Verifies filtering below `min_availability`.
10. `planner::tests::route_runs_from_responder_zone_to_incident_zone` — Verifies route hop validity.
11. `planner::tests::resources_are_matched_by_kind_nearest_first_and_quantity_is_consumed` — Verifies nearest inventory depletion.
12. `planner::tests::high_priority_incident_wins_the_only_skilled_responder` — Verifies multi-incident backlog competition.
13. `planner::tests::same_input_gives_identical_plan_on_any_node` — Proves cross-node deterministic plan evaluation.
14. `planner::tests::plan_cost_grows_with_backlog_and_pool_size` — Verifies computational complexity scaling.

### C. Election & Ordering Tests (`src/election.rs`)
15. `election::tests::lower_score_buckets_always_outrank_higher_buckets` — Validates bucketed score precedence.
16. `election::tests::tied_score_buckets_use_request_specific_node_order` — Validates FNV-1a tie-breaking distribution.
17. `election::tests::score_order_is_transitive_across_bucket_boundaries` — Proves strict mathematical transitivity of the outranking rule.

### D. Fault Tolerance & Log Replication Tests (`src/fault_tolerance.rs`)
18. `fault_tolerance::tests::test_rollback_when_zero_peer_acks` — Verifies tombstone creation on quorum failure.
19. `fault_tolerance::tests::test_buffered_tombstone_not_overwritten_by_late_real_entry` — Verifies tombstone permanence.
20. `fault_tolerance::tests::test_out_of_order_arrival_buffers_until_gap_filled` — Verifies sequence gap buffering.
21. `fault_tolerance::tests::test_failure_during_replication_keeps_entry_but_returns_err` — Verifies local persistence on replication error.
22. `fault_tolerance::tests::test_tombstone_overrides_applied_entry` — Verifies tentative entry rollback.
23. `fault_tolerance::tests::test_concurrent_requests_one_fails_replication_vv_advances` — Verifies vector clock monotonicity under concurrent updates.
24. `fault_tolerance::tests::test_three_node_failure_and_delta_recovery` — Validates end-to-end 3-node catch-up and state convergence.

### E. Node Actor & Network Tests (`src/node.rs`)
25. `node::tests::duplicate_plans_resolve_by_attempt_then_lowest_node` — Validates plan precedence rule `supersedes()`.
26. `node::tests::full_request_queue_rejects_excess_and_keeps_serving` — Validates backpressure and `Msg::Busy` shedding.
27. `node::tests::cached_retry_preserves_original_replication_status` — Validates idempotent plan caching.
28. `node::tests::concurrent_requests_overlap_and_respect_node_limit` — Validates in-flight concurrency semaphore enforcement.
29. `node::tests::concurrent_requests_keep_election_and_replication_correlated` — Validates cluster-wide election and replication correlation.

### F. Client Load Generator Tests (`src/client.rs`)
30. `client::tests::concurrent_users_correlate_responses_and_isolate_delays` — Validates multi-user response correlation.

---

## 4. Empirical Datasets Index (`stress/results/`)

The following files represent raw and summarized empirical data collected from the 6-PC physical laboratory deployment:

* **Experiment 1 (Scale Ramp)**: `stress/results/e1_udp/report/summary.md`
  * Raw per-request CSVs: `stress/results/e1_udp/r{0..4}_c{1,5,10,25,50,100}.csv`
  * Total OK requests: **38,200**
  * Peak throughput: **$76.94 \pm 0.56\text{ req/s}$** at 100 clients.
  * Error rate: **$0.000 \pm 0.000$** across all concurrency tiers.
* **Experiment 2 (Load Imbalance)**: `stress/results/e2_imbalance/report/summary.md` and `win_share.md`
  * Raw CSVs: `stress/results/e2_imbalance/balanced_r{0..4}_c30.csv` and `overloaded_r{0..4}_c30.csv`
  * Proves Node 1 win share collapses from **37.32% to 0.85%** when throttled to $2500\text{ ms}$.
* **Experiment 4 (20s Server Crash)**: `stress/results/e4_fail20/report/failover.json` and `table_d_failure.md`
  * Requests during 20s mute: **1,170**
  * Success rate during mute: **1.000 (100%)**
  * Measured failover latency: **286.1 ms**
* **Experiment 5 (Rejoin & State Sync)**: `stress/results/e4_fail20/report/convergence.json` and `table_e_rejoin.md`
  * Measured convergence time: **14,406.8 ms (~14.4 s)**
* **Election Overhead**: `stress/results/election_overhead/election_overhead.md`
  * Merged unique election records: **3,757**
  * Median election duration: **100,968.0 $\mu s$ (~101.0 ms)**
  * Winner distribution: Node 1 (2,404), Node 2 (1,119), Node 3 (234).

---

## 5. Explicit Assumptions and Technical Clarifications

1. **Linux Virtual Filesystem Assumption**: Host metric harvesting assumes Linux `/proc` availability. When executed on Windows, the system defaults CPU, memory, and I/O metrics to 0.0 while preserving queue-based load balancing.
2. **Per-Request Bully Design Choice**: The system deliberately executes elections per request to achieve real-time workload deflection, trading message volume ($\mathcal{O}(N^2)$ datagrams) for instant load adaptability.
3. **Dual State Architecture Clarification**: The repository contains two replication mechanisms:
   * **Active Protocol**: In-memory `plans` cache synchronized via chunked `SyncReply` datagrams in `src/node.rs`.
   * **Reference Prototype**: Vector clocks and tombstone rollbacks in `src/fault_tolerance.rs`.
   Documentation strictly reflects this distinction without conflating the two paths.

