# Fault Tolerance, Quorum Replication, State Sync, and Idempotency: What Was Done and the Code

Nov 8, 2026 · @Mohamed El Refai

## 1. Overview

My part of RescueMesh covers the cloud node runtime (`src/node.rs`), majority quorum replication, deterministic conflict resolution (`supersedes`), three-tier operation idempotency, failure detection, failover and rejoin state synchronization (`SYNC`), client-to-node network resilience (`src/client.rs`), and the standalone version-vector and replicated log engine (`src/fault_tolerance.rs`).

Following the same structure as the election and Service 1 guides, each section below first explains what was done in plain words and then shows the commented Rust code from the repository that implements it.

When a client submits an emergency incident to the three-node cloud cluster, my code handles the reliability and consistency lifecycle end to end:

1. **UDP Ingress & Source-Address Normalization:** Receives UDP datagrams asynchronously, binds with a `0.0.0.0` fallback for multi-PC lab machines, and rewrites `Msg::Request.reply_to` from the actual UDP packet source so replies always route back across physical subnets.
2. **Three-Tier Idempotency & Duplicate Suppression:** Coalesces duplicate in-flight requests from UDP multicast fan-out (`in_progress`), serves already-completed retries directly from the local plan cache (`plans`) while preserving their original `under_replicated` status, and guards execution with a per-node atomic set (`started`) so a node never executes the same `request_id` twice.
3. **Heartbeat Liveness & Failure Detection:** Broadcasts `Msg::Heartbeat` every `RM_HEARTBEAT_MS` (`300 ms`) and refreshes peer liveness (`last_seen`) on every peer message. Any peer silent for `RM_FAIL_AFTER_MS` (`1,000 ms`) is excluded from `alive_nodes()`.
4. **Loser Monitoring & Mid-Flight Failover:** Nodes that lose an election do not drop the request; they wait up to `RM_EXEC_TIMEOUT_MS` (`3,000 ms`) for the winner's replicated plan while polling `alive_nodes()` every `20 ms`. If the winner dies or stalls before replicating, survivors immediately trigger the next election attempt (`attempt + 1`, up to `RM_MAX_ATTEMPTS = 4`).
5. **Majority Quorum Replication (`Replicate` / `ReplicateAck`):** After executing a request, the winning coordinator sends `Msg::Replicate` to live peers and retries unacknowledged peers every `RM_REPL_RETRY_MS` (`150 ms`) up to `RM_REPL_TIMEOUT_MS` (`1,500 ms`) until a strict majority of the configured cluster (`floor(N / 2) + 1`, i.e. 2 of 3 nodes including the winner) holds the plan. If quorum is not reached in time, the plan is still returned to the client with `under_replicated = true`.
6. **Deterministic Plan Precedence (`supersedes`):** If overlapping attempts or split coordinator announcements produce two plans for the same `request_id`, every node resolves the conflict using `(attempt, node_id)` precedence: a higher election `attempt` always wins, and same-attempt ties break by the lower `node_id`.
7. **Rejoin Warmup Guard & Chunked State Sync (`SYNC`):** A newly started or unmuted node sets `ready = false` (`begin_warmup`), waits up to `RM_WARMUP_MS` (`600 ms`) to hear a live peer heartbeat, and pulls all stored plans from a live peer via `Msg::SyncRequest` and chunked `Msg::SyncReply` packets (`SYNC_CHUNK = 8` plans per datagram) before becoming eligible to win elections again.
8. **Formal Version-Vector & Replicated Log Engine (`src/fault_tolerance.rs`):** Implements `FaultToleranceManager` with per-origin monotonic sequence numbers, contiguous `VersionVector` tracking, out-of-order gap buffering (`pending_out_of_order`), sequence-preserving tombstones (`plan: None`) when replication fails, and bidirectional delta synchronization (`recover_state_from_peers`).

| File | What it holds | Written by |
| --- | --- | --- |
| `src/node.rs` | Operational UDP cloud node: message protocol (`Msg`), heartbeat detector, 3-tier idempotency, worker orchestration, majority quorum replication (`replicate`), `store_plan` conflict resolution, `mute_for` fault injection, and chunked rejoin state sync (`begin_warmup`, `sync_state`, `serve_sync`) | Mohamed El Refai (with concurrency/worker limits integrated with Farida, election trait hooks with Ismaiel, and Service 1 / periodic injector branch hooks with Haya) |
| `src/fault_tolerance.rs` | Standalone `FaultToleranceManager`: `VersionVector`, `LogEntry`, tombstone rollback, out-of-order gap buffer, TTL idempotency state machine, and multi-round delta recovery | Mohamed El Refai |
| `src/election.rs` (`supersedes`, `wait_for`, cold-start `svc_ms`) | Deterministic `(attempt, node_id)` plan precedence function, async `Notify` timeout helper, and cold-start service-time initialization (`30%` of `svc_cap_ms`) | Mohamed El Refai & Ismaiel Sabet |
| `src/client.rs` (`submit_request` resilience) | Multi-interface `0.0.0.0:0` binding, per-peer kernel route discovery for `reply_to`, bounded 1s UDP resends (`MAX_RESENDS = 4`), and `ConnectionRefused` filtering when a cloud node is down | Mohamed El Refai & Farida Elgharabawy |

---

## 2. Protocol Messages and Node State (`src/node.rs`)

**What was done:** All client-to-node and peer-to-peer communication uses UDP datagrams serialized with `serde_json`. Because UDP can drop, duplicate, or reorder packets—and has a finite datagram size—the `Msg` enum in `src/node.rs` defines explicit messages for heartbeats, client requests/replies/acknowledgements, replication, and chunked state synchronization. `Msg::sender()` extracts the peer `NodeId` from any peer message so the node can passively refresh liveness timestamps without waiting solely for `Heartbeat` packets.

```rust
pub type NodeId = u32;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Msg {
    // --- membership ---
    Heartbeat { from: NodeId },

    // --- client <-> cloud ---
    Request { req: IncidentRequest, reply_to: SocketAddr },
    Reply { plan: ResponsePlan, under_replicated: bool },
    Busy { request_id: String, node_id: NodeId },
    Ack { request_id: String }, // client confirms receipt (feeds RTT/reliability metrics)

    // --- replication ---
    Replicate { plan: ResponsePlan, attempt: u32, from: NodeId },
    ReplicateAck { request_id: String, from: NodeId },

    // --- state sync (rejoin) ---
    SyncRequest { from: NodeId },
    SyncReply {
        from: NodeId,
        part: u32,              // 0-indexed chunk number
        parts: u32,             // total chunks in this snapshot
        entries: Vec<SyncEntry>,
    },

    // --- Bully election ---
    Election { req_id: String, attempt: u32, from: NodeId, score: u64, client: String },
    Ok { req_id: String, attempt: u32, from: NodeId },
    Coordinator { req_id: String, attempt: u32, winner: NodeId },
    Bid { req_id: String, attempt: u32, from: NodeId, score: u64 },
}

impl Msg {
    /// Extracts the sending peer's NodeId so every valid peer message refreshes liveness.
    fn sender(&self) -> Option<NodeId> {
        match self {
            Msg::Heartbeat { from }
            | Msg::Election { from, .. }
            | Msg::Ok { from, .. }
            | Msg::Bid { from, .. }
            | Msg::Replicate { from, .. }
            | Msg::ReplicateAck { from, .. }
            | Msg::SyncRequest { from }
            | Msg::SyncReply { from, .. } => Some(*from),
            Msg::Coordinator { winner, .. } => Some(*winner),
            _ => None, // Client messages (Request, Ack) do not come from a cloud peer
        }
    }
}

/// One replicated plan as transferred during a rejoin sync: the plan, whether it was
/// under-replicated, and the (attempt, origin) precedence used to resolve duplicates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SyncEntry {
    pub plan: ResponsePlan,
    pub under_replicated: bool,
    pub attempt: u32,
    pub origin: NodeId,
}

/// Plans per SyncReply datagram, kept small enough for the 16 KiB UDP receive buffer.
const SYNC_CHUNK: usize = 8;
const SYNC_ATTEMPTS: usize = 3;
```

The `Node` struct holds the UDP socket, peer map, liveness table, idempotency and plan caches, replication acknowledgement sets, synchronization progress tracker, and concurrency semaphores:

```rust
pub struct Node {
    pub id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>, // all configured cloud nodes, including self
    pub cfg: Arc<Config>,
    pub metrics: Arc<Metrics>,
    sock: UdpSocket,
    strategy: Arc<dyn ElectionStrategy>,
    muted: AtomicBool,                    // true while simulating a node crash/outage
    ready: AtomicBool,                    // false during startup/rejoin warmup until synced
    last_seen: Mutex<HashMap<NodeId, Instant>>, // peer liveness table
    started: Mutex<HashSet<String>>,      // Tier 3 idempotency: request_ids executed on this node
    in_progress: Mutex<HashMap<String, Vec<(SocketAddr, String)>>>, // Tier 2: active request waiters
    plans: Mutex<HashMap<String, (ResponsePlan, bool)>>, // Tier 1: cached (plan, under_replicated)
    plan_prec: Mutex<HashMap<String, (u32, NodeId)>>, // (attempt, origin_node) of stored plan
    sync: Mutex<SyncProgress>,            // tracks chunked SyncReply parts during rejoin
    sync_notify: Notify,
    acks: Mutex<HashMap<String, HashSet<NodeId>>>, // distinct peer NodeIds that acked Replicate
    ack_notify: Notify,
    awaiting_ack: Mutex<HashMap<String, (String, Instant)>>, // replies awaiting client Msg::Ack
    request_tx: mpsc::Sender<QueuedRequest>,
    request_rx: Mutex<Option<mpsc::Receiver<QueuedRequest>>>,
    request_slots: Arc<Semaphore>,
    cpu_slots: Arc<Semaphore>,
    active_requests: AtomicU64,
    max_active_requests: AtomicU64,
    pub msgs_sent: AtomicU64,
    pub elections: AtomicU64,
    pub under_replicated: AtomicU64,
}
```

---

## 3. Asynchronous Heartbeat Failure Detection and Transport (`src/node.rs`)

**What was done:** When a node starts in `Node::new`, it first attempts to bind its configured `ip:port` from `RM_NODES`. On multi-PC lab machines where a node IP might be NATed or assigned to a specific interface, it falls back to binding `0.0.0.0:<port>` so incoming UDP datagrams are never rejected by the OS.

In `spawn_loops`, a background task broadcasts `Msg::Heartbeat { from: self.id }` every `cfg.heartbeat` (`300 ms`). Whenever *any* peer message arrives in `Node::handle`, `last_seen` records `Instant::now()` for that peer. `alive_nodes()` and `alive_peers()` check whether `t.elapsed() < self.cfg.fail_after` (`1,000 ms`). Meanwhile, if `self.is_muted()` is true (during simulated failure), `send_addr` and the UDP receive loop silently drop all outgoing and incoming packets.

```rust
// ---------- transport ----------
pub async fn send_to(&self, to: NodeId, m: &Msg) {
    if let Some((_, addr)) = self.peers.iter().find(|(i, _)| *i == to) {
        self.send_addr(*addr, m).await;
    }
}

async fn send_addr(&self, addr: SocketAddr, m: &Msg) {
    if self.is_muted() {
        return; // simulated crash: send nothing (including heartbeats)
    }
    let _ = self
        .sock
        .send_to(&serde_json::to_vec(m).unwrap(), addr)
        .await;
    self.msgs_sent.fetch_add(1, Relaxed);
}

pub async fn broadcast(&self, m: &Msg) {
    for (pid, _) in &self.peers {
        if *pid != self.id {
            self.send_to(*pid, m).await;
        }
    }
}

// ---------- failure detector ----------
pub fn alive_nodes(&self) -> Vec<NodeId> {
    let seen = self.last_seen.lock().unwrap();
    self.peers
        .iter()
        .map(|(p, _)| *p)
        .filter(|p| {
            // Self is always alive; peers are alive if heard within fail_after (default 1000 ms)
            *p == self.id
                || seen
                    .get(p)
                    .map_or(false, |t| t.elapsed() < self.cfg.fail_after)
        })
        .collect()
}

pub fn alive_peers(&self) -> Vec<NodeId> {
    self.alive_nodes()
        .into_iter()
        .filter(|p| *p != self.id)
        .collect()
}
```

In `spawn_loops()`, the UDP receiver normalizes `Msg::Request.reply_to` to the OS-reported packet `source` address so that even if a client could not determine its outward LAN IP, the cloud node replies to the exact IP and port the packet came from:

```rust
let dispatch_node = self.clone();
tokio::spawn(async move {
    while let Some((msg, source)) = incoming_rx.recv().await {
        // Always route client replies back to the real UDP source socket address
        let msg = match msg {
            Msg::Request { req, .. } => Msg::Request {
                req,
                reply_to: source,
            },
            other => other,
        };
        dispatch_node.handle(msg).await;
    }
});
```

---

## 4. Three-Tier Operation Idempotency (`src/node.rs`)

**What was done:** Clients multicast the same `IncidentRequest` to all three nodes and resend every `1,000 ms` if a reply has not arrived yet. To prevent duplicate planning or inconsistent replies, `Node::handle` and `Node::process` enforce idempotency at three levels:

1. **In-Flight Coalescing (`in_progress`):** If `req.request_id` is currently active on this node, the caller's `(reply_to, reporter_id)` is appended to the waiter list for that `request_id` without queuing a second task.
2. **Completed Plan Cache (`plans`):** If `req.request_id` has already completed execution and replication (or was received via peer `Replicate` / `SyncReply`), the node immediately sends `Msg::Reply { plan, under_replicated }` from `self.plans`, preserving the original `under_replicated` flag. A double-check after inserting into `in_progress` closes the race window where a request finishes right between the cache check and `in_progress` insertion.
3. **Execution Guard (`started`):** Inside `Node::process`, before calling `self.execute(...)`, the winning node inserts `id` into `self.started`. If `insert()` returns `false`, this node has already executed `id` in an earlier attempt and refuses to run it again.

```rust
Msg::Request { req, reply_to } => {
    let waiter = (reply_to, req.reporter_id.clone());

    // Tier 2 (fast path): if this request_id is already active, join its waiter list and return.
    let already_active = {
        let mut active = self.in_progress.lock().unwrap();
        if let Some(waiters) = active.get_mut(&req.request_id) {
            if !waiters.contains(&waiter) {
                waiters.push(waiter.clone());
            }
            true
        } else {
            false
        }
    };
    if already_active {
        return;
    }

    // Tier 1: if a plan is already stored (executed locally or replicated from a peer),
    // reply immediately with the cached plan and its original under_replicated status.
    let known = self.plans.lock().unwrap().get(&req.request_id).cloned();
    if let Some((plan, under_replicated)) = known {
        self.send_reply(reply_to, &plan, under_replicated, &req.reporter_id)
            .await;
        return;
    }

    // Register as a new active request (or join if another task raced us).
    let new_request = {
        let mut active = self.in_progress.lock().unwrap();
        match active.get_mut(&req.request_id) {
            Some(waiters) => {
                if !waiters.contains(&waiter) {
                    waiters.push(waiter.clone());
                }
                false
            }
            None => {
                active.insert(req.request_id.clone(), vec![waiter]);
                true
            }
        }
    };
    if new_request {
        // Close the race window: check `plans` one more time after registering in `in_progress`.
        let completed = self.plans.lock().unwrap().get(&req.request_id).cloned();
        if let Some((plan, under_replicated)) = completed {
            let _ = self.in_progress.lock().unwrap().remove(&req.request_id);
            self.send_reply(reply_to, &plan, under_replicated, &req.reporter_id)
                .await;
            return;
        }
        // Admit to bounded request queue; if full, remove waiters and reply Msg::Busy.
        self.metrics.queue_inc();
        if self
            .request_tx
            .try_send(QueuedRequest {
                req: req.clone(),
                queued_at: Instant::now(),
            })
            .is_err()
        {
            self.metrics.queue_dec();
            let waiters = self
                .in_progress
                .lock()
                .unwrap()
                .remove(&req.request_id)
                .unwrap_or_default();
            for (waiter, _) in waiters {
                self.send_busy(waiter, &req.request_id).await;
            }
        }
    }
}
```

---

## 5. Per-Request Failover, Loser Monitoring, and Execution Flow (`src/node.rs`)

**What was done:** Each admitted request runs in `Node::process` across up to `cfg.max_attempts` (`4`) attempts (`attempt = 0..3`):

- **If this node wins (`out.winner == self.id`):**
  1. It logs the `ELECTION` CSV line.
  2. It checks the Tier 3 idempotency guard (`self.started.lock().unwrap().insert(id.clone())`).
  3. It executes Service 1 (`self.execute(req.clone()).await`). If the CPU worker fails, it removes `id` from `started` so a retry can proceed.
  4. It records the service time in `self.metrics`, runs **mandatory majority quorum replication** (`self.replicate(&plan, out.attempt).await`), stores the plan locally via `store_plan`, logs the `REQUEST` timing CSV line, and exits the attempt loop to reply to all waiting client sockets.
- **If the election abstained (`out.winner == NO_WINNER`):**
  - This happens when the node is not ready yet or when a better peer sent `Ok` and died before announcing `Coordinator`. The node sleeps `t_ok` (`100 ms`) and advances to `attempt + 1`.
- **If another peer won (`out.winner != self.id`):**
  - The losing node waits up to `cfg.exec_timeout` (`3,000 ms`), polling every `20 ms`.
  - As soon as the winner's `Msg::Replicate` arrives (populating `self.plans`), the loser breaks out of the loop and finishes cleanly without replying to the client (since the winner replies).
  - If `!self.alive_nodes().contains(&out.winner)` becomes true (the winner stopped heartbeating for `1,000 ms`), the loser immediately aborts the wait and starts `attempt + 1` to elect a new coordinator among the surviving nodes.

```rust
// ---------- per-request flow ----------
async fn process(self: Arc<Self>, queued: QueuedRequest) {
    let QueuedRequest { req, queued_at } = queued;
    let id = req.request_id.clone();
    let queue_wait = queued_at.elapsed();
    let active = self.active_requests.fetch_add(1, Relaxed) + 1;
    self.max_active_requests.fetch_max(active, Relaxed);
    let _active_guard = ActiveRequestGuard(&self.active_requests);
    let mut reply_ready = false;
    let mut plan_under_replicated = false;

    for attempt in 0..self.cfg.max_attempts {
        if self.is_muted() || self.plans.lock().unwrap().contains_key(&id) {
            break;
        }
        let out = self
            .strategy
            .elect(&self, &id, &req.reporter_id, attempt)
            .await;
        self.elections.fetch_add(1, Relaxed);

        if out.winner == self.id {
            println!(
                "ELECTION,{},{},{},{},{},{},{}",
                self.strategy.name(),
                id,
                attempt,
                out.winner,
                out.my_score,
                out.elapsed.as_micros(),
                out.complete
            );
            // Tier 3 Idempotency: never execute the same request_id twice on this node.
            if self.is_muted()
                || self.plans.lock().unwrap().contains_key(&id)
                || !self.started.lock().unwrap().insert(id.clone())
            {
                break;
            }
            let t0 = Instant::now();
            let plan = match self.execute(req.clone()).await {
                Ok(plan) => plan,
                Err(error) => {
                    eprintln!("[node {}] CPU worker failed for request {id}: {error}", self.id);
                    self.started.lock().unwrap().remove(&id);
                    continue;
                }
            };
            let processing_elapsed = t0.elapsed();
            self.metrics
                .record_service(processing_elapsed.as_secs_f64() * 1000.0);
            if self.is_muted() {
                break; // crashed mid-execution: result is lost, surviving peers will re-elect
            }
            let replication_started = Instant::now();
            let replicated = self.replicate(&plan, out.attempt).await; // mandatory quorum step
            let replication_elapsed = replication_started.elapsed();
            self.store_plan(plan.clone(), !replicated, (out.attempt, self.id));
            if !replicated {
                self.under_replicated.fetch_add(1, Relaxed);
            }
            reply_ready = true;
            plan_under_replicated = !replicated;
            println!(
                "REQUEST,{},{},{},{},{}",
                id,
                self.id,
                queue_wait.as_micros(),
                processing_elapsed.as_micros(),
                replication_elapsed.as_micros()
            );
            break;
        }

        // No winner identified (e.g. node not ready or better candidate died after Ok).
        if out.winner == NO_WINNER {
            println!("[node {}] election for {id} abstained, retrying", self.id);
            sleep(self.cfg.t_ok).await;
            continue;
        }

        // Loser path: wait for the winner's replicated plan; re-elect early if winner dies.
        let deadline = Instant::now() + self.cfg.exec_timeout;
        let mut got = false;
        while Instant::now() < deadline && !self.is_muted() {
            if self.plans.lock().unwrap().contains_key(&id) {
                got = true;
                break;
            }
            if !self.alive_nodes().contains(&out.winner) {
                println!(
                    "[node {}] winner {} of {id} looks dead -> re-election",
                    self.id, out.winner
                );
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        if got {
            break;
        }
    }

    // Release per-request election round state and deliver replies to registered waiters.
    self.strategy.forget(&id);
    let waiters = self
        .in_progress
        .lock()
        .unwrap()
        .remove(&id)
        .unwrap_or_default();
    let completed_plan = self.plans.lock().unwrap().get(&id).cloned();
    if reply_ready {
        if let Some((plan, _)) = completed_plan {
            for (to, client) in waiters {
                self.send_reply(to, &plan, plan_under_replicated, &client)
                    .await;
            }
        }
    } else if completed_plan.is_none() {
        for (to, _) in waiters {
            self.send_busy(to, &id).await;
        }
    }
    self.metrics.queue_dec();
}
```

---

## 6. Majority Quorum Replication and Deterministic Conflict Resolution (`src/node.rs`, `src/election.rs`)

### 6.1 Majority Quorum Replication (`Node::replicate`)

**What was done:** After computing a `ResponsePlan`, the winning coordinator replicates it to live peers and waits until a strict majority of the configured cluster holds the plan:

$$\text{majority} = \lfloor N / 2 \rfloor + 1, \qquad \text{need} = (\lfloor N / 2 \rfloor + 1) - 1$$

Because `self.peers` includes all $N$ configured nodes (including `self`), in a 3-node cluster `need = (3 / 2 + 1) - 1 = 1` peer acknowledgement. The winner sends `Msg::Replicate { plan, attempt, from: self.id }` to all currently alive peers that have not yet acknowledged, and wakes up immediately via `self.ack_notify` as soon as `count(self) >= need` (or retries every `cfg.repl_retry = 150 ms` until `cfg.repl_timeout = 1,500 ms`). If no live peers remain (`targets.is_empty()`) and `count < need`, it exits early and marks the plan `under_replicated = true`.

```rust
/// Send the plan to the live peers and wait until a MAJORITY of the cluster (incl. us)
/// holds it. Un-acked peers are retried. Returns false if quorum was not reached.
async fn replicate(&self, plan: &ResponsePlan, attempt: u32) -> bool {
    let id = plan.request_id.clone();
    // Total configured cluster size is self.peers.len(); we count as 1 replica ourselves.
    let need = (self.peers.len() / 2 + 1).saturating_sub(1); // peer acks needed (1 in a 3-node cluster)
    if need == 0 {
        return true;
    }
    self.acks.lock().unwrap().insert(id.clone(), HashSet::new());
    let msg = Msg::Replicate {
        plan: plan.clone(),
        attempt,
        from: self.id,
    };
    let deadline = Instant::now() + self.cfg.repl_timeout;
    let count = |me: &Node| me.acks.lock().unwrap().get(&id).map_or(0, |s| s.len());

    let ok = loop {
        // Send only to alive peers that have not yet sent ReplicateAck for this request_id.
        let targets: Vec<NodeId> = {
            let a = self.acks.lock().unwrap();
            let got = a.get(&id);
            self.alive_peers()
                .into_iter()
                .filter(|p| got.map_or(true, |g| !g.contains(p)))
                .collect()
        };
        if targets.is_empty() && count(self) < need {
            break false; // no live un-acked peers left to ask
        }
        for t in targets {
            self.send_to(t, &msg).await;
        }
        // Wait up to repl_retry (150 ms) or wake immediately when ack_notify fires.
        let hit = wait_for(&self.ack_notify, self.cfg.repl_retry, || {
            count(self) >= need
        })
        .await;
        if hit {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
    };
    self.acks.lock().unwrap().remove(&id);
    ok
}
```

When a peer receives `Msg::Replicate`, it merges the plan with `store_plan` and always sends back `Msg::ReplicateAck`:

```rust
Msg::Replicate { plan, attempt, from } => {
    let id = plan.request_id.clone();
    // Keep the plan with higher (attempt, lower node id) precedence; always ack.
    self.store_plan(plan, false, (attempt, from));
    self.send_to(
        from,
        &Msg::ReplicateAck {
            request_id: id,
            from: self.id,
        },
    )
    .await;
}
Msg::ReplicateAck { request_id, from } => {
    if let Some(set) = self.acks.lock().unwrap().get_mut(&request_id) {
        set.insert(from);
    }
    self.ack_notify.notify_waiters();
}
```

### 6.2 Deterministic Conflict Resolution (`supersedes` and `store_plan`)

**What was done:** If a slow winner from `attempt = 0` replicates its plan *after* another node already won `attempt = 1` and replicated a newer plan—or if two nodes in the same attempt both executed—every node must keep the exact same winning plan regardless of packet arrival order. `election::supersedes` defines a strict total order on `prec = (attempt, origin_node_id)`:

1. **Higher `attempt` wins** (`new.0 > old.0`): later failover attempts always override stale attempts.
2. **Lower `NodeId` wins ties** (`new.0 == old.0 && new.1 < old.1`): matches the duplicate-coordinator tie-break rule.

```rust
// In src/election.rs:
/// Precedence for two plans of the same request: a later failover attempt wins;
/// within the same attempt, the lower node id wins (matching coordinator tie-break).
pub fn supersedes(new: (u32, NodeId), old: (u32, NodeId)) -> bool {
    new.0 > old.0 || (new.0 == old.0 && new.1 < old.1)
}

// In src/node.rs:
/// Store `plan` unless a plan with higher precedence (see `election::supersedes`)
/// is already stored. Returns true if the stored plan was replaced/created.
fn store_plan(&self, plan: ResponsePlan, under: bool, prec: (u32, NodeId)) -> bool {
    let id = plan.request_id.clone();
    let mut precs = self.plan_prec.lock().unwrap();
    let mut plans = self.plans.lock().unwrap();
    let accept = precs.get(&id).map_or(true, |old| supersedes(prec, *old));
    if accept {
        precs.insert(id.clone(), prec);
        plans.insert(id, (plan, under));
    }
    accept
}
```

---

## 7. Fault Injection (`mute_for`), Rejoin Warmup Guard, and Chunked State Sync (`src/node.rs`)

**What was done:** When a node fails for up to 20 seconds (`mute_for(secs)`) or starts up fresh, it must not jump straight into winning elections with an empty peer table and stale plan cache:

1. `mute_for(secs)` immediately sets `ready = false` and `muted = true`. After `secs` seconds, it clears volatile election state (`strategy.reset()`), clears stale peer heartbeat timestamps (`last_seen.clear()`), sets `muted = false`, and runs `begin_warmup().await`.
2. `begin_warmup()` keeps `ready = false` while it waits up to `cfg.warmup` (`600 ms`) to hear at least one peer heartbeat. Once a live peer is known, it calls `sync_state().await` before setting `ready = true`.
3. `sync_state()` sends `Msg::SyncRequest { from: self.id }` to a live peer (up to `SYNC_ATTEMPTS = 3` rounds, rotating across live peers) and waits for `SyncProgress::complete()`.
4. `serve_sync(to)` snapshots all stored plans along with their `(attempt, origin)` precedence and `under_replicated` status, splits them into chunks of `SYNC_CHUNK = 8` entries so no packet exceeds the 16 KiB UDP receive buffer, and sends `Msg::SyncReply { from, part, parts, entries }`. Even if a peer has 0 plans, it sends 1 empty `SyncReply` (`part: 0, parts: 1`) so the rejoining node knows sync completed.
5. As each `SyncReply` arrives, the rejoining node merges the entries through `store_plan(...)`, records `part` in `sync.got`, and once `got.len() == parts`, logs `SYNC,<node_id>,<peer_id>,<total_plans>` and flips `ready = true`.

```rust
// ---------- fault injection ----------
pub fn mute_for(self: &Arc<Self>, secs: u64) {
    self.ready.store(false, Relaxed); // not eligible to win elections until it has resynced
    self.muted.store(true, Relaxed);
    println!("[node {}] MUTED for {secs}s", self.id);
    let e = self.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(secs)).await;
        e.strategy.reset();                  // clear in-memory election rounds
        e.last_seen.lock().unwrap().clear(); // re-learn liveness from fresh heartbeats
        e.muted.store(false, Relaxed);
        e.begin_warmup().await;              // wait for peer heartbeat + run SYNC
        println!("[node {}] REJOINED (synced, ready)", e.id);
    });
}

/// Rejoin protocol: not ready -> wait for a peer heartbeat -> sync plans -> ready.
pub async fn begin_warmup(&self) {
    self.ready.store(false, Relaxed);
    if self.cfg.rejoin_guard && self.peers.len() > 1 {
        let deadline = Instant::now() + self.cfg.warmup;
        while Instant::now() < deadline && self.alive_peers().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
        if self.alive_peers().is_empty() {
            println!("[node {}] warmup: no peer heard, serving alone", self.id);
        } else if !self.sync_state().await {
            println!(
                "[node {}] warmup: state sync incomplete, serving anyway",
                self.id
            );
        }
    }
    self.ready.store(true, Relaxed);
}

/// Pull every plan a live peer holds and merge it with the (attempt, node) rule.
async fn sync_state(&self) -> bool {
    for round in 0..SYNC_ATTEMPTS {
        let peers = self.alive_peers();
        let Some(&peer) = peers.get(round % peers.len().max(1)) else {
            return false;
        };
        *self.sync.lock().unwrap() = SyncProgress {
            from: Some(peer),
            ..Default::default()
        };
        self.send_to(peer, &Msg::SyncRequest { from: self.id })
            .await;
        let wait = self.cfg.t_coord.max(Duration::from_millis(200));
        let done = wait_for(&self.sync_notify, wait, || {
            self.sync.lock().unwrap().complete()
        })
        .await;
        if done {
            self.sync.lock().unwrap().from = None;
            println!(
                "SYNC,{},{},{}",
                self.id,
                peer,
                self.plans.lock().unwrap().len()
            );
            return true;
        }
    }
    self.sync.lock().unwrap().from = None;
    false
}

async fn serve_sync(&self, to: NodeId) {
    let entries: Vec<SyncEntry> = {
        let precs = self.plan_prec.lock().unwrap();
        let plans = self.plans.lock().unwrap();
        plans
            .iter()
            .map(|(id, (plan, under))| {
                let (attempt, origin) = precs
                    .get(id)
                    .copied()
                    .unwrap_or((0, plan.processed_by_node));
                SyncEntry {
                    plan: plan.clone(),
                    under_replicated: *under,
                    attempt,
                    origin,
                }
            })
            .collect()
    };
    let chunks: Vec<&[SyncEntry]> = entries.chunks(SYNC_CHUNK).collect();
    let parts = chunks.len().max(1) as u32;
    if chunks.is_empty() {
        let m = Msg::SyncReply {
            from: self.id,
            part: 0,
            parts,
            entries: vec![],
        };
        self.send_to(to, &m).await;
    }
    for (i, chunk) in chunks.into_iter().enumerate() {
        let m = Msg::SyncReply {
            from: self.id,
            part: i as u32,
            parts,
            entries: chunk.to_vec(),
        };
        self.send_to(to, &m).await;
    }
}
```

---

## 8. Client-Side Fault Tolerance and Multi-Interface Routing (`src/client.rs`)

**What was done:** During physical lab testing across 6 PCs (some with both Wi-Fi and Ethernet interfaces, and with Node 2 killed/muted during Experiment E4), three client-side networking issues had to be solved in `submit_request`:

1. **Multi-Interface Binding & Per-Peer Route Discovery:** Binding a client socket to `0.0.0.0:0` lets it receive replies on any interface, but `0.0.0.0` cannot be placed in `Msg::Request.reply_to`. For each cloud node address, we probe the OS routing table (`std::net::UdpSocket::bind` + `connect(address)` + `local_addr().ip()`) to discover the exact local IP that routes to that peer.
2. **Ignoring `ConnectionRefused` from Dead Nodes:** When a cloud node process is stopped, the OS kernel may return an ICMP Port Unreachable packet that surfaces on the client's UDP socket as `ErrorKind::ConnectionRefused` on the next `recv_from`. `submit_request` explicitly ignores `ConnectionRefused` and keeps waiting for surviving nodes.
3. **Bounded 1-Second Multicast Resends (`MAX_RESENDS = 4`):** If a packet is lost or arrives while the cluster is mid-failover, the client slices its `5,000 ms` timeout into `1,000 ms` intervals (`RESEND_INTERVAL`) and resends `Msg::Request` up to 4 times. Once a valid `Msg::Reply` arrives, the client immediately sends `Msg::Ack { request_id }` back to the winning server so the server's client-reliability and RTT metrics stay accurate.

```rust
async fn submit_request(
    config: &ClientConfig,
    req: IncidentRequest,
) -> Result<ResponsePlan, String> {
    let bind_addr = match config.node_addresses[0].ip() {
        IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let socket = UdpSocket::bind(bind_addr)
        .await
        .map_err(|error| format!("bind client socket: {error}"))?;
    let local_port = socket
        .local_addr()
        .map_err(|error| format!("read client socket address: {error}"))?
        .port();

    // Build per-node request packets with the routable local IP for each cloud peer.
    let mut packets = Vec::with_capacity(config.node_addresses.len());
    for &address in &config.node_addresses {
        let client_ip = std::net::UdpSocket::bind(bind_addr)
            .and_then(|s| {
                s.connect(address)?;
                Ok(s.local_addr()?.ip())
            })
            .unwrap_or_else(|_| bind_addr.ip());
        let reply_to = SocketAddr::new(client_ip, local_port);
        let bytes = serde_json::to_vec(&Msg::Request {
            req: req.clone(),
            reply_to,
        })
        .map_err(|error| format!("serialize request: {error}"))?;
        packets.push((address, bytes));
    }
    for (address, bytes) in &packets {
        let _ = socket.send_to(bytes, address).await;
    }

    const MAX_RESENDS: usize = 4;
    const RESEND_INTERVAL: Duration = Duration::from_millis(1000);
    let deadline = Instant::now() + config.request_timeout;
    let mut resends = 0usize;
    let mut busy_nodes = 0;
    let mut buf = vec![0u8; 16384];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("request timed out".into());
        }
        let wait_slice = if resends < MAX_RESENDS {
            remaining.min(RESEND_INTERVAL)
        } else {
            remaining
        };
        let (size, source) = match tokio::time::timeout(wait_slice, socket.recv_from(&mut buf)).await {
            Ok(Ok(response)) => response,
            // Ignore ICMP Port Unreachable from a dead/stopped cloud node
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => continue,
            Ok(Err(error)) => return Err(format!("receive response: {error}")),
            Err(_) => {
                // Slice timed out: resend request to all nodes if resends remain
                if Instant::now() < deadline && resends < MAX_RESENDS {
                    resends += 1;
                    busy_nodes = 0;
                    for (address, bytes) in &packets {
                        let _ = socket.send_to(bytes, address).await;
                    }
                    continue;
                }
                return Err("request timed out".into());
            }
        };
        // ... correlate reply by request_id and send Msg::Ack { request_id } to `source` ...
```

---

## 9. Standalone Version-Vector & Replicated Log Engine (`src/fault_tolerance.rs`)

**What was done:** In addition to the live UDP snapshot replication in `src/node.rs`, I implemented `FaultToleranceManager` in `src/fault_tolerance.rs` (1,020 lines). It is a transport-agnostic state machine (parameterized over `PeerReplicationTransport`) that models formal causal log replication, contiguous version vectors, out-of-order gap buffering, tombstone sequence preservation, and delta-only state recovery.

### 9.1 Data Structures: `VersionVector`, `LogEntry`, and Tombstones

Each node assigns monotonically increasing sequence numbers (`local_seq = 1, 2, 3, ...`) to entries it originates. Every node tracks a `VersionVector` (`HashMap<u32, u64>`) mapping each `origin_node` to the highest **contiguous** sequence number applied from that origin. If a replication attempt fails to get any peer ACK (`acks == 0`), the sequence number is **never reused or rolled back**—instead, it is committed as a **tombstone (`plan: None`)** so that concurrent later sequence numbers (`seq + 1`) are never stuck behind a permanent gap.

```rust
/// Maps origin_node_id -> highest CONTIGUOUS sequence number applied from that node
pub type VersionVector = HashMap<u32, u64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeHealthState {
    Active,
    Failed { recover_at: Instant },
    Recovering,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub origin_node: u32,
    pub seq: u64,
    pub request_id: String,
    /// `None` is a tombstone: the seq was consumed but replication failed, so no plan exists.
    /// It exists only so the contiguous version vector keeps advancing.
    pub plan: Option<ResponsePlan>,
    pub committed_at_ms: u64,
}
```

### 9.2 Replicate-First Quorum Commit and Tombstone Fallback (`record_and_replicate`)

**What was done:** In `FaultToleranceManager::record_and_replicate`, the node allocates `seq = st.local_seq + 1` and broadcasts the candidate `LogEntry` to peers **before** committing it to its own `idempotency_map`:

- If `acks >= 1` (at least one peer acknowledged, forming a 2-of-3 majority with the origin), the entry is applied locally (`apply_entry_locked`) and cached as `IdempotencyRecord::Completed`.
- If `acks == 0`, the node converts the entry into a tombstone (`plan: None`), applies the tombstone locally so `version_vector[self.node_id]` still advances contiguously, removes the `InProgress` lock so the client can retry, broadcasts the tombstone best-effort to void any partial delivery whose ACK was lost, and returns `Err(FaultError::ReplicationQuorumFailed)`.

```rust
pub async fn record_and_replicate(
    &self,
    request_id: String,
    plan: ResponsePlan,
    transport: &dyn PeerReplicationTransport,
) -> Result<LogEntry, FaultError> {
    // 1. Allocate a sequence number WITHOUT committing to log or marking Completed yet
    let (seq, now_ms) = {
        let mut st = self.state.write().await;
        if !matches!(st.health, NodeHealthState::Active) {
            return Err(FaultError::NodeUnavailable);
        }
        st.local_seq += 1;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        (st.local_seq, now_ms)
    };

    let candidate_entry = LogEntry {
        origin_node: self.node_id,
        seq,
        request_id: request_id.clone(),
        plan: Some(plan),
        committed_at_ms: now_ms,
    };

    // 2. Broadcast to peers and require >= 1 peer ACK (majority of 3-node cluster) before committing
    let acks = transport.broadcast_entry(&candidate_entry).await;
    if acks == 0 {
        let tombstone = LogEntry {
            plan: None,
            ..candidate_entry
        };
        {
            let mut st = self.state.write().await;
            Self::apply_entry_locked(&mut st, tombstone.clone());
            // Clear the InProgress lock so a client retry can proceed
            if matches!(
                st.idempotency_map.get(&request_id),
                Some(IdempotencyRecord::InProgress { .. })
            ) {
                st.idempotency_map.remove(&request_id);
            }
        }
        // Best-effort: tell peers this seq is void (in case a peer applied it but its ACK was dropped)
        let _ = transport.broadcast_entry(&tombstone).await;
        return Err(FaultError::ReplicationQuorumFailed);
    }

    // 3. Replicated on >= 1 peer: commit locally.
    let mut st = self.state.write().await;
    let still_active = matches!(st.health, NodeHealthState::Active);
    Self::apply_entry_locked(&mut st, candidate_entry.clone());
    if !still_active {
        return Err(FaultError::NodeUnavailable);
    }
    Ok(candidate_entry)
}
```

### 9.3 Out-of-Order Gap Buffering (`apply_entry_locked`)

**What was done:** If entries from `origin` arrive out of order (for example, `seq = 3` and `seq = 2` arrive before `seq = 1`), `apply_entry_locked` buffers them in `pending_out_of_order[origin]` (`BTreeMap<u64, LogEntry>`) without advancing `version_vector[origin]`. As soon as `seq = 1` arrives, the `while let Some(next_entry)` loop drains `seq = 1, 2, 3` in contiguous order, updating `committed_logs`, `idempotency_map`, and `version_vector`. Furthermore, a tombstone (`plan: None`) always overrides a real entry for the same `(origin, seq)` and can never be overwritten by a late real packet.

```rust
fn apply_entry_locked(st: &mut InternalState, entry: LogEntry) {
    let origin = entry.origin_node;
    let current_contig = st.version_vector.get(&origin).copied().unwrap_or(0);

    if entry.seq <= current_contig {
        // If a tombstone arrives for an already-applied seq, void the plan and evict from cache.
        if entry.plan.is_none() {
            // ... replace existing real entry with tombstone and remove from idempotency_map ...
        }
        return;
    }

    let buf = st.pending_out_of_order.entry(origin).or_default();
    // A buffered tombstone must never be replaced by a delayed real entry for the same seq
    if matches!(buf.get(&entry.seq), Some(b) if b.plan.is_none()) && entry.plan.is_some() {
        return;
    }
    buf.insert(entry.seq, entry);

    // Drain all contiguous entries starting at current_contig + 1
    let mut next_expected = current_contig + 1;
    while let Some(next_entry) = st
        .pending_out_of_order
        .get_mut(&origin)
        .and_then(|buf| buf.remove(&next_expected))
    {
        let should_update_winner = next_entry.plan.is_some()
            && match st.idempotency_map.get(&next_entry.request_id) {
                None | Some(IdempotencyRecord::InProgress { .. }) => true,
                Some(IdempotencyRecord::Completed(existing_winner)) => {
                    (next_entry.origin_node, next_entry.seq)
                        < (existing_winner.origin_node, existing_winner.seq)
                }
            };

        if should_update_winner {
            st.idempotency_map.insert(
                next_entry.request_id.clone(),
                IdempotencyRecord::Completed(next_entry.clone()),
            );
        }

        st.committed_logs
            .entry(origin)
            .or_default()
            .insert(next_expected, next_entry);

        st.version_vector.insert(origin, next_expected);
        next_expected += 1;
    }
}
```

### 9.4 Bidirectional Version-Vector Delta Recovery (`recover_state_from_peers`)

**What was done:** When a failed node's outage timer expires, it transitions from `NodeHealthState::Failed` to `NodeHealthState::Recovering` (which accepts peer traffic but still blocks client requests). `recover_state_from_peers` fetches each reachable peer's `VersionVector`, computes `delta_for_peer` (`collect_entries_since_locked(&st, &peer_vv)`), exchanges missing entries via `sync_with_peer`, and verifies convergence (`our_vv[origin] >= peer_vv[origin]` for all origins across all reachable peers) before transitioning back to `NodeHealthState::Active` and recording `RecoveryMetrics`.

---

## 10. Tests, Repository Status, and Limitations

### 10.1 Automated Tests Covering My Parts

All tests run with `cargo test -- --test-threads=1`:

| File | Test name | What it verifies |
| --- | --- | --- |
| `src/node.rs` | `duplicate_plans_resolve_by_attempt_then_lowest_node` | `store_plan` and `supersedes` keep lower `node_id` on same attempt, higher `attempt` always overwrites lower `attempt`, and stale attempts are rejected. |
| `src/node.rs` | `concurrent_requests_overlap_and_respect_node_limit` | Duplicate `Msg::Request` datagrams coalesce in `in_progress` and execute only once (`started.len() == 3`), while respecting `max_concurrent_requests`. |
| `src/node.rs` | `full_request_queue_rejects_excess_and_keeps_serving` | When `request_queue_capacity` is full, excess requests receive `Msg::Busy` while admitted requests complete normally. |
| `src/node.rs` | `cached_retry_preserves_original_replication_status` | A request executed without live peers replies with `under_replicated: true`, and subsequent cached retries for the same `request_id` preserve `under_replicated: true`. |
| `src/node.rs` | `concurrent_requests_keep_election_and_replication_correlated` | Spins up a 3-node UDP cluster (`LoadAwareBully`), multicasts concurrent requests, verifies `under_replicated == false`, sends `Msg::Ack`, and confirms all 3 nodes hold all replicated plans. |
| `src/fault_tolerance.rs` | `test_out_of_order_arrival_buffers_until_gap_filled` | `seq = 3` and `seq = 2` buffer in `pending_out_of_order` with `VV[1] == 0` until `seq = 1` arrives, then `VV[1]` jumps to `3` and caches all three plans. |
| `src/fault_tolerance.rs` | `test_tombstone_overrides_applied_entry` | If an origin voids `seq = 1` with a tombstone (`plan: None`), a peer that had applied `seq = 1` evicts the plan from `idempotency_map` while keeping `VV[1] == 1`. |
| `src/fault_tolerance.rs` | `test_buffered_tombstone_not_overwritten_by_late_real_entry` | A buffered tombstone at `seq = 2` cannot be overwritten by a delayed real packet for `seq = 2`. |
| `src/fault_tolerance.rs` | `test_concurrent_requests_one_fails_replication_vv_advances` | If `seq = 1` fails replication (becomes a tombstone) while `seq = 2` succeeds concurrently, `VV[1]` still advances to `2` on all nodes without stalling behind `seq = 1`. |
| `src/fault_tolerance.rs` | `test_failure_during_replication_keeps_entry_but_returns_err` | If a node fails after broadcasting to peers, it still commits the entry locally (matching peers) while returning `Err(FaultError::NodeUnavailable)` to the caller. |
| `src/fault_tolerance.rs` | `test_three_node_failure_and_delta_recovery` | Full 3-node failure and recovery cycle: Node 3 fails for 1s while Nodes 1 and 2 commit `req-A` and `req-B`; Node 3 transitions `Failed -> Recovering -> Active`, pulls the 2 missing entries via version-vector delta sync, and records `RecoveryMetrics`. |
| `src/fault_tolerance.rs` | `test_rollback_when_zero_peer_acks` | When both peers are down (`0` ACKs), `record_and_replicate` returns `Err(ReplicationQuorumFailed)`, leaves `req-X` uncached so it can be retried, and consumes `seq = 1` with a tombstone. |

### 10.2 Latest Repository Branch Status (as of Nov 8, 2026)

- **`origin/main` (`949c4d8`):** Contains the core integrated UDP node, election, client, fault-tolerance module, and merged PR #1 (`origin/stress-testing`, `5035831`) with all lab CSV results (`stress/results/e1_ramp`, `e1_udp`, `e2_imbalance`, `e4_fail20`, `e5_rejoin`, `election_overhead`). Note that on `origin/main`, `Node::execute` still uses the 300 ms placeholder plan.
- **`origin/incident_proc_coord` (`5eb9ec8`) & `origin/failure-injection` (`5adfc78`):** Contains Haya's `src/planner.rs`, `src/snapshot.rs`, the `Node::execute` call to `planner::plan`, the periodic election-based failure injector (`spawn_failure_injector_with` in `src/node.rs`, commit `2dcc08b`), and the demo GUI (`gui/gui.py`, commit `5adfc78`).

### 10.3 Limitations and Assumptions

- **Two Replication Implementations:** The live UDP cluster in `src/node.rs` uses direct `Msg::Replicate` / `Msg::ReplicateAck` majority acknowledgements and chunked snapshot `SyncReply` transfers (`store_plan` with `(attempt, node_id)` precedence). The formal version-vector and tombstone log engine in `src/fault_tolerance.rs` is tested against `PeerReplicationTransport` in unit/integration tests and is not wired into the live UDP socket loop.
- **In-Memory State:** Stored plans (`plans`, `plan_prec`, `started`) and version-vector logs live in memory; if a node process is killed and restarted from scratch, it recovers its `plans` map from a live peer during `begin_warmup()` / `sync_state()`, but if all three processes are killed simultaneously, state is not persisted to disk.
- **Best-Effort Rejoin Guard:** With `RM_REJOIN_GUARD=1`, a starting or rejoining node waits up to `RM_WARMUP_MS` (`600 ms`) for a peer heartbeat and tries up to `SYNC_ATTEMPTS = 3` rounds of `SyncRequest`. If no peer responds or packets are lost across all 3 attempts, the node logs a warning and still marks itself `ready = true` so the cluster cannot deadlock.

