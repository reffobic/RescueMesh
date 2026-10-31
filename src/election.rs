//!
//!   1. CONFIG    - timeouts, weights, caps (all overridable via RM_* env vars)
//!   2. METRICS   - measured machine/client inputs + the SCORE FUNCTION
//!   3. INTERFACE - `ElectionStrategy` trait, `Outcome`, `wait_for` helper
//!   4. BULLY     - the load-aware Bully algorithm itself
//!
//! Load-aware Bully, run per request.
//!   1. Priority is dynamic: (load_score, node_id) instead of the static node ID.
//!      The score is snapshotted once per round, so one round compares fixed priorities.
//!   2. Scope is one request/attempt, not the cluster. There is no standing coordinator.
//!   3. ELECTION goes to all *live* peers (failure detector), not only "higher" ones,
//!      because with dynamic priority the initiator does not know who is better.
//!   4. Every node that receives the multicast request initiates (multiple initiators).
//!   5. Failover = re-run with attempt+1 (done by Node::process).
//! The "pairwise comparisons" (1 vs 2, 1 vs 3, 2 vs 3) happen implicitly: a node that
//! receives ELECTION from a worse node answers OK and runs its own round against the rest.

use crate::node::{Msg, Node, NodeId};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Notify, time::sleep};


// ============================================================================
// 1. CONFIG
// ============================================================================

#[derive(Clone, Copy, Debug)]
pub enum Policy {
    Weighted,  // the real score function (default)
    QueueOnly, // baselines for the "justify the metric" experiments
    CpuOnly,
    Random,
}

/// Weights should sum to 1.0. Machine terms (cpu, queue, svc, io, mem) get 80%,
/// communication terms (rel, delay) 20%.
#[derive(Clone, Copy, Debug)]
pub struct Weights {
    pub cpu: f64,
    pub queue: f64,
    pub svc: f64,
    pub io: f64,
    pub mem: f64,
    pub rel: f64,
    pub delay: f64,
}

#[derive(Clone, Debug)]
pub struct Config {
    // --- election (Bully) ---
    pub t_ok: Duration,        // wait for OK after ELECTION ("election acknowledgment")
    pub t_coord: Duration,     // wait for COORDINATOR after getting an OK
    pub max_restarts: u32,     // restarts inside one election round
    pub max_attempts: u32,     // re-elections after the winner failed to finish
    // --- failure detection / execution ---
    pub heartbeat: Duration,
    pub fail_after: Duration,
    pub exec_timeout: Duration, // losers wait this long for the winner's result
    // --- replication ---
    pub repl_timeout: Duration, // give up waiting for quorum after this
    pub repl_retry: Duration,   // resend Replicate to un-acked peers this often
    // --- client feedback ---
    pub ack_timeout: Duration,  // an un-acked reply counts against reliability
    // --- metrics ---
    pub sample_every: Duration,
    pub queue_cap: f64,         // queue length that counts as "fully loaded"
    pub svc_cap_ms: f64,        // service time that counts as "fully slow"
    pub rtt_cap_ms: f64,        // client RTT that counts as "fully bad"
    pub disk_cap_bps: f64,
    pub net_cap_bps: f64,
    pub w: Weights,
    pub policy: Policy,
}

fn env_u(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
fn env_f(k: &str, d: f64) -> f64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
fn env_ms(k: &str, d: u64) -> Duration {
    Duration::from_millis(env_u(k, d))
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            t_ok: env_ms("RM_T_OK_MS", 100),
            t_coord: env_ms("RM_T_COORD_MS", 500),
            max_restarts: env_u("RM_MAX_RESTARTS", 3) as u32,
            max_attempts: env_u("RM_MAX_ATTEMPTS", 4) as u32,
            heartbeat: env_ms("RM_HEARTBEAT_MS", 300),
            fail_after: env_ms("RM_FAIL_AFTER_MS", 1000),
            exec_timeout: env_ms("RM_EXEC_TIMEOUT_MS", 3000),
            repl_timeout: env_ms("RM_REPL_TIMEOUT_MS", 1500),
            repl_retry: env_ms("RM_REPL_RETRY_MS", 150),
            ack_timeout: env_ms("RM_ACK_TIMEOUT_MS", 2000),
            sample_every: env_ms("RM_SAMPLE_MS", 500),
            queue_cap: env_f("RM_QUEUE_CAP", 20.0),
            svc_cap_ms: env_f("RM_SVC_CAP_MS", 2000.0),
            rtt_cap_ms: env_f("RM_RTT_CAP_MS", 100.0),
            disk_cap_bps: env_f("RM_DISK_CAP_BPS", 200e6),
            net_cap_bps: env_f("RM_NET_CAP_BPS", 100e6),
            w: Weights {
                cpu: env_f("RM_W_CPU", 0.24),
                queue: env_f("RM_W_QUEUE", 0.24),
                svc: env_f("RM_W_SVC", 0.20),
                io: env_f("RM_W_IO", 0.08),
                mem: env_f("RM_W_MEM", 0.04),
                rel: env_f("RM_W_REL", 0.10),
                delay: env_f("RM_W_DELAY", 0.10),
            },
            policy: match std::env::var("RM_POLICY").as_deref() {
                Ok("queue") => Policy::QueueOnly,
                Ok("cpu") => Policy::CpuOnly,
                Ok("random") => Policy::Random,
                _ => Policy::Weighted,
            },
        }
    }
}

// ============================================================================
// 2. METRICS + SCORE FUNCTION
// ============================================================================

#[derive(Clone, Copy, Debug, Default)]
pub struct Machine {
    pub cpu: f64, // 0..1 smoothed utilisation
    pub mem: f64, // 0..1 used fraction
    pub io: f64,  // 0..1 max(disk, net) bandwidth vs. configured cap
}

#[derive(Clone, Copy, Debug)]
pub struct ClientStats {
    pub rtt_ms: f64,
    pub reliability: f64, // EWMA of "reply was acked by the client"
    samples: u32,
}
impl Default for ClientStats {
    fn default() -> Self {
        ClientStats { rtt_ms: 1.0, reliability: 1.0, samples: 0 }
    }
}

#[derive(Default)]
pub struct Metrics {
    machine: Mutex<Machine>,
    cpu_override: Mutex<Option<f64>>, // for the load-imbalance demo
    queue: AtomicU32,                 // requests this node is executing or has won
    queue_extra: AtomicU32,           // artificial load for the demo
    svc_ms: Mutex<f64>,               // EWMA of execute() duration
    clients: Mutex<HashMap<String, ClientStats>>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    // ---- queue / service time ----
    /// Called the moment this node wins an election (reserves capacity before work starts).
    pub fn queue_inc(&self) {
        self.queue.fetch_add(1, Relaxed);
    }
    pub fn queue_dec(&self) {
        let _ = self.queue.fetch_update(Relaxed, Relaxed, |q| Some(q.saturating_sub(1)));
    }
    pub fn queue_len(&self) -> u32 {
        self.queue.load(Relaxed) + self.queue_extra.load(Relaxed)
    }
    pub fn record_service(&self, ms: f64) {
        let mut s = self.svc_ms.lock().unwrap();
        *s = if *s == 0.0 { ms } else { 0.2 * ms + 0.8 * *s };
    }
    pub fn svc_ms(&self) -> f64 {
        *self.svc_ms.lock().unwrap()
    }

    // ---- demo hooks ----
    pub fn sim_load(&self, extra_queue: u32, cpu: Option<f64>) {
        self.queue_extra.store(extra_queue, Relaxed);
        *self.cpu_override.lock().unwrap() = cpu;
    }

    pub fn machine(&self) -> Machine {
        let mut m = *self.machine.lock().unwrap();
        if let Some(c) = *self.cpu_override.lock().unwrap() {
            m.cpu = c;
        }
        m
    }

    // ---- per-client communication stats (fed by client Ack messages) ----
    pub fn client(&self, client: &str) -> ClientStats {
        self.clients.lock().unwrap().get(client).copied().unwrap_or_default()
    }
    pub fn record_ack(&self, client: &str, rtt_ms: f64) {
        let mut m = self.clients.lock().unwrap();
        let c = m.entry(client.to_string()).or_default();
        c.rtt_ms = if c.samples == 0 { rtt_ms } else { 0.2 * rtt_ms + 0.8 * c.rtt_ms };
        c.reliability = 0.1 + 0.9 * c.reliability;
        c.samples += 1;
    }
    pub fn record_ack_timeout(&self, client: &str) {
        let mut m = self.clients.lock().unwrap();
        let c = m.entry(client.to_string()).or_default();
        c.reliability *= 0.9;
        c.samples += 1;
    }

    // ---- the score function ----
    pub fn score(&self, cfg: &Config, client: &str) -> u64 {
        let m = self.machine();
        let cs = self.client(client);
        let cpu = m.cpu.clamp(0.0, 1.0);
        let queue = (self.queue_len() as f64 / cfg.queue_cap).min(1.0);
        let svc = (self.svc_ms() / cfg.svc_cap_ms).min(1.0);
        let rtt = (cs.rtt_ms / cfg.rtt_cap_ms).min(1.0);
        let s = match cfg.policy {
            Policy::Weighted => {
                let w = &cfg.w;
                let mut s = w.cpu * cpu
                    + w.queue * queue
                    + w.svc * svc
                    + w.io * m.io.clamp(0.0, 1.0)
                    + w.mem * m.mem.clamp(0.0, 1.0)
                    + w.rel * (1.0 - cs.reliability)
                    + w.delay * rtt;
                // Guards: conditions that should nearly disqualify a node, not just nudge it.
                if cpu > 0.95 || m.mem > 0.90 || cs.reliability < 0.5 {
                    s += 0.5;
                }
                s
            }
            Policy::QueueOnly => queue,
            Policy::CpuOnly => cpu,
            Policy::Random => {
                (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() % 10_000) as f64 / 10_000.0
            }
        };
        (s * 10_000.0) as u64
    }

    pub fn describe(&self) -> String {
        let m = self.machine();
        format!(
            "cpu={:.2} mem={:.2} io={:.2} queue={} svc_ms={:.0}",
            m.cpu, m.mem, m.io, self.queue_len(), self.svc_ms()
        )
    }

    // ---- sampler (Linux /proc; zeros elsewhere) ----
    pub fn spawn_sampler(self: &Arc<Self>, every: Duration, disk_cap: f64, net_cap: f64) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut p_cpu = read_cpu();
            let mut p_disk = read_disk();
            let mut p_net = read_net();
            let mut last = Instant::now();
            loop {
                sleep(every).await;
                let dt = last.elapsed().as_secs_f64().max(0.001);
                last = Instant::now();
                let (c, d, n) = (read_cpu(), read_disk(), read_net());
                let mut s = *me.machine.lock().unwrap();
                if let (Some((pb, pt)), Some((b, t))) = (p_cpu, c) {
                    if t > pt {
                        s.cpu = 0.3 * ((b - pb) as f64 / (t - pt) as f64) + 0.7 * s.cpu;
                    }
                }
                if let Some(m) = read_mem() {
                    s.mem = m;
                }
                let du = match (p_disk, d) {
                    (Some(a), Some(b)) => b.saturating_sub(a) as f64 / dt / disk_cap,
                    _ => 0.0,
                };
                let nu = match (p_net, n) {
                    (Some(a), Some(b)) => b.saturating_sub(a) as f64 / dt / net_cap,
                    _ => 0.0,
                };
                s.io = 0.3 * du.max(nu).min(1.0) + 0.7 * s.io;
                (p_cpu, p_disk, p_net) = (c, d, n);
                *me.machine.lock().unwrap() = s;
            }
        });
    }
}

fn read_cpu() -> Option<(u64, u64)> {
    let s = fs::read_to_string("/proc/stat").ok()?;
    let v: Vec<u64> = s.lines().next()?.split_whitespace().skip(1).filter_map(|x| x.parse().ok()).collect();
    if v.len() < 8 {
        return None;
    }
    let idle = v[3] + v[4]; // idle + iowait
    let total: u64 = v[..8].iter().sum();
    Some((total - idle, total))
}

fn read_mem() -> Option<f64> {
    let s = fs::read_to_string("/proc/meminfo").ok()?;
    let get = |k: &str| -> Option<f64> {
        s.lines().find(|l| l.starts_with(k))?.split_whitespace().nth(1)?.parse().ok()
    };
    let (total, avail) = (get("MemTotal:")?, get("MemAvailable:")?);
    Some(1.0 - avail / total)
}

fn read_disk() -> Option<u64> {
    let s = fs::read_to_string("/proc/diskstats").ok()?;
    let mut bytes = 0u64;
    for l in s.lines() {
        let f: Vec<&str> = l.split_whitespace().collect();
        if f.len() < 10 {
            continue;
        }
        let name = f[2];
        // Whole disks only: they have an entry in /sys/block, partitions do not.
        if name.starts_with("loop") || name.starts_with("ram") || name.starts_with("dm-") || name.starts_with("md") {
            continue;
        }
        if !Path::new("/sys/block").join(name).exists() {
            continue;
        }
        let rd: u64 = f[5].parse().unwrap_or(0);
        let wr: u64 = f[9].parse().unwrap_or(0);
        bytes += (rd + wr) * 512;
    }
    Some(bytes)
}

fn read_net() -> Option<u64> {
    let s = fs::read_to_string("/proc/net/dev").ok()?;
    let mut bytes = 0u64;
    for l in s.lines().skip(2) {
        let (iface, rest) = l.split_once(':')?;
        if iface.trim() == "lo" {
            continue;
        }
        let f: Vec<u64> = rest.split_whitespace().filter_map(|x| x.parse().ok()).collect();
        if f.len() >= 9 {
            bytes += f[0] + f[8]; // rx_bytes + tx_bytes
        }
    }
    Some(bytes)
}

// ============================================================================
// 3. ELECTION INTERFACE
// ============================================================================

#[derive(Debug, Clone)]
pub struct Outcome {
    pub winner: NodeId,
    pub elapsed: Duration,
    /// This node's own score snapshot for the round (for logging/analysis).
    pub my_score: u64,
    /// false if the election gave up waiting and decided on partial information.
    pub complete: bool,
}

#[async_trait]
pub trait ElectionStrategy: Send + Sync {
    fn name(&self) -> &'static str;
    /// Elect the node that should execute `req_id`. `attempt` > 0 means a re-election
    /// after the previous winner failed to finish (failover).
    async fn elect(&self, node: &Arc<Node>, req_id: &str, client: &str, attempt: u32) -> Outcome;
    /// Handle an election-protocol message received from a peer.
    async fn on_message(&self, node: &Arc<Node>, msg: Msg);
    /// Drop per-request state once the request is finished (bounds memory).
    fn forget(&self, req_id: &str);
    /// Drop all state (called when a node rejoins after a simulated failure).
    fn reset(&self);
}

/// Race-free "wait until cond() or timeout". Registers for the wakeup *before*
/// checking the condition so a notification can't be lost in between.
pub async fn wait_for<F: Fn() -> bool>(notify: &Notify, timeout: Duration, cond: F) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let n = notify.notified();
        tokio::pin!(n);
        n.as_mut().enable();
        if cond() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        if tokio::time::timeout(deadline - now, n).await.is_err() {
            return cond();
        }
    }
}

// ============================================================================
// 4. LOAD-AWARE BULLY
// ============================================================================

struct Round {
    my_score: u64,
    got_ok: bool,
    coordinator: Option<NodeId>,
}

#[derive(Default)]
pub struct LoadAwareBully {
    rounds: Mutex<HashMap<(String, u32), Round>>,
    notify: Notify,
}

impl LoadAwareBully {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `f` on the round for `key`, creating it (with a score snapshot) if needed.
    fn with_round<T>(&self, node: &Node, key: &(String, u32), client: &str, f: impl FnOnce(&mut Round) -> T) -> T {
        let mut m = self.rounds.lock().unwrap();
        let r = m.entry(key.clone()).or_insert_with(|| Round {
            my_score: node.score_for(client),
            got_ok: false,
            coordinator: None,
        });
        f(r)
    }
}

#[async_trait]
impl ElectionStrategy for LoadAwareBully {
    fn name(&self) -> &'static str {
        "bully"
    }

    async fn elect(&self, node: &Arc<Node>, req_id: &str, client: &str, attempt: u32) -> Outcome {
        let start = Instant::now();
        let (t_ok, t_coord, max_restarts) = (node.cfg.t_ok, node.cfg.t_coord, node.cfg.max_restarts);
        let key = (req_id.to_string(), attempt);
        let my_score = self.with_round(node, &key, client, |r| r.my_score);
        let done = |w: NodeId, complete: bool| Outcome { winner: w, elapsed: start.elapsed(), my_score, complete };

        for _ in 0..max_restarts {
            // A COORDINATOR announcement may already have arrived.
            if let Some(w) = self.with_round(node, &key, client, |r| r.coordinator) {
                return done(w, true);
            }
            if node.alive_peers().is_empty() {
                break; // nobody to ask => I win
            }
            self.with_round(node, &key, client, |r| r.got_ok = false);
            node.broadcast(&Msg::Election {
                req_id: req_id.into(),
                attempt,
                from: node.id,
                score: my_score,
                client: client.into(),
            })
            .await;

            // Step 1: wait for an OK (election acknowledgment).
            let k = key.clone();
            wait_for(&self.notify, t_ok, || {
                let m = self.rounds.lock().unwrap();
                m.get(&k).map_or(false, |r| r.got_ok || r.coordinator.is_some())
            })
            .await;
            if let Some(w) = self.with_round(node, &key, client, |r| r.coordinator) {
                return done(w, true);
            }
            if !self.with_round(node, &key, client, |r| r.got_ok) {
                break; // no OK => nobody better answered => elect myself
            }

            // Step 2: someone better exists; give it t_coord to announce itself.
            let k = key.clone();
            wait_for(&self.notify, t_coord, || {
                let m = self.rounds.lock().unwrap();
                m.get(&k).map_or(false, |r| r.coordinator.is_some())
            })
            .await;
            if let Some(w) = self.with_round(node, &key, client, |r| r.coordinator) {
                return done(w, true);
            }
            // The better node never announced (probably died) -> restart the round.
        }

        // Declare myself coordinator. If restarts were exhausted a better node may still
        // exist (split decision); duplicate execution is harmless (request_id idempotency).
        let exhausted = self.with_round(node, &key, client, |r| r.got_ok);
        self.with_round(node, &key, client, |r| r.coordinator = Some(node.id));
        node.broadcast(&Msg::Coordinator { req_id: req_id.into(), attempt, winner: node.id }).await;
        done(node.id, !exhausted)
    }

    async fn on_message(&self, node: &Arc<Node>, msg: Msg) {
        match msg {
            Msg::Election { req_id, attempt, from, score, client } => {
                let key = (req_id.clone(), attempt);
                let my_score = self.with_round(node, &key, &client, |r| r.my_score);
                // I outrank the sender iff my (score, id) is smaller.
                if (my_score, node.id) < (score, from) {
                    node.send_to(from, &Msg::Ok { req_id, attempt, from: node.id }).await;
                }
            }
            Msg::Ok { req_id, attempt, .. } => {
                self.with_round(node, &(req_id, attempt), "", |r| r.got_ok = true);
                self.notify.notify_waiters();
            }
            Msg::Coordinator { req_id, attempt, winner } => {
                self.with_round(node, &(req_id, attempt), "", |r| r.coordinator = Some(winner));
                self.notify.notify_waiters();
            }
            _ => {}
        }
    }

    fn forget(&self, req_id: &str) {
        self.rounds.lock().unwrap().retain(|(r, _), _| r != req_id);
    }
    fn reset(&self) {
        self.rounds.lock().unwrap().clear();
    }
}