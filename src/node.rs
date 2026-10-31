//! Cloud node: transport, failure detector, fault injection, and the per-request flow:
//!   elect -> (winner) execute -> replicate to quorum -> reply to client -> release.
//! Losers wait for the replicated plan and re-elect if the winner dies or stalls.

use crate::election::{wait_for, Config, ElectionStrategy, Metrics};
use crate::types::{IncidentRequest, ResponsePlan};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{net::UdpSocket, sync::Notify, time::sleep};

pub type NodeId = u32;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Msg {
    // --- membership ---
    Heartbeat { from: NodeId },
    // --- client <-> cloud ---
    Request { req: IncidentRequest, reply_to: SocketAddr },
    Reply { plan: ResponsePlan, under_replicated: bool },
    Ack { request_id: String }, // client confirms it got the reply (feeds rtt/reliability)
    // --- replication ---
    Replicate { plan: ResponsePlan, from: NodeId },
    ReplicateAck { request_id: String, from: NodeId },
    // --- Bully ---
    Election { req_id: String, attempt: u32, from: NodeId, score: u64, client: String },
    Ok { req_id: String, attempt: u32, from: NodeId },
    Coordinator { req_id: String, attempt: u32, winner: NodeId },
    // --- bid baseline ---
    Bid { req_id: String, attempt: u32, from: NodeId, score: u64 },
}

impl Msg {
    fn sender(&self) -> Option<NodeId> {
        match self {
            Msg::Heartbeat { from }
            | Msg::Election { from, .. }
            | Msg::Ok { from, .. }
            | Msg::Bid { from, .. }
            | Msg::Replicate { from, .. }
            | Msg::ReplicateAck { from, .. } => Some(*from),
            Msg::Coordinator { winner, .. } => Some(*winner),
            _ => None,
        }
    }
}

pub struct Node {
    pub id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>, // includes self
    pub cfg: Arc<Config>,
    pub metrics: Arc<Metrics>,
    sock: UdpSocket,
    strategy: Arc<dyn ElectionStrategy>,
    muted: AtomicBool,
    last_seen: Mutex<HashMap<NodeId, Instant>>,
    started: Mutex<HashSet<String>>,     // idempotency: request_ids this node executed
    in_progress: Mutex<HashSet<String>>, // request_ids currently handled by process()
    plans: Mutex<HashMap<String, ResponsePlan>>, // replicated results
    acks: Mutex<HashMap<String, HashSet<NodeId>>>, // replication acks per request
    ack_notify: Notify,
    awaiting_ack: Mutex<HashMap<String, (String, Instant)>>, // reply sent, waiting for client Ack
    pub msgs_sent: AtomicU64,
    pub elections: AtomicU64,
    pub under_replicated: AtomicU64,
}

impl Node {
    pub async fn new(
        id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
        strategy: Arc<dyn ElectionStrategy>,
        cfg: Arc<Config>,
    ) -> Arc<Self> {
        let addr = peers.iter().find(|(i, _)| *i == id).expect("my id not in peer list").1;
        let sock = UdpSocket::bind(addr).await.expect("bind failed");
        Arc::new(Node {
            id,
            peers,
            cfg,
            metrics: Metrics::new(),
            sock,
            strategy,
            muted: AtomicBool::new(false),
            last_seen: Mutex::new(HashMap::new()),
            started: Mutex::new(HashSet::new()),
            in_progress: Mutex::new(HashSet::new()),
            plans: Mutex::new(HashMap::new()),
            acks: Mutex::new(HashMap::new()),
            ack_notify: Notify::new(),
            awaiting_ack: Mutex::new(HashMap::new()),
            msgs_sent: AtomicU64::new(0),
            elections: AtomicU64::new(0),
            under_replicated: AtomicU64::new(0),
        })
    }

    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }
    pub fn is_muted(&self) -> bool {
        self.muted.load(Relaxed)
    }
    /// Score snapshot for a request coming from `client`.
    pub fn score_for(&self, client: &str) -> u64 {
        self.metrics.score(&self.cfg, client)
    }

    // ---------- transport ----------
    pub async fn send_to(&self, to: NodeId, m: &Msg) {
        if let Some((_, addr)) = self.peers.iter().find(|(i, _)| *i == to) {
            self.send_addr(*addr, m).await;
        }
    }
    async fn send_addr(&self, addr: SocketAddr, m: &Msg) {
        if self.is_muted() {
            return; // simulated crash: send nothing
        }
        let _ = self.sock.send_to(&serde_json::to_vec(m).unwrap(), addr).await;
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
            .filter(|p| *p == self.id || seen.get(p).map_or(false, |t| t.elapsed() < self.cfg.fail_after))
            .collect()
    }
    pub fn alive_peers(&self) -> Vec<NodeId> {
        self.alive_nodes().into_iter().filter(|p| *p != self.id).collect()
    }

    // ---------- background loops ----------
    pub fn spawn_loops(self: &Arc<Self>) {
        self.metrics.spawn_sampler(self.cfg.sample_every, self.cfg.disk_cap_bps, self.cfg.net_cap_bps);
        let r = self.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16384];
            loop {
                let Ok((n, _)) = r.sock.recv_from(&mut buf).await else { continue };
                if r.is_muted() {
                    continue; // simulated crash: ignore everything
                }
                if let Ok(m) = serde_json::from_slice::<Msg>(&buf[..n]) {
                    r.handle(m).await;
                }
            }
        });
        let h = self.clone();
        tokio::spawn(async move {
            loop {
                h.broadcast(&Msg::Heartbeat { from: h.id }).await;
                sleep(h.cfg.heartbeat).await;
            }
        });
        // Replies the client never acknowledged count against reliability.
        let a = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(500)).await;
                let expired: Vec<String> = {
                    let mut m = a.awaiting_ack.lock().unwrap();
                    let dead: Vec<String> =
                        m.iter().filter(|(_, (_, t))| t.elapsed() > a.cfg.ack_timeout).map(|(k, _)| k.clone()).collect();
                    dead.iter().filter_map(|k| m.remove(k)).map(|(c, _)| c).collect()
                };
                for c in expired {
                    a.metrics.record_ack_timeout(&c);
                }
            }
        });
    }

    pub async fn handle(self: &Arc<Self>, m: Msg) {
        if let Some(s) = m.sender() {
            self.last_seen.lock().unwrap().insert(s, Instant::now());
        }
        match m {
            Msg::Heartbeat { .. } | Msg::Reply { .. } => {}
            Msg::Request { req, reply_to } => {
                // Idempotent retry: if we already hold the plan, just resend it.
                let known = self.plans.lock().unwrap().get(&req.request_id).cloned();
                if let Some(plan) = known {
                    self.send_reply(reply_to, &plan, false, &req.reporter_id).await;
                    return;
                }
                if !self.in_progress.lock().unwrap().insert(req.request_id.clone()) {
                    return; // already being handled here
                }
                let me = self.clone();
                tokio::spawn(async move { me.process(req, reply_to).await });
            }
            Msg::Ack { request_id } => {
                let hit = self.awaiting_ack.lock().unwrap().remove(&request_id);
                if let Some((client, t0)) = hit {
                    self.metrics.record_ack(&client, t0.elapsed().as_secs_f64() * 1000.0);
                }
            }
            Msg::Replicate { plan, from } => {
                let id = plan.request_id.clone();
                self.plans.lock().unwrap().entry(id.clone()).or_insert(plan);
                self.send_to(from, &Msg::ReplicateAck { request_id: id, from: self.id }).await;
            }
            Msg::ReplicateAck { request_id, from } => {
                if let Some(set) = self.acks.lock().unwrap().get_mut(&request_id) {
                    set.insert(from);
                }
                self.ack_notify.notify_waiters();
            }
            other => self.strategy.on_message(self, other).await,
        }
    }

    // ---------- fault injection ----------
    pub fn mute_for(self: &Arc<Self>, secs: u64) {
        self.muted.store(true, Relaxed);
        println!("[node {}] MUTED for {secs}s", self.id);
        let e = self.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(secs)).await;
            e.strategy.reset(); // election state is volatile
            e.last_seen.lock().unwrap().clear(); // re-learn liveness from fresh heartbeats
            e.muted.store(false, Relaxed);
            println!("[node {}] REJOINED (TODO: state sync before serving)", e.id);
        });
    }

    // ---------- per-request flow ----------
    pub async fn process(self: Arc<Self>, req: IncidentRequest, reply_to: SocketAddr) {
        let id = req.request_id.clone();
        for attempt in 0..self.cfg.max_attempts {
            if self.is_muted() || self.plans.lock().unwrap().contains_key(&id) {
                break;
            }
            let out = self.strategy.elect(&self, &id, &req.reporter_id, attempt).await;
            self.elections.fetch_add(1, Relaxed);

            if out.winner == self.id {
                // CSV line for the experiment scripts.
                println!(
                    "ELECTION,{},{},{},{},{},{},{}",
                    self.strategy.name(), id, attempt, out.winner, out.my_score, out.elapsed.as_micros(), out.complete
                );
                // Idempotency: never execute the same request_id twice on this node.
                if self.is_muted() || !self.started.lock().unwrap().insert(id.clone()) {
                    break;
                }
                self.metrics.queue_inc(); // reserve capacity immediately (burst protection)
                let t0 = Instant::now();
                let plan = self.execute(&req).await;
                self.metrics.record_service(t0.elapsed().as_secs_f64() * 1000.0);
                self.metrics.queue_dec();
                if self.is_muted() {
                    break; // crashed mid-execution: result is lost, others will re-elect
                }
                self.plans.lock().unwrap().insert(id.clone(), plan.clone());
                let replicated = self.replicate(&plan).await; // mandatory
                if !replicated {
                    self.under_replicated.fetch_add(1, Relaxed);
                }
                self.send_reply(reply_to, &plan, !replicated, &req.reporter_id).await;
                break;
            }

            // Not the winner: wait for the replicated plan; re-elect if the winner dies/stalls.
            let deadline = Instant::now() + self.cfg.exec_timeout;
            let mut got = false;
            while Instant::now() < deadline && !self.is_muted() {
                if self.plans.lock().unwrap().contains_key(&id) {
                    got = true;
                    break;
                }
                if !self.alive_nodes().contains(&out.winner) {
                    println!("[node {}] winner {} of {id} looks dead -> re-election", self.id, out.winner);
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
            if got {
                break;
            }
        }
        // Release: per-request coordinator state is dropped; there is no standing leader role.
        self.strategy.forget(&id);
        self.in_progress.lock().unwrap().remove(&id);
    }

    /// Send the plan to the live peers and wait until a MAJORITY of the cluster (incl. us)
    /// holds it. Un-acked peers are retried. Returns false if quorum was not reached.
    async fn replicate(&self, plan: &ResponsePlan) -> bool {
        let id = plan.request_id.clone();
        let need = (self.peers.len() / 2 + 1).saturating_sub(1); // peer acks needed
        if need == 0 {
            return true;
        }
        self.acks.lock().unwrap().insert(id.clone(), HashSet::new());
        let msg = Msg::Replicate { plan: plan.clone(), from: self.id };
        let deadline = Instant::now() + self.cfg.repl_timeout;
        let count = |me: &Node| me.acks.lock().unwrap().get(&id).map_or(0, |s| s.len());
        let ok = loop {
            let targets: Vec<NodeId> = {
                let a = self.acks.lock().unwrap();
                let got = a.get(&id);
                self.alive_peers().into_iter().filter(|p| got.map_or(true, |g| !g.contains(p))).collect()
            };
            if targets.is_empty() && count(self) < need {
                break false; // nobody left to ask
            }
            for t in targets {
                self.send_to(t, &msg).await;
            }
            let hit = wait_for(&self.ack_notify, self.cfg.repl_retry, || count(self) >= need).await;
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

    async fn send_reply(&self, to: SocketAddr, plan: &ResponsePlan, under: bool, client: &str) {
        if self.is_muted() {
            return;
        }
        self.send_addr(to, &Msg::Reply { plan: plan.clone(), under_replicated: under }).await;
        self.awaiting_ack.lock().unwrap().insert(plan.request_id.clone(), (client.to_string(), Instant::now()));
    }

    /// Placeholder for Service 1 (priority + response plan). Replace with the real planner.
    async fn execute(&self, req: &IncidentRequest) -> ResponsePlan {
        sleep(Duration::from_millis(300)).await;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let age_s = now.saturating_sub(req.timestamp_ms) as f64 / 1000.0;
        ResponsePlan {
            request_id: req.request_id.clone(),
            incident_id: req.incident_id.clone(),
            priority_score: req.severity as f64 * 10.0 + age_s.min(600.0) * 0.1,
            assigned_responders: vec![],
            assigned_resources: vec![],
            recommended_route: vec![req.location_zone],
            processed_by_node: self.id,
        }
    }

    pub fn stats(&self) -> String {
        format!(
            "alive={:?} elections={} msgs_sent={} plans={} under_replicated={} | {}",
            self.alive_nodes(),
            self.elections.load(Relaxed),
            self.msgs_sent.load(Relaxed),
            self.plans.lock().unwrap().len(),
            self.under_replicated.load(Relaxed),
            self.metrics.describe()
        )
    }
}