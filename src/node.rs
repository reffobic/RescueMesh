//! Cloud node: transport, failure detector, fault injection, and the per-request flow:
//!   elect -> (winner) execute -> replicate to quorum -> reply to client -> release.
//! Losers wait for the replicated plan and re-elect if the winner dies or stalls.

use crate::election::{supersedes, wait_for, Config, ElectionStrategy, Metrics, NO_WINNER};
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
use tokio::{
    net::UdpSocket,
    sync::{mpsc, Notify, Semaphore},
    time::sleep,
};

pub type NodeId = u32;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Msg {
    // --- membership ---
    Heartbeat {
        from: NodeId,
    },
    // --- client <-> cloud ---
    Request {
        req: IncidentRequest,
        reply_to: SocketAddr,
    },
    Reply {
        plan: ResponsePlan,
        under_replicated: bool,
    },
    Busy {
        request_id: String,
        node_id: NodeId,
    },
    Ack {
        request_id: String,
    }, // client confirms it got the reply (feeds rtt/reliability)
    // --- replication ---
    Replicate {
        plan: ResponsePlan,
        attempt: u32,
        from: NodeId,
    },
    ReplicateAck {
        request_id: String,
        from: NodeId,
    },
    // --- state sync (rejoin) ---
    SyncRequest {
        from: NodeId,
    },
    SyncReply {
        from: NodeId,
        part: u32,
        parts: u32,
        entries: Vec<SyncEntry>,
    },
    // --- Bully ---
    Election {
        req_id: String,
        attempt: u32,
        from: NodeId,
        score: u64,
        client: String,
    },
    Ok {
        req_id: String,
        attempt: u32,
        from: NodeId,
    },
    Coordinator {
        req_id: String,
        attempt: u32,
        winner: NodeId,
    },
    // --- bid baseline ---
    Bid {
        req_id: String,
        attempt: u32,
        from: NodeId,
        score: u64,
    },
}

impl Msg {
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
            _ => None,
        }
    }
}

/// One replicated plan as transferred during a rejoin sync: the plan, whether it was
/// under-replicated, and the (attempt, node) precedence used to resolve duplicates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SyncEntry {
    pub plan: ResponsePlan,
    pub under_replicated: bool,
    pub attempt: u32,
    pub origin: NodeId,
}

#[derive(Default)]
struct SyncProgress {
    from: Option<NodeId>,
    parts: Option<u32>,
    got: HashSet<u32>,
}

impl SyncProgress {
    fn complete(&self) -> bool {
        self.parts.map_or(false, |p| self.got.len() as u32 >= p)
    }
}

/// Plans per SyncReply datagram, kept small enough for the 16 KiB UDP receive buffer.
const SYNC_CHUNK: usize = 8;
const SYNC_ATTEMPTS: usize = 3;

pub struct Node {
    pub id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>, // includes self
    pub cfg: Arc<Config>,
    pub metrics: Arc<Metrics>,
    sock: UdpSocket,
    strategy: Arc<dyn ElectionStrategy>,
    muted: AtomicBool,
    ready: AtomicBool,
    last_seen: Mutex<HashMap<NodeId, Instant>>,
    started: Mutex<HashSet<String>>, // idempotency: request_ids this node executed
    in_progress: Mutex<HashMap<String, Vec<(SocketAddr, String)>>>, // request IDs -> reply waiters
    plans: Mutex<HashMap<String, (ResponsePlan, bool)>>, // result and replication status by request
    plan_prec: Mutex<HashMap<String, (u32, NodeId)>>, // (attempt, node) of the stored plan; lock before `plans`
    sync: Mutex<SyncProgress>,
    sync_notify: Notify,
    acks: Mutex<HashMap<String, HashSet<NodeId>>>, // replication acks per request
    ack_notify: Notify,
    awaiting_ack: Mutex<HashMap<String, (String, Instant)>>, // reply sent, waiting for client Ack
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

struct QueuedRequest {
    req: IncidentRequest,
    queued_at: Instant,
}

struct ActiveRequestGuard<'a>(&'a AtomicU64);

impl Drop for ActiveRequestGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        election::{ElectionStrategy, Outcome},
        types::{IncidentCategory, IncidentStatus},
    };
    use async_trait::async_trait;
    use std::{net::UdpSocket as StdUdpSocket, sync::Arc};

    struct ImmediateElection;

    #[async_trait]
    impl ElectionStrategy for ImmediateElection {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn elect(
            &self,
            node: &Arc<Node>,
            _req_id: &str,
            _client: &str,
            _attempt: u32,
        ) -> Outcome {
            Outcome {
                winner: node.id,
                attempt: 0,
                elapsed: Duration::ZERO,
                my_score: 0,
                complete: true,
            }
        }

        async fn on_message(&self, _node: &Arc<Node>, _msg: Msg) {}

        fn duplicate_coordinators(&self) -> u64 {
            0
        }

        fn forget(&self, _req_id: &str) {}

        fn reset(&self) {}
    }

    fn test_request(request_id: &str) -> IncidentRequest {
        IncidentRequest {
            request_id: request_id.into(),
            incident_id: format!("incident-{request_id}"),
            reporter_id: "test-user".into(),
            category: IncidentCategory::BlockedRoad,
            location_zone: 1,
            severity: 3,
            timestamp_ms: 0,
            status: IncidentStatus::Open,
            ttl_secs: 300,
        }
    }

    #[tokio::test]
    async fn duplicate_plans_resolve_by_attempt_then_lowest_node() {
        let reservation = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let node_address = reservation.local_addr().unwrap();
        drop(reservation);
        let node = Node::new(
            1,
            vec![(1, node_address)],
            Arc::new(ImmediateElection),
            Arc::new(Config::from_env()),
        )
        .await;
        let plan = |by: NodeId| ResponsePlan {
            request_id: "dup".into(),
            incident_id: "inc".into(),
            priority_score: 1.0,
            assigned_responders: vec![],
            assigned_resources: vec![],
            recommended_route: vec![],
            processed_by_node: by,
        };
        let stored_by = |n: &Node| {
            n.plans
                .lock()
                .unwrap()
                .get("dup")
                .unwrap()
                .0
                .processed_by_node
        };

        assert!(node.store_plan(plan(3), false, (0, 3)));
        assert!(node.store_plan(plan(2), false, (0, 2))); // same attempt, lower node wins
        assert!(!node.store_plan(plan(3), false, (0, 3))); // same attempt, higher node loses
        assert!(node.store_plan(plan(3), false, (1, 3))); // higher attempt always wins
        assert!(!node.store_plan(plan(1), false, (0, 1))); // stale attempt loses even with lowest id
        assert_eq!(stored_by(&node), 3);
    }

    #[tokio::test]
    async fn concurrent_requests_overlap_and_respect_node_limit() {
        let reservation = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let node_address = reservation.local_addr().unwrap();
        drop(reservation);
        let mut cfg = Config::from_env();
        cfg.max_concurrent_requests = 2;
        cfg.cpu_workers = 2;
        cfg.request_queue_capacity = 8;
        let node = Node::new(
            1,
            vec![(1, node_address)],
            Arc::new(ImmediateElection),
            Arc::new(cfg),
        )
        .await;
        node.spawn_loops();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reply_to = client.local_addr().unwrap();
        for request_id in ["first", "second", "third"] {
            let bytes = serde_json::to_vec(&Msg::Request {
                req: test_request(request_id),
                reply_to,
            })
            .unwrap();
            client.send_to(&bytes, node_address).await.unwrap();
        }
        let duplicate = serde_json::to_vec(&Msg::Request {
            req: test_request("first"),
            reply_to,
        })
        .unwrap();
        client.send_to(&duplicate, node_address).await.unwrap();

        let started = Instant::now();
        let mut received = HashSet::new();
        let mut buf = vec![0u8; 16384];
        while received.len() < 3 {
            let (size, _) =
                tokio::time::timeout(Duration::from_secs(3), client.recv_from(&mut buf))
                    .await
                    .expect("node did not respond to all requests")
                    .unwrap();
            if let Msg::Reply { plan, .. } = serde_json::from_slice(&buf[..size]).unwrap() {
                received.insert(plan.request_id);
            }
        }

        assert_eq!(
            received,
            HashSet::from(["first".into(), "second".into(), "third".into()])
        );
        assert_eq!(node.max_active_requests.load(Relaxed), 2);
        assert_eq!(node.started.lock().unwrap().len(), 3);
        assert!(started.elapsed() >= Duration::from_millis(500));
    }

    #[tokio::test]
    async fn full_request_queue_rejects_excess_and_keeps_serving() {
        let reservation = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let node_address = reservation.local_addr().unwrap();
        drop(reservation);
        let mut cfg = Config::from_env();
        cfg.max_concurrent_requests = 1;
        cfg.cpu_workers = 1;
        cfg.request_queue_capacity = 1;
        let node = Node::new(
            1,
            vec![(1, node_address)],
            Arc::new(ImmediateElection),
            Arc::new(cfg),
        )
        .await;
        node.spawn_loops();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reply_to = client.local_addr().unwrap();
        let send_request = |request_id: &str| {
            let request = Msg::Request {
                req: test_request(request_id),
                reply_to,
            };
            serde_json::to_vec(&request).unwrap()
        };
        client
            .send_to(&send_request("admitted-first"), node_address)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while node.active_requests.load(Relaxed) == 0 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("first request did not start");

        for request_id in ["admitted-pending", "admitted-queued", "rejected"] {
            client
                .send_to(&send_request(request_id), node_address)
                .await
                .unwrap();
        }

        let mut saw_busy = false;
        let mut saw_reply = false;
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut buf = vec![0u8; 16384];
        while Instant::now() < deadline && !(saw_busy && saw_reply) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Ok(Ok((size, _))) =
                tokio::time::timeout(remaining, client.recv_from(&mut buf)).await
            else {
                break;
            };
            match serde_json::from_slice::<Msg>(&buf[..size]).unwrap() {
                Msg::Busy { .. } => saw_busy = true,
                Msg::Reply { .. } => saw_reply = true,
                _ => {}
            }
        }

        assert!(
            saw_busy,
            "excess request should receive an explicit Busy response"
        );
        assert!(saw_reply, "admitted work should still complete");
        assert_eq!(node.max_active_requests.load(Relaxed), 1);
        assert!(node.active_requests.load(Relaxed) <= 1);
    }

    #[tokio::test]
    async fn cached_retry_preserves_original_replication_status() {
        let reservations: Vec<StdUdpSocket> = (0..3)
            .map(|_| StdUdpSocket::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers: Vec<_> = reservations
            .iter()
            .enumerate()
            .map(|(index, socket)| (index as u32 + 1, socket.local_addr().unwrap()))
            .collect();
        drop(reservations);
        let mut cfg = Config::from_env();
        cfg.repl_timeout = Duration::from_millis(10);
        cfg.request_queue_capacity = 4;
        let node = Node::new(1, peers.clone(), Arc::new(ImmediateElection), Arc::new(cfg)).await;
        node.spawn_loops();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reply_to = client.local_addr().unwrap();
        let bytes = serde_json::to_vec(&Msg::Request {
            req: test_request("under-replicated-retry"),
            reply_to,
        })
        .unwrap();
        client.send_to(&bytes, peers[0].1).await.unwrap();

        let mut buf = vec![0u8; 16384];
        let (size, _) = tokio::time::timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .expect("first request reply timed out")
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<Msg>(&buf[..size]).unwrap(),
            Msg::Reply {
                under_replicated: true,
                ..
            }
        ));

        client.send_to(&bytes, peers[0].1).await.unwrap();
        let (size, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .expect("cached retry reply timed out")
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<Msg>(&buf[..size]).unwrap(),
            Msg::Reply {
                under_replicated: true,
                ..
            }
        ));
        assert_eq!(node.started.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn concurrent_requests_keep_election_and_replication_correlated() {
        let reservations: Vec<StdUdpSocket> = (0..3)
            .map(|_| StdUdpSocket::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers: Vec<_> = reservations
            .iter()
            .enumerate()
            .map(|(index, socket)| (index as u32 + 1, socket.local_addr().unwrap()))
            .collect();
        drop(reservations);

        let mut cfg = Config::from_env();
        cfg.heartbeat = Duration::from_millis(50);
        cfg.fail_after = Duration::from_secs(1);
        cfg.t_ok = Duration::from_millis(100);
        cfg.t_coord = Duration::from_millis(250);
        cfg.exec_timeout = Duration::from_secs(2);
        cfg.repl_timeout = Duration::from_secs(1);
        cfg.repl_retry = Duration::from_millis(30);
        cfg.max_attempts = 3;
        cfg.max_concurrent_requests = 4;
        cfg.cpu_workers = 2;
        cfg.request_queue_capacity = 16;

        let mut nodes = Vec::new();
        for id in 1..=3 {
            let node = Node::new(
                id,
                peers.clone(),
                Arc::new(crate::election::LoadAwareBully::new()),
                Arc::new(cfg.clone()),
            )
            .await;
            node.spawn_loops();
            nodes.push(node);
        }
        sleep(Duration::from_millis(150)).await;

        let request_ids = ["cluster-request-a", "cluster-request-b"];
        let mut clients = Vec::new();
        for request_id in request_ids {
            let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let reply_to = client.local_addr().unwrap();
            let bytes = serde_json::to_vec(&Msg::Request {
                req: test_request(request_id),
                reply_to,
            })
            .unwrap();
            for (_, address) in &peers {
                client.send_to(&bytes, address).await.unwrap();
            }
            clients.push((request_id, client));
        }

        for (request_id, client) in &clients {
            let mut buf = vec![0u8; 16384];
            let (size, source) =
                tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut buf))
                    .await
                    .expect("client did not receive an elected node's reply")
                    .unwrap();
            let Msg::Reply {
                plan,
                under_replicated,
            } = serde_json::from_slice(&buf[..size]).unwrap()
            else {
                panic!("expected response plan");
            };
            assert_eq!(plan.request_id, *request_id);
            assert!(!under_replicated);
            let ack = serde_json::to_vec(&Msg::Ack {
                request_id: (*request_id).into(),
            })
            .unwrap();
            client.send_to(&ack, source).await.unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if nodes.iter().all(|node| {
                let plans = node.plans.lock().unwrap();
                request_ids
                    .iter()
                    .all(|request_id| plans.contains_key(*request_id))
            }) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        for node in &nodes {
            let plans = node.plans.lock().unwrap();
            for request_id in request_ids {
                assert!(
                    plans.contains_key(request_id),
                    "node {} missed {request_id}",
                    node.id
                );
            }
        }
    }
}

impl Node {
    pub async fn new(
        id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
        strategy: Arc<dyn ElectionStrategy>,
        cfg: Arc<Config>,
    ) -> Arc<Self> {
        let addr = peers
            .iter()
            .find(|(i, _)| *i == id)
            .expect("my id not in peer list")
            .1;
        let bind_fallback = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            addr.port(),
        );
        let sock = match UdpSocket::bind(addr).await {
            Ok(sock) => sock,
            Err(_) => UdpSocket::bind(bind_fallback)
                .await
                .expect("failed to bind to address or 0.0.0.0"),
        };
        let (request_tx, request_rx) = mpsc::channel(cfg.request_queue_capacity);
        let request_slots = Arc::new(Semaphore::new(cfg.max_concurrent_requests));
        let cpu_slots = Arc::new(Semaphore::new(cfg.cpu_workers));
        Arc::new(Node {
            id,
            peers,
            cfg,
            metrics: Metrics::new(),
            sock,
            strategy,
            muted: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            last_seen: Mutex::new(HashMap::new()),
            started: Mutex::new(HashSet::new()),
            in_progress: Mutex::new(HashMap::new()),
            plans: Mutex::new(HashMap::new()),
            plan_prec: Mutex::new(HashMap::new()),
            sync: Mutex::new(SyncProgress::default()),
            sync_notify: Notify::new(),
            acks: Mutex::new(HashMap::new()),
            ack_notify: Notify::new(),
            awaiting_ack: Mutex::new(HashMap::new()),
            request_tx,
            request_rx: Mutex::new(Some(request_rx)),
            request_slots,
            cpu_slots,
            active_requests: AtomicU64::new(0),
            max_active_requests: AtomicU64::new(0),
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
    /// False until the node has (re)learned liveness and synced state; the election
    /// keeps a non-ready node from winning so it never acts on stale data.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Relaxed)
    }

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
    /// The node counts as sufficiently current once it has received all parts of one
    /// peer's complete snapshot (the peer states the part count in each reply).
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

    // ---------- background loops ----------
    pub fn spawn_loops(self: &Arc<Self>) {
        self.metrics.spawn_sampler(
            self.cfg.sample_every,
            self.cfg.disk_cap_bps,
            self.cfg.net_cap_bps,
        );
        let mut requests = self
            .request_rx
            .lock()
            .unwrap()
            .take()
            .expect("node loops already started");
        let worker_node = self.clone();
        tokio::spawn(async move {
            let mut workers = tokio::task::JoinSet::new();
            let mut worker_ids: HashMap<tokio::task::Id, String> = HashMap::new();
            let mut requests_open = true;
            loop {
                if !requests_open && workers.is_empty() {
                    break;
                }
                tokio::select! {
                    completed = workers.join_next_with_id(), if !workers.is_empty() => {
                        match completed {
                            Some(Ok((task_id, ()))) => {
                                worker_ids.remove(&task_id);
                            }
                            Some(Err(error)) => {
                                if let Some(request_id) = worker_ids.remove(&error.id()) {
                                    eprintln!("[node {}] request task failed: {error}", worker_node.id);
                                    worker_node.fail_request(&request_id).await;
                                } else {
                                    eprintln!("[node {}] request task failed without a request ID: {error}", worker_node.id);
                                }
                            }
                            None => {}
                        }
                    }
                    queued = requests.recv(), if requests_open => {
                        let Some(queued) = queued else {
                            requests_open = false;
                            continue;
                        };
                        let permit = match worker_node.request_slots.clone().acquire_owned().await {
                            Ok(permit) => permit,
                            Err(error) => {
                                eprintln!("[node {}] request worker stopped before acquiring capacity: {error}", worker_node.id);
                                worker_node.fail_request(&queued.req.request_id).await;
                                break;
                            }
                        };
                        let request_id = queued.req.request_id.clone();
                        let node = worker_node.clone();
                        let task = workers.spawn(async move {
                            let _permit = permit;
                            node.process(queued).await;
                        });
                        worker_ids.insert(task.id(), request_id);
                    }
                }
            }
        });

        let r = self.clone();
        let (incoming_tx, mut incoming_rx) = mpsc::channel(self.cfg.request_queue_capacity);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16384];
            loop {
                let Ok((n, source)) = r.sock.recv_from(&mut buf).await else {
                    continue;
                };
                if r.is_muted() {
                    continue; // simulated crash: ignore everything
                }
                if let Ok(m) = serde_json::from_slice::<Msg>(&buf[..n]) {
                    if incoming_tx.send((m, source)).await.is_err() {
                        eprintln!("[node {}] inbound dispatcher stopped", r.id);
                        break;
                    }
                }
            }
        });
        let dispatch_node = self.clone();
        tokio::spawn(async move {
            while let Some((msg, source)) = incoming_rx.recv().await {
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
                    let dead: Vec<String> = m
                        .iter()
                        .filter(|(_, (_, t))| t.elapsed() > a.cfg.ack_timeout)
                        .map(|(k, _)| k.clone())
                        .collect();
                    dead.iter()
                        .filter_map(|k| m.remove(k))
                        .map(|(c, _)| c)
                        .collect()
                };
                for c in expired {
                    a.metrics.record_ack_timeout(&c);
                }
            }
        });
        // Startup: not eligible to win elections until peers are heard and state is synced.
        let w = self.clone();
        tokio::spawn(async move {
            w.begin_warmup().await;
        });
    }

    pub async fn handle(self: &Arc<Self>, m: Msg) {
        if let Some(s) = m.sender() {
            self.last_seen.lock().unwrap().insert(s, Instant::now());
        }
        match m {
            Msg::Heartbeat { .. } | Msg::Reply { .. } => {}
            Msg::Request { req, reply_to } => {
                let waiter = (reply_to, req.reporter_id.clone());
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
                // Idempotent retry: return cached results only after the previous run
                // has completed its replication phase and released its active entry.
                let known = self.plans.lock().unwrap().get(&req.request_id).cloned();
                if let Some((plan, under_replicated)) = known {
                    self.send_reply(reply_to, &plan, under_replicated, &req.reporter_id)
                        .await;
                    return;
                }
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
                    // A previous request may have finished between the cache lookup and
                    // reservation. Its result has reached quorum before it is removable.
                    let completed = self.plans.lock().unwrap().get(&req.request_id).cloned();
                    if let Some((plan, under_replicated)) = completed {
                        let _ = self.in_progress.lock().unwrap().remove(&req.request_id);
                        self.send_reply(reply_to, &plan, under_replicated, &req.reporter_id)
                            .await;
                        return;
                    }
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
            Msg::Ack { request_id } => {
                let hit = self.awaiting_ack.lock().unwrap().remove(&request_id);
                if let Some((client, t0)) = hit {
                    self.metrics
                        .record_ack(&client, t0.elapsed().as_secs_f64() * 1000.0);
                }
            }
            Msg::Replicate {
                plan,
                attempt,
                from,
            } => {
                let id = plan.request_id.clone();
                // Duplicate executions of one request: keep the plan with the higher
                // (attempt, lower node id) precedence; always ack so the sender can finish.
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
            Msg::SyncRequest { from } => self.serve_sync(from).await,
            Msg::SyncReply {
                from,
                part,
                parts,
                entries,
            } => {
                for e in entries {
                    self.store_plan(e.plan, e.under_replicated, (e.attempt, e.origin));
                }
                {
                    let mut s = self.sync.lock().unwrap();
                    if s.from == Some(from) {
                        s.parts = Some(parts);
                        s.got.insert(part);
                    }
                }
                self.sync_notify.notify_waiters();
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
        self.ready.store(false, Relaxed); // not eligible until it has resynced
        self.muted.store(true, Relaxed);
        println!("[node {}] MUTED for {secs}s", self.id);
        let e = self.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(secs)).await;
            e.strategy.reset(); // election state is volatile
            e.last_seen.lock().unwrap().clear(); // re-learn liveness from fresh heartbeats
            e.muted.store(false, Relaxed);
            e.begin_warmup().await;
            println!("[node {}] REJOINED (synced, ready)", e.id);
        });
    }

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
                // CSV line for the experiment scripts.
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
                // Idempotency: never execute the same request_id twice on this node.
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
                        eprintln!(
                            "[node {}] CPU worker failed for request {id}: {error}",
                            self.id
                        );
                        self.started.lock().unwrap().remove(&id);
                        continue;
                    }
                };
                let processing_elapsed = t0.elapsed();
                self.metrics
                    .record_service(processing_elapsed.as_secs_f64() * 1000.0);
                if self.is_muted() {
                    break; // crashed mid-execution: result is lost, others will re-elect
                }
                let replication_started = Instant::now();
                let replicated = self.replicate(&plan, out.attempt).await; // mandatory
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

            // The election could not identify a winner (e.g. this node is not ready yet).
            if out.winner == NO_WINNER {
                println!("[node {}] election for {id} abstained, retrying", self.id);
                sleep(self.cfg.t_ok).await;
                continue;
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
        // Release: per-request coordinator state is dropped; there is no standing leader role.
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

    async fn fail_request(&self, request_id: &str) {
        self.strategy.forget(request_id);
        self.started.lock().unwrap().remove(request_id);
        let waiters = self
            .in_progress
            .lock()
            .unwrap()
            .remove(request_id)
            .unwrap_or_default();
        self.metrics.queue_dec();
        for (to, _) in waiters {
            self.send_busy(to, request_id).await;
        }
    }

    /// Send the plan to the live peers and wait until a MAJORITY of the cluster (incl. us)
    /// holds it. Un-acked peers are retried. Returns false if quorum was not reached.
    async fn replicate(&self, plan: &ResponsePlan, attempt: u32) -> bool {
        let id = plan.request_id.clone();
        let need = (self.peers.len() / 2 + 1).saturating_sub(1); // peer acks needed
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
            let targets: Vec<NodeId> = {
                let a = self.acks.lock().unwrap();
                let got = a.get(&id);
                self.alive_peers()
                    .into_iter()
                    .filter(|p| got.map_or(true, |g| !g.contains(p)))
                    .collect()
            };
            if targets.is_empty() && count(self) < need {
                break false; // nobody left to ask
            }
            for t in targets {
                self.send_to(t, &msg).await;
            }
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

    async fn send_reply(&self, to: SocketAddr, plan: &ResponsePlan, under: bool, client: &str) {
        if self.is_muted() {
            return;
        }
        self.send_addr(
            to,
            &Msg::Reply {
                plan: plan.clone(),
                under_replicated: under,
            },
        )
        .await;
        self.awaiting_ack.lock().unwrap().insert(
            plan.request_id.clone(),
            (client.to_string(), Instant::now()),
        );
    }

    /// Placeholder for Service 1 (priority + response plan). Replace with the real planner.
    async fn execute(&self, req: IncidentRequest) -> Result<ResponsePlan, tokio::task::JoinError> {
        sleep(Duration::from_millis(300)).await;
        let permit = self
            .cpu_slots
            .clone()
            .acquire_owned()
            .await
            .expect("CPU worker pool closed");
        let node_id = self.id;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let age_s = now.saturating_sub(req.timestamp_ms) as f64 / 1000.0;
            ResponsePlan {
                request_id: req.request_id,
                incident_id: req.incident_id,
                priority_score: req.severity as f64 * 10.0 + age_s.min(600.0) * 0.1,
                assigned_responders: vec![],
                assigned_resources: vec![],
                recommended_route: vec![req.location_zone],
                processed_by_node: node_id,
            }
        })
        .await
    }

    async fn send_busy(&self, to: SocketAddr, request_id: &str) {
        self.send_addr(
            to,
            &Msg::Busy {
                request_id: request_id.to_string(),
                node_id: self.id,
            },
        )
        .await;
    }

    pub fn stats(&self) -> String {
        format!(
            "ready={} alive={:?} elections={} dup_coordinators={} msgs_sent={} plans={} under_replicated={} active_requests={} max_active_requests={} | {}",
            self.is_ready(),
            self.alive_nodes(),
            self.elections.load(Relaxed),
            self.strategy.duplicate_coordinators(),
            self.msgs_sent.load(Relaxed),
            self.plans.lock().unwrap().len(),
            self.under_replicated.load(Relaxed),
            self.active_requests.load(Relaxed),
            self.max_active_requests.load(Relaxed),
            self.metrics.describe()
        )
    }
}
