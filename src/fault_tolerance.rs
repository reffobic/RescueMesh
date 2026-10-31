use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::types::ResponsePlan;

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

#[derive(Debug, Clone)]
enum IdempotencyRecord {
    InProgress { last_touched: Instant },
    Completed(LogEntry),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IdempotencyOutcome {
    /// Caller acquired the lock and should now compute the ResponsePlan
    Acquired,
    /// Request was already completed and replicated; return this cached plan immediately
    Cached(ResponsePlan),
    /// Another worker on this node is actively computing this request right now
    InProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultError {
    NodeUnavailable,
    DuplicateInProgress,
    ReplicationQuorumFailed,
    ConvergenceTimeout,
    PeerRpcError(String),
}

/// Abstract peer transport so Point 1 (Haya) can plug in any RPC framework
#[async_trait::async_trait]
pub trait PeerReplicationTransport: Send + Sync {
    /// Sends a newly created LogEntry to all other peers; returns number of ACKs received
    async fn broadcast_entry(&self, entry: &LogEntry) -> usize;

    /// Lightweight query: fetches `(peer_id, peer_version_vector)` from all reachable peers
    async fn fetch_peer_versions(&self) -> Result<Vec<(u32, VersionVector)>, FaultError>;

    /// Syncs with a specific peer: sends only `delta_for_peer` (entries newer than peer's VV)
    /// and receives entries we are missing (newer than `our_vv`), plus the peer's updated VV.
    async fn sync_with_peer(
        &self,
        peer_id: u32,
        our_vv: &VersionVector,
        delta_for_peer: Vec<LogEntry>,
    ) -> Result<(VersionVector, Vec<LogEntry>), FaultError>;
}

struct InternalState {
    health: NodeHealthState,
    local_seq: u64,
    /// Contiguous version vector: origin_node -> highest contiguous seq applied
    version_vector: VersionVector,
    /// Ordered committed log per origin: origin_node -> (seq -> LogEntry)
    committed_logs: HashMap<u32, BTreeMap<u64, LogEntry>>,
    /// Buffer for out-of-order entries: origin_node -> (seq -> LogEntry)
    pending_out_of_order: HashMap<u32, BTreeMap<u64, LogEntry>>,
    /// Maps request_id -> IdempotencyRecord
    idempotency_map: HashMap<String, IdempotencyRecord>,
    // --- Measurement bookkeeping (spec: failover / recovery / convergence time) ---
    failure_injected_at: Option<Instant>,
    recovery_started_at: Option<Instant>,
    recovery_rounds: u32,
    entries_sent: u64,
    entries_received: u64,
    last_recovery: Option<RecoveryMetrics>,
}

/// Measurements for the most recent Failed -> Recovering -> Active cycle of this node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveryMetrics {
    /// Failure injection -> `Recovering` started (≈ configured failure duration)
    pub failed_for_ms: u64,
    /// `Recovering` started -> convergence finished (state transfer time)
    pub convergence_ms: u64,
    /// Failure injection -> back to `Active` (total unavailability)
    pub total_downtime_ms: u64,
    pub sync_rounds: u32,
    /// Entries newly committed locally during recovery
    pub entries_received: u64,
    /// Entries (delta) we pushed to peers during recovery
    pub entries_sent: u64,
}

/// Emits one structured JSON line on stderr: `METRIC {"event":..,"node":..,"ts_ms":..,<fields>}`.
///
/// The timestamp is taken at the call site, but the line is handed to a dedicated logger thread
/// through an unbounded channel, so callers holding the manager lock never block on stderr I/O.
/// Values are escaped by `serde_json`.
///
/// `ts_ms` is the local wall clock (Unix epoch ms). Comparing it across machines includes clock
/// offset, so for failover time prefer client-side measurement (or sync clocks and report offset).
fn emit_metric(node: u32, event: &str, fields: serde_json::Value) {
    static TX: OnceLock<mpsc::Sender<String>> = OnceLock::new();
    let tx = TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            for line in rx {
                eprintln!("METRIC {}", line);
            }
        });
        tx
    });

    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut obj = serde_json::json!({ "event": event, "node": node, "ts_ms": ts_ms });
    if let (Some(o), serde_json::Value::Object(extra)) = (obj.as_object_mut(), fields) {
        o.extend(extra);
    }
    let _ = tx.send(obj.to_string());
}

pub struct FaultToleranceManager {
    node_id: u32,
    in_progress_ttl: Duration,
    state: RwLock<InternalState>,
}

impl FaultToleranceManager {
    /// Recommended `in_progress_ttl` is >= 30s so stress tests never prematurely expire active work
    pub fn new(node_id: u32, all_node_ids: &[u32], in_progress_ttl: Duration) -> Arc<Self> {
        let mut vv = HashMap::new();
        for &id in all_node_ids {
            vv.insert(id, 0);
        }

        Arc::new(Self {
            node_id,
            in_progress_ttl,
            state: RwLock::new(InternalState {
                health: NodeHealthState::Active,
                local_seq: 0,
                version_vector: vv,
                committed_logs: HashMap::new(),
                pending_out_of_order: HashMap::new(),
                idempotency_map: HashMap::new(),
                failure_injected_at: None,
                recovery_started_at: None,
                recovery_rounds: 0,
                entries_sent: 0,
                entries_received: 0,
                last_recovery: None,
            }),
        })
    }

    /// Spawns the background task that monitors failure timers and drives state recovery until convergence
    pub fn spawn_recovery_loop(
        self: &Arc<Self>,
        transport: Arc<dyn PeerReplicationTransport>,
        poll_interval: Duration,
    ) -> JoinHandle<()> {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll_interval);
            loop {
                ticker.tick().await;

                // 1. Transition Failed -> Recovering if the 20s timer expired
                let _ = mgr.begin_recovery_if_timer_expired().await;

                // 2. If currently Recovering, drive catch-up; if it times out, stay Recovering and retry next tick
                let is_recovering = {
                    let st = mgr.state.read().await;
                    matches!(st.health, NodeHealthState::Recovering)
                };

                if is_recovering {
                    let _ = mgr.recover_state_from_peers(transport.as_ref()).await;
                }
            }
        })
    }

    /// Gate for client requests and workload elections: ONLY true when Active
    pub async fn accepts_client_work(&self) -> bool {
        let st = self.state.read().await;
        matches!(st.health, NodeHealthState::Active)
    }

    /// Gate for peer replication and catch-up RPCs: true when Active OR Recovering
    pub async fn accepts_peer_traffic(&self) -> bool {
        let st = self.state.read().await;
        matches!(
            st.health,
            NodeHealthState::Active | NodeHealthState::Recovering
        )
    }

    /// Point 4: Triggered when the 3 servers elect this node to simulate failure (clamped to 1..=20s)
    pub async fn on_elected_to_fail(&self, duration_secs: u64) {
        let clamped_secs = duration_secs.clamp(1, 20);
        let recover_at = Instant::now() + Duration::from_secs(clamped_secs);

        let mut st = self.state.write().await;
        st.health = NodeHealthState::Failed { recover_at };
        st.failure_injected_at = Some(Instant::now());
        emit_metric(
            self.node_id,
            "failure_injected",
            serde_json::json!({ "duration_secs": clamped_secs }),
        );

        // Clear all uncommitted InProgress locks so retries after recovery aren't blocked
        st.idempotency_map
            .retain(|_, v| matches!(v, IdempotencyRecord::Completed(_)));
    }

    /// Transitions node from Failed -> Recovering once the failure timer expires
    pub async fn begin_recovery_if_timer_expired(&self) -> bool {
        let mut st = self.state.write().await;
        if let NodeHealthState::Failed { recover_at } = st.health {
            if Instant::now() >= recover_at {
                st.health = NodeHealthState::Recovering;
                st.recovery_started_at = Some(Instant::now());
                st.recovery_rounds = 0;
                st.entries_sent = 0;
                st.entries_received = 0;
                emit_metric(self.node_id, "recovery_started", serde_json::json!({}));
                return true;
            }
        }
        false
    }

    /// Point 4: Atomic idempotency check before computing a request.
    pub async fn begin_request(&self, request_id: &str) -> Result<IdempotencyOutcome, FaultError> {
        let mut st = self.state.write().await;
        if !matches!(st.health, NodeHealthState::Active) {
            return Err(FaultError::NodeUnavailable);
        }

        let now = Instant::now();
        if let Some(existing) = st.idempotency_map.get(request_id) {
            match existing {
                IdempotencyRecord::Completed(entry) => {
                    if let Some(plan) = &entry.plan {
                        return Ok(IdempotencyOutcome::Cached(plan.clone()));
                    }
                }
                IdempotencyRecord::InProgress { last_touched } => {
                    if now.duration_since(*last_touched) < self.in_progress_ttl {
                        return Ok(IdempotencyOutcome::InProgress);
                    }
                }
            }
        }

        st.idempotency_map.insert(
            request_id.to_string(),
            IdempotencyRecord::InProgress { last_touched: now },
        );
        Ok(IdempotencyOutcome::Acquired)
    }

    /// Renews the `InProgress` lease while a long computation is still actively running
    pub async fn touch_in_progress(&self, request_id: &str) {
        let mut st = self.state.write().await;
        if let Some(IdempotencyRecord::InProgress { last_touched }) =
            st.idempotency_map.get_mut(request_id)
        {
            *last_touched = Instant::now();
        }
    }

    /// Clears an InProgress marker if the worker fails/aborts during computation
    pub async fn abort_request(&self, request_id: &str) {
        let mut st = self.state.write().await;
        if matches!(
            st.idempotency_map.get(request_id),
            Some(IdempotencyRecord::InProgress { .. })
        ) {
            st.idempotency_map.remove(request_id);
        }
    }

    /// Points 4 & 5: Replicates to peers FIRST; commits locally and caches ONLY if >= 1 peer ACKs.
    ///
    /// A sequence number is NEVER reused or abandoned. If replication gets zero ACKs, a tombstone
    /// (`plan: None`) is committed for that seq so the contiguous version vector keeps advancing
    /// and later seqs (possibly already replicated) are not stuck behind a gap. The tombstone is
    /// also broadcast best-effort; peers that had applied the real entry replace it with the tombstone.
    ///
    /// If the node is failed while waiting for ACKs, the already-replicated entry is deliberately
    /// committed locally (peers hold it, so the log must stay consistent) but the caller gets
    /// `NodeUnavailable`; a retry after recovery receives the cached plan.
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
            // Best-effort: tell peers this seq is void (acks intentionally ignored)
            let _ = transport.broadcast_entry(&tombstone).await;
            return Err(FaultError::ReplicationQuorumFailed);
        }

        // 3. Replicated on >= 1 peer: commit locally. Re-check health, since the node may have been
        // injected with a failure while waiting for ACKs.
        let mut st = self.state.write().await;
        let still_active = matches!(st.health, NodeHealthState::Active);
        Self::apply_entry_locked(&mut st, candidate_entry.clone());
        if !still_active {
            return Err(FaultError::NodeUnavailable);
        }
        emit_metric(
            self.node_id,
            "request_served",
            serde_json::json!({
                "seq": candidate_entry.seq,
                "request_id": candidate_entry.request_id,
            }),
        );
        Ok(candidate_entry)
    }

    /// Applies an incoming replicated LogEntry from a peer (allowed when Active or Recovering)
    pub async fn on_receive_replicated_entry(&self, entry: LogEntry) -> Result<(), FaultError> {
        let mut st = self.state.write().await;
        if !matches!(
            st.health,
            NodeHealthState::Active | NodeHealthState::Recovering
        ) {
            return Err(FaultError::NodeUnavailable);
        }
        Self::apply_entry_locked(&mut st, entry);
        Ok(())
    }

    /// Point 5 RPC handler: Applies `incoming_delta` from a peer and returns `(our_vv, missing_for_peer)`
    pub async fn handle_peer_sync(
        &self,
        peer_vv: &VersionVector,
        incoming_delta: Vec<LogEntry>,
    ) -> Result<(VersionVector, Vec<LogEntry>), FaultError> {
        let mut st = self.state.write().await;
        if !matches!(
            st.health,
            NodeHealthState::Active | NodeHealthState::Recovering
        ) {
            return Err(FaultError::NodeUnavailable);
        }

        for entry in incoming_delta {
            Self::apply_entry_locked(&mut st, entry);
        }

        let missing_for_peer = Self::collect_entries_since_locked(&st, peer_vv);
        Ok((st.version_vector.clone(), missing_for_peer))
    }

    /// Point 5: Delta-only bidirectional catch-up with inter-round backoff until VersionVector converges
    pub async fn recover_state_from_peers(
        &self,
        transport: &dyn PeerReplicationTransport,
    ) -> Result<(), FaultError> {
        const MAX_SYNC_ROUNDS: usize = 5;
        const ROUND_BACKOFF: Duration = Duration::from_millis(150);

        for round in 0..MAX_SYNC_ROUNDS {
            if round > 0 {
                tokio::time::sleep(ROUND_BACKOFF).await;
            }

            {
                let mut st = self.state.write().await;
                if !matches!(st.health, NodeHealthState::Recovering) {
                    return Err(FaultError::NodeUnavailable);
                }
                st.recovery_rounds += 1;
            }

            // 1. Fetch lightweight VersionVectors from all reachable peers
            let peer_vvs = transport.fetch_peer_versions().await?;
            if peer_vvs.is_empty() {
                continue;
            }

            let mut latest_peer_vvs = Vec::with_capacity(peer_vvs.len());

            // 2. For each peer, send ONLY the delta entries that specific peer is missing
            for (peer_id, peer_vv) in peer_vvs {
                let (our_vv, delta_for_peer) = {
                    let st = self.state.read().await;
                    (
                        st.version_vector.clone(),
                        Self::collect_entries_since_locked(&st, &peer_vv),
                    )
                };

                let sent = delta_for_peer.len() as u64;
                if let Ok((updated_peer_vv, missing_from_peer)) = transport
                    .sync_with_peer(peer_id, &our_vv, delta_for_peer)
                    .await
                {
                    let mut st = self.state.write().await;
                    let before = Self::committed_count(&st);
                    for entry in missing_from_peer {
                        Self::apply_entry_locked(&mut st, entry);
                    }
                    st.entries_sent += sent;
                    st.entries_received += Self::committed_count(&st) - before;
                    latest_peer_vvs.push(updated_peer_vv);
                }
            }

            // 3. Verify convergence: our contiguous VV must be >= every reachable peer's updated VV
            let mut st = self.state.write().await;
            let is_converged = !latest_peer_vvs.is_empty()
                && latest_peer_vvs.iter().all(|peer_vv| {
                    peer_vv.iter().all(|(origin, &peer_seq)| {
                        st.version_vector.get(origin).copied().unwrap_or(0) >= peer_seq
                    })
                });

            if is_converged {
                // Restore local_seq from version_vector[node_id] in case state was reconstructed
                let recovered_own_seq = st.version_vector.get(&self.node_id).copied().unwrap_or(0);
                if recovered_own_seq > st.local_seq {
                    st.local_seq = recovered_own_seq;
                }
                st.health = NodeHealthState::Active;

                let now = Instant::now();
                let ms = |from: Option<Instant>| {
                    from.map(|t| now.duration_since(t).as_millis() as u64)
                        .unwrap_or(0)
                };
                let convergence_ms = ms(st.recovery_started_at);
                let total_downtime_ms = ms(st.failure_injected_at);
                let metrics = RecoveryMetrics {
                    failed_for_ms: total_downtime_ms.saturating_sub(convergence_ms),
                    convergence_ms,
                    total_downtime_ms,
                    sync_rounds: st.recovery_rounds,
                    entries_received: st.entries_received,
                    entries_sent: st.entries_sent,
                };
                emit_metric(
                    self.node_id,
                    "recovery_converged",
                    serde_json::json!({
                        "failed_for_ms": metrics.failed_for_ms,
                        "convergence_ms": metrics.convergence_ms,
                        "total_downtime_ms": metrics.total_downtime_ms,
                        "sync_rounds": metrics.sync_rounds,
                        "entries_received": metrics.entries_received,
                        "entries_sent": metrics.entries_sent,
                    }),
                );
                st.last_recovery = Some(metrics);
                return Ok(());
            }
        }

        Err(FaultError::ConvergenceTimeout)
    }

    pub async fn get_version_vector(&self) -> VersionVector {
        self.state.read().await.version_vector.clone()
    }

    /// Measurements from this node's most recent completed recovery (None if it never recovered)
    pub async fn last_recovery_metrics(&self) -> Option<RecoveryMetrics> {
        self.state.read().await.last_recovery.clone()
    }

    fn committed_count(st: &InternalState) -> u64 {
        st.committed_logs.values().map(|m| m.len() as u64).sum()
    }

    // --- Internal helpers ---

    fn collect_entries_since_locked(st: &InternalState, peer_vv: &VersionVector) -> Vec<LogEntry> {
        let mut out = Vec::new();
        for (&origin, log_map) in &st.committed_logs {
            let seen_seq = peer_vv.get(&origin).copied().unwrap_or(0);
            for (_seq, entry) in log_map.range((seen_seq + 1)..) {
                out.push(entry.clone());
            }
        }
        out
    }

    fn apply_entry_locked(st: &mut InternalState, entry: LogEntry) {
        let origin = entry.origin_node;
        let current_contig = st.version_vector.get(&origin).copied().unwrap_or(0);

        if entry.seq <= current_contig {
            // A tombstone from the origin overrides a real entry we applied for the same seq
            // (origin's ACK was lost, origin voided the seq).
            if entry.plan.is_none() {
                let replaced = st
                    .committed_logs
                    .get_mut(&origin)
                    .and_then(|m| m.get_mut(&entry.seq))
                    .filter(|existing| existing.plan.is_some())
                    .map(|existing| {
                        let old = (
                            existing.request_id.clone(),
                            existing.origin_node,
                            existing.seq,
                        );
                        *existing = entry;
                        old
                    });
                if let Some((rid, o, s)) = replaced {
                    let was_winner = matches!(
                        st.idempotency_map.get(&rid),
                        Some(IdempotencyRecord::Completed(w)) if (w.origin_node, w.seq) == (o, s)
                    );
                    if was_winner {
                        st.idempotency_map.remove(&rid);
                    }
                }
            }
            return;
        }

        let buf = st.pending_out_of_order.entry(origin).or_default();
        // A buffered tombstone must never be replaced by a delayed real entry for the same seq
        if matches!(buf.get(&entry.seq), Some(b) if b.plan.is_none()) && entry.plan.is_some() {
            return;
        }
        buf.insert(entry.seq, entry);

        let mut next_expected = current_contig + 1;
        while let Some(next_entry) = st
            .pending_out_of_order
            .get_mut(&origin)
            .and_then(|buf| buf.remove(&next_expected))
        {
            // Deterministic tie-breaker for which cached plan wins if two nodes committed plans for
            // the same request_id. NOTE: this does not prevent both nodes from computing and
            // returning different plans; it only decides what is cached afterwards.
            // Tombstones never touch the idempotency map.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockClusterTransport {
        self_id: u32,
        peers: HashMap<u32, Arc<FaultToleranceManager>>,
    }

    #[async_trait::async_trait]
    impl PeerReplicationTransport for MockClusterTransport {
        async fn broadcast_entry(&self, entry: &LogEntry) -> usize {
            let mut acks = 0;
            for (&id, peer) in &self.peers {
                if id != self.self_id && peer.accepts_peer_traffic().await {
                    if peer
                        .on_receive_replicated_entry(entry.clone())
                        .await
                        .is_ok()
                    {
                        acks += 1;
                    }
                }
            }
            acks
        }

        async fn fetch_peer_versions(&self) -> Result<Vec<(u32, VersionVector)>, FaultError> {
            let mut out = Vec::new();
            for (&id, peer) in &self.peers {
                if id != self.self_id && peer.accepts_peer_traffic().await {
                    out.push((id, peer.get_version_vector().await));
                }
            }
            Ok(out)
        }

        async fn sync_with_peer(
            &self,
            peer_id: u32,
            our_vv: &VersionVector,
            delta_for_peer: Vec<LogEntry>,
        ) -> Result<(VersionVector, Vec<LogEntry>), FaultError> {
            let peer = self
                .peers
                .get(&peer_id)
                .ok_or_else(|| FaultError::PeerRpcError("Peer not found".into()))?;
            peer.handle_peer_sync(our_vv, delta_for_peer).await
        }
    }

    fn dummy_plan(req_id: &str, node: u32) -> ResponsePlan {
        ResponsePlan {
            request_id: req_id.to_string(),
            incident_id: format!("inc-{}", req_id),
            priority_score: 9.5,
            assigned_responders: vec!["resp-1".into()],
            assigned_resources: vec!["amb-1".into()],
            recommended_route: vec![1, 2],
            processed_by_node: node,
        }
    }

    fn make_entry(origin: u32, seq: u64, req: &str, plan: Option<ResponsePlan>) -> LogEntry {
        LogEntry {
            origin_node: origin,
            seq,
            request_id: req.to_string(),
            plan,
            committed_at_ms: 0,
        }
    }

    /// Wraps a mock transport; real (non-tombstone) entries for `fail_request_id` get zero ACKs.
    struct SelectiveFailTransport {
        inner: MockClusterTransport,
        fail_request_id: String,
    }

    #[async_trait::async_trait]
    impl PeerReplicationTransport for SelectiveFailTransport {
        async fn broadcast_entry(&self, entry: &LogEntry) -> usize {
            if entry.request_id == self.fail_request_id && entry.plan.is_some() {
                // Simulate a delay so concurrent requests interleave
                tokio::time::sleep(Duration::from_millis(20)).await;
                return 0;
            }
            self.inner.broadcast_entry(entry).await
        }
        async fn fetch_peer_versions(&self) -> Result<Vec<(u32, VersionVector)>, FaultError> {
            self.inner.fetch_peer_versions().await
        }
        async fn sync_with_peer(
            &self,
            peer_id: u32,
            our_vv: &VersionVector,
            delta_for_peer: Vec<LogEntry>,
        ) -> Result<(VersionVector, Vec<LogEntry>), FaultError> {
            self.inner
                .sync_with_peer(peer_id, our_vv, delta_for_peer)
                .await
        }
    }

    #[tokio::test]
    async fn test_out_of_order_arrival_buffers_until_gap_filled() {
        let n = FaultToleranceManager::new(2, &[1, 2, 3], Duration::from_secs(30));

        n.on_receive_replicated_entry(make_entry(1, 3, "r3", Some(dummy_plan("r3", 1))))
            .await
            .unwrap();
        n.on_receive_replicated_entry(make_entry(1, 2, "r2", Some(dummy_plan("r2", 1))))
            .await
            .unwrap();
        // Gap at seq 1: nothing is contiguous and nothing is cached yet
        assert_eq!(n.get_version_vector().await.get(&1), Some(&0));
        assert_eq!(
            n.begin_request("r3").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
        n.abort_request("r3").await;

        // Filling the gap releases the whole buffered run
        n.on_receive_replicated_entry(make_entry(1, 1, "r1", Some(dummy_plan("r1", 1))))
            .await
            .unwrap();
        assert_eq!(n.get_version_vector().await.get(&1), Some(&3));
        assert_eq!(
            n.begin_request("r3").await.unwrap(),
            IdempotencyOutcome::Cached(dummy_plan("r3", 1))
        );

        // Duplicate delivery is ignored
        n.on_receive_replicated_entry(make_entry(1, 2, "r2", Some(dummy_plan("r2", 1))))
            .await
            .unwrap();
        assert_eq!(n.get_version_vector().await.get(&1), Some(&3));
    }

    #[tokio::test]
    async fn test_tombstone_overrides_applied_entry() {
        let n = FaultToleranceManager::new(2, &[1, 2, 3], Duration::from_secs(30));
        n.on_receive_replicated_entry(make_entry(1, 1, "r1", Some(dummy_plan("r1", 1))))
            .await
            .unwrap();
        // Origin voided seq 1 (it never saw our ACK)
        n.on_receive_replicated_entry(make_entry(1, 1, "r1", None))
            .await
            .unwrap();
        assert_eq!(n.get_version_vector().await.get(&1), Some(&1));
        assert_eq!(
            n.begin_request("r1").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
    }

    #[tokio::test]
    async fn test_buffered_tombstone_not_overwritten_by_late_real_entry() {
        let n = FaultToleranceManager::new(2, &[1, 2, 3], Duration::from_secs(30));
        // seq 2 arrives first as a tombstone, then a delayed real seq 2, then seq 1 fills the gap
        n.on_receive_replicated_entry(make_entry(1, 2, "r2", None))
            .await
            .unwrap();
        n.on_receive_replicated_entry(make_entry(1, 2, "r2", Some(dummy_plan("r2", 1))))
            .await
            .unwrap();
        n.on_receive_replicated_entry(make_entry(1, 1, "r1", Some(dummy_plan("r1", 1))))
            .await
            .unwrap();
        assert_eq!(n.get_version_vector().await.get(&1), Some(&2));
        // The tombstone won: r2 is not cached
        assert_eq!(
            n.begin_request("r2").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
    }
    #[tokio::test]
    async fn test_concurrent_requests_one_fails_replication_vv_advances() {
        let ids = [1, 2, 3];
        let n1 = FaultToleranceManager::new(1, &ids, Duration::from_secs(30));
        let n2 = FaultToleranceManager::new(2, &ids, Duration::from_secs(30));
        let n3 = FaultToleranceManager::new(3, &ids, Duration::from_secs(30));

        let mut map = HashMap::new();
        map.insert(1, Arc::clone(&n1));
        map.insert(2, Arc::clone(&n2));
        map.insert(3, Arc::clone(&n3));
        let t1 = SelectiveFailTransport {
            inner: MockClusterTransport {
                self_id: 1,
                peers: map,
            },
            fail_request_id: "bad".into(),
        };

        n1.begin_request("bad").await.unwrap();
        n1.begin_request("good").await.unwrap();

        let (bad, good) = tokio::join!(
            n1.record_and_replicate("bad".into(), dummy_plan("bad", 1), &t1),
            n1.record_and_replicate("good".into(), dummy_plan("good", 1), &t1),
        );
        assert_eq!(bad, Err(FaultError::ReplicationQuorumFailed));
        assert!(good.is_ok());

        // Both seqs were consumed on every node (one real, one tombstone): no stalled gap
        for n in [&n1, &n2, &n3] {
            assert_eq!(n.get_version_vector().await.get(&1), Some(&2));
        }
        // The good plan is cached everywhere; the failed one is retryable
        assert_eq!(
            n2.begin_request("good").await.unwrap(),
            IdempotencyOutcome::Cached(dummy_plan("good", 1))
        );
        assert_eq!(
            n1.begin_request("bad").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
    }

    #[tokio::test]
    async fn test_failure_during_replication_keeps_entry_but_returns_err() {
        let ids = [1, 2, 3];
        let n1 = FaultToleranceManager::new(1, &ids, Duration::from_secs(30));
        let n2 = FaultToleranceManager::new(2, &ids, Duration::from_secs(30));
        let n3 = FaultToleranceManager::new(3, &ids, Duration::from_secs(30));
        let mut map = HashMap::new();
        map.insert(1, Arc::clone(&n1));
        map.insert(2, Arc::clone(&n2));
        map.insert(3, Arc::clone(&n3));
        let t1 = MockClusterTransport {
            self_id: 1,
            peers: map,
        };

        // Fail node 1 while its request is in flight: simulate by failing right after broadcast
        struct FailAfter<'a> {
            inner: &'a MockClusterTransport,
            node: Arc<FaultToleranceManager>,
        }
        #[async_trait::async_trait]
        impl PeerReplicationTransport for FailAfter<'_> {
            async fn broadcast_entry(&self, entry: &LogEntry) -> usize {
                let acks = self.inner.broadcast_entry(entry).await;
                self.node.on_elected_to_fail(5).await;
                acks
            }
            async fn fetch_peer_versions(&self) -> Result<Vec<(u32, VersionVector)>, FaultError> {
                self.inner.fetch_peer_versions().await
            }
            async fn sync_with_peer(
                &self,
                p: u32,
                v: &VersionVector,
                d: Vec<LogEntry>,
            ) -> Result<(VersionVector, Vec<LogEntry>), FaultError> {
                self.inner.sync_with_peer(p, v, d).await
            }
        }
        let t = FailAfter {
            inner: &t1,
            node: Arc::clone(&n1),
        };

        n1.begin_request("req-F").await.unwrap();
        let res = n1
            .record_and_replicate("req-F".into(), dummy_plan("req-F", 1), &t)
            .await;
        assert_eq!(res, Err(FaultError::NodeUnavailable));
        // Peers hold the entry, so it stays committed locally and the vector is consistent
        assert_eq!(n1.get_version_vector().await.get(&1), Some(&1));
        assert_eq!(n2.get_version_vector().await.get(&1), Some(&1));
    }

    #[tokio::test]
    async fn test_three_node_failure_and_delta_recovery() {
        let ids = [1, 2, 3];
        let n1 = FaultToleranceManager::new(1, &ids, Duration::from_secs(30));
        let n2 = FaultToleranceManager::new(2, &ids, Duration::from_secs(30));
        let n3 = FaultToleranceManager::new(3, &ids, Duration::from_secs(30));

        let mut map = HashMap::new();
        map.insert(1, Arc::clone(&n1));
        map.insert(2, Arc::clone(&n2));
        map.insert(3, Arc::clone(&n3));

        let t1 = Arc::new(MockClusterTransport {
            self_id: 1,
            peers: map.clone(),
        });
        let t2 = Arc::new(MockClusterTransport {
            self_id: 2,
            peers: map.clone(),
        });
        let t3 = Arc::new(MockClusterTransport {
            self_id: 3,
            peers: map.clone(),
        });

        // 1. Elect Node 3 to fail for 1 second
        n3.on_elected_to_fail(1).await;
        assert!(!n3.accepts_client_work().await);
        assert!(!n3.accepts_peer_traffic().await);

        // 2. Nodes 1 and 2 process requests while Node 3 is down
        assert_eq!(
            n1.begin_request("req-A").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
        n1.record_and_replicate("req-A".into(), dummy_plan("req-A", 1), t1.as_ref())
            .await
            .unwrap();

        assert_eq!(
            n2.begin_request("req-B").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
        n2.record_and_replicate("req-B".into(), dummy_plan("req-B", 2), t2.as_ref())
            .await
            .unwrap();

        // Node 3 missed both updates
        assert_eq!(n3.get_version_vector().await.get(&1), Some(&0));
        assert_eq!(n3.get_version_vector().await.get(&2), Some(&0));

        // 3. Spawn background recovery loop on Node 3 and poll (with a generous deadline) for
        // the 1s failure timer + convergence, rather than relying on a fixed sleep margin
        let handle = n3.spawn_recovery_loop(t3, Duration::from_millis(50));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !n3.accepts_client_work().await && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(n3.accepts_client_work().await);
        assert_eq!(n3.get_version_vector().await.get(&1), Some(&1));
        assert_eq!(n3.get_version_vector().await.get(&2), Some(&1));
        assert_eq!(
            n3.begin_request("req-A").await.unwrap(),
            IdempotencyOutcome::Cached(dummy_plan("req-A", 1))
        );

        let m = n3.last_recovery_metrics().await.expect("recovery metrics");
        assert!(m.sync_rounds >= 1);
        assert_eq!(m.entries_received, 2);
        assert!(m.total_downtime_ms >= m.convergence_ms);

        handle.abort();
    }

    #[tokio::test]
    async fn test_rollback_when_zero_peer_acks() {
        let ids = [1, 2, 3];
        let n1 = FaultToleranceManager::new(1, &ids, Duration::from_secs(30));
        let n2 = FaultToleranceManager::new(2, &ids, Duration::from_secs(30));
        let n3 = FaultToleranceManager::new(3, &ids, Duration::from_secs(30));

        // Both peers fail simultaneously -> 0 ACKs
        n2.on_elected_to_fail(5).await;
        n3.on_elected_to_fail(5).await;

        let mut map = HashMap::new();
        map.insert(1, Arc::clone(&n1));
        map.insert(2, Arc::clone(&n2));
        map.insert(3, Arc::clone(&n3));
        let t1 = MockClusterTransport {
            self_id: 1,
            peers: map,
        };

        assert_eq!(
            n1.begin_request("req-X").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
        let res = n1
            .record_and_replicate("req-X".into(), dummy_plan("req-X", 1), &t1)
            .await;
        assert_eq!(res, Err(FaultError::ReplicationQuorumFailed));

        // Verify req-X was NOT cached, but its seq was consumed by a tombstone (never reused)
        assert_eq!(n1.get_version_vector().await.get(&1), Some(&1));
        assert_eq!(
            n1.begin_request("req-X").await.unwrap(),
            IdempotencyOutcome::Acquired
        );
    }
}
