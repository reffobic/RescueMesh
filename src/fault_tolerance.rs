// Fault tolerance interface (points 4 and 5).
//
// This is only the interface for now. The node always reports healthy and
// nothing is logged or replicated yet. The signatures below are final, the
// behaviour behind them gets filled in later.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

use crate::types::ResponsePlan;

/// origin node id -> highest contiguous sequence number applied from that node
pub type VersionVector = HashMap<u32, u64>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub origin_node: u32,
    pub seq: u64,
    pub request_id: String,
    /// None means the entry is a tombstone (the seq was used but no plan was kept)
    pub plan: Option<ResponsePlan>,
    pub committed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IdempotencyOutcome {
    /// Nobody has this request yet, go ahead and compute it
    Acquired,
    /// Already done, return this plan as is
    Cached(ResponsePlan),
    /// Another worker on this node is computing it right now
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

/// Numbers collected during one failure and recovery cycle of a node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveryMetrics {
    pub failed_for_ms: u64,
    pub convergence_ms: u64,
    pub total_downtime_ms: u64,
    pub sync_rounds: u32,
    pub entries_received: u64,
    pub entries_sent: u64,
}

/// To be implemented by the RPC layer
#[async_trait::async_trait]
pub trait PeerReplicationTransport: Send + Sync {
    /// Send a new entry to the other nodes, returns how many acknowledged it
    async fn broadcast_entry(&self, entry: &LogEntry) -> usize;

    /// Ask every reachable peer for its version vector
    async fn fetch_peer_versions(&self) -> Result<Vec<(u32, VersionVector)>, FaultError>;

    /// Exchange missing entries with one peer, returns the peer's updated
    /// version vector and the entries we were missing
    async fn sync_with_peer(
        &self,
        peer_id: u32,
        our_vv: &VersionVector,
        delta_for_peer: Vec<LogEntry>,
    ) -> Result<(VersionVector, Vec<LogEntry>), FaultError>;
}

pub struct FaultToleranceManager {
    node_id: u32,
}

impl FaultToleranceManager {
    pub fn new(node_id: u32, _all_node_ids: &[u32], _in_progress_ttl: Duration) -> Arc<Self> {
        Arc::new(Self { node_id })
    }

    // Health (stub: always healthy)
    pub async fn accepts_client_work(&self) -> bool {
        true
    }

    pub async fn accepts_peer_traffic(&self) -> bool {
        true
    }

    pub async fn on_elected_to_fail(&self, _duration_secs: u64) {}

    pub async fn begin_recovery_if_timer_expired(&self) -> bool {
        false
    }

    // Duplicate request handling (stub: never a duplicate)
    pub async fn begin_request(&self, _request_id: &str) -> Result<IdempotencyOutcome, FaultError> {
        Ok(IdempotencyOutcome::Acquired)
    }

    pub async fn touch_in_progress(&self, _request_id: &str) {}

    pub async fn abort_request(&self, _request_id: &str) {}

    // Logging and replication (stub: nothing is stored, the transport is not used)
    pub async fn record_and_replicate(
        &self,
        request_id: String,
        plan: ResponsePlan,
        _transport: &dyn PeerReplicationTransport,
    ) -> Result<LogEntry, FaultError> {
        Ok(LogEntry {
            origin_node: self.node_id,
            seq: 0,
            request_id,
            plan: Some(plan),
            committed_at_ms: 0,
        })
    }

    // Peer side handlers (stub: accept and ignore)
    pub async fn on_receive_replicated_entry(&self, _entry: LogEntry) -> Result<(), FaultError> {
        Ok(())
    }

    pub async fn handle_peer_sync(
        &self,
        _peer_vv: &VersionVector,
        _incoming_delta: Vec<LogEntry>,
    ) -> Result<(VersionVector, Vec<LogEntry>), FaultError> {
        Ok((HashMap::new(), Vec::new()))
    }

    // Recovery (stub: nothing to recover)
    pub async fn recover_state_from_peers(
        &self,
        _transport: &dyn PeerReplicationTransport,
    ) -> Result<(), FaultError> {
        Ok(())
    }

    pub fn spawn_recovery_loop(
        self: &Arc<Self>,
        _transport: Arc<dyn PeerReplicationTransport>,
        _poll_interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async {})
    }

    // Inspection
    pub async fn get_version_vector(&self) -> VersionVector {
        HashMap::new()
    }

    pub async fn last_recovery_metrics(&self) -> Option<RecoveryMetrics> {
        None
    }
}
