use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentCategory {
    BlockedRoad,
    PowerOutage,
    LabIncident,
    LocalizedFlooding,
    NetworkOutage,
    MedicalTechnicalAssistance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentStatus {
    Open,
    Assigned,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncidentRequest {
    pub request_id: String, // Unambiguous operation ID for idempotent retries
    pub incident_id: String,
    pub reporter_id: String,
    pub category: IncidentCategory,
    pub location_zone: u32,
    pub severity: u8,
    pub timestamp_ms: u64, // Unix epoch in milliseconds (SystemTime)
    pub status: IncidentStatus,
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponsePlan {
    pub request_id: String,
    pub incident_id: String,
    pub priority_score: f64,
    pub assigned_responders: Vec<String>,
    pub assigned_resources: Vec<String>,
    pub recommended_route: Vec<u32>,
    pub processed_by_node: u32,
}

// ---------------------------------------------------------------------------
// Planner inputs (Service 1). Shaped like a Discovery presence record so that in Phase 2
// the Discovery service can become a second snapshot source without planner changes.
// ---------------------------------------------------------------------------

pub type ZoneId = u32;

/// Skills a responder can advertise and an incident can require.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Capability {
    Medical,
    Electrical,
    Hazmat,
    Security,
    Logistics,
    Network,
    Plumbing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ResourceKind {
    Generator,
    WaterPump,
    MedKit,
    Barrier,
    RouterKit,
    HazmatKit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Responder {
    pub id: String,
    pub zone: ZoneId,
    pub capabilities: Vec<Capability>,
    /// 0.0 (offline) .. 1.0 (fully ready)
    pub availability: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    pub id: String,
    pub kind: ResourceKind,
    pub zone: ZoneId,
    pub quantity: u32,
    /// 0.0 .. 1.0
    pub availability: f64,
}
