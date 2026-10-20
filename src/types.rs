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
