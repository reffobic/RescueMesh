//! The "currently known incident/resource snapshot" that the planner works on.
//!
//! Phase 1 has no Discovery service yet, so the snapshot is synthetic: a campus zone graph, a
//! responder pool, a resource pool and a backlog of open incidents, all generated from one seed.
//! Every node uses the same seed, so every node holds the identical snapshot and a duplicate
//! execution of a request produces the same assignment.
//!
//! `SnapshotSource` is the seam for Phase 2. A Discovery-backed source can implement it and the
//! planner does not change.

use crate::types::{Capability, IncidentCategory, Resource, ResourceKind, Responder, ZoneId};
use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

pub const UNREACHABLE: u32 = u32::MAX;

/// Undirected weighted graph of logical campus zones. Edge weight = travel cost in minutes.
#[derive(Debug, Clone)]
pub struct ZoneGraph {
    adj: Vec<Vec<(u32, u32)>>, // zone -> (neighbour, weight)
}

/// Single-source shortest paths. `parent[v]` is the next hop from `v` towards the source,
/// so following parents from any zone walks the route to the source.
#[derive(Debug, Clone)]
pub struct ShortestPaths {
    pub dist: Vec<u32>,
    pub parent: Vec<u32>,
}

impl ShortestPaths {
    /// Zones visited when travelling from `from` to the source, both ends included.
    pub fn route_from(&self, from: ZoneId) -> Vec<ZoneId> {
        let mut route = vec![from];
        if self.dist[from as usize] == UNREACHABLE {
            return route;
        }
        let mut cur = from;
        while self.parent[cur as usize] != cur {
            cur = self.parent[cur as usize];
            route.push(cur);
        }
        route
    }
}

impl ZoneGraph {
    pub fn from_edges(n: usize, edges: &[(u32, u32, u32)]) -> Self {
        let mut adj = vec![Vec::new(); n];
        for &(a, b, w) in edges {
            adj[a as usize].push((b, w));
            adj[b as usize].push((a, w));
        }
        ZoneGraph { adj }
    }

    pub fn len(&self) -> usize {
        self.adj.len()
    }

    /// Dijkstra with integer weights, so ordering and ties are deterministic.
    pub fn shortest_paths(&self, src: ZoneId) -> ShortestPaths {
        let n = self.adj.len();
        let mut dist = vec![UNREACHABLE; n];
        let mut parent: Vec<u32> = (0..n as u32).collect();
        let mut heap = BinaryHeap::new();
        dist[src as usize] = 0;
        heap.push(Reverse((0u32, src)));
        while let Some(Reverse((d, u))) = heap.pop() {
            if d > dist[u as usize] {
                continue;
            }
            for &(v, w) in &self.adj[u as usize] {
                let nd = d.saturating_add(w);
                // strict < keeps the first-found (lowest-id-first by construction) parent on ties
                if nd < dist[v as usize] {
                    dist[v as usize] = nd;
                    parent[v as usize] = u;
                    heap.push(Reverse((nd, v)));
                }
            }
        }
        ShortestPaths { dist, parent }
    }
}

/// An already-open incident that competes with a new request for responders.
#[derive(Debug, Clone)]
pub struct BacklogIncident {
    pub id: String,
    pub category: IncidentCategory,
    pub zone: ZoneId,
    pub severity: u8,
    pub age_s: f64,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub zones: ZoneGraph,
    pub responders: Vec<Responder>,
    pub resources: Vec<Resource>,
    pub backlog: Vec<BacklogIncident>,
}

pub trait SnapshotSource: Send + Sync {
    fn current(&self) -> Arc<Snapshot>;
}

/// Fixed snapshot generated once at startup.
pub struct SyntheticSource(Arc<Snapshot>);

impl SyntheticSource {
    pub fn new(p: &SnapshotParams) -> Self {
        SyntheticSource(Arc::new(generate(p)))
    }
}

impl SnapshotSource for SyntheticSource {
    fn current(&self) -> Arc<Snapshot> {
        self.0.clone()
    }
}

/// Size knobs. Planning cost grows with `responders` and `backlog`, which is how the load
/// experiments make a request more or less expensive.
#[derive(Debug, Clone)]
pub struct SnapshotParams {
    pub seed: u64,
    pub zones: usize,
    pub responders: usize,
    pub resources: usize,
    pub backlog: usize,
}

impl Default for SnapshotParams {
    fn default() -> Self {
        SnapshotParams { seed: 42, zones: 24, responders: 120, resources: 60, backlog: 30 }
    }
}

impl SnapshotParams {
    pub fn from_env() -> Self {
        let d = Self::default();
        let u = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        SnapshotParams {
            seed: u("RM_SEED", d.seed),
            zones: (u("RM_ZONES", d.zones as u64) as usize).max(2),
            responders: u("RM_RESPONDERS", d.responders as u64) as usize,
            resources: u("RM_RESOURCES", d.resources as u64) as usize,
            backlog: u("RM_BACKLOG", d.backlog as u64) as usize,
        }
    }
}

/// SplitMix64: tiny, fast, fully deterministic across machines (no `rand` dependency).
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n.max(1) as u64) as usize
    }
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

pub const ALL_CAPABILITIES: [Capability; 7] = [
    Capability::Medical,
    Capability::Electrical,
    Capability::Hazmat,
    Capability::Security,
    Capability::Logistics,
    Capability::Network,
    Capability::Plumbing,
];

pub const ALL_RESOURCE_KINDS: [ResourceKind; 6] = [
    ResourceKind::Generator,
    ResourceKind::WaterPump,
    ResourceKind::MedKit,
    ResourceKind::Barrier,
    ResourceKind::RouterKit,
    ResourceKind::HazmatKit,
];

pub fn category_from_index(i: usize) -> IncidentCategory {
    match i % 6 {
        0 => IncidentCategory::BlockedRoad,
        1 => IncidentCategory::PowerOutage,
        2 => IncidentCategory::LabIncident,
        3 => IncidentCategory::LocalizedFlooding,
        4 => IncidentCategory::NetworkOutage,
        _ => IncidentCategory::MedicalTechnicalAssistance,
    }
}

pub fn generate(p: &SnapshotParams) -> Snapshot {
    let mut rng = SplitMix64(p.seed);
    let n = p.zones.max(2);

    // Ring keeps the graph connected; chords create alternative routes.
    let mut edges: Vec<(u32, u32, u32)> = Vec::new();
    for i in 0..n {
        edges.push((i as u32, ((i + 1) % n) as u32, 1 + rng.below(5) as u32));
    }
    for _ in 0..n / 2 {
        let a = rng.below(n) as u32;
        let b = rng.below(n) as u32;
        if a != b {
            edges.push((a, b, 2 + rng.below(8) as u32));
        }
    }
    let zones = ZoneGraph::from_edges(n, &edges);

    let responders = (0..p.responders)
        .map(|i| {
            let mut caps = vec![ALL_CAPABILITIES[rng.below(ALL_CAPABILITIES.len())]];
            if rng.unit() < 0.4 {
                let extra = ALL_CAPABILITIES[rng.below(ALL_CAPABILITIES.len())];
                if !caps.contains(&extra) {
                    caps.push(extra);
                }
            }
            caps.sort();
            Responder {
                id: format!("R{i}"),
                zone: rng.below(n) as u32,
                capabilities: caps,
                availability: ((0.2 + 0.8 * rng.unit()) * 100.0).round() / 100.0,
            }
        })
        .collect();

    let resources = (0..p.resources)
        .map(|i| Resource {
            id: format!("S{i}"),
            kind: ALL_RESOURCE_KINDS[rng.below(ALL_RESOURCE_KINDS.len())],
            zone: rng.below(n) as u32,
            quantity: 1 + rng.below(3) as u32,
            availability: (0.5 + 0.5 * rng.unit()).clamp(0.0, 1.0),
        })
        .collect();

    let backlog = (0..p.backlog)
        .map(|i| BacklogIncident {
            id: format!("OPEN{i}"),
            category: category_from_index(rng.below(6)),
            zone: rng.below(n) as u32,
            severity: 1 + rng.below(5) as u8,
            age_s: (rng.unit() * 900.0).floor(),
        })
        .collect();

    Snapshot { zones, responders, resources, backlog }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_deterministic_per_seed() {
        let p = SnapshotParams::default();
        let a = generate(&p);
        let b = generate(&p);
        assert_eq!(a.responders, b.responders);
        assert_eq!(a.resources, b.resources);
        assert_eq!(a.backlog.len(), b.backlog.len());
        let c = generate(&SnapshotParams { seed: 7, ..p });
        assert_ne!(a.responders, c.responders);
    }

    #[test]
    fn graph_is_connected_and_routes_end_at_source() {
        let s = generate(&SnapshotParams::default());
        let sp = s.zones.shortest_paths(5);
        assert!(sp.dist.iter().all(|&d| d != UNREACHABLE));
        for z in 0..s.zones.len() as u32 {
            let route = sp.route_from(z);
            assert_eq!(route.first(), Some(&z));
            assert_eq!(route.last(), Some(&5));
        }
    }

    #[test]
    fn dijkstra_prefers_cheaper_two_hop_path() {
        // 0 -10- 2 directly, or 0 -1- 1 -1- 2
        let g = ZoneGraph::from_edges(3, &[(0, 2, 10), (0, 1, 1), (1, 2, 1)]);
        let sp = g.shortest_paths(2);
        assert_eq!(sp.dist[0], 2);
        assert_eq!(sp.route_from(0), vec![0, 1, 2]);
    }
}
