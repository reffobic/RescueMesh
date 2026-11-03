//! Service 1 planner: incident priority and responder/resource assignment.
//!
//! Deterministic, explainable and tunable, as the spec asks. It is a pure function of
//! (request, snapshot, params, now), so it is easy to unit test and a duplicate execution on
//! another node yields the same assignment.
//!
//! Formulation
//! -----------
//! 1. Priority of an incident (0..100) is a weighted sum of three terms:
//!      severity/5, age/age_cap (clamped to 1) and a per-category urgency weight.
//! 2. The new incident is planned together with the snapshot's backlog of open incidents,
//!    in descending priority order, because they compete for the same responders.
//! 3. Each incident needs a list of capabilities (its "roles"). Every role is a slot. Slots are
//!    matched to available responders with the Hungarian algorithm (minimum total cost):
//!      cost(slot, responder) = w_dist * dist/dmax + w_cap * [responder lacks capability]
//!                              + w_avail * (1 - availability)
//!    Extra "unassigned" columns, priced higher for higher-priority slots, let the matching
//!    stay feasible when there are fewer responders than slots.
//! 4. Resources are taken greedily, in priority order, nearest first, consuming quantities.
//! 5. The route is the Dijkstra shortest path from the first assigned responder to the incident.
//!
//! Cost knob: Hungarian is O(S^2 (R + S)) for S slots and R responders, and S grows with the
//! backlog, so `RM_BACKLOG` and `RM_RESPONDERS` scale the work per request.

use crate::snapshot::{ShortestPaths, Snapshot, UNREACHABLE};
use crate::types::{Capability, IncidentCategory, IncidentRequest, ResourceKind, ResponsePlan, ZoneId};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PlannerParams {
    // priority weights (sum to 1)
    pub w_sev: f64,
    pub w_age: f64,
    pub w_cat: f64,
    pub age_cap_s: f64,
    // assignment cost weights (sum to 1)
    pub w_dist: f64,
    pub w_cap: f64,
    pub w_avail: f64,
    /// Responders and resources below this availability are not considered.
    pub min_availability: f64,
    /// Base price of leaving a slot unassigned; must exceed the largest real cost (1.0).
    pub unassigned_cost: f64,
}

impl Default for PlannerParams {
    fn default() -> Self {
        PlannerParams {
            w_sev: 0.5,
            w_age: 0.25,
            w_cat: 0.25,
            age_cap_s: 600.0,
            w_dist: 0.4,
            w_cap: 0.5,
            w_avail: 0.1,
            min_availability: 0.3,
            unassigned_cost: 2.0,
        }
    }
}

impl PlannerParams {
    pub fn from_env() -> Self {
        let d = Self::default();
        let f = |k: &str, d: f64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        PlannerParams {
            w_sev: f("RM_PW_SEV", d.w_sev),
            w_age: f("RM_PW_AGE", d.w_age),
            w_cat: f("RM_PW_CAT", d.w_cat),
            age_cap_s: f("RM_AGE_CAP_S", d.age_cap_s),
            w_dist: f("RM_AW_DIST", d.w_dist),
            w_cap: f("RM_AW_CAP", d.w_cap),
            w_avail: f("RM_AW_AVAIL", d.w_avail),
            min_availability: f("RM_MIN_AVAIL", d.min_availability),
            unassigned_cost: f("RM_UNASSIGNED_COST", d.unassigned_cost),
        }
    }
}

/// Human-readable explanation of one plan (the spec asks for explainable output).
#[derive(Debug, Clone)]
pub struct PlanTrace {
    pub severity_term: f64,
    pub age_term: f64,
    pub category_term: f64,
    pub incidents_planned: usize,
    pub candidate_responders: usize,
    pub slots: usize,
    pub matching_cost: f64,
    pub roles: Vec<RoleTrace>,
}

#[derive(Debug, Clone)]
pub struct RoleTrace {
    pub capability: Capability,
    pub responder: Option<String>,
    pub distance: Option<u32>,
    pub has_capability: bool,
}

impl PlanTrace {
    pub fn summary(&self) -> String {
        let roles: Vec<String> = self
            .roles
            .iter()
            .map(|r| match &r.responder {
                Some(id) => format!(
                    "{:?}->{}(dist={},skill={})",
                    r.capability,
                    id,
                    r.distance.map_or("?".into(), |d| d.to_string()),
                    r.has_capability
                ),
                None => format!("{:?}->none", r.capability),
            })
            .collect();
        format!(
            "priority[sev={:.1} age={:.1} cat={:.1}] planned={} candidates={} slots={} cost={:.3} roles=[{}]",
            self.severity_term,
            self.age_term,
            self.category_term,
            self.incidents_planned,
            self.candidate_responders,
            self.slots,
            self.matching_cost,
            roles.join(", ")
        )
    }
}

fn category_urgency(c: &IncidentCategory) -> f64 {
    match c {
        IncidentCategory::MedicalTechnicalAssistance => 1.0,
        IncidentCategory::LabIncident => 0.9,
        IncidentCategory::LocalizedFlooding => 0.7,
        IncidentCategory::PowerOutage => 0.6,
        IncidentCategory::BlockedRoad => 0.5,
        IncidentCategory::NetworkOutage => 0.4,
    }
}

/// Returns (total, severity term, age term, category term), all on the 0..100 scale.
pub fn priority(p: &PlannerParams, cat: &IncidentCategory, severity: u8, age_s: f64) -> (f64, f64, f64, f64) {
    let sev = p.w_sev * (severity.clamp(1, 5) as f64 / 5.0) * 100.0;
    let age = p.w_age * (age_s.max(0.0) / p.age_cap_s).clamp(0.0, 1.0) * 100.0;
    let cat = p.w_cat * category_urgency(cat) * 100.0;
    (sev + age + cat, sev, age, cat)
}

/// Capabilities an incident needs. Higher severity calls for more people.
pub fn required_roles(cat: &IncidentCategory, severity: u8) -> Vec<Capability> {
    use Capability::*;
    let mut roles = match cat {
        IncidentCategory::BlockedRoad => vec![Security, Logistics],
        IncidentCategory::PowerOutage => vec![Electrical, Security],
        IncidentCategory::LabIncident => vec![Hazmat, Medical],
        IncidentCategory::LocalizedFlooding => vec![Plumbing, Logistics],
        IncidentCategory::NetworkOutage => vec![Network],
        IncidentCategory::MedicalTechnicalAssistance => vec![Medical, Logistics],
    };
    if severity >= 4 {
        roles.push(roles[0]);
    }
    if severity >= 5 {
        roles.push(Security);
    }
    roles
}

pub fn required_resource(cat: &IncidentCategory) -> ResourceKind {
    match cat {
        IncidentCategory::BlockedRoad => ResourceKind::Barrier,
        IncidentCategory::PowerOutage => ResourceKind::Generator,
        IncidentCategory::LabIncident => ResourceKind::HazmatKit,
        IncidentCategory::LocalizedFlooding => ResourceKind::WaterPump,
        IncidentCategory::NetworkOutage => ResourceKind::RouterKit,
        IncidentCategory::MedicalTechnicalAssistance => ResourceKind::MedKit,
    }
}

fn resource_units(severity: u8) -> u32 {
    if severity >= 4 {
        2
    } else {
        1
    }
}

/// Minimum-cost assignment of every row to a distinct column (rows <= cols).
/// Returns, for each row, the chosen column. O(rows^2 * cols).
pub fn hungarian(cost: &[Vec<f64>]) -> Vec<usize> {
    let n = cost.len();
    if n == 0 {
        return Vec::new();
    }
    let m = cost[0].len();
    assert!(n <= m, "hungarian needs rows <= cols");
    let inf = f64::INFINITY;
    let mut u = vec![0.0; n + 1];
    let mut v = vec![0.0; m + 1];
    let mut p = vec![0usize; m + 1];
    let mut way = vec![0usize; m + 1];
    for i in 1..=n {
        p[0] = i;
        let mut j0 = 0usize;
        let mut minv = vec![inf; m + 1];
        let mut used = vec![false; m + 1];
        loop {
            used[j0] = true;
            let i0 = p[j0];
            let mut delta = inf;
            let mut j1 = 0usize;
            for j in 1..=m {
                if !used[j] {
                    let cur = cost[i0 - 1][j - 1] - u[i0] - v[j];
                    if cur < minv[j] {
                        minv[j] = cur;
                        way[j] = j0;
                    }
                    if minv[j] < delta {
                        delta = minv[j];
                        j1 = j;
                    }
                }
            }
            for j in 0..=m {
                if used[j] {
                    u[p[j]] += delta;
                    v[j] -= delta;
                } else {
                    minv[j] -= delta;
                }
            }
            j0 = j1;
            if p[j0] == 0 {
                break;
            }
        }
        loop {
            let j1 = way[j0];
            p[j0] = p[j1];
            j0 = j1;
            if j0 == 0 {
                break;
            }
        }
    }
    let mut ans = vec![0usize; n];
    for j in 1..=m {
        if p[j] != 0 {
            ans[p[j] - 1] = j - 1;
        }
    }
    ans
}

struct Item {
    id: String,
    category: IncidentCategory,
    zone: ZoneId,
    severity: u8,
    priority: f64,
    is_new: bool,
}

/// Plan `req` in the context of the snapshot's backlog.
pub fn plan(
    req: &IncidentRequest,
    snap: &Snapshot,
    params: &PlannerParams,
    now_ms: u64,
    node_id: u32,
) -> (ResponsePlan, PlanTrace) {
    let zones = snap.zones.len() as u32;
    let req_zone = req.location_zone % zones;
    let req_age = now_ms.saturating_sub(req.timestamp_ms) as f64 / 1000.0;
    let (req_pri, sev_t, age_t, cat_t) = priority(params, &req.category, req.severity, req_age);

    // 1. All incidents to plan, highest priority first (ties by id for determinism).
    let mut items: Vec<Item> = snap
        .backlog
        .iter()
        .filter(|b| b.id != req.incident_id)
        .map(|b| Item {
            id: b.id.clone(),
            category: b.category.clone(),
            zone: b.zone % zones,
            severity: b.severity,
            priority: priority(params, &b.category, b.severity, b.age_s).0,
            is_new: false,
        })
        .collect();
    items.push(Item {
        id: req.incident_id.clone(),
        category: req.category.clone(),
        zone: req_zone,
        severity: req.severity,
        priority: req_pri,
        is_new: true,
    });
    items.sort_by(|a, b| b.priority.total_cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));

    // 2. One shortest-path tree per distinct incident zone.
    let mut paths: HashMap<ZoneId, ShortestPaths> = HashMap::new();
    for it in &items {
        paths.entry(it.zone).or_insert_with(|| snap.zones.shortest_paths(it.zone));
    }
    let dmax = paths
        .values()
        .flat_map(|sp| sp.dist.iter().copied())
        .filter(|&d| d != UNREACHABLE)
        .max()
        .unwrap_or(1)
        .max(1) as f64;

    // 3. Slots (one per required role) and the cost matrix against available responders.
    let cands: Vec<_> = snap.responders.iter().filter(|r| r.availability >= params.min_availability).collect();
    let mut slots: Vec<(usize, Capability)> = Vec::new(); // (item index, capability)
    for (idx, it) in items.iter().enumerate() {
        for cap in required_roles(&it.category, it.severity) {
            slots.push((idx, cap));
        }
    }
    let s = slots.len();
    let c = cands.len();
    let mut cost = vec![vec![0.0f64; c + s]; s];
    for (si, &(idx, cap)) in slots.iter().enumerate() {
        let it = &items[idx];
        let sp = &paths[&it.zone];
        for (ci, r) in cands.iter().enumerate() {
            let d = sp.dist[(r.zone % zones) as usize];
            let dn = if d == UNREACHABLE { 1.0 } else { d as f64 / dmax };
            let skill = if r.capabilities.contains(&cap) { 0.0 } else { 1.0 };
            cost[si][ci] = params.w_dist * dn + params.w_cap * skill + params.w_avail * (1.0 - r.availability);
        }
        let leave = params.unassigned_cost * (1.0 + it.priority / 100.0);
        for k in 0..s {
            cost[si][c + k] = leave;
        }
    }
    let assign = hungarian(&cost);
    let matching_cost: f64 = assign.iter().enumerate().map(|(si, &col)| cost[si][col]).sum();

    // 4. Resources, greedy in priority order with consumption.
    let mut remaining: Vec<u32> = snap.resources.iter().map(|r| r.quantity).collect();
    let mut chosen_resources: Vec<Vec<String>> = vec![Vec::new(); items.len()];
    for (idx, it) in items.iter().enumerate() {
        let kind = required_resource(&it.category);
        let sp = &paths[&it.zone];
        let mut order: Vec<usize> = (0..snap.resources.len())
            .filter(|&i| {
                let r = &snap.resources[i];
                r.kind == kind && r.availability >= params.min_availability
            })
            .collect();
        order.sort_by(|&a, &b| {
            let (ra, rb) = (&snap.resources[a], &snap.resources[b]);
            sp.dist[(ra.zone % zones) as usize]
                .cmp(&sp.dist[(rb.zone % zones) as usize])
                .then(rb.availability.total_cmp(&ra.availability))
                .then_with(|| ra.id.cmp(&rb.id))
        });
        let mut need = resource_units(it.severity);
        for i in order {
            if need == 0 {
                break;
            }
            if remaining[i] > 0 {
                let take = remaining[i].min(need);
                remaining[i] -= take;
                need -= take;
                chosen_resources[idx].push(snap.resources[i].id.clone());
            }
        }
    }

    // 5. Extract the new incident's part of the global solution.
    let new_idx = items.iter().position(|i| i.is_new).expect("new incident is always present");
    let new_zone = items[new_idx].zone;
    let mut roles = Vec::new();
    let mut responders = Vec::new();
    let mut first_zone: Option<ZoneId> = None;
    for (si, &(idx, cap)) in slots.iter().enumerate() {
        if idx != new_idx {
            continue;
        }
        let col = assign[si];
        if col < c {
            let r = cands[col];
            let d = paths[&new_zone].dist[(r.zone % zones) as usize];
            roles.push(RoleTrace {
                capability: cap,
                responder: Some(r.id.clone()),
                distance: (d != UNREACHABLE).then_some(d),
                has_capability: r.capabilities.contains(&cap),
            });
            responders.push(r.id.clone());
            first_zone.get_or_insert(r.zone % zones);
        } else {
            roles.push(RoleTrace { capability: cap, responder: None, distance: None, has_capability: false });
        }
    }
    let route = match first_zone {
        Some(z) => paths[&new_zone].route_from(z),
        None => vec![new_zone],
    };

    let plan = ResponsePlan {
        request_id: req.request_id.clone(),
        incident_id: req.incident_id.clone(),
        priority_score: (req_pri * 100.0).round() / 100.0,
        assigned_responders: responders,
        assigned_resources: chosen_resources[new_idx].clone(),
        recommended_route: route,
        processed_by_node: node_id,
    };
    let trace = PlanTrace {
        severity_term: sev_t,
        age_term: age_t,
        category_term: cat_t,
        incidents_planned: items.len(),
        candidate_responders: c,
        slots: s,
        matching_cost,
        roles,
    };
    (plan, trace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{generate, BacklogIncident, SnapshotParams, SplitMix64, ZoneGraph};
    use crate::types::{IncidentStatus, Resource, Responder};

    fn req(id: &str, cat: IncidentCategory, zone: u32, sev: u8, ts: u64) -> IncidentRequest {
        IncidentRequest {
            request_id: format!("req-{id}"),
            incident_id: id.into(),
            reporter_id: "c1".into(),
            category: cat,
            location_zone: zone,
            severity: sev,
            timestamp_ms: ts,
            status: IncidentStatus::Open,
            ttl_secs: 3600,
        }
    }

    fn responder(id: &str, zone: u32, caps: &[Capability], av: f64) -> Responder {
        Responder { id: id.into(), zone, capabilities: caps.to_vec(), availability: av }
    }

    /// Line graph 0-1-2-3, each edge 1 minute, no backlog.
    fn tiny(responders: Vec<Responder>, resources: Vec<Resource>) -> Snapshot {
        Snapshot {
            zones: ZoneGraph::from_edges(4, &[(0, 1, 1), (1, 2, 1), (2, 3, 1)]),
            responders,
            resources,
            backlog: vec![],
        }
    }

    #[test]
    fn hungarian_matches_brute_force_on_random_matrices() {
        fn brute(cost: &[Vec<f64>]) -> f64 {
            fn rec(cost: &[Vec<f64>], row: usize, used: &mut Vec<bool>) -> f64 {
                if row == cost.len() {
                    return 0.0;
                }
                let mut best = f64::INFINITY;
                for j in 0..cost[0].len() {
                    if !used[j] {
                        used[j] = true;
                        best = best.min(cost[row][j] + rec(cost, row + 1, used));
                        used[j] = false;
                    }
                }
                best
            }
            rec(cost, 0, &mut vec![false; cost[0].len()])
        }
        let mut rng = SplitMix64(1234);
        for trial in 0..200 {
            let n = 1 + rng.below(5);
            let m = n + rng.below(3);
            let cost: Vec<Vec<f64>> = (0..n).map(|_| (0..m).map(|_| rng.unit() * 10.0).collect()).collect();
            let a = hungarian(&cost);
            let mut seen = std::collections::HashSet::new();
            assert!(a.iter().all(|&c| seen.insert(c)), "columns must be distinct (trial {trial})");
            let got: f64 = a.iter().enumerate().map(|(r, &c)| cost[r][c]).sum();
            assert!((got - brute(&cost)).abs() < 1e-9, "trial {trial}: {got} vs {}", brute(&cost));
        }
    }

    #[test]
    fn priority_rises_with_severity_and_age_and_category() {
        let p = PlannerParams::default();
        let c = IncidentCategory::PowerOutage;
        assert!(priority(&p, &c, 5, 0.0).0 > priority(&p, &c, 2, 0.0).0);
        assert!(priority(&p, &c, 3, 300.0).0 > priority(&p, &c, 3, 10.0).0);
        assert!(
            priority(&p, &IncidentCategory::MedicalTechnicalAssistance, 3, 0.0).0
                > priority(&p, &IncidentCategory::NetworkOutage, 3, 0.0).0
        );
        // age saturates at the cap
        assert_eq!(priority(&p, &c, 3, 600.0).0, priority(&p, &c, 3, 6000.0).0);
        // bounded to 0..100
        assert!(priority(&p, &IncidentCategory::MedicalTechnicalAssistance, 5, 1e9).0 <= 100.0 + 1e-9);
    }

    #[test]
    fn picks_skilled_responder_over_closer_unskilled_one() {
        let snap = tiny(
            vec![
                responder("near-unskilled", 1, &[Capability::Network], 1.0),
                responder("far-skilled", 3, &[Capability::Electrical], 1.0),
                responder("security", 2, &[Capability::Security], 1.0),
            ],
            vec![],
        );
        let (plan, _) = plan(&req("i1", IncidentCategory::PowerOutage, 0, 3, 0), &snap, &PlannerParams::default(), 0, 1);
        assert!(plan.assigned_responders.contains(&"far-skilled".to_string()));
        assert!(plan.assigned_responders.contains(&"security".to_string()));
        assert!(!plan.assigned_responders.contains(&"near-unskilled".to_string()));
    }

    #[test]
    fn prefers_closer_responder_when_skills_equal() {
        let snap = tiny(
            vec![
                responder("far", 3, &[Capability::Network], 1.0),
                responder("near", 1, &[Capability::Network], 1.0),
            ],
            vec![],
        );
        let (plan, _) = plan(&req("i1", IncidentCategory::NetworkOutage, 0, 1, 0), &snap, &PlannerParams::default(), 0, 1);
        assert_eq!(plan.assigned_responders, vec!["near".to_string()]);
    }

    #[test]
    fn skips_unavailable_responders() {
        let snap = tiny(
            vec![
                responder("offline", 0, &[Capability::Network], 0.1),
                responder("ready", 3, &[Capability::Network], 0.9),
            ],
            vec![],
        );
        let (plan, _) = plan(&req("i1", IncidentCategory::NetworkOutage, 0, 1, 0), &snap, &PlannerParams::default(), 0, 1);
        assert_eq!(plan.assigned_responders, vec!["ready".to_string()]);
    }

    #[test]
    fn route_runs_from_responder_zone_to_incident_zone() {
        let snap = tiny(vec![responder("r", 3, &[Capability::Network], 1.0)], vec![]);
        let (plan, _) = plan(&req("i1", IncidentCategory::NetworkOutage, 0, 1, 0), &snap, &PlannerParams::default(), 0, 1);
        assert_eq!(plan.recommended_route, vec![3, 2, 1, 0]);
    }

    #[test]
    fn no_responders_gives_empty_assignment_and_trivial_route() {
        let snap = tiny(vec![], vec![]);
        let (plan, trace) = plan(&req("i1", IncidentCategory::BlockedRoad, 2, 3, 0), &snap, &PlannerParams::default(), 0, 1);
        assert!(plan.assigned_responders.is_empty());
        assert_eq!(plan.recommended_route, vec![2]);
        assert!(trace.roles.iter().all(|r| r.responder.is_none()));
    }

    #[test]
    fn resources_are_matched_by_kind_nearest_first_and_quantity_is_consumed() {
        let res = |id: &str, kind, zone, qty| Resource { id: id.into(), kind, zone, quantity: qty, availability: 1.0 };
        let snap = tiny(
            vec![],
            vec![
                res("gen-far", ResourceKind::Generator, 3, 5),
                res("gen-near", ResourceKind::Generator, 1, 1),
                res("pump", ResourceKind::WaterPump, 0, 5),
            ],
        );
        // severity 4 needs 2 units: 1 from the near generator, 1 from the far one
        let (plan, _) = plan(&req("i1", IncidentCategory::PowerOutage, 0, 4, 0), &snap, &PlannerParams::default(), 0, 1);
        assert_eq!(plan.assigned_resources, vec!["gen-near".to_string(), "gen-far".to_string()]);
    }

    #[test]
    fn high_priority_incident_wins_the_only_skilled_responder() {
        // One electrician. The backlog incident is more urgent than the new one.
        let mut snap = tiny(vec![responder("only-electrician", 1, &[Capability::Electrical], 1.0)], vec![]);
        snap.backlog = vec![BacklogIncident {
            id: "OPEN0".into(),
            category: IncidentCategory::PowerOutage,
            zone: 1,
            severity: 5,
            age_s: 600.0,
        }];
        let (plan, _) = plan(&req("new", IncidentCategory::PowerOutage, 1, 1, 0), &snap, &PlannerParams::default(), 0, 1);
        assert!(!plan.assigned_responders.contains(&"only-electrician".to_string()));
    }

    #[test]
    fn same_input_gives_identical_plan_on_any_node() {
        let snap = generate(&SnapshotParams::default());
        let r = req("i9", IncidentCategory::LabIncident, 7, 4, 1_000);
        let p = PlannerParams::default();
        let (a, _) = plan(&r, &snap, &p, 5_000, 1);
        let (b, _) = plan(&r, &snap, &p, 5_000, 2);
        assert_eq!(a.assigned_responders, b.assigned_responders);
        assert_eq!(a.assigned_resources, b.assigned_resources);
        assert_eq!(a.recommended_route, b.recommended_route);
        assert_eq!(a.priority_score, b.priority_score);
        assert_eq!((a.processed_by_node, b.processed_by_node), (1, 2));
    }

    #[test]
    fn plan_cost_grows_with_backlog_and_pool_size() {
        let r = req("i1", IncidentCategory::PowerOutage, 3, 4, 0);
        let p = PlannerParams::default();
        let time = |responders, backlog| {
            let snap = generate(&SnapshotParams { responders, backlog, ..SnapshotParams::default() });
            let t = std::time::Instant::now();
            for _ in 0..3 {
                let _ = plan(&r, &snap, &p, 0, 1);
            }
            t.elapsed()
        };
        let small = time(40, 5);
        let large = time(300, 120);
        assert!(large > small * 3, "small={small:?} large={large:?}");
    }
}
