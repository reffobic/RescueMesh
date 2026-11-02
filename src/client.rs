use crate::{
    node::Msg,
    types::{IncidentCategory, IncidentRequest, IncidentStatus, ResponsePlan},
};
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, Mutex as AsyncMutex, Semaphore},
    task::JoinSet,
    time::sleep,
};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub node_addresses: Vec<SocketAddr>,
    pub client_id: String,
    pub simulated_users: usize,
    pub requests_per_user: usize,
    pub max_in_flight: usize,
    pub request_timeout: Duration,
    pub request_interval: Duration,
}

impl ClientConfig {
    pub fn from_env() -> Result<Self, String> {
        let node_addresses = parse_node_addresses(
            &env::var("RM_NODES")
                .unwrap_or_else(|_| "1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003".into()),
        )?
        .into_iter()
        .map(|(_, address)| address)
        .collect();
        let client_id = env::var("RM_CLIENT_ID").unwrap_or_else(|_| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("client-{}-{now}", std::process::id())
        });
        Ok(Self {
            node_addresses,
            client_id,
            simulated_users: env_usize("RM_CLIENT_USERS", 10)?,
            requests_per_user: env_usize("RM_CLIENT_REQUESTS_PER_USER", 10)?,
            max_in_flight: env_usize("RM_CLIENT_MAX_IN_FLIGHT", 32)?,
            request_timeout: Duration::from_millis(env_u64("RM_CLIENT_TIMEOUT_MS", 5000)?),
            request_interval: Duration::from_millis(env_u64("RM_CLIENT_RATE_MS", 0)?),
        })
    }

    fn validate(&self) -> Result<(), String> {
        if self.node_addresses.is_empty() {
            return Err("at least one node address is required".into());
        }
        if self.simulated_users == 0 || self.max_in_flight == 0 {
            return Err(
                "RM_CLIENT_USERS and RM_CLIENT_MAX_IN_FLIGHT must be greater than zero".into(),
            );
        }
        if self.request_timeout.is_zero() {
            return Err("RM_CLIENT_TIMEOUT_MS must be greater than zero".into());
        }
        Ok(())
    }
}

fn env_usize(key: &str, default: usize) -> Result<usize, String> {
    env::var(key).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("{key} must be a non-negative integer: {error}"))
    })
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    env::var(key).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("{key} must be a non-negative integer: {error}"))
    })
}

pub fn parse_node_addresses(value: &str) -> Result<Vec<(u32, SocketAddr)>, String> {
    value
        .split(',')
        .map(|entry| {
            let (id, address) = entry.split_once('@').ok_or_else(|| {
                format!("invalid node entry '{entry}'; expected NODE_ID@HOST:PORT")
            })?;
            Ok((
                id.parse()
                    .map_err(|error| format!("invalid node ID '{id}': {error}"))?,
                address
                    .parse()
                    .map_err(|error| format!("invalid socket address '{address}': {error}"))?,
            ))
        })
        .collect()
}

#[derive(Debug)]
struct RequestMeasurement {
    succeeded: bool,
    timed_out: bool,
    processed_by_node: Option<u32>,
    pub latency: Duration,
    pub queue_wait: Duration,
}

#[derive(Debug)]
pub struct WorkloadReport {
    pub submitted: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub timed_out: usize,
    pub mean_latency_ms: f64,
    pub median_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub throughput_rps: f64,
    pub mean_client_queue_wait_ms: f64,
    pub max_concurrent_requests: usize,
    pub per_node_requests: HashMap<u32, usize>,
}

pub async fn run_workload(config: ClientConfig) -> Result<WorkloadReport, String> {
    config.validate()?;
    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut users = JoinSet::new();
    let user_workers = config.simulated_users.min(config.max_in_flight);
    let (user_tx, user_rx) = mpsc::channel(user_workers);
    let user_rx = Arc::new(AsyncMutex::new(user_rx));

    for _ in 0..user_workers {
        let config = config.clone();
        let semaphore = Arc::clone(&semaphore);
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        let user_rx = Arc::clone(&user_rx);
        users.spawn(async move {
            let mut measurements = Vec::new();
            loop {
                let user = user_rx.lock().await.recv().await;
                let Some(user) = user else {
                    break;
                };
                measurements.extend(
                    run_user(
                        &config,
                        user,
                        Arc::clone(&semaphore),
                        Arc::clone(&active),
                        Arc::clone(&max_active),
                    )
                    .await,
                );
            }
            measurements
        });
    }
    for user in 0..config.simulated_users {
        user_tx
            .send(user)
            .await
            .map_err(|error| format!("simulated-user queue stopped: {error}"))?;
    }
    drop(user_tx);

    let mut measurements = Vec::new();
    while let Some(result) = users.join_next().await {
        match result {
            Ok(user_measurements) => measurements.extend(user_measurements),
            Err(error) => return Err(format!("simulated-user task failed: {error}")),
        }
    }

    async fn run_user(
        config: &ClientConfig,
        user: usize,
        semaphore: Arc<Semaphore>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    ) -> Vec<RequestMeasurement> {
        let user_id = format!("{}-user-{user}", config.client_id);
        let mut measurements = Vec::new();
        for sequence in 0..config.requests_per_user {
            let queued_at = Instant::now();
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_error) => {
                    measurements.push(RequestMeasurement {
                        succeeded: false,
                        timed_out: false,
                        processed_by_node: None,
                        latency: queued_at.elapsed(),
                        queue_wait: queued_at.elapsed(),
                    });
                    continue;
                }
            };
            let queue_wait = queued_at.elapsed();
            let current = active.fetch_add(1, Ordering::Relaxed) + 1;
            max_active.fetch_max(current, Ordering::Relaxed);
            let request_id = format!("{}-{sequence}", user_id);
            let req = make_request(request_id, user_id.clone(), sequence as u32);
            let request_id = req.request_id.clone();
            let request_started = Instant::now();
            let result = submit_request(config, req).await;
            if let Err(error) = &result {
                eprintln!("CLIENT_ERROR,user_id={user_id},request_id={request_id},error={error}");
            }
            let timed_out = result
                .as_ref()
                .err()
                .is_some_and(|error| error == "request timed out");
            measurements.push(RequestMeasurement {
                succeeded: result.is_ok(),
                timed_out,
                processed_by_node: result.ok().map(|plan| plan.processed_by_node),
                latency: request_started.elapsed(),
                queue_wait,
            });
            active.fetch_sub(1, Ordering::Relaxed);
            drop(permit);
            if !config.request_interval.is_zero() {
                sleep(config.request_interval).await;
            }
        }
        measurements
    }

    Ok(summarize(
        measurements,
        started.elapsed(),
        max_active.load(Ordering::Relaxed),
    ))
}

fn make_request(request_id: String, reporter_id: String, sequence: u32) -> IncidentRequest {
    IncidentRequest {
        request_id,
        incident_id: format!("incident-{reporter_id}-{sequence}"),
        reporter_id,
        category: IncidentCategory::BlockedRoad,
        location_zone: sequence % 20,
        severity: (sequence % 5 + 1) as u8,
        timestamp_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        status: IncidentStatus::Open,
        ttl_secs: 300,
    }
}

async fn submit_request(
    config: &ClientConfig,
    req: IncidentRequest,
) -> Result<ResponsePlan, String> {
    let bind_addr = match config.node_addresses[0].ip() {
        IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let route_socket = std::net::UdpSocket::bind(bind_addr)
        .map_err(|error| format!("bind client route-discovery socket: {error}"))?;
    route_socket
        .connect(config.node_addresses[0])
        .map_err(|error| format!("select client network route: {error}"))?;
    let client_ip = route_socket
        .local_addr()
        .map_err(|error| format!("read client route: {error}"))?
        .ip();
    drop(route_socket);
    let socket = UdpSocket::bind(SocketAddr::new(client_ip, 0))
        .await
        .map_err(|error| format!("bind client socket: {error}"))?;
    let reply_to = socket
        .local_addr()
        .map_err(|error| format!("read client socket address: {error}"))?;
    let bytes = serde_json::to_vec(&Msg::Request {
        req: req.clone(),
        reply_to,
    })
    .map_err(|error| format!("serialize request: {error}"))?;
    for address in &config.node_addresses {
        socket
            .send_to(&bytes, address)
            .await
            .map_err(|error| format!("send request to {address}: {error}"))?;
    }

    let deadline = Instant::now() + config.request_timeout;
    let mut busy_nodes = 0;
    let mut buf = vec![0u8; 16384];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("request timed out".into());
        }
        let (size, source) = match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return Err(format!("receive response: {error}")),
            Err(_) => return Err("request timed out".into()),
        };
        let message = match serde_json::from_slice::<Msg>(&buf[..size]) {
            Ok(message) => message,
            Err(error) => {
                eprintln!("ignored malformed cloud response: {error}");
                continue;
            }
        };
        match message {
            Msg::Reply { plan, .. } if plan.request_id == req.request_id => {
                let ack = serde_json::to_vec(&Msg::Ack {
                    request_id: req.request_id.clone(),
                })
                .map_err(|error| format!("serialize acknowledgment: {error}"))?;
                socket
                    .send_to(&ack, source)
                    .await
                    .map_err(|error| format!("send acknowledgment: {error}"))?;
                return Ok(plan);
            }
            Msg::Busy { request_id, .. } if request_id == req.request_id => {
                busy_nodes += 1;
                if busy_nodes >= config.node_addresses.len() {
                    return Err(
                        "all cloud nodes rejected the request because their queues are full".into(),
                    );
                }
            }
            _ => {}
        }
    }
}

fn summarize(
    measurements: Vec<RequestMeasurement>,
    elapsed: Duration,
    max_concurrent_requests: usize,
) -> WorkloadReport {
    let submitted = measurements.len();
    let succeeded = measurements.iter().filter(|m| m.succeeded).count();
    let timed_out = measurements.iter().filter(|m| m.timed_out).count();
    let mut latencies: Vec<f64> = measurements
        .iter()
        .map(|m| m.latency.as_secs_f64() * 1000.0)
        .collect();
    let mean_latency_ms = mean(&latencies);
    latencies.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        if latencies.is_empty() {
            0.0
        } else {
            latencies[((latencies.len() as f64 * p).ceil() as usize)
                .saturating_sub(1)
                .min(latencies.len() - 1)]
        }
    };
    let mean_client_queue_wait_ms = mean(
        &measurements
            .iter()
            .map(|m| m.queue_wait.as_secs_f64() * 1000.0)
            .collect::<Vec<_>>(),
    );
    let per_node_requests = measurements
        .iter()
        .filter_map(|m| m.processed_by_node)
        .fold(HashMap::new(), |mut counts, plan| {
            *counts.entry(plan).or_insert(0) += 1;
            counts
        });
    WorkloadReport {
        submitted,
        succeeded,
        failed: submitted - succeeded,
        timed_out,
        mean_latency_ms,
        median_latency_ms: percentile(0.50),
        p95_latency_ms: percentile(0.95),
        p99_latency_ms: percentile(0.99),
        throughput_rps: if elapsed.is_zero() {
            0.0
        } else {
            succeeded as f64 / elapsed.as_secs_f64()
        },
        mean_client_queue_wait_ms,
        max_concurrent_requests,
        per_node_requests,
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::UdpSocket, task::JoinSet};

    #[tokio::test]
    async fn concurrent_users_correlate_responses_and_isolate_delays() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap();
        let service = tokio::spawn(async move {
            let mut buf = vec![0u8; 16384];
            let mut tasks = JoinSet::new();
            let server = Arc::new(server);
            for _ in 0..4 {
                let (size, source) = server.recv_from(&mut buf).await.unwrap();
                let Msg::Request { req, reply_to } = serde_json::from_slice(&buf[..size]).unwrap()
                else {
                    panic!("expected request");
                };
                let response = Msg::Reply {
                    plan: ResponsePlan {
                        request_id: req.request_id.clone(),
                        incident_id: req.incident_id,
                        priority_score: 1.0,
                        assigned_responders: vec![],
                        assigned_resources: vec![],
                        recommended_route: vec![],
                        processed_by_node: 7,
                    },
                    under_replicated: false,
                };
                let server = Arc::clone(&server);
                tasks.spawn(async move {
                    if req.request_id.contains("user-0") {
                        sleep(Duration::from_millis(75)).await;
                        return;
                    }
                    let bytes = serde_json::to_vec(&response).unwrap();
                    server.send_to(&bytes, reply_to).await.unwrap();
                    let _ = source;
                });
            }
            while tasks.join_next().await.is_some() {}
        });
        let config = ClientConfig {
            node_addresses: vec![address],
            client_id: "test".into(),
            simulated_users: 4,
            requests_per_user: 1,
            max_in_flight: 4,
            request_timeout: Duration::from_millis(200),
            request_interval: Duration::ZERO,
        };
        let report = run_workload(config).await.unwrap();
        service.await.unwrap();
        assert_eq!(report.submitted, 4);
        assert_eq!(report.succeeded, 3);
        assert_eq!(report.failed, 1);
        assert_eq!(report.timed_out, 1);
        assert_eq!(report.max_concurrent_requests, 4);
        assert_eq!(report.per_node_requests.get(&7), Some(&3));
        assert!(report.mean_client_queue_wait_ms < 20.0);
    }
}
