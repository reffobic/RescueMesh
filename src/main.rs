mod client;
mod election;
mod fault_tolerance;
mod node;
mod planner;
#[rustfmt::skip]
mod rpc;
mod snapshot;
mod types;
#[rustfmt::skip]
mod worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "node".into());
    match mode.as_str() {
        "node" => {
            let peers =
                client::parse_node_addresses(&std::env::var("RM_NODES").unwrap_or_else(|_| {
                    "1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003".into()
                }))?;
            let id: u32 = std::env::var("RM_NODE_ID")
                .unwrap_or_else(|_| "1".into())
                .parse()?;
            let node = node::Node::new(
                id,
                peers,
                std::sync::Arc::new(election::LoadAwareBully::new()),
                std::sync::Arc::new(election::Config::from_env()),
            )
            .await;
            node.spawn_loops();
            println!("RescueMesh node {} listening; {}", node.id, node.stats());
            std::future::pending::<()>().await;
        }
        "client" => {
            let report = client::run_workload(client::ClientConfig::from_env()?).await?;
            println!(
                "CLIENT_METRICS,submitted={},succeeded={},failed={},timed_out={},mean_ms={:.2},median_ms={:.2},p95_ms={:.2},p99_ms={:.2},throughput_rps={:.2},queue_wait_ms={:.2},max_in_flight={},per_node={:?}",
                report.submitted,
                report.succeeded,
                report.failed,
                report.timed_out,
                report.mean_latency_ms,
                report.median_latency_ms,
                report.p95_latency_ms,
                report.p99_latency_ms,
                report.throughput_rps,
                report.mean_client_queue_wait_ms,
                report.max_concurrent_requests,
                report.per_node_requests
            );
        }
        _ => return Err("usage: rescue_mesh [node|client]".into()),
    }
    Ok(())
}
