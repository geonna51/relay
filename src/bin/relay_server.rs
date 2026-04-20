use clap::Parser;
use relay::api::create_router;
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "relay-server", about = "Relay Scheduler Server Daemon")]
pub struct ServerArgs {
    #[arg(short, long, default_value = "8000")]
    pub port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value = "relay.db")]
    pub db: String,

    #[arg(long, default_value = "15")]
    pub heartbeat_timeout_secs: u64,

    #[arg(long, default_value = "30")]
    pub lease_duration_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = ServerArgs::parse();

    info!("Initializing Relay Scheduler persistent store at {}", args.db);
    let store = Arc::new(Store::new(&args.db)?);

    let config = SchedulerConfig {
        heartbeat_timeout: Duration::from_secs(args.heartbeat_timeout_secs),
        default_lease_duration: Duration::from_secs(args.lease_duration_secs),
        reap_interval: Duration::from_secs(1),
    };

    let scheduler = Scheduler::new(store, config);
    scheduler.recover_state()?;
    scheduler.start_background_tasks();

    let addr = format!("{}:{}", args.host, args.port);
    info!("Relay Scheduler listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let app = create_router(scheduler);
    axum::serve(listener, app).await?;

    Ok(())
}
