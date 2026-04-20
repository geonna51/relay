use clap::Parser;
use relay::worker::{WorkerConfig, WorkerDaemon};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "relay-worker", about = "Relay Worker Daemon")]
pub struct WorkerArgs {
    #[arg(short, long, default_value = "http://127.0.0.1:8000")]
    pub server: String,

    #[arg(long)]
    pub id: Option<String>,

    #[arg(long)]
    pub cpus: Option<u32>,

    #[arg(long)]
    pub ram: Option<u64>,

    #[arg(long, default_value = "o")]
    pub output_dir: String,

    #[arg(long, default_value_t = false)]
    pub oneshot: bool,

    #[arg(long, default_value_t = 4)]
    pub max_concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = WorkerArgs::parse();

    let mut cfg = WorkerConfig::default();
    cfg.scheduler_url = args.server;
    cfg.output_dir = args.output_dir;
    cfg.oneshot = args.oneshot;
    cfg.max_concurrent_jobs = args.max_concurrency;

    if let Some(id) = args.id {
        cfg.worker_id = id;
    }
    if let Some(cpus) = args.cpus {
        cfg.cpus = cpus;
    }
    if let Some(ram) = args.ram {
        cfg.memory_mb = ram;
    }

    let daemon = WorkerDaemon::new(cfg);
    daemon.run().await?;

    Ok(())
}
