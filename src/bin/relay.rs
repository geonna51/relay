use clap::{Parser, Subcommand};
use relay::api::create_router;
use relay::benchmark::{run_benchmark, BenchmarkConfig};
use relay::chaos::{run_chaos_test, ChaosConfig};
use relay::client::RelayClient;
use relay::models::{
    BatchSubmitRequest, Priority, ResourceRequirements, SubmitJobRequest,
};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use relay::worker::{WorkerConfig, WorkerDaemon};
use std::collections::HashMap;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "relay", about = "Relay — Distributed Job Scheduler CLI")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8000", global = true)]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "Submit a job to the Relay scheduler")]
    Submit {
        command: String,

        #[arg(long)]
        name: Option<String>,

        #[arg(long, default_value = "normal")]
        priority: String,

        #[arg(long, default_value_t = 3)]
        max_retries: u32,

        #[arg(long)]
        timeout: Option<u64>,

        #[arg(long, default_value_t = 1)]
        cpus: u32,

        #[arg(long, default_value_t = 512)]
        ram: u64,

        #[arg(long)]
        depends_on: Vec<String>,
    },

    #[command(about = "Submit an array of jobs in batch")]
    Batch {
        command: String,

        #[arg(long, default_value_t = 10)]
        count: usize,

        #[arg(long, default_value = "normal")]
        priority: String,
    },

    #[command(about = "Check the status and details of a job")]
    Status {
        job_id: String,
    },

    #[command(about = "Fetch stdout and stderr logs for a job")]
    Logs {
        job_id: String,
    },

    #[command(about = "Cancel a queued or running job")]
    Cancel {
        job_id: String,
    },

    #[command(about = "List jobs in the queue")]
    List {
        #[arg(long)]
        status: Option<String>,

        #[arg(long, default_value_t = 50)]
        limit: usize,
    },

    #[command(about = "List registered workers and their status")]
    Workers,

    #[command(about = "Display cluster queue metrics")]
    Metrics,

    #[command(about = "Run the Relay scheduler daemon")]
    Server {
        #[arg(short, long, default_value = "8000")]
        port: u16,

        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        #[arg(long, default_value = "relay.db")]
        db: String,
    },

    #[command(about = "Run a Relay worker process")]
    Worker {
        #[arg(long)]
        id: Option<String>,

        #[arg(long)]
        cpus: Option<u32>,

        #[arg(long)]
        ram: Option<u64>,

        #[arg(long, default_value_t = false)]
        oneshot: bool,
    },

    #[command(about = "Run chaos failure injection tests")]
    Chaos {
        #[arg(long, default_value_t = 50)]
        jobs: usize,

        #[arg(long, default_value_t = 4)]
        workers: usize,

        #[arg(long, default_value_t = 20)]
        duration: u64,
    },

    #[command(about = "Run high-throughput scheduling benchmark")]
    Benchmark {
        #[arg(long, default_value_t = 1000)]
        jobs: usize,

        #[arg(long, default_value_t = 8)]
        workers: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let client = RelayClient::new(&cli.server);

    match cli.command {
        Commands::Submit {
            command,
            name,
            priority,
            max_retries,
            timeout,
            cpus,
            ram,
            depends_on,
        } => {
            let prio = priority.parse().unwrap_or(Priority::Normal);
            let req = SubmitJobRequest {
                command,
                args: vec![],
                cwd: None,
                env: HashMap::new(),
                priority: prio,
                max_retries,
                timeout_seconds: timeout,
                resources: ResourceRequirements {
                    cpus,
                    memory_mb: ram,
                    labels: HashMap::new(),
                },
                depends_on,
                name,
            };

            match client.submit(req).await {
                Ok(resp) => {
                    println!("Job submitted: {}", resp.job_id);
                    println!("Status: {}", resp.status);
                }
                Err(e) => eprintln!("Error submitting job: {e}"),
            }
        }

        Commands::Batch {
            command,
            count,
            priority,
        } => {
            let prio = priority.parse().unwrap_or(Priority::Normal);
            let mut jobs = Vec::with_capacity(count);
            for i in 0..count {
                let mut env = HashMap::new();
                env.insert("RELAY_ARRAY_INDEX".to_string(), i.to_string());
                jobs.push(SubmitJobRequest {
                    command: command.clone(),
                    args: vec![],
                    cwd: None,
                    env,
                    priority: prio,
                    max_retries: 3,
                    timeout_seconds: None,
                    resources: ResourceRequirements::default(),
                    depends_on: vec![],
                    name: Some(format!("batch-{}-job-{}", count, i)),
                });
            }

            match client.submit_batch(BatchSubmitRequest { jobs }).await {
                Ok(resp) => {
                    println!("Batch submitted: {} jobs queued", resp.count);
                    if let Some(first) = resp.job_ids.first() {
                        println!("First job ID: {}", first);
                    }
                    if let Some(last) = resp.job_ids.last() {
                        println!("Last job ID: {}", last);
                    }
                }
                Err(e) => eprintln!("Error submitting batch: {e}"),
            }
        }

        Commands::Status { job_id } => match client.status(&job_id).await {
            Ok(job) => {
                println!("Job:             {}", job.id);
                if let Some(ref name) = job.name {
                    println!("Name:            {}", name);
                }
                println!("Command:         {}", job.command);
                println!("Status:          {}", job.status);
                println!("Priority:        {}", job.priority);
                println!("Retries:         {}/{}", job.retry_count, job.max_retries);
                if let Some(ref w) = job.worker_id {
                    println!("Worker:          {}", w);
                }
                if let Some(ref att) = job.current_attempt_id {
                    println!("Current Attempt: {}", att);
                }
                if let Some(code) = job.exit_code {
                    println!("Exit Code:       {}", code);
                }
                if let Some(rt) = job.runtime_ms {
                    println!("Runtime:         {}ms", rt);
                }
            }
            Err(e) => eprintln!("Error: {e}"),
        },

        Commands::Logs { job_id } => match client.logs(&job_id).await {
            Ok(val) => {
                println!("=== Logs for {} ===", job_id);
                if let Some(file_content) = val.get("log_file").and_then(|v| v.as_str()) {
                    print!("{}", file_content);
                } else {
                    println!("--- STDOUT ---");
                    println!("{}", val.get("stdout").and_then(|v| v.as_str()).unwrap_or(""));
                    println!("--- STDERR ---");
                    println!("{}", val.get("stderr").and_then(|v| v.as_str()).unwrap_or(""));
                }
            }
            Err(e) => eprintln!("Error fetching logs: {e}"),
        },

        Commands::Cancel { job_id } => match client.cancel(&job_id).await {
            Ok(val) => println!("Cancelled job {}: {}", job_id, val),
            Err(e) => eprintln!("Error cancelling job: {e}"),
        },

        Commands::List { status, limit } => {
            let status_filter = status.and_then(|s| s.parse().ok());
            match client.list(status_filter, limit).await {
                Ok(jobs) => {
                    println!(
                        "{:<16} {:<12} {:<10} {:<14} {:<8} {:<30}",
                        "JOB ID", "STATUS", "PRIORITY", "WORKER", "RETRIES", "COMMAND"
                    );
                    println!("{}", "-".repeat(95));
                    for j in jobs {
                        let worker = j.worker_id.unwrap_or_else(|| "—".to_string());
                        let cmd = if j.command.len() > 28 {
                            format!("{}...", &j.command[..25])
                        } else {
                            j.command
                        };
                        println!(
                            "{:<16} {:<12} {:<10} {:<14} {:<8} {:<30}",
                            j.id,
                            j.status.as_str(),
                            j.priority.to_string(),
                            worker,
                            format!("{}/{}", j.retry_count, j.max_retries),
                            cmd
                        );
                    }
                }
                Err(e) => eprintln!("Error listing jobs: {e}"),
            }
        }

        Commands::Workers => match client.workers().await {
            Ok(workers) => {
                println!(
                    "{:<18} {:<12} {:<8} {:<12} {:<12} {:<24}",
                    "WORKER ID", "STATUS", "CPUS", "RAM (MB)", "ACTIVE JOBS", "LAST HEARTBEAT"
                );
                println!("{}", "-".repeat(90));
                for w in workers {
                    println!(
                        "{:<18} {:<12} {:<8} {:<12} {:<12} {:<24}",
                        w.id,
                        w.status.as_str(),
                        w.cpus,
                        w.memory_mb,
                        w.active_jobs,
                        w.last_heartbeat.format("%Y-%m-%d %H:%M:%S").to_string()
                    );
                }
            }
            Err(e) => eprintln!("Error fetching workers: {e}"),
        },

        Commands::Metrics => match client.metrics().await {
            Ok(m) => {
                println!("=== Relay Cluster Metrics ===");
                println!("Total Jobs:          {}", m.total_jobs);
                println!("  Queued:            {}", m.jobs_queued);
                println!("  Assigned:          {}", m.jobs_assigned);
                println!("  Running:           {}", m.jobs_running);
                println!("  Retrying:          {}", m.jobs_retrying);
                println!("  Succeeded:         {}", m.jobs_succeeded);
                println!("  Failed:            {}", m.jobs_failed);
                println!("  Cancelled:         {}", m.jobs_cancelled);
                println!("  Blocked (DAG):     {}", m.jobs_blocked);
                println!("Workers:             {}/{} active", m.workers_active, m.workers_total);
                println!("Active Leases:       {}", m.active_leases);
            }
            Err(e) => eprintln!("Error fetching metrics: {e}"),
        },

        Commands::Server { port, host, db } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
                )
                .init();

            let store = Arc::new(Store::new(&db)?);
            let config = SchedulerConfig::default();
            let scheduler = Scheduler::new(store, config);
            scheduler.recover_state()?;
            scheduler.start_background_tasks();

            let addr = format!("{}:{}", host, port);
            println!("Relay Scheduler running on http://{}", addr);

            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let app = create_router(scheduler);
            axum::serve(listener, app).await?;
        }

        Commands::Worker {
            id,
            cpus,
            ram,
            oneshot,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
                )
                .init();

            let mut cfg = WorkerConfig::default();
            cfg.scheduler_url = cli.server;
            cfg.oneshot = oneshot;
            if let Some(i) = id {
                cfg.worker_id = i;
            }
            if let Some(c) = cpus {
                cfg.cpus = c;
            }
            if let Some(r) = ram {
                cfg.memory_mb = r;
            }

            let daemon = WorkerDaemon::new(cfg);
            daemon.run().await?;
        }

        Commands::Chaos {
            jobs,
            workers,
            duration,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
                )
                .init();

            let cfg = ChaosConfig {
                num_jobs: jobs,
                num_workers: workers,
                duration_secs: duration,
                kill_interval_secs: 3,
            };

            let report = run_chaos_test(cfg).await?;
            println!("\n=== Chaos Failure Test Report ===");
            println!("Jobs Submitted:      {}", report.jobs_submitted);
            println!("Jobs Succeeded:      {}", report.jobs_succeeded);
            println!("Jobs Failed:         {}", report.jobs_failed);
            println!("Jobs Lost:           {}", report.jobs_lost);
            println!("Workers Killed:      {}", report.workers_killed);
            println!("Duration:            {:.2}s", report.total_duration.as_secs_f64());
            println!("Result:              {}", if report.success { "PASSED (Zero jobs lost)" } else { "FAILED" });
        }

        Commands::Benchmark { jobs, workers } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env().add_directive(tracing::Level::WARN.into()),
                )
                .init();

            println!("Running scheduling benchmark with {} jobs across {} workers...", jobs, workers);
            let cfg = BenchmarkConfig {
                num_jobs: jobs,
                num_workers: workers,
            };

            let report = run_benchmark(cfg).await?;
            println!("\n=== Benchmark Results ===");
            println!("Jobs:                {}", report.jobs);
            println!("Workers:             {}", report.workers);
            println!("Total Duration:      {:.2}s", report.total_duration.as_secs_f64());
            println!("Throughput:          {:.1} jobs/sec", report.throughput_jobs_per_sec);
            println!("Latency (p50):       {:.2} ms", report.p50_ms);
            println!("Latency (p95):       {:.2} ms", report.p95_ms);
            println!("Latency (p99):       {:.2} ms", report.p99_ms);
        }
    }

    Ok(())
}
