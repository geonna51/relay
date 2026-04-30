use crate::api::create_router;
use crate::models::SubmitJobRequest;
use crate::scheduler::{Scheduler, SchedulerConfig};
use crate::store::Store;
use crate::worker::{WorkerConfig, WorkerDaemon};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::time::sleep;

pub struct BenchmarkConfig {
    pub num_jobs: usize,
    pub num_workers: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            num_jobs: 1000,
            num_workers: 8,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub jobs: usize,
    pub workers: usize,
    pub total_duration: Duration,
    pub throughput_jobs_per_sec: f64,
    pub submit_p50_ms: f64,
    pub submit_p95_ms: f64,
    pub submit_p99_ms: f64,
    pub sched_p50_ms: f64,
    pub sched_p95_ms: f64,
    pub sched_p99_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

pub async fn run_benchmark(config: BenchmarkConfig) -> Result<BenchmarkReport, Box<dyn std::error::Error + Send + Sync>> {
    let temp_db = NamedTempFile::new()?;
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path)?);
    let sched_config = SchedulerConfig {
        heartbeat_timeout: Duration::from_secs(10),
        default_lease_duration: Duration::from_secs(15),
        reap_interval: Duration::from_millis(500),
    };

    let scheduler = Scheduler::new(store.clone(), sched_config);
    let _reaper = scheduler.start_background_tasks();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_url = format!("http://{}", addr);

    let app = create_router(scheduler.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let mut daemons = Vec::new();
    for i in 0..config.num_workers {
        let mut w_cfg = WorkerConfig::default();
        w_cfg.worker_id = format!("bench-worker-{:02}", i + 1);
        w_cfg.scheduler_url = server_url.clone();
        w_cfg.poll_interval = Duration::from_millis(5);
        w_cfg.heartbeat_interval = Duration::from_secs(5);
        w_cfg.renew_interval = Duration::from_secs(5);
        w_cfg.max_concurrent_jobs = 16;

        let daemon = Arc::new(WorkerDaemon::new(w_cfg));
        let daemon_clone = daemon.clone();
        tokio::spawn(async move {
            let _ = daemon_clone.run().await;
        });
        daemons.push(daemon);
    }

    let start_time = Instant::now();
    let mut submit_latencies = Vec::with_capacity(config.num_jobs);

    for i in 0..config.num_jobs {
        let t0 = Instant::now();
        let req = SubmitJobRequest {
            command: "true".to_string(),
            args: vec![],
            cwd: None,
            env: std::collections::HashMap::new(),
            priority: crate::models::Priority::Normal,
            max_retries: 3,
            timeout_seconds: Some(5),
            resources: crate::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some(format!("bench-job-{}", i)),
        };
        scheduler.submit_job(req)?;
        submit_latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
    }

    while start_time.elapsed() < Duration::from_secs(60) {
        let metrics = store.get_metrics()?;
        if metrics.jobs_succeeded + metrics.jobs_failed >= config.num_jobs {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }

    let total_duration = start_time.elapsed();
    let throughput = config.num_jobs as f64 / total_duration.as_secs_f64();

    submit_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let submit_p50_ms = percentile(&submit_latencies, 0.50);
    let submit_p95_ms = percentile(&submit_latencies, 0.95);
    let submit_p99_ms = percentile(&submit_latencies, 0.99);

    let jobs = store.list_jobs(None, config.num_jobs + 100)?;
    let mut sched_latencies = Vec::with_capacity(jobs.len());
    for job in &jobs {
        if let Some(lat) = job.scheduling_latency_ms {
            sched_latencies.push(lat);
        } else if let (Some(started), created) = (job.started_at, job.created_at) {
            let lat = (started - created).num_microseconds().unwrap_or(0) as f64 / 1000.0;
            sched_latencies.push(lat);
        }
    }

    sched_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sched_p50_ms = percentile(&sched_latencies, 0.50);
    let sched_p95_ms = percentile(&sched_latencies, 0.95);
    let sched_p99_ms = percentile(&sched_latencies, 0.99);

    for d in daemons {
        d.kill();
    }

    Ok(BenchmarkReport {
        jobs: config.num_jobs,
        workers: config.num_workers,
        total_duration,
        throughput_jobs_per_sec: throughput,
        submit_p50_ms,
        submit_p95_ms,
        submit_p99_ms,
        sched_p50_ms,
        sched_p95_ms,
        sched_p99_ms,
        p50_ms: sched_p50_ms,
        p95_ms: sched_p95_ms,
        p99_ms: sched_p99_ms,
    })
}
