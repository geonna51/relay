use crate::api::create_router;
use crate::models::SubmitJobRequest;
use crate::scheduler::{Scheduler, SchedulerConfig};
use crate::store::Store;
use crate::worker::{WorkerConfig, WorkerDaemon};
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::time::sleep;
use tracing::{info, warn};

pub struct ChaosConfig {
    pub num_jobs: usize,
    pub num_workers: usize,
    pub duration_secs: u64,
    pub kill_interval_secs: u64,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            num_jobs: 50,
            num_workers: 4,
            duration_secs: 30,
            kill_interval_secs: 4,
        }
    }
}

#[derive(Debug)]
pub struct ChaosReport {
    pub jobs_submitted: usize,
    pub jobs_succeeded: usize,
    pub jobs_failed: usize,
    pub jobs_lost: usize,
    pub workers_killed: usize,
    pub total_duration: Duration,
    pub success: bool,
}

pub async fn run_chaos_test(config: ChaosConfig) -> Result<ChaosReport, Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting Chaos Failure Injection Test");
    let temp_db = NamedTempFile::new()?;
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path)?);
    let sched_config = SchedulerConfig {
        heartbeat_timeout: Duration::from_millis(1500),
        default_lease_duration: Duration::from_millis(2500),
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

    let mut worker_controls = Vec::new();
    for i in 0..config.num_workers {
        let worker_id = format!("chaos-worker-{:02}", i + 1);
        let mut w_cfg = WorkerConfig::default();
        w_cfg.worker_id = worker_id.clone();
        w_cfg.scheduler_url = server_url.clone();
        w_cfg.heartbeat_interval = Duration::from_millis(500);
        w_cfg.renew_interval = Duration::from_millis(800);
        w_cfg.poll_interval = Duration::from_millis(200);

        let daemon = Arc::new(WorkerDaemon::new(w_cfg));
        let daemon_clone = daemon.clone();
        let handle = tokio::spawn(async move {
            let _ = daemon_clone.run().await;
        });

        worker_controls.push((worker_id, daemon, handle));
    }

    info!("Submitting {} test jobs...", config.num_jobs);
    for i in 0..config.num_jobs {
        let req = SubmitJobRequest {
            command: format!("echo 'chaos job {}' && sleep 0.3", i),
            args: vec![],
            cwd: None,
            env: std::collections::HashMap::new(),
            priority: crate::models::Priority::Normal,
            max_retries: 5,
            timeout_seconds: Some(10),
            resources: crate::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some(format!("chaos-job-{}", i)),
        };
        scheduler.submit_job(req)?;
    }

    let start_time = Instant::now();
    let mut workers_killed = 0usize;

    let chaos_duration = Duration::from_secs(config.duration_secs);
    let kill_interval = Duration::from_secs(config.kill_interval_secs);
    let mut rng = rand::rng();

    while start_time.elapsed() < chaos_duration {
        sleep(kill_interval).await;

        let victim_idx = rng.random_range(0..worker_controls.len());
        let victim_id = worker_controls[victim_idx].0.clone();

        warn!("[CHAOS] Injecting failure: Abruptly killing {}", victim_id);
        // daemon.kill() aborts heartbeat loop, execution tasks, and active lease renewals immediately
        worker_controls[victim_idx].1.kill();
        worker_controls[victim_idx].2.abort();
        workers_killed += 1;

        sleep(Duration::from_millis(1500)).await;
        info!("[CHAOS] Reviving {}", victim_id);
        let mut w_cfg = WorkerConfig::default();
        w_cfg.worker_id = victim_id.clone();
        w_cfg.scheduler_url = server_url.clone();
        w_cfg.heartbeat_interval = Duration::from_millis(500);
        w_cfg.renew_interval = Duration::from_millis(800);
        w_cfg.poll_interval = Duration::from_millis(200);

        let new_daemon = Arc::new(WorkerDaemon::new(w_cfg));
        let new_daemon_clone = Arc::clone(&new_daemon);
        let new_handle = tokio::spawn(async move {
            let _ = new_daemon_clone.run().await;
        });

        worker_controls[victim_idx] = (victim_id, new_daemon, new_handle);

        let metrics = store.get_metrics()?;
        if metrics.jobs_succeeded + metrics.jobs_failed >= config.num_jobs {
            info!("All jobs finished ahead of chaos deadline");
            break;
        }
    }

    let drain_start = Instant::now();
    while drain_start.elapsed() < Duration::from_secs(20) {
        let metrics = store.get_metrics()?;
        if metrics.jobs_succeeded + metrics.jobs_failed >= config.num_jobs {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    let metrics = store.get_metrics()?;
    let total_duration = start_time.elapsed();
    let jobs_lost = config
        .num_jobs
        .saturating_sub(metrics.jobs_succeeded + metrics.jobs_failed);

    let report = ChaosReport {
        jobs_submitted: config.num_jobs,
        jobs_succeeded: metrics.jobs_succeeded,
        jobs_failed: metrics.jobs_failed,
        jobs_lost,
        workers_killed,
        total_duration,
        success: jobs_lost == 0,
    };

    info!(
        "Chaos Test Summary: Submitted={}, Succeeded={}, Failed={}, Lost={}, Workers Killed={}, Duration={:.1}s",
        report.jobs_submitted,
        report.jobs_succeeded,
        report.jobs_failed,
        report.jobs_lost,
        report.workers_killed,
        report.total_duration.as_secs_f64()
    );

    for (_, daemon, handle) in worker_controls {
        daemon.kill();
        handle.abort();
    }

    Ok(report)
}
