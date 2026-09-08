use relay::api::create_router;
use relay::models::{JobStatus, Priority, ResourceRequirements, SubmitJobRequest};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use relay::worker::{WorkerConfig, WorkerDaemon};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, NamedTempFile};
use tokio::time::sleep;

#[tokio::test]
async fn test_worker_kill_terminates_lease_renewal() -> Result<(), Box<dyn std::error::Error>> {
    let temp_db = NamedTempFile::new()?;
    let store = Arc::new(Store::new(temp_db.path().to_str().unwrap())?);
    let scheduler = Scheduler::new(
        store.clone(),
        SchedulerConfig {
            heartbeat_timeout: Duration::from_secs(2),
            default_lease_duration: Duration::from_millis(800),
            reap_interval: Duration::from_millis(200),
        },
    );
    let _reaper = scheduler.start_background_tasks();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_url = format!("http://{}", addr);

    let app = create_router(scheduler.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let log_dir = tempdir()?;
    let mut w_cfg = WorkerConfig::default();
    w_cfg.worker_id = "kill-test-worker".to_string();
    w_cfg.scheduler_url = server_url;
    w_cfg.poll_interval = Duration::from_millis(50);
    w_cfg.heartbeat_interval = Duration::from_millis(500);
    w_cfg.renew_interval = Duration::from_millis(200);
    w_cfg.output_dir = log_dir.path().to_str().unwrap().to_string();

    let daemon = Arc::new(WorkerDaemon::new(w_cfg));
    let daemon_clone = daemon.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = daemon_clone.run().await;
    });

    let submit_resp = scheduler.submit_job(SubmitJobRequest {
        command: "sleep 10".to_string(),
        args: vec![],
        cwd: None,
        env: HashMap::new(),
        priority: Priority::Normal,
        max_retries: 3,
        timeout_seconds: Some(30),
        resources: ResourceRequirements::default(),
        depends_on: vec![],
        name: Some("kill-test-job".to_string()),
    })?;
    let job_id = submit_resp.job_id;

    let mut running = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(100)).await;
        if let Ok(Some(j)) = store.get_job(&job_id) {
            if j.status == JobStatus::Running || j.status == JobStatus::Assigned {
                running = true;
                break;
            }
        }
    }
    assert!(running, "Worker should have claimed the job");

    sleep(Duration::from_millis(300)).await;

    daemon.kill();
    worker_handle.abort();

    sleep(Duration::from_millis(1500)).await;

    let reaped_job = store.get_job(&job_id)?.expect("Job must exist");
    assert_eq!(
        reaped_job.status,
        JobStatus::Retrying,
        "Lease renewal must terminate upon worker kill, allowing job to be reaped and requeued"
    );

    Ok(())
}
