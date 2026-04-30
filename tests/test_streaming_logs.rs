use relay::api::create_router;
use relay::models::{JobStatus, Priority, ResourceRequirements, SubmitJobRequest};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use relay::worker::{WorkerConfig, WorkerDaemon};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, NamedTempFile};
use tokio::time::sleep;

#[tokio::test]
async fn test_realtime_streaming_logs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_db = NamedTempFile::new()?;
    let store = Arc::new(Store::new(temp_db.path().to_str().unwrap())?);
    let scheduler = Scheduler::new(
        store.clone(),
        SchedulerConfig {
            heartbeat_timeout: Duration::from_secs(5),
            default_lease_duration: Duration::from_secs(10),
            reap_interval: Duration::from_secs(1),
        },
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_url = format!("http://{}", addr);

    let app = create_router(scheduler.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let log_dir = tempdir()?;
    let log_path_str = log_dir.path().to_str().unwrap().to_string();

    let mut w_cfg = WorkerConfig::default();
    w_cfg.worker_id = "stream-worker-1".to_string();
    w_cfg.scheduler_url = server_url;
    w_cfg.poll_interval = Duration::from_millis(50);
    w_cfg.heartbeat_interval = Duration::from_millis(500);
    w_cfg.renew_interval = Duration::from_millis(500);
    w_cfg.output_dir = log_path_str.clone();

    let daemon = Arc::new(WorkerDaemon::new(w_cfg));
    let daemon_clone = daemon.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = daemon_clone.run().await;
    });

    let submit_resp = scheduler.submit_job(SubmitJobRequest {
        command: "echo 'LIVE_CHUNK_1' && sleep 1 && echo 'LIVE_CHUNK_2'".to_string(),
        args: vec![],
        cwd: None,
        env: HashMap::new(),
        priority: Priority::Normal,
        max_retries: 1,
        timeout_seconds: Some(10),
        resources: ResourceRequirements::default(),
        depends_on: vec![],
        name: Some("stream-test".to_string()),
    })?;
    let job_id = submit_resp.job_id;

    // Check log file while child process is sleeping
    let log_file = log_dir.path().join(format!("out.{}", job_id));
    let mut saw_chunk_1_early = false;

    for _ in 0..20 {
        sleep(Duration::from_millis(150)).await;
        if log_file.exists() {
            if let Ok(content) = fs::read_to_string(&log_file) {
                if let Some(body) = content.split("--- LIVE OUTPUT ---").nth(1) {
                    if body.contains("LIVE_CHUNK_1") && !body.contains("LIVE_CHUNK_2") {
                        saw_chunk_1_early = true;
                        break;
                    }
                }
            }
        }
    }

    assert!(
        saw_chunk_1_early,
        "Log output must stream in real-time before process completion"
    );

    // Now wait for completion
    for _ in 0..30 {
        sleep(Duration::from_millis(100)).await;
        if let Ok(Some(j)) = store.get_job(&job_id) {
            if j.status == JobStatus::Succeeded {
                break;
            }
        }
    }

    let final_content = fs::read_to_string(&log_file)?;
    assert!(final_content.contains("LIVE_CHUNK_1"));
    assert!(final_content.contains("LIVE_CHUNK_2"));
    assert!(final_content.contains("=== Exit Code: 0 ==="));

    daemon.kill();
    worker_handle.abort();

    Ok(())
}
