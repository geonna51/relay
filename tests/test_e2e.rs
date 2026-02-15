use relay::api::create_router;
use relay::client::RelayClient;
use relay::models::{JobStatus, SubmitJobRequest};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use relay::worker::{WorkerConfig, WorkerDaemon};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_end_to_end_job_lifecycle() {
    let temp_db = NamedTempFile::new().unwrap();
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path).unwrap());
    let sched_config = SchedulerConfig::default();
    let scheduler = Scheduler::new(store.clone(), sched_config);
    let _reaper = scheduler.start_background_tasks();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_url = format!("http://{}", addr);

    let app = create_router(scheduler.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let client = RelayClient::new(&server_url);

    let submit_req = SubmitJobRequest {
        command: "echo 'hello relay'".to_string(),
        args: vec![],
        cwd: None,
        env: HashMap::new(),
        priority: relay::models::Priority::High,
        max_retries: 3,
        timeout_seconds: Some(10),
        resources: relay::models::ResourceRequirements::default(),
        depends_on: vec![],
        name: Some("test-job-e2e".to_string()),
    };

    let submit_resp = client.submit(submit_req).await.unwrap();
    assert_eq!(submit_resp.status, JobStatus::Queued);

    let job = client.status(&submit_resp.job_id).await.unwrap();
    assert_eq!(job.status, JobStatus::Queued);

    let mut w_cfg = WorkerConfig::default();
    w_cfg.worker_id = "test-worker-01".to_string();
    w_cfg.scheduler_url = server_url.clone();
    w_cfg.oneshot = true;

    let worker = WorkerDaemon::new(w_cfg);
    worker.run().await.unwrap();

    let mut completed_job = None;
    for i in 0..30 {
        sleep(Duration::from_millis(100)).await;
        let j = client.status(&submit_resp.job_id).await.unwrap();
        println!("Poll {} status: {:?}, exit_code: {:?}", i, j.status, j.exit_code);
        if j.status == JobStatus::Succeeded {
            completed_job = Some(j);
            break;
        }
    }

    let finished = completed_job.expect("Job should have reached SUCCEEDED");
    assert_eq!(finished.exit_code, Some(0));
    assert!(finished.stdout.unwrap().contains("hello relay"));
    assert_eq!(finished.worker_id.as_deref(), Some("test-worker-01"));
}
