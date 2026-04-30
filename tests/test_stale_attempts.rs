use relay::models::{CompleteJobRequest, JobStatus, SubmitJobRequest, WorkerCapacity, WorkerRegisterRequest};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_stale_attempt_rejection() {
    let temp_db = NamedTempFile::new().unwrap();
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path).unwrap());
    let sched_config = SchedulerConfig {
        heartbeat_timeout: Duration::from_millis(500),
        default_lease_duration: Duration::from_millis(600),
        reap_interval: Duration::from_millis(100),
    };

    let scheduler = Scheduler::new(store.clone(), sched_config);
    let _reaper = scheduler.start_background_tasks();

    scheduler
        .register_worker(WorkerRegisterRequest {
            worker_id: "worker-a".to_string(),
            cpus: 4,
            memory_mb: 4096,
            labels: HashMap::new(),
        })
        .unwrap();

    scheduler
        .register_worker(WorkerRegisterRequest {
            worker_id: "worker-b".to_string(),
            cpus: 4,
            memory_mb: 4096,
            labels: HashMap::new(),
        })
        .unwrap();

    let sub = scheduler
        .submit_job(SubmitJobRequest {
            command: "echo 'stale test'".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: relay::models::Priority::Normal,
            max_retries: 3,
            timeout_seconds: Some(10),
            resources: relay::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some("stale-job".to_string()),
        })
        .unwrap();

    let cap = WorkerCapacity {
        cpus: 4,
        memory_mb: 4096,
        labels: HashMap::new(),
    };

    let claimed_a = store
        .claim_jobs("worker-a", &cap, 1, Duration::from_millis(500))
        .unwrap();
    let (_job_a, att_a) = &claimed_a[0];
    let attempt_1_id = att_a.id.clone();

    // Allow Worker A's lease to expire and wait out backoff (2s)
    sleep(Duration::from_millis(800)).await;
    sleep(Duration::from_millis(2200)).await;

    let claimed_b = store
        .claim_jobs("worker-b", &cap, 1, Duration::from_secs(10))
        .unwrap();
    let (_, att_b) = &claimed_b[0];
    let attempt_2_id = att_b.id.clone();
    assert_ne!(attempt_1_id, attempt_2_id);

    let resp_a = scheduler
        .complete_job(
            &sub.job_id,
            CompleteJobRequest {
                attempt_id: attempt_1_id,
                exit_code: 0,
                stdout: "worker-a late output".to_string(),
                stderr: String::new(),
                runtime_ms: 1000,
            },
        )
        .unwrap();

    assert_eq!(resp_a.status, "stale_ignored");

    let job_mid = store.get_job(&sub.job_id).unwrap().unwrap();
    assert_eq!(job_mid.status, JobStatus::Assigned);
    assert_eq!(job_mid.worker_id.as_deref(), Some("worker-b"));

    let resp_b = scheduler
        .complete_job(
            &sub.job_id,
            CompleteJobRequest {
                attempt_id: attempt_2_id,
                exit_code: 0,
                stdout: "worker-b valid output".to_string(),
                stderr: String::new(),
                runtime_ms: 500,
            },
        )
        .unwrap();

    assert_eq!(resp_b.status, "accepted");
    let job_final = store.get_job(&sub.job_id).unwrap().unwrap();
    assert_eq!(job_final.status, JobStatus::Succeeded);
    assert!(job_final.stdout.unwrap().contains("worker-b valid output"));
}
