use relay::models::{JobStatus, SubmitJobRequest, WorkerCapacity, WorkerRegisterRequest};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_lease_expiration_and_reassignment() {
    let temp_db = NamedTempFile::new().unwrap();
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path).unwrap());
    let sched_config = SchedulerConfig {
        heartbeat_timeout: Duration::from_millis(500),
        default_lease_duration: Duration::from_millis(800),
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
            command: "echo 'lease test'".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: relay::models::Priority::Normal,
            max_retries: 3,
            timeout_seconds: Some(10),
            resources: relay::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some("lease-job".to_string()),
        })
        .unwrap();

    let cap = WorkerCapacity {
        cpus: 4,
        memory_mb: 4096,
        labels: HashMap::new(),
    };
    let claimed_a = store
        .claim_jobs("worker-a", &cap, 1, Duration::from_millis(600))
        .unwrap();
    assert_eq!(claimed_a.len(), 1);
    let (job_a, att_a) = &claimed_a[0];
    assert_eq!(job_a.id, sub.job_id);
    assert_eq!(att_a.attempt_number, 1);

    // Wait for lease to expire and reaper to execute
    sleep(Duration::from_millis(900)).await;

    let job_reaped = store.get_job(&sub.job_id).unwrap().unwrap();
    assert_eq!(job_reaped.status, JobStatus::Retrying);
    assert_eq!(job_reaped.retry_count, 1);

    // Wait out exponential backoff (2^1 = 2 seconds)
    sleep(Duration::from_millis(2200)).await;

    let claimed_b = store
        .claim_jobs("worker-b", &cap, 1, Duration::from_secs(10))
        .unwrap();
    assert_eq!(claimed_b.len(), 1);
    let (job_b, att_b) = &claimed_b[0];
    assert_eq!(job_b.id, sub.job_id);
    assert_eq!(att_b.attempt_number, 2);
    assert_eq!(job_b.worker_id.as_deref(), Some("worker-b"));
}
