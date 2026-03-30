use relay::models::{CompleteJobRequest, JobStatus, SubmitJobRequest, WorkerCapacity};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_max_retries_and_failure() {
    let temp_db = NamedTempFile::new().unwrap();
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path).unwrap());
    let sched_config = SchedulerConfig::default();
    let scheduler = Scheduler::new(store.clone(), sched_config);

    // Job with max_retries = 1 (can only retry once, total 2 attempts)
    let sub = scheduler
        .submit_job(SubmitJobRequest {
            command: "exit 1".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: relay::models::Priority::Normal,
            max_retries: 1,
            timeout_seconds: None,
            resources: relay::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some("retry-job".to_string()),
        })
        .unwrap();

    let cap = WorkerCapacity {
        cpus: 1,
        memory_mb: 512,
        labels: HashMap::new(),
    };

    // Attempt 1
    let claimed_1 = store.claim_jobs("worker-1", &cap, 1, Duration::from_secs(10)).unwrap();
    assert_eq!(claimed_1.len(), 1);
    let att_1_id = claimed_1[0].1.id.clone();

    let res_1 = scheduler
        .complete_job(
            &sub.job_id,
            CompleteJobRequest {
                attempt_id: att_1_id,
                exit_code: 1,
                stdout: String::new(),
                stderr: "crash 1".to_string(),
                runtime_ms: 50,
            },
        )
        .unwrap();

    assert_eq!(res_1.status, "retrying");
    let job_after_1 = store.get_job(&sub.job_id).unwrap().unwrap();
    assert_eq!(job_after_1.status, JobStatus::Retrying);
    assert_eq!(job_after_1.retry_count, 1);

    // Wait for backoff to elapse (2^1 = 2 seconds)
    sleep(Duration::from_millis(2100)).await;

    // Attempt 2 (retry claimed)
    let claimed_2 = store.claim_jobs("worker-1", &cap, 1, Duration::from_secs(10)).unwrap();
    assert_eq!(claimed_2.len(), 1);
    let att_2_id = claimed_2[0].1.id.clone();

    let res_2 = scheduler
        .complete_job(
            &sub.job_id,
            CompleteJobRequest {
                attempt_id: att_2_id,
                exit_code: 1,
                stdout: String::new(),
                stderr: "crash 2".to_string(),
                runtime_ms: 50,
            },
        )
        .unwrap();

    assert_eq!(res_2.status, "accepted");

    // Now max_retries (1) has been reached -> Final status FAILED
    let job_final = store.get_job(&sub.job_id).unwrap().unwrap();
    assert_eq!(job_final.status, JobStatus::Failed);
    assert_eq!(job_final.exit_code, Some(1));
}
