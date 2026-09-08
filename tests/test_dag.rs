use relay::models::{CompleteJobRequest, JobStatus, SubmitJobRequest, WorkerCapacity};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_dag_dependency_resolution() {
    let temp_db = NamedTempFile::new().unwrap();
    let db_path = temp_db.path().to_str().unwrap();

    let store = Arc::new(Store::new(db_path).unwrap());
    let sched_config = SchedulerConfig::default();
    let scheduler = Scheduler::new(store.clone(), sched_config);

    let job_a = scheduler
        .submit_job(SubmitJobRequest {
            command: "echo 'step a'".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: relay::models::Priority::Normal,
            max_retries: 1,
            timeout_seconds: None,
            resources: relay::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some("job-a".to_string()),
        })
        .unwrap();

    let job_b = scheduler
        .submit_job(SubmitJobRequest {
            command: "echo 'step b'".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: relay::models::Priority::Normal,
            max_retries: 1,
            timeout_seconds: None,
            resources: relay::models::ResourceRequirements::default(),
            depends_on: vec![],
            name: Some("job-b".to_string()),
        })
        .unwrap();

    let job_c = scheduler
        .submit_job(SubmitJobRequest {
            command: "echo 'step c'".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: relay::models::Priority::High,
            max_retries: 1,
            timeout_seconds: None,
            resources: relay::models::ResourceRequirements::default(),
            depends_on: vec![job_a.job_id.clone(), job_b.job_id.clone()],
            name: Some("job-c".to_string()),
        })
        .unwrap();

    assert_eq!(job_a.status, JobStatus::Queued);
    assert_eq!(job_b.status, JobStatus::Queued);
    assert_eq!(job_c.status, JobStatus::Blocked);

    let cap = WorkerCapacity {
        cpus: 4,
        memory_mb: 4096,
        labels: HashMap::new(),
    };

    let claimed_a = store.claim_jobs("worker-01", &cap, 1, Duration::from_secs(30)).unwrap();
    scheduler
        .complete_job(
            &job_a.job_id,
            CompleteJobRequest {
                attempt_id: claimed_a[0].1.id.clone(),
                exit_code: 0,
                stdout: "done a".to_string(),
                stderr: String::new(),
                runtime_ms: 100,
            },
        )
        .unwrap();

    let c_state = store.get_job(&job_c.job_id).unwrap().unwrap();
    assert_eq!(c_state.status, JobStatus::Blocked);

    let claimed_b = store.claim_jobs("worker-01", &cap, 1, Duration::from_secs(30)).unwrap();
    scheduler
        .complete_job(
            &job_b.job_id,
            CompleteJobRequest {
                attempt_id: claimed_b[0].1.id.clone(),
                exit_code: 0,
                stdout: "done b".to_string(),
                stderr: String::new(),
                runtime_ms: 100,
            },
        )
        .unwrap();

    let c_state_unblocked = store.get_job(&job_c.job_id).unwrap().unwrap();
    assert_eq!(c_state_unblocked.status, JobStatus::Queued);
}
