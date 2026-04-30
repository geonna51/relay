use relay::models::{JobStatus, Priority, ResourceRequirements, SubmitJobRequest, WorkerCapacity};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::{CompleteJobOutcome, Store};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_cancellation_race_condition() -> Result<(), Box<dyn std::error::Error>> {
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
        name: Some("test-cancel-race".to_string()),
    })?;
    let job_id = submit_resp.job_id;

    let capacity = WorkerCapacity {
        cpus: 4,
        memory_mb: 4096,
        labels: HashMap::new(),
    };
    let claimed = store.claim_jobs("worker-1", &capacity, 1, Duration::from_secs(10))?;
    assert_eq!(claimed.len(), 1);
    let (claimed_job, attempt) = &claimed[0];
    assert_eq!(claimed_job.id, job_id);
    let attempt_id = attempt.id.clone();

    let job_before_cancel = store.get_job(&job_id)?.unwrap();
    assert_eq!(job_before_cancel.status, JobStatus::Assigned);
    assert_eq!(job_before_cancel.current_attempt_id.as_deref(), Some(attempt_id.as_str()));

    let cancelled_job = store.cancel_job(&job_id)?;
    assert_eq!(cancelled_job.status, JobStatus::Cancelled);
    assert_eq!(cancelled_job.current_attempt_id, None);
    assert_eq!(cancelled_job.worker_id, None);

    let job_after_cancel = store.get_job(&job_id)?.unwrap();
    assert_eq!(job_after_cancel.status, JobStatus::Cancelled);
    assert_eq!(job_after_cancel.current_attempt_id, None);

    let outcome = store.complete_job(
        &job_id,
        &attempt_id,
        0,
        "finished late",
        "",
        5000,
    )?;

    assert!(matches!(outcome, CompleteJobOutcome::StaleAttemptIgnored));

    let final_job = store.get_job(&job_id)?.unwrap();
    assert_eq!(final_job.status, JobStatus::Cancelled);
    assert!(final_job.completed_at.is_some());

    Ok(())
}
