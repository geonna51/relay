use relay::models::{JobStatus, SubmitJobRequest, WorkerCapacity};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_scheduler_crash_recovery() {
    let temp_db = NamedTempFile::new().unwrap();
    let db_path = temp_db.path().to_str().unwrap().to_string();

    let job_id_1;
    let job_id_2;

    // Run scheduler 1
    {
        let store = Arc::new(Store::new(&db_path).unwrap());
        let sched = Scheduler::new(store.clone(), SchedulerConfig::default());

        let j1 = sched
            .submit_job(SubmitJobRequest {
                command: "echo 'job 1'".to_string(),
                args: vec![],
                cwd: None,
                env: HashMap::new(),
                priority: relay::models::Priority::Normal,
                max_retries: 3,
                timeout_seconds: None,
                resources: relay::models::ResourceRequirements::default(),
                depends_on: vec![],
                name: Some("recovery-job-1".to_string()),
            })
            .unwrap();
        job_id_1 = j1.job_id;

        let j2 = sched
            .submit_job(SubmitJobRequest {
                command: "echo 'job 2'".to_string(),
                args: vec![],
                cwd: None,
                env: HashMap::new(),
                priority: relay::models::Priority::Normal,
                max_retries: 3,
                timeout_seconds: None,
                resources: relay::models::ResourceRequirements::default(),
                depends_on: vec![],
                name: Some("recovery-job-2".to_string()),
            })
            .unwrap();
        job_id_2 = j2.job_id;

        // Claim job 1 with a short lease
        let cap = WorkerCapacity {
            cpus: 2,
            memory_mb: 2048,
            labels: HashMap::new(),
        };
        let _ = store.claim_jobs("worker-crash", &cap, 1, Duration::from_millis(200)).unwrap();

        // Job 2 remains Queued
        // Now simulate scheduler crash: drop store and scheduler!
    }

    // Wait for the active lease of job 1 to expire in real time (> 200ms)
    sleep(Duration::from_millis(400)).await;

    // Scheduler 2 restarts with the same database
    {
        let store2 = Arc::new(Store::new(&db_path).unwrap());
        let sched2 = Scheduler::new(store2.clone(), SchedulerConfig::default());

        // Run recovery reconciler
        sched2.recover_state().unwrap();

        // Job 1 should have been reaped from expired lease and moved to RETRYING
        let state_j1 = store2.get_job(&job_id_1).unwrap().unwrap();
        assert_eq!(state_j1.status, JobStatus::Retrying);
        assert_eq!(state_j1.retry_count, 1);

        // Job 2 should still be QUEUED
        let state_j2 = store2.get_job(&job_id_2).unwrap().unwrap();
        assert_eq!(state_j2.status, JobStatus::Queued);
    }
}
