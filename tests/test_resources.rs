use relay::models::{Priority, ResourceRequirements, SubmitJobRequest, WorkerCapacity};
use relay::scheduler::{Scheduler, SchedulerConfig};
use relay::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_batch_single_large_job_saturation() -> Result<(), Box<dyn std::error::Error>> {
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

    for i in 0..3 {
        scheduler.submit_job(SubmitJobRequest {
            command: format!("echo job-{}", i),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            priority: Priority::Normal,
            max_retries: 3,
            timeout_seconds: Some(30),
            resources: ResourceRequirements {
                cpus: 4,
                memory_mb: 2048,
                labels: HashMap::new(),
            },
            depends_on: vec![],
            name: Some(format!("res-job-{}", i)),
        })?;
    }

    let capacity = WorkerCapacity {
        cpus: 4,
        memory_mb: 4096,
        labels: HashMap::new(),
    };

    let claimed = store.claim_jobs("worker-4cpu", &capacity, 4, Duration::from_secs(10))?;
    assert_eq!(
        claimed.len(),
        1,
        "A 4-CPU worker must not be assigned multiple 4-CPU jobs in a single batch"
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_multiple_fitting_jobs() -> Result<(), Box<dyn std::error::Error>> {
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

    let j1 = scheduler.submit_job(SubmitJobRequest {
        command: "echo job-2cpu-1".to_string(),
        args: vec![],
        cwd: None,
        env: HashMap::new(),
        priority: Priority::Normal,
        max_retries: 3,
        timeout_seconds: Some(30),
        resources: ResourceRequirements {
            cpus: 2,
            memory_mb: 1024,
            labels: HashMap::new(),
        },
        depends_on: vec![],
        name: Some("res-2cpu-1".to_string()),
    })?;

    let j2 = scheduler.submit_job(SubmitJobRequest {
        command: "echo job-2cpu-2".to_string(),
        args: vec![],
        cwd: None,
        env: HashMap::new(),
        priority: Priority::Normal,
        max_retries: 3,
        timeout_seconds: Some(30),
        resources: ResourceRequirements {
            cpus: 2,
            memory_mb: 1024,
            labels: HashMap::new(),
        },
        depends_on: vec![],
        name: Some("res-2cpu-2".to_string()),
    })?;

    let _j3 = scheduler.submit_job(SubmitJobRequest {
        command: "echo job-extra-4cpu".to_string(),
        args: vec![],
        cwd: None,
        env: HashMap::new(),
        priority: Priority::Normal,
        max_retries: 3,
        timeout_seconds: Some(30),
        resources: ResourceRequirements {
            cpus: 4,
            memory_mb: 1024,
            labels: HashMap::new(),
        },
        depends_on: vec![],
        name: Some("res-extra".to_string()),
    })?;

    let capacity = WorkerCapacity {
        cpus: 4,
        memory_mb: 4096,
        labels: HashMap::new(),
    };
    let claimed_2cpu = store.claim_jobs("worker-4cpu", &capacity, 4, Duration::from_secs(10))?;
    let claimed_ids: Vec<String> = claimed_2cpu.iter().map(|(j, _)| j.id.clone()).collect();

    assert_eq!(claimed_2cpu.len(), 2);
    assert!(claimed_ids.contains(&j1.job_id));
    assert!(claimed_ids.contains(&j2.job_id));

    Ok(())
}
