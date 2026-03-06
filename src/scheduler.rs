use crate::models::{
    AssignedJob, BatchSubmitRequest, BatchSubmitResponse, CompleteJobRequest, CompleteJobResponse,
    Job, JobStatus, MetricsResponse, RenewLeaseRequest, RenewLeaseResponse, SubmitJobRequest,
    SubmitJobResponse, Worker, WorkerCapacity, WorkerPollRequest, WorkerPollResponse,
    WorkerRegisterRequest, WorkerStatus,
};
use crate::store::{CompleteJobOutcome, Store, StoreError};
use chrono::Utc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

pub struct SchedulerConfig {
    pub heartbeat_timeout: Duration,
    pub default_lease_duration: Duration,
    pub reap_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(15),
            default_lease_duration: Duration::from_secs(30),
            reap_interval: Duration::from_secs(1),
        }
    }
}

pub struct Scheduler {
    store: Arc<Store>,
    config: SchedulerConfig,
    running: AtomicBool,
}

impl Scheduler {
    pub fn new(store: Arc<Store>, config: SchedulerConfig) -> Arc<Self> {
        Arc::new(Self {
            store,
            config,
            running: AtomicBool::new(true),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    pub fn recover_state(&self) -> Result<(), StoreError> {
        info!("Running scheduler recovery on startup...");
        let reaped = self.store.reap_expired_leases()?;
        if !reaped.is_empty() {
            info!("Recovered {} orphaned/expired jobs from previous run", reaped.len());
        }
        let dead = self.store.reap_dead_workers(self.config.heartbeat_timeout)?;
        if !dead.is_empty() {
            info!("Marked {} inactive workers as unavailable on startup", dead.len());
        }
        Ok(())
    }

    pub fn start_background_tasks(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let scheduler = Arc::clone(self);
        tokio::spawn(async move {
            info!("Scheduler background reaper started");
            while scheduler.running.load(Ordering::Relaxed) {
                sleep(scheduler.config.reap_interval).await;

                // 1. Detect dead workers (missed heartbeats)
                match scheduler.store.reap_dead_workers(scheduler.config.heartbeat_timeout) {
                    Ok(dead) => {
                        for worker_id in dead {
                            warn!("Worker {} missed heartbeat. Marked UNAVAILABLE.", worker_id);
                        }
                    }
                    Err(e) => warn!("Error reaping dead workers: {}", e),
                }

                // 2. Detect expired leases
                match scheduler.store.reap_expired_leases() {
                    Ok(reaped) => {
                        for (job, worker_id) in reaped {
                            warn!(
                                "Lease expired for job {} on worker {}. Requeueing (status={:?}, retry_count={})",
                                job.id, worker_id, job.status, job.retry_count
                            );
                        }
                    }
                    Err(e) => warn!("Error reaping expired leases: {}", e),
                }
            }
        })
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    pub fn submit_job(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, StoreError> {
        let job_id = format!("job-{}", uuid::Uuid::new_v4().simple());
        let status = if req.depends_on.is_empty() {
            JobStatus::Queued
        } else {
            JobStatus::Blocked
        };

        let job = Job {
            id: job_id.clone(),
            name: req.name,
            command: req.command,
            args: req.args,
            cwd: req.cwd,
            env: req.env,
            priority: req.priority,
            status,
            max_retries: req.max_retries,
            retry_count: 0,
            timeout_seconds: req.timeout_seconds,
            resources: req.resources,
            depends_on: req.depends_on,
            worker_id: None,
            current_attempt_id: None,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            lease_expires_at: None,
            next_retry_at: None,
            exit_code: None,
            stdout: None,
            stderr: None,
            runtime_ms: None,
        };

        self.store.save_job(&job)?;
        Ok(SubmitJobResponse { job_id, status })
    }

    pub fn submit_batch(&self, req: BatchSubmitRequest) -> Result<BatchSubmitResponse, StoreError> {
        let mut job_ids = Vec::with_capacity(req.jobs.len());
        for job_req in req.jobs {
            let res = self.submit_job(job_req)?;
            job_ids.push(res.job_id);
        }
        let count = job_ids.len();
        Ok(BatchSubmitResponse { job_ids, count })
    }

    pub fn register_worker(&self, req: WorkerRegisterRequest) -> Result<(), StoreError> {
        let worker = Worker {
            id: req.worker_id,
            cpus: req.cpus,
            memory_mb: req.memory_mb,
            labels: req.labels,
            status: WorkerStatus::Active,
            active_jobs: 0,
            last_heartbeat: Utc::now(),
            registered_at: Utc::now(),
        };
        self.store.register_worker(&worker)
    }

    pub fn worker_heartbeat(&self, worker_id: &str) -> Result<bool, StoreError> {
        self.store.worker_heartbeat(worker_id)
    }

    pub fn poll_work(
        &self,
        worker_id: &str,
        req: WorkerPollRequest,
    ) -> Result<WorkerPollResponse, StoreError> {
        let _ = self.store.worker_heartbeat(worker_id);

        let capacity = WorkerCapacity {
            cpus: req.available_cpus,
            memory_mb: req.available_memory_mb,
            labels: req.labels,
        };

        let claimed = self.store.claim_jobs(
            worker_id,
            &capacity,
            req.max_jobs,
            self.config.default_lease_duration,
        )?;

        let jobs = claimed
            .into_iter()
            .map(|(job, attempt)| AssignedJob {
                job,
                attempt_id: attempt.id,
                lease_duration_seconds: self.config.default_lease_duration.as_secs(),
            })
            .collect();

        Ok(WorkerPollResponse { jobs })
    }

    pub fn renew_lease(
        &self,
        job_id: &str,
        req: RenewLeaseRequest,
    ) -> Result<RenewLeaseResponse, StoreError> {
        let duration = req
            .duration_seconds
            .map(Duration::from_secs)
            .unwrap_or(self.config.default_lease_duration);

        let new_expires_at = self.store.renew_lease(job_id, &req.attempt_id, duration)?;
        Ok(RenewLeaseResponse {
            status: "renewed".to_string(),
            new_expires_at,
        })
    }

    pub fn complete_job(
        &self,
        job_id: &str,
        req: CompleteJobRequest,
    ) -> Result<CompleteJobResponse, StoreError> {
        let outcome = self.store.complete_job(
            job_id,
            &req.attempt_id,
            req.exit_code,
            &req.stdout,
            &req.stderr,
            req.runtime_ms,
        )?;

        match outcome {
            CompleteJobOutcome::Accepted(_) => Ok(CompleteJobResponse {
                status: "accepted".to_string(),
                message: None,
            }),
            CompleteJobOutcome::Retrying(_) => Ok(CompleteJobResponse {
                status: "retrying".to_string(),
                message: Some("Job failed but scheduled for retry with backoff".to_string()),
            }),
            CompleteJobOutcome::StaleAttemptIgnored => {
                warn!(
                    "Rejected stale attempt {} for job {}. Job was reassigned or cancelled.",
                    req.attempt_id, job_id
                );
                Ok(CompleteJobResponse {
                    status: "stale_ignored".to_string(),
                    message: Some("Attempt ID does not match active lease. Ignored.".to_string()),
                })
            }
        }
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<Job, StoreError> {
        self.store.cancel_job(job_id)
    }

    pub fn get_metrics(&self) -> Result<MetricsResponse, StoreError> {
        self.store.get_metrics()
    }
}
