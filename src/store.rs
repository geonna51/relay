use crate::models::{
    Job, JobAttempt, JobStatus, MetricsResponse, Priority,
    Worker, WorkerCapacity, WorkerStatus,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Job not found: {0}")]
    JobNotFound(String),
    #[error("Worker not found: {0}")]
    WorkerNotFound(String),
    #[error("Invalid state transition: {0}")]
    InvalidTransition(String),
}

#[derive(Debug, Clone)]
pub enum CompleteJobOutcome {
    Accepted(Job),
    Retrying(Job),
    StaleAttemptIgnored,
}

#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

fn row_to_job(row: &rusqlite::Row) -> Result<Job, rusqlite::Error> {
    let args_str: String = row.get(3)?;
    let env_str: String = row.get(5)?;
    let priority_int: i32 = row.get(6)?;
    let status_str: String = row.get(7)?;
    let max_retries: i64 = row.get(8)?;
    let retry_count: i64 = row.get(9)?;
    let timeout_seconds: Option<i64> = row.get(10)?;
    let res_str: String = row.get(11)?;
    let dep_str: String = row.get(12)?;
    let created_str: String = row.get(15)?;
    let started_str: Option<String> = row.get(16)?;
    let completed_str: Option<String> = row.get(17)?;
    let lease_str: Option<String> = row.get(18)?;
    let retry_str: Option<String> = row.get(19)?;
    let runtime_ms: Option<i64> = row.get(23)?;
    let scheduling_latency_ms: Option<f64> = row.get(24)?;

    let args = serde_json::from_str(&args_str).unwrap_or_default();
    let env = serde_json::from_str(&env_str).unwrap_or_default();
    let priority = Priority::from_i32(priority_int);
    let status = status_str.parse().unwrap_or(JobStatus::Queued);
    let resources = serde_json::from_str(&res_str).unwrap_or_default();
    let depends_on = serde_json::from_str(&dep_str).unwrap_or_default();
    let created_at = DateTime::parse_from_rfc3339(&created_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let started_at = started_str
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));
    let completed_at = completed_str
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));
    let lease_expires_at = lease_str
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));
    let next_retry_at = retry_str
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    Ok(Job {
        id: row.get(0)?,
        name: row.get(1)?,
        command: row.get(2)?,
        args,
        cwd: row.get(4)?,
        env,
        priority,
        status,
        max_retries: max_retries as u32,
        retry_count: retry_count as u32,
        timeout_seconds: timeout_seconds.map(|v| v as u64),
        resources,
        depends_on,
        worker_id: row.get(13)?,
        current_attempt_id: row.get(14)?,
        created_at,
        started_at,
        completed_at,
        lease_expires_at,
        next_retry_at,
        exit_code: row.get(20)?,
        stdout: row.get(21)?,
        stderr: row.get(22)?,
        runtime_ms: runtime_ms.map(|v| v as u64),
        scheduling_latency_ms,
    })
}

fn row_to_worker(row: &rusqlite::Row) -> Result<Worker, rusqlite::Error> {
    let cpus: i64 = row.get(1)?;
    let memory_mb: i64 = row.get(2)?;
    let labels_str: String = row.get(3)?;
    let status_str: String = row.get(4)?;
    let active_jobs: i64 = row.get(5)?;
    let heartbeat_str: String = row.get(6)?;
    let reg_str: String = row.get(7)?;

    let labels = serde_json::from_str(&labels_str).unwrap_or_default();
    let status = match status_str.as_str() {
        "ACTIVE" => WorkerStatus::Active,
        "UNAVAILABLE" => WorkerStatus::Unavailable,
        _ => WorkerStatus::Dead,
    };
    let last_heartbeat = DateTime::parse_from_rfc3339(&heartbeat_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let registered_at = DateTime::parse_from_rfc3339(&reg_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

    Ok(Worker {
        id: row.get(0)?,
        cpus: cpus as u32,
        memory_mb: memory_mb as u64,
        labels,
        status,
        active_jobs: active_jobs as u32,
        last_heartbeat,
        registered_at,
    })
}

impl Store {
    pub fn new(path: &str) -> Result<Self, StoreError> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )?;

        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                name TEXT,
                command TEXT NOT NULL,
                args TEXT NOT NULL,
                cwd TEXT,
                env TEXT NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                max_retries INTEGER NOT NULL,
                retry_count INTEGER NOT NULL,
                timeout_seconds INTEGER,
                resources TEXT NOT NULL,
                depends_on TEXT NOT NULL,
                worker_id TEXT,
                current_attempt_id TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                lease_expires_at TEXT,
                next_retry_at TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                runtime_ms INTEGER,
                scheduling_latency_ms REAL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
            CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);

            CREATE TABLE IF NOT EXISTS job_attempts (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                worker_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                exit_code INTEGER,
                runtime_ms INTEGER,
                error TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_attempts_job ON job_attempts(job_id);

            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                cpus INTEGER NOT NULL,
                memory_mb INTEGER NOT NULL,
                labels TEXT NOT NULL,
                status TEXT NOT NULL,
                active_jobs INTEGER NOT NULL DEFAULT 0,
                last_heartbeat TEXT NOT NULL,
                registered_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);

            CREATE TABLE IF NOT EXISTS leases (
                job_id TEXT PRIMARY KEY,
                attempt_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at);",
        )?;
        Ok(())
    }

    pub fn save_job(&self, job: &Job) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        let args_json = serde_json::to_string(&job.args)?;
        let env_json = serde_json::to_string(&job.env)?;
        let resources_json = serde_json::to_string(&job.resources)?;
        let depends_json = serde_json::to_string(&job.depends_on)?;

        conn.execute(
            "INSERT INTO jobs (
                id, name, command, args, cwd, env, priority, status,
                max_retries, retry_count, timeout_seconds, resources, depends_on,
                worker_id, current_attempt_id, created_at, started_at, completed_at,
                lease_expires_at, next_retry_at, exit_code, stdout, stderr, runtime_ms,
                scheduling_latency_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                command=excluded.command,
                args=excluded.args,
                cwd=excluded.cwd,
                env=excluded.env,
                priority=excluded.priority,
                status=excluded.status,
                max_retries=excluded.max_retries,
                retry_count=excluded.retry_count,
                timeout_seconds=excluded.timeout_seconds,
                resources=excluded.resources,
                depends_on=excluded.depends_on,
                worker_id=excluded.worker_id,
                current_attempt_id=excluded.current_attempt_id,
                started_at=excluded.started_at,
                completed_at=excluded.completed_at,
                lease_expires_at=excluded.lease_expires_at,
                next_retry_at=excluded.next_retry_at,
                exit_code=excluded.exit_code,
                stdout=excluded.stdout,
                stderr=excluded.stderr,
                runtime_ms=excluded.runtime_ms,
                scheduling_latency_ms=excluded.scheduling_latency_ms",
            params![
                job.id,
                job.name,
                job.command,
                args_json,
                job.cwd,
                env_json,
                job.priority.as_i32(),
                job.status.as_str(),
                job.max_retries as i64,
                job.retry_count as i64,
                job.timeout_seconds.map(|v| v as i64),
                resources_json,
                depends_json,
                job.worker_id,
                job.current_attempt_id,
                job.created_at.to_rfc3339(),
                job.started_at.map(|t| t.to_rfc3339()),
                job.completed_at.map(|t| t.to_rfc3339()),
                job.lease_expires_at.map(|t| t.to_rfc3339()),
                job.next_retry_at.map(|t| t.to_rfc3339()),
                job.exit_code,
                job.stdout,
                job.stderr,
                job.runtime_ms.map(|v| v as i64),
                job.scheduling_latency_ms,
            ],
        )?;
        Ok(())
    }

    pub fn get_job(&self, id: &str) -> Result<Option<Job>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, command, args, cwd, env, priority, status,
                    max_retries, retry_count, timeout_seconds, resources, depends_on,
                    worker_id, current_attempt_id, created_at, started_at, completed_at,
                    lease_expires_at, next_retry_at, exit_code, stdout, stderr, runtime_ms,
                    scheduling_latency_ms
             FROM jobs WHERE id = ?1",
        )?;

        let job = stmt.query_row(params![id], row_to_job).optional()?;
        Ok(job)
    }

    pub fn list_jobs(
        &self,
        status: Option<JobStatus>,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut sql = "SELECT id, name, command, args, cwd, env, priority, status,
                              max_retries, retry_count, timeout_seconds, resources, depends_on,
                              worker_id, current_attempt_id, created_at, started_at, completed_at,
                              lease_expires_at, next_retry_at, exit_code, stdout, stderr, runtime_ms,
                              scheduling_latency_ms
                       FROM jobs".to_string();

        if let Some(st) = status {
            sql.push_str(&format!(" WHERE status = '{}'", st.as_str()));
        }
        sql.push_str(&format!(" ORDER BY created_at DESC LIMIT {}", limit));

        let mut stmt = conn.prepare(&sql)?;
        let job_iter = stmt.query_map([], row_to_job)?;

        let mut jobs = Vec::new();
        for job in job_iter {
            jobs.push(job?);
        }
        Ok(jobs)
    }

    pub fn claim_jobs(
        &self,
        worker_id: &str,
        capacity: &WorkerCapacity,
        max_jobs: usize,
        lease_duration: Duration,
    ) -> Result<Vec<(Job, JobAttempt)>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let now = Utc::now();
        let now_str = now.to_rfc3339();

        let mut stmt = tx.prepare(
            "SELECT id, name, command, args, cwd, env, priority, status,
                    max_retries, retry_count, timeout_seconds, resources, depends_on,
                    worker_id, current_attempt_id, created_at, started_at, completed_at,
                    lease_expires_at, next_retry_at, exit_code, stdout, stderr, runtime_ms,
                    scheduling_latency_ms
             FROM jobs
             WHERE status = 'QUEUED'
                OR (status = 'RETRYING' AND (next_retry_at IS NULL OR next_retry_at <= ?1))",
        )?;

        let candidates = stmt
            .query_map(params![now_str], row_to_job)?
            .filter_map(|r| r.ok())
            .collect::<Vec<Job>>();

        drop(stmt);

        let mut scored_candidates: Vec<(f64, Job)> = candidates
            .into_iter()
            .filter(|job| capacity.satisfies(&job.resources))
            .map(|job| {
                let wait_secs = (now - job.created_at).num_seconds().max(0) as f64;
                let score = job.priority.effective_score(wait_secs);
                (score, job)
            })
            .collect();

        scored_candidates.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.created_at.cmp(&b.1.created_at))
        });

        let mut claimed = Vec::new();
        let expires_at = now + ChronoDuration::from_std(lease_duration).unwrap();
        let mut remaining_capacity = capacity.clone();

        for (_, mut job) in scored_candidates {
            if claimed.len() >= max_jobs {
                break;
            }

            if !remaining_capacity.satisfies(&job.resources) {
                continue;
            }

            remaining_capacity.cpus = remaining_capacity.cpus.saturating_sub(job.resources.cpus);
            remaining_capacity.memory_mb = remaining_capacity.memory_mb.saturating_sub(job.resources.memory_mb);

            let prev_attempts: i64 = tx.query_row(
                "SELECT COUNT(*) FROM job_attempts WHERE job_id = ?1",
                params![job.id],
                |row| row.get(0),
            )?;

            let attempt_number = (prev_attempts + 1) as u32;
            let attempt_id = format!("att-{}", uuid::Uuid::new_v4().simple());

            let attempt = JobAttempt {
                id: attempt_id.clone(),
                job_id: job.id.clone(),
                attempt_number,
                worker_id: worker_id.to_string(),
                status: "RUNNING".to_string(),
                started_at: now,
                completed_at: None,
                exit_code: None,
                runtime_ms: None,
                error: None,
            };

            tx.execute(
                "INSERT INTO job_attempts (id, job_id, attempt_number, worker_id, status, started_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    attempt.id,
                    attempt.job_id,
                    attempt.attempt_number as i64,
                    attempt.worker_id,
                    attempt.status,
                    attempt.started_at.to_rfc3339(),
                ],
            )?;

            tx.execute(
                "INSERT INTO leases (job_id, attempt_id, worker_id, expires_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(job_id) DO UPDATE SET
                     attempt_id=excluded.attempt_id,
                     worker_id=excluded.worker_id,
                     expires_at=excluded.expires_at",
                params![job.id, attempt.id, worker_id, expires_at.to_rfc3339()],
            )?;

            let sched_latency = (now - job.created_at).num_microseconds().unwrap_or(0) as f64 / 1000.0;
            job.status = JobStatus::Assigned;
            job.worker_id = Some(worker_id.to_string());
            job.current_attempt_id = Some(attempt_id.clone());
            job.started_at = Some(now);
            job.lease_expires_at = Some(expires_at);
            job.scheduling_latency_ms = Some(sched_latency);

            tx.execute(
                "UPDATE jobs SET
                     status = 'ASSIGNED',
                     worker_id = ?1,
                     current_attempt_id = ?2,
                     started_at = ?3,
                     lease_expires_at = ?4,
                     scheduling_latency_ms = ?5
                 WHERE id = ?6",
                params![
                    worker_id,
                    attempt_id,
                    now.to_rfc3339(),
                    expires_at.to_rfc3339(),
                    job.scheduling_latency_ms,
                    job.id,
                ],
            )?;

            claimed.push((job, attempt));
        }

        if !claimed.is_empty() {
            tx.execute(
                "UPDATE workers SET active_jobs = active_jobs + ?1 WHERE id = ?2",
                params![claimed.len() as i64, worker_id],
            )?;
        }

        tx.commit()?;
        Ok(claimed)
    }

    pub fn renew_lease(
        &self,
        job_id: &str,
        attempt_id: &str,
        duration: Duration,
    ) -> Result<DateTime<Utc>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now();
        let new_expires_at = now + ChronoDuration::from_std(duration).unwrap();

        let updated = conn.execute(
            "UPDATE leases SET expires_at = ?1 WHERE job_id = ?2 AND attempt_id = ?3",
            params![new_expires_at.to_rfc3339(), job_id, attempt_id],
        )?;

        if updated == 0 {
            return Err(StoreError::InvalidTransition(format!(
                "Cannot renew lease: lease for job {job_id} with attempt {attempt_id} not found or expired"
            )));
        }

        conn.execute(
            "UPDATE jobs SET status = 'RUNNING', lease_expires_at = ?1 WHERE id = ?2 AND current_attempt_id = ?3",
            params![new_expires_at.to_rfc3339(), job_id, attempt_id],
        )?;

        Ok(new_expires_at)
    }

    pub fn complete_job(
        &self,
        job_id: &str,
        attempt_id: &str,
        exit_code: i32,
        stdout: &str,
        stderr: &str,
        runtime_ms: u64,
    ) -> Result<CompleteJobOutcome, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut job = match self.get_job_tx(&tx, job_id)? {
            Some(j) => j,
            None => return Err(StoreError::JobNotFound(job_id.to_string())),
        };

        if job.status.is_terminal()
            || job.status == JobStatus::Cancelled
            || job.current_attempt_id.as_deref() != Some(attempt_id)
        {
            return Ok(CompleteJobOutcome::StaleAttemptIgnored);
        }

        let now = Utc::now();
        let attempt_status = if exit_code == 0 { "SUCCEEDED" } else { "FAILED" };

        tx.execute(
            "UPDATE job_attempts SET
                 status = ?1,
                 completed_at = ?2,
                 exit_code = ?3,
                 runtime_ms = ?4
             WHERE id = ?5",
            params![
                attempt_status,
                now.to_rfc3339(),
                exit_code,
                runtime_ms as i64,
                attempt_id,
            ],
        )?;

        tx.execute("DELETE FROM leases WHERE job_id = ?1", params![job_id])?;

        if let Some(ref wid) = job.worker_id {
            tx.execute(
                "UPDATE workers SET active_jobs = MAX(0, active_jobs - 1) WHERE id = ?1",
                params![wid],
            )?;
        }

        let outcome = if exit_code == 0 {
            job.status = JobStatus::Succeeded;
            job.completed_at = Some(now);
            job.exit_code = Some(0);
            job.stdout = Some(stdout.to_string());
            job.stderr = Some(stderr.to_string());
            job.runtime_ms = Some(runtime_ms);

            tx.execute(
                "UPDATE jobs SET
                     status = 'SUCCEEDED',
                     completed_at = ?1,
                     exit_code = 0,
                     stdout = ?2,
                     stderr = ?3,
                     runtime_ms = ?4,
                     lease_expires_at = NULL
                 WHERE id = ?5",
                params![
                    now.to_rfc3339(),
                    job.stdout,
                    job.stderr,
                    job.runtime_ms.map(|v| v as i64),
                    job.id,
                ],
            )?;

            self.unblock_dependents_tx(&tx, job_id)?;
            CompleteJobOutcome::Accepted(job)
        } else if job.retry_count < job.max_retries {
            job.retry_count += 1;
            job.status = JobStatus::Retrying;
            let backoff_secs = 2u64.pow(job.retry_count);
            let next_retry = now + ChronoDuration::seconds(backoff_secs as i64);
            job.next_retry_at = Some(next_retry);
            job.worker_id = None;
            job.current_attempt_id = None;
            job.lease_expires_at = None;

            tx.execute(
                "UPDATE jobs SET
                     status = 'RETRYING',
                     retry_count = ?1,
                     next_retry_at = ?2,
                     worker_id = NULL,
                     current_attempt_id = NULL,
                     lease_expires_at = NULL
                 WHERE id = ?3",
                params![job.retry_count as i64, next_retry.to_rfc3339(), job.id],
            )?;

            CompleteJobOutcome::Retrying(job)
        } else {
            job.status = JobStatus::Failed;
            job.completed_at = Some(now);
            job.exit_code = Some(exit_code);
            job.stdout = Some(stdout.to_string());
            job.stderr = Some(stderr.to_string());
            job.runtime_ms = Some(runtime_ms);

            tx.execute(
                "UPDATE jobs SET
                     status = 'FAILED',
                     completed_at = ?1,
                     exit_code = ?2,
                     stdout = ?3,
                     stderr = ?4,
                     runtime_ms = ?5,
                     lease_expires_at = NULL
                 WHERE id = ?6",
                params![
                    now.to_rfc3339(),
                    job.exit_code,
                    job.stdout,
                    job.stderr,
                    job.runtime_ms.map(|v| v as i64),
                    job.id,
                ],
            )?;

            self.fail_dependents_tx(&tx, job_id)?;
            CompleteJobOutcome::Accepted(job)
        };

        tx.commit()?;
        Ok(outcome)
    }

    pub fn reap_expired_leases(&self) -> Result<Vec<(Job, String)>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let now = Utc::now();
        let mut stmt = tx.prepare(
            "SELECT job_id, attempt_id, worker_id, expires_at FROM leases WHERE expires_at < ?1",
        )?;

        let expired = stmt
            .query_map(params![now.to_rfc3339()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect::<Vec<(String, String, String)>>();

        drop(stmt);

        let mut reaped = Vec::new();

        for (job_id, attempt_id, worker_id) in expired {
            tx.execute(
                "UPDATE job_attempts SET status = 'LOST', completed_at = ?1 WHERE id = ?2",
                params![now.to_rfc3339(), attempt_id],
            )?;

            tx.execute("DELETE FROM leases WHERE job_id = ?1", params![job_id])?;

            tx.execute(
                "UPDATE workers SET active_jobs = MAX(0, active_jobs - 1) WHERE id = ?1",
                params![worker_id],
            )?;

            if let Some(mut job) = self.get_job_tx(&tx, &job_id)? {
                if job.retry_count < job.max_retries {
                    job.retry_count += 1;
                    job.status = JobStatus::Retrying;
                    let backoff_secs = 2u64.pow(job.retry_count);
                    let next_retry = now + ChronoDuration::seconds(backoff_secs as i64);
                    job.next_retry_at = Some(next_retry);
                    job.worker_id = None;
                    job.current_attempt_id = None;
                    job.lease_expires_at = None;

                    tx.execute(
                        "UPDATE jobs SET
                             status = 'RETRYING',
                             retry_count = ?1,
                             next_retry_at = ?2,
                             worker_id = NULL,
                             current_attempt_id = NULL,
                             lease_expires_at = NULL
                         WHERE id = ?3",
                        params![job.retry_count as i64, next_retry.to_rfc3339(), job.id],
                    )?;
                } else {
                    job.status = JobStatus::Failed;
                    job.completed_at = Some(now);

                    tx.execute(
                        "UPDATE jobs SET
                             status = 'FAILED',
                             completed_at = ?1,
                             worker_id = NULL,
                             current_attempt_id = NULL,
                             lease_expires_at = NULL
                         WHERE id = ?2",
                        params![now.to_rfc3339(), job.id],
                    )?;
                }
                reaped.push((job, worker_id));
            }
        }

        tx.commit()?;
        Ok(reaped)
    }

    pub fn reap_dead_workers(&self, timeout: Duration) -> Result<Vec<String>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let cutoff = Utc::now() - ChronoDuration::from_std(timeout).unwrap();

        let mut stmt = conn.prepare(
            "SELECT id FROM workers WHERE status = 'ACTIVE' AND last_heartbeat < ?1",
        )?;

        let dead_ids = stmt
            .query_map(params![cutoff.to_rfc3339()], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect::<Vec<String>>();

        for id in &dead_ids {
            conn.execute(
                "UPDATE workers SET status = 'UNAVAILABLE' WHERE id = ?1",
                params![id],
            )?;
        }

        Ok(dead_ids)
    }

    pub fn register_worker(&self, worker: &Worker) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        let labels_json = serde_json::to_string(&worker.labels)?;

        conn.execute(
            "INSERT INTO workers (id, cpus, memory_mb, labels, status, active_jobs, last_heartbeat, registered_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
                 cpus=excluded.cpus,
                 memory_mb=excluded.memory_mb,
                 labels=excluded.labels,
                 status='ACTIVE',
                 last_heartbeat=excluded.last_heartbeat",
            params![
                worker.id,
                worker.cpus as i64,
                worker.memory_mb as i64,
                labels_json,
                worker.status.as_str(),
                worker.active_jobs as i64,
                worker.last_heartbeat.to_rfc3339(),
                worker.registered_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn worker_heartbeat(&self, worker_id: &str) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now();

        let updated = conn.execute(
            "UPDATE workers SET last_heartbeat = ?1, status = 'ACTIVE' WHERE id = ?2",
            params![now.to_rfc3339(), worker_id],
        )?;

        Ok(updated > 0)
    }

    pub fn get_worker(&self, worker_id: &str) -> Result<Option<Worker>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, cpus, memory_mb, labels, status, active_jobs, last_heartbeat, registered_at
             FROM workers WHERE id = ?1",
        )?;

        let worker = stmt.query_row(params![worker_id], row_to_worker).optional()?;
        Ok(worker)
    }

    pub fn list_workers(&self) -> Result<Vec<Worker>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, cpus, memory_mb, labels, status, active_jobs, last_heartbeat, registered_at
             FROM workers ORDER BY registered_at ASC",
        )?;

        let worker_iter = stmt.query_map([], row_to_worker)?;

        let mut workers = Vec::new();
        for w in worker_iter {
            workers.push(w?);
        }
        Ok(workers)
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<Job, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut job = match self.get_job_tx(&tx, job_id)? {
            Some(j) => j,
            None => return Err(StoreError::JobNotFound(job_id.to_string())),
        };

        if job.status.is_terminal() {
            return Ok(job);
        }

        let now = Utc::now();
        let prev_worker_id = job.worker_id.clone();
        let prev_attempt_id = job.current_attempt_id.clone();

        job.status = JobStatus::Cancelled;
        job.completed_at = Some(now);
        job.worker_id = None;
        job.current_attempt_id = None;
        job.lease_expires_at = None;

        tx.execute(
            "UPDATE jobs SET
                 status = 'CANCELLED',
                 completed_at = ?1,
                 worker_id = NULL,
                 current_attempt_id = NULL,
                 lease_expires_at = NULL
             WHERE id = ?2",
            params![now.to_rfc3339(), job_id],
        )?;

        tx.execute("DELETE FROM leases WHERE job_id = ?1", params![job_id])?;

        if let Some(ref wid) = prev_worker_id {
            tx.execute(
                "UPDATE workers SET active_jobs = MAX(0, active_jobs - 1) WHERE id = ?1",
                params![wid],
            )?;
        }

        if let Some(ref att) = prev_attempt_id {
            tx.execute(
                "UPDATE job_attempts SET status = 'CANCELLED', completed_at = ?1 WHERE id = ?2",
                params![now.to_rfc3339(), att],
            )?;
        }

        self.fail_dependents_tx(&tx, job_id)?;
        tx.commit()?;
        Ok(job)
    }

    pub fn get_metrics(&self) -> Result<MetricsResponse, StoreError> {
        let conn = self.conn.lock().unwrap();

        let count_status = |st: &str| -> usize {
            conn.query_row(
                "SELECT COUNT(*) FROM jobs WHERE status = ?1",
                params![st],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize
        };

        let total_jobs: usize = conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap_or(0) as usize;

        let workers_active: usize = conn
            .query_row(
                "SELECT COUNT(*) FROM workers WHERE status = 'ACTIVE'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize;

        let workers_unavailable: usize = conn
            .query_row(
                "SELECT COUNT(*) FROM workers WHERE status = 'UNAVAILABLE'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize;

        let workers_total: usize = conn
            .query_row("SELECT COUNT(*) FROM workers", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap_or(0) as usize;

        let active_leases: usize = conn
            .query_row("SELECT COUNT(*) FROM leases", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap_or(0) as usize;

        Ok(MetricsResponse {
            jobs_queued: count_status("QUEUED"),
            jobs_assigned: count_status("ASSIGNED"),
            jobs_running: count_status("RUNNING"),
            jobs_retrying: count_status("RETRYING"),
            jobs_succeeded: count_status("SUCCEEDED"),
            jobs_failed: count_status("FAILED"),
            jobs_cancelled: count_status("CANCELLED"),
            jobs_blocked: count_status("BLOCKED"),
            total_jobs,
            workers_active,
            workers_unavailable,
            workers_total,
            active_leases,
        })
    }

    fn get_job_tx(&self, tx: &rusqlite::Transaction, id: &str) -> Result<Option<Job>, StoreError> {
        let mut stmt = tx.prepare(
            "SELECT id, name, command, args, cwd, env, priority, status,
                    max_retries, retry_count, timeout_seconds, resources, depends_on,
                    worker_id, current_attempt_id, created_at, started_at, completed_at,
                    lease_expires_at, next_retry_at, exit_code, stdout, stderr, runtime_ms,
                    scheduling_latency_ms
             FROM jobs WHERE id = ?1",
        )?;

        let job = stmt.query_row(params![id], row_to_job).optional()?;
        Ok(job)
    }

    fn unblock_dependents_tx(
        &self,
        tx: &rusqlite::Transaction,
        completed_job_id: &str,
    ) -> Result<(), StoreError> {
        let mut stmt = tx.prepare("SELECT id, depends_on FROM jobs WHERE status = 'BLOCKED'")?;
        let blocked_jobs = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let deps_str: String = row.get(1)?;
                Ok((id, deps_str))
            })?
            .filter_map(|r| r.ok())
            .collect::<Vec<(String, String)>>();

        for (job_id, deps_str) in blocked_jobs {
            let deps: Vec<String> = serde_json::from_str(&deps_str).unwrap_or_default();
            if deps.iter().any(|d| d == completed_job_id) {
                let mut all_succeeded = true;
                for dep in &deps {
                    let status: Option<String> = tx
                        .query_row(
                            "SELECT status FROM jobs WHERE id = ?1",
                            params![dep],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if status.as_deref() != Some("SUCCEEDED") {
                        all_succeeded = false;
                        break;
                    }
                }
                if all_succeeded {
                    tx.execute(
                        "UPDATE jobs SET status = 'QUEUED' WHERE id = ?1",
                        params![job_id],
                    )?;
                }
            }
        }
        Ok(())
    }

    fn fail_dependents_tx(
        &self,
        tx: &rusqlite::Transaction,
        failed_job_id: &str,
    ) -> Result<(), StoreError> {
        let mut stmt = tx.prepare("SELECT id, depends_on FROM jobs WHERE status = 'BLOCKED'")?;
        let blocked_jobs = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let deps_str: String = row.get(1)?;
                Ok((id, deps_str))
            })?
            .filter_map(|r| r.ok())
            .collect::<Vec<(String, String)>>();

        for (job_id, deps_str) in blocked_jobs {
            let deps: Vec<String> = serde_json::from_str(&deps_str).unwrap_or_default();
            if deps.iter().any(|d| d == failed_job_id) {
                tx.execute(
                    "UPDATE jobs SET status = 'CANCELLED' WHERE id = ?1",
                    params![job_id],
                )?;
            }
        }
        Ok(())
    }
}
