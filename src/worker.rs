use crate::models::{
    AssignedJob, CompleteJobRequest, CompleteJobResponse, RenewLeaseRequest, WorkerPollRequest,
    WorkerPollResponse, WorkerRegisterRequest,
};
use chrono::Utc;
use reqwest::Client;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{error, info, warn};

pub struct WorkerConfig {
    pub worker_id: String,
    pub scheduler_url: String,
    pub cpus: u32,
    pub memory_mb: u64,
    pub labels: HashMap<String, String>,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub renew_interval: Duration,
    pub output_dir: String,
    pub oneshot: bool,
    pub max_concurrent_jobs: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        let cpus = sys.cpus().len().max(1) as u32;
        let memory_mb = (sys.total_memory() / (1024 * 1024)).max(512);
        let worker_id = format!("worker-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);

        Self {
            worker_id,
            scheduler_url: "http://127.0.0.1:8000".to_string(),
            cpus,
            memory_mb,
            labels: HashMap::new(),
            poll_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(3),
            renew_interval: Duration::from_secs(5),
            output_dir: "o".to_string(),
            oneshot: false,
            max_concurrent_jobs: 4,
        }
    }
}

struct ResourceGuard {
    cpus: u32,
    memory_mb: u64,
    allocated_cpus: Arc<AtomicU32>,
    allocated_memory_mb: Arc<AtomicU64>,
}

impl Drop for ResourceGuard {
    fn drop(&mut self) {
        self.allocated_cpus.fetch_sub(self.cpus, Ordering::SeqCst);
        self.allocated_memory_mb.fetch_sub(self.memory_mb, Ordering::SeqCst);
    }
}

pub struct WorkerDaemon {
    config: WorkerConfig,
    client: Client,
    running: Arc<AtomicBool>,
    allocated_cpus: Arc<AtomicU32>,
    allocated_memory_mb: Arc<AtomicU64>,
    active_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl WorkerDaemon {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            running: Arc::new(AtomicBool::new(true)),
            allocated_cpus: Arc::new(AtomicU32::new(0)),
            allocated_memory_mb: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        fs::create_dir_all(&self.config.output_dir)?;

        info!(
            "Starting worker {} (CPUs={}, RAM={}MB, Server={})",
            self.config.worker_id, self.config.cpus, self.config.memory_mb, self.config.scheduler_url
        );

        self.register().await?;

        let heartbeat_running = Arc::clone(&self.running);
        let heartbeat_worker_id = self.config.worker_id.clone();
        let heartbeat_url = self.config.scheduler_url.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let client = self.client.clone();

        let heartbeat_handle = tokio::spawn(async move {
            while heartbeat_running.load(Ordering::Relaxed) {
                sleep(heartbeat_interval).await;
                let url = format!("{}/workers/{}/heartbeat", heartbeat_url, heartbeat_worker_id);
                if let Err(e) = client.post(&url).send().await {
                    warn!("Heartbeat failed to {}: {}", url, e);
                }
            }
        });

        {
            let mut tasks = self.active_tasks.lock().unwrap();
            tasks.push(heartbeat_handle);
        }

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_jobs));

        while self.running.load(Ordering::Relaxed) {
            let available_permits = semaphore.available_permits();
            let cur_cpus = self.allocated_cpus.load(Ordering::Relaxed);
            let cur_mem = self.allocated_memory_mb.load(Ordering::Relaxed);
            let avail_cpus = self.config.cpus.saturating_sub(cur_cpus);
            let avail_mem = self.config.memory_mb.saturating_sub(cur_mem);

            if available_permits == 0 || avail_cpus == 0 {
                sleep(self.config.poll_interval).await;
                continue;
            }

            let poll_req = WorkerPollRequest {
                available_cpus: avail_cpus,
                available_memory_mb: avail_mem,
                labels: self.config.labels.clone(),
                max_jobs: available_permits.min(4),
            };

            let poll_url = format!("{}/workers/{}/poll", self.config.scheduler_url, self.config.worker_id);
            let response = match self.client.post(&poll_url).json(&poll_req).send().await {
                Ok(resp) => resp,
                Err(_) => {
                    sleep(self.config.poll_interval).await;
                    continue;
                }
            };

            if !response.status().is_success() {
                sleep(self.config.poll_interval).await;
                continue;
            }

            let poll_resp: WorkerPollResponse = match response.json().await {
                Ok(r) => r,
                Err(_) => {
                    sleep(self.config.poll_interval).await;
                    continue;
                }
            };

            if poll_resp.jobs.is_empty() {
                sleep(self.config.poll_interval).await;
                continue;
            }

            for assigned in poll_resp.jobs {
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let req_cpus = assigned.job.resources.cpus;
                let req_mem = assigned.job.resources.memory_mb;
                self.allocated_cpus.fetch_add(req_cpus, Ordering::SeqCst);
                self.allocated_memory_mb.fetch_add(req_mem, Ordering::SeqCst);

                let allocated_cpus = Arc::clone(&self.allocated_cpus);
                let allocated_memory_mb = Arc::clone(&self.allocated_memory_mb);
                let client = self.client.clone();
                let server_url = self.config.scheduler_url.clone();
                let worker_id = self.config.worker_id.clone();
                let output_dir = self.config.output_dir.clone();
                let renew_interval = self.config.renew_interval;

                if self.config.oneshot {
                    let _guard = ResourceGuard {
                        cpus: req_cpus,
                        memory_mb: req_mem,
                        allocated_cpus: Arc::clone(&allocated_cpus),
                        allocated_memory_mb: Arc::clone(&allocated_memory_mb),
                    };
                    Self::execute_job(
                        client,
                        server_url,
                        worker_id,
                        assigned,
                        output_dir,
                        renew_interval,
                    )
                    .await;
                    drop(permit);
                    info!("Oneshot mode: executed 1 job, shutting down worker.");
                    return Ok(());
                }

                let job_handle = tokio::spawn(async move {
                    let _guard = ResourceGuard {
                        cpus: req_cpus,
                        memory_mb: req_mem,
                        allocated_cpus,
                        allocated_memory_mb,
                    };
                    Self::execute_job(
                        client,
                        server_url,
                        worker_id,
                        assigned,
                        output_dir,
                        renew_interval,
                    )
                    .await;
                    drop(permit);
                });

                {
                    let mut tasks = self.active_tasks.lock().unwrap();
                    tasks.retain(|h| !h.is_finished());
                    tasks.push(job_handle);
                }
            }
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    pub fn kill(&self) {
        self.running.store(false, Ordering::Relaxed);
        let mut tasks = self.active_tasks.lock().unwrap();
        for handle in tasks.drain(..) {
            handle.abort();
        }
    }

    async fn register(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let reg_req = WorkerRegisterRequest {
            worker_id: self.config.worker_id.clone(),
            cpus: self.config.cpus,
            memory_mb: self.config.memory_mb,
            labels: self.config.labels.clone(),
        };

        let url = format!("{}/workers/register", self.config.scheduler_url);
        let resp = self.client.post(&url).json(&reg_req).send().await?;
        if !resp.status().is_success() {
            return Err(format!("Worker registration failed with status: {}", resp.status()).into());
        }
        info!("Worker {} registered successfully", self.config.worker_id);
        Ok(())
    }

    async fn execute_job(
        client: Client,
        server_url: String,
        worker_id: String,
        assigned: AssignedJob,
        output_dir: String,
        renew_interval: Duration,
    ) {
        let job = assigned.job;
        let attempt_id = assigned.attempt_id;
        info!(
            "Worker {} starting job {} (attempt={}, cmd='{}')",
            worker_id, job.id, attempt_id, job.command
        );

        let lease_active = Arc::new(AtomicBool::new(true));
        let renew_handle = {
            let client = client.clone();
            let server_url = server_url.clone();
            let job_id = job.id.clone();
            let attempt_id = attempt_id.clone();
            let lease_active = Arc::clone(&lease_active);

            tokio::spawn(async move {
                while lease_active.load(Ordering::Relaxed) {
                    sleep(renew_interval).await;
                    if !lease_active.load(Ordering::Relaxed) {
                        break;
                    }
                    let renew_req = RenewLeaseRequest {
                        attempt_id: attempt_id.clone(),
                        duration_seconds: None,
                    };
                    let renew_url = format!("{}/jobs/{}/renew", server_url, job_id);
                    if let Err(e) = client.post(&renew_url).json(&renew_req).send().await {
                        warn!("Lease renewal failed for job {}: {}", job_id, e);
                    }
                }
            })
        };

        let log_path = Path::new(&output_dir).join(format!("out.{}", job.id));
        let log_file = match File::create(&log_path) {
            Ok(mut f) => {
                let _ = writeln!(f, "=== Job ID: {} (Attempt: {}) ===", job.id, attempt_id);
                let _ = writeln!(f, "=== Command: {} ===", job.command);
                let _ = writeln!(f, "=== Started At: {} ===\n", Utc::now().to_rfc3339());
                let _ = writeln!(f, "--- LIVE OUTPUT ---");
                let _ = f.flush();
                Some(f)
            }
            Err(e) => {
                warn!("Could not create log file at {:?}: {}", log_path, e);
                None
            }
        };

        let start_time = Instant::now();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(&job.command);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        if let Some(ref cwd) = job.cwd {
            cmd.current_dir(cwd);
        }

        for (k, v) in &job.env {
            cmd.env(k, v);
        }

        let timeout_duration = job.timeout_seconds.map(Duration::from_secs);

        let (exit_code, stdout_str, stderr_str) = match cmd.spawn() {
            Ok(mut child) => {
                use tokio::io::AsyncBufReadExt;
                let log_file_mutex = Arc::new(std::sync::Mutex::new(log_file));
                let stdout_buf = Arc::new(std::sync::Mutex::new(String::new()));
                let stderr_buf = Arc::new(std::sync::Mutex::new(String::new()));

                let stdout_task = if let Some(stdout) = child.stdout.take() {
                    let file = Arc::clone(&log_file_mutex);
                    let buf = Arc::clone(&stdout_buf);
                    Some(tokio::spawn(async move {
                        let reader = tokio::io::BufReader::new(stdout);
                        let mut lines = reader.lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            if let Ok(mut guard) = file.lock() {
                                if let Some(ref mut f) = *guard {
                                    let _ = writeln!(f, "{}", line);
                                    let _ = f.flush();
                                }
                            }
                            if let Ok(mut s) = buf.lock() {
                                if s.len() < 1024 * 512 {
                                    s.push_str(&line);
                                    s.push('\n');
                                }
                            }
                        }
                    }))
                } else {
                    None
                };

                let stderr_task = if let Some(stderr) = child.stderr.take() {
                    let file = Arc::clone(&log_file_mutex);
                    let buf = Arc::clone(&stderr_buf);
                    Some(tokio::spawn(async move {
                        let reader = tokio::io::BufReader::new(stderr);
                        let mut lines = reader.lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            if let Ok(mut guard) = file.lock() {
                                if let Some(ref mut f) = *guard {
                                    let _ = writeln!(f, "[STDERR] {}", line);
                                    let _ = f.flush();
                                }
                            }
                            if let Ok(mut s) = buf.lock() {
                                if s.len() < 1024 * 512 {
                                    s.push_str(&line);
                                    s.push('\n');
                                }
                            }
                        }
                    }))
                } else {
                    None
                };

                let wait_res = if let Some(timeout) = timeout_duration {
                    match tokio::time::timeout(timeout, child.wait()).await {
                        Ok(res) => res,
                        Err(_) => {
                            warn!("Job {} exceeded timeout of {:?}", job.id, timeout);
                            let _ = child.kill().await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Job execution timed out",
                            ))
                        }
                    }
                } else {
                    child.wait().await
                };

                if let Some(t) = stdout_task {
                    let _ = t.await;
                }
                if let Some(t) = stderr_task {
                    let _ = t.await;
                }

                let code = match wait_res {
                    Ok(status) => status.code().unwrap_or(-1),
                    Err(e) => {
                        if let Ok(mut guard) = log_file_mutex.lock() {
                            if let Some(ref mut f) = *guard {
                                let _ = writeln!(f, "\n[ERROR] {}", e);
                                let _ = f.flush();
                            }
                        }
                        -1
                    }
                };

                let out_str = stdout_buf.lock().map(|s| s.clone()).unwrap_or_default();
                let mut err_str = stderr_buf.lock().map(|s| s.clone()).unwrap_or_default();
                if code != 0 && err_str.is_empty() {
                    err_str = format!("Process exited with code {}", code);
                }

                (code, out_str, err_str)
            }
            Err(e) => {
                let err_msg = format!("Failed to spawn process: {}", e);
                (-1, String::new(), err_msg)
            }
        };

        let runtime_ms = start_time.elapsed().as_millis() as u64;

        lease_active.store(false, Ordering::Relaxed);
        renew_handle.abort();

        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(&log_path) {
            let _ = writeln!(f, "\n--- END OF OUTPUT ---");
            let _ = writeln!(f, "=== Exit Code: {} ===", exit_code);
            let _ = writeln!(f, "=== Runtime: {}ms ===", runtime_ms);
            let _ = f.flush();
        }

        let complete_req = CompleteJobRequest {
            attempt_id: attempt_id.clone(),
            exit_code,
            stdout: stdout_str,
            stderr: stderr_str,
            runtime_ms,
        };

        let complete_url = format!("{}/jobs/{}/complete", server_url, job.id);
        match client.post(&complete_url).json(&complete_req).send().await {
            Ok(resp) => {
                if let Ok(c_resp) = resp.json::<CompleteJobResponse>().await {
                    if c_resp.status == "stale_ignored" {
                        warn!(
                            "Scheduler rejected result for job {} (attempt={}): stale attempt",
                            job.id, attempt_id
                        );
                    } else {
                        info!(
                            "Job {} finished with exit code {} (status={})",
                            job.id, exit_code, c_resp.status
                        );
                    }
                }
            }
            Err(e) => {
                error!("Failed to report completion for job {}: {}", job.id, e);
            }
        }
    }
}
