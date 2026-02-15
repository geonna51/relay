use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Queued,
    Assigned,
    Running,
    Retrying,
    Succeeded,
    Failed,
    Cancelled,
    Blocked,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Assigned => "ASSIGNED",
            Self::Running => "RUNNING",
            Self::Retrying => "RETRYING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Blocked => "BLOCKED",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "QUEUED" => Ok(Self::Queued),
            "ASSIGNED" => Ok(Self::Assigned),
            "RUNNING" => Ok(Self::Running),
            "RETRYING" => Ok(Self::Retrying),
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            "BLOCKED" => Ok(Self::Blocked),
            other => Err(format!("Unknown job status: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Priority {
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }

    pub fn from_i32(val: i32) -> Self {
        match val {
            1 => Self::Low,
            3 => Self::High,
            4 => Self::Critical,
            _ => Self::Normal,
        }
    }

    pub fn effective_score(&self, queued_duration_secs: f64) -> f64 {
        // Priority aging prevents low priority jobs from starving during high load
        self.as_i32() as f64 + (queued_duration_secs / 60.0) * 0.5
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "low"),
            Self::Normal => write!(f, "normal"),
            Self::High => write!(f, "high"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

impl std::str::FromStr for Priority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "low" => Ok(Self::Low),
            "normal" => Ok(Self::Normal),
            "high" => Ok(Self::High),
            "critical" => Ok(Self::Critical),
            other => Err(format!("Unknown priority: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequirements {
    #[serde(default = "default_cpu")]
    pub cpus: u32,
    #[serde(default = "default_mem")]
    pub memory_mb: u64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_cpu() -> u32 {
    1
}

fn default_mem() -> u64 {
    512
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            cpus: 1,
            memory_mb: 512,
            labels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapacity {
    pub cpus: u32,
    pub memory_mb: u64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

impl WorkerCapacity {
    pub fn satisfies(&self, reqs: &ResourceRequirements) -> bool {
        if self.cpus < reqs.cpus || self.memory_mb < reqs.memory_mb {
            return false;
        }
        for (k, v) in &reqs.labels {
            if self.labels.get(k) != Some(v) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerStatus {
    Active,
    Unavailable,
    Dead,
}

impl WorkerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Unavailable => "UNAVAILABLE",
            Self::Dead => "DEAD",
        }
    }
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: Option<String>,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub priority: Priority,
    pub status: JobStatus,
    pub max_retries: u32,
    pub retry_count: u32,
    pub timeout_seconds: Option<u64>,
    pub resources: ResourceRequirements,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub worker_id: Option<String>,
    pub current_attempt_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub runtime_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobAttempt {
    pub id: String,
    pub job_id: String,
    pub attempt_number: u32,
    pub worker_id: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub runtime_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub id: String,
    pub cpus: u32,
    pub memory_mb: u64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    pub status: WorkerStatus,
    pub active_jobs: u32,
    pub last_heartbeat: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub job_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub expires_at: DateTime<Utc>,
}

// Request & Response DTOs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobRequest {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default = "default_priority")]
    pub priority: Priority,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub resources: ResourceRequirements,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub name: Option<String>,
}

fn default_priority() -> Priority {
    Priority::Normal
}

fn default_max_retries() -> u32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSubmitRequest {
    pub jobs: Vec<SubmitJobRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    pub job_id: String,
    pub status: JobStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSubmitResponse {
    pub job_ids: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterRequest {
    pub worker_id: String,
    pub cpus: u32,
    pub memory_mb: u64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPollRequest {
    pub available_cpus: u32,
    pub available_memory_mb: u64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default = "default_max_jobs")]
    pub max_jobs: usize,
}

fn default_max_jobs() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignedJob {
    pub job: Job,
    pub attempt_id: String,
    pub lease_duration_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPollResponse {
    pub jobs: Vec<AssignedJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenewLeaseRequest {
    pub attempt_id: String,
    pub duration_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenewLeaseResponse {
    pub status: String,
    pub new_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteJobRequest {
    pub attempt_id: String,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub runtime_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteJobResponse {
    pub status: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub jobs_queued: usize,
    pub jobs_assigned: usize,
    pub jobs_running: usize,
    pub jobs_retrying: usize,
    pub jobs_succeeded: usize,
    pub jobs_failed: usize,
    pub jobs_cancelled: usize,
    pub jobs_blocked: usize,
    pub total_jobs: usize,
    pub workers_active: usize,
    pub workers_unavailable: usize,
    pub workers_total: usize,
    pub active_leases: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_score_aging() {
        let low = Priority::Low;
        let high = Priority::High;

        // Immediately, high priority beats low priority
        assert!(high.effective_score(0.0) > low.effective_score(0.0));

        // After waiting 300 seconds (5 minutes), low priority gets 5 * 0.5 = 2.5 points boost
        // Low: 1.0 + 2.5 = 3.5 > High at 0s (3.0)
        assert!(low.effective_score(300.0) > high.effective_score(0.0));
    }

    #[test]
    fn test_worker_capacity_matching() {
        let mut worker_labels = HashMap::new();
        worker_labels.insert("arch".to_string(), "x86_64".to_string());
        worker_labels.insert("zone".to_string(), "us-east-1".to_string());

        let cap = WorkerCapacity {
            cpus: 8,
            memory_mb: 16384,
            labels: worker_labels,
        };

        let mut req_labels = HashMap::new();
        req_labels.insert("arch".to_string(), "x86_64".to_string());

        let req_fit = ResourceRequirements {
            cpus: 4,
            memory_mb: 8192,
            labels: req_labels.clone(),
        };
        assert!(cap.satisfies(&req_fit));

        let req_too_many_cpus = ResourceRequirements {
            cpus: 16,
            memory_mb: 8192,
            labels: req_labels.clone(),
        };
        assert!(!cap.satisfies(&req_too_many_cpus));

        let mut wrong_labels = req_labels;
        wrong_labels.insert("gpu".to_string(), "true".to_string());
        let req_missing_label = ResourceRequirements {
            cpus: 4,
            memory_mb: 8192,
            labels: wrong_labels,
        };
        assert!(!cap.satisfies(&req_missing_label));
    }

    #[test]
    fn test_status_parsing() {
        assert_eq!("QUEUED".parse::<JobStatus>().unwrap(), JobStatus::Queued);
        assert_eq!("RUNNING".parse::<JobStatus>().unwrap(), JobStatus::Running);
        assert_eq!("RETRYING".parse::<JobStatus>().unwrap(), JobStatus::Retrying);
        assert_eq!("SUCCEEDED".parse::<JobStatus>().unwrap(), JobStatus::Succeeded);
        assert_eq!("FAILED".parse::<JobStatus>().unwrap(), JobStatus::Failed);
        assert_eq!("CANCELLED".parse::<JobStatus>().unwrap(), JobStatus::Cancelled);
        assert_eq!("BLOCKED".parse::<JobStatus>().unwrap(), JobStatus::Blocked);
    }
}
