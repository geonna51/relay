use crate::models::{
    BatchSubmitRequest, BatchSubmitResponse, Job, JobStatus, MetricsResponse, SubmitJobRequest,
    SubmitJobResponse, Worker,
};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct RelayClient {
    base_url: String,
    client: Client,
}

impl RelayClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    pub async fn submit(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, String> {
        let url = format!("{}/jobs", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Submit failed: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn submit_batch(&self, req: BatchSubmitRequest) -> Result<BatchSubmitResponse, String> {
        let url = format!("{}/jobs/batch", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Batch submit failed: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn status(&self, job_id: &str) -> Result<Job, String> {
        let url = format!("{}/jobs/{}", self.base_url, job_id);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Failed to get status for {job_id}: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn logs(&self, job_id: &str) -> Result<Value, String> {
        let url = format!("{}/jobs/{}/logs", self.base_url, job_id);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Failed to get logs for {job_id}: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn cancel(&self, job_id: &str) -> Result<Value, String> {
        let url = format!("{}/jobs/{}/cancel", self.base_url, job_id);
        let resp = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Failed to cancel {job_id}: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn list(&self, status: Option<JobStatus>, limit: usize) -> Result<Vec<Job>, String> {
        let mut url = format!("{}/jobs?limit={}", self.base_url, limit);
        if let Some(s) = status {
            url.push_str(&format!("&status={}", s.as_str()));
        }

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("List failed: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn workers(&self) -> Result<Vec<Worker>, String> {
        let url = format!("{}/workers", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Failed to get workers: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }

    pub async fn metrics(&self) -> Result<MetricsResponse, String> {
        let url = format!("{}/metrics", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Network error: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Failed to get metrics: {body}"));
        }

        resp.json().await.map_err(|e| format!("Failed to parse response: {e}"))
    }
}
