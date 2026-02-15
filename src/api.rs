use crate::models::{
    BatchSubmitRequest, CompleteJobRequest, JobStatus, RenewLeaseRequest, SubmitJobRequest,
    WorkerPollRequest, WorkerRegisterRequest,
};
use crate::scheduler::Scheduler;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub type AppState = Arc<Scheduler>;

pub fn create_router(scheduler: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .route("/jobs", post(submit_job).get(list_jobs))
        .route("/jobs/batch", post(submit_batch))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/logs", get(get_job_logs))
        .route("/jobs/{id}/renew", post(renew_lease))
        .route("/jobs/{id}/complete", post(complete_job))
        .route("/workers", get(list_workers))
        .route("/workers/register", post(register_worker))
        .route("/workers/{id}/heartbeat", post(worker_heartbeat))
        .route("/workers/{id}/poll", post(worker_poll))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(scheduler)
}

async fn health_check() -> impl IntoResponse {
    Json(json!({ "status": "ok", "service": "relay" }))
}

async fn get_metrics(State(scheduler): State<AppState>) -> Response {
    match scheduler.get_metrics() {
        Ok(metrics) => Json(metrics).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn submit_job(
    State(scheduler): State<AppState>,
    Json(req): Json<SubmitJobRequest>,
) -> Response {
    match scheduler.submit_job(req) {
        Ok(resp) => (StatusCode::CREATED, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn submit_batch(
    State(scheduler): State<AppState>,
    Json(req): Json<BatchSubmitRequest>,
) -> Response {
    match scheduler.submit_batch(req) {
        Ok(resp) => (StatusCode::CREATED, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_job(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match scheduler.store().get_job(&id) {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Job {} not found", id) })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct ListJobsParams {
    status: Option<String>,
    limit: Option<usize>,
}

async fn list_jobs(
    State(scheduler): State<AppState>,
    Query(params): Query<ListJobsParams>,
) -> Response {
    let status_filter = params.status.and_then(|s| s.parse::<JobStatus>().ok());
    let limit = params.limit.unwrap_or(100);

    match scheduler.store().list_jobs(status_filter, limit) {
        Ok(jobs) => Json(jobs).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn cancel_job(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match scheduler.cancel_job(&id) {
        Ok(job) => Json(json!({ "status": "cancelled", "job": job })).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_job_logs(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match scheduler.store().get_job(&id) {
        Ok(Some(job)) => {
            let file_log = fs::read_to_string(format!("o/out.{}", id)).ok();
            let stdout = job.stdout.unwrap_or_default();
            let stderr = job.stderr.unwrap_or_default();

            Json(json!({
                "job_id": id,
                "exit_code": job.exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "log_file": file_log
            }))
            .into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Job {} not found", id) })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn renew_lease(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<RenewLeaseRequest>,
) -> Response {
    match scheduler.renew_lease(&id, req) {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn complete_job(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CompleteJobRequest>,
) -> Response {
    match scheduler.complete_job(&id, req) {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn list_workers(State(scheduler): State<AppState>) -> Response {
    match scheduler.store().list_workers() {
        Ok(workers) => Json(workers).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn register_worker(
    State(scheduler): State<AppState>,
    Json(req): Json<WorkerRegisterRequest>,
) -> Response {
    match scheduler.register_worker(req) {
        Ok(_) => Json(json!({ "status": "registered" })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn worker_heartbeat(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match scheduler.worker_heartbeat(&id) {
        Ok(true) => Json(json!({ "status": "heartbeat_acknowledged" })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Worker not registered" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn worker_poll(
    State(scheduler): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<WorkerPollRequest>,
) -> Response {
    match scheduler.poll_work(&id, req) {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
