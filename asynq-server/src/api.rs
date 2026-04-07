//! REST API handlers for the Asynq web dashboard
//!
//! This module provides JSON REST API endpoints that power the web UI dashboard.
//! All endpoints require an InspectorTrait-capable backend.

use crate::server::AppState;
use asynq::backend::pagination::Pagination;
use asynq::base::keys::TaskState;
use axum::{
  extract::{Path, Query, State},
  http::StatusCode,
  response::IntoResponse,
  Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::{IntoParams, OpenApi, ToSchema};

/// Maximum page size for task listing
const MAX_PAGE_SIZE: i64 = 100;

/// Query parameters for task listing
#[derive(Debug, Deserialize, IntoParams)]
pub struct TaskListQuery {
  /// Task state filter (active, pending, scheduled, retry, archived, completed, aggregating)
  #[serde(default = "default_state")]
  pub state: String,
  /// Page number (0-based)
  #[serde(default)]
  pub page: i64,
  /// Page size
  #[serde(default = "default_page_size")]
  pub size: i64,
}

fn default_state() -> String {
  "pending".to_string()
}
fn default_page_size() -> i64 {
  20
}

/// API error response
#[derive(Serialize, ToSchema)]
pub struct ApiError {
  pub error: String,
}

impl ApiError {
  pub fn new(msg: impl Into<String>) -> Self {
    Self { error: msg.into() }
  }
}

/// Queue stats summary for API response
#[derive(Serialize, ToSchema)]
pub struct QueueStatsResponse {
  pub queue: String,
  pub memory_usage_bytes: i64,
  pub size: i64,
  pub groups: i64,
  pub latency_msec: u64,
  pub display_latency: String,
  pub active: i64,
  pub pending: i64,
  pub aggregating: i64,
  pub scheduled: i64,
  pub retry: i64,
  pub archived: i64,
  pub completed: i64,
  pub processed: i64,
  pub succeeded: i64,
  pub failed: i64,
  pub paused: bool,
  pub timestamp: String,
}

// Add a small named type to replace the complex tuple used for aggregation
#[derive(Default)]
struct AggCounts {
  active: i64,
  pending: i64,
  scheduled: i64,
  retry: i64,
  archived: i64,
  completed: i64,
  aggregating: i64,
}

/// Wrapper for the list-queues response
#[derive(Serialize, ToSchema)]
pub struct QueueListResponse {
  pub queues: Vec<QueueStatsResponse>,
}

/// Format a duration (milliseconds) as a human-readable string, e.g. "7m29.47s".
fn format_duration_ms(ms: u64) -> String {
  let minutes = ms / 60000;
  let remaining_ms = ms % 60000;
  let secs = remaining_ms as f64 / 1000.0;
  if minutes > 0 {
    format!("{minutes}m{secs:.2}s")
  } else {
    format!("{secs:.2}s")
  }
}

/// Task info for API response
#[derive(Serialize, ToSchema)]
pub struct TaskInfoResponse {
  pub id: String,
  pub queue: String,
  pub task_type: String,
  pub state: String,
  pub max_retry: i32,
  pub retried: i32,
  pub last_err: Option<String>,
  pub last_failed_at: Option<String>,
  pub next_process_at: Option<String>,
  pub completed_at: Option<String>,
  pub deadline: Option<String>,
  pub group: Option<String>,
  pub is_orphaned: bool,
}

/// Paginated task list response
#[derive(Serialize, ToSchema)]
pub struct TaskListResponse {
  pub tasks: Vec<TaskInfoResponse>,
  pub state: String,
  pub queue: String,
  pub page: i64,
  pub size: i64,
}

/// Success response
#[derive(Serialize, ToSchema)]
pub struct SuccessResponse {
  pub message: String,
  pub count: Option<i64>,
}

/// Query parameters for bulk-delete operations
#[derive(Deserialize, IntoParams)]
pub struct BulkDeleteQuery {
  /// Task state to delete (archived, retry, scheduled, pending)
  pub state: String,
}

/// Query parameters for requeue operations
#[derive(Deserialize, IntoParams)]
pub struct RequeueQuery {
  /// Task state to requeue (archived, retry, scheduled)
  pub state: String,
}

/// OpenAPI document aggregating all REST API paths and schemas
#[derive(OpenApi)]
#[openapi(
  paths(
    list_queues,
    get_queue,
    list_tasks,
    delete_task,
    run_task,
    archive_task,
    bulk_delete_tasks,
    pause_queue,
    resume_queue,
    requeue_tasks,
  ),
  components(schemas(
    QueueStatsResponse,
    QueueListResponse,
    TaskInfoResponse,
    TaskListResponse,
    ApiError,
    SuccessResponse,
  )),
  tags(
    (name = "queues", description = "Queue management operations"),
    (name = "tasks", description = "Task management operations"),
  ),
  info(
    title = "Asynq Server API",
    description = "REST API for monitoring and managing Asynq task queues",
    version = "0.1.0",
  ),
)]
pub struct ApiDoc;

/// GET /api/queues – list all queues with stats
#[utoipa::path(
  get,
  path = "/api/queues",
  responses(
    (status = 200, description = "List of all queues with statistics", body = QueueListResponse),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "queues",
)]
pub async fn list_queues(State(state): State<Arc<AppState>>) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.get_all_queue_stats().await {
    Ok(stats) => {
      // Aggregate stats by queue name (sum counters for duplicate queue entries)
      let mut agg: HashMap<String, AggCounts> = HashMap::new();
      // Aggregate using a small named struct for clarity
      for qs in stats {
        let e = agg.entry(qs.name).or_default();
        e.active += qs.active;
        e.pending += qs.pending;
        e.scheduled += qs.scheduled;
        e.retry += qs.retry;
        e.archived += qs.archived;
        e.completed += qs.completed;
        e.aggregating += qs.aggregating;
      }

      let mut responses: Vec<QueueStatsResponse> = Vec::new();
      for (
        queue_name,
        AggCounts {
          active,
          pending,
          scheduled,
          retry,
          archived,
          completed,
          aggregating,
        },
      ) in agg
      {
        let info = inspector.get_queue_info(&queue_name).await.ok();
        let paused = info.as_ref().map(|i| i.paused).unwrap_or(false);
        let latency_msec = info
          .as_ref()
          .map(|i| i.latency.as_millis() as u64)
          .unwrap_or(0);
        let processed = info.as_ref().map(|i| i.processed as i64).unwrap_or(0);
        let failed = info.as_ref().map(|i| i.failed as i64).unwrap_or(0);
        responses.push(QueueStatsResponse {
          queue: queue_name,
          active,
          pending,
          scheduled,
          retry,
          archived,
          completed,
          aggregating,
          processed,
          // succeeded = tasks that completed without failure (processed includes both success and failure)
          succeeded: (processed - failed).max(0),
          failed,
          paused,
          size: info.as_ref().map(|i| i.size as i64).unwrap_or(0),
          groups: info.as_ref().map(|i| i.groups as i64).unwrap_or(0),
          latency_msec,
          display_latency: format_duration_ms(latency_msec),
          memory_usage_bytes: info.as_ref().map(|i| i.memory_usage).unwrap_or(0),
          timestamp: info
            .as_ref()
            .map(|i| i.timestamp.to_rfc3339())
            .unwrap_or_default(),
        });
      }

      let body = QueueListResponse { queues: responses };
      (StatusCode::OK, Json(serde_json::to_value(body).unwrap()))
    }
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// GET /api/queues/:queue – get a single queue's stats
#[utoipa::path(
  get,
  path = "/api/queues/{queue}",
  params(
    ("queue" = String, Path, description = "Queue name"),
  ),
  responses(
    (status = 200, description = "Queue statistics", body = QueueStatsResponse),
    (status = 404, description = "Queue not found", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "queues",
)]
pub async fn get_queue(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.get_queue_info(&queue).await {
    Ok(info) => {
      let latency_msec = info.latency.as_millis() as u64;
      let processed = info.processed as i64;
      let failed = info.failed as i64;
      let resp = QueueStatsResponse {
        queue: info.queue.clone(),
        active: info.active as i64,
        pending: info.pending as i64,
        scheduled: info.scheduled as i64,
        retry: info.retry as i64,
        archived: info.archived as i64,
        completed: info.completed as i64,
        aggregating: info.aggregating as i64,
        processed,
        // succeeded = tasks that completed without failure (processed includes both success and failure)
        succeeded: (processed - failed).max(0),
        failed,
        paused: info.paused,
        size: info.size as i64,
        groups: info.groups as i64,
        latency_msec,
        display_latency: format_duration_ms(latency_msec),
        memory_usage_bytes: info.memory_usage,
        timestamp: info.timestamp.to_rfc3339(),
      };
      (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()))
    }
    Err(e) => (
      StatusCode::NOT_FOUND,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// GET /api/queues/:queue/tasks – list tasks in a queue filtered by state
#[utoipa::path(
  get,
  path = "/api/queues/{queue}/tasks",
  params(
    ("queue" = String, Path, description = "Queue name"),
    TaskListQuery,
  ),
  responses(
    (status = 200, description = "Paginated list of tasks", body = TaskListResponse),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "tasks",
)]
pub async fn list_tasks(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
  Query(params): Query<TaskListQuery>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  let task_state = match params.state.as_str() {
    "active" => TaskState::Active,
    "pending" => TaskState::Pending,
    "scheduled" => TaskState::Scheduled,
    "retry" => TaskState::Retry,
    "archived" => TaskState::Archived,
    "completed" => TaskState::Completed,
    "aggregating" => TaskState::Aggregating,
    _ => TaskState::Pending,
  };

  let pagination = Pagination {
    size: params.size.clamp(1, MAX_PAGE_SIZE),
    page: params.page.max(0),
  };

  match inspector.list_tasks(&queue, task_state, pagination).await {
    Ok(tasks) => {
      let responses: Vec<TaskInfoResponse> = tasks
        .into_iter()
        .map(|t| TaskInfoResponse {
          id: t.id,
          queue: t.queue,
          task_type: t.task_type,
          state: t.state.as_str().to_string(),
          max_retry: t.max_retry,
          retried: t.retried,
          last_err: t.last_err,
          last_failed_at: t.last_failed_at.map(|dt| dt.to_rfc3339()),
          next_process_at: t.next_process_at.map(|dt| dt.to_rfc3339()),
          completed_at: t.completed_at.map(|dt| dt.to_rfc3339()),
          deadline: t.deadline.map(|dt| dt.to_rfc3339()),
          group: t.group,
          is_orphaned: t.is_orphaned,
        })
        .collect();

      let resp = TaskListResponse {
        tasks: responses,
        state: params.state.clone(),
        queue,
        page: params.page,
        size: params.size,
      };
      (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()))
    }
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// DELETE /api/queues/:queue/tasks/:task_id – delete a specific task
#[utoipa::path(
  delete,
  path = "/api/queues/{queue}/tasks/{task_id}",
  params(
    ("queue" = String, Path, description = "Queue name"),
    ("task_id" = String, Path, description = "Task ID"),
  ),
  responses(
    (status = 200, description = "Task deleted successfully", body = SuccessResponse),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "tasks",
)]
pub async fn delete_task(
  State(state): State<Arc<AppState>>,
  Path((queue, task_id)): Path<(String, String)>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.delete_task(&queue, &task_id).await {
    Ok(()) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": "Task deleted successfully" })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// POST /api/queues/:queue/tasks/:task_id/run – run a task immediately
#[utoipa::path(
  post,
  path = "/api/queues/{queue}/tasks/{task_id}/run",
  params(
    ("queue" = String, Path, description = "Queue name"),
    ("task_id" = String, Path, description = "Task ID"),
  ),
  responses(
    (status = 200, description = "Task enqueued for immediate processing", body = SuccessResponse),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "tasks",
)]
pub async fn run_task(
  State(state): State<Arc<AppState>>,
  Path((queue, task_id)): Path<(String, String)>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.run_task(&queue, &task_id).await {
    Ok(()) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": "Task enqueued for immediate processing" })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// POST /api/queues/:queue/tasks/:task_id/archive – archive a specific task
#[utoipa::path(
  post,
  path = "/api/queues/{queue}/tasks/{task_id}/archive",
  params(
    ("queue" = String, Path, description = "Queue name"),
    ("task_id" = String, Path, description = "Task ID"),
  ),
  responses(
    (status = 200, description = "Task archived successfully", body = SuccessResponse),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "tasks",
)]
pub async fn archive_task(
  State(state): State<Arc<AppState>>,
  Path((queue, task_id)): Path<(String, String)>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.archive_task(&queue, &task_id).await {
    Ok(()) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": "Task archived successfully" })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// DELETE /api/queues/:queue/tasks – bulk delete tasks by state
#[utoipa::path(
  delete,
  path = "/api/queues/{queue}/tasks",
  params(
    ("queue" = String, Path, description = "Queue name"),
    BulkDeleteQuery,
  ),
  responses(
    (status = 200, description = "Tasks deleted successfully", body = SuccessResponse),
    (status = 400, description = "Invalid task state", body = ApiError),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "tasks",
)]
pub async fn bulk_delete_tasks(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
  Query(params): Query<BulkDeleteQuery>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  let result = match params.state.as_str() {
    "archived" => inspector.delete_all_archived_tasks(&queue).await,
    "retry" => inspector.delete_all_retry_tasks(&queue).await,
    "scheduled" => inspector.delete_all_scheduled_tasks(&queue).await,
    "pending" => inspector.delete_all_pending_tasks(&queue).await,
    _ => {
      return (
        StatusCode::BAD_REQUEST,
        Json(
          serde_json::json!({ "error": "Invalid state. Use: archived, retry, scheduled, pending" }),
        ),
      );
    }
  };

  match result {
    Ok(count) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": format!("Deleted {} tasks", count), "count": count })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// POST /api/queues/:queue/pause – pause a queue
#[utoipa::path(
  post,
  path = "/api/queues/{queue}/pause",
  params(
    ("queue" = String, Path, description = "Queue name"),
  ),
  responses(
    (status = 200, description = "Queue paused successfully", body = SuccessResponse),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "queues",
)]
pub async fn pause_queue(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.pause_queue(&queue).await {
    Ok(()) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": format!("Queue '{}' paused", queue) })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// POST /api/queues/:queue/resume – resume a paused queue
#[utoipa::path(
  post,
  path = "/api/queues/{queue}/resume",
  params(
    ("queue" = String, Path, description = "Queue name"),
  ),
  responses(
    (status = 200, description = "Queue resumed successfully", body = SuccessResponse),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "queues",
)]
pub async fn resume_queue(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  match inspector.unpause_queue(&queue).await {
    Ok(()) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": format!("Queue '{}' resumed", queue) })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}

/// POST /api/queues/:queue/tasks/requeue – requeue all tasks by state
#[utoipa::path(
  post,
  path = "/api/queues/{queue}/tasks/requeue",
  params(
    ("queue" = String, Path, description = "Queue name"),
    RequeueQuery,
  ),
  responses(
    (status = 200, description = "Tasks requeued successfully", body = SuccessResponse),
    (status = 400, description = "Invalid task state", body = ApiError),
    (status = 500, description = "Internal server error", body = ApiError),
    (status = 503, description = "Inspector not available", body = ApiError),
  ),
  tag = "tasks",
)]
pub async fn requeue_tasks(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
  Query(params): Query<RequeueQuery>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return (
      StatusCode::SERVICE_UNAVAILABLE,
      Json(serde_json::json!({ "error": "Inspector not available" })),
    );
  };

  let result = match params.state.as_str() {
    "archived" => inspector.requeue_all_archived_tasks(&queue).await,
    "retry" => inspector.requeue_all_retry_tasks(&queue).await,
    "scheduled" => inspector.requeue_all_scheduled_tasks(&queue).await,
    _ => {
      return (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": "Invalid state. Use: archived, retry, scheduled" })),
      );
    }
  };

  match result {
    Ok(count) => (
      StatusCode::OK,
      Json(serde_json::json!({ "message": format!("Requeued {} tasks", count), "count": count })),
    ),
    Err(e) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      Json(serde_json::json!({ "error": e.to_string() })),
    ),
  }
}
