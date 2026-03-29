//! HTML form POST action handlers for queue and task management.

use crate::server::AppState;
use axum::{
  extract::{Path, Query, State},
  response::{IntoResponse, Redirect},
};
use serde::Deserialize;
use std::sync::Arc;

/// `POST /ui/queues/:queue/pause` – Pause a queue.
pub async fn ui_pause_queue(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    let _ = inspector.pause_queue(&queue).await;
  }
  Redirect::to(&format!("/ui/queues/{queue}"))
}

/// `POST /ui/queues/:queue/resume` – Resume a paused queue.
pub async fn ui_resume_queue(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    let _ = inspector.unpause_queue(&queue).await;
  }
  Redirect::to(&format!("/ui/queues/{queue}"))
}

/// `POST /ui/queues/:queue/tasks/:id/delete` – Delete a specific task.
pub async fn ui_delete_task(
  State(state): State<Arc<AppState>>,
  Path((queue, task_id)): Path<(String, String)>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    let _ = inspector.delete_task(&queue, &task_id).await;
  }
  Redirect::to(&format!("/ui/queues/{queue}"))
}

/// `POST /ui/queues/:queue/tasks/:id/run` – Run a task immediately.
pub async fn ui_run_task(
  State(state): State<Arc<AppState>>,
  Path((queue, task_id)): Path<(String, String)>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    let _ = inspector.run_task(&queue, &task_id).await;
  }
  Redirect::to(&format!("/ui/queues/{queue}"))
}

/// `POST /ui/queues/:queue/tasks/:id/archive` – Archive a specific task.
pub async fn ui_archive_task(
  State(state): State<Arc<AppState>>,
  Path((queue, task_id)): Path<(String, String)>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    let _ = inspector.archive_task(&queue, &task_id).await;
  }
  Redirect::to(&format!("/ui/queues/{queue}"))
}

/// Query parameters for bulk task operations.
#[derive(Deserialize)]
pub struct BulkStateQuery {
  pub state: Option<String>,
}

/// `POST /ui/queues/:queue/tasks/bulk-delete?state=xxx` – Delete all tasks in
/// the given state.
pub async fn ui_bulk_delete(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
  Query(params): Query<BulkStateQuery>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    match params.state.as_deref().unwrap_or("") {
      "archived" => {
        let _ = inspector.delete_all_archived_tasks(&queue).await;
      }
      "retry" => {
        let _ = inspector.delete_all_retry_tasks(&queue).await;
      }
      "scheduled" => {
        let _ = inspector.delete_all_scheduled_tasks(&queue).await;
      }
      "pending" => {
        let _ = inspector.delete_all_pending_tasks(&queue).await;
      }
      _ => {}
    }
  }
  let state_param = params.state.as_deref().unwrap_or("pending");
  Redirect::to(&format!("/ui/queues/{queue}/tasks?state={state_param}"))
}

/// `POST /ui/queues/:queue/tasks/requeue?state=xxx` – Requeue all tasks in the
/// given state.
pub async fn ui_requeue_tasks(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
  Query(params): Query<BulkStateQuery>,
) -> impl IntoResponse {
  if let Some(ref inspector) = state.inspector {
    match params.state.as_deref().unwrap_or("") {
      "archived" => {
        let _ = inspector.requeue_all_archived_tasks(&queue).await;
      }
      "retry" => {
        let _ = inspector.requeue_all_retry_tasks(&queue).await;
      }
      "scheduled" => {
        let _ = inspector.requeue_all_scheduled_tasks(&queue).await;
      }
      _ => {}
    }
  }
  let state_param = params.state.as_deref().unwrap_or("pending");
  Redirect::to(&format!("/ui/queues/{queue}/tasks?state={state_param}"))
}
