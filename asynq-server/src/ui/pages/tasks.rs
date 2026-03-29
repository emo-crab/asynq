//! Task-list page – browse tasks in a queue filtered by state, with pagination.

use crate::server::AppState;
use asynq::backend::pagination::Pagination;
use asynq::task::{QueueInfo, TaskInfo};
use axum::{
  extract::{Path, Query, State},
  response::{Html, IntoResponse},
};
use dioxus::prelude::*;
use serde::Deserialize;
use std::sync::Arc;

use crate::ui::components::NavBar;
use crate::ui::helpers::{escape_html, inspector_unavailable, make_page, parse_task_state};
use crate::ui::styles::TASK_PAGE_SIZE;

/// Query parameters accepted by the task-list page.
#[derive(Deserialize)]
pub struct TasksQuery {
  #[serde(default = "default_state_str")]
  pub state: String,
  #[serde(default)]
  pub page_num: i64,
}

fn default_state_str() -> String {
  "pending".to_string()
}

/// `GET /ui/queues/:queue/tasks` – Fetch and render the paginated task list.
pub async fn tasks_handler(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
  Query(params): Query<TasksQuery>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return inspector_unavailable();
  };

  let task_state = parse_task_state(&params.state);
  let cur_page = params.page_num.max(0);
  let pagination = Pagination {
    size: TASK_PAGE_SIZE,
    page: cur_page,
  };

  let tasks = inspector
    .list_tasks(&queue, task_state, pagination)
    .await
    .unwrap_or_default();

  let info = inspector.get_queue_info(&queue).await.ok();
  let total_for_state = info.as_ref().map(|i| match task_state {
    asynq::base::keys::TaskState::Active => i.active as i64,
    asynq::base::keys::TaskState::Pending => i.pending as i64,
    asynq::base::keys::TaskState::Scheduled => i.scheduled as i64,
    asynq::base::keys::TaskState::Retry => i.retry as i64,
    asynq::base::keys::TaskState::Archived => i.archived as i64,
    asynq::base::keys::TaskState::Completed => i.completed as i64,
    asynq::base::keys::TaskState::Aggregating => i.aggregating as i64,
  });

  render_tasks(
    queue,
    tasks,
    params.state,
    cur_page,
    TASK_PAGE_SIZE,
    total_for_state.unwrap_or(0),
    info,
  )
}

/// Pre-built row data to avoid ownership issues inside RSX loops.
struct TaskRow {
  id: String,
  id_short: String,
  task_type: String,
  payload: String,
  retried: i32,
  max_retry: i32,
  last_err: String,
  next_run: String,
  completed_at_str: String,
  queue: String,
}

fn payload_to_string(payload: &[u8]) -> String {
  // Try to parse as JSON for pretty display, fall back to lossy UTF-8
  if let Ok(s) = std::str::from_utf8(payload) {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
      return serde_json::to_string_pretty(&v).unwrap_or_else(|_| s.to_string());
    }
    s.to_string()
  } else {
    format!("<{} bytes>", payload.len())
  }
}

#[allow(clippy::too_many_arguments)]
fn render_tasks(
  queue: String,
  tasks: Vec<TaskInfo>,
  state_filter: String,
  cur_page: i64,
  page_size: i64,
  total: i64,
  info: Option<QueueInfo>,
) -> Html<String> {
  let has_prev = cur_page > 0;
  let has_next = (cur_page + 1) * page_size < total;
  let q = queue.clone();
  let sf = state_filter.clone();

  let active_count = info.as_ref().map(|i| i.active as i64).unwrap_or(0);
  let pending_count = info.as_ref().map(|i| i.pending as i64).unwrap_or(0);
  let scheduled_count = info.as_ref().map(|i| i.scheduled as i64).unwrap_or(0);
  let retry_count = info.as_ref().map(|i| i.retry as i64).unwrap_or(0);
  let archived_count = info.as_ref().map(|i| i.archived as i64).unwrap_or(0);
  let completed_count = info.as_ref().map(|i| i.completed as i64).unwrap_or(0);

  let prev_page = cur_page - 1;
  let next_page = cur_page + 1;
  let cur_page_display = cur_page + 1;

  let show_err_col = state_filter == "retry" || state_filter == "archived";
  let show_nextrun_col = state_filter == "scheduled" || state_filter == "retry";
  let show_completed_col = state_filter == "completed";

  let rows: Vec<TaskRow> = tasks
    .iter()
    .map(|t| {
      let id = t.id.clone();
      let id_short = if id.len() > 8 {
        format!("{}…", &id[..8])
      } else {
        id.clone()
      };
      TaskRow {
        id,
        id_short,
        task_type: t.task_type.clone(),
        payload: payload_to_string(&t.payload),
        retried: t.retried,
        max_retry: t.max_retry,
        last_err: t.last_err.clone().unwrap_or_default(),
        next_run: t
          .next_process_at
          .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
          .unwrap_or_default(),
        completed_at_str: t
          .completed_at
          .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
          .unwrap_or_default(),
        queue: t.queue.clone(),
      }
    })
    .collect();

  let sf2 = sf.clone();

  let content = rsx! {
    NavBar { active: "queues" }
    div { class: "container",
      div { class: "breadcrumb",
        a { href: "/ui/", "Dashboard" }
        " \u{203a} "
        a { href: "/ui/queues/{queue}", "{queue}" }
        " \u{203a} "
        span { "Tasks ({state_filter})" }
      }
      div { class: "page-header",
        h1 { "Tasks in \"{queue}\"" }
        if state_filter != "active" && state_filter != "completed" {
          div { class: "btn-group",
            if matches!(sf.as_str(), "archived" | "retry" | "scheduled") {
              form { method: "post", action: "/ui/queues/{q}/tasks/requeue?state={sf2}",
                button { r#type: "submit", class: "btn btn-secondary btn-sm",
                  "\u{21a9} Requeue All"
                }
              }
            }
            {
              let sf3 = sf.clone();
              let q2 = q.clone();
              rsx! {
                form { method: "post", action: "/ui/queues/{q2}/tasks/bulk-delete?state={sf3}",
                  button { r#type: "submit", class: "btn btn-danger btn-sm",
                    "\u{1f5d1} Delete All {state_filter}"
                  }
                }
              }
            }
          }
        }
      }

      // State tabs
      div { class: "tabs",
        for (tab_state, label, count) in [
          ("active", "Active", active_count),
          ("pending", "Pending", pending_count),
          ("scheduled", "Scheduled", scheduled_count),
          ("retry", "Retry", retry_count),
          ("archived", "Archived", archived_count),
          ("completed", "Completed", completed_count),
        ] {
          {
            let tab_class = if tab_state == state_filter { "tab active" } else { "tab" };
            rsx! {
              div { class: tab_class,
                a { href: "/ui/queues/{queue}/tasks?state={tab_state}",
                  "{label}"
                  span { class: "count", "{count}" }
                }
              }
            }
          }
        }
      }

      // Task table
      div { class: "card",
        if rows.is_empty() {
          div { class: "empty",
            div { class: "empty-icon", "\u{1f4ed}" }
            p { "No {state_filter} tasks in queue \"{queue}\"" }
          }
        } else {
          div { class: "table-wrap",
            table {
              thead {
                tr {
                  th { "ID" }
                  th { "Type" }
                  th { "Payload" }
                  th { class: "text-right", "Retried" }
                  th { class: "text-right", "Max Retry" }
                  if show_err_col { th { "Error" } }
                  if show_nextrun_col { th { "Next Run" } }
                  if show_completed_col { th { "Completed" } }
                  th { class: "text-center", "Actions" }
                }
              }
              tbody {
                for row in &rows {
                  {
                    let task_id = row.id.clone();
                    let task_type = row.task_type.clone();
                    let id_short = row.id_short.clone();
                    let retried = row.retried;
                    let max_retry = row.max_retry;
                    let last_err = row.last_err.clone();
                    let next_run = row.next_run.clone();
                    let completed_at_str = row.completed_at_str.clone();
                    let row_queue = row.queue.clone();
                    let payload_escaped = escape_html(&row.payload);
                    let sf4 = state_filter.clone();
                    rsx! {
                      tr {
                        td { class: "task-id", title: "{task_id}", "{id_short}" }
                        td { class: "task-type", "{task_type}" }
                        td {
                          pre { class: "payload-pre",
                            dangerous_inner_html: "{payload_escaped}"
                          }
                        }
                        td { class: "text-right", "{retried}" }
                        td { class: "text-right", "{max_retry}" }
                        if show_err_col {
                          td { class: "err-msg", title: "{last_err}", "{last_err}" }
                        }
                        if show_nextrun_col {
                          td { class: "text-muted", "{next_run}" }
                        }
                        if show_completed_col {
                          td { class: "text-muted", "{completed_at_str}" }
                        }
                        td { class: "text-center",
                          div { class: "btn-group",
                            // Run immediately button
                            if sf4 != "active" && sf4 != "completed" {
                              form { method: "post", action: "/ui/queues/{row_queue}/tasks/{task_id}/run",
                                button { r#type: "submit", class: "btn btn-primary btn-sm",
                                  "\u{25b6} Run"
                                }
                              }
                            }
                            // Archive button (for pending/scheduled/retry)
                            if sf4 == "pending" || sf4 == "scheduled" || sf4 == "retry" {
                              {
                                let row_queue2 = row_queue.clone();
                                let task_id2 = task_id.clone();
                                rsx! {
                                  form { method: "post", action: "/ui/queues/{row_queue2}/tasks/{task_id2}/archive",
                                    button { r#type: "submit", class: "btn btn-secondary btn-sm",
                                      "\u{1f4e6} Archive"
                                    }
                                  }
                                }
                              }
                            }
                            // Delete button
                            {
                              let row_queue3 = row_queue.clone();
                              let task_id3 = task_id.clone();
                              rsx! {
                                form { method: "post", action: "/ui/queues/{row_queue3}/tasks/{task_id3}/delete",
                                  button { r#type: "submit", class: "btn btn-danger btn-sm",
                                    "\u{1f5d1}"
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }

          // Pagination controls
          if has_prev || has_next {
            div { class: "pagination",
              if has_prev {
                a { class: "pager", href: "/ui/queues/{q}/tasks?state={sf}&page_num={prev_page}",
                  "\u{2190} Prev"
                }
              } else {
                span { class: "pager disabled", "\u{2190} Prev" }
              }
              span { class: "pager active", "Page {cur_page_display}" }
              if has_next {
                a { class: "pager", href: "/ui/queues/{q}/tasks?state={sf}&page_num={next_page}",
                  "Next \u{2192}"
                }
              } else {
                span { class: "pager disabled", "Next \u{2192}" }
              }
            }
          }
        }
      }
    }
  };
  make_page(&format!("Tasks: {queue} [{state_filter}]"), content)
}
