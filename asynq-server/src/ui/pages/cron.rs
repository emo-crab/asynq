//! Scheduled Tasks (定时任务) page – aggregates tasks in the "scheduled"
//! state across all queues, giving a system-wide view of upcoming work.

use crate::server::AppState;
use asynq::backend::pagination::Pagination;
use asynq::base::keys::TaskState;
use asynq::task::TaskInfo;
use axum::{
  extract::State,
  response::{Html, IntoResponse},
};
use dioxus::prelude::*;
use std::sync::Arc;

use crate::ui::components::NavBar;
use crate::ui::helpers::{inspector_unavailable, make_page};
use crate::ui::styles::TASK_PAGE_SIZE;

/// `GET /ui/cron` – Fetch scheduled tasks from all queues and render the page.
pub async fn cron_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return inspector_unavailable();
  };

  let queue_names = inspector.get_queues().await.unwrap_or_default();

  // Collect scheduled tasks from every queue (first page only for overview)
  let pagination = Pagination {
    size: TASK_PAGE_SIZE,
    page: 0,
  };

  let mut all_tasks: Vec<TaskInfo> = Vec::new();
  for queue in &queue_names {
    let tasks = inspector
      .list_tasks(queue, TaskState::Scheduled, pagination)
      .await
      .unwrap_or_default();
    all_tasks.extend(tasks);
  }

  // Sort by next_process_at ascending
  all_tasks.sort_by(|a, b| a.next_process_at.cmp(&b.next_process_at));

  render_cron(all_tasks, queue_names.len())
}

struct CronRow {
  id: String,
  id_short: String,
  queue: String,
  task_type: String,
  next_run: String,
}

fn render_cron(tasks: Vec<TaskInfo>, queue_count: usize) -> Html<String> {
  let count = tasks.len();

  let rows: Vec<CronRow> = tasks
    .iter()
    .map(|t| {
      let id = t.id.clone();
      let id_short = id.chars().take(12).chain(['…']).collect::<String>();
      CronRow {
        id,
        id_short,
        queue: t.queue.clone(),
        task_type: t.task_type.clone(),
        next_run: t
          .next_process_at
          .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
          .unwrap_or_default(),
      }
    })
    .collect();

  let content = rsx! {
    NavBar { active: "cron" }
    div { class: "container",
      div { class: "page-header",
        h1 { "Scheduled Tasks (定时任务)" }
        span { class: "text-muted", "{count} scheduled task(s) across {queue_count} queue(s)" }
      }

      div { class: "card",
        if rows.is_empty() {
          div { class: "empty",
            div { class: "empty-icon", "🗓" }
            p { "No scheduled tasks found across all queues." }
          }
        } else {
          div { class: "table-wrap",
            table {
              thead {
                tr {
                  th { "Task ID" }
                  th { "Queue" }
                  th { "Type" }
                  th { "Next Run (UTC)" }
                  th { "Actions" }
                }
              }
              tbody {
                for row in &rows {
                  {
                    let task_id = row.id.clone();
                    let queue = row.queue.clone();
                    let queue2 = queue.clone();
                    let task_id2 = task_id.clone();
                    let id_short = row.id_short.clone();
                    let task_type = row.task_type.clone();
                    let next_run = row.next_run.clone();
                    rsx! {
                      tr {
                        td { class: "task-id", title: "{task_id}", "{id_short}" }
                        td {
                          a { href: "/ui/queues/{queue}/tasks?state=scheduled", "{queue}" }
                        }
                        td { class: "task-type", "{task_type}" }
                        td { class: "text-muted c-scheduled", "{next_run}" }
                        td {
                          div { class: "btn-group",
                            form { method: "post", action: "/ui/queues/{queue2}/tasks/{task_id2}/run",
                              button { r#type: "submit", class: "btn btn-primary btn-sm",
                                "▶ Run Now"
                              }
                            }
                            {
                              let queue3 = row.queue.clone();
                              let task_id3 = task_id.clone();
                              rsx! {
                                form { method: "post", action: "/ui/queues/{queue3}/tasks/{task_id3}/delete",
                                  button { r#type: "submit", class: "btn btn-danger btn-sm",
                                    "🗑"
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
        }
      }
    }
  };
  make_page("Scheduled Tasks", content)
}
