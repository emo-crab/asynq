//! Dashboard page – shows an overview of all queues.

use crate::server::AppState;
use asynq::task::QueueInfo;
use axum::{
  extract::State,
  response::{Html, IntoResponse},
};
use dioxus::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;

use crate::ui::components::NavBar;
use crate::ui::helpers::{inspector_unavailable, make_page};

/// `GET /ui/` – Fetch queue statistics and render the dashboard.
pub async fn dashboard_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return inspector_unavailable();
  };

  let queues = inspector.get_all_queue_stats().await.unwrap_or_default();
  let mut queue_infos: Vec<QueueInfo> = Vec::new();
  // Aggregate by queue name: ensure we only fetch info once per unique queue
  let mut seen: HashSet<String> = HashSet::new();
  for q in &queues {
    if seen.insert(q.name.clone()) {
      if let Ok(info) = inspector.get_queue_info(&q.name).await {
        queue_infos.push(info);
      }
    }
  }

  render_dashboard(queue_infos)
}

struct QueueRow {
  name: String,
  paused: bool,
  size: i64,
  memory_bytes: i64,
  display_latency: String,
  processed: i64,
  failed: i64,
  error_rate: String,
}

fn format_bytes(bytes: i64) -> String {
  if bytes < 1024 {
    format!("{bytes} B")
  } else if bytes < 1024 * 1024 {
    format!("{:.2} kB", bytes as f64 / 1024.0)
  } else {
    format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
  }
}

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

fn render_dashboard(infos: Vec<QueueInfo>) -> Html<String> {
  let queue_count = infos.len();

  let rows: Vec<QueueRow> = infos
    .iter()
    .map(|info| {
      let latency_ms = info.latency.as_millis() as u64;
      let processed = info.processed as i64;
      let failed = info.failed as i64;
      let error_rate = if processed > 0 {
        format!("{:.2}%", failed as f64 / processed as f64 * 100.0)
      } else {
        "0.00%".to_string()
      };
      QueueRow {
        name: info.queue.clone(),
        paused: info.paused,
        size: info.size as i64,
        memory_bytes: info.memory_usage,
        display_latency: format_duration_ms(latency_ms),
        processed,
        failed,
        error_rate,
      }
    })
    .collect();

  let content = rsx! {
    NavBar { active: "queues" }
    div { class: "container",
      div { class: "page-header",
        h1 { "Queues" }
        span { class: "text-muted", "{queue_count} queue(s)" }
      }

      div { class: "card",
        if rows.is_empty() {
          div { class: "empty",
            div { class: "empty-icon", "📭" }
            p { "No queues found. Start enqueueing tasks to see queues here." }
          }
        } else {
          div { class: "table-wrap",
            table {
              thead {
                tr {
                  th { "Queue" }
                  th { "State" }
                  th { class: "text-right", "Size" }
                  th { class: "text-right", "Memory usage" }
                  th { class: "text-right", "Latency" }
                  th { class: "text-right", "Processed" }
                  th { class: "text-right", "Failed" }
                  th { class: "text-right", "Error rate" }
                  th { class: "text-center", "Actions" }
                }
              }
              tbody {
                for row in &rows {
                  {
                    let name = row.name.clone();
                    let paused = row.paused;
                    let size = row.size;
                    let memory = format_bytes(row.memory_bytes);
                    let latency = row.display_latency.clone();
                    let processed = row.processed;
                    let failed = row.failed;
                    let error_rate = row.error_rate.clone();
                    let q = name.clone();
                    rsx! {
                      tr {
                        td {
                          a { href: "/ui/queues/{name}", "{name}" }
                        }
                        td {
                          if paused {
                            span { class: "badge badge-paused", "⏸ Paused" }
                          } else {
                            span { class: "badge badge-running", "▶ Running" }
                          }
                        }
                        td { class: "text-right", "{size}" }
                        td { class: "text-right", "{memory}" }
                        td { class: "text-right", "{latency}" }
                        td { class: "text-right", "{processed}" }
                        td { class: "text-right", "{failed}" }
                        td { class: "text-right", "{error_rate}" }
                        td { class: "text-center",
                          div { class: "btn-group",
                            if paused {
                              form { method: "post", action: "/ui/queues/{q}/resume",
                                button { r#type: "submit", class: "btn btn-success btn-sm",
                                  "▶ Resume"
                                }
                              }
                            } else {
                              form { method: "post", action: "/ui/queues/{q}/pause",
                                button { r#type: "submit", class: "btn btn-warning btn-sm",
                                  "⏸ Pause"
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
  make_page("Dashboard", content)
}
