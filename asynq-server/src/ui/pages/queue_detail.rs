//! Queue detail page – shows statistics and task-state breakdown for one queue.

use crate::server::AppState;
use asynq::task::QueueInfo;
use axum::{
  extract::{Path, State},
  response::{Html, IntoResponse},
};
use dioxus::prelude::*;
use std::sync::Arc;

use crate::ui::components::NavBar;
use crate::ui::helpers::{error_page, inspector_unavailable, make_page};

/// `GET /ui/queues/:queue` – Fetch queue info and render the detail page.
pub async fn queue_detail_handler(
  State(state): State<Arc<AppState>>,
  Path(queue): Path<String>,
) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return inspector_unavailable();
  };

  let info = match inspector.get_queue_info(&queue).await {
    Ok(i) => i,
    Err(e) => {
      return error_page(&format!("Queue '{}' not found: {}", queue, e));
    }
  };

  render_queue_detail(queue, info)
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

fn render_queue_detail(queue: String, info: QueueInfo) -> Html<String> {
  let latency_ms = info.latency.as_millis() as u64;
  let display_latency = format_duration_ms(latency_ms);
  let memory_str = format_bytes(info.memory_usage);
  let is_paused = info.paused;

  let active_count = info.active as i64;
  let pending_count = info.pending as i64;
  let scheduled_count = info.scheduled as i64;
  let retry_count = info.retry as i64;
  let archived_count = info.archived as i64;
  let completed_count = info.completed as i64;
  let aggregating_count = info.aggregating as i64;

  let processed = info.processed as i64;
  let failed = info.failed as i64;
  let error_rate = if processed > 0 {
    format!("{:.2}%", failed as f64 / processed as f64 * 100.0)
  } else {
    "0.00%".to_string()
  };

  let q = queue.clone();

  let content = rsx! {
    NavBar { active: "queues" }
    div { class: "container",
      div { class: "breadcrumb",
        a { href: "/ui/", "Dashboard" }
        " \u{203a} "
        span { "{queue}" }
      }
      div { class: "page-header",
        div {
          h1 { "{queue}" }
          " "
          if is_paused {
            span { class: "badge badge-paused", "\u{23f8} Paused" }
          } else {
            span { class: "badge badge-running", "\u{25b6} Running" }
          }
        }
        div { class: "btn-group",
          if is_paused {
            form { method: "post", action: "/ui/queues/{q}/resume",
              button { r#type: "submit", class: "btn btn-success",
                "\u{25b6} Resume Queue"
              }
            }
          } else {
            form { method: "post", action: "/ui/queues/{q}/pause",
              button { r#type: "submit", class: "btn btn-warning",
                "\u{23f8} Pause Queue"
              }
            }
          }
        }
      }

      // Queue info summary cards
      div { class: "grid grid-5",
        div { class: "card",
          div { class: "card-title", "Memory usage" }
          div { class: "card-value", "{memory_str}" }
        }
        div { class: "card",
          div { class: "card-title", "Latency" }
          div { class: "card-value", "{display_latency}" }
        }
        div { class: "card",
          div { class: "card-title", "Processed" }
          div { class: "card-value c-completed", "{processed}" }
        }
        div { class: "card",
          div { class: "card-title", "Failed" }
          div { class: "card-value c-retry", "{failed}" }
        }
        div { class: "card",
          div { class: "card-title", "Error rate" }
          div { class: "card-value", "{error_rate}" }
        }
      }

      // Task state chips
      div { class: "section-title", "Tasks" }
      div { class: "state-chips",
        for (tab_state, label, count, is_primary) in [
          ("active", "Active", active_count, true),
          ("pending", "Pending", pending_count, false),
          ("aggregating", "Aggregating", aggregating_count, false),
          ("scheduled", "Scheduled", scheduled_count, false),
          ("retry", "Retry", retry_count, false),
          ("archived", "Archived", archived_count, false),
          ("completed", "Completed", completed_count, false),
        ] {
          {
            let chip_class = if is_primary {
              "state-chip state-chip-primary"
            } else {
              "state-chip"
            };
            rsx! {
              a { class: chip_class, href: "/ui/queues/{queue}/tasks?state={tab_state}",
                "{label} "
                span { class: "chip-count", "{count}" }
              }
            }
          }
        }
      }
    }
  };
  make_page(&format!("Queue: {queue}"), content)
}
