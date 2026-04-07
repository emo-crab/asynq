//! Servers page – shows all connected worker servers.

use crate::server::AppState;
use asynq::proto::ServerInfo;
use axum::{
  extract::State,
  response::{Html, IntoResponse},
};
use dioxus::prelude::*;
use std::sync::Arc;

use crate::ui::components::NavBar;
use crate::ui::helpers::{inspector_unavailable, make_page};

/// `GET /ui/servers` – Fetch server list and render the servers page.
pub async fn servers_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
  let Some(ref inspector) = state.inspector else {
    return inspector_unavailable();
  };

  let servers = inspector.get_servers().await.unwrap_or_default();
  render_servers(servers)
}

struct ServerRow {
  host: String,
  pid: i32,
  server_id: String,
  status: String,
  concurrency: i32,
  active_workers: i32,
  queues: String,
  started_at: String,
}

fn render_servers(servers: Vec<ServerInfo>) -> Html<String> {
  let count = servers.len();

  let rows: Vec<ServerRow> = servers
    .iter()
    .map(|s| {
      let mut queue_list: Vec<String> = s.queues.iter().map(|(q, p)| format!("{q}:{p}")).collect();
      queue_list.sort();
      let queues = queue_list.join(", ");
      let started_at = s
        .start_time
        .as_ref()
        .map(|ts| {
          use std::time::{Duration, UNIX_EPOCH};
          let d = UNIX_EPOCH + Duration::from_secs(ts.seconds.max(0) as u64);
          let dt: chrono::DateTime<chrono::Utc> = d.into();
          dt.format("%Y-%m-%d %H:%M UTC").to_string()
        })
        .unwrap_or_default();
      ServerRow {
        host: s.host.clone(),
        pid: s.pid,
        server_id: s.server_id.clone(),
        status: s.status.clone(),
        concurrency: s.concurrency,
        active_workers: s.active_worker_count,
        queues,
        started_at,
      }
    })
    .collect();

  let content = rsx! {
    NavBar { active: "servers" }
    div { class: "container",
      div { class: "page-header",
        h1 { "Servers" }
        span { class: "text-muted", "{count} server(s)" }
      }

      div { class: "card",
        if rows.is_empty() {
          div { class: "empty",
            div { class: "empty-icon", "🖥" }
            p { "No active servers found. Start an Asynq worker server to see it here." }
          }
        } else {
          div { class: "table-wrap",
            table {
              thead {
                tr {
                  th { "Host" }
                  th { "PID" }
                  th { "Server ID" }
                  th { "Status" }
                  th { "Concurrency" }
                  th { "Active Workers" }
                  th { "Queues" }
                  th { "Started At" }
                }
              }
              tbody {
                for row in &rows {
                  {
                    let status_class = match row.status.as_str() {
                      "active" => "badge badge-running",
                      _ => "badge badge-paused",
                    };
                    let status = row.status.clone();
                    rsx! {
                      tr {
                        td { "{row.host}" }
                        td { class: "text-muted", "{row.pid}" }
                        td { class: "task-id", title: "{row.server_id}", "{row.server_id}" }
                        td { span { class: status_class, "{status}" } }
                        td { "{row.concurrency}" }
                        td { class: "c-active", "{row.active_workers}" }
                        td { class: "text-muted", "{row.queues}" }
                        td { class: "text-muted", "{row.started_at}" }
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
  make_page("Servers", content)
}
