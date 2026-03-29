//! Shared helper utilities for rendering HTML pages and error responses.

use crate::ui::styles::STYLE;
use axum::response::Html;
use dioxus::prelude::Element;

/// Wrap a Dioxus-rendered body element in a full HTML document.
///
/// Dioxus SSR renders only the inner content; this function wraps it with a
/// proper `<!DOCTYPE html>` document, `<head>` (including the embedded
/// stylesheet), and `<body>` tags.
pub(crate) fn make_page(title: &str, body_element: Element) -> Html<String> {
  let body_html = dioxus_ssr::render_element(body_element);
  Html(format!(
    "<!DOCTYPE html><html lang=\"en\"><head>\
      <meta charset=\"utf-8\">\
      <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
      <title>{title} \u{2013} Asynq Dashboard</title>\
      <style>{STYLE}</style>\
    </head><body>{body_html}</body></html>",
    title = escape_html(title),
    STYLE = STYLE,
    body_html = body_html,
  ))
}

/// Escape HTML special characters to prevent injection.
pub(crate) fn escape_html(s: &str) -> String {
  s.replace('&', "&amp;")
    .replace('<', "&lt;")
    .replace('>', "&gt;")
    .replace('"', "&quot;")
    .replace('\'', "&#x27;")
}

/// Return an error HTML page when the inspector is unavailable.
pub(crate) fn inspector_unavailable() -> Html<String> {
  Html(
    "<!DOCTYPE html><html><body><h2>Inspector not configured \u{2013} UI unavailable</h2>\
     <p>Start the server with a Redis connection to enable the web UI.</p></body></html>"
      .to_string(),
  )
}

/// Return an error HTML page for a not-found or query error.
pub(crate) fn error_page(msg: &str) -> Html<String> {
  Html(format!(
    "<!DOCTYPE html><html><body><h2>Error</h2><p>{}</p>\
     <p><a href=\"/ui/\">Back to dashboard</a></p></body></html>",
    escape_html(msg)
  ))
}

/// Parse a task-state string into the corresponding [`asynq::base::keys::TaskState`].
pub(crate) fn parse_task_state(s: &str) -> asynq::base::keys::TaskState {
  use asynq::base::keys::TaskState;
  match s {
    "active" => TaskState::Active,
    "pending" => TaskState::Pending,
    "scheduled" => TaskState::Scheduled,
    "retry" => TaskState::Retry,
    "archived" => TaskState::Archived,
    "completed" => TaskState::Completed,
    "aggregating" => TaskState::Aggregating,
    _ => TaskState::Pending,
  }
}
