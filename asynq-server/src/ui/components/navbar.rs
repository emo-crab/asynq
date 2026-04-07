//! Navigation bar component rendered at the top of every page.

use dioxus::prelude::*;

/// Sticky top navigation bar with the Asynq brand and primary links.
///
/// The `active` prop selects which tab is highlighted:
/// - `"queues"` – Queue overview (default / dashboard)
/// - `"servers"` – Worker servers page
/// - `"cron"` – Scheduled tasks (定时任务) page
#[component]
pub(crate) fn NavBar(active: String) -> Element {
  let queues_class = if active.as_str() == "queues" {
    "nav-link active"
  } else {
    "nav-link"
  };
  let servers_class = if active.as_str() == "servers" {
    "nav-link active"
  } else {
    "nav-link"
  };
  let cron_class = if active.as_str() == "cron" {
    "nav-link active"
  } else {
    "nav-link"
  };
  rsx! {
    nav {
      span { class: "brand",
        span { class: "brand-icon", "⚡" }
        "Asynq"
      }
      a { class: queues_class, href: "/ui/", "Queues" }
      a { class: servers_class, href: "/ui/servers", "Servers" }
      a { class: cron_class, href: "/ui/cron", "Scheduled Tasks" }
      a { class: "nav-link", href: "/swagger-ui/", "API Docs" }
    }
  }
}
