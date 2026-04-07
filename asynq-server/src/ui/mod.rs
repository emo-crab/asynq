//! Web UI module for Asynq Server
//!
//! This module implements a server-side rendered (SSR) web dashboard for
//! monitoring and managing Asynq task queues, inspired by asynqmon.
//!
//! The UI is built using Dioxus with SSR rendering, producing HTML on the
//! server side for each page request.  No JavaScript framework is required –
//! the pages are standard HTML with embedded styles.
//!
//! ## Module layout
//!
//! | Module | Contents |
//! |---|---|
//! | [`styles`] | Embedded CSS stylesheet and page-size constant |
//! | [`helpers`] | Page-wrapping, HTML escaping, and error-page utilities |
//! | [`components`] | Reusable Dioxus components (e.g. `NavBar`) |
//! | [`pages`] | Page-level handlers and renderers |
//! | [`actions`] | HTML-form POST action handlers |
//!
//! ## Pages
//!
//! - `GET /ui/` – Queue overview (all queues and their statistics)
//! - `GET /ui/servers` – Worker servers currently connected
//! - `GET /ui/cron` – Scheduled tasks (定时任务) across all queues
//! - `GET /ui/queues/:queue` – Queue detail page with task counts by state
//! - `GET /ui/queues/:queue/tasks` – Task list with state filter and pagination
//!
//! ## Management Actions (HTML form POST)
//!
//! - `POST /ui/queues/:queue/pause` – Pause a queue
//! - `POST /ui/queues/:queue/resume` – Resume a paused queue
//! - `POST /ui/queues/:queue/tasks/:id/delete` – Delete a specific task
//! - `POST /ui/queues/:queue/tasks/:id/run` – Run a task immediately
//! - `POST /ui/queues/:queue/tasks/:id/archive` – Archive a task
//! - `POST /ui/queues/:queue/tasks/bulk-delete` – Bulk delete tasks by state
//! - `POST /ui/queues/:queue/tasks/requeue` – Requeue tasks by state

pub(crate) mod actions;
pub(crate) mod components;
pub(crate) mod helpers;
pub(crate) mod pages;
pub(crate) mod styles;

// Re-export page handlers so that server.rs can reference them as `ui::*`
// without knowing the internal submodule structure.
pub use actions::{
  ui_archive_task, ui_bulk_delete, ui_delete_task, ui_pause_queue, ui_requeue_tasks,
  ui_resume_queue, ui_run_task, BulkStateQuery,
};
pub use pages::cron::cron_handler;
pub use pages::dashboard::dashboard_handler;
pub use pages::queue_detail::queue_detail_handler;
pub use pages::servers::servers_handler;
pub use pages::tasks::{tasks_handler, TasksQuery};
