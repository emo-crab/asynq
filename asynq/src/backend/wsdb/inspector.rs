//! WebSocket Inspector stub
//!
//! A minimal inspector implementation for WebSocket broker.
//! Most operations are not supported as they should be handled by the asynq-server directly.

use crate::backend::pagination::Pagination;
use crate::base::keys::TaskState;
use crate::error::{Error, Result};
use crate::inspector::InspectorTrait;
use crate::task::{DailyStats, QueueInfo, QueueStats, TaskInfo};
use async_trait::async_trait;

/// Error message for unsupported inspector operations
const UNSUPPORTED_MSG: &str =
  "Inspector operations not supported via WebSocket client. Use the asynq-server's API directly.";

/// WebSocket Inspector
///
/// A stub inspector for WebSocket backend. Most operations are not supported
/// because the asynq-server handles queue inspection directly.
pub struct WebSocketInspector;

impl WebSocketInspector {
  /// Create a new WebSocket inspector
  pub fn new() -> Self {
    Self
  }
}

impl Default for WebSocketInspector {
  fn default() -> Self {
    Self::new()
  }
}

#[async_trait]
impl InspectorTrait for WebSocketInspector {
  async fn get_queue_stats(&self, _queue: &str) -> Result<QueueStats> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_queue_info(&self, _queue: &str) -> Result<QueueInfo> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_all_queue_stats(&self) -> Result<Vec<QueueStats>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_queues(&self) -> Result<Vec<String>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_groups(&self, _queue: &str) -> Result<Vec<String>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn list_tasks(
    &self,
    _queue: &str,
    _state: TaskState,
    _pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_task_info(&self, _queue: &str, _task_id: &str) -> Result<TaskInfo> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn delete_task(&self, _queue: &str, _task_id: &str) -> Result<()> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn delete_all_archived_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn delete_all_retry_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn delete_all_scheduled_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn delete_all_pending_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn requeue_all_archived_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn requeue_all_retry_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn requeue_all_scheduled_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn run_task(&self, _queue: &str, _task_id: &str) -> Result<()> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn archive_task(&self, _queue: &str, _task_id: &str) -> Result<()> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn archive_all_pending_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn archive_all_retry_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn archive_all_scheduled_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn archive_all_aggregating_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn pause_queue(&self, _queue: &str) -> Result<()> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn unpause_queue(&self, _queue: &str) -> Result<()> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn is_queue_paused(&self, _queue: &str) -> Result<bool> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_task_result(&self, _queue: &str, _task_id: &str) -> Result<Option<Vec<u8>>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn delete_expired_completed_tasks(&self, _queue: &str) -> Result<i64> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_servers(&self) -> Result<Vec<crate::proto::ServerInfo>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_server_info(&self, _server_id: &str) -> Result<Option<crate::proto::ServerInfo>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn get_history(&self, _queue: &str, _days: i32) -> Result<Vec<DailyStats>> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }

  async fn cancel_processing(&self, _task_id: &str) -> Result<()> {
    Err(Error::not_supported(UNSUPPORTED_MSG))
  }
}
