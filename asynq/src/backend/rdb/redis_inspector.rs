//! Redis Inspector 实现模块
//! Redis Inspector implementation module
//!
//! 提供基于 Redis 的队列和任务检查和管理功能
//! Provides Redis-based queue and task inspection and management functions

use crate::backend::pagination::Pagination;
use crate::backend::rdb::redis::RedisConnectionType;
use crate::backend::rdb::RedisBroker;
use crate::base::keys::TaskState;
use crate::base::Broker;
use crate::error::Result;
use crate::proto::ServerInfo;
use crate::task::{DailyStats, TaskInfo};
use std::sync::Arc;

/// Redis 队列检查器实现
/// Redis queue inspector implementation
pub struct RedisInspector {
  rdb: Arc<RedisBroker>,
}

impl RedisInspector {
  /// 通过 RedisConnectionType 创建 Inspector
  /// Create via RedisConnectionType Inspector
  pub async fn new(redis_connection_config: RedisConnectionType) -> Result<Self> {
    let broker = RedisBroker::new(redis_connection_config).await?;
    Ok(Self {
      rdb: Arc::new(broker),
    })
  }

  /// 从已存在的 RedisBroker 创建
  /// Create from an existing RedisBroker
  pub fn from_broker(broker: Arc<RedisBroker>) -> Self {
    Self { rdb: broker }
  }

  /// 列出活跃任务 (Go: ListActiveTasks)
  /// List active tasks (Go: ListActiveTasks)
  pub async fn list_active_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Active, Pagination::default())
      .await
  }

  /// 列出等待中任务 (Go: ListPendingTasks)
  /// List pending tasks (Go: ListPendingTasks)
  pub async fn list_pending_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Pending, Pagination::default())
      .await
  }

  /// 分页列出等待中任务
  /// List pending tasks with pagination
  pub async fn list_pending_tasks_with_pagination(
    &self,
    queue: &str,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    if pagination.page < 1 || pagination.size <= 0 {
      return Ok(vec![]);
    }
    self
      .rdb
      .list_tasks(queue, TaskState::Pending, pagination)
      .await
  }

  /// 列出已调度任务 (Go: ListScheduledTasks)
  /// List scheduled tasks (Go: ListScheduledTasks)
  pub async fn list_scheduled_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Scheduled, Pagination::default())
      .await
  }

  /// 列出重试任务 (Go: ListRetryTasks)
  /// List retry tasks (Go: ListRetryTasks)
  pub async fn list_retry_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Retry, Pagination::default())
      .await
  }

  /// 列出已归档任务 (Go: ListArchivedTasks)
  /// List archived tasks (Go: ListArchivedTasks)
  pub async fn list_archived_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Archived, Pagination::default())
      .await
  }

  /// 列出已完成任务 (Go: ListCompletedTasks)
  /// List completed tasks (Go: ListCompletedTasks)
  pub async fn list_completed_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Completed, Pagination::default())
      .await
  }

  /// 列出聚合中任务 (Go: ListAggregatingTasks)
  /// List aggregating tasks (Go: ListAggregatingTasks)
  pub async fn list_aggregating_tasks(&self, queue: &str) -> Result<Vec<TaskInfo>> {
    self
      .rdb
      .list_tasks(queue, TaskState::Aggregating, Pagination::default())
      .await
  }

  /// 运行所有归档任务 (Go: RunAllArchivedTasks - 等价于 requeue)
  /// Run all archived tasks (Go: RunAllArchivedTasks - equivalent to requeue)
  pub async fn run_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_archived_tasks(queue).await
  }

  /// 运行所有重试任务 (Go: RunAllRetryTasks - 等价于 requeue)
  /// Run all retry tasks (Go: RunAllRetryTasks - equivalent to requeue)
  pub async fn run_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_retry_tasks(queue).await
  }

  /// 运行所有已调度任务 (Go: RunAllScheduledTasks - 等价于 requeue)
  /// Run all scheduled tasks (Go: RunAllScheduledTasks - equivalent to requeue)
  pub async fn run_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_scheduled_tasks(queue).await
  }

  /// 获取历史统计信息 (Go: History - 别名方法)
  /// Get historical statistics (Go: History - alias method)
  pub async fn history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>> {
    self.rdb.get_history(queue, days).await
  }

  /// 删除队列 (Go: DeleteQueue)
  /// Delete a queue (Go: DeleteQueue)
  pub async fn delete_queue(&self, queue: &str) -> Result<()> {
    // 删除队列需要删除所有相关的键
    // Deleting a queue requires deleting all related keys
    let _ = self.rdb.delete_all_pending_tasks(queue).await?;
    // 忽略删除活跃任务的错误，因为它们不能被直接删除
    // Ignore errors from deleting active tasks since they can't be deleted directly
    let _ = self.delete_all_active_tasks(queue).await;
    let _ = self.rdb.delete_all_scheduled_tasks(queue).await?;
    let _ = self.rdb.delete_all_retry_tasks(queue).await?;
    let _ = self.rdb.delete_all_archived_tasks(queue).await?;
    let _ = self.rdb.delete_expired_completed_tasks(queue).await?;
    Ok(())
  }

  /// 删除所有活跃任务 (便利方法)
  /// Delete all active tasks (convenience method)
  pub async fn delete_all_active_tasks(&self, _queue: &str) -> Result<i64> {
    // 活跃任务通常不能直接删除，因为它们正在被处理
    // 这个操作可能需要特殊处理
    // Active tasks cannot be deleted directly as they are being processed
    // This operation may require special handling
    Err(crate::error::Error::NotImplemented(
      "delete_all_active_tasks not implemented - active tasks are being processed".to_string(),
    ))
  }
}

// Implement InspectorTrait for RedisInspector
// 为 RedisInspector 实现 InspectorTrait
#[async_trait::async_trait]
impl crate::inspector::InspectorTrait for RedisInspector {
  async fn get_queue_stats(&self, queue: &str) -> Result<crate::task::QueueStats> {
    self.rdb.get_queue_stats(queue).await
  }

  async fn get_queue_info(&self, queue: &str) -> Result<crate::task::QueueInfo> {
    self.rdb.get_queue_info(queue).await
  }

  async fn get_all_queue_stats(&self) -> Result<Vec<crate::task::QueueStats>> {
    self.rdb.get_all_queue_stats().await
  }

  async fn get_queues(&self) -> Result<Vec<String>> {
    self.rdb.get_queues().await
  }

  async fn get_groups(&self, queue: &str) -> Result<Vec<String>> {
    self.rdb.list_groups(queue).await
  }

  async fn list_tasks(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<crate::task::TaskInfo>> {
    self.rdb.list_tasks(queue, state, pagination).await
  }

  async fn get_task_info(&self, queue: &str, task_id: &str) -> Result<crate::task::TaskInfo> {
    self.rdb.get_task_info(queue, task_id).await
  }

  async fn delete_task(&self, queue: &str, task_id: &str) -> Result<()> {
    self.rdb.delete_task(queue, task_id).await
  }

  async fn delete_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_archived_tasks(queue).await
  }

  async fn delete_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_retry_tasks(queue).await
  }

  async fn delete_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_scheduled_tasks(queue).await
  }

  async fn delete_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_pending_tasks(queue).await
  }

  async fn requeue_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_archived_tasks(queue).await
  }

  async fn requeue_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_retry_tasks(queue).await
  }

  async fn requeue_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_scheduled_tasks(queue).await
  }

  async fn run_task(&self, queue: &str, task_id: &str) -> Result<()> {
    self.rdb.run_task(queue, task_id).await
  }

  async fn archive_task(&self, queue: &str, task_id: &str) -> Result<()> {
    self.rdb.archive_task(queue, task_id).await
  }

  async fn archive_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_pending_tasks(queue).await
  }

  async fn archive_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_retry_tasks(queue).await
  }

  async fn archive_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_scheduled_tasks(queue).await
  }

  async fn archive_all_aggregating_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_aggregating_tasks(queue).await
  }

  async fn pause_queue(&self, queue: &str) -> Result<()> {
    self.rdb.pause_queue(queue).await
  }

  async fn unpause_queue(&self, queue: &str) -> Result<()> {
    self.rdb.unpause_queue(queue).await
  }

  async fn is_queue_paused(&self, queue: &str) -> Result<bool> {
    self.rdb.is_queue_paused(queue).await
  }

  async fn get_task_result(&self, queue: &str, task_id: &str) -> Result<Option<Vec<u8>>> {
    self.rdb.get_result(queue, task_id).await
  }

  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_expired_completed_tasks(queue).await
  }

  async fn get_servers(&self) -> Result<Vec<ServerInfo>> {
    self.rdb.get_servers().await
  }

  async fn get_server_info(&self, server_id: &str) -> Result<Option<ServerInfo>> {
    self.rdb.get_server_info(server_id).await
  }

  async fn get_history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>> {
    self.rdb.get_history(queue, days).await
  }

  async fn cancel_processing(&self, task_id: &str) -> Result<()> {
    self.rdb.publish_cancellation(task_id).await
  }
}
