//! 检查器模块
//! Inspector module
//!
//! 提供队列和任务的检查和管理功能
//! Provides inspection and management functions for queues and tasks

use crate::base::keys::TaskState;
use crate::base::Broker;
use crate::error::Error;
use crate::error::Result;
use crate::proto::ServerInfo;
use crate::rdb::inspect::Pagination;
use crate::rdb::RedisBroker;
use crate::redis::RedisConfig;
use crate::task::{DailyStats, QueueInfo, QueueStats, TaskInfo};
use std::sync::Arc;

/// 队列检查器，用于检查和管理队列及任务
/// Queue inspector, used for inspecting and managing queues and tasks
pub struct Inspector {
  rdb: Arc<RedisBroker>,
}

impl Inspector {
  /// 通过 RedisConfig 创建（测试期望）
  /// Create via RedisConfig (for testing purposes)
  pub async fn new(redis_config: RedisConfig) -> Result<Self> {
    let mut broker = RedisBroker::new(redis_config)?;
    broker.init_scripts().await?;
    Ok(Self {
      rdb: Arc::new(broker),
    })
  }

  /// 兼容原 API：从已存在的 Broker
  /// Compatible with original API: from an existing Broker
  pub fn from_broker(broker: Arc<RedisBroker>) -> Self {
    Self { rdb: broker }
  }

  /// 获取队列统计信息
  /// Get queue statistics
  pub async fn get_queue_stats(&self, queue: &str) -> Result<QueueStats> {
    self.rdb.get_queue_stats(queue).await
  }

  /// 获取队列信息
  /// Get queue information
  pub async fn get_queue_info(&self, queue: &str) -> Result<QueueInfo> {
    self.rdb.get_queue_info(queue).await
  }

  /// 获取所有队列的统计信息
  /// Get statistics for all queues
  pub async fn get_all_queue_stats(&self) -> Result<Vec<QueueStats>> {
    self.rdb.get_all_queue_stats().await
  }

  /// 获取所有队列名称
  /// Get all queue names
  pub async fn get_queues(&self) -> Result<Vec<String>> {
    self.rdb.get_queues().await
  }

  /// 获取队列中的任务组列表
  /// Get the list of task groups in the queue
  pub async fn get_groups(&self, queue: &str) -> Result<Vec<String>> {
    self.rdb.list_groups(queue).await
  }

  /// 列出指定状态的任务
  /// List tasks with the specified state
  pub async fn list_tasks(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    self.rdb.list_tasks(queue, state, pagination).await
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

  /// 获取任务信息（若不存在返回 TaskNotFound）
  /// Get task information (return TaskNotFound if not exists)
  pub async fn get_task_info(&self, queue: &str, task_id: &str) -> Result<TaskInfo> {
    match self.rdb.get_task_info(queue, task_id).await? {
      Some(info) => Ok(info),
      None => Err(Error::TaskNotFound {
        id: task_id.to_string(),
      }),
    }
  }

  /// 删除任务
  /// Delete a task
  pub async fn delete_task(&self, queue: &str, task_id: &str) -> Result<()> {
    self.rdb.delete_task(queue, task_id).await
  }

  /// 删除所有归档任务
  /// Delete all archived tasks
  pub async fn delete_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_archived_tasks(queue).await
  }

  /// 删除所有重试任务
  /// Delete all retry tasks
  pub async fn delete_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_retry_tasks(queue).await
  }

  /// 删除所有调度任务
  /// Delete all scheduled tasks
  pub async fn delete_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_scheduled_tasks(queue).await
  }

  /// 删除所有等待任务
  /// Delete all pending tasks
  pub async fn delete_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_all_pending_tasks(queue).await
  }

  /// 重新排队所有归档任务
  /// Requeue all archived tasks
  pub async fn requeue_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_archived_tasks(queue).await
  }

  /// 重新排队所有重试任务
  /// Requeue all retry tasks
  pub async fn requeue_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_retry_tasks(queue).await
  }

  /// 重新排队所有调度任务
  /// Requeue all scheduled tasks
  pub async fn requeue_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.requeue_all_scheduled_tasks(queue).await
  }

  /// 运行所有归档任务 (Go: RunAllArchivedTasks - 等价于 requeue)
  /// Run all archived tasks (Go: RunAllArchivedTasks - equivalent to requeue)
  pub async fn run_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    self.requeue_all_archived_tasks(queue).await
  }

  /// 运行所有重试任务 (Go: RunAllRetryTasks - 等价于 requeue)
  /// Run all retry tasks (Go: RunAllRetryTasks - equivalent to requeue)
  pub async fn run_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.requeue_all_retry_tasks(queue).await
  }

  /// 运行所有已调度任务 (Go: RunAllScheduledTasks - 等价于 requeue)
  /// Run all scheduled tasks (Go: RunAllScheduledTasks - equivalent to requeue)
  pub async fn run_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.requeue_all_scheduled_tasks(queue).await
  }

  /// 运行单个任务 (Go: RunTask)
  /// Run a single task (Go: RunTask)
  pub async fn run_task(&self, queue: &str, task_id: &str) -> Result<()> {
    // 根据任务状态选择适当的重新排队方法
    // Select the appropriate requeue method based on the task state
    if let Ok(task_info) = self.get_task_info(queue, task_id).await {
      match task_info.state {
        TaskState::Archived | TaskState::Retry | TaskState::Scheduled => {
          self.rdb.run_task(queue, task_id).await
        }
        _ => Ok(()), // 其他状态不需要重新排队
                     // No need to requeue for other states
      }
    } else {
      Ok(()) // 任务不存在
             // Task does not exist
    }
  }

  /// 归档单个任务 (Go: ArchiveTask)
  /// Archive a single task (Go: ArchiveTask)
  pub async fn archive_task(&self, queue: &str, task_id: &str) -> Result<()> {
    self.rdb.archive_task(queue, task_id).await
  }

  /// 归档所有等待任务 (Go: ArchiveAllPendingTasks)  
  /// Archive all pending tasks (Go: ArchiveAllPendingTasks)
  pub async fn archive_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_pending_tasks(queue).await
  }

  /// 归档所有重试任务 (Go: ArchiveAllRetryTasks)
  /// Archive all retry tasks (Go: ArchiveAllRetryTasks)
  pub async fn archive_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_retry_tasks(queue).await
  }

  /// 归档所有已调度任务 (Go: ArchiveAllScheduledTasks)
  /// Archive all scheduled tasks (Go: ArchiveAllScheduledTasks)
  pub async fn archive_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_scheduled_tasks(queue).await
  }

  /// 归档所有聚合任务 (Go: ArchiveAllAggregatingTasks)
  /// Archive all aggregating tasks (Go: ArchiveAllAggregatingTasks)
  pub async fn archive_all_aggregating_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.archive_all_aggregating_tasks(queue).await
  }

  /// 暂停队列
  /// Pause a queue
  pub async fn pause_queue(&self, queue: &str) -> Result<()> {
    self.rdb.pause_queue(queue).await
  }

  /// 恢复队列
  /// Resume a queue
  pub async fn unpause_queue(&self, queue: &str) -> Result<()> {
    self.rdb.unpause_queue(queue).await
  }

  /// 检查队列是否暂停
  /// Check if a queue is paused
  pub async fn is_queue_paused(&self, queue: &str) -> Result<bool> {
    self.rdb.is_queue_paused(queue).await
  }

  /// 获取暂停的队列列表
  /// Get the list of paused queues
  pub async fn get_paused_queues(&self) -> Result<Vec<String>> {
    self.rdb.get_paused_queues().await
  }
  /// 获取任务结果
  /// Get the result of a task
  pub async fn get_task_result(&self, queue: &str, task_id: &str) -> Result<Option<Vec<u8>>> {
    self.rdb.get_result(queue, task_id).await
  }

  /// 删除过期的完成任务
  /// Delete expired completed tasks
  pub async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64> {
    self.rdb.delete_expired_completed_tasks(queue).await
  }

  /// 获取服务器列表
  /// Get the list of servers
  pub async fn get_servers(&self) -> Result<Vec<ServerInfo>> {
    self.rdb.get_servers().await
  }

  /// 获取服务器信息
  /// Get server information
  pub async fn get_server_info(&self, server_id: &str) -> Result<Option<ServerInfo>> {
    self.rdb.get_server_info(server_id).await
  }

  /// 获取历史统计信息
  /// Get historical statistics
  pub async fn get_history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>> {
    self.rdb.get_history(queue, days).await
  }

  /// 获取历史统计信息 (Go: History - 别名方法)
  /// Get historical statistics (Go: History - alias method)
  pub async fn history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>> {
    self.get_history(queue, days).await
  }

  /// 删除队列 (Go: DeleteQueue)
  /// Delete a queue (Go: DeleteQueue)
  pub async fn delete_queue(&self, queue: &str) -> Result<()> {
    // 删除队列需要删除所有相关的键
    // Deleting a queue requires deleting all related keys
    let _ = self.delete_all_pending_tasks(queue).await?;
    let _ = self.delete_all_active_tasks(queue).await?;
    let _ = self.delete_all_scheduled_tasks(queue).await?;
    let _ = self.delete_all_retry_tasks(queue).await?;
    let _ = self.delete_all_archived_tasks(queue).await?;
    let _ = self.delete_expired_completed_tasks(queue).await?;
    Ok(())
  }

  /// 删除所有活跃任务 (便利方法)
  /// Delete all active tasks (convenience method)
  pub async fn delete_all_active_tasks(&self, _queue: &str) -> Result<i64> {
    // 活跃任务通常不能直接删除，因为它们正在被处理
    // 这个操作可能需要特殊处理
    // Active tasks cannot be deleted directly as they are being processed
    // This operation may require special handling
    Err(Error::NotImplemented(
      "delete_all_active_tasks not implemented - active tasks are being processed".to_string(),
    ))
  }

  /// 取消处理中的任务 (Go: CancelProcessing)
  /// Cancel a processing task (Go: CancelProcessing)
  pub async fn cancel_processing(&self, task_id: &str) -> Result<()> {
    self.rdb.publish_cancellation(task_id).await
  }
}
