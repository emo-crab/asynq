//! 检查器模块
//! Inspector module
//!
//! 提供队列和任务的检查和管理功能
//! Provides inspection and management functions for queues and tasks

use crate::backend::pagination::Pagination;
use crate::base::keys::TaskState;
use crate::error::Result;
use crate::proto::ServerInfo;
use crate::task::{DailyStats, QueueInfo, QueueStats, TaskInfo};
use async_trait::async_trait;

/// 检查器特性，定义了队列和任务的检查和管理接口
/// Inspector trait, defines the interface for queue and task inspection and management
#[async_trait]
pub trait InspectorTrait: Send + Sync {
  /// 获取队列统计信息
  /// Get queue statistics
  async fn get_queue_stats(&self, queue: &str) -> Result<QueueStats>;

  /// 获取队列信息
  /// Get queue information
  async fn get_queue_info(&self, queue: &str) -> Result<QueueInfo>;

  /// 获取所有队列的统计信息
  /// Get statistics for all queues
  async fn get_all_queue_stats(&self) -> Result<Vec<QueueStats>>;

  /// 获取所有队列名称
  /// Get all queue names
  async fn get_queues(&self) -> Result<Vec<String>>;

  /// 获取队列中的任务组列表
  /// Get the list of task groups in the queue
  async fn get_groups(&self, queue: &str) -> Result<Vec<String>>;

  /// 列出指定状态的任务
  /// List tasks with the specified state
  async fn list_tasks(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>>;

  /// 获取任务信息
  /// Get task information
  async fn get_task_info(&self, queue: &str, task_id: &str) -> Result<TaskInfo>;

  /// 删除任务
  /// Delete a task
  async fn delete_task(&self, queue: &str, task_id: &str) -> Result<()>;

  /// 删除所有归档任务
  /// Delete all archived tasks
  async fn delete_all_archived_tasks(&self, queue: &str) -> Result<i64>;

  /// 删除所有重试任务
  /// Delete all retry tasks
  async fn delete_all_retry_tasks(&self, queue: &str) -> Result<i64>;

  /// 删除所有调度任务
  /// Delete all scheduled tasks
  async fn delete_all_scheduled_tasks(&self, queue: &str) -> Result<i64>;

  /// 删除所有等待任务
  /// Delete all pending tasks
  async fn delete_all_pending_tasks(&self, queue: &str) -> Result<i64>;

  /// 重新排队所有归档任务
  /// Requeue all archived tasks
  async fn requeue_all_archived_tasks(&self, queue: &str) -> Result<i64>;

  /// 重新排队所有重试任务
  /// Requeue all retry tasks
  async fn requeue_all_retry_tasks(&self, queue: &str) -> Result<i64>;

  /// 重新排队所有调度任务
  /// Requeue all scheduled tasks
  async fn requeue_all_scheduled_tasks(&self, queue: &str) -> Result<i64>;

  /// 运行单个任务
  /// Run a single task
  async fn run_task(&self, queue: &str, task_id: &str) -> Result<()>;

  /// 归档单个任务
  /// Archive a single task
  async fn archive_task(&self, queue: &str, task_id: &str) -> Result<()>;

  /// 归档所有等待任务
  /// Archive all pending tasks
  async fn archive_all_pending_tasks(&self, queue: &str) -> Result<i64>;

  /// 归档所有重试任务
  /// Archive all retry tasks
  async fn archive_all_retry_tasks(&self, queue: &str) -> Result<i64>;

  /// 归档所有已调度任务
  /// Archive all scheduled tasks
  async fn archive_all_scheduled_tasks(&self, queue: &str) -> Result<i64>;

  /// 归档所有聚合任务
  /// Archive all aggregating tasks
  async fn archive_all_aggregating_tasks(&self, queue: &str) -> Result<i64>;

  /// 暂停队列
  /// Pause a queue
  async fn pause_queue(&self, queue: &str) -> Result<()>;

  /// 恢复队列
  /// Resume a queue
  async fn unpause_queue(&self, queue: &str) -> Result<()>;

  /// 检查队列是否暂停
  /// Check if a queue is paused
  async fn is_queue_paused(&self, queue: &str) -> Result<bool>;

  /// 获取任务结果
  /// Get the result of a task
  async fn get_task_result(&self, queue: &str, task_id: &str) -> Result<Option<Vec<u8>>>;

  /// 删除过期的完成任务
  /// Delete expired completed tasks
  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64>;

  /// 获取服务器列表
  /// Get the list of servers
  async fn get_servers(&self) -> Result<Vec<ServerInfo>>;

  /// 获取服务器信息
  /// Get server information
  async fn get_server_info(&self, server_id: &str) -> Result<Option<ServerInfo>>;

  /// 获取历史统计信息
  /// Get historical statistics
  async fn get_history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>>;

  /// 取消处理中的任务
  /// Cancel a processing task
  async fn cancel_processing(&self, task_id: &str) -> Result<()>;
}

// Re-export RedisInspector from backend module (always available as the default backend)
// 从 backend 模块重新导出 RedisInspector（始终可用作为默认后端）
pub use crate::backend::RedisInspector;

// Re-export PostgresInspector when postgres feature is enabled
// 当启用 postgres 特性时重新导出 PostgresInspector
#[cfg(feature = "postgres")]
pub use crate::backend::PostgresInspector;

// Re-export WebSocketInspector when websocket feature is enabled
// 当启用 websocket 特性时重新导出 WebSocketInspector
#[cfg(feature = "websocket")]
pub use crate::backend::WebSocketInspector;

/// 默认的 Inspector 类型别名
/// Default Inspector type alias
///
/// 始终使用 Redis 后端作为默认实现。
/// Always uses Redis backend as the default implementation.
///
/// 如需使用其他后端，请直接使用具体类型：
/// To use other backends, use the specific types directly:
///
/// - PostgreSQL: `PostgresInspector` (需要 `postgres` feature)
/// - WebSocket: `WebSocketInspector` (需要 `websocket` feature)
pub use crate::backend::Inspector;
