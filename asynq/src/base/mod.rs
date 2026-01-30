//! 经纪人模块
//! Broker module
//!
//! 定义了与 Redis 交互的抽象层
//! Defines the abstraction layer for interacting with Redis

use crate::error::Result;
use crate::proto::{ServerInfo, TaskMessage};
use crate::task::{Task, TaskInfo};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::str::FromStr;
use std::time::Duration;

pub mod constants;
pub mod keys;
/// 经纪人特性，定义了与任务存储后端交互的接口
/// Broker trait, defines the interface for interacting with the task storage backend
/// 对应 Go asynq 的 internal/base/base.go 中的 Broker interface
/// Corresponds to the Broker interface in Go asynq's internal/base/base.go
#[async_trait]
pub trait Broker: Send + Sync {
  // === 以下方法与 Go Broker 接口一一对应，按 Go 声明顺序排列 ===

  /// Ping Redis 连接 - Go: Ping
  /// Ping Redis connection - Go: Ping
  async fn ping(&self) -> Result<()>;

  /// 关闭连接 - Go: Close
  /// Close connection - Go: Close
  async fn close(&self) -> Result<()>;

  /// 将任务加入队列 - Go: Enqueue
  /// Enqueue a task - Go: Enqueue
  async fn enqueue(&self, task: &Task) -> Result<TaskInfo>;

  /// 将唯一任务加入队列 - Go: EnqueueUnique
  /// Enqueue a unique task - Go: EnqueueUnique
  async fn enqueue_unique(&self, task: &Task, ttl: Duration) -> Result<TaskInfo>;

  /// 从队列中出队任务 - Go: Dequeue
  /// Dequeue a task from the queue - Go: Dequeue
  async fn dequeue(&self, queues: &[String]) -> Result<Option<TaskMessage>>;

  /// 完成任务 - Go: Done
  /// Mark a task as done - Go: Done
  async fn done(&self, msg: &TaskMessage) -> Result<()>;

  /// 标记任务为完成状态 - Go: MarkAsComplete
  /// Mark a task as complete - Go: MarkAsComplete
  async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()>;

  /// 重新排队任务进行重试 - Go: Requeue
  /// Requeue a task for retry - Go: Requeue
  async fn requeue(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
  ) -> Result<()>;

  /// 调度任务在指定时间执行 - Go: Schedule
  /// Schedule a task to execute at a specific time - Go: Schedule
  async fn schedule(&self, task: &Task, process_at: DateTime<Utc>) -> Result<TaskInfo>;

  /// 调度唯一任务在指定时间执行 - Go: ScheduleUnique
  /// Schedule a unique task to execute at a specific time - Go: ScheduleUnique
  async fn schedule_unique(
    &self,
    task: &Task,
    process_at: DateTime<Utc>,
    ttl: Duration,
  ) -> Result<TaskInfo>;

  /// 重试失败的任务 - Go: Retry
  /// Retry a failed task - Go: Retry
  async fn retry(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
    is_failure: bool,
  ) -> Result<()>;

  /// 归档任务 - Go: Archive
  /// Archive a task - Go: Archive
  async fn archive(&self, msg: &TaskMessage, error_msg: &str) -> Result<()>;

  /// 将调度的任务转发到等待队列 - Go: ForwardIfReady
  /// Forward scheduled tasks to the waiting queue if ready - Go: ForwardIfReady
  async fn forward_if_ready(&self, queues: &[String]) -> Result<i64>;

  // Group aggregation related methods
  // 任务组聚合相关方法

  /// 将任务添加到组中进行聚合 - Go: AddToGroup
  /// Add a task to a group for aggregation - Go: AddToGroup
  async fn add_to_group(&self, task: &Task, group: &str) -> Result<TaskInfo>;

  /// 将唯一任务添加到组中进行聚合 - Go: AddToGroupUnique
  /// Add a unique task to a group for aggregation - Go: AddToGroupUnique
  async fn add_to_group_unique(&self, task: &Task, group: &str, ttl: Duration) -> Result<TaskInfo>;

  /// 获取队列中的任务组列表 - Go: ListGroups
  /// Get the list of task groups in a queue - Go: ListGroups
  async fn list_groups(&self, queue: &str) -> Result<Vec<String>>;

  /// 检查聚合条件是否满足 - Go: AggregationCheck
  /// Check if aggregation conditions are met - Go: AggregationCheck
  async fn aggregation_check(
    &self,
    queue: &str,
    group: &str,
    aggregation_delay: Duration,
    max_delay: Duration,
    max_size: usize,
  ) -> Result<Option<String>>;

  /// 读取聚合集合中的任务 - Go: ReadAggregationSet
  /// Read tasks from an aggregation set - Go: ReadAggregationSet
  async fn read_aggregation_set(
    &self,
    queue: &str,
    group: &str,
    set_id: &str,
  ) -> Result<Vec<TaskMessage>>;

  /// 删除聚合集合 - Go: DeleteAggregationSet
  /// Delete an aggregation set - Go: DeleteAggregationSet
  async fn delete_aggregation_set(&self, queue: &str, group: &str, set_id: &str) -> Result<()>;

  /// 回收过期的聚合集合 - Go: ReclaimStaleAggregationSets
  /// Reclaim expired aggregation sets - Go: ReclaimStaleAggregationSets
  async fn reclaim_stale_aggregation_sets(&self, queue: &str) -> Result<()>;

  // Task retention related method
  // 任务保留相关方法

  /// 删除过期的完成任务 - Go: DeleteExpiredCompletedTasks
  /// Delete expired completed tasks - Go: DeleteExpiredCompletedTasks
  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64>;

  // Lease related methods
  // 租约相关方法

  /// 列出租约已过期的任务 - Go: ListLeaseExpired
  /// List tasks with expired leases - Go: ListLeaseExpired
  async fn list_lease_expired(
    &self,
    cutoff: DateTime<Utc>,
    queues: &[String],
  ) -> Result<Vec<TaskMessage>>;

  /// 延长任务处理租约 - Go: ExtendLease
  /// Extend task lease - Go: ExtendLease
  async fn extend_lease(&self, queue: &str, task_id: &str, lease_duration: Duration) -> Result<()>;

  // State snapshot related methods
  // 状态快照相关方法

  /// 写入服务器状态 - Go: WriteServerState
  /// Write server state - Go: WriteServerState
  async fn write_server_state(&self, server_info: &ServerInfo, ttl: Duration) -> Result<()>;

  /// 清除服务器状态 - Go: ClearServerState
  /// Clear server state - Go: ClearServerState
  async fn clear_server_state(&self, host: &str, pid: i32, server_id: &str) -> Result<()>;

  // Cancelation related methods
  // 取消相关方法

  /// 订阅任务取消事件 - Go: CancelationPubSub
  /// Subscribe to task cancellation events - Go: CancelationPubSub
  async fn cancellation_pub_sub(
    &self,
  ) -> Result<Box<dyn futures::Stream<Item = Result<String>> + Unpin + Send>>;

  /// 发布任务取消通知 - Go: PublishCancelation
  /// Publish task cancellation notification - Go: PublishCancelation
  async fn publish_cancellation(&self, task_id: &str) -> Result<()>;

  /// 写入任务结果 - Go: WriteResult
  /// Write task result - Go: WriteResult
  async fn write_result(&self, queue: &str, task_id: &str, result: &[u8]) -> Result<()>;
}

/// 调度器相关功能的 Broker 特性扩展
/// Broker trait extension for scheduler-related functionality
///
/// 此特性定义了调度器（Scheduler）所需的持久化和查询接口
/// This trait defines the persistence and query interface required by the Scheduler
#[async_trait]
pub trait SchedulerBroker: Send + Sync {
  /// 批量写入 scheduler entries
  /// Batch write scheduler entries
  async fn write_scheduler_entries(
    &self,
    entries: &[crate::proto::SchedulerEntry],
    scheduler_id: &str,
    ttl_secs: u64,
  ) -> Result<()>;

  /// 记录调度事件
  /// Record scheduling event
  async fn record_scheduler_enqueue_event(
    &self,
    event: &crate::proto::SchedulerEnqueueEvent,
    entry_id: &str,
  ) -> Result<()>;

  /// 获取所有 SchedulerEntry
  /// Get all SchedulerEntry
  async fn scheduler_entries_script(
    &self,
    scheduler_id: &str,
  ) -> Result<std::collections::HashMap<String, Vec<u8>>>;

  /// 获取调度事件列表
  /// Get scheduling event list
  async fn scheduler_events_script(&self, count: usize) -> Result<Vec<Vec<u8>>>;

  /// 删除 scheduler entries 数据
  /// Delete scheduler entries data
  async fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()>;
}

/// 服务器状态
/// Server state
#[derive(Debug, Clone, PartialEq)]
pub enum ServerState {
  /// 新建状态
  /// New
  New,
  /// 活跃状态
  /// Active
  Active,
  /// 已停止状态
  /// Stopped
  Stopped,
  /// 已关闭状态
  /// Closed
  Closed,
}

impl ServerState {
  /// 转换为字符串
  /// Convert to string
  pub fn as_str(&self) -> &'static str {
    match self {
      Self::New => "new",
      Self::Active => "active",
      Self::Stopped => "stopped",
      Self::Closed => "closed",
    }
  }
}

impl FromStr for ServerState {
  type Err = ();

  fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
    match s {
      "new" => Ok(Self::New),
      "active" => Ok(Self::Active),
      "stopped" => Ok(Self::Stopped),
      "closed" => Ok(Self::Closed),
      _ => Err(()),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_server_state_conversion() {
    assert_eq!(ServerState::Active.as_str(), "active");
    assert_eq!("active".parse::<ServerState>(), Ok(ServerState::Active));
    assert!("invalid".parse::<ServerState>().is_err());
  }
}
