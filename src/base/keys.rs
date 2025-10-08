//! Redis 键名常量 - 与 Go 版本保持兼容
//! Redis key name constants - Compatible with Go version

use crate::base::constants::TIME_LAYOUT_YMD;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// 全局 Redis 键
/// Global Redis keys
pub const ALL_SERVERS: &str = "asynq:servers";
pub const ALL_WORKERS: &str = "asynq:workers";
pub const ALL_SCHEDULERS: &str = "asynq:schedulers";
pub const ALL_QUEUES: &str = "asynq:queues";
pub const CANCEL_CHANNEL: &str = "asynq:cancel";
pub const SCHEDULER_EVENTS: &str = "asynq:scheduler:events";

// 为了向后兼容，保留旧常量
// For backward compatibility, keep old constants
pub const QUEUE_PREFIX: &str = "asynq:{";
pub const ACTIVE_PREFIX: &str = "asynq:active:";
pub const SCHEDULED_PREFIX: &str = "asynq:scheduled:";
pub const RETRY_PREFIX: &str = "asynq:retry:";
pub const ARCHIVED_PREFIX: &str = "asynq:archived:";
pub const COMPLETED_PREFIX: &str = "asynq:completed:";
pub const AGGREGATING_PREFIX: &str = "asynq:aggregating:";
pub const SERVERS_PREFIX: &str = "asynq:servers:";
pub const WORKERS_PREFIX: &str = "asynq:workers:";
pub const PAUSED_QUEUES: &str = "asynq:paused";
pub const TASK_RESULT_PREFIX: &str = "asynq:result:";
pub const UNIQUE_PREFIX: &str = "asynq:unique:";
/// 任务状态
/// Task state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskState {
  /// 任务正在被处理
  /// Task is being processed
  Active,
  /// 任务准备好被处理
  /// Task is ready to be processed
  Pending,
  /// 任务被安排在将来某个时间处理
  /// Task is scheduled to be processed at a later time
  Scheduled,
  /// 任务之前失败了，安排在将来某个时间重试
  /// Task has failed before, scheduled to retry at a later time
  Retry,
  /// 任务被归档并存储以供检查
  /// Task is archived and stored for inspection
  Archived,
  /// 任务处理成功并保留到保留 TTL 过期
  /// Task is successfully processed and retained until retention TTL expires
  Completed,
  /// 任务在组中等待聚合
  /// Task is waiting for aggregation in the group
  Aggregating,
}

impl TaskState {
  /// 将任务状态转换为字符串
  /// Convert task state to string
  pub fn as_str(&self) -> &'static str {
    match self {
      Self::Active => "active",
      Self::Pending => "pending",
      Self::Scheduled => "scheduled",
      Self::Retry => "retry",
      Self::Archived => "archived",
      Self::Completed => "completed",
      Self::Aggregating => "aggregating",
    }
  }
  pub fn queue_key(&self, qname: &str, gname: Option<&str>) -> String {
    match self {
      Self::Active => active_key(qname),
      Self::Pending => pending_key(qname),
      Self::Scheduled => scheduled_key(qname),
      Self::Retry => retry_key(qname),
      Self::Archived => archived_key(qname),
      Self::Completed => completed_key(qname),
      Self::Aggregating => aggregating_key(qname, gname.unwrap_or("")), // 需要提供组名
    }
  }
}

impl FromStr for TaskState {
  type Err = ();

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "active" => Ok(Self::Active),
      "pending" => Ok(Self::Pending),
      "scheduled" => Ok(Self::Scheduled),
      "retry" => Ok(Self::Retry),
      "archived" => Ok(Self::Archived),
      "completed" => Ok(Self::Completed),
      "aggregating" => Ok(Self::Aggregating),
      _ => Err(()),
    }
  }
}

impl std::fmt::Display for TaskState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

/// 生成队列键前缀 - 与 Go 版本兼容: asynq:{qname}:
/// Generate queue key prefix - Compatible with Go version: asynq:{qname}:
pub fn queue_key_prefix(qname: &str) -> String {
  format!("asynq:{{{}}}:", qname)
}

/// 生成任务键前缀
/// Generate task key prefix
pub fn task_key_prefix(qname: &str) -> String {
  format!("{}t:", queue_key_prefix(qname))
}

/// 生成任务键
/// Generate task key
pub fn task_key(qname: &str, id: &str) -> String {
  format!("{}{}", task_key_prefix(qname), id)
}

/// 生成队列键 - 对应 Go 的 PendingKey
/// Generate queue key - Corresponds to Go's PendingKey
pub fn pending_key(qname: &str) -> String {
  format!("{}{}", queue_key_prefix(qname), TaskState::Pending)
}

/// 生成活跃任务键 - 对应 Go 的 ActiveKey  
/// Generate active task key - Corresponds to Go's ActiveKey
pub fn active_key(qname: &str) -> String {
  format!("{}{}", queue_key_prefix(qname), TaskState::Active)
}

/// 生成调度任务键 - 对应 Go 的 ScheduledKey
/// Generate scheduled task key - Corresponds to Go's ScheduledKey
pub fn scheduled_key(qname: &str) -> String {
  format!("{}{}", queue_key_prefix(qname), TaskState::Scheduled)
}

/// 生成重试任务键 - 对应 Go 的 RetryKey
/// Generate retry task key - Corresponds to Go's RetryKey
pub fn retry_key(qname: &str) -> String {
  format!("{}{}", queue_key_prefix(qname), TaskState::Retry)
}

/// 生成已归档任务键 - 对应 Go 的 ArchivedKey
/// Generate archived task key - Corresponds to Go's ArchivedKey
pub fn archived_key(qname: &str) -> String {
  format!("{}{}", queue_key_prefix(qname), TaskState::Archived)
}

/// 生成已完成任务键 - 对应 Go 的 CompletedKey
/// Generate completed task key - Corresponds to Go's CompletedKey
pub fn completed_key(qname: &str) -> String {
  format!("{}{}", queue_key_prefix(qname), TaskState::Completed)
}

/// 生成暂停键 - 对应 Go 的 PausedKey
/// Generate paused key - Corresponds to Go's PausedKey
pub fn paused_key(qname: &str) -> String {
  format!("{}paused", queue_key_prefix(qname))
}

/// 生成租约键 - 对应 Go 的 LeaseKey
/// Generate lease key - Corresponds to Go's LeaseKey
pub fn lease_key(qname: &str) -> String {
  format!("{}lease", queue_key_prefix(qname))
}

/// 生成聚合任务键（向后兼容） - 映射到组键
/// Generate aggregating task key (backward compatible) - Mapped to group key
pub fn aggregating_key(queue: &str, group: &str) -> String {
  group_key(queue, group)
}

/// 生成唯一键 - 对应 Go 的 UniqueKey，使用 MD5 校验和
/// Generate unique key - Corresponds to Go's UniqueKey, using MD5 checksum
pub fn unique_key(qname: &str, task_type: &str, payload: &[u8]) -> String {
  if payload.is_empty() {
    return format!("{}unique:{}:", queue_key_prefix(qname), task_type);
  }

  // 使用 MD5 哈希与 Go 版本保持兼容
  // Use MD5 hash to保持兼容 with Go version
  let digest = md5::compute(payload);
  let checksum = format!("{:x}", digest);

  format!(
    "{}unique:{}:{}",
    queue_key_prefix(qname),
    task_type,
    checksum
  )
}

/// 生成组键前缀
/// Generate group key prefix
pub fn group_key_prefix(qname: &str) -> String {
  format!("{}g:", queue_key_prefix(qname))
}

/// 生成组键 - 对应 Go 的 GroupKey
/// Generate group key - Corresponds to Go's GroupKey
pub fn group_key(qname: &str, group_key: &str) -> String {
  format!("{}{}", group_key_prefix(qname), group_key)
}

/// 生成聚合集合键 - 对应 Go 的 AggregationSetKey
/// Generate aggregation set key - Corresponds to Go's AggregationSetKey
pub fn aggregation_set_key(qname: &str, group_name: &str, set_id: &str) -> String {
  format!("{}:{}", group_key(qname, group_name), set_id)
}

/// 生成所有组键 - 对应 Go 的 AllGroups
/// Generate all group keys - Corresponds to Go's AllGroups
pub fn all_groups(qname: &str) -> String {
  format!("{}groups", queue_key_prefix(qname))
}

/// 生成组键（别名 all_groups）
/// Generate groups key (alias for all_groups)
pub fn groups_key(qname: &str) -> String {
  all_groups(qname)
}

/// 生成所有聚合集合键 - 对应 Go 的 AllAggregationSets
/// Generate all aggregation set keys - Corresponds to Go's AllAggregationSets
pub fn all_aggregation_sets(qname: &str) -> String {
  format!("{}aggregation_sets", queue_key_prefix(qname))
}

/// 生成服务器信息键 - 对应 Go 的 ServerInfoKey
/// Generate server info key - Corresponds to Go's ServerInfoKey
pub fn server_info_key(hostname: &str, pid: i32, server_id: &str) -> String {
  format!("{}{{{}:{}:{}}}", SERVERS_PREFIX, hostname, pid, server_id)
}

/// 生成工作者键 - 对应 Go 的 WorkersKey
/// Generate workers key - Corresponds to Go's WorkersKey
pub fn workers_key(hostname: &str, pid: i32, server_id: &str) -> String {
  format!("{}{{{}:{}:{}}}", WORKERS_PREFIX, hostname, pid, server_id)
}

/// 生成调度器条目键 - 对应 Go 的 SchedulerEntriesKey
/// Generate scheduler entries key - Corresponds to Go's SchedulerEntriesKey
pub fn scheduler_entries_key(scheduler_id: &str) -> String {
  format!("{}:{{{}}}", ALL_SCHEDULERS, scheduler_id)
}

/// 生成调度器历史键 - 对应 Go 的 SchedulerHistoryKey
/// Generate scheduler history key - Corresponds to Go's SchedulerHistoryKey
pub fn scheduler_history_key(entry_id: &str) -> String {
  format!("asynq:scheduler_history:{}", entry_id)
}

/// 生成处理总数键 - 对应 Go 的 ProcessedTotalKey
/// Generate processed total key - Corresponds to Go's ProcessedTotalKey
pub fn processed_total_key(qname: &str) -> String {
  format!("{}processed", queue_key_prefix(qname))
}

/// 生成失败总数键 - 对应 Go 的 FailedTotalKey  
/// Generate failed total key - Corresponds to Go's FailedTotalKey
pub fn failed_total_key(qname: &str) -> String {
  format!("{}failed", queue_key_prefix(qname))
}

/// 生成按日处理数键 - 对应 Go 的 ProcessedKey
/// Generate daily processed key - Corresponds to Go's ProcessedKey
pub fn processed_key(qname: &str, date: &DateTime<Utc>) -> String {
  format!(
    "{}processed:{}",
    queue_key_prefix(qname),
    date.format(TIME_LAYOUT_YMD)
  )
}

/// 生成按日失败数键 - 对应 Go 的 FailedKey
/// Generate daily failed key - Corresponds to Go's FailedKey
pub fn failed_key(qname: &str, date: &DateTime<Utc>) -> String {
  format!(
    "{}failed:{}",
    queue_key_prefix(qname),
    date.format(TIME_LAYOUT_YMD)
  )
}

// 保留这些函数以便向后兼容
// Keep these functions for backward compatibility
pub fn server_info_key_legacy(server_id: &str) -> String {
  format!("{}{}", SERVERS_PREFIX, server_id)
}

/// 完整的服务器信息键生成函数 - 对应 Go 的 ServerInfoKey
/// Full server info key generation function - Corresponds to Go's ServerInfoKey
pub fn server_info_key_full(hostname: &str, pid: i32, server_id: &str) -> String {
  format!("{}{{{}:{}:{}}}", SERVERS_PREFIX, hostname, pid, server_id)
}

/// 完整的工作者键生成函数 - 对应 Go 的 WorkersKey
/// Full workers key generation function - Corresponds to Go's WorkersKey
pub fn workers_key_full(hostname: &str, pid: i32, server_id: &str) -> String {
  format!("{}{{{}:{}:{}}}", WORKERS_PREFIX, hostname, pid, server_id)
}

#[cfg(test)]
mod tests {
  use crate::base::keys;

  #[test]
  fn test_keys_generation() {
    // 测试与 Go 版本兼容的键生成
    // Test key generation compatible with Go version
    assert_eq!(keys::pending_key("default"), "asynq:{default}:pending");
    assert_eq!(keys::pending_key("default"), "asynq:{default}:pending"); // 别名
    assert_eq!(keys::active_key("default"), "asynq:{default}:active");
    assert_eq!(keys::scheduled_key("default"), "asynq:{default}:scheduled");
    assert_eq!(keys::retry_key("default"), "asynq:{default}:retry");
    assert_eq!(keys::archived_key("default"), "asynq:{default}:archived");
    assert_eq!(keys::completed_key("default"), "asynq:{default}:completed");

    // 测试全局键
    // Test global keys
    assert_eq!(keys::ALL_SERVERS, "asynq:servers");
    assert_eq!(keys::ALL_WORKERS, "asynq:workers");
    assert_eq!(keys::ALL_QUEUES, "asynq:queues");

    // 测试服务器和工作者键（新格式）
    // Test server and worker keys (new format)
    assert_eq!(
      keys::server_info_key("localhost", 12345, "server1"),
      "asynq:servers:{localhost:12345:server1}"
    );
    assert_eq!(
      keys::workers_key("localhost", 12345, "server1"),
      "asynq:workers:{localhost:12345:server1}"
    );

    // 测试任务和组相关键
    // Test task and group related keys
    assert_eq!(
      keys::task_key("default", "task1"),
      "asynq:{default}:t:task1"
    );
    assert_eq!(
      keys::group_key("default", "group1"),
      "asynq:{default}:g:group1"
    );

    // 测试唯一键
    // Test unique key
    let unique_key = keys::unique_key("default", "email:send", b"test payload");
    assert!(unique_key.starts_with("asynq:{default}:unique:email:send:"));

    let empty_unique_key = keys::unique_key("default", "email:send", b"");
    assert_eq!(empty_unique_key, "asynq:{default}:unique:email:send:");
    assert_eq!(
      keys::task_key("default", "task1"),
      "asynq:{default}:t:task1"
    );
  }
}
