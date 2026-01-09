use crate::base::keys::TaskState;
use crate::base::{keys, Broker};
use crate::error::{Error, Result};
use crate::proto::{ServerInfo, TaskMessage};
use crate::rdb::redis_scripts::RedisArg;
use crate::rdb::RedisBroker;
use crate::task::{DailyStats, QueueInfo, QueueStats, Task, TaskInfo};
use chrono::{TimeZone, Utc};
use prost::Message;
use redis::AsyncCommands;
use std::time::Duration;

/// Pagination specifies the page size and page number for list operations.
/// 分页结构体，指定列表操作的页面大小和页面编号
#[derive(Debug, Clone, Copy)]
pub struct Pagination {
  /// Number of items in the page.
  /// 每页的项目数
  pub size: i64,
  /// Page number starting from zero.
  /// 从零开始的页面编号
  pub page: i64,
}

impl Pagination {
  /// Returns the start index for the current page.
  /// 返回当前页面的起始索引
  pub fn start(&self) -> i64 {
    self.size * self.page
  }
  /// Returns the stop index for the current page.
  /// 返回当前页面的结束索引
  pub fn stop(&self) -> i64 {
    self.size * self.page + self.size - 1
  }
}
impl Default for Pagination {
  fn default() -> Self {
    Pagination { size: 20, page: 0 }
  }
}
impl RedisBroker {
  /// 获取所有队列名称
  /// Get all queue names
  pub async fn all_queues(&self) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;
    let queue_names: Vec<String> = conn.smembers(keys::ALL_QUEUES).await?;
    Ok(queue_names)
  }

  /// 使用 CURRENT_STATS_CMD 脚本获取队列的当前统计信息
  /// Get current queue statistics using CURRENT_STATS_CMD script
  ///
  /// 返回包含各种队列统计信息的原始数组
  /// Returns raw array containing various queue statistics
  pub async fn current_stats(&self, queue: &str) -> Result<Vec<redis::Value>> {
    let mut conn = self.get_async_connection().await?;

    let pending_key = keys::pending_key(queue);
    let active_key = keys::active_key(queue);
    let scheduled_key = keys::scheduled_key(queue);
    let retry_key = keys::retry_key(queue);
    let archived_key = keys::archived_key(queue);
    let completed_key = keys::completed_key(queue);

    let now = Utc::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let processed_key = format!("asynq:{queue}:processed:{date_str}");
    let failed_key = format!("asynq:{queue}:failed:{date_str}");
    let processed_total_key = format!("asynq:{queue}:processed");
    let failed_total_key = format!("asynq:{queue}:failed");
    let paused_key = format!("asynq:{queue}:paused");
    let groups_key = format!("asynq:{queue}:groups");

    let task_key_prefix = format!("asynq:{queue}:t:");
    let group_key_prefix = format!("asynq:{queue}:g:");

    let keys = vec![
      pending_key,
      active_key,
      scheduled_key,
      retry_key,
      archived_key,
      completed_key,
      processed_key,
      failed_key,
      processed_total_key,
      failed_total_key,
      paused_key,
      groups_key,
    ];

    let args = vec![
      RedisArg::Str(task_key_prefix),
      RedisArg::Str(group_key_prefix),
    ];

    let result: Vec<redis::Value> = self
      .script_manager
      .eval_script(&mut conn, "current_stats", &keys, &args)
      .await?;

    Ok(result)
  }
  /// 获取队列内存使用情况 - 使用脚本 - Go: MemoryUsage
  /// Get queue memory usage using script - Go: MemoryUsage
  pub async fn memory_usage(&self, queue: &str, sample_size: i64) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;

    let pending_key = keys::pending_key(queue);
    let active_key = keys::active_key(queue);
    let scheduled_key = keys::scheduled_key(queue);
    let retry_key = keys::retry_key(queue);
    let archived_key = keys::archived_key(queue);
    let completed_key = keys::completed_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![
      pending_key,
      active_key,
      scheduled_key,
      retry_key,
      archived_key,
      completed_key,
    ];
    let args = vec![RedisArg::Str(task_key_prefix), RedisArg::Int(sample_size)];

    let memory: i64 = self
      .script_manager
      .eval_script(&mut conn, "memory_usage", &keys, &args)
      .await?;

    Ok(memory)
  }

  /// 获取历史统计信息 - 使用脚本 - Go: HistoricalStats
  /// Get historical statistics using script - Go: HistoricalStats
  pub async fn historical_stats(
    &self,
    queue: &str,
    dates: &[chrono::NaiveDate],
  ) -> Result<Vec<(i64, i64)>> {
    let mut conn = self.get_async_connection().await?;

    let mut keys = Vec::new();
    for date in dates {
      let naive_datetime = date.and_hms_opt(0, 0, 0).unwrap_or_default();
      let dt = chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
      let processed_key = keys::processed_key(queue, &dt);
      let failed_key = keys::failed_key(queue, &dt);
      keys.push(processed_key);
      keys.push(failed_key);
    }

    let args: Vec<RedisArg> = vec![];

    let raw_result: Vec<i64> = self
      .script_manager
      .eval_script(&mut conn, "historical_stats", &keys, &args)
      .await?;

    // Parse results into (processed, failed) pairs
    let mut result = Vec::new();
    for chunk in raw_result.chunks(2) {
      if let [processed, failed] = chunk {
        result.push((*processed, *failed));
      }
    }

    Ok(result)
  }
  /// 获取任务信息。
  /// Get task information.
  pub async fn get_task_info(&self, queue: &str, task_id: &str) -> Result<Option<TaskInfo>> {
    let mut conn = self.get_async_connection().await?;

    // 检查各个队列中的任务
    // Check tasks in various queues
    let keys_to_check = vec![
      (keys::pending_key(queue), TaskState::Pending),
      (keys::active_key(queue), TaskState::Active),
      (keys::scheduled_key(queue), TaskState::Scheduled),
      (keys::retry_key(queue), TaskState::Retry),
      (keys::archived_key(queue), TaskState::Archived),
      (keys::completed_key(queue), TaskState::Completed),
    ];

    for (key, state) in keys_to_check {
      // 对于列表类型的键（pending, active）
      if matches!(state, TaskState::Pending | TaskState::Active) {
        let tasks: Vec<Vec<u8>> = conn.lrange(&key, 0, -1).await?;
        for task_data in tasks {
          if let Ok(msg) = self.decode_task_message(&task_data) {
            if msg.id == task_id {
              return Ok(Some(TaskInfo::from_proto(&msg, state, None, None)));
            }
          }
        }
      } else {
        // 对于有序集合类型的键（scheduled, retry, archived, completed）
        let tasks: Vec<Vec<u8>> = conn.zrange(&key, 0, -1).await?;
        for task_data in tasks {
          if let Ok(msg) = self.decode_task_message(&task_data) {
            if msg.id == task_id {
              return Ok(Some(TaskInfo::from_proto(&msg, state, None, None)));
            }
          }
        }
      }
    }

    // 检查聚合队列
    // Check aggregation queues
    let aggregating_pattern = format!("{}{}:*", keys::AGGREGATING_PREFIX, queue);
    let aggregating_keys: Vec<String> = conn.keys(&aggregating_pattern).await?;
    for key in aggregating_keys {
      let tasks: Vec<Vec<u8>> = conn.lrange(&key, 0, -1).await?;
      for task_data in tasks {
        if let Ok(msg) = self.decode_task_message(&task_data) {
          if msg.id == task_id {
            return Ok(Some(TaskInfo::from_proto(
              &msg,
              TaskState::Aggregating,
              None,
              None,
            )));
          }
        }
      }
    }

    Ok(None)
  }
}
impl RedisBroker {
  /// 获取组统计信息 - 使用脚本 - Go: GroupStats
  /// Get group statistics using script - Go: GroupStats
  pub async fn group_stats(&self, queue: &str) -> Result<Vec<(String, i64)>> {
    let mut conn = self.get_async_connection().await?;

    let groups_key = keys::groups_key(queue);
    let group_key_prefix = keys::group_key_prefix(queue);

    let keys = vec![groups_key];
    let args = vec![RedisArg::Str(group_key_prefix)];

    let raw_result: Vec<redis::Value> = self
      .script_manager
      .eval_script(&mut conn, "group_stats", &keys, &args)
      .await?;

    // Parse results into (group_name, size) pairs
    let mut result = Vec::new();
    for chunk in raw_result.chunks(2) {
      if let [name_val, size_val] = chunk {
        let name: String = redis::from_redis_value(name_val.clone())?;
        let size: i64 = redis::from_redis_value(size_val.clone())?;
        result.push((name, size));
      }
    }

    Ok(result)
  }
}
impl RedisBroker {
  // Reports whether a queue with the given name exists.
  async fn queue_exists(&self, queue: &str) -> Result<bool> {
    let mut conn = self.get_async_connection().await?;
    let exists: bool = conn.sismember(keys::ALL_QUEUES, queue).await?;
    Ok(exists)
  }

  /// 列出列表类型队列的消息 - 使用脚本 - Go: ListMessages
  /// List messages in list-type queues using script - Go: ListMessages
  async fn list_messages(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    let mut conn = self.get_async_connection().await?;
    let key = state.queue_key(queue, None);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![key];
    let args = vec![
      RedisArg::Int(pagination.start()),
      RedisArg::Int(pagination.stop()),
      RedisArg::Str(task_key_prefix),
    ];

    let raw_result: Vec<Vec<u8>> = self
      .script_manager
      .eval_script(&mut conn, "list_messages", &keys, &args)
      .await?;

    // Parse the result: alternating msg and result
    let mut result = Vec::new();
    for chunk in raw_result.chunks(2) {
      if let [msg, res] = chunk {
        let task = self.decode_task_message(msg)?;
        result.push(TaskInfo::from_proto(&task, state, None, Some(res.clone())));
      }
    }
    Ok(result)
  }
  /// 列出有序集合类型队列的条目 - 使用脚本 - Go: ListZSetEntries
  /// List entries in zset-type queues using script - Go: ListZSetEntries
  async fn list_zset_entries(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    let mut conn = self.get_async_connection().await?;
    let key = state.queue_key(queue, None);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![key];
    let args = vec![
      RedisArg::Int(pagination.start()),
      RedisArg::Int(pagination.stop()),
      RedisArg::Str(task_key_prefix),
    ];

    let raw_result: Vec<redis::Value> = self
      .script_manager
      .eval_script(&mut conn, "list_zset_entries", &keys, &args)
      .await?;

    // Parse the result: msg, score, result triplets
    let mut result = Vec::new();
    for chunk in raw_result.chunks(3) {
      if let [msg_val, score_val, res_val] = chunk {
        let msg: Vec<u8> = redis::from_redis_value(msg_val.clone())?;
        let score: f64 = redis::from_redis_value(score_val.clone())?;
        let next_process_at = if score > 0.0 {
          Some(
            Utc
              .timestamp_opt(score as i64, 0)
              .single()
              .unwrap_or_default(),
          )
        } else {
          None
        };
        let res_bytes: Vec<u8> = redis::from_redis_value(res_val.clone())?;
        let res_opt = if res_bytes.is_empty() {
          None
        } else {
          Some(res_bytes)
        };
        let task = self.decode_task_message(&msg)?;
        result.push(TaskInfo::from_proto(&task, state, next_process_at, res_opt));
      }
    }

    Ok(result)
  }
}
impl RedisBroker {
  /// 列出活跃调度器键 - 使用脚本 - Go: ListSchedulerKeys
  /// List active scheduler keys using script - Go: ListSchedulerKeys
  pub async fn list_scheduler_keys(&self) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;
    let now = Utc::now().timestamp();

    let keys = vec![keys::ALL_SCHEDULERS.to_string()];
    let args = vec![RedisArg::Int(now)];

    let result: Vec<String> = self
      .script_manager
      .eval_script(&mut conn, "list_scheduler_keys", &keys, &args)
      .await?;

    Ok(result)
  }

  /// 列出任务。
  /// List tasks.
  pub async fn list_tasks(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    if !self.queue_exists(queue).await? {
      return Err(Error::other(format!("Queue '{queue}' does not exist")));
    }
    if pagination.page < 0 || pagination.size < 1 {
      return Ok(Vec::new());
    }
    let task_info_list: Vec<TaskInfo> = if matches!(state, TaskState::Pending | TaskState::Active) {
      // 列表类型
      // List type
      self.list_messages(queue, state, pagination).await?
    } else {
      // 有序集合类型，按分数降序获取（最新的在前）
      // Sorted set type, get in descending order by score (latest first)
      self.list_zset_entries(queue, state, pagination).await?
    };
    Ok(task_info_list)
  }
}
impl RedisBroker {
  /// 获取指定队列的任务ID列表（按状态）
  /// Get task IDs for a queue by state
  pub async fn list_task_ids(&self, queue: &str, state: &TaskState) -> Result<Vec<String>> {
    let key = format!("asynq:{queue}:{state}"); // 这里可进一步用 keys.rs 的辅助函数优化
    let mut conn = self.get_async_connection().await?;
    let ids: Vec<String> = conn.zrange(&key, 0, -1).await?;
    Ok(ids)
  }

  /// 获取所有活跃服务节点ID
  /// Get all active server IDs
  pub async fn list_server_ids(&self) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;
    let ids: Vec<String> = conn.smembers(keys::ALL_SERVERS).await?;
    Ok(ids)
  }

  /// 获取单个任务的详细信息（返回TaskMessage）
  /// Get detail info for a single task (returns TaskMessage)
  pub async fn get_task(
    &self,
    queue: &str,
    state: TaskState,
    task_id: &str,
  ) -> Result<Option<TaskMessage>> {
    let key = format!("asynq:{queue}:{state}"); // 这里可进一步用 keys.rs 的辅助函数优化
    let mut conn = self.get_async_connection().await?;
    let value: Option<Vec<u8>> = conn.zscore(&key, task_id).await?;
    if let Some(bytes) = value {
      // 反序列化为TaskMessage
      let msg = TaskMessage::decode(&*bytes)?;
      Ok(Some(msg))
    } else {
      Ok(None)
    }
  }

  /// 获取队列各状态任务数量
  /// Get task counts for each state in a queue
  pub async fn get_queue_state_counts(
    &self,
    queue: &str,
  ) -> Result<std::collections::HashMap<String, i64>> {
    let mut conn = self.get_async_connection().await?;
    let mut counts = std::collections::HashMap::new();
    for state in [
      "pending",
      "active",
      "scheduled",
      "retry",
      "archived",
      "completed",
    ] {
      let key = format!("asynq:{queue}:{state}"); // 这里可进一步用 keys.rs 的辅助函数优化
      let count: i64 = conn.zcard(&key).await?;
      counts.insert(state.to_string(), count);
    }
    Ok(counts)
  }

  /// 列出活跃服务器键 - 使用脚本 - Go: ListServerKeys
  /// List active server keys using script - Go: ListServerKeys
  pub async fn list_server_keys(&self) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;
    let now = Utc::now().timestamp();

    let keys = vec![keys::ALL_SERVERS.to_string()];
    let args = vec![RedisArg::Int(now)];

    let result: Vec<String> = self
      .script_manager
      .eval_script(&mut conn, "list_server_keys", &keys, &args)
      .await?;

    Ok(result)
  }

  /// 列出指定服务器的工作者 - 使用脚本 - Go: ListWorkers
  /// List workers for a specific server using script - Go: ListWorkers
  pub async fn list_workers(&self, host: &str, pid: i32, server_id: &str) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;
    let workers_key = keys::workers_key(host, pid, server_id);
    let now = Utc::now().timestamp();

    let keys = vec![workers_key];
    let args = vec![RedisArg::Int(now)];

    let result: Vec<String> = self
      .script_manager
      .eval_script(&mut conn, "list_workers", &keys, &args)
      .await?;

    Ok(result)
  }

  /// 删除任务。
  /// Delete a task.
  pub async fn delete_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // 使用 delete_task 脚本
    // Use delete_task script
    let task_key = keys::task_key(queue, task_id);
    let groups_key = keys::groups_key(queue);
    let queue_key_prefix = format!("asynq:{queue}:");
    let group_key_prefix = keys::group_key_prefix(queue);

    let keys = vec![task_key, groups_key];
    let args = vec![
      RedisArg::Str(task_id.to_string()),
      RedisArg::Str(queue_key_prefix),
      RedisArg::Str(group_key_prefix),
    ];

    let result: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_task", &keys, &args)
      .await?;

    match result {
      1 => Ok(()),
      0 => Err(Error::other("Task not found")),
      -1 => Err(Error::other("Cannot delete active task")),
      _ => Err(Error::other("Unexpected script result")),
    }
  }
  /// 删除所有归档任务。
  /// Delete all archived tasks.
  pub async fn delete_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let archived_key = keys::archived_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![archived_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_all", &keys, &args)
      .await?;

    Ok(count)
  }
  /// 删除所有重试任务。
  /// Delete all retry tasks.
  pub async fn delete_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let retry_key = keys::retry_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![retry_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_all", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 删除所有调度任务。
  /// Delete all scheduled tasks.
  pub async fn delete_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let scheduled_key = keys::scheduled_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![scheduled_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_all", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 删除所有待处理任务。
  /// Delete all pending tasks.
  pub async fn delete_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let pending_key = keys::pending_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![pending_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_all_pending", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 删除所有已完成任务 - Go: DeleteAllCompletedTasks
  pub async fn delete_all_completed_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let key = keys::completed_key(queue);
    let count: i64 = conn.zcard(&key).await?;
    conn.del::<_, ()>(&key).await?;
    Ok(count)
  }
  /// 重新排队所有归档任务。
  /// Requeue all archived tasks.
  pub async fn requeue_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let archived_key = keys::archived_key(queue);
    let pending_key = keys::pending_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![archived_key, pending_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "run_all", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 重新排队所有重试任务。
  /// Requeue all retry tasks.
  pub async fn requeue_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let retry_key = keys::retry_key(queue);
    let pending_key = keys::pending_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![retry_key, pending_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "run_all", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 重新排队所有调度任务。
  /// Requeue all scheduled tasks.
  pub async fn requeue_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let scheduled_key = keys::scheduled_key(queue);
    let pending_key = keys::pending_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![scheduled_key, pending_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "run_all", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 获取队列统计信息。
  /// Get queue statistics.
  pub async fn get_queue_stats(&self, queue: &str) -> Result<QueueStats> {
    let mut conn = self.get_async_connection().await?;

    // 获取各个队列的长度
    // Get the length of each queue
    let pending: i64 = conn.llen(keys::pending_key(queue)).await?;
    let active: i64 = conn.llen(keys::active_key(queue)).await?;
    let scheduled: i64 = conn.zcard(keys::scheduled_key(queue)).await?;
    let retry: i64 = conn.zcard(keys::retry_key(queue)).await?;
    let archived: i64 = conn.zcard(keys::archived_key(queue)).await?;
    let completed: i64 = conn.zcard(keys::completed_key(queue)).await?;

    // 获取聚合任务数量
    // Get the number of aggregating tasks
    let aggregating_pattern = format!("{}{}:*", keys::AGGREGATING_PREFIX, queue);
    let aggregating_keys: Vec<String> = conn.keys(&aggregating_pattern).await?;
    let mut aggregating = 0i64;
    for key in aggregating_keys {
      let count: i64 = conn.llen(&key).await?;
      aggregating += count;
    }

    // 构建每日统计信息（简化版本 - 实际实现可能需要更复杂的逻辑）
    // Build daily statistics (simplified version - actual implementation may require more complex logic)
    let daily_stats = Vec::new(); // TODO: 可以添加基于日期的统计信息

    Ok(QueueStats {
      name: queue.to_string(),
      active,
      pending,
      scheduled,
      retry,
      archived,
      completed,
      aggregating,
      daily_stats,
    })
  }

  /// 获取队列信息。
  /// Get queue information.
  pub async fn get_queue_info(&self, queue: &str) -> Result<QueueInfo> {
    // 获取队列的各种统计信息
    // Get various statistics of the queue
    let stats = self.get_queue_stats(queue).await?;
    // let state_counts = self.get_queue_state_counts(queue).await?;
    // 计算队列大小（所有状态任务的总数）
    // Calculate the queue size (total number of tasks in all states)
    let size = stats.active
      + stats.pending
      + stats.scheduled
      + stats.retry
      + stats.archived
      + stats.completed
      + stats.aggregating;

    // 获取内存使用情况 (使用近似值)
    // Get memory usage (using approximate value)
    let memory_usage = size * 1024; // 假设每个任务约 1KB

    // 获取延迟信息 (简化实现，实际应该基于任务的等待时间)
    // Get latency information (simplified implementation, should be based on task waiting time in reality)
    let latency = Duration::from_secs(0); // TODO: 实现真实的延迟计算

    // 获取任务组数量
    // Get the number of task groups
    let groups = self.list_groups(queue).await?.len() as i32;

    Ok(QueueInfo {
      queue: queue.to_string(),
      memory_usage,
      latency,
      size: size as i32,
      groups,
      pending: stats.pending as i32,
      active: stats.active as i32,
      scheduled: stats.scheduled as i32,
      retry: stats.retry as i32,
      archived: stats.archived as i32,
      completed: stats.completed as i32,
      aggregating: stats.aggregating as i32,
      processed: 0,       // TODO: 获取今日处理数
      failed: 0,          // TODO: 获取今日失败数
      processed_total: 0, // TODO: 获取总处理数
      failed_total: 0,    // TODO: 获取总失败数
      paused: self.is_queue_paused(queue).await?,
      timestamp: Utc::now(),
    })
  }

  /// 获取所有队列的统计信息。
  /// Get statistics of all queues.
  pub async fn get_all_queue_stats(&self) -> Result<Vec<QueueStats>> {
    let mut conn = self.get_async_connection().await?;

    // 获取所有队列名
    // Get all queue names
    let queue_pattern = format!("{}{}*", keys::QUEUE_PREFIX, keys::QUEUE_START);
    let queue_keys: Vec<String> = conn.keys(&queue_pattern).await?;

    let mut all_stats = Vec::new();

    for queue_key in queue_keys {
      // 从键名中提取队列名
      // Extract queue name from key
      if let Some(queue_name) =
        queue_key.strip_prefix(&format!("{}{}", keys::QUEUE_PREFIX, keys::QUEUE_START))
      {
        let stats = self.get_queue_stats(queue_name).await?;
        all_stats.push(stats);
      }
    }

    Ok(all_stats)
  }

  /// 获取所有队列的名称。
  /// Get the names of all queues.
  pub async fn get_queues(&self) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;

    // 获取所有队列键模式
    // Get all queue key patterns
    let queue_pattern = format!("{}{}*", keys::QUEUE_PREFIX, keys::QUEUE_START);
    let queue_keys: Vec<String> = conn.keys(&queue_pattern).await?;

    let mut queues = std::collections::HashSet::new();

    for key in queue_keys {
      // 从键名中提取队列名
      // Extract queue name from key
      if let Some(after_prefix) =
        key.strip_prefix(&format!("{}{}", keys::QUEUE_PREFIX, keys::QUEUE_START))
      {
        if let Some(queue_name) = after_prefix.split(':').next() {
          let queue_name = queue_name.trim_end_matches(keys::QUEUE_END);
          if !queue_name.is_empty() {
            queues.insert(queue_name.to_string());
          }
        }
      }
    }

    Ok(queues.into_iter().collect())
  }

  /// 暂停队列。
  /// Pause a queue.
  pub async fn pause_queue(&self, queue: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // 将队列添加到暂停队列集合中
    // Add the queue to the paused queue set
    let key = keys::paused_key(queue);
    let now = Utc::now().timestamp();
    let _: i32 = conn.set_nx(key, now).await?;

    Ok(())
  }

  /// 恢复队列。
  /// Unpause a queue.
  pub async fn unpause_queue(&self, queue: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // 从暂停队列集合中移除队列
    // Remove the queue from the paused queue set
    let key = keys::paused_key(queue);
    let _: i32 = conn.del(key).await?;

    Ok(())
  }

  // Additional Go asynq Broker interface methods:

  /// 检查队列是否被暂停。
  /// Check if a queue is paused.
  pub async fn is_queue_paused(&self, queue: &str) -> Result<bool> {
    let mut conn = self.get_async_connection().await?;

    // 检查队列是否在暂停集合中
    // Check if the queue is in the paused set
    let key = keys::paused_key(queue);
    let exists: bool = conn.exists(key).await?;

    Ok(exists)
  }

  /// 获取任务结果。
  /// Get task result.
  pub async fn get_result(&self, queue: &str, task_id: &str) -> Result<Option<Vec<u8>>> {
    let mut conn = self.get_async_connection().await?;

    let result_key = keys::task_key(queue, task_id);

    // 获取任务结果
    // Get the task result
    let result: Option<Vec<u8>> = conn.hget(&result_key, "result").await?;

    Ok(result)
  }

  /// 检查速率限制
  /// Check rate limit
  pub async fn check_rate_limit(&self, task: &Task) -> Result<bool> {
    // 如果任务没有配置速率限制，直接允许
    // If the task has no rate limit configured, allow directly
    let rate_limit = match &task.options.rate_limit {
      Some(limit) => limit,
      None => return Ok(true),
    };

    let mut conn = self.get_async_connection().await?;

    // 生成速率限制键
    // Generate rate limit key
    let rate_key = rate_limit.generate_key(&task.task_type, &task.options.queue);
    let window_seconds = rate_limit.window.as_secs();
    let limit_count = rate_limit.limit;
    let now = Utc::now().timestamp();

    // 使用 Lua 脚本进行速率限制检查
    // Use Lua script to check rate limit
    let result: Vec<i64> = {
      let keys = vec![rate_key];
      let args = vec![
        RedisArg::Int(window_seconds as i64),
        RedisArg::Int(limit_count as i64),
        RedisArg::Int(now),
      ];

      // 使用 ScriptManager 执行脚本
      // Use ScriptManager to execute script
      self
        .script_manager
        .eval_script(&mut conn, "rate_limit", &keys, &args)
        .await?
    };

    // result[0] = 1 表示允许，0 表示限流
    // result[1] = 剩余配额
    Ok(result.first().unwrap_or(&0) == &1)
  }

  /// 恢复孤儿任务。
  /// Recover orphaned tasks.
  pub async fn recover_orphaned_tasks(&self, queues: &[String]) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let mut recovered = 0i64;

    // 恢复孤儿任务 - 这些是在active队列中但对应的worker已经死亡的任务
    // Recover orphaned tasks - these are tasks in the active queue but the corresponding worker has died
    for queue in queues {
      let active_key = keys::active_key(queue);
      let queue_key = keys::pending_key(queue);

      // 获取所有活跃任务
      // Get all active tasks
      let active_tasks: Vec<Vec<u8>> = conn.smembers(&active_key).await?;

      // 简化版本：将所有活跃任务重新排队
      // Simplified version: requeue all active tasks
      // 在实际实现中，应该检查worker的心跳状态
      // In the actual implementation, the worker's heartbeat status should be checked
      if !active_tasks.is_empty() {
        let mut pipe = redis::pipe();
        pipe.atomic();

        for task_data in &active_tasks {
          // 从活跃集合移除
          // Remove from active set
          pipe.srem(&active_key, task_data);
          // 重新排队
          // Requeue
          pipe.lpush(&queue_key, task_data);
        }

        let _: Vec<i32> = pipe.query_async(&mut conn).await?;
        recovered += active_tasks.len() as i64;
      }
    }

    Ok(recovered)
  }

  /// 聚合任务组。
  /// Aggregate a group of tasks.
  pub async fn aggregate_group(
    &self,
    queue: &str,
    group: &str,
    max_size: usize,
  ) -> Result<Option<Vec<TaskMessage>>> {
    let mut conn = self.get_async_connection().await?;
    let aggregating_key = keys::aggregating_key(queue, group);

    // 获取指定数量的任务进行聚合
    // Get a specified number of tasks for aggregation
    let task_data_list: Vec<Vec<u8>> = conn
      .lrange(&aggregating_key, 0, max_size as isize - 1)
      .await?;

    if task_data_list.is_empty() {
      return Ok(None);
    }

    let mut tasks = Vec::new();
    let mut pipe = redis::pipe();
    pipe.atomic();

    // 解码任务并从聚合队列中移除
    // Decode tasks and remove from aggregation queue
    for task_data in &task_data_list {
      if let Ok(msg) = self.decode_task_message(task_data) {
        tasks.push(msg);
        pipe.lrem(&aggregating_key, 1, task_data);
      }
    }

    // 如果聚合队列为空，删除键
    // If the aggregation queue is empty, delete the key
    let remaining_count: i64 = conn.llen(&aggregating_key).await?;
    if remaining_count <= task_data_list.len() as i64 {
      pipe.del(&aggregating_key);
    }

    let _: Vec<i32> = pipe.query_async(&mut conn).await?;

    if tasks.is_empty() {
      Ok(None)
    } else {
      Ok(Some(tasks))
    }
  }

  /// 注销服务器。
  /// Unregister a server.
  pub async fn unregister_server(&self, server_id: &str) -> Result<()> {
    // 解析 server_id 格式以获取 host, pid, server_id 组件
    // Parse server_id format to get host, pid, server_id components
    let parts: Vec<&str> = server_id.split(':').collect();
    if parts.len() != 3 {
      return Err(Error::other(format!(
        "Invalid server_id format: {server_id}, expected hostname:pid:uuid"
      )));
    }

    let hostname = parts[0];
    let pid: i32 = parts[1]
      .parse()
      .map_err(|_| Error::other(format!("Invalid pid in server_id: {}", parts[1])))?;
    let uuid = parts[2];

    // 使用 clear_server_state 来正确地注销服务器
    // Use clear_server_state to correctly unregister the server
    self.clear_server_state(hostname, pid, uuid).await
  }

  /// 心跳检测。
  /// Heartbeat detection.
  pub async fn heartbeat(&self, server_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    let server_key = keys::server_info_key_legacy(server_id);
    let timestamp = Utc::now().timestamp();

    // 更新最后心跳时间
    // Update the last heartbeat time
    let _: () = conn.hset(&server_key, "last_heartbeat", timestamp).await?;
    let _: () = conn.expire(&server_key, 3600).await?; // 重新设置过期时间

    Ok(())
  }

  /// 获取所有服务器信息。
  /// Get information of all servers.
  pub async fn get_servers(&self) -> Result<Vec<ServerInfo>> {
    let mut conn = self.get_async_connection().await?;

    // 从 asynq:servers ZSET 中获取所有服务器ID (格式: hostname:pid:uuid)
    // Get all server IDs from asynq:servers ZSET (format: hostname:pid:uuid)
    let server_ids: Vec<String> = conn.zrange(keys::ALL_SERVERS, 0, -1).await?;
    let mut servers = Vec::new();
    for server_id in server_ids {
      if let Ok(Some(server_info)) = self.get_server_info(&server_id).await {
        servers.push(server_info);
      }
    }

    Ok(servers)
  }

  /// 获取指定服务器的信息。
  /// Get information of a specified server.
  pub async fn get_server_info(&self, server_key: &str) -> Result<Option<ServerInfo>> {
    let mut conn = self.get_async_connection().await?;
    // 从 Redis 获取 protobuf 编码的服务器信息
    // Get protobuf encoded server information from Redis
    let server_data: Option<Vec<u8>> = conn.get(server_key).await?;
    if let Some(data) = server_data {
      match ServerInfo::decode(&data[..]) {
        Ok(server_info) => Ok(Some(server_info)),
        Err(_err) => Ok(None),
      }
    } else {
      Ok(None)
    }
  }

  /// 获取历史统计信息。
  /// Get historical statistics.
  pub async fn get_history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>> {
    let mut conn = self.get_async_connection().await?;
    let mut history = Vec::new();

    for i in 0..days {
      let date = Utc::now() - chrono::Duration::days(i as i64);

      // 获取该日期的处理数和失败数
      // Get the processed and failed count for the date
      let processed_key = keys::processed_key(queue, &date);
      let failed_key = keys::failed_key(queue, &date);

      let processed: i64 = conn.get(&processed_key).await.unwrap_or(0);
      let failed: i64 = conn.get(&failed_key).await.unwrap_or(0);

      history.push(DailyStats {
        queue: queue.to_string(),
        processed,
        failed,
        date,
      });
    }

    Ok(history)
  }

  /// 清理死掉的服务器。
  /// Cleanup dead servers.
  pub async fn cleanup_dead_servers(&self, timeout: Duration) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;

    let _cutoff_time = Utc::now().timestamp() - timeout.as_secs() as i64;
    // 从 asynq:servers ZSET 中获取所有服务器ID (格式: hostname:pid:uuid)
    // Get all server IDs from asynq:servers ZSET (format: hostname:pid:uuid)
    let server_ids: Vec<String> = conn.zrange(keys::ALL_SERVERS, 0, -1).await?;

    let mut cleaned_up = 0i64;

    for server_id in server_ids {
      // 解析 server_id 格式: hostname:pid:uuid
      // Parse server_id format: hostname:pid:uuid
      let parts: Vec<&str> = server_id.split(':').collect();
      if parts.len() != 3 {
        continue; // 跳过格式不正确的ID
      }

      let hostname = parts[0];
      let pid: i32 = match parts[1].parse() {
        Ok(p) => p,
        Err(_) => continue, // 跳过无效的pid
      };
      let uuid = parts[2];

      // 构建服务器键和工作者键
      // Build server and worker keys
      let server_key = keys::server_info_key(hostname, pid, uuid);
      let workers_key = keys::workers_key(hostname, pid, uuid);

      // 检查服务器是否仍然活跃（通过检查服务器键是否存在）
      // Check if the server is still active (by checking if the server key exists)
      let exists: bool = conn.exists(&server_key).await?;

      if !exists {
        // 服务器已死亡，从ZSET中移除
        // Server is dead, remove from ZSET
        let _: i32 = conn.zrem(keys::ALL_SERVERS, &server_id).await?;
        let _: i32 = conn.zrem(keys::ALL_WORKERS, &workers_key).await?;
        cleaned_up += 1;
      }
    }

    Ok(cleaned_up)
  }

  /// 归档单个任务 - Go: ArchiveTask
  pub async fn archive_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // Check if task exists and get its current state
    let task_info = self.get_task_info(queue, task_id).await?;
    if task_info.is_none() {
      return Err(Error::other("Task not found"));
    }

    let task_info = match task_info {
      Some(info) => info,
      None => return Err(Error::other("Task not found")),
    };
    let task_key = keys::task_key(queue, task_id);
    let archived_key = keys::archived_key(queue);
    let current_time = Utc::now().timestamp();

    // Determine source key based on task state
    let source_key = match task_info.state {
      TaskState::Pending => keys::pending_key(queue),
      TaskState::Scheduled => keys::scheduled_key(queue),
      TaskState::Retry => keys::retry_key(queue),
      TaskState::Active => keys::active_key(queue),
      TaskState::Aggregating => {
        if let Some(group) = &task_info.group {
          keys::group_key(queue, group)
        } else {
          return Err(Error::other("Aggregating task must have group"));
        }
      }
      TaskState::Archived => return Ok(()), // Already archived
      TaskState::Completed => return Err(Error::other("Cannot archive completed task")),
    };

    // For active tasks, we also need to consider the lease key
    let keys = if task_info.state == TaskState::Active {
      vec![task_key, source_key, archived_key, keys::lease_key(queue)]
    } else {
      vec![task_key, source_key, archived_key]
    };

    let args = vec![
      RedisArg::Str(task_id.to_string()),
      RedisArg::Int(current_time),
    ];

    let result: i64 = self
      .script_manager
      .eval_script(&mut conn, "archive_task", &keys, &args)
      .await?;

    if result == 0 {
      return Err(Error::other(
        "Failed to archive task - task may not exist in expected state",
      ));
    }

    Ok(())
  }

  /// 归档所有等待任务 - Go: ArchiveAllPendingTasks
  pub async fn archive_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;

    let pending_key = keys::pending_key(queue);
    let archived_key = keys::archived_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);
    let current_time = Utc::now().timestamp();

    let keys = vec![pending_key, archived_key];
    let args = vec![RedisArg::Str(task_key_prefix), RedisArg::Int(current_time)];

    let result: i64 = self
      .script_manager
      .eval_script(&mut conn, "archive_all_pending", &keys, &args)
      .await?;

    Ok(result)
  }

  /// 归档所有重试任务 - Go: ArchiveAllRetryTasks
  pub async fn archive_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;

    let retry_key = keys::retry_key(queue);
    let archived_key = keys::archived_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);
    let current_time = Utc::now().timestamp();

    let keys = vec![retry_key, archived_key];
    let args = vec![RedisArg::Str(task_key_prefix), RedisArg::Int(current_time)];

    let result: i64 = self
      .script_manager
      .eval_script(&mut conn, "archive_all", &keys, &args)
      .await?;

    Ok(result)
  }

  /// 归档所有调度任务 - Go: ArchiveAllScheduledTasks
  pub async fn archive_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;

    let scheduled_key = keys::scheduled_key(queue);
    let archived_key = keys::archived_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);
    let current_time = Utc::now().timestamp();

    let keys = vec![scheduled_key, archived_key];
    let args = vec![RedisArg::Str(task_key_prefix), RedisArg::Int(current_time)];

    let result: i64 = self
      .script_manager
      .eval_script(&mut conn, "archive_all", &keys, &args)
      .await?;

    Ok(result)
  }

  /// 归档所有聚合任务 - Go: ArchiveAllAggregatingTasks
  pub async fn archive_all_aggregating_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;

    // Get all groups for this queue first
    let groups = self.list_groups(queue).await?;
    let mut total_archived = 0;

    for group in groups {
      let group_key = keys::group_key(queue, &group);
      let archived_key = keys::archived_key(queue);
      let all_aggregation_sets_key = keys::all_aggregation_sets(queue);
      let task_key_prefix = keys::task_key_prefix(queue);
      let current_time = Utc::now().timestamp();

      let keys = vec![group_key, archived_key, all_aggregation_sets_key];
      let args = vec![
        RedisArg::Str(task_key_prefix),
        RedisArg::Int(current_time),
        RedisArg::Str(group.clone()),
      ];

      let result: i64 = self
        .script_manager
        .eval_script(&mut conn, "archive_all_aggregating", &keys, &args)
        .await?;

      total_archived += result;
    }

    Ok(total_archived)
  }

  /// 运行单个任务 - Go: RunTask
  pub async fn run_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let keys = vec![
      keys::task_key(queue, task_id),
      keys::pending_key(queue),
      keys::all_groups(queue),
    ];
    let args = vec![
      RedisArg::Str(task_id.to_string()),
      RedisArg::Str(keys::queue_key_prefix(queue)),
      RedisArg::Str(keys::group_key_prefix(queue)),
    ];
    let result: i64 = self
      .script_manager
      .eval_script(&mut conn, "run_task", &keys, &args)
      .await?;
    match result {
      1 => Ok(()),
      0 => Err(Error::other("Task not found")),
      -1 => Err(Error::other("Task is in active state")),
      -2 => Err(Error::other("Task is already in pending state")),
      _ => Err(Error::other("Unexpected script result")),
    }
  }

  /// 删除队列 - Go: RemoveQueue
  pub async fn remove_queue(&self, queue: &str, force: bool) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    if !force {
      // Check if queue has active tasks
      let active_key = keys::active_key(queue);
      let active_count: i64 = conn.hlen(&active_key).await?;
      if active_count > 0 {
        return Err(Error::other(
          "Queue has active tasks. Use force=true to remove anyway.",
        ));
      }
    }

    // Remove all queue-related keys
    let keys_to_remove = vec![
      keys::pending_key(queue),
      keys::active_key(queue),
      keys::scheduled_key(queue),
      keys::retry_key(queue),
      keys::archived_key(queue),
      keys::completed_key(queue),
      keys::paused_key(queue),
      keys::lease_key(queue),
    ];

    for key in keys_to_remove {
      conn.del::<_, ()>(&key).await?;
    }

    Ok(())
  }

  /// 获取聚合集合 - Go: AggregationSets
  pub async fn get_aggregation_sets(&self, queue: &str, group: &str) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;
    let key = keys::all_aggregation_sets(queue);
    let _pattern = format!("{group}:*");

    // Use SMEMBERS to get all aggregation sets, then filter by pattern
    let all_sets: Vec<String> = conn.smembers(&key).await?;
    let filtered_sets: Vec<String> = all_sets
      .into_iter()
      .filter(|s| s.starts_with(&format!("{group}:")))
      .collect();

    Ok(filtered_sets)
  }
  /// 删除所有聚合任务 - 使用脚本 - Go: DeleteAllAggregating
  /// Delete all aggregating tasks using script - Go: DeleteAllAggregating
  pub async fn delete_all_aggregating_tasks(&self, queue: &str, group: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let group_key = keys::group_key(queue, group);
    let groups_key = keys::groups_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![group_key, groups_key];
    let args = vec![
      RedisArg::Str(task_key_prefix),
      RedisArg::Str(group.to_string()),
    ];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_all_aggregating", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 运行所有聚合任务 - 使用脚本 - Go: RunAllAggregating
  /// Run all aggregating tasks using script - Go: RunAllAggregating
  pub async fn run_all_aggregating_tasks(&self, queue: &str, group: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let group_key = keys::group_key(queue, group);
    let pending_key = keys::pending_key(queue);
    let groups_key = keys::groups_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    let keys = vec![group_key, pending_key, groups_key];
    let args = vec![
      RedisArg::Str(task_key_prefix),
      RedisArg::Str(group.to_string()),
    ];

    let count: i64 = self
      .script_manager
      .eval_script(&mut conn, "run_all_aggregating", &keys, &args)
      .await?;

    Ok(count)
  }

  /// 使用 REQUEUE 脚本将活跃任务重新入队 - Go: Requeue
  /// Requeue active task using REQUEUE script - Go: Requeue
  pub async fn requeue_active_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let active_key = keys::active_key(queue);
    let lease_key = keys::lease_key(queue);
    let pending_key = keys::pending_key(queue);
    let task_key = keys::task_key(queue, task_id);

    let keys = vec![active_key, lease_key, pending_key, task_key];
    let args = vec![RedisArg::Str(task_id.to_string())];

    self
      .script_manager
      .eval_script::<()>(&mut conn, "requeue", &keys, &args)
      .await?;

    Ok(())
  }

  /// 使用 DELETE_AGGREGATION_SET_CMD 脚本删除聚合集合
  /// Delete aggregation set using DELETE_AGGREGATION_SET_CMD script
  pub async fn del_aggregation_set(&self, queue: &str, group: &str, set_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    let aggregation_set_key = format!("asynq:{queue}:g:{group}:{set_id}");
    let all_aggregation_sets_key = keys::all_aggregation_sets(queue);
    let task_key_prefix = format!("asynq:{queue}:t:");

    let keys = vec![aggregation_set_key, all_aggregation_sets_key];
    let args = vec![RedisArg::Str(task_key_prefix)];

    self
      .script_manager
      .eval_script::<()>(&mut conn, "delete_aggregation_set", &keys, &args)
      .await?;

    Ok(())
  }
}
