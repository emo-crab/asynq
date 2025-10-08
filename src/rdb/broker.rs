use crate::base::constants::{DEFAULT_ARCHIVED_EXPIRATION_IN_DAYS, DEFAULT_MAX_ARCHIVE_SIZE};
use crate::base::keys;
use crate::base::keys::TaskState;
use crate::base::Broker;
use crate::proto::{ServerInfo, TaskMessage};
use crate::rdb::redis_scripts::RedisArg;
use crate::rdb::RedisBroker;
use crate::task::{generate_task_id, Task, TaskInfo};
use crate::error::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message;
use redis::AsyncCommands;
use std::time::Duration;

const STATS_TTL: i32 = 90 * 24 * 60 * 60; // 90 days in seconds
/// RedisBroker 实现 Broker trait，提供与 Redis 的任务存储和调度交互。
/// RedisBroker implements the Broker trait, providing task storage and scheduling interaction with Redis.
#[async_trait]
impl Broker for RedisBroker {
  /// 测试连接。
  /// Ping the server.
  async fn ping(&self) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let _: String = conn.ping().await?;
    Ok(())
  }

  /// 关闭连接。
  /// Close the connection.
  async fn close(&self) -> Result<()> {
    // Redis客户端会自动管理连接的关闭
    // Redis client will manage connection closing automatically
    Ok(())
  }

  /// 将任务加入队列。
  /// Enqueue a task into the queue.
  async fn enqueue(&self, task: &Task) -> Result<TaskInfo> {
    let mut conn = self.get_async_connection().await?;

    let msg = self.task_to_message(task);
    let encoded = self.encode_task_message(&msg)?;

    // 确保队列在全局队列集合中注册
    // Ensure the queue is registered in the global queue set
    let _: () = conn.sadd(keys::ALL_QUEUES, &msg.queue).await?;

    // 使用与 Go 版本兼容的脚本
    // Use script compatible with Go version
    let task_key = keys::task_key(&msg.queue, &msg.id);
    let pending_key = keys::pending_key(&msg.queue);
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();

    let result: i64 = {
      let keys = vec![task_key, pending_key];
      let args = vec![
        RedisArg::Bytes(encoded),
        RedisArg::Str(msg.id.clone()),
        RedisArg::Int(now_nanos),
      ];

      // 使用 ScriptManager 执行脚本
      // Use ScriptManager to execute script
      self
        .script_manager
        .eval_script(&mut conn, "enqueue", &keys, &args)
        .await?
    };

    if result == 0 {
      return Err(Error::TaskIdConflict);
    }

    Ok(TaskInfo::from_proto(&msg, TaskState::Pending, None, None))
  }

  /// 将任务以唯一方式加入队列，确保在给定的 TTL 内任务 ID 唯一。
  /// Enqueue a task uniquely into the queue, ensuring the task ID is unique within the given TTL.
  async fn enqueue_unique(&self, task: &Task, ttl: Duration) -> Result<TaskInfo> {
    let mut conn = self.get_async_connection().await?;

    let mut msg = self.task_to_message(task);

    // 生成唯一键 - 与 Go 版本兼容
    // Generate unique key - Compatible with Go version
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();

    let encoded = self.encode_task_message(&msg)?;

    // 确保队列在全局队列集合中注册
    // Ensure the queue is registered in the global queue set
    let _: () = conn.sadd(keys::ALL_QUEUES, &msg.queue).await?;

    // 使用与 Go 版本兼容的脚本
    // Use script compatible with Go version
    let task_key = keys::task_key(&msg.queue, &msg.id);
    let pending_key = keys::pending_key(&msg.queue);
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();

    let result: i64 = {
      let keys = vec![unique_key, task_key, pending_key];
      let args = vec![
        RedisArg::Str(msg.id.clone()),
        RedisArg::Int(ttl.as_secs() as i64),
        RedisArg::Bytes(encoded),
        RedisArg::Int(now_nanos),
      ];

      // 使用 ScriptManager 执行脚本
      // Use ScriptManager to execute script
      self
        .script_manager
        .eval_script(&mut conn, "enqueue_unique", &keys, &args)
        .await?
    };

    match result {
      -1 => Err(Error::TaskDuplicate),
      0 => Err(Error::TaskIdConflict),
      1 => Ok(TaskInfo::from_proto(&msg, TaskState::Pending, None, None)),
      _ => Err(Error::other("Unexpected script result")),
    }
  }

  /// 从队列中取出任务。
  /// Dequeue a task from the queue.
  async fn dequeue(&self, queues: &[String]) -> Result<Option<TaskMessage>> {
    let mut conn = self.get_async_connection().await?;

    if queues.is_empty() {
      return Ok(None);
    }

    // Try to dequeue from each queue in priority order
    for queue in queues {
      // Build keys according to Go asynq dequeue script format:
      // KEYS[1] -> asynq:{<qname>}:pending
      // KEYS[2] -> asynq:{<qname>}:paused
      // KEYS[3] -> asynq:{<qname>}:active
      // KEYS[4] -> asynq:{<qname>}:lease
      let script_keys = vec![
        keys::pending_key(queue), // KEYS[1]
        keys::paused_key(queue),  // KEYS[2]
        keys::active_key(queue),  // KEYS[3]
        keys::lease_key(queue),   // KEYS[4]
      ];

      // ARGV[1] -> lease expiration unix time
      // ARGV[2] -> task key prefix (e.g., "asynq:{qname}:t:")
      let lease_expiry = Utc::now().timestamp() + 3600; // 1 hour lease
      let task_key_prefix = keys::task_key_prefix(queue);
      let args = vec![RedisArg::Int(lease_expiry), RedisArg::Str(task_key_prefix)];

      // The dequeue script returns the task message directly (or nil)
      let result: Option<Vec<u8>> = self
        .script_manager
        .eval_script(&mut conn, "dequeue", &script_keys, &args)
        .await?;

      if let Some(encoded_msg) = result {
        // Decode the task message directly from the script result
        let msg = self.decode_task_message(&encoded_msg)?;
        return Ok(Some(msg));
      }
    }

    Ok(None)
  }

  /// 标记任务为完成。
  /// Mark a task as done.
  async fn done(&self, msg: &TaskMessage) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // Generate date string for daily statistics - format: YYYY-MM-DD
    let now = Utc::now();
    // Calculate stats expiration (end of next day)
    let tomorrow = now + chrono::Duration::days(1);
    let end_of_tomorrow = match tomorrow.date_naive().and_hms_opt(23, 59, 59) {
      Some(dt) => dt.and_utc(),
      None => return Err(Error::other("Invalid time for end_of_tomorrow")),
    };
    let stats_expiration = end_of_tomorrow.timestamp();
    // Use regular DONE script for non-unique tasks
    // KEYS[1] -> asynq:{<qname>}:active
    // KEYS[2] -> asynq:{<qname>}:lease
    // KEYS[3] -> asynq:{<qname>}:t:<task_id>
    // KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
    // KEYS[5] -> asynq:{<qname>}:processed
    let mut keys = vec![
      keys::active_key(&msg.queue),
      keys::lease_key(&msg.queue),
      keys::task_key(&msg.queue, &msg.id),
      keys::processed_key(&msg.queue, &now),
      keys::processed_total_key(&msg.queue),
    ];
    // ARGV[1] -> task ID
    // ARGV[2] -> stats expiration timestamp
    // ARGV[3] -> max int64 value
    let args = vec![
      RedisArg::Str(msg.id.clone()),
      RedisArg::Int(stats_expiration),
      RedisArg::Int(i64::MAX),
    ];
    if !msg.unique_key.is_empty() {
      keys.push(msg.unique_key.clone());
    }
    let _result: String = self
      .script_manager
      .eval_script(&mut conn, "done", &keys, &args)
      .await?;
    Ok(())
  }

  /// 标记任务为完成状态 - Go: MarkAsComplete
  async fn mark_as_complete(&self, msg: &TaskMessage, result: &[u8]) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let queue = &msg.queue;
    let now = Utc::now();
    let completed_key = keys::completed_key(queue);
    let active_key = keys::active_key(queue);
    let lease_key = keys::lease_key(queue);
    let task_key = keys::task_key(queue, &msg.id);
    let processed_key = keys::processed_key(queue, &now);
    let processed_total_key = keys::processed_total_key(queue);
    let stats_expiration = Utc::now().timestamp() + 86400;
    let completed_at = Utc::now().timestamp();
    let retention = msg.retention;
    let keys = if msg.unique_key.is_empty() {
      vec![
        active_key,
        lease_key,
        completed_key,
        task_key,
        processed_key,
        processed_total_key,
      ]
    } else {
      vec![
        active_key,
        lease_key,
        completed_key,
        task_key,
        processed_key,
        processed_total_key,
        msg.unique_key.clone(),
      ]
    };
    let args = vec![
      RedisArg::Str(msg.id.clone()),
      RedisArg::Int(stats_expiration),
      RedisArg::Int(completed_at + retention),
      RedisArg::Bytes(result.to_vec()),
      RedisArg::Int(i64::MAX),
    ];
    let script = if msg.unique_key.is_empty() {
      "mark_as_complete"
    } else {
      "mark_as_complete_unique"
    };
    self
      .script_manager
      .eval_script::<()>(&mut conn, script, &keys, &args)
      .await?;
    Ok(())
  }

  /// 重新排队任务。
  /// Requeue a task.
  async fn requeue(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
  ) -> Result<()> {
    // Use the script-based retry function for Go compatibility
    self
      .retry(msg, process_at, error_msg, !error_msg.is_empty())
      .await
  }

  /// 将任务调度到指定时间处理。
  /// Schedule a task to be processed at a specific time.
  async fn schedule(&self, task: &Task, process_at: DateTime<Utc>) -> Result<TaskInfo> {
    let mut conn = self.get_async_connection().await?;

    let msg = self.task_to_message(task);
    let encoded = self.encode_task_message(&msg)?;

    // 确保队列在全局队列集合中注册
    // Ensure the queue is registered in the global queue set
    let _: () = conn.sadd(keys::ALL_QUEUES, &msg.queue).await?;

    // 使用与 Go 版本兼容的脚本
    // Use script compatible with Go version
    let task_key = keys::task_key(&msg.queue, &msg.id);
    let scheduled_key = keys::scheduled_key(&msg.queue);
    let process_timestamp = process_at.timestamp();

    let result: i64 = {
      let keys = vec![task_key, scheduled_key];
      let args = vec![
        RedisArg::Bytes(encoded),
        RedisArg::Int(process_timestamp),
        RedisArg::Str(msg.id.clone()),
      ];

      // 使用 ScriptManager 执行脚本
      // Use ScriptManager to execute script
      self
        .script_manager
        .eval_script(&mut conn, "schedule", &keys, &args)
        .await?
    };

    if result == 0 {
      return Err(Error::TaskIdConflict);
    }

    Ok(TaskInfo::from_proto(&msg, TaskState::Scheduled, None, None))
  }

  /// 将任务以唯一方式调度，确保在给定的 TTL 内任务 ID 唯一。
  /// Schedule a task uniquely to be processed at a specific time, ensuring the task ID is unique within the given TTL.
  async fn schedule_unique(
    &self,
    task: &Task,
    process_at: DateTime<Utc>,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let mut conn = self.get_async_connection().await?;

    let mut msg = self.task_to_message(task);

    // 生成唯一键 - 与 Go 版本兼容
    // Generate unique key - Compatible with Go version
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();

    let encoded = self.encode_task_message(&msg)?;

    // 确保队列在全局队列集合中注册
    // Ensure the queue is registered in the global queue set
    let _: () = conn.sadd(keys::ALL_QUEUES, &msg.queue).await?;

    // 使用与 Go 版本兼容的脚本
    // Use script compatible with Go version
    let task_key = keys::task_key(&msg.queue, &msg.id);
    let scheduled_key = keys::scheduled_key(&msg.queue);
    let process_timestamp = process_at.timestamp();

    let result: i64 = {
      let keys = vec![unique_key.clone(), task_key, scheduled_key];
      let args = vec![
        RedisArg::Str(msg.id.clone()),
        RedisArg::Int(ttl.as_secs() as i64),
        RedisArg::Int(process_timestamp),
        RedisArg::Bytes(encoded),
      ];

      self
        .script_manager
        .eval_script(&mut conn, "schedule_unique", &keys, &args)
        .await?
    };

    match result {
      1 => Ok(TaskInfo::from_proto(&msg, TaskState::Scheduled, None, None)),
      0 => Err(Error::TaskIdConflict),
      -1 => Err(Error::TaskDuplicate),
      _ => Err(Error::other("Unexpected script result")),
    }
  }

  /// 重试失败的任务 - Go: Retry
  async fn retry(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
    is_failure: bool,
  ) -> Result<()> {
    let mut msg = msg.clone();
    if is_failure {
      msg.retried += 1;
    }
    msg.error_msg = error_msg.to_string();
    msg.last_failed_at = Utc::now().timestamp();
    let mut conn = self.get_async_connection().await?;
    let queue = &msg.queue;
    let now = Utc::now();
    let retry_key = keys::retry_key(queue);
    let active_key = keys::active_key(queue);
    let lease_key = keys::lease_key(queue);
    let task_key = keys::task_key(queue, &msg.id);
    let processed_key = keys::processed_key(queue, &now);
    let failed_key = keys::failed_key(queue, &now);
    let process_total_key = keys::processed_total_key(queue);
    let failed_total_key = keys::failed_total_key(queue);
    let expire_at = Utc::now().timestamp() + STATS_TTL as i64;

    let keys = vec![
      task_key,
      active_key,
      lease_key,
      retry_key,
      processed_key,
      failed_key,
      process_total_key,
      failed_total_key,
    ];
    let args = vec![
      RedisArg::Str(msg.id.clone()),
      RedisArg::Bytes(msg.encode_to_vec()),
      RedisArg::Int(process_at.timestamp()),
      RedisArg::Int(expire_at),
      RedisArg::Bool(is_failure),
      RedisArg::Int(i64::MAX),
    ];
    self
      .script_manager
      .eval_script::<()>(&mut conn, "retry", &keys, &args)
      .await?;
    Ok(())
  }

  /// 归档任务。
  /// Archive a task.
  async fn archive(&self, msg: &TaskMessage, error_msg: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let now = Utc::now();
    let cutoff = now - chrono::Duration::days(DEFAULT_ARCHIVED_EXPIRATION_IN_DAYS);
    let expire_at = now.timestamp() + STATS_TTL as i64;

    let mut archived_msg = msg.clone();
    archived_msg.error_msg = error_msg.to_string();
    archived_msg.last_failed_at = now.timestamp();
    let archived_encoded = self.encode_task_message(&archived_msg)?;

    // 使用 Lua 脚本确保原子性归档操作
    // Use Lua script to ensure atomic archive operation
    let keys = vec![
      keys::task_key(&msg.queue, &msg.id),
      keys::active_key(&msg.queue),
      keys::lease_key(&msg.queue),
      keys::archived_key(&msg.queue),
      keys::processed_key(&msg.queue, &now),
      keys::failed_key(&msg.queue, &now),
      keys::processed_total_key(&msg.queue),
      keys::failed_total_key(&msg.queue),
      keys::task_key_prefix(&msg.queue),
    ];
    let args = vec![
      RedisArg::Str(msg.id.clone()), // 传递任务ID
      RedisArg::Bytes(archived_encoded),
      RedisArg::Int(now.timestamp()),
      RedisArg::Int(cutoff.timestamp()),
      RedisArg::Int(DEFAULT_MAX_ARCHIVE_SIZE),
      RedisArg::Int(expire_at),
      RedisArg::Int(i64::MAX),
    ];
    // 使用 ScriptManager 执行脚本
    // Use ScriptManager to execute script
    let _:()=self
      .script_manager
      .eval_script(&mut conn, "archive", &keys, &args)
      .await?;
    Ok(())
  }

  /// 转发就绪任务到待处理队列。
  /// Forward ready tasks to the pending queue.
  async fn forward_if_ready(&self, queues: &[String]) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let now = Utc::now().timestamp();
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let mut forwarded = 0i64;

    for queue in queues {
      // 使用 forward_scheduled 脚本处理调度队列
      // Use forward_scheduled script for scheduled queue
      let scheduled_key = keys::scheduled_key(queue);
      let pending_key = keys::pending_key(queue);
      let task_key_prefix = keys::task_key_prefix(queue);
      let group_key_prefix = keys::group_key_prefix(queue);

      let keys = vec![scheduled_key, pending_key.clone()];
      let args = vec![
        RedisArg::Int(now),
        RedisArg::Str(task_key_prefix.clone()),
        RedisArg::Int(now_nanos),
        RedisArg::Str(group_key_prefix.clone()),
      ];

      let count: i64 = self
        .script_manager
        .eval_script(&mut conn, "forward", &keys, &args)
        .await?;
      forwarded += count;

      // 使用 forward_retry 脚本处理重试队列
      // Use forward_retry script for retry queue
      let retry_key = keys::retry_key(queue);
      let keys = vec![retry_key, pending_key];
      let args = vec![
        RedisArg::Int(now),
        RedisArg::Str(task_key_prefix),
        RedisArg::Int(now_nanos),
        RedisArg::Str(group_key_prefix),
      ];

      let count: i64 = self
        .script_manager
        .eval_script(&mut conn, "forward", &keys, &args)
        .await?;
      forwarded += count;
    }

    Ok(forwarded)
  }

  /// 将任务添加到指定组。
  /// Add a task to a specified group.
  async fn add_to_group(&self, task: &Task, group: &str) -> Result<TaskInfo> {
    let mut conn = self.get_async_connection().await?;

    let mut msg = self.task_to_message(task);
    msg.group_key = group.to_string();

    let encoded = self.encode_task_message(&msg)?;

    // 确保队列在全局队列集合中注册
    // Ensure the queue is registered in the global queue set
    let _: () = conn.sadd(keys::ALL_QUEUES, &msg.queue).await?;

    // 使用与 Go 版本兼容的脚本
    // Use script compatible with Go version
    let task_key = keys::task_key(&msg.queue, &msg.id);
    let group_key = keys::group_key(&msg.queue, group);
    let groups_key = keys::groups_key(&msg.queue);
    let now = Utc::now().timestamp();

    let result: i64 = {
      let keys = vec![task_key, group_key, groups_key];
      let args = vec![
        RedisArg::Bytes(encoded),
        RedisArg::Str(msg.id.clone()),
        RedisArg::Int(now),
        RedisArg::Str(group.to_string()),
      ];

      self
        .script_manager
        .eval_script(&mut conn, "add_to_group", &keys, &args)
        .await?
    };

    if result == 0 {
      return Err(Error::TaskIdConflict);
    }

    Ok(TaskInfo::from_proto(
      &msg,
      TaskState::Aggregating,
      None,
      None,
    ))
  }

  /// 将任务以唯一方式添加到指定组。
  /// Add a task uniquely to a specified group, ensuring uniqueness within the given TTL.
  async fn add_to_group_unique(&self, task: &Task, group: &str, ttl: Duration) -> Result<TaskInfo> {
    let mut conn = self.get_async_connection().await?;

    let mut msg = self.task_to_message(task);
    msg.group_key = group.to_string();

    // 生成唯一键 - 与 Go 版本兼容
    // Generate unique key - Compatible with Go version
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();

    let encoded = self.encode_task_message(&msg)?;

    // 确保队列在全局队列集合中注册
    // Ensure the queue is registered in the global queue set
    let _: () = conn.sadd(keys::ALL_QUEUES, &msg.queue).await?;

    // 使用与 Go 版本兼容的脚本
    // Use script compatible with Go version
    let task_key = keys::task_key(&msg.queue, &msg.id);
    let group_key = keys::group_key(&msg.queue, group);
    let groups_key = keys::groups_key(&msg.queue);
    let now = Utc::now().timestamp();

    let result: i64 = {
      let keys = vec![unique_key.clone(), task_key, group_key, groups_key];
      let args = vec![
        RedisArg::Str(msg.id.clone()),
        RedisArg::Int(ttl.as_secs() as i64),
        RedisArg::Bytes(encoded),
        RedisArg::Int(now),
        RedisArg::Str(group.to_string()),
      ];

      self
        .script_manager
        .eval_script(&mut conn, "add_to_group_unique", &keys, &args)
        .await?
    };

    match result {
      1 => Ok(TaskInfo::from_proto(
        &msg,
        TaskState::Aggregating,
        None,
        None,
      )),
      0 => Err(Error::TaskIdConflict),
      -1 => Err(Error::TaskDuplicate),
      _ => Err(Error::other("Unexpected script result")),
    }
  }

  /// 获取指定队列的所有组。
  /// Get all groups of the specified queue.
  async fn list_groups(&self, queue: &str) -> Result<Vec<String>> {
    let mut conn = self.get_async_connection().await?;

    // 获取聚合队列的所有组
    // Get all groups of the aggregation queue
    let groups: Vec<String> = conn.smembers(keys::all_groups(queue)).await?;
    Ok(groups)
  }

  /// 检查聚合条件是否满足 - Go: AggregationCheck
  async fn aggregation_check(
    &self,
    queue: &str,
    group: &str,
    aggregation_delay: Duration,
    max_delay: Duration,
    max_size: usize,
  ) -> Result<Option<String>> {
    let mut conn = self.get_async_connection().await?;
    let aggregation_set_id = generate_task_id();
    let keys = vec![
      keys::group_key(queue, group),
      keys::aggregation_set_key(queue, group, &aggregation_set_id),
      keys::all_aggregation_sets(queue),
      keys::all_groups(queue),
    ];
    let expire_at = Utc::now().timestamp() + STATS_TTL as i64;

    let args = vec![
      RedisArg::Int(max_size as i64),
      RedisArg::Int(max_delay.as_secs() as i64),
      RedisArg::Int(aggregation_delay.as_secs() as i64),
      RedisArg::Float(expire_at as f64),
      RedisArg::Float(Utc::now().timestamp() as f64),
      RedisArg::Str(group.to_string()),
    ];
    let result: Option<String> = self
      .script_manager
      .eval_script(&mut conn, "aggregation_check", &keys, &args)
      .await?;
    match result {
      Some(ref s) => match s.as_str() {
        "1" => Ok(Some(aggregation_set_id)),
        _ => Ok(None),
      },
      None => Ok(None),
    }
  }

  /// 读取聚合集合中的任务 - Go: ReadAggregationSet
  async fn read_aggregation_set(
    &self,
    queue: &str,
    group: &str,
    set_id: &str,
  ) -> Result<Vec<TaskMessage>> {
    let mut conn = self.get_async_connection().await?;
    let aggregation_key = keys::aggregation_set_key(queue, group, set_id);
    let keys = vec![aggregation_key.clone()];
    let args = vec![RedisArg::Str(keys::task_key_prefix(queue))];
    let result: Vec<Vec<u8>> = self
      .script_manager
      .eval_script(&mut conn, "read_aggregation_set", &keys, &args)
      .await?;
    let mut messages = Vec::new();
    for data in result {
      if let Ok(msg) = self.decode_task_message(&data) {
        messages.push(msg);
      }
    }
    let _deadline_unix: f64 = conn
      .zscore(keys::all_aggregation_sets(queue), aggregation_key)
      .await?;
    Ok(messages)
  }

  /// 关闭聚合集合 - Go: CloseAggregationSet
  async fn delete_aggregation_set(&self, queue: &str, group: &str, set_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    let keys = vec![
      keys::aggregation_set_key(queue, group, set_id),
      keys::all_aggregation_sets(queue),
    ];
    let args = vec![RedisArg::Str(keys::task_key_prefix(queue))];

    let _: () = self
      .script_manager
      .eval_script(&mut conn, "delete_aggregation_set", &keys, &args)
      .await?;

    Ok(())
  }

  /// 回收过期的聚合集合 - Go: ReclaimStaleAggregationSets
  async fn reclaim_stale_aggregation_sets(&self, queue: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let all_sets_key = keys::all_aggregation_sets(queue);
    let keys = vec![all_sets_key];
    let args = vec![RedisArg::Int(Utc::now().timestamp())];
    let _: () = self
      .script_manager
      .eval_script(&mut conn, "reclaim_stale_aggregation_sets", &keys, &args)
      .await?;
    Ok(())
  }

  /// 删除过期的已完成任务。
  /// Delete expired completed tasks.
  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64> {
    let mut conn = self.get_async_connection().await?;
    let completed_key = keys::completed_key(queue);
    let task_key_prefix = keys::task_key_prefix(queue);

    // 删除超过一定时间的已完成任务（例如超过7天的）
    // Delete completed tasks that are older than a certain period (e.g., older than 7 days)
    let cutoff_time = (Utc::now() - chrono::Duration::days(7)).timestamp();
    let batch_size = 100; // Process in batches

    let keys = vec![completed_key];
    let args = vec![
      RedisArg::Int(cutoff_time),
      RedisArg::Str(task_key_prefix),
      RedisArg::Int(batch_size),
    ];

    let deleted_count: i64 = self
      .script_manager
      .eval_script(&mut conn, "delete_expired_completed_tasks", &keys, &args)
      .await?;

    Ok(deleted_count)
  }

  /// 列出租约已过期的任务 - Go: ListLeaseExpired
  async fn list_lease_expired(
    &self,
    cutoff: DateTime<Utc>,
    queues: &[String],
  ) -> Result<Vec<TaskMessage>> {
    let mut conn = self.get_async_connection().await?;
    let mut expired_tasks = Vec::new();
    let cutoff_timestamp = cutoff.timestamp();

    for queue in queues {
      let lease_key = keys::lease_key(queue);
      let task_key_prefix = keys::task_key_prefix(queue);

      let keys = vec![lease_key];
      let args = vec![
        RedisArg::Int(cutoff_timestamp),
        RedisArg::Str(task_key_prefix),
      ];

      // Get expired task messages from the script
      let task_data_list: Vec<Vec<u8>> = self
        .script_manager
        .eval_script(&mut conn, "list_lease_expired", &keys, &args)
        .await?;

      for task_data in task_data_list {
        if let Ok(msg) = self.decode_task_message(&task_data) {
          expired_tasks.push(msg);
        }
      }
    }

    Ok(expired_tasks)
  }

  /// 延长任务处理租约 - Go: ExtendLease
  async fn extend_lease(&self, queue: &str, task_id: &str, lease_duration: Duration) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    let lease_key = keys::lease_key(queue);
    let keys = vec![lease_key];
    let args = vec![
      RedisArg::Str(task_id.to_string()),
      RedisArg::Int(lease_duration.as_secs() as i64),
    ];
    self
      .script_manager
      .eval_script::<()>(&mut conn, "extend_lease", &keys, &args)
      .await?;
    Ok(())
  }

  /// 写入服务器状态 - Go: WriteServerState
  async fn write_server_state(&self, server_info: &ServerInfo, ttl: Duration) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // 使用 ServerInfo 中的信息构建键
    // Build keys using information from ServerInfo
    let server_key =
      keys::server_info_key(&server_info.host, server_info.pid, &server_info.server_id);
    let workers_key = keys::workers_key(&server_info.host, server_info.pid, &server_info.server_id);

    // 计算过期时间戳
    // Calculate expiration timestamp
    let exp_timestamp = (Utc::now()
      + chrono::Duration::from_std(ttl).map_err(|e| Error::other(format!("invalid ttl: {e}")))?)
    .timestamp();

    // 1. 将服务器ID添加到 AllServers ZSET，分数为过期时间戳 (服务器跟踪)
    // Add the server ID to AllServers ZSET with the expiration timestamp as score (server tracking)
    let _: () = conn
      .zadd(keys::ALL_SERVERS, &server_key, exp_timestamp as f64)
      .await?;

    // 2. 将工作者键添加到 AllWorkers ZSET，分数为过期时间戳 (工作者跟踪)
    // Add the worker key to AllWorkers ZSET with the expiration timestamp as score (worker tracking)
    let _: () = conn
      .zadd(keys::ALL_WORKERS, &workers_key, exp_timestamp as f64)
      .await?;

    // 3. 使用 Lua 脚本原子性地写入服务器状态
    // Use Lua script to atomically write server state
    let server_info_bytes = server_info.encode_to_vec();
    let keys = vec![server_key, workers_key];
    let string_args = vec![ttl.as_secs().to_string()];
    let binary_args = vec![server_info_bytes];

    // ARGV[1] = TTL in seconds
    // ARGV[2] = server info (encoded protobuf as binary)
    // ARGV[3+] = worker info (暂时为空，需要支持 WorkerInfo)
    // ARGV[3+] = worker info (currently empty, needs to support WorkerInfo)

    let _: String = self
      .script_manager
      .eval_script_with_binary_args(
        &mut conn,
        "write_server_state",
        &keys,
        &string_args,
        &binary_args,
      )
      .await?;

    Ok(())
  }

  /// 清除服务器状态 - Go: ClearServerState
  async fn clear_server_state(&self, host: &str, pid: i32, server_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // 生成键
    // Generate keys
    let server_key = keys::server_info_key(host, pid, server_id);
    let workers_key = keys::workers_key(host, pid, server_id);

    // 构造完整的 server_id (hostname:pid:uuid 格式)
    // Construct full server_id (hostname:pid:uuid format)
    let full_server_id = format!("{}:{}:{}", host, pid, server_id);

    // 1. 从 AllServers ZSET 中删除服务器 (服务器跟踪)
    // Remove the server from AllServers ZSET (server tracking)
    let _: () = conn.zrem(keys::ALL_SERVERS, &full_server_id).await?;

    // 2. 从 AllWorkers ZSET 中删除工作者键 (如果存在)
    // Remove the worker key from AllWorkers ZSET (if exists)
    let _: () = conn.zrem(keys::ALL_WORKERS, &workers_key).await?;

    // 3. 使用 Lua 脚本原子性地清除服务器状态
    // Use Lua script to atomically clear server state
    let keys = vec![server_key, workers_key];
    let string_args: Vec<String> = vec![];
    let binary_args: Vec<Vec<u8>> = vec![];

    let _: String = self
      .script_manager
      .eval_script_with_binary_args(
        &mut conn,
        "clear_server_state",
        &keys,
        &string_args,
        &binary_args,
      )
      .await?;

    Ok(())
  }

  /// 订阅任务取消事件 - Go: CancelationPubSub
  async fn cancellation_pub_sub(
    &self,
  ) -> Result<Box<dyn futures::Stream<Item = Result<String>> + Unpin + Send>> {
    use futures::StreamExt;

    // 获取 PubSub 连接
    // Get PubSub connection
    let mut pubsub = self.get_pubsub().await?;

    // 订阅取消频道
    // Subscribe to cancellation channel
    pubsub.subscribe(keys::CANCEL_CHANNEL).await?;

    // 获取消息流并将其转换为拥有所有权的 stream
    // Get message stream and convert it to an owned stream
    let message_stream = pubsub.into_on_message();

    // 创建一个流，将 Redis 消息转换为任务 ID 字符串
    // Create a stream that converts Redis messages to task ID strings
    let stream = message_stream.filter_map(|msg| async move {
      match msg.get_payload::<String>() {
        Ok(task_id) => Some(Ok(task_id)),
        Err(e) => {
          tracing::warn!("Failed to parse cancellation message: {}", e);
          Some(Err(Error::other(format!(
            "Failed to parse message: {}",
            e
          ))))
        }
      }
    });

    Ok(Box::new(Box::pin(stream)))
  }

  /// 发布任务取消通知 - Go: PublishCancelation
  async fn publish_cancellation(&self, task_id: &str) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    // Publish cancellation notification
    let _: i32 = conn.publish(keys::CANCEL_CHANNEL, task_id).await?;

    Ok(())
  }

  /// 写入任务结果。
  /// Write task result.
  async fn write_result(&self, queue: &str, task_id: &str, result: &[u8]) -> Result<()> {
    let mut conn = self.get_async_connection().await?;

    let result_key = keys::task_key(queue, task_id);

    // 存储任务结果，设置过期时间（例如24小时）
    // Store the task result with an expiration time (e.g., 24 hours)
    let _: () = conn.hset(&result_key, "result", result).await?;

    Ok(())
  }
}
