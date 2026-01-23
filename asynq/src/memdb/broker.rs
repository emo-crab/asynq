//! 内存 Broker trait 实现
//! Memory Broker trait implementation
//!
//! 使用内存数据结构实现 Broker trait
//! Implements Broker trait using in-memory data structures

use crate::base::constants::{DEFAULT_ARCHIVED_EXPIRATION_IN_DAYS, DEFAULT_MAX_ARCHIVE_SIZE};
use crate::base::keys::TaskState;
use crate::base::Broker;
use crate::error::{Error, Result};
use crate::memdb::memory_broker::{MemoryStorage, TaskData};
use crate::memdb::MemoryBroker;
use crate::proto::{ServerInfo, TaskMessage};
use crate::task::{generate_task_id, Task, TaskInfo};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::HashSet;
use std::time::Duration;

const LEASE_DURATION_SECS: i64 = 3600;

#[async_trait]
impl Broker for MemoryBroker {
  /// 测试连接
  /// Ping the connection
  async fn ping(&self) -> Result<()> {
    // 内存存储始终可用
    // Memory storage is always available
    Ok(())
  }

  /// 关闭连接
  /// Close the connection
  async fn close(&self) -> Result<()> {
    // 内存存储不需要关闭
    // Memory storage doesn't need to be closed
    Ok(())
  }

  /// 将任务加入队列
  /// Enqueue a task into the queue
  async fn enqueue(&self, task: &Task) -> Result<TaskInfo> {
    let msg = self.task_to_message(task);
    let encoded = self.encode_task_message(&msg)?;
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();

    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // 检查任务是否已存在
    // Check if task already exists
    if storage.tasks.contains_key(&task_key) {
      return Err(Error::TaskIdConflict);
    }

    // 存储任务数据
    // Store task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 添加到待处理队列
    // Add to pending queue
    let queue_data = storage.get_or_create_queue(&msg.queue);
    queue_data
      .pending
      .entry(now_nanos)
      .or_default()
      .push(msg.id.clone());

    Ok(TaskInfo::from_proto(&msg, TaskState::Pending, None, None))
  }

  /// 将唯一任务加入队列
  /// Enqueue a unique task
  async fn enqueue_unique(&self, task: &Task, ttl: Duration) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();

    let encoded = self.encode_task_message(&msg)?;
    let now = Utc::now();
    let now_nanos = now.timestamp_nanos_opt().unwrap_or_default();
    let expires_at = now.timestamp() + ttl.as_secs() as i64;

    let mut storage = self.storage.write().await;

    // 检查唯一键是否已存在且未过期
    // Check if unique key already exists and is not expired
    if let Some((_, exp)) = storage.unique_keys.get(&unique_key) {
      if *exp > now.timestamp() {
        return Err(Error::TaskDuplicate);
      }
    }

    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // 检查任务 ID 是否冲突
    // Check if task ID conflicts
    if storage.tasks.contains_key(&task_key) {
      return Err(Error::TaskIdConflict);
    }

    // 存储唯一键
    // Store unique key
    storage
      .unique_keys
      .insert(unique_key, (msg.id.clone(), expires_at));

    // 存储任务数据
    // Store task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 添加到待处理队列
    // Add to pending queue
    let queue_data = storage.get_or_create_queue(&msg.queue);
    queue_data
      .pending
      .entry(now_nanos)
      .or_default()
      .push(msg.id.clone());

    Ok(TaskInfo::from_proto(&msg, TaskState::Pending, None, None))
  }

  /// 从队列中取出任务
  /// Dequeue a task from the queue
  async fn dequeue(&self, queues: &[String]) -> Result<Option<TaskMessage>> {
    if queues.is_empty() {
      return Ok(None);
    }

    let mut storage = self.storage.write().await;
    let now = Utc::now();
    let lease_expiry = now.timestamp() + LEASE_DURATION_SECS;

    for queue in queues {
      let queue_data = match storage.queue_data.get_mut(queue) {
        Some(data) => data,
        None => continue,
      };

      // 检查队列是否暂停
      // Check if queue is paused
      if queue_data.paused {
        continue;
      }

      // 从待处理队列中取出第一个任务
      // Take the first task from pending queue
      // First get the first key
      let first_key = queue_data.pending.keys().next().copied();
      if let Some(score) = first_key {
        if let Some(task_ids) = queue_data.pending.get_mut(&score) {
          if let Some(task_id) = task_ids.pop() {
            // 如果这个分数下没有更多任务，移除该条目
            // Remove the entry if no more tasks at this score
            if task_ids.is_empty() {
              queue_data.pending.remove(&score);
            }

            // 添加到活跃队列
            // Add to active queue
            queue_data.active.insert(task_id.clone());

            // 设置租约
            // Set lease
            queue_data.lease.insert(task_id.clone(), lease_expiry);

            // 获取任务数据
            // Get task data
            let task_key = MemoryStorage::task_key(queue, &task_id);
            if let Some(task_data) = storage.tasks.get(&task_key) {
              return Ok(Some(task_data.message.clone()));
            }
          }
        }
      }
    }

    Ok(None)
  }

  /// 标记任务为完成
  /// Mark a task as done
  async fn done(&self, msg: &TaskMessage) -> Result<()> {
    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);
    let now = Utc::now();
    let date_str = MemoryBroker::get_date_string(&now);

    if let Some(queue_data) = storage.queue_data.get_mut(&msg.queue) {
      // 从活跃队列中移除
      // Remove from active queue
      queue_data.active.remove(&msg.id);

      // 移除租约
      // Remove lease
      queue_data.lease.remove(&msg.id);

      // 更新统计
      // Update statistics
      *queue_data.processed_daily.entry(date_str).or_insert(0) += 1;
      queue_data.processed_total += 1;
    }

    // 移除任务数据
    // Remove task data
    storage.tasks.remove(&task_key);

    // 移除唯一键（如果存在）
    // Remove unique key (if exists)
    if !msg.unique_key.is_empty() {
      storage.unique_keys.remove(&msg.unique_key);
    }

    Ok(())
  }

  /// 标记任务为完成状态
  /// Mark a task as complete
  async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()> {
    let mut msg = msg.clone();
    let now = Utc::now();
    msg.completed_at = now.timestamp();
    let date_str = MemoryBroker::get_date_string(&now);

    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    if let Some(queue_data) = storage.queue_data.get_mut(&msg.queue) {
      // 从活跃队列中移除
      // Remove from active queue
      queue_data.active.remove(&msg.id);

      // 移除租约
      // Remove lease
      queue_data.lease.remove(&msg.id);

      // 添加到已完成队列
      // Add to completed queue
      let completed_at = now.timestamp() + msg.retention;
      queue_data
        .completed
        .entry(completed_at)
        .or_default()
        .push(msg.id.clone());

      // 更新统计
      // Update statistics
      *queue_data.processed_daily.entry(date_str).or_insert(0) += 1;
      queue_data.processed_total += 1;
    }

    // 更新任务数据
    // Update task data
    let encoded = self.encode_task_message(&msg)?;
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 移除唯一键（如果存在）
    // Remove unique key (if exists)
    if !msg.unique_key.is_empty() {
      storage.unique_keys.remove(&msg.unique_key);
    }

    Ok(())
  }

  /// 重新排队任务
  /// Requeue a task
  async fn requeue(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
  ) -> Result<()> {
    self
      .retry(msg, process_at, error_msg, !error_msg.is_empty())
      .await
  }

  /// 调度任务
  /// Schedule a task
  async fn schedule(&self, task: &Task, process_at: DateTime<Utc>) -> Result<TaskInfo> {
    let msg = self.task_to_message(task);
    let encoded = self.encode_task_message(&msg)?;
    let process_timestamp = process_at.timestamp();

    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // 检查任务是否已存在
    // Check if task already exists
    if storage.tasks.contains_key(&task_key) {
      return Err(Error::TaskIdConflict);
    }

    // 存储任务数据
    // Store task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 添加到调度队列
    // Add to scheduled queue
    let queue_data = storage.get_or_create_queue(&msg.queue);
    queue_data
      .scheduled
      .entry(process_timestamp)
      .or_default()
      .push(msg.id.clone());

    Ok(TaskInfo::from_proto(&msg, TaskState::Scheduled, None, None))
  }

  /// 调度唯一任务
  /// Schedule a unique task
  async fn schedule_unique(
    &self,
    task: &Task,
    process_at: DateTime<Utc>,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();

    let encoded = self.encode_task_message(&msg)?;
    let now = Utc::now();
    let process_timestamp = process_at.timestamp();
    let expires_at = now.timestamp() + ttl.as_secs() as i64;

    let mut storage = self.storage.write().await;

    // 检查唯一键是否已存在且未过期
    // Check if unique key already exists and is not expired
    if let Some((_, exp)) = storage.unique_keys.get(&unique_key) {
      if *exp > now.timestamp() {
        return Err(Error::TaskDuplicate);
      }
    }

    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // 检查任务 ID 是否冲突
    // Check if task ID conflicts
    if storage.tasks.contains_key(&task_key) {
      return Err(Error::TaskIdConflict);
    }

    // 存储唯一键
    // Store unique key
    storage
      .unique_keys
      .insert(unique_key, (msg.id.clone(), expires_at));

    // 存储任务数据
    // Store task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 添加到调度队列
    // Add to scheduled queue
    let queue_data = storage.get_or_create_queue(&msg.queue);
    queue_data
      .scheduled
      .entry(process_timestamp)
      .or_default()
      .push(msg.id.clone());

    Ok(TaskInfo::from_proto(&msg, TaskState::Scheduled, None, None))
  }

  /// 重试失败的任务
  /// Retry a failed task
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

    let encoded = self.encode_task_message(&msg)?;
    let process_timestamp = process_at.timestamp();
    let now = Utc::now();
    let date_str = MemoryBroker::get_date_string(&now);

    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    if let Some(queue_data) = storage.queue_data.get_mut(&msg.queue) {
      // 从活跃队列中移除
      // Remove from active queue
      queue_data.active.remove(&msg.id);

      // 移除租约
      // Remove lease
      queue_data.lease.remove(&msg.id);

      // 添加到重试队列
      // Add to retry queue
      queue_data
        .retry
        .entry(process_timestamp)
        .or_default()
        .push(msg.id.clone());

      // 更新统计
      // Update statistics
      if is_failure {
        *queue_data
          .processed_daily
          .entry(date_str.clone())
          .or_insert(0) += 1;
        *queue_data.failed_daily.entry(date_str).or_insert(0) += 1;
        queue_data.processed_total += 1;
        queue_data.failed_total += 1;
      }
    }

    // 更新任务数据
    // Update task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg,
      },
    );

    Ok(())
  }

  /// 归档任务
  /// Archive a task
  async fn archive(&self, msg: &TaskMessage, error_msg: &str) -> Result<()> {
    let now = Utc::now();
    let date_str = MemoryBroker::get_date_string(&now);
    let cutoff = now.timestamp() - (DEFAULT_ARCHIVED_EXPIRATION_IN_DAYS * 24 * 60 * 60);

    let mut archived_msg = msg.clone();
    archived_msg.error_msg = error_msg.to_string();
    archived_msg.last_failed_at = now.timestamp();

    let encoded = self.encode_task_message(&archived_msg)?;

    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // First, collect keys to remove from archived queue that are from storage.tasks
    let keys_to_remove_from_tasks: Vec<String>;

    if let Some(queue_data) = storage.queue_data.get_mut(&msg.queue) {
      // 从活跃队列中移除
      // Remove from active queue
      queue_data.active.remove(&msg.id);

      // 移除租约
      // Remove lease
      queue_data.lease.remove(&msg.id);

      // 添加到归档队列
      // Add to archived queue
      queue_data
        .archived
        .entry(now.timestamp())
        .or_default()
        .push(msg.id.clone());

      // 清理过期的归档任务
      // Clean up expired archived tasks
      let mut to_remove: Vec<i64> = Vec::new();
      for (&score, _) in queue_data.archived.iter() {
        if score < cutoff {
          to_remove.push(score);
        }
      }
      for score in to_remove {
        queue_data.archived.remove(&score);
      }

      // 限制归档大小 - collect IDs to remove first
      // Limit archive size - collect IDs to remove first
      let mut total_count: i64 = queue_data
        .archived
        .values()
        .map(|v: &Vec<String>| v.len() as i64)
        .sum();

      let mut task_keys_to_remove: Vec<String> = Vec::new();
      while total_count > DEFAULT_MAX_ARCHIVE_SIZE {
        let oldest_score = queue_data.archived.keys().next().copied();
        if let Some(oldest_score) = oldest_score {
          if let Some(ids) = queue_data.archived.get_mut(&oldest_score) {
            if let Some(removed_id) = ids.pop() {
              task_keys_to_remove.push(MemoryStorage::task_key(&msg.queue, &removed_id));
              total_count -= 1;
            }
            if ids.is_empty() {
              queue_data.archived.remove(&oldest_score);
            }
          }
        } else {
          break;
        }
      }
      keys_to_remove_from_tasks = task_keys_to_remove;

      // 更新统计
      // Update statistics
      *queue_data
        .processed_daily
        .entry(date_str.clone())
        .or_insert(0) += 1;
      *queue_data.failed_daily.entry(date_str).or_insert(0) += 1;
      queue_data.processed_total += 1;
      queue_data.failed_total += 1;
    } else {
      keys_to_remove_from_tasks = Vec::new();
    }

    // Now remove the task keys outside the queue_data borrow
    for key in keys_to_remove_from_tasks {
      storage.tasks.remove(&key);
    }

    // 更新任务数据
    // Update task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: archived_msg,
      },
    );

    Ok(())
  }

  /// 转发就绪任务到待处理队列
  /// Forward ready tasks to pending queue
  async fn forward_if_ready(&self, queues: &[String]) -> Result<i64> {
    let now = Utc::now().timestamp();
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let mut forwarded: i64 = 0;

    let mut storage = self.storage.write().await;

    for queue in queues {
      // First collect all tasks to forward from scheduled queue
      let ready_scheduled: Vec<(i64, Vec<String>)> = storage
        .queue_data
        .get(queue)
        .map(|qd| {
          qd.scheduled
            .iter()
            .filter(|(&score, _): &(&i64, &Vec<String>)| score <= now)
            .map(|(&score, ids): (&i64, &Vec<String>)| (score, ids.clone()))
            .collect()
        })
        .unwrap_or_default();

      // Collect tasks with group info
      let mut to_pending: Vec<String> = Vec::new();
      let mut to_groups: Vec<(String, String)> = Vec::new(); // (group_key, task_id)
      let mut scheduled_scores_to_remove: Vec<i64> = Vec::new();

      for (score, task_ids) in &ready_scheduled {
        scheduled_scores_to_remove.push(*score);
        for task_id in task_ids {
          let task_key = MemoryStorage::task_key(queue, task_id);
          let group_key = storage.tasks.get(&task_key).and_then(|td| {
            if td.message.group_key.is_empty() {
              None
            } else {
              Some(td.message.group_key.clone())
            }
          });

          if let Some(gk) = group_key {
            to_groups.push((gk, task_id.clone()));
          } else {
            to_pending.push(task_id.clone());
          }
          forwarded += 1;
        }
      }

      // Now collect from retry queue
      let ready_retry: Vec<(i64, Vec<String>)> = storage
        .queue_data
        .get(queue)
        .map(|qd| {
          qd.retry
            .iter()
            .filter(|(&score, _): &(&i64, &Vec<String>)| score <= now)
            .map(|(&score, ids): (&i64, &Vec<String>)| (score, ids.clone()))
            .collect()
        })
        .unwrap_or_default();

      let mut retry_scores_to_remove: Vec<i64> = Vec::new();

      for (score, task_ids) in &ready_retry {
        retry_scores_to_remove.push(*score);
        for task_id in task_ids {
          let task_key = MemoryStorage::task_key(queue, task_id);
          let group_key = storage.tasks.get(&task_key).and_then(|td| {
            if td.message.group_key.is_empty() {
              None
            } else {
              Some(td.message.group_key.clone())
            }
          });

          if let Some(gk) = group_key {
            to_groups.push((gk, task_id.clone()));
          } else {
            to_pending.push(task_id.clone());
          }
          forwarded += 1;
        }
      }

      // Now apply all the changes
      if let Some(queue_data) = storage.queue_data.get_mut(queue) {
        // Remove from scheduled
        for score in scheduled_scores_to_remove {
          queue_data.scheduled.remove(&score);
        }

        // Remove from retry
        for score in retry_scores_to_remove {
          queue_data.retry.remove(&score);
        }

        // Add to pending
        for task_id in to_pending {
          queue_data
            .pending
            .entry(now_nanos)
            .or_default()
            .push(task_id);
        }

        // Add to groups
        for (group_key, task_id) in to_groups {
          let group = queue_data.groups.entry(group_key).or_default();
          group.tasks.entry(now).or_default().push(task_id);
        }
      }
    }

    Ok(forwarded)
  }

  /// 将任务添加到组中
  /// Add a task to a group
  async fn add_to_group(&self, task: &Task, group: &str) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    msg.group_key = group.to_string();

    let encoded = self.encode_task_message(&msg)?;
    let now = Utc::now().timestamp();

    let mut storage = self.storage.write().await;
    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // 检查任务是否已存在
    // Check if task already exists
    if storage.tasks.contains_key(&task_key) {
      return Err(Error::TaskIdConflict);
    }

    // 存储任务数据
    // Store task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 添加到组中
    // Add to group
    let queue_data = storage.get_or_create_queue(&msg.queue);
    let group_data = queue_data.groups.entry(group.to_string()).or_default();
    if group_data.created_at == 0 {
      group_data.created_at = now;
    }
    group_data
      .tasks
      .entry(now)
      .or_default()
      .push(msg.id.clone());

    Ok(TaskInfo::from_proto(
      &msg,
      TaskState::Aggregating,
      None,
      None,
    ))
  }

  /// 将唯一任务添加到组中
  /// Add a unique task to a group
  async fn add_to_group_unique(&self, task: &Task, group: &str, ttl: Duration) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    msg.group_key = group.to_string();

    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();

    let encoded = self.encode_task_message(&msg)?;
    let now = Utc::now();
    let expires_at = now.timestamp() + ttl.as_secs() as i64;

    let mut storage = self.storage.write().await;

    // 检查唯一键是否已存在且未过期
    // Check if unique key already exists and is not expired
    if let Some((_, exp)) = storage.unique_keys.get(&unique_key) {
      if *exp > now.timestamp() {
        return Err(Error::TaskDuplicate);
      }
    }

    let task_key = MemoryStorage::task_key(&msg.queue, &msg.id);

    // 检查任务 ID 是否冲突
    // Check if task ID conflicts
    if storage.tasks.contains_key(&task_key) {
      return Err(Error::TaskIdConflict);
    }

    // 存储唯一键
    // Store unique key
    storage
      .unique_keys
      .insert(unique_key, (msg.id.clone(), expires_at));

    // 存储任务数据
    // Store task data
    storage.tasks.insert(
      task_key,
      TaskData {
        encoded,
        message: msg.clone(),
      },
    );

    // 添加到组中
    // Add to group
    let queue_data = storage.get_or_create_queue(&msg.queue);
    let group_data = queue_data.groups.entry(group.to_string()).or_default();
    if group_data.created_at == 0 {
      group_data.created_at = now.timestamp();
    }
    group_data
      .tasks
      .entry(now.timestamp())
      .or_default()
      .push(msg.id.clone());

    Ok(TaskInfo::from_proto(
      &msg,
      TaskState::Aggregating,
      None,
      None,
    ))
  }

  /// 获取队列中的所有组
  /// Get all groups in a queue
  async fn list_groups(&self, queue: &str) -> Result<Vec<String>> {
    let storage = self.storage.read().await;
    if let Some(queue_data) = storage.queue_data.get(queue) {
      Ok(queue_data.groups.keys().cloned().collect())
    } else {
      Ok(Vec::new())
    }
  }

  /// 检查聚合条件是否满足
  /// Check if aggregation conditions are met
  async fn aggregation_check(
    &self,
    queue: &str,
    group: &str,
    aggregation_delay: Duration,
    max_delay: Duration,
    max_size: usize,
  ) -> Result<Option<String>> {
    let now = Utc::now().timestamp();
    let aggregation_set_id = generate_task_id();

    let mut storage = self.storage.write().await;

    if let Some(queue_data) = storage.queue_data.get_mut(queue) {
      if let Some(group_data) = queue_data.groups.get_mut(group) {
        // 计算组中的任务数量
        // Calculate number of tasks in group
        let task_count: usize = group_data
          .tasks
          .values()
          .map(|v: &Vec<String>| v.len())
          .sum();

        if task_count == 0 {
          return Ok(None);
        }

        // 检查是否达到最大大小
        // Check if max size is reached
        let size_ready = task_count >= max_size;

        // 检查是否达到最大延迟
        // Check if max delay is reached
        let delay_ready = now - group_data.created_at >= max_delay.as_secs() as i64;

        // 检查最早任务是否超过聚合延迟
        // Check if earliest task exceeds aggregation delay
        let oldest_task_ready = group_data
          .tasks
          .keys()
          .next()
          .is_some_and(|&score| now - score >= aggregation_delay.as_secs() as i64);

        if size_ready || delay_ready || oldest_task_ready {
          // 创建聚合集合
          // Create aggregation set
          let mut task_ids: HashSet<String> = HashSet::new();
          let mut tasks_to_take = max_size;
          let mut scores_to_remove: Vec<i64> = Vec::new();

          for (&score, ids) in group_data.tasks.iter() {
            let mut all_taken_from_score = true;
            for id in ids {
              if tasks_to_take == 0 {
                all_taken_from_score = false;
                break;
              }
              task_ids.insert(id.clone());
              tasks_to_take -= 1;
            }
            // Only remove the score if all tasks were taken from it
            if all_taken_from_score {
              scores_to_remove.push(score);
            }
            if tasks_to_take == 0 {
              break;
            }
          }

          // 从组中移除已取出的任务
          // Remove taken tasks from group
          for score in scores_to_remove {
            group_data.tasks.remove(&score);
          }

          // 存储聚合集合
          // Store aggregation set
          let expires_at = now + 3600; // 1 hour expiration
          queue_data.aggregation_sets.insert(
            format!("{}:{}", group, aggregation_set_id),
            crate::memdb::memory_broker::AggregationSet {
              task_ids,
              expires_at,
            },
          );

          return Ok(Some(aggregation_set_id));
        }
      }
    }

    Ok(None)
  }

  /// 读取聚合集合中的任务
  /// Read tasks from an aggregation set
  async fn read_aggregation_set(
    &self,
    queue: &str,
    group: &str,
    set_id: &str,
  ) -> Result<Vec<TaskMessage>> {
    let storage = self.storage.read().await;
    let set_key = format!("{}:{}", group, set_id);

    let mut messages = Vec::new();

    if let Some(queue_data) = storage.queue_data.get(queue) {
      if let Some(agg_set) = queue_data.aggregation_sets.get(&set_key) {
        for task_id in &agg_set.task_ids {
          let task_key = MemoryStorage::task_key(queue, task_id);
          if let Some(task_data) = storage.tasks.get(&task_key) {
            messages.push(task_data.message.clone());
          }
        }
      }
    }

    Ok(messages)
  }

  /// 删除聚合集合
  /// Delete an aggregation set
  async fn delete_aggregation_set(&self, queue: &str, group: &str, set_id: &str) -> Result<()> {
    let mut storage = self.storage.write().await;
    let set_key = format!("{}:{}", group, set_id);

    if let Some(queue_data) = storage.queue_data.get_mut(queue) {
      // 获取要删除的任务 ID
      // Get task IDs to delete
      let task_ids_to_delete: Vec<String> = queue_data
        .aggregation_sets
        .get(&set_key)
        .map(|s| s.task_ids.iter().cloned().collect())
        .unwrap_or_default();

      // 删除聚合集合
      // Delete aggregation set
      queue_data.aggregation_sets.remove(&set_key);

      // 删除相关任务
      // Delete related tasks
      for task_id in task_ids_to_delete {
        let task_key = MemoryStorage::task_key(queue, &task_id);
        storage.tasks.remove(&task_key);
      }
    }

    Ok(())
  }

  /// 回收过期的聚合集合
  /// Reclaim stale aggregation sets
  async fn reclaim_stale_aggregation_sets(&self, queue: &str) -> Result<()> {
    let now = Utc::now().timestamp();
    let mut storage = self.storage.write().await;

    if let Some(queue_data) = storage.queue_data.get_mut(queue) {
      // 找出过期的聚合集合
      // Find expired aggregation sets
      let expired_sets: Vec<String> = queue_data
        .aggregation_sets
        .iter()
        .filter(
          |(_, s): &(&String, &crate::memdb::memory_broker::AggregationSet)| s.expires_at < now,
        )
        .map(|(k, _): (&String, &crate::memdb::memory_broker::AggregationSet)| k.clone())
        .collect();

      // 移除过期的聚合集合
      // Remove expired aggregation sets
      for set_key in expired_sets {
        queue_data.aggregation_sets.remove(&set_key);
      }
    }

    Ok(())
  }

  /// 删除过期的已完成任务
  /// Delete expired completed tasks
  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64> {
    let now = Utc::now().timestamp();
    let mut deleted_count: i64 = 0;

    let mut storage = self.storage.write().await;

    // First collect expired entries and task keys
    let expired_entries: Vec<(i64, Vec<String>)>;
    let task_keys_to_remove: Vec<String>;

    if let Some(queue_data) = storage.queue_data.get(queue) {
      // 找出过期的已完成任务
      // Find expired completed tasks
      expired_entries = queue_data
        .completed
        .iter()
        .filter(|(&score, _): &(&i64, &Vec<String>)| score < now)
        .map(|(&score, ids): (&i64, &Vec<String>)| (score, ids.clone()))
        .collect();

      task_keys_to_remove = expired_entries
        .iter()
        .flat_map(|(_, task_ids)| {
          task_ids
            .iter()
            .map(|task_id| MemoryStorage::task_key(queue, task_id))
        })
        .collect();
    } else {
      return Ok(0);
    }

    // Remove tasks
    for task_key in &task_keys_to_remove {
      storage.tasks.remove(task_key);
      deleted_count += 1;
    }

    // Remove from completed queue
    if let Some(queue_data) = storage.queue_data.get_mut(queue) {
      for (score, _) in &expired_entries {
        queue_data.completed.remove(score);
      }
    }

    Ok(deleted_count)
  }

  /// 列出租约已过期的任务
  /// List tasks with expired leases
  async fn list_lease_expired(
    &self,
    cutoff: DateTime<Utc>,
    queues: &[String],
  ) -> Result<Vec<TaskMessage>> {
    let cutoff_timestamp = cutoff.timestamp();
    let storage = self.storage.read().await;
    let mut expired_tasks = Vec::new();

    for queue in queues {
      if let Some(queue_data) = storage.queue_data.get(queue) {
        for (task_id, &lease_expiry) in &queue_data.lease {
          if lease_expiry <= cutoff_timestamp {
            let task_key = MemoryStorage::task_key(queue, task_id);
            if let Some(task_data) = storage.tasks.get(&task_key) {
              expired_tasks.push(task_data.message.clone());
            }
          }
        }
      }
    }

    Ok(expired_tasks)
  }

  /// 延长任务处理租约
  /// Extend task lease
  async fn extend_lease(&self, queue: &str, task_id: &str, lease_duration: Duration) -> Result<()> {
    let now = Utc::now();
    let new_expiry = now.timestamp() + lease_duration.as_secs() as i64;

    let mut storage = self.storage.write().await;

    if let Some(queue_data) = storage.queue_data.get_mut(queue) {
      if queue_data.lease.contains_key(task_id) {
        queue_data.lease.insert(task_id.to_string(), new_expiry);
      }
    }

    Ok(())
  }

  /// 写入服务器状态
  /// Write server state
  async fn write_server_state(&self, server_info: &ServerInfo, ttl: Duration) -> Result<()> {
    let server_key = format!(
      "{}:{}:{}",
      server_info.host, server_info.pid, server_info.server_id
    );
    let expires_at = Utc::now().timestamp() + ttl.as_secs() as i64;
    let info = server_info.encode_to_vec();

    let mut storage = self.storage.write().await;
    storage.servers.insert(
      server_key,
      crate::memdb::memory_broker::ServerStateData { info, expires_at },
    );

    Ok(())
  }

  /// 清除服务器状态
  /// Clear server state
  async fn clear_server_state(&self, host: &str, pid: i32, server_id: &str) -> Result<()> {
    let server_key = format!("{host}:{pid}:{server_id}");

    let mut storage = self.storage.write().await;
    storage.servers.remove(&server_key);

    Ok(())
  }

  /// 订阅任务取消事件
  /// Subscribe to task cancellation events
  async fn cancellation_pub_sub(
    &self,
  ) -> Result<Box<dyn futures::Stream<Item = Result<String>> + Unpin + Send>> {
    use futures::stream::unfold;

    let receiver = self.cancel_tx.subscribe();

    // Create a stream from the broadcast receiver, filtering out errors
    let stream = unfold(receiver, |mut rx| async move {
      loop {
        match rx.recv().await {
          Ok(task_id) => return Some((Ok(task_id), rx)),
          Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
            // Skip lagged messages, try again
            continue;
          }
          Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
        }
      }
    });

    Ok(Box::new(Box::pin(stream)))
  }

  /// 发布任务取消通知
  /// Publish task cancellation notification
  async fn publish_cancellation(&self, task_id: &str) -> Result<()> {
    // 忽略发送失败（没有订阅者时）
    // Ignore send failure (when there are no subscribers)
    let _ = self.cancel_tx.send(task_id.to_string());
    Ok(())
  }

  /// 写入任务结果
  /// Write task result
  async fn write_result(&self, queue: &str, task_id: &str, result: &[u8]) -> Result<()> {
    let result_key = MemoryStorage::task_key(queue, task_id);

    let mut storage = self.storage.write().await;
    storage.results.insert(result_key, result.to_vec());

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::task::Task;

  #[tokio::test]
  async fn test_memory_broker_ping() {
    let broker = MemoryBroker::new();
    assert!(broker.ping().await.is_ok());
  }

  #[tokio::test]
  async fn test_memory_broker_enqueue() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:task", b"test payload").unwrap();

    let result = broker.enqueue(&task).await;
    assert!(result.is_ok());

    let task_info = result.unwrap();
    assert_eq!(task_info.task_type, "test:task");
    assert_eq!(task_info.state, TaskState::Pending);
  }

  #[tokio::test]
  async fn test_memory_broker_enqueue_unique() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:unique", b"payload").unwrap();

    // First enqueue should succeed
    let result1 = broker.enqueue_unique(&task, Duration::from_secs(60)).await;
    assert!(result1.is_ok());

    // Second enqueue with same payload should fail
    let result2 = broker.enqueue_unique(&task, Duration::from_secs(60)).await;
    assert!(matches!(result2, Err(Error::TaskDuplicate)));
  }

  #[tokio::test]
  async fn test_memory_broker_dequeue() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:dequeue", b"payload")
      .unwrap()
      .with_queue("test_queue");

    // Enqueue a task
    broker.enqueue(&task).await.unwrap();

    // Dequeue the task
    let result = broker.dequeue(&["test_queue".to_string()]).await;
    assert!(result.is_ok());

    let msg = result.unwrap();
    assert!(msg.is_some());
    assert_eq!(msg.unwrap().r#type, "test:dequeue");
  }

  #[tokio::test]
  async fn test_memory_broker_done() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:done", b"payload")
      .unwrap()
      .with_queue("test_queue");

    // Enqueue and dequeue
    broker.enqueue(&task).await.unwrap();
    let msg = broker
      .dequeue(&["test_queue".to_string()])
      .await
      .unwrap()
      .unwrap();

    // Mark as done
    let result = broker.done(&msg).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_memory_broker_schedule() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:schedule", b"payload").unwrap();
    let process_at = Utc::now() + chrono::Duration::hours(1);

    let result = broker.schedule(&task, process_at).await;
    assert!(result.is_ok());

    let task_info = result.unwrap();
    assert_eq!(task_info.state, TaskState::Scheduled);
  }

  #[tokio::test]
  async fn test_memory_broker_retry() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:retry", b"payload")
      .unwrap()
      .with_queue("test_queue");

    // Enqueue and dequeue
    broker.enqueue(&task).await.unwrap();
    let msg = broker
      .dequeue(&["test_queue".to_string()])
      .await
      .unwrap()
      .unwrap();

    // Retry
    let process_at = Utc::now() + chrono::Duration::minutes(5);
    let result = broker.retry(&msg, process_at, "test error", true).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_memory_broker_forward_if_ready() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:forward", b"payload")
      .unwrap()
      .with_queue("test_queue");

    // Schedule a task for now
    let process_at = Utc::now() - chrono::Duration::seconds(1);
    broker.schedule(&task, process_at).await.unwrap();

    // Forward ready tasks
    let forwarded = broker.forward_if_ready(&["test_queue".to_string()]).await;
    assert!(forwarded.is_ok());
    assert_eq!(forwarded.unwrap(), 1);

    // Now we should be able to dequeue
    let msg = broker.dequeue(&["test_queue".to_string()]).await.unwrap();
    assert!(msg.is_some());
  }

  #[tokio::test]
  async fn test_memory_broker_add_to_group() {
    let broker = MemoryBroker::new();
    let task = Task::new("test:group", b"payload").unwrap();

    let result = broker.add_to_group(&task, "test_group").await;
    assert!(result.is_ok());

    let task_info = result.unwrap();
    assert_eq!(task_info.state, TaskState::Aggregating);
    assert_eq!(task_info.group, Some("test_group".to_string()));
  }

  #[tokio::test]
  async fn test_memory_broker_list_groups() {
    let broker = MemoryBroker::new();
    let task1 = Task::new("test:group1", b"payload1")
      .unwrap()
      .with_queue("test_queue");
    let task2 = Task::new("test:group2", b"payload2")
      .unwrap()
      .with_queue("test_queue");

    broker.add_to_group(&task1, "group_a").await.unwrap();
    broker.add_to_group(&task2, "group_b").await.unwrap();

    let groups = broker.list_groups("test_queue").await.unwrap();
    assert_eq!(groups.len(), 2);
    assert!(groups.contains(&"group_a".to_string()));
    assert!(groups.contains(&"group_b".to_string()));
  }

  #[tokio::test]
  async fn test_memory_broker_cancellation() {
    let broker = MemoryBroker::new();

    // Publish cancellation
    let result = broker.publish_cancellation("task_123").await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_memory_broker_write_result() {
    let broker = MemoryBroker::new();

    let result = broker
      .write_result("test_queue", "task_123", b"result data")
      .await;
    assert!(result.is_ok());

    // Verify result is stored
    let storage = broker.storage.read().await;
    let result_key = MemoryStorage::task_key("test_queue", "task_123");
    assert!(storage.results.contains_key(&result_key));
  }

  #[tokio::test]
  async fn test_memory_broker_server_state() {
    let broker = MemoryBroker::new();

    let server_info = ServerInfo {
      host: "localhost".to_string(),
      pid: 1234,
      server_id: "server_1".to_string(),
      ..Default::default()
    };

    // Write server state
    let result = broker
      .write_server_state(&server_info, Duration::from_secs(60))
      .await;
    assert!(result.is_ok());

    // Clear server state
    let result = broker
      .clear_server_state("localhost", 1234, "server_1")
      .await;
    assert!(result.is_ok());
  }
}
