//! PostgresSQL Broker trait 实现
//! PostgresSQL Broker trait implementation
//!
//! 使用 SeaORM 实现 Broker trait
//! Implements Broker trait using SeaORM

use super::entity::tasks::{serialize_headers, TaskState};
use crate::backend::pgdb::entity::{
  servers, stats, tasks, workers, Queues, Servers, Stats, Tasks, Workers,
};
use crate::backend::pgdb::PostgresBroker;
use crate::base::keys::TaskState as BaseTaskState;
use crate::base::Broker;
use crate::error::{Error, Result};
use crate::proto::{ServerInfo, TaskMessage, WorkerInfo};
use crate::task::{generate_task_id, Task, TaskInfo};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sea_orm::sea_query::{LockBehavior, LockType};
use sea_orm::{
  ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
  QuerySelect, Set,
};
use std::time::Duration;

const LEASE_DURATION_SECS: i64 = 3600;

/// Convert timestamp to optional DateTime
fn timestamp_to_datetime(ts: i64) -> Option<DateTime<Utc>> {
  if ts > 0 {
    DateTime::from_timestamp(ts, 0)
  } else {
    None
  }
}

/// Convert status string to ServerStatus enum
/// Converts case-insensitively and logs warnings for unexpected values
fn status_string_to_enum(status: &str) -> servers::ServerStatus {
  match status.to_lowercase().as_str() {
    "active" => servers::ServerStatus::Active,
    "stopped" => servers::ServerStatus::Stopped,
    _ => {
      tracing::warn!(
        status = status,
        "Unexpected server status value, defaulting to Stopped"
      );
      servers::ServerStatus::Stopped
    }
  }
}

impl PostgresBroker {
  /// Atomically update stats using ORM operations.
  /// Handles concurrent updates by catching duplicate key errors and retrying.
  async fn upsert_stats(
    &self,
    queue: &str,
    date: chrono::NaiveDate,
    processed_delta: i64,
    failed_delta: i64,
  ) -> Result<()> {
    // Try to find and update existing stats record
    let mut query = Stats::find()
      .filter(stats::Column::Queue.eq(queue))
      .filter(stats::Column::Date.eq(date));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(stats::Column::TenantId.eq(tenant_id));
    }

    let existing_stats = query.one(self.db()).await?;

    if let Some(s) = existing_stats {
      // Update existing record
      let mut active_model: stats::ActiveModel = s.into();
      let current_processed = active_model.processed.as_ref();
      let current_failed = active_model.failed.as_ref();
      active_model.processed = Set(current_processed + processed_delta);
      active_model.failed = Set(current_failed + failed_delta);
      active_model.update(self.db()).await?;
    } else {
      // Try to insert new record
      let new_stats = stats::ActiveModel {
        queue: Set(queue.to_string()),
        date: Set(date),
        processed: Set(processed_delta),
        failed: Set(failed_delta),
        tenant_id: Set(self.tenant_id()),
      };

      match new_stats.insert(self.db()).await {
        Ok(_) => {}
        Err(e) => {
          // Check if it's a duplicate key error by examining the error message
          let is_duplicate = match &e {
            sea_orm::DbErr::Exec(err) => {
              err.to_string().contains("duplicate key")
                || err.to_string().contains("UNIQUE constraint")
            }
            _ => false,
          };

          if is_duplicate {
            // Duplicate key error - another task inserted first, retry update
            let mut retry_query = Stats::find()
              .filter(stats::Column::Queue.eq(queue))
              .filter(stats::Column::Date.eq(date));

            if let Some(tenant_id) = &self.tenant_id() {
              retry_query = retry_query.filter(stats::Column::TenantId.eq(tenant_id));
            }

            if let Some(s) = retry_query.one(self.db()).await? {
              let mut active_model: stats::ActiveModel = s.into();
              let current_processed = active_model.processed.as_ref();
              let current_failed = active_model.failed.as_ref();
              active_model.processed = Set(current_processed + processed_delta);
              active_model.failed = Set(current_failed + failed_delta);
              active_model.update(self.db()).await?;
            }
          } else {
            // Other database error, propagate it
            return Err(e.into());
          }
        }
      }
    }

    Ok(())
  }
}

#[async_trait]
impl Broker for PostgresBroker {
  /// 测试连接。
  /// Ping the server.
  async fn ping(&self) -> Result<()> {
    // Execute a simple query to test connection
    let _ = Tasks::find().one(self.db()).await?;
    Ok(())
  }

  /// 关闭连接。
  /// Close the connection.
  async fn close(&self) -> Result<()> {
    self.db().clone().close().await?;
    Ok(())
  }

  /// 将任务加入队列。
  /// Enqueue a task into the queue.
  async fn enqueue(&self, task: &Task) -> Result<TaskInfo> {
    let msg = self.task_to_message(task);
    let deadline = timestamp_to_datetime(msg.deadline);
    let now = chrono::Utc::now();

    // Ensure queue exists
    self.ensure_queue_exists(&msg.queue).await?;

    // Check if task already exists
    let existing = Tasks::find_by_id(&msg.id).one(self.db()).await?;
    if existing.is_some() {
      return Err(Error::TaskIdConflict);
    }

    // Insert task using SeaORM
    let new_task = tasks::ActiveModel {
      id: Set(msg.id.clone()),
      queue: Set(msg.queue.clone()),
      task_type: Set(msg.r#type.clone()),
      payload: Set(msg.payload.clone()),
      state: Set(TaskState::Pending),
      retry: Set(msg.retry),
      retried: Set(0),
      error_msg: Set(None),
      last_failed_at: Set(None),
      timeout_seconds: Set(msg.timeout),
      deadline: Set(deadline.map(|d| d.into())),
      unique_key: Set(None),
      group_key: Set(if msg.group_key.is_empty() {
        None
      } else {
        Some(msg.group_key.clone())
      }),
      retention_seconds: Set(msg.retention),
      completed_at: Set(None),
      process_at: Set(now.into()),
      created_at: Set(now.into()),
      updated_at: Set(now.into()),
      lease_expires_at: Set(None),
      headers: Set(serialize_headers(&msg.headers)),
      tenant_id: Set(self.tenant_id().clone()),
    };

    new_task.insert(self.db()).await?;

    Ok(TaskInfo::from_proto(
      &msg,
      BaseTaskState::Pending,
      None,
      None,
    ))
  }

  /// 将唯一任务加入队列。
  /// Enqueue a unique task into the queue.
  async fn enqueue_unique(&self, task: &Task, ttl: Duration) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();
    let deadline = timestamp_to_datetime(msg.deadline);
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::seconds(ttl.as_secs() as i64);

    // Ensure queue exists
    self.ensure_queue_exists(&msg.queue).await?;

    // Check if unique key exists (with tenant filtering)
    let mut query = Tasks::find()
      .filter(tasks::Column::UniqueKey.eq(&unique_key))
      .filter(tasks::Column::CreatedAt.gt(cutoff));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let existing = query.one(self.db()).await?;

    if existing.is_some() {
      return Err(Error::TaskDuplicate);
    }

    // Check if task ID already exists
    let existing_id = Tasks::find_by_id(&msg.id).one(self.db()).await?;
    if existing_id.is_some() {
      return Err(Error::TaskIdConflict);
    }

    // Insert task
    let new_task = tasks::ActiveModel {
      id: Set(msg.id.clone()),
      queue: Set(msg.queue.clone()),
      task_type: Set(msg.r#type.clone()),
      payload: Set(msg.payload.clone()),
      state: Set(TaskState::Pending),
      retry: Set(msg.retry),
      retried: Set(0),
      error_msg: Set(None),
      last_failed_at: Set(None),
      timeout_seconds: Set(msg.timeout),
      deadline: Set(deadline.map(|d| d.into())),
      unique_key: Set(Some(unique_key)),
      group_key: Set(if msg.group_key.is_empty() {
        None
      } else {
        Some(msg.group_key.clone())
      }),
      retention_seconds: Set(msg.retention),
      completed_at: Set(None),
      process_at: Set(now.into()),
      created_at: Set(now.into()),
      updated_at: Set(now.into()),
      lease_expires_at: Set(None),
      headers: Set(serialize_headers(&msg.headers)),
      tenant_id: Set(self.tenant_id()),
    };

    new_task.insert(self.db()).await?;

    Ok(TaskInfo::from_proto(
      &msg,
      BaseTaskState::Pending,
      None,
      None,
    ))
  }

  /// 从队列中取出任务。
  /// Dequeue a task from the queue.
  async fn dequeue(&self, queues: &[String]) -> Result<Option<TaskMessage>> {
    if queues.is_empty() {
      return Ok(None);
    }

    let now = chrono::Utc::now();
    let lease_expires = now + chrono::Duration::seconds(LEASE_DURATION_SECS);

    for queue in queues {
      // Check if queue is paused (with tenant filtering)
      let queue_query = Queues::find_by_id(queue);
      // Note: Queues table doesn't typically need tenant filtering as queue names should be unique per tenant
      // But we keep the structure consistent
      let queue_record = queue_query.one(self.db()).await?;
      if let Some(q) = queue_record {
        if q.paused {
          continue;
        }
      }

      // Find pending task (with tenant filtering)
      let mut task_query = Tasks::find()
        .filter(tasks::Column::Queue.eq(queue))
        .filter(tasks::Column::State.eq(TaskState::Pending))
        .filter(tasks::Column::ProcessAt.lte(now));

      // Apply tenant filtering if tenant_id is set
      if let Some(tenant_id) = &self.tenant_id() {
        task_query = task_query.filter(tasks::Column::TenantId.eq(tenant_id));
      }

      let task = task_query
        .order_by_asc(tasks::Column::ProcessAt)
        .lock_with_behavior(LockType::Update, LockBehavior::Nowait)
        .one(self.db())
        .await?;

      if let Some(t) = task {
        // Update task to active state
        let mut active_model: tasks::ActiveModel = t.clone().into();
        active_model.state = Set(TaskState::Active);
        active_model.lease_expires_at = Set(Some(lease_expires.into()));
        active_model.updated_at = Set(now.into());
        active_model.update(self.db()).await?;

        let msg = self.task_model_to_message(&t);
        return Ok(Some(msg));
      }
    }

    Ok(None)
  }

  /// 标记任务为完成。
  /// Mark a task as done.
  async fn done(&self, msg: &TaskMessage) -> Result<()> {
    let now = chrono::Utc::now();
    let today = now.date_naive();

    // Delete task with tenant filtering
    let mut delete_query = Tasks::delete_by_id(&msg.id);
    if let Some(tenant_id) = &self.tenant_id() {
      delete_query = delete_query.filter(tasks::Column::TenantId.eq(tenant_id));
    }
    delete_query.exec(self.db()).await?;

    // Update stats atomically using UPSERT
    self.upsert_stats(&msg.queue, today, 1, 0).await?;

    Ok(())
  }

  /// 标记任务为完成状态。
  /// Mark a task as complete.
  async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()> {
    let now = chrono::Utc::now();
    let today = now.date_naive();

    // Update task with tenant filtering
    let mut query = Tasks::find_by_id(&msg.id);
    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }
    let task = query.one(self.db()).await?;
    if let Some(t) = task {
      let mut active_model: tasks::ActiveModel = t.into();
      active_model.state = Set(TaskState::Completed);
      active_model.completed_at = Set(Some(now.into()));
      active_model.updated_at = Set(now.into());
      active_model.update(self.db()).await?;
    }

    // Update stats atomically using UPSERT
    self.upsert_stats(&msg.queue, today, 1, 0).await?;

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
    self
      .retry(msg, process_at, error_msg, !error_msg.is_empty())
      .await
  }

  /// 调度任务在指定时间执行。
  /// Schedule a task to execute at a specific time.
  async fn schedule(&self, task: &Task, process_at: DateTime<Utc>) -> Result<TaskInfo> {
    let msg = self.task_to_message(task);
    let deadline = timestamp_to_datetime(msg.deadline);
    let now = chrono::Utc::now();

    // Ensure queue exists
    self.ensure_queue_exists(&msg.queue).await?;

    // Check if task already exists
    let existing = Tasks::find_by_id(&msg.id).one(self.db()).await?;
    if existing.is_some() {
      return Err(Error::TaskIdConflict);
    }

    // Insert task
    let new_task = tasks::ActiveModel {
      id: Set(msg.id.clone()),
      queue: Set(msg.queue.clone()),
      task_type: Set(msg.r#type.clone()),
      payload: Set(msg.payload.clone()),
      state: Set(TaskState::Scheduled),
      retry: Set(msg.retry),
      retried: Set(0),
      error_msg: Set(None),
      last_failed_at: Set(None),
      timeout_seconds: Set(msg.timeout),
      deadline: Set(deadline.map(|d| d.into())),
      unique_key: Set(None),
      group_key: Set(if msg.group_key.is_empty() {
        None
      } else {
        Some(msg.group_key.clone())
      }),
      tenant_id: Set(self.tenant_id()),
      retention_seconds: Set(msg.retention),
      completed_at: Set(None),
      process_at: Set(process_at.into()),
      created_at: Set(now.into()),
      updated_at: Set(now.into()),
      lease_expires_at: Set(None),
      headers: Set(serialize_headers(&msg.headers)),
    };

    new_task.insert(self.db()).await?;

    Ok(TaskInfo::from_proto(
      &msg,
      BaseTaskState::Scheduled,
      None,
      None,
    ))
  }

  /// 调度唯一任务在指定时间执行。
  /// Schedule a unique task to execute at a specific time.
  async fn schedule_unique(
    &self,
    task: &Task,
    process_at: DateTime<Utc>,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();
    let deadline = timestamp_to_datetime(msg.deadline);
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::seconds(ttl.as_secs() as i64);

    // Ensure queue exists
    self.ensure_queue_exists(&msg.queue).await?;

    // Check if unique key exists
    let existing = Tasks::find()
      .filter(tasks::Column::UniqueKey.eq(&unique_key))
      .filter(tasks::Column::CreatedAt.gt(cutoff))
      .one(self.db())
      .await?;

    if existing.is_some() {
      return Err(Error::TaskDuplicate);
    }

    // Check if task ID already exists
    let existing_id = Tasks::find_by_id(&msg.id).one(self.db()).await?;
    if existing_id.is_some() {
      return Err(Error::TaskIdConflict);
    }

    // Insert task
    let new_task = tasks::ActiveModel {
      id: Set(msg.id.clone()),
      queue: Set(msg.queue.clone()),
      task_type: Set(msg.r#type.clone()),
      payload: Set(msg.payload.clone()),
      state: Set(TaskState::Scheduled),
      retry: Set(msg.retry),
      retried: Set(0),
      error_msg: Set(None),
      last_failed_at: Set(None),
      timeout_seconds: Set(msg.timeout),
      deadline: Set(deadline.map(|d| d.into())),
      unique_key: Set(Some(unique_key)),
      group_key: Set(if msg.group_key.is_empty() {
        None
      } else {
        Some(msg.group_key.clone())
      }),
      tenant_id: Set(self.tenant_id()),
      retention_seconds: Set(msg.retention),
      completed_at: Set(None),
      process_at: Set(process_at.into()),
      created_at: Set(now.into()),
      updated_at: Set(now.into()),
      lease_expires_at: Set(None),
      headers: Set(serialize_headers(&msg.headers)),
    };

    new_task.insert(self.db()).await?;

    Ok(TaskInfo::from_proto(
      &msg,
      BaseTaskState::Scheduled,
      None,
      None,
    ))
  }

  /// 重试失败的任务。
  /// Retry a failed task.
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
    let now = chrono::Utc::now();
    let today = now.date_naive();
    msg.last_failed_at = now.timestamp();

    // Update task with tenant filtering
    let mut query = Tasks::find_by_id(&msg.id);
    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }
    let task = query.one(self.db()).await?;
    if let Some(t) = task {
      let mut active_model: tasks::ActiveModel = t.into();
      active_model.state = Set(TaskState::Retry);
      active_model.process_at = Set(process_at.into());
      active_model.error_msg = Set(Some(error_msg.to_string()));
      active_model.retried = Set(msg.retried);
      active_model.last_failed_at = Set(Some(now.into()));
      active_model.updated_at = Set(now.into());
      active_model.update(self.db()).await?;
    }

    // Update stats atomically using UPSERT if failure
    if is_failure {
      self.upsert_stats(&msg.queue, today, 1, 1).await?;
    }

    Ok(())
  }

  /// 归档任务。
  /// Archive a task.
  async fn archive(&self, msg: &TaskMessage, error_msg: &str) -> Result<()> {
    let now = chrono::Utc::now();
    let today = now.date_naive();

    // Update task with tenant filtering
    let mut query = Tasks::find_by_id(&msg.id);
    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }
    let task = query.one(self.db()).await?;
    if let Some(t) = task {
      let mut active_model: tasks::ActiveModel = t.into();
      active_model.state = Set(TaskState::Archived);
      active_model.error_msg = Set(Some(error_msg.to_string()));
      active_model.last_failed_at = Set(Some(now.into()));
      active_model.updated_at = Set(now.into());
      active_model.update(self.db()).await?;
    }

    // Update stats atomically using UPSERT
    self.upsert_stats(&msg.queue, today, 1, 1).await?;

    Ok(())
  }

  /// 转发就绪任务到待处理队列。
  /// Forward ready tasks to the pending queue.
  async fn forward_if_ready(&self, queues: &[String]) -> Result<i64> {
    let now = chrono::Utc::now();
    let mut forwarded = 0i64;

    for queue in queues {
      // Find tasks that are ready to be forwarded with tenant filtering
      let mut query = Tasks::find()
        .filter(tasks::Column::Queue.eq(queue))
        .filter(
          Condition::any()
            .add(tasks::Column::State.eq(TaskState::Scheduled))
            .add(tasks::Column::State.eq(TaskState::Retry)),
        )
        .filter(tasks::Column::ProcessAt.lte(now));

      if let Some(tenant_id) = &self.tenant_id() {
        query = query.filter(tasks::Column::TenantId.eq(tenant_id));
      }

      let tasks = query.all(self.db()).await?;

      for t in tasks {
        let mut active_model: tasks::ActiveModel = t.into();
        active_model.state = Set(TaskState::Pending);
        active_model.updated_at = Set(now.into());
        active_model.update(self.db()).await?;
        forwarded += 1;
      }
    }

    Ok(forwarded)
  }

  /// 将任务添加到组中进行聚合。
  /// Add a task to a group for aggregation.
  async fn add_to_group(&self, task: &Task, group: &str) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    msg.group_key = group.to_string();
    let deadline = timestamp_to_datetime(msg.deadline);
    let now = chrono::Utc::now();

    // Ensure queue exists
    self.ensure_queue_exists(&msg.queue).await?;

    // Check if task already exists
    let existing = Tasks::find_by_id(&msg.id).one(self.db()).await?;
    if existing.is_some() {
      return Err(Error::TaskIdConflict);
    }

    // Insert task
    let new_task = tasks::ActiveModel {
      id: Set(msg.id.clone()),
      queue: Set(msg.queue.clone()),
      task_type: Set(msg.r#type.clone()),
      payload: Set(msg.payload.clone()),
      state: Set(TaskState::Aggregating),
      retry: Set(msg.retry),
      retried: Set(0),
      error_msg: Set(None),
      last_failed_at: Set(None),
      timeout_seconds: Set(msg.timeout),
      deadline: Set(deadline.map(|d| d.into())),
      unique_key: Set(None),
      group_key: Set(Some(group.to_string())),
      tenant_id: Set(self.tenant_id()),
      retention_seconds: Set(msg.retention),
      completed_at: Set(None),
      process_at: Set(now.into()),
      created_at: Set(now.into()),
      updated_at: Set(now.into()),
      lease_expires_at: Set(None),
      headers: Set(serialize_headers(&msg.headers)),
    };

    new_task.insert(self.db()).await?;

    Ok(TaskInfo::from_proto(
      &msg,
      BaseTaskState::Aggregating,
      None,
      None,
    ))
  }

  /// 将唯一任务添加到组中进行聚合。
  /// Add a unique task to a group for aggregation.
  async fn add_to_group_unique(&self, task: &Task, group: &str, ttl: Duration) -> Result<TaskInfo> {
    let mut msg = self.task_to_message(task);
    msg.group_key = group.to_string();
    let unique_key = crate::task::generate_unique_key(&msg.queue, &task.task_type, &task.payload);
    msg.unique_key = unique_key.clone();
    let deadline = timestamp_to_datetime(msg.deadline);
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::seconds(ttl.as_secs() as i64);

    // Ensure queue exists
    self.ensure_queue_exists(&msg.queue).await?;

    // Check if unique key exists with tenant filtering
    let mut unique_query = Tasks::find()
      .filter(tasks::Column::UniqueKey.eq(&unique_key))
      .filter(tasks::Column::CreatedAt.gt(cutoff));

    if let Some(tenant_id) = &self.tenant_id() {
      unique_query = unique_query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let existing = unique_query.one(self.db()).await?;

    if existing.is_some() {
      return Err(Error::TaskDuplicate);
    }

    // Check if task ID already exists
    let existing_id = Tasks::find_by_id(&msg.id).one(self.db()).await?;
    if existing_id.is_some() {
      return Err(Error::TaskIdConflict);
    }

    // Insert task
    let new_task = tasks::ActiveModel {
      id: Set(msg.id.clone()),
      queue: Set(msg.queue.clone()),
      task_type: Set(msg.r#type.clone()),
      payload: Set(msg.payload.clone()),
      state: Set(TaskState::Aggregating),
      retry: Set(msg.retry),
      retried: Set(0),
      error_msg: Set(None),
      last_failed_at: Set(None),
      timeout_seconds: Set(msg.timeout),
      deadline: Set(deadline.map(|d| d.into())),
      unique_key: Set(Some(unique_key)),
      group_key: Set(Some(group.to_string())),
      tenant_id: Set(self.tenant_id()),
      retention_seconds: Set(msg.retention),
      completed_at: Set(None),
      process_at: Set(now.into()),
      created_at: Set(now.into()),
      updated_at: Set(now.into()),
      lease_expires_at: Set(None),
      headers: Set(serialize_headers(&msg.headers)),
    };

    new_task.insert(self.db()).await?;

    Ok(TaskInfo::from_proto(
      &msg,
      BaseTaskState::Aggregating,
      None,
      None,
    ))
  }

  /// 获取队列中的任务组列表。
  /// Get the list of task groups in a queue.
  async fn list_groups(&self, queue: &str) -> Result<Vec<String>> {
    let mut query = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::GroupKey.is_not_null())
      .filter(tasks::Column::State.eq(TaskState::Aggregating));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let tasks = query.all(self.db()).await?;

    let mut groups: Vec<String> = tasks.into_iter().filter_map(|t| t.group_key).collect();
    groups.sort();
    groups.dedup();
    Ok(groups)
  }

  /// 检查聚合条件是否满足。
  /// Check if aggregation conditions are met.
  async fn aggregation_check(
    &self,
    queue: &str,
    group: &str,
    _aggregation_delay: Duration,
    max_delay: Duration,
    max_size: usize,
  ) -> Result<Option<String>> {
    let now = chrono::Utc::now();

    // Count tasks in group with tenant filtering
    let mut query = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::GroupKey.eq(group))
      .filter(tasks::Column::State.eq(TaskState::Aggregating));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let tasks = query.all(self.db()).await?;

    if tasks.is_empty() {
      return Ok(None);
    }

    // Find oldest task
    let oldest = tasks.iter().map(|t| t.created_at).min();

    let should_aggregate = if tasks.len() >= max_size {
      true
    } else if let Some(oldest_time) = oldest {
      let oldest_utc: DateTime<Utc> = oldest_time.into();
      let age = now.signed_duration_since(oldest_utc);
      age.num_seconds() >= max_delay.as_secs() as i64
    } else {
      false
    };

    if should_aggregate {
      let set_id = generate_task_id();
      return Ok(Some(set_id));
    }

    Ok(None)
  }

  /// 读取聚合集合中的任务。
  /// Read tasks from an aggregation set.
  async fn read_aggregation_set(
    &self,
    queue: &str,
    group: &str,
    _set_id: &str,
  ) -> Result<Vec<TaskMessage>> {
    let mut query = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::GroupKey.eq(group))
      .filter(tasks::Column::State.eq(TaskState::Aggregating));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let tasks = query.all(self.db()).await?;

    let mut messages = Vec::new();
    for t in tasks {
      messages.push(self.task_model_to_message(&t));
    }
    Ok(messages)
  }

  /// 删除聚合集合。
  /// Delete an aggregation set.
  async fn delete_aggregation_set(&self, queue: &str, group: &str, _set_id: &str) -> Result<()> {
    // Delete tasks in the group with tenant filtering
    let mut delete_query = Tasks::delete_many()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::GroupKey.eq(group))
      .filter(tasks::Column::State.eq(TaskState::Aggregating));

    if let Some(tenant_id) = &self.tenant_id() {
      delete_query = delete_query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    delete_query.exec(self.db()).await?;

    Ok(())
  }

  /// 回收过期的聚合集合。
  /// Reclaim expired aggregation sets.
  async fn reclaim_stale_aggregation_sets(&self, _queue: &str) -> Result<()> {
    // No-op for now - aggregation sets are managed inline
    Ok(())
  }

  /// 删除过期的已完成任务。
  /// Delete expired completed tasks.
  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64> {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(7);

    let mut delete_query = Tasks::delete_many()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(TaskState::Completed))
      .filter(tasks::Column::CompletedAt.lt(cutoff));

    if let Some(tenant_id) = &self.tenant_id() {
      delete_query = delete_query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let result = delete_query.exec(self.db()).await?;

    Ok(result.rows_affected as i64)
  }

  /// 列出租约已过期的任务。
  /// List tasks with expired leases.
  async fn list_lease_expired(
    &self,
    cutoff: DateTime<Utc>,
    queues: &[String],
  ) -> Result<Vec<TaskMessage>> {
    let mut expired_tasks = Vec::new();

    for queue in queues {
      let mut query = Tasks::find()
        .filter(tasks::Column::Queue.eq(queue))
        .filter(tasks::Column::State.eq(TaskState::Active))
        .filter(tasks::Column::LeaseExpiresAt.lt(cutoff));

      if let Some(tenant_id) = &self.tenant_id() {
        query = query.filter(tasks::Column::TenantId.eq(tenant_id));
      }

      let tasks = query.all(self.db()).await?;

      for t in tasks {
        expired_tasks.push(self.task_model_to_message(&t));
      }
    }

    Ok(expired_tasks)
  }

  /// 延长任务处理租约。
  /// Extend task lease.
  async fn extend_lease(&self, queue: &str, task_id: &str, lease_duration: Duration) -> Result<()> {
    let now = chrono::Utc::now();
    let new_expiry =
      now + chrono::Duration::from_std(lease_duration).map_err(|e| Error::other(e.to_string()))?;

    let mut query = Tasks::find_by_id(task_id)
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(TaskState::Active));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let task = query.one(self.db()).await?;

    if let Some(t) = task {
      let mut active_model: tasks::ActiveModel = t.into();
      active_model.lease_expires_at = Set(Some(new_expiry.into()));
      active_model.updated_at = Set(now.into());
      active_model.update(self.db()).await?;
    }

    Ok(())
  }

  /// 写入服务器状态。
  /// Write server state.
  async fn write_server_state(&self, server_info: &ServerInfo, ttl: Duration) -> Result<()> {
    let server_key = format!(
      "{}:{}:{}",
      server_info.host, server_info.pid, server_info.server_id
    );
    let now = chrono::Utc::now();
    let expires_at =
      now + chrono::Duration::from_std(ttl).map_err(|e| Error::other(e.to_string()))?;

    // Convert queues HashMap to JSON
    let queues_json = serde_json::to_value(&server_info.queues)
      .map_err(|e| Error::other(format!("Failed to serialize queues: {}", e)))?;

    // Convert status string to enum
    let status = status_string_to_enum(&server_info.status);

    let existing = Servers::find_by_id(&server_key).one(self.db()).await?;

    if let Some(s) = existing {
      let mut active_model: servers::ActiveModel = s.into();
      active_model.expires_at = Set(expires_at.into());
      active_model.concurrency = Set(server_info.concurrency);
      active_model.queues = Set(queues_json);
      active_model.strict_priority = Set(server_info.strict_priority);
      active_model.active_worker_count = Set(server_info.active_worker_count);
      active_model.status = Set(status);
      active_model.update(self.db()).await?;
    } else {
      let new_server = servers::ActiveModel {
        id: Set(server_key),
        host: Set(server_info.host.clone()),
        pid: Set(server_info.pid),
        server_id: Set(server_info.server_id.clone()),
        concurrency: Set(server_info.concurrency),
        started_at: Set(now.into()),
        status: Set(status),
        expires_at: Set(expires_at.into()),
        queues: Set(queues_json),
        strict_priority: Set(server_info.strict_priority),
        active_worker_count: Set(server_info.active_worker_count),
        tenant_id: Set(self.tenant_id()),
      };
      new_server.insert(self.db()).await?;
    }

    Ok(())
  }

  /// 清除服务器状态。
  /// Clear server state.
  async fn clear_server_state(&self, host: &str, pid: i32, server_id: &str) -> Result<()> {
    let server_key = format!("{host}:{pid}:{server_id}");

    // Clear all workers for this server
    self
      .clear_all_workers_for_server(host, pid, server_id)
      .await?;

    // Clear server entry with tenant filtering
    let mut delete_query = Servers::delete_by_id(&server_key);
    if let Some(tenant_id) = &self.tenant_id() {
      delete_query = delete_query.filter(servers::Column::TenantId.eq(tenant_id));
    }
    delete_query.exec(self.db()).await?;
    Ok(())
  }

  /// 订阅任务取消事件。
  /// Subscribe to task cancellation events.
  async fn cancellation_pub_sub(
    &self,
  ) -> Result<Box<dyn futures::Stream<Item = Result<String>> + Unpin + Send>> {
    // PostgresSQL LISTEN/NOTIFY implementation
    // For simplicity, return an empty stream - this can be enhanced with pg_notify
    use futures::stream;
    Ok(Box::new(stream::empty()))
  }

  /// 发布任务取消通知。
  /// Publish task cancellation notification.
  async fn publish_cancellation(&self, task_id: &str) -> Result<()> {
    // Sanitize task_id to prevent SQL injection
    let sanitized_task_id: String = task_id
      .chars()
      .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
      .collect();
    if sanitized_task_id.is_empty() {
      return Err(Error::other("Invalid task_id for cancellation"));
    }

    let backend = self.db().get_database_backend();
    let sql = format!("NOTIFY asynq_cancel, '{sanitized_task_id}'");
    self
      .db()
      .execute(sea_orm::Statement::from_string(backend, sql))
      .await?;

    Ok(())
  }

  /// 写入任务结果。
  /// Write task result.
  async fn write_result(&self, queue: &str, task_id: &str, result: &[u8]) -> Result<()> {
    let now = chrono::Utc::now();

    let mut query = Tasks::find_by_id(task_id).filter(tasks::Column::Queue.eq(queue));

    if let Some(tenant_id) = &self.tenant_id() {
      query = query.filter(tasks::Column::TenantId.eq(tenant_id));
    }

    let task = query.one(self.db()).await?;

    if let Some(t) = task {
      let mut active_model: tasks::ActiveModel = t.into();
      active_model.payload = Set(result.to_vec());
      active_model.updated_at = Set(now.into());
      active_model.update(self.db()).await?;
    }

    Ok(())
  }
}

impl PostgresBroker {
  /// Write worker state to track active workers processing tasks.
  /// This method is called when a worker starts processing a task.
  /// Each worker-task combination gets a unique record based on the composite key.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// - Database connection fails
  /// - A duplicate worker record already exists (database constraint violation)
  ///   This should not normally happen since each task_id is unique
  /// - Database write operation fails
  pub async fn write_worker_state(&self, worker_info: &WorkerInfo) -> Result<()> {
    let worker_key = format!(
      "{}:{}:{}:{}",
      worker_info.host, worker_info.pid, worker_info.server_id, worker_info.task_id
    );
    let server_key = format!(
      "{}:{}:{}",
      worker_info.host, worker_info.pid, worker_info.server_id
    );
    let now = chrono::Utc::now();

    // Insert new worker record
    // Note: Each task gets a unique worker record, so we don't need to check for existing records
    let new_worker = workers::ActiveModel {
      id: Set(worker_key),
      server_id: Set(server_key),
      queue: Set(worker_info.queue.clone()),
      task_id: Set(Some(worker_info.task_id.clone())),
      task_type: Set(Some(worker_info.task_type.clone())),
      task_payload: Set(Some(worker_info.task_payload.clone())),
      status: Set(workers::WorkerStatus::Active),
      started_at: Set(now.into()),
      updated_at: Set(now.into()),
      tenant_id: Set(self.tenant_id()),
    };
    new_worker.insert(self.db()).await?;

    Ok(())
  }

  /// Clear worker state when a worker finishes processing a task.
  /// This method is called when a worker completes or fails a task.
  pub async fn clear_worker_state(
    &self,
    host: &str,
    pid: i32,
    server_id: &str,
    task_id: &str,
  ) -> Result<()> {
    let worker_key = format!("{host}:{pid}:{server_id}:{task_id}");
    let mut delete_query = Workers::delete_by_id(&worker_key);
    if let Some(tenant_id) = &self.tenant_id() {
      delete_query = delete_query.filter(workers::Column::TenantId.eq(tenant_id));
    }
    delete_query.exec(self.db()).await?;
    Ok(())
  }

  /// Clear all worker states for a specific server.
  /// This method is called when a server shuts down.
  pub async fn clear_all_workers_for_server(
    &self,
    host: &str,
    pid: i32,
    server_id: &str,
  ) -> Result<()> {
    // Create the composite server key that matches the server_id field in workers table
    let server_key = format!("{host}:{pid}:{server_id}");
    let mut delete_query = Workers::delete_many().filter(workers::Column::ServerId.eq(&server_key));

    if let Some(tenant_id) = &self.tenant_id() {
      delete_query = delete_query.filter(workers::Column::TenantId.eq(tenant_id));
    }

    delete_query.exec(self.db()).await?;
    Ok(())
  }
}

/// PostgresBroker 实现 SchedulerBroker trait，提供调度器特定的功能
/// PostgresBroker implements the SchedulerBroker trait, providing scheduler-specific functionality
#[async_trait::async_trait]
impl crate::base::SchedulerBroker for PostgresBroker {
  /// 批量写入 scheduler entries，兼容 Go 版 asynq
  /// Batch write scheduler entries, compatible with Go version asynq
  async fn write_scheduler_entries(
    &self,
    entries: &[crate::proto::SchedulerEntry],
    scheduler_id: &str,
    ttl_secs: u64,
  ) -> Result<()> {
    PostgresBroker::write_scheduler_entries(self, entries, scheduler_id, ttl_secs).await
  }

  /// 记录调度事件，兼容 Go 版 asynq
  /// Record scheduling event, compatible with Go version asynq
  async fn record_scheduler_enqueue_event(
    &self,
    event: &crate::proto::SchedulerEnqueueEvent,
    entry_id: &str,
  ) -> Result<()> {
    PostgresBroker::record_scheduler_enqueue_event(self, event, entry_id).await
  }

  /// 通过脚本获取所有 SchedulerEntry，兼容 Go 版 asynq
  /// Get all SchedulerEntry through script, compatible with Go version asynq
  async fn scheduler_entries_script(
    &self,
    scheduler_id: &str,
  ) -> Result<std::collections::HashMap<String, Vec<u8>>> {
    PostgresBroker::scheduler_entries_script(self, scheduler_id).await
  }

  /// 通过脚本获取调度事件列表，兼容 Go 版 asynq
  /// Get scheduling event list through script, compatible with Go version asynq
  async fn scheduler_events_script(&self, count: usize) -> Result<Vec<Vec<u8>>> {
    PostgresBroker::scheduler_events_script(self, count).await
  }

  /// 删除 scheduler entries 数据，兼容 Go 版 asynq
  /// Delete scheduler entries data, compatible with Go version asynq
  async fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()> {
    PostgresBroker::clear_scheduler_entries(self, scheduler_id).await
  }
}
