//! PostgresSQL Inspector 实现
//! PostgresSQL Inspector implementation

use crate::backend::pagination::Pagination;
use crate::backend::pgdb::entity::{queues, servers, stats, tasks, Queues, Servers, Stats, Tasks};
use crate::backend::pgdb::PostgresBroker;
use crate::base::keys::TaskState;
use crate::base::Broker;
use crate::error::{Error, Result};
use crate::inspector::InspectorTrait;
use crate::proto::ServerInfo;
use crate::task::{DailyStats, QueueInfo, QueueStats, TaskInfo};
use async_trait::async_trait;
use sea_orm::{
  ActiveModelTrait, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
  Set,
};
use std::sync::Arc;
use std::time::Duration;

/// PostgresSQL 队列检查器实现
/// PostgresSQL queue inspector implementation
pub struct PostgresInspector {
  broker: Arc<PostgresBroker>,
}

impl PostgresInspector {
  /// 从 PostgresBroker 创建 Inspector
  /// Create Inspector from PostgresBroker
  pub fn from_broker(broker: Arc<PostgresBroker>) -> Self {
    Self { broker }
  }

  /// Helper to get task counts by state for a queue
  async fn get_task_counts(&self, queue: &str) -> Result<(i64, i64, i64, i64, i64, i64, i64)> {
    let pending = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Pending))
      .count(self.broker.db())
      .await? as i64;

    let active = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Active))
      .count(self.broker.db())
      .await? as i64;

    let scheduled = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Scheduled))
      .count(self.broker.db())
      .await? as i64;

    let retry = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Retry))
      .count(self.broker.db())
      .await? as i64;

    let archived = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Archived))
      .count(self.broker.db())
      .await? as i64;

    let completed = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Completed))
      .count(self.broker.db())
      .await? as i64;

    let aggregating = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Aggregating))
      .count(self.broker.db())
      .await? as i64;

    Ok((
      pending,
      active,
      scheduled,
      retry,
      archived,
      completed,
      aggregating,
    ))
  }

  /// Helper to convert task entity to TaskInfo
  fn task_to_info(&self, task: tasks::Model) -> Result<TaskInfo> {
    let msg = self.broker.task_model_to_message(&task);
    let state = match task.state {
      tasks::TaskState::Pending => TaskState::Pending,
      tasks::TaskState::Active => TaskState::Active,
      tasks::TaskState::Scheduled => TaskState::Scheduled,
      tasks::TaskState::Retry => TaskState::Retry,
      tasks::TaskState::Archived => TaskState::Archived,
      tasks::TaskState::Completed => TaskState::Completed,
      tasks::TaskState::Aggregating => TaskState::Aggregating,
    };

    let next_process_at = Some(task.process_at.into());
    let result = None; // Result would be in payload for completed tasks

    Ok(TaskInfo::from_proto(&msg, state, next_process_at, result))
  }

  /// Helper to reconstruct ServerInfo from database columns
  /// 从数据库字段重构 ServerInfo
  fn reconstruct_server_info(record: &servers::Model) -> Result<ServerInfo> {
    // Parse queues from JSON
    let queues: std::collections::HashMap<String, i32> = serde_json::from_value(record.queues.clone())
      .map_err(|e| Error::other(format!("Failed to deserialize queues: {}", e)))?;

    // Convert started_at to protobuf Timestamp
    let start_time = {
      let datetime: chrono::DateTime<chrono::Utc> = record.started_at.into();
      Some(prost_types::Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
      })
    };

    // Convert status enum to string
    let status = match record.status {
      servers::ServerStatus::Active => "active".to_string(),
      servers::ServerStatus::Stopped => "stopped".to_string(),
    };

    Ok(ServerInfo {
      host: record.host.clone(),
      pid: record.pid,
      server_id: record.server_id.clone(),
      concurrency: record.concurrency,
      queues,
      strict_priority: record.strict_priority,
      status,
      start_time,
      active_worker_count: record.active_worker_count,
    })
  }
}

#[async_trait]
impl InspectorTrait for PostgresInspector {
  async fn get_queue_stats(&self, queue: &str) -> Result<QueueStats> {
    // Get task counts using helper
    let (pending, active, scheduled, retry, archived, completed, aggregating) =
      self.get_task_counts(queue).await?;

    // Get daily stats for the last 7 days
    let daily_stats = self.get_history(queue, 7).await?;

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

  async fn get_queue_info(&self, queue: &str) -> Result<QueueInfo> {
    use sea_orm::QuerySelect;

    // Get task counts using helper
    let (pending, active, scheduled, retry, archived, completed, aggregating) =
      self.get_task_counts(queue).await?;

    // Count groups
    let groups = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::GroupKey.is_not_null())
      .filter(tasks::Column::State.eq(tasks::TaskState::Aggregating))
      .select_only()
      .column(tasks::Column::GroupKey)
      .distinct()
      .into_tuple::<Option<String>>()
      .all(self.broker.db())
      .await?
      .len() as i64;

    let size = (pending + active + scheduled + retry + archived + completed + aggregating) as i32;

    // Get stats for today
    let today = chrono::Utc::now().date_naive();
    let today_stats = Stats::find()
      .filter(stats::Column::Queue.eq(queue))
      .filter(stats::Column::Date.eq(today))
      .one(self.broker.db())
      .await?;

    let (processed, failed) = if let Some(s) = today_stats {
      (s.processed as i32, s.failed as i32)
    } else {
      (0, 0)
    };

    // Get total stats
    let total_stats: Vec<(i64, i64)> = Stats::find()
      .filter(stats::Column::Queue.eq(queue))
      .select_only()
      .column_as(stats::Column::Processed.sum(), "processed_sum")
      .column_as(stats::Column::Failed.sum(), "failed_sum")
      .into_tuple()
      .one(self.broker.db())
      .await?
      .into_iter()
      .collect();

    let (processed_total, failed_total) = total_stats.first().copied().unwrap_or((0, 0));
    let (processed_total, failed_total) = (processed_total as i32, failed_total as i32);

    // Check if queue is paused
    let paused = self.is_queue_paused(queue).await?;

    Ok(QueueInfo {
      queue: queue.to_string(),
      memory_usage: 0,                 // Not applicable for PostgresSQL
      latency: Duration::from_secs(0), // Would need to calculate from task processing times
      size,
      groups: groups as i32,
      pending: pending as i32,
      active: active as i32,
      scheduled: scheduled as i32,
      retry: retry as i32,
      archived: archived as i32,
      completed: completed as i32,
      aggregating: aggregating as i32,
      processed,
      failed,
      processed_total,
      failed_total,
      paused,
      timestamp: chrono::Utc::now(),
    })
  }

  async fn get_all_queue_stats(&self) -> Result<Vec<QueueStats>> {
    let queues = self.get_queues().await?;
    let mut stats = Vec::new();

    for queue in queues {
      match self.get_queue_stats(&queue).await {
        Ok(stat) => stats.push(stat),
        Err(_) => continue, // Skip queues that fail
      }
    }

    Ok(stats)
  }

  async fn get_queues(&self) -> Result<Vec<String>> {
    // Query distinct queue names from tasks table and queues table
    let task_queues: Vec<String> = Tasks::find()
      .select_only()
      .column(tasks::Column::Queue)
      .distinct()
      .into_tuple()
      .all(self.broker.db())
      .await?;

    // Also get queues from the queues table
    let queue_records = Queues::find().all(self.broker.db()).await?;
    let mut all_queues: Vec<String> = queue_records.into_iter().map(|q| q.name).collect();

    // Merge and deduplicate
    for queue in task_queues {
      if !all_queues.contains(&queue) {
        all_queues.push(queue);
      }
    }

    all_queues.sort();
    Ok(all_queues)
  }

  async fn get_groups(&self, queue: &str) -> Result<Vec<String>> {
    // Use broker's list_groups method
    self.broker.list_groups(queue).await
  }

  async fn list_tasks(
    &self,
    queue: &str,
    state: TaskState,
    pagination: Pagination,
  ) -> Result<Vec<TaskInfo>> {
    let task_state = match state {
      TaskState::Pending => tasks::TaskState::Pending,
      TaskState::Active => tasks::TaskState::Active,
      TaskState::Scheduled => tasks::TaskState::Scheduled,
      TaskState::Retry => tasks::TaskState::Retry,
      TaskState::Archived => tasks::TaskState::Archived,
      TaskState::Completed => tasks::TaskState::Completed,
      TaskState::Aggregating => tasks::TaskState::Aggregating,
    };

    let page = pagination.page.max(1) as u64;
    let size = pagination.size.clamp(1, 100) as u64;

    let tasks = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(task_state))
      .order_by_asc(tasks::Column::CreatedAt)
      .paginate(self.broker.db(), size)
      .fetch_page(page - 1)
      .await?;

    let mut task_infos = Vec::new();
    for task in tasks {
      task_infos.push(self.task_to_info(task)?);
    }

    Ok(task_infos)
  }

  async fn get_task_info(&self, queue: &str, task_id: &str) -> Result<TaskInfo> {
    let task = Tasks::find_by_id(task_id)
      .filter(tasks::Column::Queue.eq(queue))
      .one(self.broker.db())
      .await?
      .ok_or_else(|| Error::other(format!("Task {} not found in queue {}", task_id, queue)))?;

    self.task_to_info(task)
  }

  async fn delete_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let result = Tasks::delete_many()
      .filter(tasks::Column::Id.eq(task_id))
      .filter(tasks::Column::Queue.eq(queue))
      .exec(self.broker.db())
      .await?;

    if result.rows_affected == 0 {
      return Err(Error::other(format!(
        "Task {} not found in queue {}",
        task_id, queue
      )));
    }

    Ok(())
  }

  async fn delete_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    let result = Tasks::delete_many()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Archived))
      .exec(self.broker.db())
      .await?;

    Ok(result.rows_affected as i64)
  }

  async fn delete_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    let result = Tasks::delete_many()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Retry))
      .exec(self.broker.db())
      .await?;

    Ok(result.rows_affected as i64)
  }

  async fn delete_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    let result = Tasks::delete_many()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Scheduled))
      .exec(self.broker.db())
      .await?;

    Ok(result.rows_affected as i64)
  }

  async fn delete_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    let result = Tasks::delete_many()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Pending))
      .exec(self.broker.db())
      .await?;

    Ok(result.rows_affected as i64)
  }

  async fn requeue_all_archived_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_requeue = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Archived))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_requeue.len() as i64;

    for task in tasks_to_requeue {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Pending);
      active_model.process_at = Set(chrono::Utc::now().into());
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn requeue_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_requeue = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Retry))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_requeue.len() as i64;

    for task in tasks_to_requeue {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Pending);
      active_model.process_at = Set(chrono::Utc::now().into());
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn requeue_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_requeue = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Scheduled))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_requeue.len() as i64;

    for task in tasks_to_requeue {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Pending);
      active_model.process_at = Set(chrono::Utc::now().into());
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn run_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let task = Tasks::find_by_id(task_id)
      .filter(tasks::Column::Queue.eq(queue))
      .one(self.broker.db())
      .await?
      .ok_or_else(|| Error::other(format!("Task {} not found in queue {}", task_id, queue)))?;

    let mut active_model: tasks::ActiveModel = task.into();
    active_model.state = Set(tasks::TaskState::Pending);
    active_model.process_at = Set(chrono::Utc::now().into());
    active_model.updated_at = Set(chrono::Utc::now().into());
    active_model.update(self.broker.db()).await?;

    Ok(())
  }

  async fn archive_task(&self, queue: &str, task_id: &str) -> Result<()> {
    let task = Tasks::find_by_id(task_id)
      .filter(tasks::Column::Queue.eq(queue))
      .one(self.broker.db())
      .await?
      .ok_or_else(|| Error::other(format!("Task {} not found in queue {}", task_id, queue)))?;

    let mut active_model: tasks::ActiveModel = task.into();
    active_model.state = Set(tasks::TaskState::Archived);
    active_model.updated_at = Set(chrono::Utc::now().into());
    active_model.update(self.broker.db()).await?;

    Ok(())
  }

  async fn archive_all_pending_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_archive = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Pending))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_archive.len() as i64;

    for task in tasks_to_archive {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Archived);
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn archive_all_retry_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_archive = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Retry))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_archive.len() as i64;

    for task in tasks_to_archive {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Archived);
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn archive_all_scheduled_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_archive = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Scheduled))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_archive.len() as i64;

    for task in tasks_to_archive {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Archived);
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn archive_all_aggregating_tasks(&self, queue: &str) -> Result<i64> {
    let tasks_to_archive = Tasks::find()
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Aggregating))
      .all(self.broker.db())
      .await?;

    let count = tasks_to_archive.len() as i64;

    for task in tasks_to_archive {
      let mut active_model: tasks::ActiveModel = task.into();
      active_model.state = Set(tasks::TaskState::Archived);
      active_model.updated_at = Set(chrono::Utc::now().into());
      active_model.update(self.broker.db()).await?;
    }

    Ok(count)
  }

  async fn pause_queue(&self, queue: &str) -> Result<()> {
    let queue_record = Queues::find_by_id(queue).one(self.broker.db()).await?;

    if let Some(q) = queue_record {
      let mut active_model: queues::ActiveModel = q.into();
      active_model.paused = Set(true);
      active_model.update(self.broker.db()).await?;
      Ok(())
    } else {
      // Create queue if it doesn't exist and pause it
      let new_queue = queues::ActiveModel {
        name: Set(queue.to_string()),
        paused: Set(true),
        created_at: Set(chrono::Utc::now().into()),
        tenant_id: Set(self.broker.tenant_id()),
      };
      new_queue.insert(self.broker.db()).await?;
      Ok(())
    }
  }

  async fn unpause_queue(&self, queue: &str) -> Result<()> {
    let queue_record = Queues::find_by_id(queue).one(self.broker.db()).await?;

    if let Some(q) = queue_record {
      let mut active_model: queues::ActiveModel = q.into();
      active_model.paused = Set(false);
      active_model.update(self.broker.db()).await?;
      Ok(())
    } else {
      // Queue doesn't exist, nothing to unpause
      Ok(())
    }
  }

  async fn is_queue_paused(&self, queue: &str) -> Result<bool> {
    let queue_record = Queues::find_by_id(queue).one(self.broker.db()).await?;
    Ok(queue_record.map(|q| q.paused).unwrap_or(false))
  }

  async fn get_task_result(&self, queue: &str, task_id: &str) -> Result<Option<Vec<u8>>> {
    let task = Tasks::find_by_id(task_id)
      .filter(tasks::Column::Queue.eq(queue))
      .filter(tasks::Column::State.eq(tasks::TaskState::Completed))
      .one(self.broker.db())
      .await?;

    Ok(task.map(|t| t.payload))
  }

  async fn delete_expired_completed_tasks(&self, queue: &str) -> Result<i64> {
    // Delete completed tasks older than their retention period
    self.broker.delete_expired_completed_tasks(queue).await
  }

  async fn get_servers(&self) -> Result<Vec<ServerInfo>> {
    let mut query = Servers::find().filter(servers::Column::ExpiresAt.gt(chrono::Utc::now()));

    // Add tenant_id filter for multi-tenancy isolation
    if let Some(tenant_id) = self.broker.tenant_id() {
      query = query.filter(servers::Column::TenantId.eq(tenant_id));
    }

    let server_records = query.all(self.broker.db()).await?;

    let mut servers = Vec::new();
    for record in server_records {
      match Self::reconstruct_server_info(&record) {
        Ok(info) => servers.push(info),
        Err(e) => {
          tracing::warn!(
            server_id = %record.server_id,
            error = %e,
            "Failed to reconstruct server info, skipping"
          );
          continue; // Skip invalid server info
        }
      }
    }

    Ok(servers)
  }

  async fn get_server_info(&self, server_id: &str) -> Result<Option<ServerInfo>> {
    let mut query = Servers::find()
      .filter(servers::Column::ServerId.eq(server_id))
      .filter(servers::Column::ExpiresAt.gt(chrono::Utc::now()));

    // Add tenant_id filter for multi-tenancy isolation
    if let Some(tenant_id) = self.broker.tenant_id() {
      query = query.filter(servers::Column::TenantId.eq(tenant_id));
    }

    let server_record = query.one(self.broker.db()).await?;

    if let Some(record) = server_record {
      match Self::reconstruct_server_info(&record) {
        Ok(info) => Ok(Some(info)),
        Err(e) => {
          tracing::warn!(
            server_id = %record.server_id,
            error = %e,
            "Failed to reconstruct server info"
          );
          Ok(None)
        }
      }
    } else {
      Ok(None)
    }
  }

  async fn get_history(&self, queue: &str, days: i32) -> Result<Vec<DailyStats>> {
    use crate::backend::pgdb::entity::{stats, Stats};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

    let utc_now = chrono::Utc::now();
    let start_date = (utc_now - chrono::Duration::days(days as i64)).date_naive();

    let stats_records = Stats::find()
      .filter(stats::Column::Queue.eq(queue))
      .filter(stats::Column::Date.gte(start_date))
      .order_by_asc(stats::Column::Date)
      .all(self.broker.db())
      .await?;

    let mut daily_stats = Vec::new();
    for record in stats_records {
      let datetime = record
        .date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| Error::other(format!("Invalid date in stats record: {}", record.date)))?
        .and_utc();

      daily_stats.push(DailyStats {
        queue: record.queue,
        processed: record.processed,
        failed: record.failed,
        date: datetime,
      });
    }

    Ok(daily_stats)
  }

  async fn cancel_processing(&self, task_id: &str) -> Result<()> {
    self.broker.publish_cancellation(task_id).await
  }
}
