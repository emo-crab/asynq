//! PostgresSQL 经纪人实现
//! PostgresSQL broker implementation
//!
//! 使用 SeaORM 实现基于 PostgresSQL 的任务存储和管理
//! Implements task storage and management based on PostgresSQL using SeaORM

use crate::backend::pgdb::entity::{
  queues, Queues, SchedulerEntries, SchedulerEvents, Schedulers, Servers, Stats, Tasks, Workers,
};
use crate::base::constants::DEFAULT_QUEUE_NAME;
use crate::error::{Error, Result};
use crate::proto::{SchedulerEnqueueEvent, SchedulerEntry, TaskMessage};
use crate::task::Task;
use prost::Message;
use sea_orm::{
  ActiveModelTrait, ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection,
  EntityTrait, QueryFilter, Schema, Set,
};
use uuid::Uuid;

enum PostgresBrokerSource {
  DatabaseUrl(String),
  Connection(DatabaseConnection),
}

/// PostgresSQL Broker builder
///
/// Unified factory entry for creating `PostgresBroker` instances.
pub struct PostgresBrokerBuilder {
  source: Option<PostgresBrokerSource>,
  tenant_id: Option<String>,
  max_connections: Option<u32>,
  init_schema: bool,
}

impl PostgresBrokerBuilder {
  fn new() -> Self {
    Self {
      source: None,
      tenant_id: None,
      max_connections: Some(10),
      init_schema: true,
    }
  }

  /// Set database URL as broker source.
  pub fn database_url(mut self, database_url: impl Into<String>) -> Self {
    self.source = Some(PostgresBrokerSource::DatabaseUrl(database_url.into()));
    self
  }

  /// Set an existing SeaORM database connection as broker source.
  pub fn connection(mut self, db: DatabaseConnection) -> Self {
    self.source = Some(PostgresBrokerSource::Connection(db));
    self
  }

  /// Set tenant id for multi-tenant isolation.
  pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
    self.tenant_id = Some(tenant_id.into());
    self
  }

  /// Set optional tenant id for multi-tenant isolation.
  pub fn tenant_id_opt(mut self, tenant_id: Option<String>) -> Self {
    self.tenant_id = tenant_id;
    self
  }

  /// Set maximum database connections used when connecting from URL.
  pub fn max_connections(mut self, max_connections: u32) -> Self {
    self.max_connections = Some(max_connections);
    self
  }

  /// Control whether schema initialization runs during build.
  pub fn init_schema(mut self, init_schema: bool) -> Self {
    self.init_schema = init_schema;
    self
  }

  /// Build a PostgresBroker from configured source and options.
  pub async fn build(self) -> Result<PostgresBroker> {
    let db = match self.source {
      Some(PostgresBrokerSource::DatabaseUrl(url)) => {
        let max_connections = self.max_connections.unwrap_or(10);
        let opt = ConnectOptions::new(url)
          .max_connections(max_connections)
          .to_owned();
        Database::connect(opt).await?
      }
      Some(PostgresBrokerSource::Connection(db)) => db,
      None => {
        return Err(Error::other(
          "PostgresBrokerBuilder source is not configured; call database_url(...) or connection(...)",
        ));
      }
    };

    let broker = PostgresBroker {
      db,
      tenant_id: self.tenant_id,
    };

    if self.init_schema {
      broker.init_schema().await?;
    }

    Ok(broker)
  }
}

/// PostgresSQL 经纪人实现
/// PostgresSQL broker implementation
pub struct PostgresBroker {
  db: DatabaseConnection,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  tenant_id: Option<String>,
}

impl PostgresBroker {
  /// PostgresSQL 经纪人的统一创建入口
  /// Unified factory entry for PostgresSQL broker creation
  pub fn builder() -> PostgresBrokerBuilder {
    PostgresBrokerBuilder::new()
  }

  /// 获取数据库连接
  /// Get the database connection
  pub fn db(&self) -> &DatabaseConnection {
    &self.db
  }

  /// 获取租户 ID
  /// Get tenant ID
  pub fn tenant_id(&self) -> Option<String> {
    self.tenant_id.clone()
  }

  /// 初始化数据库 schema
  /// Initialize database schema
  pub async fn init_schema(&self) -> Result<()> {
    let backend = self.db.get_database_backend();
    let schema = Schema::new(backend);
    // Create queues table
    self
      .db
      .execute(backend.build(schema.create_table_from_entity(Queues).if_not_exists()))
      .await?;

    // Create tasks table
    self
      .db
      .execute(backend.build(schema.create_table_from_entity(Tasks).if_not_exists()))
      .await?;

    // Create servers table
    self
      .db
      .execute(backend.build(schema.create_table_from_entity(Servers).if_not_exists()))
      .await?;

    // Create workers table
    self
      .db
      .execute(backend.build(schema.create_table_from_entity(Workers).if_not_exists()))
      .await?;

    // Create schedulers table
    self
      .db
      .execute(backend.build(schema.create_table_from_entity(Schedulers).if_not_exists()))
      .await?;

    // Create scheduler_entries table
    self
      .db
      .execute(
        backend.build(
          schema
            .create_table_from_entity(SchedulerEntries)
            .if_not_exists(),
        ),
      )
      .await?;

    // Create scheduler_events table
    self
      .db
      .execute(
        backend.build(
          schema
            .create_table_from_entity(SchedulerEvents)
            .if_not_exists(),
        ),
      )
      .await?;

    // Create stats table
    self
      .db
      .execute(backend.build(schema.create_table_from_entity(Stats).if_not_exists()))
      .await?;
    Ok(())
  }

  /// 从数据库任务模型重建 TaskMessage
  /// Reconstruct TaskMessage from database task model
  pub fn task_model_to_message(
    &self,
    task: &crate::backend::pgdb::entity::tasks::Model,
  ) -> TaskMessage {
    TaskMessage {
      r#type: task.task_type.clone(),
      payload: task.payload.clone(),
      headers: task.parse_headers(),
      id: task.id.clone(),
      queue: task.queue.clone(),
      retry: task.retry,
      retried: task.retried,
      error_msg: task.error_msg.clone().unwrap_or_default(),
      last_failed_at: task
        .last_failed_at
        .map(|dt| {
          let dt: chrono::DateTime<chrono::Utc> = dt.into();
          dt.timestamp()
        })
        .unwrap_or(0),
      timeout: task.timeout_seconds,
      deadline: task
        .deadline
        .map(|dt| {
          let dt: chrono::DateTime<chrono::Utc> = dt.into();
          dt.timestamp()
        })
        .unwrap_or(0),
      unique_key: task.unique_key.clone().unwrap_or_default(),
      group_key: task.group_key.clone().unwrap_or_default(),
      retention: task.retention_seconds,
      completed_at: task
        .completed_at
        .map(|dt| {
          let dt: chrono::DateTime<chrono::Utc> = dt.into();
          dt.timestamp()
        })
        .unwrap_or(0),
    }
  }

  /// 从 Task 创建 TaskMessage
  /// Create TaskMessage from Task
  pub(crate) fn task_to_message(&self, task: &Task) -> TaskMessage {
    TaskMessage {
      r#type: task.task_type.clone(),
      payload: task.payload.clone(),
      headers: task.headers.clone(),
      id: task
        .options
        .task_id
        .clone()
        .unwrap_or(Uuid::new_v4().to_string()),
      queue: if task.options.queue.is_empty() {
        DEFAULT_QUEUE_NAME.to_string()
      } else {
        task.options.queue.clone()
      },
      retry: task.options.max_retry,
      retried: 0,
      error_msg: String::new(),
      last_failed_at: 0,
      timeout: task
        .options
        .timeout
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0),
      deadline: task.options.deadline.map(|d| d.timestamp()).unwrap_or(0),
      unique_key: String::new(),
      group_key: task.options.group.clone().unwrap_or_default(),
      retention: task
        .options
        .retention
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0),
      completed_at: 0,
    }
  }

  /// 确保队列存在
  /// Ensure queue exists
  pub(crate) async fn ensure_queue_exists(&self, queue_name: &str) -> Result<()> {
    let mut query = Queues::find_by_id(queue_name);
    if let Some(tenant_id) = &self.tenant_id {
      query = query.filter(queues::Column::TenantId.eq(tenant_id));
    }
    let existing = query.one(&self.db).await?;
    if existing.is_none() {
      let new_queue = queues::ActiveModel {
        name: Set(queue_name.to_string()),
        paused: Set(false),
        created_at: Set(chrono::Utc::now().into()),
        tenant_id: Set(self.tenant_id.clone()),
      };
      let _ = new_queue.insert(&self.db).await;
    }
    Ok(())
  }

  /// 批量写入 scheduler entries，兼容 Go 版 asynq
  /// Batch write scheduler entries, compatible with Go version asynq
  pub async fn write_scheduler_entries(
    &self,
    entries: &[SchedulerEntry],
    scheduler_id: &str,
    ttl_secs: u64,
    tenant: Option<&str>,
  ) -> Result<()> {
    use crate::backend::pgdb::entity::scheduler_entries;
    use chrono::Duration;

    let expires_at = chrono::Utc::now() + Duration::seconds(ttl_secs as i64);

    // Determine the effective tenant: prefer the broker's own tenant_id, fall back to the
    // scheduler-supplied tenant parameter for multi-tenant isolation.
    let effective_tenant = self.tenant_id.as_deref().or(tenant);

    // Delete old entries for this scheduler
    let mut delete_query = SchedulerEntries::delete_many()
      .filter(scheduler_entries::Column::SchedulerId.eq(scheduler_id));

    if let Some(tenant_id) = effective_tenant {
      delete_query = delete_query.filter(scheduler_entries::Column::TenantId.eq(tenant_id));
    }

    let _ = delete_query.exec(&self.db).await;

    // Insert new entries
    for entry in entries {
      let next_enqueue_time = entry.next_enqueue_time.as_ref().map(|t| {
        chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
          .unwrap_or_else(chrono::Utc::now)
          .into()
      });

      let prev_enqueue_time = entry.prev_enqueue_time.as_ref().map(|t| {
        chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
          .unwrap_or_else(chrono::Utc::now)
          .into()
      });

      let new_entry = scheduler_entries::ActiveModel {
        id: Set(format!("{}:{}", scheduler_id, &entry.id)),
        scheduler_id: Set(scheduler_id.to_string()),
        spec: Set(entry.spec.clone()),
        task_type: Set(entry.task_type.clone()),
        task_payload: Set(entry.task_payload.clone()),
        enqueue_options: Set(entry.enqueue_options.clone()),
        next_enqueue_time: Set(next_enqueue_time),
        prev_enqueue_time: Set(prev_enqueue_time),
        expires_at: Set(expires_at.into()),
        tenant_id: Set(effective_tenant.map(|t| t.to_string())),
      };

      let _ = new_entry.insert(&self.db).await;
    }

    Ok(())
  }

  /// 记录调度事件，兼容 Go 版 asynq
  /// Record scheduling event, compatible with Go version asynq
  #[cfg(feature = "postgres")]
  pub async fn record_scheduler_enqueue_event(
    &self,
    event: &SchedulerEnqueueEvent,
    task_id: &str,
  ) -> Result<()> {
    use crate::backend::pgdb::entity::scheduler_events;

    let enqueue_time = event
      .enqueue_time
      .as_ref()
      .map(|t| {
        chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
          .unwrap_or_else(chrono::Utc::now)
          .into()
      })
      .unwrap_or_else(|| chrono::Utc::now().into());

    let mut buf = Vec::new();
    event
      .encode(&mut buf)
      .map_err(|e| Error::other(format!("prost encode error: {e}")))?;

    let new_event = scheduler_events::ActiveModel {
      id: Set(0), // Auto-increment
      task_id: Set(task_id.to_string()),
      enqueue_time: Set(enqueue_time),
      event_data: Set(buf),
      tenant_id: Set(self.tenant_id.clone()),
    };

    let _ = new_event.insert(&self.db).await;

    // Keep only the most recent 1000 events (cleanup old events)
    // Use a more efficient query with NOT IN and LIMIT
    let cleanup_sql = if let Some(tenant_id) = &self.tenant_id {
      // Using parameterized query to prevent SQL injection
      sea_orm::Statement::from_sql_and_values(
        self.db.get_database_backend(),
        r#"
          DELETE FROM asynq_scheduler_events
          WHERE tenant_id = $1 AND id NOT IN (
            SELECT id FROM asynq_scheduler_events
            WHERE tenant_id = $1
            ORDER BY enqueue_time DESC 
            LIMIT 1000
          )
        "#,
        vec![tenant_id.clone().into()],
      )
    } else {
      sea_orm::Statement::from_sql_and_values(
        self.db.get_database_backend(),
        r#"
          DELETE FROM asynq_scheduler_events
          WHERE id NOT IN (
            SELECT id FROM asynq_scheduler_events
            ORDER BY enqueue_time DESC
            LIMIT 1000
          )
        "#,
        vec![],
      )
    };

    let _ = self.db.execute(cleanup_sql).await;

    Ok(())
  }

  /// 获取所有 SchedulerEntry，兼容 Go 版 asynq
  /// Get all SchedulerEntry, compatible with Go version asynq
  #[cfg(feature = "postgres")]
  pub async fn scheduler_entries_script(
    &self,
    scheduler_id: &str,
  ) -> Result<std::collections::HashMap<String, Vec<u8>>> {
    use crate::backend::pgdb::entity::scheduler_entries;

    let mut query =
      SchedulerEntries::find().filter(scheduler_entries::Column::SchedulerId.eq(scheduler_id));

    if let Some(tenant_id) = &self.tenant_id {
      query = query.filter(scheduler_entries::Column::TenantId.eq(tenant_id));
    }

    let entries = query.all(&self.db).await?;

    let mut map = std::collections::HashMap::new();
    for entry_model in entries {
      // Extract just the entry ID part (after the "scheduler_id:" prefix)
      let entry_id = entry_model
        .id
        .split_once(':')
        .map(|(_, id)| id.to_string())
        .unwrap_or_else(|| entry_model.id.clone());

      // Reconstruct SchedulerEntry proto message
      let proto_entry = SchedulerEntry {
        id: entry_id.clone(),
        spec: entry_model.spec.clone(),
        task_type: entry_model.task_type.clone(),
        task_payload: entry_model.task_payload.clone(),
        enqueue_options: entry_model.enqueue_options.clone(),
        next_enqueue_time: entry_model.next_enqueue_time.map(|dt| {
          let datetime: chrono::DateTime<chrono::Utc> = dt.into();
          prost_types::Timestamp {
            seconds: datetime.timestamp(),
            nanos: datetime.timestamp_subsec_nanos() as i32,
          }
        }),
        prev_enqueue_time: entry_model.prev_enqueue_time.map(|dt| {
          let datetime: chrono::DateTime<chrono::Utc> = dt.into();
          prost_types::Timestamp {
            seconds: datetime.timestamp(),
            nanos: datetime.timestamp_subsec_nanos() as i32,
          }
        }),
      };

      let mut buf = Vec::new();
      proto_entry
        .encode(&mut buf)
        .map_err(|e| Error::other(format!("prost encode error: {e}")))?;

      // Use the entry_id (without scheduler prefix) as the map key
      map.insert(entry_id, buf);
    }

    Ok(map)
  }

  /// 获取调度事件列表，兼容 Go 版 asynq
  /// Get scheduling event list, compatible with Go version asynq
  #[cfg(feature = "postgres")]
  pub async fn scheduler_events_script(&self, count: usize) -> Result<Vec<Vec<u8>>> {
    use crate::backend::pgdb::entity::scheduler_events;
    use sea_orm::{PaginatorTrait, QueryOrder};

    let mut query = SchedulerEvents::find().order_by_desc(scheduler_events::Column::EnqueueTime);

    if let Some(tenant_id) = &self.tenant_id {
      query = query.filter(scheduler_events::Column::TenantId.eq(tenant_id));
    }

    let events: Vec<crate::backend::pgdb::entity::scheduler_events::Model> =
      query.paginate(&self.db, count as u64).fetch_page(0).await?;

    let result: Vec<Vec<u8>> = events.into_iter().map(|e| e.event_data).collect();
    Ok(result)
  }

  /// 删除 scheduler entries 数据，兼容 Go 版 asynq
  /// Delete scheduler entries data, compatible with Go version asynq
  #[cfg(feature = "postgres")]
  pub async fn clear_scheduler_entries(
    &self,
    scheduler_id: &str,
    tenant: Option<&str>,
  ) -> Result<()> {
    use crate::backend::pgdb::entity::scheduler_entries;

    // Determine the effective tenant: prefer the broker's own tenant_id, fall back to the
    // scheduler-supplied tenant parameter for multi-tenant isolation.
    let effective_tenant = self.tenant_id.as_deref().or(tenant);

    let mut delete_query = SchedulerEntries::delete_many()
      .filter(scheduler_entries::Column::SchedulerId.eq(scheduler_id));

    if let Some(tenant_id) = effective_tenant {
      delete_query = delete_query.filter(scheduler_entries::Column::TenantId.eq(tenant_id));
    }

    let _ = delete_query.exec(&self.db).await;

    Ok(())
  }
}
