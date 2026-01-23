//! PostgresSQL 经纪人实现
//! PostgresSQL broker implementation
//!
//! 使用 SeaORM 实现基于 PostgresSQL 的任务存储和管理
//! Implements task storage and management based on PostgresSQL using SeaORM

use crate::base::constants::DEFAULT_QUEUE_NAME;
use crate::error::{Error, Result};
use crate::pgdb::entity::{queues, Queues, Schedulers, Servers, Stats, Tasks, Workers};
use crate::proto::TaskMessage;
use crate::task::Task;
use prost::Message;
use sea_orm::{
  ActiveModelTrait, ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection,
  EntityTrait, QueryFilter, Schema, Set,
};
use uuid::Uuid;

/// PostgresSQL 经纪人实现
/// PostgresSQL broker implementation
pub struct PostgresBroker {
  db: DatabaseConnection,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  tenant_id: Option<String>,
}

impl PostgresBroker {
  /// 从连接字符串创建新的 PostgresSQL 经纪人实例
  /// Create a new PostgresSQL broker instance from connection string
  pub async fn new(database_url: &str) -> Result<Self> {
    Self::new_with_tenant(database_url, None).await
  }

  /// 从连接字符串创建新的带租户的 PostgresSQL 经纪人实例
  /// Create a new PostgresSQL broker instance from connection string with tenant
  pub async fn new_with_tenant(database_url: &str, tenant_id: Option<String>) -> Result<Self> {
    let opt = ConnectOptions::new(database_url)
      .max_connections(10)
      .to_owned();
    let db = Database::connect(opt).await?;
    let broker = Self { db, tenant_id };
    broker.init_schema().await?;
    Ok(broker)
  }

  /// 从现有数据库连接创建 PostgresSQL 经纪人实例
  /// Create a PostgresSQL broker instance from an existing database connection
  pub fn from_connection(db: DatabaseConnection) -> Self {
    Self {
      db,
      tenant_id: None,
    }
  }

  /// 从现有数据库连接创建带租户的 PostgresSQL 经纪人实例
  /// Create a PostgresSQL broker instance from an existing database connection with tenant
  pub fn from_connection_with_tenant(db: DatabaseConnection, tenant_id: Option<String>) -> Self {
    Self { db, tenant_id }
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

    // Create tasks table
    let stmt = schema.create_table_from_entity(Tasks);
    let _ = self.db.execute(backend.build(&stmt)).await;

    // Create queues table
    let stmt = schema.create_table_from_entity(Queues);
    let _ = self.db.execute(backend.build(&stmt)).await;

    // Create servers table
    let stmt = schema.create_table_from_entity(Servers);
    let _ = self.db.execute(backend.build(&stmt)).await;

    // Create workers table
    let stmt = schema.create_table_from_entity(Workers);
    let _ = self.db.execute(backend.build(&stmt)).await;

    // Create schedulers table
    let stmt = schema.create_table_from_entity(Schedulers);
    let _ = self.db.execute(backend.build(&stmt)).await;

    // Create stats table
    let stmt = schema.create_table_from_entity(Stats);
    let _ = self.db.execute(backend.build(&stmt)).await;

    // Create indexes using raw SQL (SeaORM doesn't have index creation API in schema)
    let backend = self.db.get_database_backend();
    let index_sql = r#"
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_queue_state ON asynq_tasks(queue, state);
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_process_at ON asynq_tasks(process_at);
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_unique_key ON asynq_tasks(unique_key) WHERE unique_key IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_group_key ON asynq_tasks(queue, group_key) WHERE group_key IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_lease_expires_at ON asynq_tasks(lease_expires_at) WHERE state = 'active';
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_tenant_id ON asynq_tasks(tenant_id) WHERE tenant_id IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_tasks_tenant_queue_state ON asynq_tasks(tenant_id, queue, state) WHERE tenant_id IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_queues_tenant_id ON asynq_queues(tenant_id) WHERE tenant_id IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_servers_tenant_id ON asynq_servers(tenant_id) WHERE tenant_id IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_workers_tenant_id ON asynq_workers(tenant_id) WHERE tenant_id IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_schedulers_tenant_id ON asynq_schedulers(tenant_id) WHERE tenant_id IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_asynq_stats_tenant_id ON asynq_stats(tenant_id) WHERE tenant_id IS NOT NULL;
    "#;
    let _ = self
      .db
      .execute(sea_orm::Statement::from_string(backend, index_sql))
      .await;

    Ok(())
  }

  /// 将任务消息编码为字节
  /// Encode task message to bytes
  pub(crate) fn encode_task_message(&self, msg: &TaskMessage) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    msg.encode(&mut buf)?;
    Ok(buf)
  }

  /// 从字节解码任务消息
  /// Decode task message from bytes
  pub fn decode_task_message(&self, data: &[u8]) -> Result<TaskMessage> {
    match TaskMessage::decode(data) {
      Ok(msg) => Ok(msg),
      Err(decode_err) => Err(Error::ProtoDecode(decode_err)),
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
}
