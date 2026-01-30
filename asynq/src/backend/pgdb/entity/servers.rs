//! 服务器实体
//! Server entity

use sea_orm::entity::prelude::*;

/// 服务器状态枚举
/// Server status enum
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(50))")]
pub enum ServerStatus {
  #[sea_orm(string_value = "active")]
  Active,
  #[sea_orm(string_value = "stopped")]
  Stopped,
}

/// 服务器实体模型
/// Server entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "servers")]
pub struct Model {
  #[sea_orm(primary_key, auto_increment = false)]
  pub id: String,
  pub host: String,
  pub pid: i32,
  pub server_id: String,
  pub concurrency: i32,
  pub started_at: DateTimeWithTimeZone,
  pub status: ServerStatus,
  pub expires_at: DateTimeWithTimeZone,
  /// Queues with their priorities (stored as JSONB)
  /// 队列及其优先级 (存储为 JSONB)
  #[sea_orm(column_type = "JsonBinary")]
  pub queues: Json,
  /// Strict priority mode flag
  /// 严格优先级模式标志
  pub strict_priority: bool,
  /// Number of active workers
  /// 活跃工作者数量
  pub active_worker_count: i32,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
