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
#[sea_orm(table_name = "asynq_servers")]
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
  #[sea_orm(column_type = "VarBinary(StringLen::None)")]
  pub server_info: Vec<u8>,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
