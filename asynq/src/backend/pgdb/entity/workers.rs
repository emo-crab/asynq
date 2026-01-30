//! 工作者实体
//! Worker entity

use sea_orm::entity::prelude::*;

/// 工作者状态枚举
/// Worker status enum
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(50))")]
pub enum WorkerStatus {
  #[sea_orm(string_value = "active")]
  Active,
  #[sea_orm(string_value = "idle")]
  Idle,
  #[sea_orm(string_value = "stopped")]
  Stopped,
}

/// 工作者实体模型
/// Worker entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "workers")]
pub struct Model {
  #[sea_orm(primary_key, auto_increment = false)]
  pub id: String,
  pub server_id: String,
  pub queue: String,
  pub task_id: Option<String>,
  pub task_type: Option<String>,
  #[sea_orm(column_type = "VarBinary(StringLen::None)", nullable)]
  pub task_payload: Option<Vec<u8>>,
  pub status: WorkerStatus,
  pub started_at: DateTimeWithTimeZone,
  pub updated_at: DateTimeWithTimeZone,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
