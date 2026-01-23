//! 任务实体
//! Task entity

use sea_orm::entity::prelude::*;

/// 任务状态枚举
/// Task state enum
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(50))")]
pub enum TaskState {
  #[sea_orm(string_value = "pending")]
  Pending,
  #[sea_orm(string_value = "active")]
  Active,
  #[sea_orm(string_value = "scheduled")]
  Scheduled,
  #[sea_orm(string_value = "retry")]
  Retry,
  #[sea_orm(string_value = "archived")]
  Archived,
  #[sea_orm(string_value = "completed")]
  Completed,
  #[sea_orm(string_value = "aggregating")]
  Aggregating,
}

/// 任务实体模型
/// Task entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "asynq_tasks")]
pub struct Model {
  #[sea_orm(primary_key, auto_increment = false)]
  pub id: String,
  pub queue: String,
  pub task_type: String,
  #[sea_orm(column_type = "VarBinary(StringLen::None)")]
  pub payload: Vec<u8>,
  pub state: TaskState,
  pub retry: i32,
  pub retried: i32,
  pub error_msg: Option<String>,
  pub last_failed_at: Option<DateTimeWithTimeZone>,
  pub timeout_seconds: i64,
  pub deadline: Option<DateTimeWithTimeZone>,
  pub unique_key: Option<String>,
  pub group_key: Option<String>,
  pub retention_seconds: i64,
  pub completed_at: Option<DateTimeWithTimeZone>,
  pub process_at: DateTimeWithTimeZone,
  pub created_at: DateTimeWithTimeZone,
  pub updated_at: DateTimeWithTimeZone,
  pub lease_expires_at: Option<DateTimeWithTimeZone>,
  #[sea_orm(column_type = "VarBinary(StringLen::None)")]
  pub encoded_task: Vec<u8>,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
