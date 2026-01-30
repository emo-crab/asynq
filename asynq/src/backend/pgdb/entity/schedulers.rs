//! 调度器实体
//! Scheduler entity

use sea_orm::entity::prelude::*;

/// 调度器实体模型
/// Scheduler entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "schedulers")]
pub struct Model {
  #[sea_orm(primary_key, auto_increment = false)]
  pub id: String,
  pub host: String,
  pub pid: i32,
  pub status: String,
  pub started_at: DateTimeWithTimeZone,
  pub expires_at: DateTimeWithTimeZone,
  #[sea_orm(column_type = "VarBinary(StringLen::None)", nullable)]
  pub scheduler_info: Option<Vec<u8>>,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
