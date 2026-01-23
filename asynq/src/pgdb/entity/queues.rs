//! 队列实体
//! Queue entity

use sea_orm::entity::prelude::*;

/// 队列实体模型
/// Queue entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "asynq_queues")]
pub struct Model {
  #[sea_orm(primary_key, auto_increment = false)]
  pub name: String,
  pub paused: bool,
  pub created_at: DateTimeWithTimeZone,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
