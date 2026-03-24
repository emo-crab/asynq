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
pub enum Relation {
  #[sea_orm(has_many = "super::tasks::Entity")]
  Tasks,
  #[sea_orm(has_many = "super::workers::Entity")]
  Workers,
  #[sea_orm(has_many = "super::stats::Entity")]
  Stats,
}

impl Related<super::tasks::Entity> for Entity {
  fn to() -> RelationDef {
    Relation::Tasks.def()
  }
}

impl Related<super::workers::Entity> for Entity {
  fn to() -> RelationDef {
    Relation::Workers.def()
  }
}

impl Related<super::stats::Entity> for Entity {
  fn to() -> RelationDef {
    Relation::Stats.def()
  }
}

impl ActiveModelBehavior for ActiveModel {}
