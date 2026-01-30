//! 调度器事件实体
//! Scheduler events entity

use sea_orm::entity::prelude::*;

/// 调度器事件实体模型
/// Scheduler events entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "scheduler_events")]
pub struct Model {
  /// 主键 ID
  /// Primary key ID
  #[sea_orm(primary_key)]
  pub id: i64,
  /// 任务 ID
  /// Task ID
  pub task_id: String,
  /// 入队时间
  /// Enqueue time
  pub enqueue_time: DateTimeWithTimeZone,
  /// 事件数据（Protocol Buffers 编码）
  /// Event data (Protocol Buffers encoded)
  #[sea_orm(column_type = "VarBinary(StringLen::None)")]
  pub event_data: Vec<u8>,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
