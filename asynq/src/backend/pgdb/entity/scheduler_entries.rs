//! 调度器条目实体
//! Scheduler entries entity

use sea_orm::entity::prelude::*;

/// 调度器条目实体模型
/// Scheduler entries entity model
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "scheduler_entries")]
pub struct Model {
  /// 条目 ID (scheduler_id:entry_id)
  /// Entry ID (scheduler_id:entry_id)
  #[sea_orm(primary_key, auto_increment = false)]
  pub id: String,
  /// 调度器 ID
  /// Scheduler ID
  pub scheduler_id: String,
  /// Cron 表达式
  /// Cron expression
  pub spec: String,
  /// 任务类型
  /// Task type
  pub task_type: String,
  /// 任务负载（Protocol Buffers 编码）
  /// Task payload (Protocol Buffers encoded)
  #[sea_orm(column_type = "VarBinary(StringLen::None)")]
  pub task_payload: Vec<u8>,
  /// 入队选项
  /// Enqueue options
  pub enqueue_options: Vec<String>,
  /// 下次入队时间
  /// Next enqueue time
  pub next_enqueue_time: Option<DateTimeWithTimeZone>,
  /// 上次入队时间
  /// Previous enqueue time
  pub prev_enqueue_time: Option<DateTimeWithTimeZone>,
  /// 过期时间
  /// Expiration time
  pub expires_at: DateTimeWithTimeZone,
  /// 租户 ID，用于多租户隔离
  /// Tenant ID for multi-tenancy isolation
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
