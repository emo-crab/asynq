//! 统计实体
//! Stats entity

use sea_orm::entity::prelude::*;

/// 统计实体模型
/// Stats entity model
///
/// **重要：多租户过滤说明 / Important: Multi-Tenancy Filtering**
///
/// tenant_id 字段不是主键的一部分，以保持向后兼容性。
/// The tenant_id field is NOT part of the primary key to maintain backward compatibility.
///
/// **查询时必须手动添加租户过滤！**
/// **Manual tenant filtering MUST be added to queries!**
///
/// 示例 / Example:
/// ```rust
/// let mut query = Stats::find()
///     .filter(stats::Column::Queue.eq(queue))
///     .filter(stats::Column::Date.eq(date));
///
/// // 添加租户过滤 / Add tenant filter
/// if let Some(tenant_id) = &broker.tenant_id() {
///     query = query.filter(stats::Column::TenantId.eq(tenant_id));
/// }
/// ```
///
/// 所有使用 Stats 实体的代码都应该检查并应用租户过滤，否则可能导致数据泄漏。
/// All code using Stats entity should check and apply tenant filtering, otherwise data leakage may occur.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "asynq_stats")]
pub struct Model {
  #[sea_orm(primary_key, auto_increment = false)]
  pub queue: String,
  #[sea_orm(primary_key, auto_increment = false)]
  pub date: Date,
  pub processed: i64,
  pub failed: i64,
  /// 租户 ID，用于多租户隔离（非主键）
  /// Tenant ID for multi-tenancy isolation (not part of primary key)
  /// Note: Kept as Option for backward compatibility. Filter queries manually by tenant_id.
  pub tenant_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
