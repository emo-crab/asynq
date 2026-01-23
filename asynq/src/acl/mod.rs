//! ACL 多租户模块
//! ACL multi-tenancy module
//!
//! 实现基于 Redis ACL 前缀和 PostgresSQL RLS 的多租户隔离
//! Implements multi-tenant isolation based on Redis ACL prefix and PostgresSQL RLS
//!
//! 每个用户只能访问自己的任务，通过 Redis ACL 键前缀或 PostgresSQL RLS 实现隔离
//! Each user can only access their own tasks, isolation is achieved through Redis ACL key prefix or PostgresSQL RLS

mod redis_acl;

#[cfg(feature = "postgresql")]
mod postgres_acl;

pub use redis_acl::RedisAclManager;

#[cfg(feature = "postgresql")]
pub use postgres_acl::PostgresAclManager;

use crate::error::Result;
use async_trait::async_trait;
use redis::acl::Rule;

/// 节点配置
/// Node configuration
///
/// 配置后端存储节点的连接信息和 ACL 相关参数，支持 Redis 和 PostgresSQL
/// Configure backend storage node connection information and ACL related parameters, supports Redis and PostgresSQL
///
/// 用户名直接作为 ACL 前缀/租户标识，简化配置并保证租户隔离
/// Username is used directly as ACL prefix/tenant identifier, simplifying configuration and ensuring tenant isolation
///
/// 这种设计同时适用于 Redis 和 PostgresSQL 多租户场景：
/// This design works for both Redis and PostgresSQL multi-tenancy:
/// - Redis: 使用用户名作为键前缀进行 ACL 隔离
/// - Redis: Uses username as key prefix for ACL isolation
/// - PostgresSQL: 使用用户名进行行级安全策略 (RLS) 和数据库用户隔离
/// - PostgresSQL: Uses username for row-level security (RLS) policies and database user isolation
#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
  /// 后端地址（Redis 地址或 PostgresSQL 连接字符串）
  /// Backend address (Redis address or PostgresSQL connection string)
  pub addr: String,
  /// 用户名（Redis ACL 前缀或 PostgresSQL 数据库用户）
  /// Username (Redis ACL prefix or PostgresSQL database user)
  pub username: String,
  /// 密码
  /// Password
  pub password: String,
  /// Redis 数据库编号（仅用于 Redis）
  /// Redis database number (only for Redis)
  pub db: i32,
}

impl NodeConfig {
  /// 创建新的节点配置
  /// Create a new node configuration
  ///
  /// 用户名直接作为 ACL 前缀使用
  /// Username is used directly as ACL prefix
  pub fn new(addr: &str, username: &str, password: &str, db: i32) -> Self {
    Self {
      addr: addr.to_string(),
      username: username.to_string(),
      password: password.to_string(),
      db,
    }
  }

  /// 获取 ACL 前缀（即用户名）
  /// Get ACL prefix (which is the username)
  ///
  /// 返回用户名，用于租户隔离
  /// Returns username, used for tenant isolation
  pub fn acl_prefix(&self) -> &str {
    &self.username
  }

  /// 生成 ACL 前缀字符串（带冒号）
  /// Generate ACL prefix string (with colon)
  ///
  /// 返回格式: `{username}:`
  /// Returns format: `{username}:`
  pub fn acl_prefix_with_colon(&self) -> String {
    format!("{}:", self.username)
  }

  /// 生成 Asynq ACL 键模式
  /// Generate Asynq ACL key pattern
  ///
  /// 返回格式: `~asynq:{username:*`
  /// Returns format: `~asynq:{username:*`
  ///
  /// 注意：使用 Redis hash tag 格式 `{username}` 确保相关键在集群中路由到同一节点
  /// Note: Uses Redis hash tag format `{username}` to ensure related keys route to the same cluster node
  pub fn asynq_key_pattern(&self) -> Rule {
    Rule::Pattern(format!("asynq:{{{}:*", self.username))
  }
}

impl std::fmt::Display for NodeConfig {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:", self.username)
  }
}

/// ACL 配置
/// ACL configuration
///
/// 用于配置 ACL 多租户功能
/// Used to configure ACL multi-tenancy feature
#[derive(Debug, Clone, Default)]
pub struct AclConfig {
  /// 是否启用 ACL 特性
  /// Whether to enable ACL feature
  pub enabled: bool,
  /// 节点配置
  /// Node configuration
  pub node_config: NodeConfig,
  /// 只写键列表
  /// Write-only key list
  pub write_only_keys: Vec<String>,
}

impl AclConfig {
  /// 创建新的 ACL 配置
  /// Create a new ACL configuration
  pub fn new(node_config: NodeConfig) -> Self {
    Self {
      enabled: true,
      node_config,
      write_only_keys: Vec::new(),
    }
  }

  /// 启用或禁用 ACL 特性
  /// Enable or disable ACL feature
  pub fn enable(mut self, enabled: bool) -> Self {
    self.enabled = enabled;
    self
  }

  /// 添加只写键
  /// Add write-only key
  pub fn add_write_only_key<S: Into<String>>(mut self, key: S) -> Self {
    self.write_only_keys.push(key.into());
    self
  }

  /// 设置只写键列表
  /// Set write-only key list
  pub fn write_only_keys(mut self, keys: Vec<String>) -> Self {
    self.write_only_keys = keys;
    self
  }

  /// 获取默认的键模式列表
  /// Get default key pattern list
  pub fn default_key_patterns() -> Vec<Rule> {
    vec![
      Rule::Pattern("asynq:queues".to_string()),
      Rule::Pattern("asynq:servers:*".to_string()),
      Rule::Pattern("asynq:servers".to_string()),
      Rule::Pattern("asynq:workers".to_string()),
      Rule::Pattern("asynq:workers:*".to_string()),
      Rule::Pattern("asynq:schedulers".to_string()),
      Rule::Pattern("asynq:schedulers:*".to_string()),
      Rule::Other("&asynq:cancel".to_string()),
    ]
  }
}

/// ACL 命令生成结果
/// ACL command generation result
#[derive(Debug, Clone)]
pub struct AclCommand {
  /// 完整的 ACL 命令参数列表
  /// Complete ACL command argument list
  pub args: Vec<String>,
}

impl AclCommand {
  /// 将命令转换为 Redis 命令字符串
  /// Convert command to Redis command string
  pub fn to_command_string(&self) -> String {
    self.args.join(" ")
  }

  /// 获取命令参数列表
  /// Get command argument list
  pub fn get_args(&self) -> &[String] {
    &self.args
  }
}

/// ACL 管理器特性
/// ACL manager trait
///
/// 定义了 ACL 管理的通用接口，便于支持不同的后端
/// Defines the common interface for ACL management, facilitating support for different backends
#[async_trait]
pub trait AclManager: Send + Sync {
  /// 创建租户用户
  /// Create tenant user
  async fn create_tenant_user(&self, config: &AclConfig) -> Result<()>;

  /// 删除租户用户
  /// Delete tenant user
  async fn delete_tenant_user(&self, username: &str) -> Result<()>;

  /// 获取租户用户列表
  /// Get tenant user list
  async fn list_tenant_users(&self) -> Result<Vec<String>>;

  /// 检查租户用户是否存在
  /// Check if tenant user exists
  async fn tenant_user_exists(&self, username: &str) -> Result<bool>;

  /// 更新租户用户配置
  /// Update tenant user configuration
  async fn update_tenant_user(&self, config: &AclConfig) -> Result<()>;
}
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_node_config_creation() {
    let config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    assert_eq!(config.addr, "localhost:6379");
    assert_eq!(config.username, "tenant1");
    assert_eq!(config.password, "pass123");
    assert_eq!(config.db, 0);
  }

  #[test]
  fn test_node_config_acl_prefix() {
    let config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    assert_eq!(config.acl_prefix(), "tenant1");
  }

  #[test]
  fn test_node_config_acl_prefix_with_colon() {
    let config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    assert_eq!(config.acl_prefix_with_colon(), "tenant1:");
  }

  #[test]
  fn test_node_config_asynq_key_pattern() {
    let config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    assert_eq!(
      config.asynq_key_pattern(),
      Rule::Pattern("asynq:{tenant1:*".to_string())
    );
  }

  #[test]
  fn test_node_config_display() {
    let config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    assert_eq!(config.to_string(), "tenant1:");
  }

  #[test]
  fn test_acl_config_creation() {
    let node_config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    let acl_config = AclConfig::new(node_config);
    assert!(acl_config.enabled);
    assert!(acl_config.write_only_keys.is_empty());
  }

  #[test]
  fn test_acl_config_builder() {
    let node_config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    let acl_config = AclConfig::new(node_config)
      .enable(true)
      .add_write_only_key("test:result:*");
    assert!(acl_config.enabled);
    assert_eq!(acl_config.write_only_keys.len(), 1);
    assert_eq!(acl_config.write_only_keys[0], "test:result:*");
  }

  #[test]
  fn test_acl_config_disable() {
    let node_config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    let acl_config = AclConfig::new(node_config).enable(false);
    assert!(!acl_config.enabled);
  }

  #[test]
  fn test_acl_command_to_string() {
    let cmd = AclCommand {
      args: vec![
        "ACL".to_string(),
        "SETUSER".to_string(),
        "user1".to_string(),
        "on".to_string(),
      ],
    };
    assert_eq!(cmd.to_command_string(), "ACL SETUSER user1 on");
  }
}
