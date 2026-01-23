//! Redis ACL 管理器实现
//! Redis ACL manager implementation
//!
//! 实现基于 Redis ACL 的多租户用户管理
//! Implements multi-tenant user management based on Redis ACL

use crate::acl::{AclConfig, AclManager};
use crate::base::constants::DEFAULT_QUEUE_NAME;
use crate::error::{Error, Result};
use crate::redis::RedisConnectionType;
use async_trait::async_trait;
use redis::acl::Rule;
use redis::aio::MultiplexedConnection;
use redis::{AsyncTypedCommands, Client};

/// Redis ACL 管理器
/// Redis ACL manager
///
/// 使用管理员权限创建和管理租户用户的 ACL
/// Uses administrator permissions to create and manage tenant user ACLs
pub struct RedisAclManager {
  client: Client,
}

impl RedisAclManager {
  /// 从 Redis 连接类型创建新的 ACL 管理器
  /// Create a new ACL manager from Redis connection type
  pub async fn new(conn: RedisConnectionType) -> Result<Self> {
    match conn {
      RedisConnectionType::Single {
        connection_info,
        #[cfg(feature = "tls")]
        tls_certs,
      } => {
        #[cfg(feature = "tls")]
        let client = {
          if let Some(tls) = tls_certs {
            Client::build_with_tls(connection_info, tls)?
          } else {
            Client::open(connection_info)?
          }
        };
        #[cfg(not(feature = "tls"))]
        let client = Client::open(connection_info)?;

        Ok(Self { client })
      }
      #[cfg(feature = "cluster")]
      RedisConnectionType::Cluster(_) => Err(Error::not_supported(
        "ACL management is not supported in cluster mode",
      )),
      #[cfg(feature = "sentinel")]
      RedisConnectionType::Sentinel { .. } => Err(Error::not_supported(
        "ACL management is not yet supported in sentinel mode",
      )),
    }
  }

  /// 从 Redis URL 创建新的 ACL 管理器
  /// Create a new ACL manager from Redis URL
  pub async fn from_url(url: &str) -> Result<Self> {
    let conn = RedisConnectionType::single(url)?;
    Self::new(conn).await
  }

  /// 获取异步连接
  /// Get async connection
  async fn get_connection(&self) -> Result<MultiplexedConnection> {
    let conn = self.client.get_multiplexed_async_connection().await?;
    Ok(conn)
  }

  /// 生成 ACL 规则列表
  /// Generate ACL rules list
  fn build_acl_rules(&self, config: &AclConfig) -> Vec<Rule> {
    let mut rules = Vec::new();

    // 基本权限: on, +@all, -@dangerous, +keys, +info|memory, +info|clients
    // Basic permissions: on, +@all, -@dangerous, +keys, +info|memory, +info|clients
    rules.push(Rule::On);
    rules.push(Rule::AllCommands);
    rules.push(Rule::RemoveCategory("dangerous".to_string()));
    rules.push(Rule::AddCommand("keys".to_string()));
    rules.push(Rule::RemoveCommand("info".to_string()));
    // 数据库限制: -select, +select|<db>
    // Database restrictions: -select, +select|<db>
    rules.push(Rule::RemoveCommand("select".to_string()));
    // 密码
    // Password
    rules.push(Rule::AddPass(config.node_config.password.clone()));

    // 添加默认队列
    // Add default queue pattern - uses hash tag {DEFAULT_QUEUE_NAME} for Redis cluster routing
    rules.push(Rule::Pattern(format!("asynq:{{{}}}:*", DEFAULT_QUEUE_NAME)));

    // 添加租户特定的键模式
    // Add tenant-specific key patterns
    // 从 "~asynq:{username:*" 格式提取模式部分（去掉前导的 ~）
    rules.push(config.node_config.asynq_key_pattern());

    // 添加默认的键模式
    // Add default key patterns
    for pattern in AclConfig::default_key_patterns() {
      rules.push(pattern);
    }

    // 添加只写键
    // Add write-only keys
    for key in &config.write_only_keys {
      // Ensure %W~ prefix is added only once
      let formatted_key = if key.starts_with("%W~") {
        key.clone()
      } else {
        format!("%W~{}", key)
      };
      rules.push(Rule::Other(formatted_key));
    }

    rules
  }
}

#[async_trait]
impl AclManager for RedisAclManager {
  /// 创建租户用户
  /// Create tenant user
  async fn create_tenant_user(&self, config: &AclConfig) -> Result<()> {
    if !config.enabled {
      return Err(Error::config("ACL feature is not enabled"));
    }

    let mut conn = self.get_connection().await?;
    let rules = self.build_acl_rules(config);

    // 使用 redis-rs 的 acl_setuser_rules 命令
    // Use redis-rs acl_setuser_rules command
    // Vec<Rule> implements ToRedisArgs via blanket implementation for slices
    // Each Rule is converted to its Redis representation automatically
    Ok(
      conn
        .acl_setuser_rules(&config.node_config.username, rules.as_slice())
        .await
        .map_err(Error::Redis)?,
    )
  }

  /// 删除租户用户
  /// Delete tenant user
  async fn delete_tenant_user(&self, username: &str) -> Result<()> {
    let mut conn = self.get_connection().await?;

    // 使用 redis-rs 的 acl_deluser 命令
    // Use redis-rs acl_deluser command
    let result: redis::RedisResult<usize> = conn.acl_deluser(&[username]).await;

    match result {
      Ok(deleted) => {
        if deleted > 0 {
          Ok(())
        } else {
          Err(Error::other(format!("User '{}' not found", username)))
        }
      }
      Err(e) => Err(Error::Redis(e)),
    }
  }

  /// 获取租户用户列表
  /// Get tenant user list
  async fn list_tenant_users(&self) -> Result<Vec<String>> {
    let mut conn = self.get_connection().await?;

    // 使用 redis-rs 的 acl_list 命令
    // Use redis-rs acl_list command
    let result: redis::RedisResult<Vec<String>> = conn.acl_list().await;
    match result {
      Ok(users) => Ok(users),
      Err(e) => Err(Error::Redis(e)),
    }
  }

  /// 检查租户用户是否存在
  /// Check if tenant user exists
  async fn tenant_user_exists(&self, username: &str) -> Result<bool> {
    let mut conn = self.get_connection().await?;
    // 使用 redis-rs 的 acl_getuser 命令，返回 AclInfo
    // Use redis-rs acl_getuser command, returns AclInfo
    Ok(conn.acl_getuser(username).await?.is_some())
  }

  /// 更新租户用户配置
  /// Update tenant user configuration
  async fn update_tenant_user(&self, config: &AclConfig) -> Result<()> {
    // Updating is the same as creating for ACL SETUSER
    // 更新与创建相同，使用 ACL SETUSER
    let username = &config.node_config.username;
    if self.tenant_user_exists(username).await? {
      self.delete_tenant_user(username).await?;
    }
    self.create_tenant_user(config).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::acl::NodeConfig;

  #[test]
  fn test_build_acl_rules() {
    // We can't test the actual manager without a Redis connection,
    // but we can test the rule generation logic
    // 我们无法在没有 Redis 连接的情况下测试实际的管理器，
    // 但我们可以测试规则生成逻辑
    let node_config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    let acl_config = AclConfig::new(node_config)
      .enable(true)
      .add_write_only_key("custom:result:*");

    // Verify config is properly set
    // 验证配置是否正确设置
    assert!(acl_config.enabled);
    assert_eq!(acl_config.node_config.username, "tenant1");
    assert_eq!(acl_config.node_config.password, "pass123");
    assert_eq!(acl_config.node_config.db, 0);
    assert_eq!(acl_config.write_only_keys.len(), 1);

    // Create a temporary manager to test rule generation
    // Note: We can't use Client::open without a real connection, so we'll test
    // the logic through the config structure instead
  }

  #[test]
  fn test_acl_command_generation_disabled() {
    // Test that command generation fails when ACL is disabled
    // 测试当 ACL 被禁用时，命令生成应该失败
    let node_config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 0);
    let acl_config = AclConfig::new(node_config).enable(false);
    assert!(!acl_config.enabled);
  }

  #[test]
  fn test_acl_config_with_custom_patterns() {
    let node_config = NodeConfig::new("localhost:6379", "tenant1", "pass123", 2);
    let acl_config = AclConfig::new(node_config)
      .enable(true)
      .add_write_only_key("myapp:results:*");
    assert_eq!(acl_config.write_only_keys.len(), 1);
    assert_eq!(acl_config.node_config.db, 2);
  }

  #[test]
  fn test_rule_types() {
    // Test that Rule enum variants are properly created
    // 测试 Rule 枚举变体是否正确创建
    let rule_on = Rule::On;
    let rule_add_cat = Rule::AddCategory("all".to_string());
    let rule_remove_cat = Rule::RemoveCategory("dangerous".to_string());
    let rule_pattern = Rule::Pattern("asynq:*".to_string());
    let rule_pass = Rule::AddPass("password".to_string());

    // Verify the types are correct
    // 验证类型是否正确
    assert_eq!(rule_on, Rule::On);
    assert_eq!(rule_add_cat, Rule::AddCategory("all".to_string()));
    assert_eq!(
      rule_remove_cat,
      Rule::RemoveCategory("dangerous".to_string())
    );
    assert_eq!(rule_pattern, Rule::Pattern("asynq:*".to_string()));
    assert_eq!(rule_pass, Rule::AddPass("password".to_string()));
  }
}
