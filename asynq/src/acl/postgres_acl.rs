//! PostgresSQL ACL 管理器实现
//! PostgresSQL ACL manager implementation
//!
//! 实现基于 PostgresSQL Row-Level Security (RLS) 的多租户用户管理
//! Implements multi-tenant user management based on PostgresSQL Row-Level Security (RLS)

use crate::acl::{AclConfig, AclManager};
use crate::error::{Error, Result};
use async_trait::async_trait;
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};

/// PostgresSQL ACL 管理器
/// PostgresSQL ACL manager
///
/// 使用 PostgresSQL 用户和 RLS 策略管理租户隔离
/// Uses PostgresSQL users and RLS policies to manage tenant isolation
pub struct PostgresAclManager {
  db: DatabaseConnection,
  /// 管理员连接用于创建用户和策略
  /// Admin connection for creating users and policies
  _admin_user: String,
}

impl PostgresAclManager {
  /// 创建新的 PostgresSQL ACL 管理器
  /// Create a new PostgresSQL ACL manager
  pub fn new(db: DatabaseConnection, admin_user: Option<String>) -> Self {
    Self {
      db,
      _admin_user: admin_user.unwrap_or_else(|| "postgres".to_string()),
    }
  }

  /// 验证并转义 SQL 标识符（用户名、表名等）
  /// Validate and escape SQL identifiers (usernames, table names, etc.)
  ///
  /// PostgresSQL 标识符规则：
  /// - 以字母或下划线开头
  /// - 只包含字母、数字、下划线
  /// - 最大 63 字符
  fn validate_sql_identifier(identifier: &str) -> Result<String> {
    // 检查长度
    if identifier.is_empty() || identifier.len() > 63 {
      return Err(Error::config("Identifier must be 1-63 characters"));
    }

    // 检查首字符
    let first_char = identifier.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
      return Err(Error::config(
        "Identifier must start with letter or underscore",
      ));
    }

    // 检查所有字符
    for c in identifier.chars() {
      if !c.is_ascii_alphanumeric() && c != '_' {
        return Err(Error::config(
          "Identifier can only contain letters, numbers, and underscores",
        ));
      }
    }

    // 使用双引号包围标识符以支持大小写
    // Use double quotes around identifier to support case sensitivity
    Ok(format!("\"{}\"", identifier))
  }

  /// 转义 SQL 字符串值
  /// Escape SQL string value
  fn escape_sql_string(value: &str) -> String {
    // PostgresSQL 字符串转义：单引号加倍
    // PostgresSQL string escaping: double single quotes
    value.replace('\'', "''")
  }

  /// 启用表的 RLS 策略
  /// Enable RLS policies for a table
  async fn enable_rls_for_table(&self, table_name: &str) -> Result<()> {
    let table_name = Self::validate_sql_identifier(table_name)?;
    let sql = format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", table_name);
    self
      .db
      .execute(Statement::from_string(self.db.get_database_backend(), sql))
      .await?;
    Ok(())
  }

  /// 创建租户隔离策略
  /// Create tenant isolation policy
  async fn create_tenant_policy(&self, table_name: &str, tenant_user: &str) -> Result<()> {
    let table_name_escaped = Self::validate_sql_identifier(table_name)?;
    let tenant_user_escaped = Self::validate_sql_identifier(tenant_user)?;
    let tenant_user_str = Self::escape_sql_string(tenant_user);
    let policy_name = format!("tenant_isolation_policy_{}", tenant_user);
    let policy_name_escaped = Self::validate_sql_identifier(&policy_name)?;

    // 首先删除已存在的策略（如果有）
    // First drop existing policy if any
    let drop_sql = format!(
      "DROP POLICY IF EXISTS {} ON {};",
      policy_name_escaped, table_name_escaped
    );
    let _ = self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        drop_sql,
      ))
      .await;

    // 创建新策略：用户只能访问其 tenant_id 匹配的行
    // Create new policy: user can only access rows where tenant_id matches
    let create_sql = format!(
      "CREATE POLICY {} ON {} FOR ALL TO {} USING (tenant_id = '{}');",
      policy_name_escaped, table_name_escaped, tenant_user_escaped, tenant_user_str
    );
    self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        create_sql,
      ))
      .await?;

    Ok(())
  }

  /// 为所有 asynq 表启用 RLS
  /// Enable RLS for all asynq tables
  async fn enable_rls_for_all_tables(&self) -> Result<()> {
    let tables = vec![
      "asynq_tasks",
      "asynq_queues",
      "asynq_servers",
      "asynq_workers",
      "asynq_stats",
      "asynq_schedulers",
    ];

    for table in tables {
      self.enable_rls_for_table(table).await?;
    }

    Ok(())
  }

  /// 为租户创建所有表的策略
  /// Create policies for all tables for a tenant
  async fn create_all_tenant_policies(&self, tenant_user: &str) -> Result<()> {
    let tables = vec![
      "asynq_tasks",
      "asynq_queues",
      "asynq_servers",
      "asynq_workers",
      "asynq_stats",
      "asynq_schedulers",
    ];

    for table in tables {
      self.create_tenant_policy(table, tenant_user).await?;
    }

    Ok(())
  }

  /// 删除租户的所有策略
  /// Delete all policies for a tenant
  async fn drop_all_tenant_policies(&self, tenant_user: &str) -> Result<()> {
    let tables = vec![
      "asynq_tasks",
      "asynq_queues",
      "asynq_servers",
      "asynq_workers",
      "asynq_stats",
      "asynq_schedulers",
    ];

    for table in tables {
      let table_escaped = Self::validate_sql_identifier(table)?;
      let policy_name = format!("tenant_isolation_policy_{}", tenant_user);
      let policy_name_escaped = Self::validate_sql_identifier(&policy_name)?;
      let drop_sql = format!(
        "DROP POLICY IF EXISTS {} ON {};",
        policy_name_escaped, table_escaped
      );
      let _ = self
        .db
        .execute(Statement::from_string(
          self.db.get_database_backend(),
          drop_sql,
        ))
        .await;
    }

    Ok(())
  }
}

#[async_trait]
impl AclManager for PostgresAclManager {
  /// 创建租户用户
  /// Create tenant user
  async fn create_tenant_user(&self, config: &AclConfig) -> Result<()> {
    if !config.enabled {
      return Err(Error::config("ACL feature is not enabled"));
    }

    let username = &config.node_config.username;
    let password = &config.node_config.password;

    // 验证用户名和密码
    // Validate username and password
    let username_escaped = Self::validate_sql_identifier(username)?;
    let password_escaped = Self::escape_sql_string(password);

    // 创建数据库用户
    // Create database user
    let create_user_sql = format!(
      "CREATE USER IF NOT EXISTS {} WITH PASSWORD '{}';",
      username_escaped, password_escaped
    );

    // 某些 PostgresSQL 版本不支持 IF NOT EXISTS，需要捕获错误
    // Some PostgresSQL versions don't support IF NOT EXISTS, need to catch errors
    let create_user_sql_alt = format!(
      "CREATE USER {} WITH PASSWORD '{}';",
      username_escaped, password_escaped
    );

    match self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        create_user_sql,
      ))
      .await
    {
      Ok(_) => {}
      Err(_) => {
        // 尝试不带 IF NOT EXISTS 的版本
        // Try version without IF NOT EXISTS
        match self
          .db
          .execute(Statement::from_string(
            self.db.get_database_backend(),
            create_user_sql_alt,
          ))
          .await
        {
          Ok(_) => {}
          Err(e) => {
            // 如果错误是因为用户已存在，忽略
            // If error is because user already exists, ignore
            // Note: Error code 42710 is "duplicate_object" in PostgresSQL
            // We also check for common error messages for compatibility
            let error_str = e.to_string().to_lowercase();
            let is_duplicate_error = error_str.contains("already exists")
              || error_str.contains("duplicate")
              || error_str.contains("42710");

            if !is_duplicate_error {
              return Err(e.into());
            }
            // If duplicate error, silently continue (user already exists)
          }
        }
      }
    }

    // 授予表权限
    // Grant table permissions
    let grant_sql = format!(
      "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {};",
      username_escaped
    );
    self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        grant_sql,
      ))
      .await?;

    // 授予序列权限（用于自增 ID）
    // Grant sequence permissions (for auto-increment IDs)
    let grant_seq_sql = format!(
      "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {};",
      username_escaped
    );
    self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        grant_seq_sql,
      ))
      .await?;

    // 启用 RLS
    // Enable RLS
    self.enable_rls_for_all_tables().await?;

    // 创建租户策略
    // Create tenant policies
    self.create_all_tenant_policies(username).await?;

    Ok(())
  }

  /// 删除租户用户
  /// Delete tenant user
  async fn delete_tenant_user(&self, username: &str) -> Result<()> {
    // 验证用户名
    // Validate username
    let username_escaped = Self::validate_sql_identifier(username)?;

    // 删除策略
    // Drop policies
    self.drop_all_tenant_policies(username).await?;

    // 撤销权限
    // Revoke permissions
    let revoke_sql = format!(
      "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {};",
      username_escaped
    );
    let _ = self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        revoke_sql,
      ))
      .await;

    // 删除用户
    // Drop user
    let drop_sql = format!("DROP USER IF EXISTS {};", username_escaped);
    self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        drop_sql,
      ))
      .await?;

    Ok(())
  }

  /// 获取租户用户列表
  /// Get tenant user list
  async fn list_tenant_users(&self) -> Result<Vec<String>> {
    // 查询所有非系统用户
    // Query all non-system users
    // Note: This query uses LIKE with hardcoded pattern which is safe as we're not using user input
    // The 'pg_%' pattern is a PostgresSQL system user prefix, and 'postgres' is the default admin user
    // This is a static query with no user-supplied parameters, so it's safe from SQL injection
    let query_sql =
      "SELECT usename FROM pg_user WHERE usename NOT LIKE 'pg_%' AND usename != 'postgres';";

    let result = self
      .db
      .query_all(Statement::from_string(
        self.db.get_database_backend(),
        query_sql,
      ))
      .await?;

    let mut users = Vec::new();
    for row in result {
      if let Ok(username) = row.try_get::<String>("", "usename") {
        users.push(username);
      }
    }

    Ok(users)
  }

  /// 检查租户用户是否存在
  /// Check if tenant user exists
  async fn tenant_user_exists(&self, username: &str) -> Result<bool> {
    // 转义用户名用于查询
    // Escape username for query
    let username_escaped = Self::escape_sql_string(username);
    let query_sql = format!(
      "SELECT 1 FROM pg_user WHERE usename = '{}';",
      username_escaped
    );

    let result = self
      .db
      .query_one(Statement::from_string(
        self.db.get_database_backend(),
        query_sql,
      ))
      .await;

    Ok(result.is_ok())
  }

  /// 更新租户用户配置
  /// Update tenant user configuration
  async fn update_tenant_user(&self, config: &AclConfig) -> Result<()> {
    if !config.enabled {
      return Err(Error::config("ACL feature is not enabled"));
    }

    let username = &config.node_config.username;
    let password = &config.node_config.password;

    // 检查用户是否存在
    // Check if user exists
    if !self.tenant_user_exists(username).await? {
      return Err(Error::config("Tenant user does not exist"));
    }

    // 验证并转义
    // Validate and escape
    let username_escaped = Self::validate_sql_identifier(username)?;
    let password_escaped = Self::escape_sql_string(password);

    // 更新密码
    // Update password
    let alter_sql = format!(
      "ALTER USER {} WITH PASSWORD '{}';",
      username_escaped, password_escaped
    );
    self
      .db
      .execute(Statement::from_string(
        self.db.get_database_backend(),
        alter_sql,
      ))
      .await?;

    // 重新创建策略（删除旧的，创建新的）
    // Recreate policies (drop old, create new)
    self.drop_all_tenant_policies(username).await?;
    self.create_all_tenant_policies(username).await?;

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_validate_sql_identifier() {
    // 有效的标识符
    // Valid identifiers
    assert!(PostgresAclManager::validate_sql_identifier("tenant_user1").is_ok());
    assert!(PostgresAclManager::validate_sql_identifier("_user").is_ok());
    assert!(PostgresAclManager::validate_sql_identifier("User123").is_ok());

    // 无效的标识符
    // Invalid identifiers
    assert!(PostgresAclManager::validate_sql_identifier("").is_err());
    assert!(PostgresAclManager::validate_sql_identifier("123user").is_err());
    assert!(PostgresAclManager::validate_sql_identifier("user-name").is_err());
    assert!(PostgresAclManager::validate_sql_identifier("user name").is_err());
    assert!(PostgresAclManager::validate_sql_identifier("user;DROP TABLE").is_err());
  }

  #[test]
  fn test_escape_sql_string() {
    assert_eq!(
      PostgresAclManager::escape_sql_string("password"),
      "password"
    );
    assert_eq!(
      PostgresAclManager::escape_sql_string("pass'word"),
      "pass''word"
    );
    assert_eq!(PostgresAclManager::escape_sql_string("it's"), "it''s");
  }
}
