//! ACL 多租户示例
//! ACL multi-tenancy example
//!
//! 演示如何使用 asynq ACL 模块创建和管理 Redis 多租户用户
//! Demonstrates how to use asynq ACL module to create and manage Redis multi-tenant users
//!
//! 运行此示例需要 Redis 服务器具有 ACL 支持（Redis 6.0+）并且使用管理员权限连接
//! Running this example requires a Redis server with ACL support (Redis 6.0+) and admin connection
//!
//! ```bash
//! # 运行此示例
//! # Run this example
//! cargo run --example acl_example --features acl
//! ```

use asynq::acl::{AclConfig, AclManager, NodeConfig, RedisAclManager};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();
  let node_config = NodeConfig::new(
    "localhost:6379", // Redis 地址 / Redis address
    "tenant1",        // 用户名（同时作为 ACL 前缀）/ Username (also used as ACL prefix)
    "secure_pass123", // 密码 / Password
    0,                // 数据库编号 / Database number
  );
  let acl_config = AclConfig::new(node_config.clone()).enable(true);
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

  // 尝试连接到 Redis
  // Try to connect to Redis
  match RedisAclManager::from_url(&redis_url).await {
    Ok(manager) => {
      println!("   ✅ Connected to Redis at: {redis_url}");
      // 注意：以下操作需要管理员权限
      // Note: The following operations require admin privileges

      // 检查用户是否存在
      // Check if user exists
      match manager.tenant_user_exists(&node_config.username).await {
        Ok(exists) => {
          if exists {
            println!("   User '{}' already exists", node_config.username);
          } else {
            println!("   User '{}' does not exist", node_config.username);
            // 创建租户用户（需要管理员权限）
            // Create tenant user (requires admin privileges)
            match manager.create_tenant_user(&acl_config).await {
              Ok(_) => println!("   ✅ Created user '{}'", node_config.username),
              Err(e) => println!("   ❌ Failed to create user: {e}"),
            }
          }
        }
        Err(e) => {
          println!("   ⚠️  Could not check user existence: {e}");
        }
      }

      // 列出所有用户
      // List all users
      match manager.list_tenant_users().await {
        Ok(users) => {
          println!("   Current ACL users ({} total):", users.len());
          for user in users.iter().take(5) {
            // 只显示前5个用户 / Only show first 5 users
            println!("     - {user}");
          }
          if users.len() > 5 {
            println!("     ... and {} more", users.len() - 5);
          }
        }
        Err(e) => {
          println!("   ⚠️  Could not list users: {e}");
        }
      }
    }
    Err(e) => {
      println!("   ⚠️  Could not connect to Redis: {e}");
      println!("   Make sure Redis is running at: {redis_url}");
      println!("\n   Showing ACL command that would be generated:");

      // 即使没有连接，也可以显示配置信息
      // Even without connection, we can show configuration info
      println!("   ACL SETUSER {} on +@all -@dangerous +keys +info|memory +info|clients -select +select|{} >*** {:?}",
        acl_config.node_config.username,
        acl_config.node_config.db,
        acl_config.node_config.asynq_key_pattern()
      );
    }
  }
  Ok(())
}
