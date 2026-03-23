//! Admin Scheduler Multi-Tenant Distribution Example
//! 管理员调度器多租户分发示例
//!
//! This example demonstrates how a single admin scheduler can distribute
//! periodic tasks to different tenant queues.
//! 本示例演示单个管理员调度器如何将周期性任务分发到不同的租户队列。
//!
//! The admin scheduler runs with high privileges and centrally manages
//! periodic tasks for multiple tenants.
//! 管理员调度器以高权限运行，集中管理多个租户的周期性任务。

use asynq::backend::RedisConnectionType;
use asynq::client::Client;
use asynq::components::periodic_task_manager::{
  PeriodicTaskConfig, PeriodicTaskConfigProvider, PeriodicTaskManager, PeriodicTaskManagerConfig,
};
use asynq::scheduler::Scheduler;
use std::sync::Arc;
use std::time::Duration;

/// Admin config provider that manages tasks for multiple tenants
/// 管理多个租户任务的管理员配置提供者
struct AdminMultiTenantConfigProvider {
  configs: Vec<PeriodicTaskConfig>,
}

#[async_trait::async_trait]
impl PeriodicTaskConfigProvider for AdminMultiTenantConfigProvider {
  async fn get_configs(&self) -> asynq::error::Result<Vec<PeriodicTaskConfig>> {
    Ok(self.configs.clone())
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // Initialize logging
  tracing_subscriber::fmt::init();

  println!("=== Admin Scheduler Multi-Tenant Distribution ===");
  println!("=== 管理员调度器多租户分发 ===\n");

  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = RedisConnectionType::single(redis_url)?;
  let client = Arc::new(Client::new(redis_config).await?);

  println!("Creating admin scheduler (no tenant isolation)...");
  println!("创建管理员调度器（无租户隔离）...\n");

  // Create admin scheduler without tenant isolation
  // This scheduler runs with admin privileges
  let admin_scheduler = Arc::new(Scheduler::new(client, Some(Duration::from_secs(10))).await?);

  println!("Configuring periodic tasks for multiple tenants...");
  println!("配置多个租户的周期性任务...\n");

  // Create tasks for different tenants
  let multi_tenant_configs = vec![
    // Daily report task for tenant1
    PeriodicTaskConfig::new_with_tenant(
      "daily_report".to_string(),
      "0 0 8 * * *".to_string(), // Every day at 8:00 AM
      b"Generate daily report for tenant1".to_vec(),
      "reports".to_string(),
      "tenant1".to_string(),
    ),
    // Hourly sync task for tenant1
    PeriodicTaskConfig::new_with_tenant(
      "hourly_sync".to_string(),
      "0 0 * * * *".to_string(), // Every hour
      b"Sync data for tenant1".to_vec(),
      "sync".to_string(),
      "tenant1".to_string(),
    ),
    // Daily report task for tenant2
    PeriodicTaskConfig::new_with_tenant(
      "daily_report".to_string(),
      "0 0 9 * * *".to_string(), // Every day at 9:00 AM
      b"Generate daily report for tenant2".to_vec(),
      "reports".to_string(),
      "tenant2".to_string(),
    ),
    // Email notification task for tenant2
    PeriodicTaskConfig::new_with_tenant(
      "email_notification".to_string(),
      "0 */30 * * * *".to_string(), // Every 30 minutes
      b"Send email notifications for tenant2".to_vec(),
      "notifications".to_string(),
      "tenant2".to_string(),
    ),
    // Cleanup task for tenant3
    PeriodicTaskConfig::new_with_tenant(
      "cleanup".to_string(),
      "0 0 2 * * *".to_string(), // Every day at 2:00 AM
      b"Clean up old data for tenant3".to_vec(),
      "maintenance".to_string(),
      "tenant3".to_string(),
    ),
  ];

  println!("Task distribution plan:");
  println!("任务分发计划：\n");
  println!("Tenant1:");
  println!("  - daily_report → tenant1:reports (every day at 8:00)");
  println!("  - hourly_sync → tenant1:sync (every hour)");
  println!("\nTenant2:");
  println!("  - daily_report → tenant2:reports (every day at 9:00)");
  println!("  - email_notification → tenant2:notifications (every 30 min)");
  println!("\nTenant3:");
  println!("  - cleanup → tenant3:maintenance (every day at 2:00)\n");

  // Create admin config provider
  let admin_config_provider = Arc::new(AdminMultiTenantConfigProvider {
    configs: multi_tenant_configs,
  });

  // Create admin manager configuration
  let admin_manager_config = PeriodicTaskManagerConfig::new().sync_interval(Duration::from_secs(60));

  // Create admin periodic task manager
  let _admin_manager = Arc::new(PeriodicTaskManager::new(
    admin_scheduler,
    admin_manager_config,
    admin_config_provider,
  ));

  println!("Admin PeriodicTaskManager created successfully!");
  println!("管理员 PeriodicTaskManager 创建成功！\n");

  println!("=== Summary / 总结 ===");
  println!("\nKey Features / 主要特性:");
  println!("1. Single admin scheduler manages all tenant tasks");
  println!("   单个管理员调度器管理所有租户任务");
  println!("\n2. Each task specifies its target tenant via PeriodicTaskConfig");
  println!("   每个任务通过 PeriodicTaskConfig 指定目标租户");
  println!("\n3. Tasks are automatically distributed to tenant-specific queues:");
  println!("   任务自动分发到租户专用队列：");
  println!("   - tenant1 tasks → tenant1:* queues");
  println!("   - tenant2 tasks → tenant2:* queues");
  println!("   - tenant3 tasks → tenant3:* queues");
  println!("\n4. Complete queue isolation between tenants");
  println!("   租户之间的队列完全隔离");

  println!("\n=== Usage / 使用方法 ===");
  println!("\nTo create a task for a specific tenant:");
  println!("为特定租户创建任务：");
  println!("\n```rust");
  println!("let config = PeriodicTaskConfig::new_with_tenant(");
  println!("    \"task_name\".to_string(),");
  println!("    \"0/30 * * * * *\".to_string(),  // cron expression");
  println!("    b\"task payload\".to_vec(),");
  println!("    \"queue_name\".to_string(),");
  println!("    \"tenant_name\".to_string(),      // <-- specify tenant");
  println!(");");
  println!("```");

  println!("\nAlternatively, use the builder pattern:");
  println!("或者，使用构建器模式：");
  println!("\n```rust");
  println!("let config = PeriodicTaskConfig::new(...)");
  println!("    .with_tenant(\"tenant_name\".to_string());");
  println!("```");

  println!("\n=== To Start / 启动方式 ===");
  println!("\nUncomment the following to start the admin manager:");
  println!("取消注释以下内容以启动管理员管理器：");
  println!("\n// let _handle = admin_manager.clone().start();");
  println!("// tokio::time::sleep(Duration::from_secs(300)).await;");
  println!("// admin_manager.shutdown();");

  Ok(())
}
