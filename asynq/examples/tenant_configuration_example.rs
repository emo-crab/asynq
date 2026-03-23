//! Tenant configuration example for Scheduler and PeriodicTaskManager
//! 演示 Scheduler 和 PeriodicTaskManager 的租户配置示例
//!
//! This example demonstrates two scenarios:
//! 本示例演示两种场景：
//!
//! Scenario 1: Scheduler with tenant isolation
//! 场景 1：带租户隔离的调度器
//! When tenant configuration is enabled, scheduler keys use the format:
//! 当启用租户配置时，调度器键使用以下格式：
//! `asynq:schedulers:{tenant:hostname:pid:uuid}`
//!
//! Scenario 2: Admin scheduler distributing tasks to multiple tenants
//! 场景 2：管理员调度器分发任务到多个租户
//! An admin scheduler can distribute PeriodicTasks to different tenant queues
//! 管理员调度器可以将周期性任务分发到不同租户的队列
//! by specifying tenant in PeriodicTaskConfig
//! 通过在 PeriodicTaskConfig 中指定租户

use asynq::backend::RedisConnectionType;
use asynq::client::Client;
use asynq::components::periodic_task_manager::{
  PeriodicTaskConfig, PeriodicTaskConfigProvider, PeriodicTaskManager, PeriodicTaskManagerConfig,
};
use asynq::scheduler::Scheduler;
use std::sync::Arc;
use std::time::Duration;

/// Simple config provider for demonstration
/// 用于演示的简单配置提供者
struct TenantConfigProvider {
  _tenant_name: String,
  configs: Vec<PeriodicTaskConfig>,
}

#[async_trait::async_trait]
impl PeriodicTaskConfigProvider for TenantConfigProvider {
  async fn get_configs(&self) -> asynq::error::Result<Vec<PeriodicTaskConfig>> {
    Ok(self.configs.clone())
  }
}

/// Config provider for admin scheduler that distributes to multiple tenants
/// 管理员调度器的配置提供者，分发到多个租户
struct AdminConfigProvider {
  configs: Vec<PeriodicTaskConfig>,
}

#[async_trait::async_trait]
impl PeriodicTaskConfigProvider for AdminConfigProvider {
  async fn get_configs(&self) -> asynq::error::Result<Vec<PeriodicTaskConfig>> {
    Ok(self.configs.clone())
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // Initialize logging
  // 初始化日志
  tracing_subscriber::fmt::init();

  println!("=== Tenant Configuration Example ===");
  println!("=== 租户配置示例 ===\n");

  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = RedisConnectionType::single(redis_url)?;

  // Create client
  // 创建客户端
  let client = Arc::new(Client::new(redis_config).await?);

  println!("=== Scenario 1: Scheduler with Tenant Isolation ===");
  println!("=== 场景 1：带租户隔离的调度器 ===\n");

  // Example 1: Create scheduler with tenant1
  // 示例 1：为 tenant1 创建调度器
  println!("1. Creating scheduler for tenant1...");
  println!("1. 为 tenant1 创建调度器...");

  let scheduler_tenant1 = Arc::new(
    Scheduler::new_with_tenant(
      client.clone(),
      Some(Duration::from_secs(10)),
      Some("tenant1".to_string()),
    )
    .await?,
  );

  println!(
    "   Scheduler ID (tenant1): Starts with 'tenant1:'\n   调度器 ID (tenant1): 以 'tenant1:' 开头"
  );

  // Example 2: Create scheduler with tenant2
  // 示例 2：为 tenant2 创建调度器
  println!("\n2. Creating scheduler for tenant2...");
  println!("2. 为 tenant2 创建调度器...");

  let scheduler_tenant2 = Arc::new(
    Scheduler::new_with_tenant(
      client.clone(),
      Some(Duration::from_secs(10)),
      Some("tenant2".to_string()),
    )
    .await?,
  );

  println!(
    "   Scheduler ID (tenant2): Starts with 'tenant2:'\n   调度器 ID (tenant2): 以 'tenant2:' 开头"
  );

  // Example 3: Create PeriodicTaskManager for tenant1
  // 示例 3：为 tenant1 创建 PeriodicTaskManager
  println!("\n3. Creating PeriodicTaskManager for tenant1...");
  println!("3. 为 tenant1 创建 PeriodicTaskManager...");

  let config_provider_tenant1 = Arc::new(TenantConfigProvider {
    _tenant_name: "tenant1".to_string(),
    configs: vec![PeriodicTaskConfig::new(
      "tenant1:task".to_string(),
      "0/30 * * * * *".to_string(), // Every 30 seconds
      b"tenant1 payload".to_vec(),
      "tenant1:queue".to_string(),
    )],
  });

  let manager_config_tenant1 = PeriodicTaskManagerConfig::new()
    .sync_interval(Duration::from_secs(30));

  let _manager_tenant1 = Arc::new(PeriodicTaskManager::new(
    scheduler_tenant1,
    manager_config_tenant1,
    config_provider_tenant1,
  ));

  println!("   PeriodicTaskManager created for tenant1");
  println!("   已为 tenant1 创建 PeriodicTaskManager");

  // Example 4: Create PeriodicTaskManager for tenant2
  // 示例 4：为 tenant2 创建 PeriodicTaskManager
  println!("\n4. Creating PeriodicTaskManager for tenant2...");
  println!("4. 为 tenant2 创建 PeriodicTaskManager...");

  let config_provider_tenant2 = Arc::new(TenantConfigProvider {
    _tenant_name: "tenant2".to_string(),
    configs: vec![PeriodicTaskConfig::new(
      "tenant2:task".to_string(),
      "0/45 * * * * *".to_string(), // Every 45 seconds
      b"tenant2 payload".to_vec(),
      "tenant2:queue".to_string(),
    )],
  });

  let manager_config_tenant2 = PeriodicTaskManagerConfig::new()
    .sync_interval(Duration::from_secs(30));

  let _manager_tenant2 = Arc::new(PeriodicTaskManager::new(
    scheduler_tenant2,
    manager_config_tenant2,
    config_provider_tenant2,
  ));

  println!("   PeriodicTaskManager created for tenant2");
  println!("   已为 tenant2 创建 PeriodicTaskManager");

  println!("\n=== Scenario 2: Admin Scheduler Distributing to Multiple Tenants ===");
  println!("=== 场景 2：管理员调度器分发到多个租户 ===\n");

  // Example 5: Create admin scheduler (no tenant isolation)
  // 示例 5：创建管理员调度器（无租户隔离）
  println!("5. Creating admin scheduler (no tenant)...");
  println!("5. 创建管理员调度器（无租户）...");

  let admin_scheduler = Arc::new(Scheduler::new(client.clone(), Some(Duration::from_secs(10))).await?);

  println!("   Admin scheduler created");
  println!("   已创建管理员调度器");

  // Example 6: Create tasks for different tenants
  // 示例 6：为不同租户创建任务
  println!("\n6. Creating periodic tasks for different tenants...");
  println!("6. 为不同租户创建周期性任务...");

  let admin_configs = vec![
    // Task for tenant1
    // 租户 1 的任务
    PeriodicTaskConfig::new_with_tenant(
      "admin:task1".to_string(),
      "0/20 * * * * *".to_string(), // Every 20 seconds
      b"task for tenant1".to_vec(),
      "default".to_string(),
      "tenant1".to_string(),
    ),
    // Task for tenant2
    // 租户 2 的任务
    PeriodicTaskConfig::new_with_tenant(
      "admin:task2".to_string(),
      "0/25 * * * * *".to_string(), // Every 25 seconds
      b"task for tenant2".to_vec(),
      "default".to_string(),
      "tenant2".to_string(),
    ),
    // Task for tenant3
    // 租户 3 的任务
    PeriodicTaskConfig::new_with_tenant(
      "admin:task3".to_string(),
      "0/30 * * * * *".to_string(), // Every 30 seconds
      b"task for tenant3".to_vec(),
      "default".to_string(),
      "tenant3".to_string(),
    ),
  ];

  let admin_config_provider = Arc::new(AdminConfigProvider {
    configs: admin_configs,
  });

  let admin_manager_config = PeriodicTaskManagerConfig::new().sync_interval(Duration::from_secs(60));

  let _admin_manager = Arc::new(PeriodicTaskManager::new(
    admin_scheduler,
    admin_manager_config,
    admin_config_provider,
  ));

  println!("   Admin PeriodicTaskManager created");
  println!("   已创建管理员 PeriodicTaskManager");
  println!("   Tasks will be distributed to:");
  println!("   任务将被分发到：");
  println!("     - tenant1:default (every 20s)");
  println!("     - tenant2:default (every 25s)");
  println!("     - tenant3:default (every 30s)");

  // Summary
  // 总结
  println!("\n=== Summary / 总结 ===");
  println!("\nScenario 1: Scheduler Isolation");
  println!("场景 1：调度器隔离");
  println!("  - tenant1 scheduler: asynq:schedulers:{{tenant1:hostname:pid:uuid}}");
  println!("  - tenant2 scheduler: asynq:schedulers:{{tenant2:hostname:pid:uuid}}");
  println!("  Each scheduler manages its own tasks independently.");
  println!("  每个调度器独立管理自己的任务。");

  println!("\nScenario 2: Multi-Tenant Task Distribution");
  println!("场景 2：多租户任务分发");
  println!("  - Admin scheduler (no tenant isolation)");
  println!("  - 管理员调度器（无租户隔离）");
  println!("  - Distributes tasks to different tenant queues:");
  println!("  - 分发任务到不同租户队列：");
  println!("    * admin:task1 → tenant1:default");
  println!("    * admin:task2 → tenant2:default");
  println!("    * admin:task3 → tenant3:default");

  println!("\nNote: To actually start the managers, uncomment the following:");
  println!("注意：要实际启动管理器，请取消注释以下内容：");
  println!("  // let _handle1 = manager_tenant1.clone().start();");
  println!("  // let _handle2 = manager_tenant2.clone().start();");
  println!("  // let _admin_handle = admin_manager.clone().start();");
  println!("  // tokio::time::sleep(Duration::from_secs(120)).await;");

  Ok(())
}
