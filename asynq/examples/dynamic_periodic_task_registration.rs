//! Example: Dynamic periodic task registration with PeriodicTaskConfigProvider
//! 示例：使用 PeriodicTaskConfigProvider 动态注册周期性任务
//!
//! This example demonstrates how to use PeriodicTaskConfigProvider to dynamically
//! sync periodic tasks with the Scheduler.
//! 此示例演示如何使用 PeriodicTaskConfigProvider 动态同步周期性任务到 Scheduler。
//!
//! The PeriodicTaskManager polls the config provider and automatically registers/unregisters
//! tasks based on configuration changes.
//! PeriodicTaskManager 会轮询配置提供者，并根据配置变更自动注册/注销任务。

use asynq::client::Client;
use asynq::components::periodic_task_manager::{
  PeriodicTaskConfig, PeriodicTaskConfigProvider, PeriodicTaskManager, PeriodicTaskManagerConfig,
};
use asynq::error::Result;
use asynq::redis::RedisConnectionConfig;
use asynq::scheduler::Scheduler;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Example config provider that returns a static list of tasks
/// 示例配置提供者，返回静态任务列表
struct ExampleConfigProvider {
  configs: Arc<Mutex<Vec<PeriodicTaskConfig>>>,
}

impl ExampleConfigProvider {
  fn new(configs: Vec<PeriodicTaskConfig>) -> Self {
    Self {
      configs: Arc::new(Mutex::new(configs)),
    }
  }

  /// Update the configuration dynamically
  /// 动态更新配置
  async fn update_configs(&self, configs: Vec<PeriodicTaskConfig>) {
    let mut c = self.configs.lock().await;
    *c = configs;
  }
}

#[async_trait]
impl PeriodicTaskConfigProvider for ExampleConfigProvider {
  async fn get_configs(&self) -> Result<Vec<PeriodicTaskConfig>> {
    let configs = self.configs.lock().await;
    Ok(configs.clone())
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  // Redis connection URL - can be configured via environment variable
  // Redis 连接 URL - 可以通过环境变量配置
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
  let redis_config = RedisConnectionConfig::single(redis_url)?;

  println!("Creating Scheduler and PeriodicTaskManager with ConfigProvider");
  println!("创建 Scheduler 和使用 ConfigProvider 的 PeriodicTaskManager");

  // Create client and scheduler
  // 创建客户端和调度器
  let client = Arc::new(Client::new(redis_config).await?);
  let scheduler = Arc::new(Scheduler::new(client, Some(Duration::from_secs(10))).await?);

  // Create config provider with initial tasks
  // 创建带有初始任务的配置提供者
  let initial_configs = vec![
    PeriodicTaskConfig::new(
      "demo:task1".to_string(),
      "0 */30 * * * *".to_string(), // Every 30 seconds (sec min hour day month dow)
      b"Task 1 payload".to_vec(),
      "default".to_string(),
    ),
    PeriodicTaskConfig::new(
      "demo:task2".to_string(),
      "0 */45 * * * *".to_string(), // Every 45 seconds
      b"Task 2 payload".to_vec(),
      "default".to_string(),
    ),
  ];

  let config_provider = Arc::new(ExampleConfigProvider::new(initial_configs));

  // Create and start PeriodicTaskManager
  // 创建并启动 PeriodicTaskManager
  let manager_config = PeriodicTaskManagerConfig {
    sync_interval: Duration::from_secs(5), // Sync every 5 seconds
  };

  let manager = Arc::new(PeriodicTaskManager::new(
    scheduler.clone(),
    manager_config,
    config_provider.clone(),
  ));

  println!("\n=== Starting PeriodicTaskManager ===");
  println!("=== 启动 PeriodicTaskManager ===\n");
  
  let manager_handle = manager.clone().start();

  // Wait for initial sync
  // 等待初始同步
  tokio::time::sleep(Duration::from_secs(2)).await;

  println!("✓ PeriodicTaskManager started and synced initial tasks");
  println!("✓ PeriodicTaskManager 已启动并同步初始任务");

  // Demonstrate dynamic config update - remove task1 and add task3
  // 演示动态配置更新 - 移除 task1 并添加 task3
  println!("\n=== Updating configuration (removing task1, adding task3) ===");
  println!("=== 更新配置（移除 task1，添加 task3）===");

  let updated_configs = vec![
    PeriodicTaskConfig::new(
      "demo:task2".to_string(),
      "0 */45 * * * *".to_string(),
      b"Task 2 payload".to_vec(),
      "default".to_string(),
    ),
    PeriodicTaskConfig::new(
      "demo:task3".to_string(),
      "0 */60 * * * *".to_string(), // Every 60 seconds
      b"Task 3 payload".to_vec(),
      "default".to_string(),
    ),
  ];

  config_provider.update_configs(updated_configs).await;

  // Wait for sync to happen
  // 等待同步发生
  tokio::time::sleep(Duration::from_secs(6)).await;

  println!("✓ Configuration updated and synced");
  println!("✓ 配置已更新并同步");

  // Shutdown
  // 关闭
  println!("\n=== Shutting down ===");
  println!("=== 关闭 ===");

  manager.shutdown();
  let _ = manager_handle.await;

  println!("\n✓ Example completed successfully!");
  println!("✓ 示例成功完成！");
  println!("\nNote: The PeriodicTaskManager automatically syncs tasks from the ConfigProvider.");
  println!("注意：PeriodicTaskManager 会自动从 ConfigProvider 同步任务。");
  println!("This pattern is compatible with the Go version's PeriodicTaskManager design.");
  println!("此模式与 Go 版本的 PeriodicTaskManager 设计兼容。");

  Ok(())
}
