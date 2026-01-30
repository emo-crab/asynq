//! PeriodicTaskManager 完整示例：演示如何使用 ConfigProvider 管理周期性任务
//! PeriodicTaskManager complete example: demonstrates how to manage periodic tasks with ConfigProvider
//!
//! 本示例展示如何：
//! This example shows how to:
//! 1. 创建 Scheduler 和 PeriodicTaskManager
//!    Create Scheduler and PeriodicTaskManager
//! 2. 使用 PeriodicTaskConfigProvider 提供任务配置
//!    Use PeriodicTaskConfigProvider to provide task configurations
//! 3. 任务会根据配置自动同步到 Redis
//!    Tasks are automatically synced to Redis based on configuration

/// Simple config provider for demo purposes
/// 用于演示的简单配置提供者
struct SimpleConfigProvider {
  configs: Vec<asynq::components::periodic_task_manager::PeriodicTaskConfig>,
}

#[async_trait::async_trait]
impl asynq::components::periodic_task_manager::PeriodicTaskConfigProvider for SimpleConfigProvider {
  async fn get_configs(
    &self,
  ) -> asynq::error::Result<Vec<asynq::components::periodic_task_manager::PeriodicTaskConfig>> {
    Ok(self.configs.clone())
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  use asynq::scheduler::Scheduler;

  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = asynq::backend::RedisConnectionType::single(redis_url)?;

  println!("创建 Scheduler 和 PeriodicTaskManager");
  println!("Creating Scheduler and PeriodicTaskManager");

  // 创建客户端和调度器
  // Create client and scheduler
  let client = std::sync::Arc::new(asynq::client::Client::new(redis_config).await?);
  let scheduler =
    std::sync::Arc::new(Scheduler::new(client, Some(std::time::Duration::from_secs(10))).await?);

  // 创建配置提供者，包含两个周期性任务
  // Create config provider with two periodic tasks
  let config_provider = std::sync::Arc::new(SimpleConfigProvider {
    configs: vec![
      asynq::components::periodic_task_manager::PeriodicTaskConfig::new(
        "demo:minute_task".to_string(),
        "0 0 * * * *".to_string(), // 每分钟
        b"minute task payload".to_vec(),
        "default".to_string(),
      ),
      asynq::components::periodic_task_manager::PeriodicTaskConfig::new(
        "demo:30sec_task".to_string(),
        "*/30 * * * * *".to_string(), // 每 30 秒
        b"30-second task payload".to_vec(),
        "default".to_string(),
      ),
    ],
  });

  // 创建 PeriodicTaskManager 配置
  // Create PeriodicTaskManager configuration
  let manager_config = asynq::components::periodic_task_manager::PeriodicTaskManagerConfig {
    sync_interval: std::time::Duration::from_secs(10), // 每 10 秒同步一次
  };

  // 创建 PeriodicTaskManager 实例
  // Create PeriodicTaskManager instance
  let manager = std::sync::Arc::new(
    asynq::components::periodic_task_manager::PeriodicTaskManager::new(
      scheduler.clone(),
      manager_config,
      config_provider,
    ),
  );

  println!("PeriodicTaskManager 创建成功");
  println!("PeriodicTaskManager created successfully");

  // 启动 PeriodicTaskManager（它会自动启动 Scheduler）
  // Start PeriodicTaskManager (it automatically starts Scheduler)
  let _manager_handle = manager.clone().start();

  println!("\n示例运行中，将演示任务同步...");
  println!("Example running, demonstrating task synchronization...");
  println!("按 Ctrl+C 退出");
  println!("Press Ctrl+C to exit");

  // 等待一段时间来演示
  // Wait for a while to demonstrate
  tokio::time::sleep(std::time::Duration::from_secs(30)).await;

  // 停止 PeriodicTaskManager（它会自动停止 Scheduler）
  // Stop PeriodicTaskManager (it automatically stops Scheduler)
  manager.shutdown();

  println!("\n示例完成");
  println!("Example completed");

  Ok(())
}
