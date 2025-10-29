//! PeriodicTaskManager 示例：演示如何使用 Scheduler 和 PeriodicTaskConfigProvider
//! PeriodicTaskManager example: demonstrates how to use Scheduler with PeriodicTaskConfigProvider
//!
//! 本示例展示如何：
//! This example shows how to:
//! 1. 创建 Scheduler 和 PeriodicTaskManager
//!    Create Scheduler and PeriodicTaskManager
//! 2. 使用 PeriodicTaskConfigProvider 提供任务配置
//!    Use PeriodicTaskConfigProvider to provide task configurations
//! 3. 让 PeriodicTaskManager 自动同步任务到 Redis
//!    Let PeriodicTaskManager automatically sync tasks to Redis

use async_trait::async_trait;
use asynq::client::Client;
use asynq::components::periodic_task_manager::{
  PeriodicTaskConfig, PeriodicTaskConfigProvider, PeriodicTaskManager, PeriodicTaskManagerConfig,
};
use asynq::config::ServerConfig;
use asynq::redis::RedisConnectionType;
use asynq::scheduler::Scheduler;
use asynq::server::{AsyncHandlerFunc, Server};
use asynq::task::Task;
use std::sync::Arc;
use std::time::Duration;

/// Simple config provider for demo purposes
/// 用于演示的简单配置提供者
struct SimpleConfigProvider {
  configs: Vec<PeriodicTaskConfig>,
}

#[async_trait]
impl PeriodicTaskConfigProvider for SimpleConfigProvider {
  async fn get_configs(&self) -> asynq::error::Result<Vec<PeriodicTaskConfig>> {
    Ok(self.configs.clone())
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = RedisConnectionType::single(redis_url)?;

  println!("创建 Scheduler 和 PeriodicTaskManager");
  println!("Creating Scheduler and PeriodicTaskManager");

  // 创建客户端和调度器
  // Create client and scheduler
  let client = Arc::new(Client::new(redis_config.clone()).await?);
  let scheduler = Arc::new(Scheduler::new(client, Some(Duration::from_secs(10))).await?);

  // 创建配置提供者
  // Create config provider
  let config_provider = Arc::new(SimpleConfigProvider {
    configs: vec![PeriodicTaskConfig::new(
      "demo:periodic_task".to_string(),
      "0/30 * * * * *".to_string(), // Every 30 seconds
      b"periodic payload".to_vec(),
      "default".to_string(),
    )],
  });

  // 创建 PeriodicTaskManager
  // Create PeriodicTaskManager
  let manager_config = PeriodicTaskManagerConfig {
    sync_interval: Duration::from_secs(30),
  };

  let manager = Arc::new(PeriodicTaskManager::new(
    scheduler.clone(),
    manager_config,
    config_provider,
  ));

  println!("  - Sync interval: 30 seconds");
  println!("  - 同步间隔: 30 秒");

  // 启动 PeriodicTaskManager
  // Start PeriodicTaskManager
  let manager_handle = manager.clone().start();

  // 创建服务器处理任务
  // Create server to process tasks
  let server_config = ServerConfig::new().concurrency(4).add_queue("default", 1)?;

  let mut server = Server::new(redis_config, server_config).await?;

  // 定义任务处理器
  // Define task handler
  let handler = AsyncHandlerFunc::new(|task: Task| async move {
    println!("Processing task: {}", task.get_type());
    println!(
      "  Payload: {:?}",
      String::from_utf8_lossy(task.get_payload())
    );

    // 模拟任务处理
    // Simulate task processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("  ✓ Task completed: {}", task.get_type());
    Ok(())
  });

  println!("\n服务器启动中...");
  println!("Starting server...");
  println!("按 Ctrl+C 停止服务器");
  println!("Press Ctrl+C to stop the server");

  // 注意：Scheduler 的启动和停止现在由 PeriodicTaskManager 自动管理
  // Note: Scheduler start and stop are now automatically managed by PeriodicTaskManager

  // 运行服务器
  // Run server
  tokio::select! {
    result = server.run(handler) => {
      result?;
    }
    _ = tokio::signal::ctrl_c() => {
      println!("\n收到停止信号");
      println!("Received shutdown signal");
    }
  }

  // 停止 PeriodicTaskManager（它会自动停止 Scheduler）
  // Stop PeriodicTaskManager (it automatically stops Scheduler)
  manager.shutdown();
  let _ = manager_handle.await;

  println!("\n服务器已停止");
  println!("Server stopped");

  Ok(())
}
