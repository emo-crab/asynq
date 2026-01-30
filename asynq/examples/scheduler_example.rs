//! Scheduler 示例：演示如何通过 PeriodicTaskManager 管理定时任务
//! Scheduler example: demonstrates how to manage periodic tasks via PeriodicTaskManager
//!
//! 注意：Scheduler 的 start 和 stop 方法现在由 PeriodicTaskManager 管理
//! Note: Scheduler's start and stop methods are now managed by PeriodicTaskManager

/// Simple config provider for demo purposes
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
async fn main() {
  use asynq::scheduler::Scheduler;

  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = asynq::backend::RedisConnectionType::single(redis_url).unwrap();

  // 创建 Client 和 Scheduler
  // Create Client and Scheduler
  let client = std::sync::Arc::new(
    asynq::client::Client::new(redis_config.clone())
      .await
      .unwrap(),
  );
  let scheduler = std::sync::Arc::new(Scheduler::new(client.clone(), None).await.unwrap());

  // 创建配置提供者
  // Create config provider
  let config_provider = std::sync::Arc::new(SimpleConfigProvider {
    configs: vec![
      asynq::components::periodic_task_manager::PeriodicTaskConfig::new(
        "demo:periodic_task".to_string(),
        "0/30 * * * * *".to_string(), // 每30秒
        b"hello scheduler".to_vec(),
        "default".to_string(),
      ),
    ],
  });

  // 创建 PeriodicTaskManager（它会管理 Scheduler 的生命周期）
  // Create PeriodicTaskManager (it manages Scheduler's lifecycle)
  let manager_config = asynq::components::periodic_task_manager::PeriodicTaskManagerConfig {
    sync_interval: std::time::Duration::from_secs(30),
  };
  let manager = std::sync::Arc::new(
    asynq::components::periodic_task_manager::PeriodicTaskManager::new(
      scheduler.clone(),
      manager_config,
      config_provider,
    ),
  );

  // 启动 PeriodicTaskManager（它会自动启动 Scheduler）
  // Start PeriodicTaskManager (it automatically starts Scheduler)
  let _manager_handle = manager.clone().start();

  // 等待 Ctrl+C 信号退出
  // Wait for Ctrl+C signal to exit
  println!("Scheduler running via PeriodicTaskManager. Press Ctrl+C to exit...");
  tokio::signal::ctrl_c().await.unwrap();

  // 查询所有调度条目
  // Query all scheduler entries
  let entries = scheduler.list_entries("demo_scheduler").await;
  println!("Scheduler Entries:");
  for entry in entries {
    println!(
      "  id: {}, type: {}, next: {:?}",
      entry.id, entry.task_type, entry.next_enqueue_time
    );
  }

  // 查询调度事件
  // Query scheduler events
  let events = scheduler.list_events(10).await;
  println!("Scheduler Events:");
  for event in events {
    println!(
      "  task_id: {}, enqueue_time: {:?}",
      event.task_id, event.enqueue_time
    );
  }

  // 停止 PeriodicTaskManager（它会自动停止 Scheduler）
  // Stop PeriodicTaskManager (it automatically stops Scheduler)
  manager.shutdown();

  // 给一点时间让 scheduler 完成清理
  // Give scheduler some time to finish cleanup
  tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
