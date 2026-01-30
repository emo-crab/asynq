//! Scheduler 示例：演示如何通过 PeriodicTaskManager 使用 TaskOptions 注册定时任务
//! Scheduler example: demonstrates how to register periodic tasks with TaskOptions via PeriodicTaskManager
//!
//! 注意：Scheduler 的 start 和 stop 方法现在由 PeriodicTaskManager 管理
//! Note: Scheduler's start and stop methods are now managed by PeriodicTaskManager

/// Config provider for tasks with custom options
struct OptionsConfigProvider {
  configs: Vec<asynq::components::periodic_task_manager::PeriodicTaskConfig>,
}

#[async_trait::async_trait]
impl asynq::components::periodic_task_manager::PeriodicTaskConfigProvider
  for OptionsConfigProvider
{
  async fn get_configs(
    &self,
  ) -> asynq::error::Result<Vec<asynq::components::periodic_task_manager::PeriodicTaskConfig>> {
    Ok(self.configs.clone())
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  use asynq::scheduler::Scheduler;

  use asynq::backend::option::TaskOptions;
  use asynq::backend::RedisConnectionType;

  use asynq::scheduler::PeriodicTask;

  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = RedisConnectionType::single(redis_url)?;

  // 创建 Client 和 RedisBroker
  let client = std::sync::Arc::new(asynq::client::Client::new(redis_config.clone()).await?);

  // 创建 Scheduler
  let scheduler = std::sync::Arc::new(Scheduler::new(client.clone(), None).await?);

  // 示例 1: 简单的周期性任务配置
  println!("📝 注册简单的周期性任务...");
  let simple_config = asynq::components::periodic_task_manager::PeriodicTaskConfig::new(
    "email:newsletter".to_string(),
    "0 0 9 * * *".to_string(), // 每天上午9点
    b"Send daily newsletter".to_vec(),
    "default".to_string(),
  );
  println!("✅ 简单任务配置已创建");

  // 示例 2: 自定义选项的周期性任务
  println!("\n📝 注册带自定义选项的周期性任务...");
  let mut custom_opts = TaskOptions {
    queue: "critical".to_string(),
    ..Default::default()
  };
  custom_opts.max_retry = 10;
  custom_opts.timeout = Some(std::time::Duration::from_secs(120));
  custom_opts.retention = Some(std::time::Duration::from_secs(3600));
  custom_opts.task_id = Some("backup-daily-001".to_string());

  let _custom_task = PeriodicTask::new_with_options(
    "backup:daily".to_string(),
    "0 0 2 * * *".to_string(), // 每天凌晨2点
    b"Perform daily backup".to_vec(),
    custom_opts.clone(),
  )?;
  println!("✅ 带自定义选项的任务已创建");

  // 示例 3: 演示选项字符串化（stringify_options）
  println!("\n🔍 演示选项字符串化:");
  let option_strings = Scheduler::stringify_options(&custom_opts);
  for opt_str in &option_strings {
    println!("  - {opt_str}");
  }

  // 示例 4: 演示选项解析（parse_options）
  println!("\n🔍 演示选项解析:");
  let parsed_opts = Scheduler::parse_options(&option_strings);
  println!("  解析后的队列: {}", parsed_opts.queue);
  println!("  解析后的最大重试: {}", parsed_opts.max_retry);
  println!("  解析后的超时: {:?}", parsed_opts.timeout);
  println!("  解析后的保留时间: {:?}", parsed_opts.retention);

  // 创建配置提供者
  let config_provider = std::sync::Arc::new(OptionsConfigProvider {
    configs: vec![
      simple_config,
      asynq::components::periodic_task_manager::PeriodicTaskConfig::new(
        "backup:daily".to_string(),
        "0 0 2 * * *".to_string(),
        b"Perform daily backup".to_vec(),
        "critical".to_string(),
      ),
    ],
  });

  // 创建 PeriodicTaskManager（它会管理 Scheduler 的生命周期）
  let manager_config = asynq::components::periodic_task_manager::PeriodicTaskManagerConfig {
    sync_interval: std::time::Duration::from_secs(10),
  };
  let manager = std::sync::Arc::new(
    asynq::components::periodic_task_manager::PeriodicTaskManager::new(
      scheduler.clone(),
      manager_config,
      config_provider,
    ),
  );

  // 启动 PeriodicTaskManager（它会自动启动 Scheduler）
  let _manager_handle = manager.clone().start();

  println!("\n🚀 调度器已通过 PeriodicTaskManager 启动，按 Ctrl+C 退出...");

  // 等待一段时间来演示
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // 停止 PeriodicTaskManager（它会自动停止 Scheduler）
  println!("\n🛑 停止调度器...");
  manager.shutdown();

  // 给一点时间让 scheduler 完成清理
  tokio::time::sleep(std::time::Duration::from_millis(500)).await;

  println!("✅ 调度器已停止");

  Ok(())
}
