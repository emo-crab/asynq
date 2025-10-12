//! Scheduler 示例：演示如何使用 TaskOptions 注册定时任务
//! Scheduler example: demonstrates how to register periodic tasks with TaskOptions

use asynq::client::Client;
use asynq::rdb::option::TaskOptions;
use asynq::scheduler::{PeriodicTask, Scheduler};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // 初始化 RedisConfig
  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = asynq::redis::RedisConfig::from_url(redis_url)?;

  // 创建 Client 和 RedisBroker
  let client = Arc::new(Client::new(redis_config.clone()).await?);

  // 创建 Scheduler
  let scheduler = Scheduler::new(client.clone(), None).await?;

  // 示例 1: 使用默认选项的周期性任务
  println!("📝 注册简单的周期性任务...");
  let simple_task = PeriodicTask::new(
    "email:newsletter".to_string(),
    "0 0 9 * * *".to_string(), // 每天上午9点
    b"Send daily newsletter".to_vec(),
    "default".to_string(),
  )?;
  let _ = scheduler.register(simple_task, "default").await?;
  println!("✅ 简单任务已注册");

  // 示例 2: 使用自定义选项的周期性任务
  println!("\n📝 注册带自定义选项的周期性任务...");
  let mut custom_opts = TaskOptions {
    queue: "critical".to_string(),
    ..Default::default()
  };
  custom_opts.max_retry = 10;
  custom_opts.timeout = Some(Duration::from_secs(120));
  custom_opts.retention = Some(Duration::from_secs(3600));
  custom_opts.task_id = Some("backup-daily-001".to_string());

  let custom_task = PeriodicTask::new_with_options(
    "backup:daily".to_string(),
    "0 0 2 * * *".to_string(), // 每天凌晨2点
    b"Perform daily backup".to_vec(),
    custom_opts.clone(),
  )?;
  let _ = scheduler.register(custom_task, "critical").await?;
  println!("✅ 带自定义选项的任务已注册");

  // 示例 3: 演示选项字符串化（stringify_options）
  println!("\n🔍 演示选项字符串化:");
  let option_strings = Scheduler::stringify_options(&custom_opts);
  for opt_str in &option_strings {
    println!("  - {}", opt_str);
  }

  // 示例 4: 演示选项解析（parse_options）
  println!("\n🔍 演示选项解析:");
  let parsed_opts = Scheduler::parse_options(&option_strings);
  println!("  解析后的队列: {}", parsed_opts.queue);
  println!("  解析后的最大重试: {}", parsed_opts.max_retry);
  println!("  解析后的超时: {:?}", parsed_opts.timeout);
  println!("  解析后的保留时间: {:?}", parsed_opts.retention);

  // 启动调度器
  let mut scheduler = scheduler;
  scheduler.start();

  println!("\n🚀 调度器已启动，按 Ctrl+C 退出...");

  // 等待一段时间来演示
  tokio::time::sleep(Duration::from_secs(5)).await;

  // 停止调度器
  println!("\n🛑 停止调度器...");
  scheduler.stop().await;
  println!("✅ 调度器已停止");

  Ok(())
}
