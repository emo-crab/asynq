//! Group Task Producer 示例
//! Group Task Producer example
//!
//! 演示如何创建带有组标签的任务以进行批量聚合
//! Demonstrates how to create tasks with group labels for batch aggregation

use asynq::redis::RedisConnectionConfig;
use asynq::{client::Client, task::Task};
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  println!("🚀 Starting Group Task Producer...");
  println!("   This producer creates tasks with group labels for aggregation");
  println!();

  // 创建 Redis 配置
  // Create Redis configuration
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");

  let redis_config = RedisConnectionConfig::single(redis_url)?;

  // 创建客户端
  // Create client
  let client = Client::new(redis_config).await?;
  println!("✅ Client connected");
  println!();

  // 示例 1: 创建带有组标签的电子邮件任务
  // Example 1: Create email tasks with group labels
  println!("📧 Creating email tasks with group 'daily-digest'...");

  for i in 1..=6 {
    let payload = EmailPayload {
      to: format!("user{i}@example.com"),
      subject: "Daily Digest".to_string(),
      body: format!("Your daily digest #{i}"),
    };

    let task = Task::new(
      "email:send",
      &serde_json::to_vec(&payload).unwrap_or_default(),
    )?
    .with_queue("default")
    .with_group("daily-digest") // 设置组标签 / Set group label
    .with_group_grace_period(Duration::from_secs(5)); // 设置组宽限期 / Set group grace period

    let task_info = client.enqueue(task).await?;
    println!("   ✅ Enqueued task {} with ID: {}", i, task_info.id);
  }

  println!();
  println!("📊 All tasks created!");
  println!();
  println!("💡 These tasks will be aggregated by the server:");
  println!("   • Group: 'daily-digest'");
  println!("   • Grace period: 5 seconds");
  println!("   • Once conditions are met (grace period, max size, or max delay),");
  println!("     the GroupAggregator will combine them into a single batch task");
  println!();

  // 示例 2: 创建另一个组的任务
  // Example 2: Create tasks for another group
  println!("📧 Creating email tasks with group 'weekly-report'...");

  for i in 1..=3 {
    let payload = EmailPayload {
      to: format!("manager{i}@example.com"),
      subject: "Weekly Report".to_string(),
      body: format!("Your weekly report #{i}"),
    };

    let task = Task::new(
      "email:send",
      &serde_json::to_vec(&payload).unwrap_or_default(),
    )?
    .with_queue("default")
    .with_group("weekly-report") // 不同的组 / Different group
    .with_group_grace_period(Duration::from_secs(5));

    let task_info = client.enqueue(task).await?;
    println!("   ✅ Enqueued task {} with ID: {}", i, task_info.id);
  }

  println!();
  println!("✨ Producer finished!");
  println!();
  println!("📝 Next steps:");
  println!("   1. Start the consumer with group aggregator enabled:");
  println!("      cargo run --example group_aggregator_example");
  println!("   2. Watch as tasks are aggregated into batch tasks");
  println!("   3. The aggregator will combine tasks from the same group");
  println!();

  Ok(())
}
