//! PostgreSQL Scheduler 示例
//! PostgreSQL Scheduler example
//!
//! 演示如何使用 PostgreSQL 后端的 Scheduler 功能
//! Demonstrates how to use Scheduler with PostgreSQL backend

use asynq::client::Client;
use asynq::scheduler::{PeriodicTask, Scheduler};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}
#[cfg(not(feature = "postgres"))]
fn main() {}
#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  // 创建 PostgreSQL 配置
  // Create PostgreSQL config
  let database_url = std::env::var("DATABASE_URL")
    .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/asynq".to_string());
  println!("🔗 Using PostgreSQL URL: {database_url}");

  // 创建 PostgreSQL 客户端
  // Create PostgreSQL client
  let client = std::sync::Arc::new(Client::new_with_postgres(&database_url).await?);
  println!("✅ Connected to PostgreSQL");

  // 创建调度器
  // Create scheduler
  let scheduler = std::sync::Arc::new(Scheduler::new(client.clone(), None).await?);
  let scheduler_id = "postgres_demo_scheduler";

  // 添加周期性任务 - 每分钟发送一封邮件
  // Add periodic task - send an email every minute
  let email_payload = EmailPayload {
    to: "admin@example.com".to_string(),
    subject: "Daily Report".to_string(),
    body: "Here is your daily report!".to_string(),
  };

  let email_payload_bin = serde_json::to_vec(&email_payload)?;

  let task = PeriodicTask::new(
    "email:send".to_string(),
    "* * * * * *".to_string(), // 每分钟执行 (Every minute)
    email_payload_bin,
    "default".to_string(),
  )?;

  let _entry_id1 = scheduler.register(task, "default").await?;
  println!("📝 Registered periodic email task");

  // 添加另一个任务 - 每5分钟执行一次
  // Add another task - runs every 5 minutes
  let report_payload = serde_json::to_vec(&serde_json::json!({
    "type": "system_report"
  }))?;

  let report_task = PeriodicTask::new(
    "report:generate".to_string(),
    "*/5 * * * * *".to_string(), // 每5分钟执行 (Every 5 minutes)
    report_payload,
    "default".to_string(),
  )?;

  let _entry_id2 = scheduler.register(report_task, "default").await?;
  println!("📝 Registered periodic report task");

  // 启动调度器
  // Start scheduler
  scheduler.start().await;
  println!("🚀 Scheduler started!");

  // 列出所有已注册的任务
  // List all registered tasks
  let entries = scheduler.list_entries(scheduler_id).await;
  println!("📋 Registered scheduler entries: {}", entries.len());
  for entry in &entries {
    println!("  - Task: {} (cron: {})", entry.task_type, entry.spec);
  }

  // 运行一段时间
  // Run for a while
  println!("⏰ Scheduler will run for 30 seconds...");
  tokio::time::sleep(std::time::Duration::from_secs(30)).await;

  // 列出调度事件历史
  // List scheduler event history
  let events = scheduler.list_events(10).await;
  println!("📊 Scheduler events: {}", events.len());
  for event in &events {
    println!("  - Enqueued task: {}", event.task_id);
  }

  // 停止调度器
  // Stop scheduler
  scheduler.stop().await;
  println!("🛑 Scheduler stopped");

  Ok(())
}
