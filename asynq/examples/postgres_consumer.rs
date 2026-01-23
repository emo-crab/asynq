//! PostgresSQL 消费者示例
//! PostgresSQL Consumer example
//!
//! 演示如何使用 ServerBuilder 处理 PostgresSQL 队列中的任务
//! Demonstrates how to use ServerBuilder to process tasks from PostgresSQL queue

use async_trait::async_trait;
use asynq::components::aggregator::GroupAggregatorFunc;
use asynq::error::{Error, Result};
use asynq::pgdb::PostgresBroker;
use asynq::task::Task;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
/// 自定义聚合器示例 - 将多个任务的 payload 合并
/// Custom aggregator example - combines payloads from multiple tasks
fn aggregate_tasks(group: &str, tasks: Vec<Task>) -> Result<Task> {
  println!(
    "📦 Aggregating {} tasks from group '{}'",
    tasks.len(),
    group
  );

  // 合并所有任务的 payload
  // Combine payloads from all tasks
  let mut combined_payload: Vec<serde_json::Value> = Vec::new();
  for (idx, task) in tasks.iter().enumerate() {
    println!(
      "   Task {}: type='{}', payload size={} bytes",
      idx + 1,
      task.get_type(),
      task.get_payload().len()
    );

    if let Ok(payload_str) = serde_json::from_slice(task.get_payload()) {
      combined_payload.push(payload_str);
    }
  }

  println!("   ✅ Created aggregated task with combined payload");
  let data = serde_json::to_vec(&combined_payload)?;
  // 创建聚合后的任务
  // Create the aggregated task
  Task::new("batch:process", &data)
}

#[derive(Serialize, Deserialize)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}

#[derive(Serialize, Deserialize)]
struct ImageResizePayload {
  src_url: String,
  width: u32,
  height: u32,
}

/// 任务处理器
pub struct TaskProcessor;

#[async_trait]
impl asynq::server::Handler for TaskProcessor {
  async fn process_task(&self, task: Task) -> Result<()> {
    match task.get_type() {
      "email:send" => {
        let payload: EmailPayload = serde_json::from_slice(task.get_payload())
          .map_err(|e| Error::other(format!("Failed to deserialize email:send payload: {}", e)))?;
        self.handle_email_send(payload).await
      }
      "email:reminder" => {
        let payload: EmailPayload = serde_json::from_slice(task.get_payload()).map_err(|e| {
          Error::other(format!(
            "Failed to deserialize email:reminder payload: {}",
            e
          ))
        })?;
        self.handle_email_reminder(payload).await
      }
      "image:resize" => {
        let payload: ImageResizePayload =
          serde_json::from_slice(task.get_payload()).map_err(|e| {
            Error::other(format!("Failed to deserialize image:resize payload: {}", e))
          })?;
        self.handle_image_resize(payload).await
      }
      "report:daily" => {
        let payload: serde_json::Value =
          serde_json::from_slice(task.get_payload()).map_err(|e| {
            Error::other(format!("Failed to deserialize report:daily payload: {}", e))
          })?;
        self.handle_daily_report(payload).await
      }
      "batch:process" => {
        let payload: serde_json::Value =
          serde_json::from_slice(task.get_payload()).map_err(|e| {
            Error::other(format!(
              "Failed to deserialize batch:process payload: {}",
              e
            ))
          })?;
        self.handle_batch_process(payload).await
      }
      "group:process" => {
        let payload: serde_json::Value =
          serde_json::from_slice(task.get_payload()).map_err(|e| {
            Error::other(format!(
              "Failed to deserialize group:process payload: {}",
              e
            ))
          })?;
        self.handle_group_process(payload).await
      }
      _ => {
        println!("❌ Unknown task type: {}", task.get_type());
        Err(Error::other(format!(
          "Unknown task type: {}",
          task.get_type()
        )))
      }
    }
  }
}

impl TaskProcessor {
  async fn handle_email_send(&self, payload: EmailPayload) -> Result<()> {
    println!("📧 Sending email to: {}", payload.to);
    println!("   Subject: {}", payload.subject);
    println!("   Body: {}", payload.body);

    // 模拟邮件发送处理
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✅ Email sent successfully to {}", payload.to);
    Ok(())
  }

  async fn handle_email_reminder(&self, payload: EmailPayload) -> Result<()> {
    println!("⏰ Sending reminder email to: {}", payload.to);
    println!("   Subject: {}", payload.subject);

    // 模拟提醒邮件处理
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✅ Reminder email sent to {}", payload.to);
    Ok(())
  }

  async fn handle_image_resize(&self, payload: ImageResizePayload) -> Result<()> {
    println!("🖼️  Resizing image: {}", payload.src_url);
    println!("   Target size: {}x{}", payload.width, payload.height);

    // 模拟图片处理
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("✅ Image resized successfully: {}", payload.src_url);
    Ok(())
  }

  async fn handle_daily_report(&self, payload: serde_json::Value) -> Result<()> {
    println!("📊 Generating daily report for: {payload}");

    // 模拟报告生成
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✅ Daily report generated successfully");
    Ok(())
  }

  async fn handle_batch_process(&self, payload: serde_json::Value) -> Result<()> {
    println!("🔄 Processing batch item: {payload}");

    // 模拟批处理
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("✅ Batch item processed: {payload}");
    Ok(())
  }

  async fn handle_group_process(&self, payload: serde_json::Value) -> Result<()> {
    println!("🔄 Processing group item: {payload}");

    // 模拟组任务处理
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("✅ Group item processed: {payload}");
    Ok(())
  }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  println!("🚀 Starting Asynq worker with PostgresSQL...");

  // 创建 PostgresSQL 配置 - 优先从环境变量中读取，否则使用本地 PostgresSQL
  let database_url = std::env::var("DATABASE_URL")
    .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/asynq".to_string());
  println!("🔗 Using PostgresSQL URL: {database_url}");

  // 创建 PostgresSQL 经纪人
  let broker = Arc::new(PostgresBroker::new(&database_url).await?);
  println!("✅ Connected to PostgresSQL");

  // 配置队列优先级
  let mut queues = HashMap::new();
  queues.insert("critical".to_string(), 6); // 最高优先级
  queues.insert("default".to_string(), 3); // 默认优先级
  queues.insert("image_processing".to_string(), 2); // 图片处理队列
  queues.insert("low".to_string(), 1); // 低优先级

  // 创建服务器配置
  let server_config = asynq::config::ServerConfig::new()
    .concurrency(4) // 4 个并发工作者
    .queues(queues)
    .strict_priority(false) // 不使用严格优先级
    .enable_group_aggregator(true)
    .task_check_interval(Duration::from_secs(1))
    .shutdown_timeout(Duration::from_secs(10));

  // 使用 ServerBuilder 创建服务器
  // Use ServerBuilder to create server
  // postgres_broker() 会自动创建 PostgresInspector 用于任务监控
  // postgres_broker() automatically creates PostgresInspector for task monitoring
  let mut server = asynq::server::ServerBuilder::new()
    .postgres_broker(broker)
    .server_config(server_config)
    .build()
    .await?;
  // 设置组聚合器
  // Set group aggregator
  println!("📦 Setting up group aggregator function...");
  let aggregator = GroupAggregatorFunc::new(aggregate_tasks);
  server.set_group_aggregator(aggregator);
  println!("   ✅ Group aggregator configured");
  // 创建任务处理器
  let handler = TaskProcessor;

  // 运行服务器
  println!("🔄 Server is running and waiting for tasks...");
  println!("Press Ctrl+C to gracefully shutdown");

  server.run(handler).await?;

  println!("👋 Server shutdown complete");

  Ok(())
}
