//! 消费者示例
//! Consumer example
//!
//! 演示如何使用 asynq 服务器处理任务
//! Demonstrates how to use asynq server to process tasks

use async_trait::async_trait;
use asynq::components::aggregator::GroupAggregatorFunc;
use asynq::error::{Error, Result};
use asynq::task::Task;
use asynq::{
  config::ServerConfig,
  redis::RedisConfig,
  server::{Handler, ServerBuilder},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

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
impl Handler for TaskProcessor {
  async fn process_task(&self, task: Task) -> Result<()> {
    match task.get_type() {
      "email:send" => {
        let payload: EmailPayload = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_email_send(payload).await
      }
      "email:reminder" => {
        let payload: EmailPayload = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_email_reminder(payload).await
      }
      "image:resize" => {
        let payload: ImageResizePayload = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_image_resize(payload).await
      }
      "report:daily" => {
        let payload: serde_json::Value = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_daily_report(payload).await
      }
      "batch:process" => {
        let payload: serde_json::Value = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_batch_process(payload).await
      }
      "payment:process" => {
        let payload: serde_json::Value = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_payment_process(payload).await
      }
      "image:process" => {
        let payload: ImageResizePayload = serde_json::from_slice(task.get_payload()).unwrap();
        self.handle_image_process(payload).await
      }
      "demo:periodic_task" => {
        println!(
          "⏰ Received demo:periodic_task, payload: {:?}",
          String::from_utf8_lossy(task.get_payload())
        );
        Ok(())
      }
      _ => {
        println!("Unknown task type: {}", task.get_type());
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

    // 模拟邮件发送处理（延长耗时以测试任务取消）
    tokio::time::sleep(Duration::from_secs(10)).await;

    println!("✅ Email sent successfully to {}", payload.to);
    Ok(())
  }

  async fn handle_email_reminder(&self, payload: EmailPayload) -> Result<()> {
    println!("⏰ Sending reminder email to: {}", payload.to);
    println!("   Subject: {}", payload.subject);

    // 模拟提醒邮件处理
    tokio::time::sleep(Duration::from_secs(50)).await;

    println!("✅ Reminder email sent to {}", payload.to);
    Ok(())
  }

  async fn handle_image_resize(&self, payload: ImageResizePayload) -> Result<()> {
    println!("🖼️  Resizing image: {}", payload.src_url);
    println!("   Target size: {}x{}", payload.width, payload.height);

    // 模拟图片处理（延长耗时以测试任务取消）
    tokio::time::sleep(Duration::from_secs(60)).await;

    println!("✅ Image resized successfully: {}", payload.src_url);
    Ok(())
  }

  async fn handle_daily_report(&self, payload: serde_json::Value) -> Result<()> {
    println!("📊 Generating daily report for: {}", payload);

    // 模拟报告生成
    tokio::time::sleep(Duration::from_secs(20)).await;

    println!("✅ Daily report generated successfully");
    Ok(())
  }

  async fn handle_batch_process(&self, payload: serde_json::Value) -> Result<()> {
    use redis::AsyncCommands;
    let group = "daily_batch";
    let redis_url =
      std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    // 每处理一个任务，计数器+1
    let count: i32 = conn.incr(format!("group:{}:count", group), 1).await?;
    // 假设你知道组内任务总数为5
    if count == 5 {
      println!("Group {} completed! Do aggregation here.", group);
      // 执行聚合逻辑
    }
    let _: () = conn.expire(format!("group:{}:count", group), 120).await?;
    println!("🔄 Processing batch item: {}", payload);
    // 模拟批处理
    tokio::time::sleep(Duration::from_secs(50)).await;
    println!("✅ Batch item processed: {}", payload);
    Ok(())
  }

  async fn handle_payment_process(&self, payload: serde_json::Value) -> Result<()> {
    println!("💰 Processing payment: {}", payload);

    // 模拟支付处理 - 可能需要重试
    let success_rate = 0.8; // 80% 成功率
    if rand::random::<f64>() < success_rate {
      tokio::time::sleep(Duration::from_secs(20)).await;
      println!("✅ Payment processed successfully: {}", payload);
      Ok(())
    } else {
      // 模拟支付失败，需要重试
      Err(Error::other("Payment gateway temporarily unavailable"))
    }
  }

  async fn handle_image_process(&self, payload: ImageResizePayload) -> Result<()> {
    println!(
      "🖼️  Processing image with advanced retry: {}",
      payload.src_url
    );
    println!("   Target size: {}x{}", payload.width, payload.height);

    // 模拟可能失败的图片处理
    let success_rate = 0.7; // 70% 成功率
    if rand::random::<f64>() < success_rate {
      tokio::time::sleep(Duration::from_secs(80)).await;
      println!("✅ Image processed successfully: {}", payload.src_url);
      Ok(())
    } else {
      // 模拟处理失败，将会使用指数退避重试
      Err(Error::other("Image processing service overloaded"))
    }
  }
}
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
  let mut combined_payload = Vec::new();
  for (idx, task) in tasks.iter().enumerate() {
    println!(
      "   Task {}: type='{}', payload size={} bytes",
      idx + 1,
      task.get_type(),
      task.get_payload().len()
    );

    combined_payload.push(String::from_utf8_lossy(task.get_payload()));
  }
  println!("   ✅ Created aggregated task with combined payload");

  // 创建聚合后的任务
  // Create the aggregated task
  Task::new(
    "batch:process",
    &serde_json::to_vec(&combined_payload).unwrap_or_default(),
  )
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  println!("🚀 Starting Asynq worker server...");

  // 创建 Redis 配置 - 优先从环境变量中读取，否则使用本地 Redis
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {}", redis_url);
  let redis_config = RedisConfig::from_url(&redis_url)?;

  // 配置队列优先级
  let mut queues = HashMap::new();
  queues.insert("critical".to_string(), 6); // 最高优先级
  queues.insert("default".to_string(), 3); // 默认优先级
  queues.insert("image_processing".to_string(), 2); // 图片处理队列
  queues.insert("low".to_string(), 1); // 低优先级
  let aggregator = GroupAggregatorFunc::new(aggregate_tasks);
  // 创建服务器配置
  let server_config = ServerConfig::new()
    .concurrency(4) // 4 个并发工作者
    .queues(queues)
    .strict_priority(false) // 不使用严格优先级
    .task_check_interval(Duration::from_secs(1))
    .shutdown_timeout(Duration::from_secs(10))
    .group_grace_period(Duration::from_secs(5))? // 组聚合宽限期
    .group_max_size(10) // 组最大大小
    .enable_group_aggregator(true); // 启用组聚合器 / Enable group aggregator

  // 创建服务器
  let mut server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;
  server.set_group_aggregator(aggregator);
  // 创建任务处理器
  let handler = TaskProcessor;

  // 运行服务器
  println!("🔄 Server is running and waiting for tasks...");
  println!("Press Ctrl+C to gracefully shutdown");

  server.run(handler).await?;

  println!("👋 Server shutdown complete");

  Ok(())
}
