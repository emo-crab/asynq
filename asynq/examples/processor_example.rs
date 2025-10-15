//! Processor 使用示例
//! Processor usage example
//!
//! 演示如何使用 Processor 处理任务，兼容 Go asynq processor.go
//! Demonstrates how to use Processor to process tasks, compatible with Go asynq processor.go

use async_trait::async_trait;
use asynq::error::{Error, Result};
use asynq::redis::RedisConnectionConfig;
use asynq::{
  config::ServerConfig,
  server::{Handler, ServerBuilder},
  task::Task,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}

/// 邮件处理器 - 演示如何实现 Handler trait
/// Email processor - demonstrates how to implement Handler trait
pub struct EmailProcessor;

#[async_trait]
impl Handler for EmailProcessor {
  async fn process_task(&self, task: Task) -> Result<()> {
    println!("📨 Processing task: {}", task.get_type());

    match task.get_type() {
      "email:send" => {
        let payload: EmailPayload = serde_json::from_slice(task.get_payload()).unwrap();
        println!("✉️  Sending email to: {}", payload.to);
        println!("   Subject: {}", payload.subject);
        println!("   Body: {}", payload.body);

        // 模拟邮件发送处理
        // Simulate email sending
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("✅ Email sent successfully to {}", payload.to);
        Ok(())
      }
      "email:reminder" => {
        let payload: EmailPayload = serde_json::from_slice(task.get_payload()).unwrap();
        println!("⏰ Sending reminder email to: {}", payload.to);
        println!("   Subject: {}", payload.subject);

        // 模拟提醒邮件处理
        // Simulate reminder email processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("✅ Reminder email sent to {}", payload.to);
        Ok(())
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

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  println!("🚀 Starting Asynq worker server with Processor...");
  println!("   The Processor module is compatible with Go asynq processor.go");
  println!();

  // 创建 Redis 配置
  // Create Redis configuration
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");
  let redis_config = RedisConnectionConfig::single(redis_url)?;

  // 配置队列优先级
  // Configure queue priorities
  let mut queues = HashMap::new();
  queues.insert("critical".to_string(), 6); // 最高优先级 / Highest priority
  queues.insert("default".to_string(), 3); // 默认优先级 / Default priority
  queues.insert("low".to_string(), 1); // 低优先级 / Low priority

  // 创建服务器配置
  // Create server configuration
  let server_config = ServerConfig::new()
    .concurrency(2) // 2 个并发工作者 / 2 concurrent workers
    .queues(queues)
    .strict_priority(false) // 不使用严格优先级 / Don't use strict priority
    .task_check_interval(Duration::from_secs(1))
    .shutdown_timeout(Duration::from_secs(10));

  // 创建服务器
  // Create server
  let mut server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;

  println!();
  println!("✨ Processor features:");
  println!("   • Semaphore-based concurrency control");
  println!("   • Task timeout support");
  println!("   • Queue priority selection (strict and weighted)");
  println!("   • Graceful shutdown");
  println!("   • Automatic retry with exponential backoff");
  println!("   • Task archival after max retries");
  println!();

  // 创建任务处理器
  // Create task handler
  let handler = EmailProcessor;

  // 运行服务器
  // Run server
  println!("🔄 Server is running and waiting for tasks...");
  println!("   Press Ctrl+C to gracefully shutdown");
  println!();

  server.run(handler).await?;

  println!();
  println!("👋 Server shutdown complete");

  Ok(())
}
