//! ResultWriter 示例
//! ResultWriter example
//!
//! 演示如何在任务处理完成后写入结果
//! Demonstrates how to write results after task processing is complete

use async_trait::async_trait;
use asynq::error::Result;
use asynq::redis::RedisConnectionType;
use asynq::task::Task;
use asynq::{
  config::ServerConfig,
  server::{Handler, ServerBuilder},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct ComputePayload {
  operation: String,
  values: Vec<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ComputeResult {
  operation: String,
  input: Vec<i32>,
  result: i32,
  timestamp: String,
}

/// 任务处理器，演示 ResultWriter 的使用
/// Task processor demonstrating ResultWriter usage
pub struct ResultWriterHandler;

#[async_trait]
impl Handler for ResultWriterHandler {
  async fn process_task(&self, task: Task) -> Result<()> {
    match task.get_type() {
      "default:sum" | "default:multiply" => {
        let payload: ComputePayload = serde_json::from_slice(task.get_payload())?;
        self.handle_compute(task, payload).await
      }
      _ => {
        println!("Unknown task type: {}", task.get_type());
        Ok(())
      }
    }
  }
}

impl ResultWriterHandler {
  async fn handle_compute(&self, task: Task, payload: ComputePayload) -> Result<()> {
    println!("📊 Processing compute task: {}", payload.operation);
    println!("   Input values: {:?}", payload.values);

    // 执行计算
    // Perform computation
    let result = match payload.operation.as_str() {
      "sum" => payload.values.iter().sum(),
      "multiply" => payload.values.iter().product(),
      _ => 0,
    };

    // 模拟一些处理时间
    // Simulate some processing time
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("   Result: {}", result);

    // 使用 ResultWriter 写入结果
    // Write result using ResultWriter
    if let Some(writer) = task.result_writer() {
      let compute_result = ComputeResult {
        operation: payload.operation.clone(),
        input: payload.values.clone(),
        result,
        timestamp: chrono::Utc::now().to_rfc3339(),
      };

      let result_json = serde_json::to_vec(&compute_result)?;

      match writer.write(&result_json).await {
        Ok(bytes_written) => {
          println!("✅ Result written successfully: {} bytes", bytes_written);
          println!("   Task ID: {}", writer.task_id());
        }
        Err(e) => {
          println!("❌ Failed to write result: {}", e);
        }
      }
    } else {
      println!("⚠️  No ResultWriter available (task not from processor)");
    }

    Ok(())
  }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  println!("🚀 Starting ResultWriter Example Server...");

  // 创建 Redis 配置
  // Create Redis configuration
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");
  let redis_config = RedisConnectionType::single(redis_url)?;

  // 配置队列
  // Configure queues
  let mut queues = HashMap::new();
  queues.insert("default".to_string(), 3);

  // 创建服务器配置
  // Create server configuration
  let server_config = ServerConfig::new()
    .concurrency(2)
    .queues(queues)
    .strict_priority(false)
    .task_check_interval(Duration::from_secs(1))
    .shutdown_timeout(Duration::from_secs(10));

  // 创建服务器
  // Create server
  let mut server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;

  // 创建任务处理器
  // Create task handler
  let handler = ResultWriterHandler;

  println!("🔄 Server is running and waiting for tasks...");
  println!("💡 Run the producer to enqueue tasks with:");
  println!("   cargo run --example result_writer_producer");
  println!("Press Ctrl+C to gracefully shutdown");

  // 运行服务器
  // Run server
  server.run(handler).await?;

  println!("👋 Server shutdown complete");

  Ok(())
}
