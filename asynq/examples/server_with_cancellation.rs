//! 任务取消服务器示例
//! Task Cancellation Server Example
//!
//! 这个示例展示如何在服务器中集成任务取消功能
//! This example demonstrates how to integrate task cancellation in a server
//!
//! 服务器会自动处理通过 Redis pub/sub 发布的任务取消事件
//! The server automatically handles task cancellation events published through Redis pub/sub

use asynq::config::ServerConfig;
use asynq::error::Result;
use asynq::redis::RedisConnectionType;
use asynq::server::{AsyncHandlerFunc, Server};
use asynq::task::Task;
use std::time::Duration;

/// 长时间运行的任务处理器
/// Long-running task handler
async fn handle_long_task(task: Task) -> Result<()> {
  println!("🚀 Starting long task: {:?}", task.get_type());

  // 模拟长时间运行的任务（例如大文件处理、视频转码等）
  // Simulate a long-running task (e.g., large file processing, video transcoding, etc.)
  for i in 1..=20 {
    // 在实际应用中，这里应该检查任务是否被取消
    // In a real application, you should check if the task was cancelled here
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("  Task {:?} progress: {}%", task.get_type(), i * 5);

    // 如果任务在执行过程中被取消，tokio::select! 会中断执行
    // If the task is cancelled during execution, tokio::select! will interrupt execution
  }

  println!("✅ Task {:?} completed successfully", task.get_type());
  Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  println!("🚀 Task Cancellation Server Example");
  println!("====================================\n");

  // 1. 创建 Redis 配置
  // 1. Create Redis configuration
  let redis_config = RedisConnectionType::single("redis://localhost:6379")?;
  println!("✅ Connected to Redis\n");

  // 2. 创建服务器配置
  // 2. Create server configuration
  let mut server_config = ServerConfig::default();
  server_config = server_config.add_queue("default", 1)?;
  server_config = server_config.concurrency(5);

  // 3. 创建并启动服务器
  // 3. Create and start server
  println!("📢 Starting server with task cancellation support...");
  println!("   The server will automatically handle cancellation events\n");

  let mut server = Server::new(redis_config.clone(), server_config).await?;

  // 创建任务处理器
  // Create task handler
  let handler = AsyncHandlerFunc::new(handle_long_task);

  println!("🎯 Server is ready to process tasks");
  println!("   To test cancellation:");
  println!("   1. In another terminal, run: cargo run --example enqueue_long_tasks");
  println!("   2. Then run: cargo run --example cancel_tasks\n");
  println!("📝 Server is now running. Press Ctrl+C to shutdown...\n");

  // 启动服务器
  // Start server
  server.run(handler).await?;

  Ok(())
}
