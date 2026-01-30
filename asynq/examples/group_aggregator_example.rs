//! Group Aggregator 使用示例
//! Group Aggregator usage example
//!
//! 演示如何使用 GroupAggregator 聚合组中的任务
//! Demonstrates how to use GroupAggregator to aggregate tasks in a group
//!
//! 对应 Go asynq 的 GroupAggregator 接口
//! Corresponds to Go asynq's GroupAggregator interface

use asynq::error::{Error, Result};
use asynq::{server::Handler, task::Task};

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
  let mut combined_payload = String::new();
  for (idx, task) in tasks.iter().enumerate() {
    println!(
      "   Task {}: type='{}', payload size={} bytes",
      idx + 1,
      task.get_type(),
      task.get_payload().len()
    );

    if let Ok(payload_str) = std::str::from_utf8(task.get_payload()) {
      if !combined_payload.is_empty() {
        combined_payload.push('\n');
      }
      combined_payload.push_str(payload_str);
    }
  }

  println!("   ✅ Created aggregated task with combined payload");

  // 创建聚合后的任务
  // Create the aggregated task
  asynq::task::Task::new("batch:process", combined_payload.as_bytes())
}

/// 批处理任务处理器
/// Batch processing task handler
pub struct BatchProcessor;

#[async_trait::async_trait]
impl Handler for BatchProcessor {
  async fn process_task(&self, task: Task) -> Result<()> {
    match task.get_type() {
      "batch:process" => {
        println!("\n🔄 Processing aggregated batch task");
        if let Ok(payload) = std::str::from_utf8(task.get_payload()) {
          println!("   Combined payload:");
          for (idx, line) in payload.lines().enumerate() {
            println!("     {}. {}", idx + 1, line);
          }
        }
        println!("   ✅ Batch processing completed\n");
        Ok(())
      }
      "email:send" => {
        // 这些任务会被聚合到批处理任务中
        // These tasks will be aggregated into batch task
        println!("📧 Processing individual email:send task");
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
  use asynq::backend::RedisConnectionType;

  use asynq::config::ServerConfig;

  tracing_subscriber::fmt::init();

  println!("🚀 Starting Asynq server with Group Aggregator...");
  println!("   Group Aggregator aggregates multiple tasks into one before processing");
  println!();

  // 创建 Redis 配置
  // Create Redis configuration
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");
  let redis_config = RedisConnectionType::single(redis_url)?;

  // 配置队列
  // Configure queues
  let mut queues = std::collections::HashMap::new();
  queues.insert("default".to_string(), 1);

  // 创建服务器配置，启用组聚合器
  // Create server configuration with group aggregator enabled
  let server_config = ServerConfig::default()
    .concurrency(2)
    .queues(queues)
    .enable_group_aggregator(true) // 启用组聚合器 / Enable group aggregator
    .group_grace_period(std::time::Duration::from_secs(10))? // 聚合宽限期 / Aggregation grace period
    .group_max_size(5) // 最多聚合 5 个任务 / Aggregate up to 5 tasks
    .group_max_delay(std::time::Duration::from_secs(30)); // 最大延迟 30 秒 / Max delay 30 seconds

  println!("⚙️  Server configuration:");
  println!("   • Group aggregator: enabled");
  println!("   • Grace period: 10 seconds");
  println!("   • Max group size: 5 tasks");
  println!("   • Max delay: 30 seconds");
  println!();

  // 构建服务器
  // Build server
  let mut server = asynq::server::ServerBuilder::new()
    .redis_config(redis_config.clone())
    .server_config(server_config)
    .build()
    .await?;

  // 设置组聚合器
  // Set group aggregator
  println!("📦 Setting up group aggregator function...");
  let aggregator = asynq::components::aggregator::GroupAggregatorFunc::new(aggregate_tasks);
  server.set_group_aggregator(aggregator);
  println!("   ✅ Group aggregator configured");
  println!();

  println!("💡 To use GroupAggregator in your application:");
  println!("   1. Create tasks with .with_group(\"your-group-name\")");
  println!("   2. Call server.set_group_aggregator(your_aggregator)");
  println!("   3. Tasks in the same group will be aggregated");
  println!();

  // 创建任务处理器
  // Create task handler
  let handler = BatchProcessor;

  println!("🔄 Server is running...");
  println!("   Press Ctrl+C to gracefully shutdown");
  println!();

  // 运行服务器
  // Run server
  server.run(handler).await?;

  println!("👋 Server shutdown complete");

  Ok(())
}
