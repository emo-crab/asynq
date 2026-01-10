//! ResultWriter Producer 示例
//! ResultWriter Producer example
//!
//! 演示如何创建任务并稍后检查结果
//! Demonstrates how to create tasks and check results later

use asynq::client::Client;
use asynq::redis::RedisConnectionType;
use asynq::task::Task;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct ComputePayload {
  operation: String,
  values: Vec<i32>,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  println!("🚀 ResultWriter Producer Example");

  // 创建 Redis 配置
  // Create Redis configuration
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");
  let redis_config = RedisConnectionType::single(redis_url)?;

  // 创建客户端
  // Create client
  let client = Client::new(redis_config).await?;

  println!("\n📤 Enqueuing compute tasks...\n");

  // 创建求和任务
  // Create sum task
  let sum_payload = ComputePayload {
    operation: "sum".to_string(),
    values: vec![1, 2, 3, 4, 5],
  };

  let sum_task = Task::new("default:sum", &serde_json::to_vec(&sum_payload)?)?
    .with_queue("default")
    .with_retention(Duration::from_secs(3600)); // 保留结果 1 小时 / Retain result for 1 hour

  let sum_info = client.enqueue(sum_task).await?;
  println!("✅ Enqueued sum task:");
  println!("   Task ID: {}", sum_info.id);
  println!("   Queue: {}", sum_info.queue);
  println!("   Type: {}", sum_info.task_type);
  println!("   Payload: {:?}", sum_payload);

  // 创建乘法任务
  // Create multiply task
  let multiply_payload = ComputePayload {
    operation: "multiply".to_string(),
    values: vec![2, 3, 4],
  };

  let multiply_task = Task::new("default:multiply", &serde_json::to_vec(&multiply_payload)?)?
    .with_queue("default")
    .with_retention(Duration::from_secs(3600)); // 保留结果 1 小时 / Retain result for 1 hour

  let multiply_info = client.enqueue(multiply_task).await?;
  println!("\n✅ Enqueued multiply task:");
  println!("   Task ID: {}", multiply_info.id);
  println!("   Queue: {}", multiply_info.queue);
  println!("   Type: {}", multiply_info.task_type);
  println!("   Payload: {:?}", multiply_payload);

  println!("\n💡 Tasks enqueued successfully!");
  println!("   The worker will process these tasks and write results.");
  println!("   Results can be retrieved later using the task IDs above.");
  println!("\n📝 Note: Make sure the result_writer_example consumer is running!");

  Ok(())
}
