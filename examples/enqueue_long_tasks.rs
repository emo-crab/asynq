//! 入队长任务示例
//! Enqueue Long Tasks Example
//!
//! 这个示例创建一些长时间运行的任务用于测试取消功能
//! This example creates some long-running tasks for testing cancellation

use asynq::client::Client;
use asynq::redis::RedisConfig;
use asynq::task::Task;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  println!("📤 Enqueuing long-running tasks...\n");

  // 创建客户端
  // Create client
  let redis_config = RedisConfig::from_url("redis://localhost:6379")?;
  let client = Client::new(redis_config).await?;

  // 创建并入队多个长任务
  // Create and enqueue multiple long tasks
  let mut task_ids = Vec::new();

  for i in 1..=5 {
    let task = Task::new("long_task", format!("task_{}", i).as_bytes())?;
    let info = client.enqueue(task).await?;
    let task_id = info.id.clone();

    println!("✅ Enqueued task {}: {}", i, task_id);
    println!("   Queue: {}", info.queue);
    println!("   State: {:?}\n", info.state);

    task_ids.push(task_id);
  }

  println!("📊 Summary:");
  println!("   Total tasks enqueued: {}", task_ids.len());
  println!("\n💡 Task IDs for cancellation:");
  for (i, task_id) in task_ids.iter().enumerate() {
    println!("   Task {}: {}", i + 1, task_id);
  }

  println!("\n🎯 Next steps:");
  println!("   Run: cargo run --example cancel_tasks");
  println!("   Or use Inspector to cancel specific tasks");

  Ok(())
}
