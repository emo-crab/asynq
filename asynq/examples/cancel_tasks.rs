//! 取消任务示例
//! Cancel Tasks Example
//!
//! 这个示例展示如何使用 Inspector 取消正在运行的任务
//! This example demonstrates how to use Inspector to cancel running tasks

use asynq::inspector::Inspector;
use asynq::redis::RedisConnectionType;
use std::sync::Arc;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  println!("🔍 Task Cancellation Example\n");

  // 创建 Inspector
  // Create Inspector
  let redis_config = RedisConnectionType::single("redis://localhost:6379")?;
  let inspector = Arc::new(Inspector::new(redis_config).await?);

  // 1. 列出当前活跃的任务
  // 1. List currently active tasks
  println!("📋 Listing active tasks...\n");
  let active_tasks = inspector.list_active_tasks("default").await?;

  if active_tasks.is_empty() {
    println!("⚠️  No active tasks found. Please run:");
    println!("   cargo run --example enqueue_long_tasks");
    println!("   cargo run --example server_with_cancellation");
    return Ok(());
  }

  println!("Found {} active task(s):\n", active_tasks.len());
  for (i, task) in active_tasks.iter().enumerate() {
    println!("  {}. Task ID: {}", i + 1, task.id);
    println!("     Type: {}", task.task_type);
    println!("     Queue: {}", task.queue);
    println!("     State: {:?}", task.state);
    println!("     Next: {:?}", task.next_process_at);
    println!();
  }

  // 2. 选择要取消的任务（这里取消第一个任务）
  // 2. Select task to cancel (cancel the first task here)
  if let Some(task) = active_tasks.first() {
    println!("🎯 Cancelling task: {}\n", task.id);

    // 使用 Inspector 取消任务
    // Use Inspector to cancel the task
    match inspector.cancel_processing(&task.id).await {
      Ok(_) => {
        println!("✅ Cancellation request sent successfully!");
        println!("   The task will be cancelled shortly.");
        println!("   Check the server logs to see the cancellation.");
      }
      Err(e) => {
        println!("❌ Failed to cancel task: {e}");
      }
    }
  }

  // 3. 等待一会儿，再次检查任务状态
  // 3. Wait a moment and check task status again
  println!("\n⏳ Waiting 2 seconds...\n");
  tokio::time::sleep(std::time::Duration::from_secs(2)).await;

  println!("📋 Listing active tasks again...\n");
  let active_tasks_after = inspector.list_active_tasks("default").await?;
  println!("Active tasks now: {}\n", active_tasks_after.len());

  println!("💡 Tips:");
  println!("   - Cancelled tasks are gracefully stopped");
  println!("   - The cancellation event is sent via Redis pub/sub");
  println!("   - The server receives the event and cancels the running task");

  Ok(())
}
