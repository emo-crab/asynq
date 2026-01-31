//! PostgresSQL 生产者示例
//! PostgresSQL Producer example
//!
//! 演示如何使用 asynq 客户端将任务加入 PostgresSQL 队列
//! Demonstrates how to use asynq client to enqueue tasks to PostgresSQL

use asynq::client::Client;
use serde::{Deserialize, Serialize};
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
#[cfg(not(feature = "postgres"))]
fn main() {}
#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();

  // 创建 PostgresSQL 配置 - 优先从环境变量中读取，否则使用默认的测试 PostgresSQL 服务器
  // Create PostgresSQL config - first read from environment variable, otherwise use the default test PostgresSQL server
  let database_url = std::env::var("DATABASE_URL")
    .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/asynq".to_string());
  println!("🔗 Using PostgresSQL URL: {database_url}");

  // 创建 PostgresSQL 客户端
  // Create PostgresSQL client
  let client = Client::new_with_postgres(&database_url).await?;
  println!("✅ Connected to PostgresSQL");

  // 示例 1: 创建邮件发送任务
  // Example 1: Create email sending task
  let email_payload = EmailPayload {
    to: "user@example.com".to_string(),
    subject: "Welcome!".to_string(),
    body: "Welcome to our service!".to_string(),
  };

  let email_payload_bin = serde_json::to_vec(&email_payload)?;
  let email_task = asynq::task::Task::new("email:send", &email_payload_bin).unwrap();

  // 立即排队处理
  // Immediately enqueue for processing
  match client.enqueue(email_task).await {
    Ok(task_info) => {
      println!("📧 Email task enqueued: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("❌ Failed to enqueue email task: {e}");
    }
  }

  // 示例 2: 创建图片调整大小任务
  // Example 2: Create image resize task
  let image_payload = ImageResizePayload {
    src_url: "https://example.com/image.jpg".to_string(),
    width: 800,
    height: 600,
  };

  let image_payload_bin = serde_json::to_vec(&image_payload)?;
  let image_task = asynq::task::Task::new("image:resize", &image_payload_bin)
    .unwrap()
    .with_queue("image_processing")
    .with_max_retry(5)
    .with_timeout(std::time::Duration::from_secs(300)); // 5 分钟超时

  // 立即排队处理
  // Immediately enqueue for processing
  match client.enqueue(image_task).await {
    Ok(task_info) => {
      println!("🖼️  Image task enqueued: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("❌ Failed to enqueue image task: {e}");
    }
  }

  // 示例 3: 调度延迟任务
  // Example 3: Schedule delayed task
  let delayed_email_bin = serde_json::to_vec(&email_payload)?;
  let delayed_email = asynq::task::Task::new("email:reminder", &delayed_email_bin).unwrap();

  // 30 秒后执行
  // Execute after 30 seconds
  let process_at = std::time::SystemTime::now()
    .checked_add(std::time::Duration::from_secs(30))
    .unwrap();
  match client.schedule(delayed_email, process_at).await {
    Ok(task_info) => {
      println!("⏰ Delayed email task scheduled: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("❌ Failed to schedule delayed task: {e}");
    }
  }

  // 示例 4: 唯一任务（去重）
  // Example 4: Unique task (deduplication)
  let unique_payload_bin = serde_json::to_vec(&serde_json::json!({"date": "2023-01-01"}))?;
  let unique_task = asynq::task::Task::new("report:daily", &unique_payload_bin).unwrap();

  // 在 1 小时内保持唯一性
  // Maintain uniqueness within 1 hour
  match client
    .enqueue_unique(unique_task, std::time::Duration::from_secs(3600))
    .await
  {
    Ok(task_info) => {
      println!("🔒 Unique task enqueued: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("❌ Failed to enqueue unique task: {e}");
    }
  }

  // 示例 5: 组任务（用于聚合）
  // Example 5: Group task (for aggregation)
  for i in 1..=5 {
    let batch_payload_bin = serde_json::to_vec(&serde_json::json!({"item": i}))?;
    let batch_task = asynq::task::Task::new("batch:process", &batch_payload_bin).unwrap();

    match client.add_to_group(batch_task, "daily_batch").await {
      Ok(task_info) => {
        println!("📦 Batch task {} added to group: ID = {}", i, task_info.id);
      }
      Err(e) => {
        println!("❌ Failed to add batch task {i} to group: {e}");
      }
    }
  }

  // 示例 6: 使用唯一组任务
  // Example 6: Use unique group task
  let group_payload_bin = serde_json::to_vec(&serde_json::json!({"priority": "high"}))?;
  let group_task = asynq::task::Task::new("group:process", &group_payload_bin).unwrap();

  match client
    .add_to_group_unique(
      group_task,
      "priority_group",
      std::time::Duration::from_secs(3600),
    )
    .await
  {
    Ok(task_info) => {
      println!("🔒 Unique group task added: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("❌ Failed to add unique group task: {e}");
    }
  }

  // 关闭连接
  // Close connection
  client.close().await?;

  println!("\n✅ All tasks have been enqueued successfully!");

  Ok(())
}
