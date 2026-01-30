//! 生产者示例
//! Producer example
//!
//! 演示如何使用 asynq 客户端将任务加入队列
//! Demonstrates how to use asynq client to enqueue tasks

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  use asynq::backend::RedisConnectionType;
  use asynq::client::Client;
  tracing_subscriber::fmt::init();

  // 创建 Redis 配置 - 优先从环境变量中读取，否则使用默认的测试 Redis 服务器
  // Create Redis config - first read from environment variable, otherwise use the default test Redis server
  let redis_url = std::env::var("REDIS_URL")
    .unwrap_or_else(|_| "redis://tenant1:secure_pass123@localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");
  let redis_config = RedisConnectionType::single(redis_url.clone())?;

  // 创建客户端配置
  // Create client config
  let mut client_config = asynq::config::ClientConfig::new();

  // 如果启用了 acl 特性，配置 ACL
  // If acl feature is enabled, configure ACL
  #[cfg(feature = "acl")]
  {
    // 从 Redis URL 中提取用户名作为租户
    // Extract username from Redis URL as tenant
    // 格式: redis://username:password@host:port
    if let Some(username) = extract_username_from_redis_url(&redis_url) {
      println!("🔐 ACL enabled with tenant: {username}");
      client_config = client_config.acl_tenant(username);
    }
  }

  // 创建客户端
  // Create client
  let client = Client::with_config(redis_config, client_config).await?;

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
      println!("Email task enqueued: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("Failed to enqueue email task: {e}");
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
      println!("Image task enqueued: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("Failed to enqueue image task: {e}");
    }
  }

  // 示例 3: 调度延迟任务
  // Example 3: Schedule delayed task
  let delayed_email_bin = serde_json::to_vec(&email_payload)?;
  let delayed_email = asynq::task::Task::new("email:reminder", &delayed_email_bin).unwrap();

  // 5 分钟后执行
  // Execute after 5 minutes
  match client
    .enqueue_in(delayed_email, std::time::Duration::from_secs(30))
    .await
  {
    Ok(task_info) => {
      println!("Delayed email task scheduled: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("Failed to schedule delayed task: {e}");
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
      println!("Unique task enqueued: ID = {}", task_info.id);
    }
    Err(e) => {
      println!("Failed to enqueue unique task: {e}");
    }
  }

  // 示例 5: 组任务（用于聚合）
  // Example 5: Group task (for aggregation)
  for i in 1..=5 {
    let batch_payload_bin = serde_json::to_vec(&serde_json::json!({"item": i}))?;
    let batch_task = asynq::task::Task::new("batch:process", &batch_payload_bin).unwrap();

    match client.add_to_group(batch_task, "daily_batch").await {
      Ok(task_info) => {
        println!("Batch task {} added to group: ID = {}", i, task_info.id);
      }
      Err(e) => {
        println!("Failed to add batch task {i} to group: {e}");
      }
    }
  }

  // 示例 6: 使用高级重试策略
  // Example 6: Use advanced retry policy
  let advanced_payload_bin = serde_json::to_vec(&image_payload)?;
  let advanced_task = asynq::task::Task::new("image:process", &advanced_payload_bin)
    .unwrap()
    .with_queue("image_processing")
    .with_retry_policy(asynq::backend::option::RetryPolicy::Exponential {
      base_delay: std::time::Duration::from_secs(2),
      max_delay: std::time::Duration::from_secs(600), // 最大10分钟
      multiplier: 2.0,
      jitter: true, // 添加随机抖动避免惊群效应
    })
    .with_rate_limit(asynq::backend::option::RateLimit::per_task_type(
      std::time::Duration::from_secs(60),
      10,
    )); // 每分钟最多10个

  match client.enqueue(advanced_task).await {
    Ok(task_info) => {
      println!(
        "Advanced task with retry policy enqueued: ID = {}",
        task_info.id
      );
    }
    Err(e) => {
      println!("Failed to enqueue advanced task: {e}");
    }
  }

  // 示例 7: 使用线性重试策略的关键任务
  // Example 7: Critical task with linear retry policy
  let critical_payload_bin = serde_json::to_vec(&serde_json::json!({
    "amount": 100.00,
    "currency": "USD",
    "user_id": "12345"
  }))?;
  let critical_task = asynq::task::Task::new("payment:process", &critical_payload_bin)
    .unwrap()
    .with_queue("critical")
    .with_max_retry(10)
    .with_retry_policy(asynq::backend::option::RetryPolicy::Linear {
      base_delay: std::time::Duration::from_secs(30),
      max_delay: std::time::Duration::from_secs(300), // 最大5分钟
      step: std::time::Duration::from_secs(30),       // 每次增加30秒
    })
    .with_rate_limit(asynq::backend::option::RateLimit::per_queue(
      std::time::Duration::from_secs(60),
      5,
    )); // 队列级限流

  match client.enqueue(critical_task).await {
    Ok(task_info) => {
      println!(
        "Critical task with linear retry enqueued: ID = {}",
        task_info.id
      );
    }
    Err(e) => {
      println!("Failed to enqueue critical task: {e}");
    }
  }

  // 关闭客户端
  // Close client
  client.close().await?;

  println!("All tasks have been enqueued successfully!");

  Ok(())
}

/// 从 Redis URL 中提取用户名
/// Extract username from Redis URL
/// 格式: redis://username:password@host:port
#[cfg(feature = "acl")]
fn extract_username_from_redis_url(url: &str) -> Option<String> {
  // 查找 "://" 之后和 "@" 之前的部分
  if let Some(start_idx) = url.find("://") {
    let after_scheme = &url[start_idx + 3..];
    if let Some(at_idx) = after_scheme.find('@') {
      let credentials = &after_scheme[..at_idx];
      // 分离用户名和密码
      if let Some(colon_idx) = credentials.find(':') {
        let username = &credentials[..colon_idx];
        if !username.is_empty() {
          return Some(username.to_string());
        }
      }
    }
  }
  None
}
