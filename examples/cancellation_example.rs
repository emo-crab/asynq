//! 任务取消示例
//! Task Cancellation Example
//!
//! 这个示例展示如何使用 Subscriber 订阅任务取消事件
//! This example demonstrates how to use Subscriber to subscribe to task cancellation events

use asynq::base::Broker;
use asynq::components::subscriber::{Subscriber, SubscriberConfig, SubscriptionEvent};
use asynq::rdb::RedisBroker;
use asynq::redis::RedisConfig;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  println!("🚀 Task Cancellation Example");
  println!("=============================\n");

  // 1. 创建 Redis 配置
  // 1. Create Redis configuration
  let redis_config = RedisConfig::from_url("redis://localhost:6379")?;
  let broker: Arc<dyn Broker> = Arc::new(RedisBroker::new(redis_config)?);
  println!("✅ Connected to Redis\n");

  // 2. 创建并启动 Subscriber
  // 2. Create and start Subscriber
  let subscriber_config = SubscriberConfig { buffer_size: 100 };
  let mut subscriber = Subscriber::new(Arc::clone(&broker), subscriber_config);

  // 获取事件接收器
  // Get event receiver
  let mut event_rx = subscriber
    .take_receiver()
    .expect("Failed to get event receiver");

  let subscriber_arc = Arc::new(subscriber);
  let handle = subscriber_arc.clone().start();
  println!("📢 Subscriber started and listening for cancellation events\n");

  // 等待订阅者完全启动
  // Wait for subscriber to fully start
  tokio::time::sleep(std::time::Duration::from_millis(200)).await;

  // 3. 模拟发布取消事件
  // 3. Simulate publishing cancellation events
  println!("📤 Publishing cancellation events...\n");

  let task_ids = vec!["task_001", "task_002", "task_003"];

  for task_id in &task_ids {
    println!("  Cancelling task: {}", task_id);
    broker.publish_cancellation(task_id).await?;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
  }

  println!("\n📥 Receiving cancellation events:\n");

  // 4. 接收并处理取消事件
  // 4. Receive and handle cancellation events
  let mut received_count = 0;
  let timeout_duration = std::time::Duration::from_secs(3);

  while received_count < task_ids.len() {
    match tokio::time::timeout(timeout_duration, event_rx.recv()).await {
      Ok(Some(event)) => match event {
        SubscriptionEvent::TaskCancelled { task_id } => {
          received_count += 1;
          println!(
            "  ✓ Task cancelled: {} ({}/{})",
            task_id,
            received_count,
            task_ids.len()
          );
        }
        _ => {
          println!("  ℹ Other event received: {:?}", event);
        }
      },
      Ok(None) => {
        println!("  ⚠ Channel closed");
        break;
      }
      Err(_) => {
        println!("  ⏱ Timeout waiting for events");
        break;
      }
    }
  }

  println!("\n📊 Summary:");
  println!("  Total cancellation events published: {}", task_ids.len());
  println!("  Total cancellation events received: {}", received_count);

  // 5. 关闭 Subscriber
  // 5. Shutdown Subscriber
  println!("\n🛑 Shutting down subscriber...");
  subscriber_arc.shutdown();
  let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;

  println!("✅ Example completed successfully!");

  Ok(())
}
