//! Scheduler 示例：演示如何注册和查询定时任务
//! Scheduler example: demonstrates how to register and query periodic tasks

use asynq::client::Client;
use asynq::scheduler::{PeriodicTask, Scheduler};
use std::sync::Arc;

#[tokio::main]
async fn main() {
  // 初始化 RedisConfig
  let redis_url = "redis://127.0.0.1:6379";
  let redis_config = asynq::redis::RedisConfig::from_url(redis_url).unwrap();
  // 创建 Client 和 RedisBroker
  let client = Arc::new(Client::new(redis_config.clone()).await.unwrap());
  // 创建 Scheduler
  let scheduler = Scheduler::new(client.clone(), None).await.unwrap();

  // 注册一个每分钟执行的任务
  let task = PeriodicTask::new(
    "demo:periodic_task".to_string(),
    "0/30 * * * * *".to_string(), // 每半分钟
    b"hello scheduler".to_vec(),
    "default".to_string(),
  )
  .unwrap();
  let _ = scheduler.register(task, "default").await;

  // 启动调度器
  let mut scheduler = scheduler;
  scheduler.start();

  // 等待 Ctrl+C 信号退出（类似 Go 的 waitForSignals）
  println!("Scheduler running. Press Ctrl+C to exit...");
  tokio::signal::ctrl_c().await.unwrap();

  // 查询所有调度条目
  let entries = scheduler.list_entries("demo_scheduler").await;
  println!("Scheduler Entries:");
  for entry in entries {
    println!(
      "  id: {}, type: {}, next: {:?}",
      entry.id, entry.task_type, entry.next_enqueue_time
    );
  }

  // 查询调度事件
  let events = scheduler.list_events(10).await;
  println!("Scheduler Events:");
  for event in events {
    println!(
      "  task_id: {}, enqueue_time: {:?}",
      event.task_id, event.enqueue_time
    );
  }

  // 停止调度器
  scheduler.stop().await;
}
