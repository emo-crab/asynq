use asynq::{
  backend::RedisConnectionType,
  client::Client,
  scheduler::{PeriodicTask, Scheduler},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_scheduler_pushes_periodic_task() {
  // 使用本地 Redis
  let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379").unwrap();
  let client = Arc::new(Client::new(redis_config.clone()).await.unwrap());
  let scheduler = Scheduler::new(client.clone(), None).await.unwrap();

  // 注册周期性任务，每5秒触发一次
  let task = PeriodicTask::new(
    "test_periodic".to_string(),
    "*/5 * * * * * *".to_string(), // 每5秒
    b"test_payload".to_vec(),
    "default".to_string(),
  )
  .unwrap();
  let _ = scheduler.register(task, "test_scheduler").await;
  scheduler.start().await;

  // 等待10秒，期间应至少推送2次
  sleep(Duration::from_secs(10)).await;
  scheduler.stop().await;
  // TODO: 可用 client/inspector 查询队列，验证任务确实被推送
}

#[tokio::test]
async fn test_scheduler_in_arc() {
  // Test that Scheduler can be used from Arc without requiring mut
  let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379").unwrap();
  let client = Arc::new(Client::new(redis_config.clone()).await.unwrap());
  let scheduler = Arc::new(Scheduler::new(client.clone(), None).await.unwrap());

  // Register a periodic task
  let task = PeriodicTask::new(
    "test_arc_scheduler".to_string(),
    "*/5 * * * * * *".to_string(),
    b"test_payload".to_vec(),
    "default".to_string(),
  )
  .unwrap();
  let _ = scheduler.register(task, "test_scheduler").await;

  // Start scheduler from Arc - this should work now
  scheduler.start().await;

  // Wait a bit
  sleep(Duration::from_secs(2)).await;

  // Stop scheduler from Arc - this should work now
  scheduler.stop().await;
}
