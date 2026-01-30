//! 测试 Server 并发 worker 限制
use async_trait::async_trait;
use asynq::backend::RedisConnectionType;
use asynq::config::ServerConfig;
use asynq::error::Result;
use asynq::server::{Handler, ServerBuilder};
use asynq::task::Task;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Barrier;
#[derive(Clone)]
struct CountingHandler {
  active: Arc<Mutex<usize>>,
  max_active: Arc<Mutex<usize>>,
  barrier: Arc<Barrier>,
}

#[async_trait]
impl Handler for CountingHandler {
  async fn process_task(&self, _task: Task) -> Result<()> {
    {
      let mut active = self.active.lock().unwrap();
      *active += 1;
      let mut max_active = self.max_active.lock().unwrap();
      if *active > *max_active {
        *max_active = *active;
      }
    }
    // 所有 worker 同步到这里，确保并发
    self.barrier.wait().await;
    // 模拟任务处理
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
      let mut active = self.active.lock().unwrap();
      *active -= 1;
    }
    Ok(())
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_with_concurrency_limit() {
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
  let redis_config = RedisConnectionType::single(redis_url).unwrap();
  let concurrency = 2;
  let barrier = Arc::new(Barrier::new(concurrency));
  let handler = CountingHandler {
    active: Arc::new(Mutex::new(0)),
    max_active: Arc::new(Mutex::new(0)),
    barrier: barrier.clone(),
  };
  let server_config = ServerConfig::new().concurrency(concurrency);
  let mut server = ServerBuilder::new()
    .redis_config(redis_config.clone())
    .server_config(server_config)
    .build()
    .await
    .unwrap();
  // 投递多个任务
  let client = asynq::client::Client::new(redis_config).await.unwrap();
  for _ in 0..4 {
    let task = Task::new("test:concurrency", b"{}").unwrap();
    client.enqueue(task).await.unwrap();
  }
  // 只运行一小段时间
  // 直接传 handler，不要 Arc 包装
  let server_handle = tokio::spawn({
    let handler = handler.clone();
    async move {
      server.run(handler).await.unwrap();
    }
  });
  // 等待任务处理
  tokio::time::sleep(Duration::from_secs(1)).await;
  // 检查最大并发
  let max_active = *handler.max_active.lock().unwrap();
  assert!(
    max_active <= concurrency,
    "最大并发数超出限制: {} > {}",
    max_active,
    concurrency
  );
  // 关闭 server
  server_handle.abort();
}
