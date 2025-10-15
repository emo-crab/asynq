//! 客户端模块
//! Client module
//!
//! 提供任务排队功能
//! Provides task queuing functionality

use crate::base::Broker;
use crate::config::ClientConfig;
use crate::error::Result;
use crate::rdb::RedisBroker;
use crate::redis::RedisConnectionConfig;
use crate::task::{Task, TaskInfo};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Asynq 客户端，负责将任务排队
/// Asynq client, responsible for enqueuing tasks
pub struct Client {
  broker: Arc<RedisBroker>,
  #[allow(dead_code)]
  config: ClientConfig,
}

impl Client {
  /// 创建新的客户端实例
  /// Create a new client instance
  pub async fn new(redis_connection: RedisConnectionConfig) -> Result<Self> {
    Self::with_config(redis_connection, ClientConfig::default()).await
  }

  /// 使用指定配置创建客户端实例
  /// Create a client instance with the specified configuration
  pub async fn with_config(
    redis_connection: RedisConnectionConfig,
    config: ClientConfig,
  ) -> Result<Self> {
    // 创建RedisBroker实例
    // Create RedisBroker instance
    let mut broker = RedisBroker::new(redis_connection)?;
    broker.init_scripts().await?;
    Ok(Self {
      broker: Arc::new(broker),
      config,
    })
  }

  /// 获取 RedisBroker 实例
  /// Get the RedisBroker instance
  pub fn get_broker(&self) -> Arc<RedisBroker> {
    self.broker.clone()
  }

  /// 将任务加入队列立即处理
  /// Enqueue a task for immediate processing
  pub async fn enqueue(&self, task: Task) -> Result<TaskInfo> {
    self.broker.enqueue(&task).await
  }

  /// 将唯一任务加入队列
  /// Enqueue a unique task
  pub async fn enqueue_unique(&self, task: Task, ttl: Duration) -> Result<TaskInfo> {
    self.broker.enqueue_unique(&task, ttl).await
  }

  /// 调度任务在指定时间处理
  /// Schedule a task for processing at a specific time
  pub async fn schedule(&self, task: Task, process_at: SystemTime) -> Result<TaskInfo> {
    let date_time = chrono::DateTime::<chrono::Utc>::from(process_at);
    self.broker.schedule(&task, date_time).await
  }

  /// 调度唯一任务在指定时间处理
  /// Schedule a unique task for processing at a specific time
  pub async fn schedule_unique(
    &self,
    task: Task,
    process_at: SystemTime,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let date_time = chrono::DateTime::<chrono::Utc>::from(process_at);
    self.broker.schedule_unique(&task, date_time, ttl).await
  }

  /// 在指定延迟后处理任务
  /// Enqueue a task to be processed after a specific delay
  pub async fn enqueue_in(&self, task: Task, delay: Duration) -> Result<TaskInfo> {
    let process_at = SystemTime::now()
      .checked_add(delay)
      .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Delay overflow"))?;
    self.schedule(task, process_at).await
  }

  /// 将任务添加到组中进行聚合
  /// Add a task to a group for aggregation
  pub async fn add_to_group(&self, task: Task, group: &str) -> Result<TaskInfo> {
    self.broker.add_to_group(&task, group).await
  }

  /// 将唯一任务添加到组中进行聚合
  /// Add a unique task to a group for aggregation
  pub async fn add_to_group_unique(
    &self,
    task: Task,
    group: &str,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    self.broker.add_to_group_unique(&task, group, ttl).await
  }

  /// Ping Redis 连接
  /// Ping the Redis connection
  pub async fn ping(&self) -> Result<()> {
    self.broker.ping().await
  }

  /// 关闭客户端
  /// Close the client
  pub async fn close(&self) -> Result<()> {
    self.broker.close().await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::task::Task;
  use redis::ConnectionInfo;
  use std::str::FromStr;

  #[tokio::test]
  async fn test_task_creation() {
    let task = Task::new("test_task", b"test payload").unwrap();
    assert_eq!(task.get_type(), "test_task");
    assert_eq!(task.get_payload(), b"test payload");
  }

  #[tokio::test]
  async fn test_client_creation() {
    // 测试客户端创建（不需要实际连接）
    // Test client creation (no actual connection needed)
    let redis_config = ConnectionInfo::from_str("redis://127.0.0.1:6379").unwrap();
    let config = ClientConfig::default();

    // 这里只测试配置解析，不测试实际连接
    // Here we only test configuration parsing, not the actual connection
    // 由于客户端创建需要连接Redis，我们只测试配置解析和基础结构
    // Since client creation requires a connection to Redis, we only test configuration parsing and basic structure
    assert_eq!(redis_config.addr.to_string(), "127.0.0.1:6379");
    assert_eq!(config.max_retries, 3);
  }

  // 注意: 由于需要实际的 Redis 连接，完整的集成测试需要在 CI/CD 环境中运行
  // Note: Full integration tests require an actual Redis connection and should be run in a CI/CD environment
}
