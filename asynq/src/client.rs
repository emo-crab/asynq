//! 客户端模块
//! Client module
//!
//! 提供任务排队功能
//! Provides task queuing functionality

use crate::base::Broker;
use crate::config::ClientConfig;
use crate::error::Result;
use crate::memdb::MemoryBroker;
use crate::rdb::RedisBroker;
use crate::redis::RedisConnectionType;
use crate::task::{Task, TaskInfo};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[cfg(feature = "postgresql")]
use crate::pgdb::PostgresBroker;

#[cfg(feature = "websocket")]
use crate::wsdb::WebSocketBroker;

/// 客户端后端类型
/// Client backend type
enum ClientBroker {
  /// Redis 后端
  /// Redis backend
  Redis(Arc<RedisBroker>),
  #[cfg(feature = "postgresql")]
  /// PostgresSQL 后端
  /// PostgresSQL backend
  Postgres(Arc<PostgresBroker>),
  /// 内存后端（本地运行，不依赖外部服务）
  /// Memory backend (runs locally, no external service dependencies)
  Memory(Arc<MemoryBroker>),
  #[cfg(feature = "websocket")]
  /// WebSocket 后端（连接到 asynq-server）
  /// WebSocket backend (connects to asynq-server)
  WebSocket(Arc<WebSocketBroker>),
}

impl ClientBroker {
  /// 获取 Broker trait 对象
  /// Get the Broker trait object
  fn as_broker(&self) -> Arc<dyn Broker> {
    match self {
      ClientBroker::Redis(broker) => broker.clone(),
      #[cfg(feature = "postgresql")]
      ClientBroker::Postgres(broker) => broker.clone(),
      ClientBroker::Memory(broker) => broker.clone(),
      #[cfg(feature = "websocket")]
      ClientBroker::WebSocket(broker) => broker.clone(),
    }
  }

  /// 尝试获取 RedisBroker 实例
  /// Try to get the RedisBroker instance
  fn as_redis(&self) -> Option<Arc<RedisBroker>> {
    match self {
      ClientBroker::Redis(broker) => Some(broker.clone()),
      #[cfg(feature = "postgresql")]
      ClientBroker::Postgres(_) => None,
      ClientBroker::Memory(_) => None,
      #[cfg(feature = "websocket")]
      ClientBroker::WebSocket(_) => None,
    }
  }

  /// 尝试获取 MemoryBroker 实例
  /// Try to get the MemoryBroker instance
  fn as_memory(&self) -> Option<Arc<MemoryBroker>> {
    match self {
      ClientBroker::Memory(broker) => Some(broker.clone()),
      ClientBroker::Redis(_) => None,
      #[cfg(feature = "postgresql")]
      ClientBroker::Postgres(_) => None,
      #[cfg(feature = "websocket")]
      ClientBroker::WebSocket(_) => None,
    }
  }

  #[cfg(feature = "websocket")]
  /// 尝试获取 WebSocketBroker 实例
  /// Try to get the WebSocketBroker instance
  fn as_websocket(&self) -> Option<Arc<WebSocketBroker>> {
    match self {
      ClientBroker::WebSocket(broker) => Some(broker.clone()),
      ClientBroker::Redis(_) => None,
      #[cfg(feature = "postgresql")]
      ClientBroker::Postgres(_) => None,
      ClientBroker::Memory(_) => None,
    }
  }
}

/// Asynq 客户端，负责将任务排队
/// Asynq client, responsible for enqueuing tasks
pub struct Client {
  broker: ClientBroker,
  #[allow(dead_code)]
  config: ClientConfig,
}

impl Client {
  /// 创建新的客户端实例（使用 Redis 后端）
  /// Create a new client instance (with Redis backend)
  pub async fn new(redis_connection: RedisConnectionType) -> Result<Self> {
    Self::with_config(redis_connection, ClientConfig::default()).await
  }

  /// 使用指定配置创建客户端实例（使用 Redis 后端）
  /// Create a client instance with the specified configuration (with Redis backend)
  pub async fn with_config(
    redis_connection: RedisConnectionType,
    config: ClientConfig,
  ) -> Result<Self> {
    // 创建RedisBroker实例
    // Create RedisBroker instance
    let broker = Arc::new(RedisBroker::new(redis_connection).await?);
    Ok(Self {
      broker: ClientBroker::Redis(broker),
      config,
    })
  }

  /// 从 PostgresSQL 数据库 URL 创建新的客户端实例
  /// Create a new client instance from a PostgresSQL database URL
  #[cfg(feature = "postgresql")]
  pub async fn new_with_postgres(database_url: &str) -> Result<Self> {
    Self::new_with_postgres_config(database_url, ClientConfig::default()).await
  }

  /// 从 PostgresSQL 数据库 URL 和指定配置创建客户端实例
  /// Create a client instance from a PostgresSQL database URL with the specified configuration
  #[cfg(feature = "postgresql")]
  pub async fn new_with_postgres_config(database_url: &str, config: ClientConfig) -> Result<Self> {
    // 创建PostgresBroker实例
    // Create PostgresBroker instance
    let broker = Arc::new(PostgresBroker::new(database_url).await?);
    Ok(Self {
      broker: ClientBroker::Postgres(broker),
      config,
    })
  }

  /// 创建使用内存后端的新客户端实例（本地运行，不依赖外部服务）
  /// Create a new client instance using memory backend (runs locally, no external service dependencies)
  pub fn new_with_memory() -> Self {
    Self::new_with_memory_config(ClientConfig::default())
  }

  /// 使用指定配置创建内存后端客户端实例
  /// Create a memory backend client instance with the specified configuration
  pub fn new_with_memory_config(config: ClientConfig) -> Self {
    // 创建 MemoryBroker 实例
    // Create MemoryBroker instance
    let broker = Arc::new(MemoryBroker::new());
    Self {
      broker: ClientBroker::Memory(broker),
      config,
    }
  }

  /// 创建使用 WebSocket 后端的新客户端实例（连接到 asynq-server）
  /// Create a new client instance using WebSocket backend (connects to asynq-server)
  #[cfg(feature = "websocket")]
  pub async fn new_with_websocket(url: &str) -> Result<Self> {
    Self::new_with_websocket_config(url, ClientConfig::default()).await
  }

  /// 使用指定配置创建 WebSocket 后端客户端实例
  /// Create a WebSocket backend client instance with the specified configuration
  #[cfg(feature = "websocket")]
  pub async fn new_with_websocket_config(url: &str, config: ClientConfig) -> Result<Self> {
    // 创建 WebSocketBroker 实例
    // Create WebSocketBroker instance
    let broker = Arc::new(WebSocketBroker::new(url).await?);
    Ok(Self {
      broker: ClientBroker::WebSocket(broker),
      config,
    })
  }

  /// 使用 HTTP Basic 认证创建 WebSocket 后端客户端实例
  /// Create a WebSocket backend client instance with HTTP Basic authentication
  #[cfg(feature = "websocket")]
  pub async fn new_with_websocket_basic_auth(
    url: &str,
    username: String,
    password: String,
  ) -> Result<Self> {
    Self::new_with_websocket_basic_auth_config(url, username, password, ClientConfig::default())
      .await
  }

  /// 使用 HTTP Basic 认证和指定配置创建 WebSocket 后端客户端实例
  /// Create a WebSocket backend client instance with HTTP Basic authentication and specified configuration
  #[cfg(feature = "websocket")]
  pub async fn new_with_websocket_basic_auth_config(
    url: &str,
    username: String,
    password: String,
    config: ClientConfig,
  ) -> Result<Self> {
    // 创建带 Basic 认证的 WebSocketBroker 实例
    // Create WebSocketBroker instance with Basic authentication
    let broker =
      Arc::new(WebSocketBroker::with_basic_auth(url, Some(username), Some(password)).await?);
    Ok(Self {
      broker: ClientBroker::WebSocket(broker),
      config,
    })
  }

  /// 获取 Broker 实例
  /// Get the Broker instance
  pub fn get_broker(&self) -> Arc<dyn Broker> {
    self.broker.as_broker()
  }

  /// 尝试获取 RedisBroker 实例（如果底层 broker 是 RedisBroker）
  /// Try to get the RedisBroker instance (if the underlying broker is RedisBroker)
  ///
  /// 某些组件（如 Scheduler）需要 Redis 特定的功能
  /// Some components (like Scheduler) require Redis-specific features
  pub fn get_redis_broker(&self) -> Option<Arc<RedisBroker>> {
    self.broker.as_redis()
  }

  /// 尝试获取 MemoryBroker 实例（如果底层 broker 是 MemoryBroker）
  /// Try to get the MemoryBroker instance (if the underlying broker is MemoryBroker)
  pub fn get_memory_broker(&self) -> Option<Arc<MemoryBroker>> {
    self.broker.as_memory()
  }

  /// 尝试获取 WebSocketBroker 实例（如果底层 broker 是 WebSocketBroker）
  /// Try to get the WebSocketBroker instance (if the underlying broker is WebSocketBroker)
  #[cfg(feature = "websocket")]
  pub fn get_websocket_broker(&self) -> Option<Arc<WebSocketBroker>> {
    self.broker.as_websocket()
  }

  /// 应用 ACL 前缀到任务的队列名称
  /// Apply ACL prefix to task's queue name
  fn apply_acl_prefix_to_task(&self, mut task: Task) -> Task {
    if self.config.acl_tenant.is_some() {
      let queue = task.get_queue();
      let prefixed_queue = self.config.get_queue_name_with_prefix(queue);
      task = task.with_queue(prefixed_queue);
    }
    task
  }

  /// 将任务加入队列立即处理
  /// Enqueue a task for immediate processing
  pub async fn enqueue(&self, task: Task) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    self.broker.as_broker().enqueue(&task).await
  }

  /// 将唯一任务加入队列
  /// Enqueue a unique task
  pub async fn enqueue_unique(&self, task: Task, ttl: Duration) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    self.broker.as_broker().enqueue_unique(&task, ttl).await
  }

  /// 调度任务在指定时间处理
  /// Schedule a task for processing at a specific time
  pub async fn schedule(&self, task: Task, process_at: SystemTime) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    let date_time = chrono::DateTime::<chrono::Utc>::from(process_at);
    self.broker.as_broker().schedule(&task, date_time).await
  }

  /// 调度唯一任务在指定时间处理
  /// Schedule a unique task for processing at a specific time
  pub async fn schedule_unique(
    &self,
    task: Task,
    process_at: SystemTime,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    let date_time = chrono::DateTime::<chrono::Utc>::from(process_at);
    self
      .broker
      .as_broker()
      .schedule_unique(&task, date_time, ttl)
      .await
  }

  /// 在指定延迟后处理任务
  /// Enqueue a task to be processed after a specific delay
  pub async fn enqueue_in(&self, task: Task, delay: Duration) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    let process_at = SystemTime::now()
      .checked_add(delay)
      .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Delay overflow"))?;
    self.schedule(task, process_at).await
  }

  /// 将任务添加到组中进行聚合
  /// Add a task to a group for aggregation
  pub async fn add_to_group(&self, task: Task, group: &str) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    self.broker.as_broker().add_to_group(&task, group).await
  }

  /// 将唯一任务添加到组中进行聚合
  /// Add a unique task to a group for aggregation
  pub async fn add_to_group_unique(
    &self,
    task: Task,
    group: &str,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let task = self.apply_acl_prefix_to_task(task);
    self
      .broker
      .as_broker()
      .add_to_group_unique(&task, group, ttl)
      .await
  }

  /// Ping the backend connection
  pub async fn ping(&self) -> Result<()> {
    self.broker.as_broker().ping().await
  }

  /// 关闭客户端
  /// Close the client
  pub async fn close(&self) -> Result<()> {
    self.broker.as_broker().close().await
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
    assert_eq!(redis_config.addr().to_string(), "127.0.0.1:6379");
    assert_eq!(config.max_retries, 3);
  }

  #[tokio::test]
  async fn test_memory_client_creation() {
    // 测试内存客户端创建（不需要任何外部服务）
    // Test memory client creation (no external service required)
    let client = Client::new_with_memory();

    // 验证 ping 成功
    // Verify ping succeeds
    assert!(client.ping().await.is_ok());

    // 验证可以获取 MemoryBroker
    // Verify we can get MemoryBroker
    assert!(client.get_memory_broker().is_some());
    assert!(client.get_redis_broker().is_none());
  }

  #[tokio::test]
  async fn test_memory_client_enqueue() {
    // 测试内存客户端入队功能
    // Test memory client enqueue functionality
    let client = Client::new_with_memory();
    let task = Task::new("test:memory", b"test payload").unwrap();

    let result = client.enqueue(task).await;
    assert!(result.is_ok());

    let task_info = result.unwrap();
    assert_eq!(task_info.task_type, "test:memory");
  }

  #[tokio::test]
  async fn test_memory_client_schedule() {
    // 测试内存客户端调度功能
    // Test memory client schedule functionality
    let client = Client::new_with_memory();
    let task = Task::new("test:schedule", b"payload").unwrap();
    let process_at = SystemTime::now() + Duration::from_secs(3600);

    let result = client.schedule(task, process_at).await;
    assert!(result.is_ok());
  }

  // 注意: 由于需要实际的 Redis 连接，完整的集成测试需要在 CI/CD 环境中运行
  // Note: Full integration tests require an actual Redis connection and should be run in a CI/CD environment
}
