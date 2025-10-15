//! 服务器模块
//! Server module
//!
//! 提供任务处理服务器功能
//! Provides task processing server functionality

use crate::base::Broker;
use crate::components::heartbeat::{Heartbeat, HeartbeatMeta};
use crate::components::processor::{Processor, ProcessorParams};
use crate::components::subscriber::SubscriberConfig;
use crate::components::ComponentLifecycle;
pub use crate::config::ServerConfig;
use crate::error::{Error, Result};
use crate::rdb::RedisBroker;
use crate::redis::RedisConnectionConfig;
use crate::task::Task;
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// 任务处理器特性
/// Task handler trait
#[async_trait]
pub trait Handler: Send + Sync {
  /// 处理任务
  /// Process a task
  async fn process_task(&self, task: Task) -> Result<()>;
}

/// 函数式处理器适配器
/// Functional handler adapter
pub struct HandlerFunc<F> {
  func: F,
}

impl<F> HandlerFunc<F>
where
  F: Fn(Task) -> Result<()> + Send + Sync,
{
  /// 创建新的函数式处理器
  /// Create a new functional handler
  pub fn new(func: F) -> Self {
    Self { func }
  }
}

#[async_trait]
impl<F> Handler for HandlerFunc<F>
where
  F: Fn(Task) -> Result<()> + Send + Sync,
{
  async fn process_task(&self, task: Task) -> Result<()> {
    (self.func)(task)
  }
}

/// 异步函数式处理器适配器
/// Asynchronous functional handler adapter
pub struct AsyncHandlerFunc<F, Fut> {
  func: F,
  _phantom: std::marker::PhantomData<Fut>,
}

impl<F, Fut> AsyncHandlerFunc<F, Fut>
where
  F: Fn(Task) -> Fut + Send + Sync,
  Fut: std::future::Future<Output = Result<()>> + Send + Sync,
{
  /// 创建新的异步函数式处理器
  /// Create a new asynchronous functional handler
  pub fn new(func: F) -> Self {
    Self {
      func,
      _phantom: std::marker::PhantomData,
    }
  }
}

#[async_trait]
impl<F, Fut> Handler for AsyncHandlerFunc<F, Fut>
where
  F: Fn(Task) -> Fut + Send + Sync,
  Fut: std::future::Future<Output = Result<()>> + Send + Sync,
{
  async fn process_task(&self, task: Task) -> Result<()> {
    (self.func)(task).await
  }
}

/// 服务器状态
/// Server state
#[derive(Debug, Clone, Copy, PartialEq)]
enum ServerState {
  // 新建初始化
  New,
  // 正在运行可以接任务
  Running,
  // 停止
  Stopped,
  // 关闭
  Closed,
}

/// Asynq 服务器，负责处理任务
/// Asynq server, responsible for processing tasks
pub struct Server {
  broker: Arc<dyn Broker>,
  config: ServerConfig,
  state: ServerState,
  // 原先仅保存 uuid，现在按照 Go 版语义分别保存
  // Originally only saves uuid, now saves separately according to Go version semantics
  host: String,
  pid: i32,
  server_uuid: String,
  // 新增：活跃 worker 计数
  // New: Active worker count
  active_workers: Arc<AtomicUsize>,
  // 处理器
  // Processor
  processor: Option<Processor>,
  // 插件列表 - 统一管理实现了 ComponentLifecycle 的组件
  // Component list - unified management of components implementing ComponentLifecycle
  components: Vec<(Arc<dyn ComponentLifecycle + Send + Sync>, JoinHandle<()>)>,
  // 组聚合器 - 将一组任务聚合为一个任务
  // Group aggregator - aggregates a group of tasks into one task
  group_aggregator: Option<Arc<dyn crate::components::aggregator::GroupAggregator>>,
}

impl Server {
  /// 返回组合 server id (hostname:pid:uuid) —— 仅在需要列出或调试时使用
  /// Returns the combined server id (hostname:pid:uuid) - for listing or debugging purposes only
  pub fn full_server_id(&self) -> String {
    format!("{}:{}:{}", self.host, self.pid, self.server_uuid)
  }

  /// 创建新的服务器实例
  /// Create a new server instance
  pub async fn new(
    redis_connection_config: RedisConnectionConfig,
    config: ServerConfig,
  ) -> Result<Self> {
    // 验证配置
    // Validate configuration
    config.validate()?;
    // 创建 RedisBroker 实例
    // Create RedisBroker instance
    let mut redis_broker = RedisBroker::new(redis_connection_config)?;
    // 初始化脚本
    // Initialize scripts
    redis_broker.init_scripts().await?;
    let broker = Arc::new(redis_broker);

    // 与 Go heartbeater 初始化保持一致：获取 host、pid、生成 uuid
    // Consistent with Go heartbeater initialization: get host, pid, generate uuid
    let host = hostname::get()
      .unwrap_or_default()
      .to_string_lossy()
      .to_string();
    let pid = std::process::id() as i32;
    let server_uuid = Uuid::new_v4().to_string();

    Ok(Self {
      broker,
      config,
      state: ServerState::New,
      host,
      pid,
      server_uuid,
      active_workers: Arc::new(AtomicUsize::new(0)),
      processor: None,
      components: Vec::new(),
      group_aggregator: None,
    })
  }

  /// 设置组聚合器
  /// Set group aggregator
  ///
  /// 在启动服务器之前调用此方法以设置自定义的组聚合器
  /// Call this method before starting the server to set a custom group aggregator
  ///
  /// # Example
  /// ```no_run
  /// use asynq::components::aggregator::GroupAggregatorFunc;
  /// use asynq::task::Task;
  ///
  /// fn aggregate_tasks(group: &str, tasks: Vec<Task>) -> asynq::error::Result<Task> {
  ///     // Combine tasks logic
  ///     Task::new("batch:process", b"combined")
  /// }
  ///
  /// // server.set_group_aggregator(GroupAggregatorFunc::new(aggregate_tasks));
  /// ```
  pub fn set_group_aggregator<A>(&mut self, aggregator: A)
  where
    A: crate::components::aggregator::GroupAggregator + 'static,
  {
    self.group_aggregator = Some(Arc::new(aggregator));
  }

  /// 启动服务器
  /// Start the server
  pub async fn start<H>(&mut self, handler: H) -> Result<()>
  where
    H: Handler + 'static,
  {
    if self.state != ServerState::New {
      return Err(Error::ServerRunning);
    }

    self.state = ServerState::Running;

    // 注册服务器
    // Register the server
    self.register_server().await?;

    // 使用新的 HeartbeatMeta + Heartbeater
    // Use new HeartbeatMeta + Heartbeater
    let meta = HeartbeatMeta {
      host: self.host.clone(),
      pid: self.pid,
      server_uuid: self.server_uuid.clone(),
      concurrency: self.config.concurrency,
      queues: self.config.queues.clone(),
      strict_priority: self.config.strict_priority,
      started: std::time::SystemTime::now(),
    };
    let hb = Arc::new(Heartbeat::new(
      Arc::clone(&self.broker),
      self.config.heartbeat_interval,
      meta,
      Arc::clone(&self.active_workers),
    ));
    let hb_handle = hb.clone().start();
    self
      .components
      .push((hb as Arc<dyn ComponentLifecycle + Send + Sync>, hb_handle));

    // 启动新的组件（按 Go asynq 架构）
    // Start new components (following Go asynq architecture)

    // 启动 Janitor - 清理过期任务和死亡服务器
    // Start Janitor - cleanup expired tasks and dead servers
    let janitor_config = crate::components::janitor::JanitorConfig {
      interval: self.config.janitor_interval,
      batch_size: self.config.janitor_batch_size,
      queues: self.config.queues.keys().cloned().collect(),
    };
    let janitor = Arc::new(crate::components::janitor::Janitor::new(
      Arc::clone(&self.broker),
      janitor_config,
    ));
    let janitor_handle = janitor.clone().start();
    self.components.push((
      janitor as Arc<dyn ComponentLifecycle + Send + Sync>,
      janitor_handle,
    ));
    // 启动 Subscriber - 订阅事件
    // Start Subscriber - subscribe to events
    let mut subscriber = crate::components::subscriber::Subscriber::new(
      Arc::clone(&self.broker),
      SubscriberConfig::default(),
    );

    // 获取事件接收器用于处理取消事件
    // Get event receiver for handling cancellation events
    let event_rx = subscriber.take_receiver();

    let subscriber = Arc::new(subscriber);
    let subscriber_handle = subscriber.clone().start();
    self.components.push((
      subscriber as Arc<dyn ComponentLifecycle + Send + Sync>,
      subscriber_handle,
    ));

    // 启动 Recoverer - 恢复孤儿任务
    // Start Recoverer - recover orphaned tasks
    let recoverer_config = crate::components::recoverer::RecovererConfig {
      interval: self.config.janitor_interval, // 使用相同的间隔
      queues: self.config.queues.keys().cloned().collect(),
    };
    let recoverer = Arc::new(crate::components::recoverer::Recoverer::new(
      Arc::clone(&self.broker),
      recoverer_config,
    ));
    let recoverer_handle = recoverer.clone().start();
    self.components.push((
      recoverer as Arc<dyn ComponentLifecycle + Send + Sync>,
      recoverer_handle,
    ));

    // 启动 Forwarder - 转发调度任务
    // Start Forwarder - forward scheduled tasks
    let forwarder_config = crate::components::forwarder::ForwarderConfig {
      interval: self.config.delayed_task_check_interval,
      queues: self.config.queues.keys().cloned().collect(),
    };
    let forwarder = Arc::new(crate::components::forwarder::Forwarder::new(
      Arc::clone(&self.broker),
      forwarder_config,
    ));
    let forwarder_handle = forwarder.clone().start();
    self.components.push((
      forwarder as Arc<dyn ComponentLifecycle + Send + Sync>,
      forwarder_handle,
    ));

    // 启动 Healthcheck - 健康检查
    // Start Healthcheck - health check
    let healthcheck_config = crate::components::healthcheck::HealthcheckConfig {
      interval: self.config.health_check_interval,
    };
    let healthcheck = Arc::new(crate::components::healthcheck::Healthcheck::new(
      Arc::clone(&self.broker),
      healthcheck_config,
    ));
    let healthcheck_handle = healthcheck.clone().start();
    self.components.push((
      healthcheck as Arc<dyn ComponentLifecycle + Send + Sync>,
      healthcheck_handle,
    ));

    // 启动 Aggregator - 聚合任务到组中
    // Start Aggregator - aggregate tasks into groups
    if self.config.group_aggregator_enabled {
      let aggregator_config = crate::components::aggregator::AggregatorConfig {
        interval: Duration::from_secs(5),
        queues: self.config.queues.keys().cloned().collect(),
        grace_period: self.config.group_grace_period,
        max_delay: self.config.group_max_delay,
        max_size: self.config.group_max_size,
        group_aggregator: self.group_aggregator.clone(),
      };
      let aggregator = Arc::new(crate::components::aggregator::Aggregator::new(
        Arc::clone(&self.broker),
        aggregator_config,
      ));
      let aggregator_handle = aggregator.clone().start();
      self.components.push((
        aggregator as Arc<dyn ComponentLifecycle + Send + Sync>,
        aggregator_handle,
      ));
    }

    // 创建处理器并启动
    // Create and start processor
    let processor_params = ProcessorParams {
      broker: Arc::clone(&self.broker),
      queues: self.config.queues.clone(),
      concurrency: self.config.concurrency,
      strict_priority: self.config.strict_priority,
      task_check_interval: self.config.task_check_interval,
      shutdown_timeout: self.config.shutdown_timeout,
      active_workers: Arc::clone(&self.active_workers),
    };

    let mut processor = Processor::new(processor_params);
    let handler = Arc::new(handler);
    processor.start(handler);

    // 获取任务取消追踪器用于处理取消事件
    // Get cancellation tracker for handling cancellation events
    let cancellations = processor.cancellations();

    // 如果成功获取事件接收器，启动取消事件处理
    // If event receiver was successfully obtained, start cancellation event handling
    if let Some(mut rx) = event_rx {
      tokio::spawn(async move {
        use crate::components::subscriber::SubscriptionEvent;

        while let Some(event) = rx.recv().await {
          if let SubscriptionEvent::TaskCancelled { task_id } = event {
            tracing::info!("Received cancellation request for task: {}", task_id);
            if cancellations.cancel(&task_id) {
              tracing::info!("Successfully cancelled task: {}", task_id);
            } else {
              tracing::debug!("Task {} not found or already completed", task_id);
            }
          }
        }
      });
    }

    self.processor = Some(processor);

    // 等待服务器停止信号
    // Wait for server stop signal
    self.wait_for_signal().await;

    // 停止处理器
    // Stop processor
    if let Some(processor) = self.processor.as_mut() {
      processor.shutdown().await;
    }

    // 停止新组件
    // Stop new components
    for (component, handle) in self.components.drain(..) {
      component.shutdown();
      let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }

    Ok(())
  }

  /// 运行服务器直到收到停止信号
  /// Run the server until a stop signal is received
  pub async fn run<H>(&mut self, handler: H) -> Result<()>
  where
    H: Handler + 'static,
  {
    // 启动服务器
    // Start the server
    let result = self.start(handler).await;

    // 优雅停止
    // Graceful shutdown
    self.shutdown().await?;

    result
  }

  /// 停止服务器
  /// Stop the server
  pub async fn stop(&mut self) -> Result<()> {
    if self.state == ServerState::Running {
      self.state = ServerState::Stopped;
    }
    Ok(())
  }

  /// 关闭服务器
  /// Shutdown the server
  pub async fn shutdown(&mut self) -> Result<()> {
    if self.state == ServerState::Closed {
      return Ok(());
    }
    self.state = ServerState::Closed;

    // 停止处理器（如果还没停止）
    // Stop processor (if not already stopped)
    if let Some(processor) = self.processor.as_mut() {
      processor.shutdown().await;
    }

    // 停止新组件（如果还在运行）
    // Stop new components (if still running)
    for (component, handle) in self.components.drain(..) {
      component.shutdown();
      let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }

    // 主动清理服务器状态
    // Actively clean up server state
    if let Err(e) = self
      .broker
      .clear_server_state(&self.host, self.pid, &self.server_uuid)
      .await
    {
      tracing::warn!(
        "Failed to clear server state ({}:{}:{}): {}",
        self.host,
        self.pid,
        self.server_uuid,
        e
      );
    } else {
      tracing::debug!("Server state cleared: {}", self.full_server_id());
    }

    // 关闭连接（目前为幂等空操作）
    // Close connection (currently an idempotent no-op)
    self.broker.close().await?;

    Ok(())
  }

  /// Ping Redis 连接
  /// Ping Redis connection
  pub async fn ping(&self) -> Result<()> {
    self.broker.ping().await
  }

  /// 注册服务器
  /// Register the server
  async fn register_server(&self) -> Result<()> {
    // Go 版 ServerInfo.ServerID 仅保存 uuid，本实现保持一致；组合 id 只在 ZSET 成员里使用，由 rdb.write_server_state 负责
    // Go version ServerInfo.ServerID only saves uuid, this implementation is consistent; combined id is only used in ZSET members, managed by rdb.write_server_state
    let server_info = crate::proto::ServerInfo {
      host: self.host.clone(),
      pid: self.pid,
      server_id: self.server_uuid.clone(),
      concurrency: self.config.concurrency as i32,
      queues: self.config.queues.clone(),
      strict_priority: self.config.strict_priority,
      status: "active".to_string(),
      start_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
      active_worker_count: 0,
    };

    self
      .broker
      .write_server_state(&server_info, Duration::from_secs(3600))
      .await
  }

  /// 检查任务是否已过期（基于 deadline）
  /// Check if the task has expired (based on deadline)
  #[allow(dead_code)]
  fn is_task_expired(&self, task_msg: &crate::proto::TaskMessage) -> bool {
    if task_msg.deadline <= 0 {
      return false;
    }

    let now = chrono::Utc::now().timestamp();
    now > task_msg.deadline
  }

  /// 等待停止信号
  /// Wait for stop signal
  async fn wait_for_signal(&self) {
    let _ = signal::ctrl_c().await;
    tracing::info!("Received shutdown signal");
  }
}

impl Drop for Server {
  fn drop(&mut self) {
    // 如果已经关闭则无需再清理
    // No need to clean up if already closed
    if self.state == ServerState::Closed {
      return;
    }
    // 尽力而为：尝试在当前运行时 spawn 清理任务
    // Best effort: try to spawn cleanup task in the current runtime
    let host = self.host.clone();
    let pid = self.pid;
    let uuid = self.server_uuid.clone();
    let broker = Arc::clone(&self.broker);
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
      rt.spawn(async move {
        if let Err(e) = broker.clear_server_state(&host, pid, &uuid).await {
          tracing::warn!(
            "(Drop) Failed to clear server state {}:{}:{}: {}",
            host,
            pid,
            uuid,
            e
          );
        } else {
          tracing::debug!("(Drop) Server state cleared {}:{}:{}", host, pid, uuid);
        }
      });
    } else {
      // 无运行时，只能放弃（进程退出后 TTL 过期也会被清理）
      // No runtime available, give up (cleanup will be done when the process exits and TTL expires)
      tracing::error!(
        "[asynq] Drop without runtime; server keys will expire via TTL for {}:{}:{}",
        host,
        pid,
        uuid
      );
    }
  }
}

/// 服务器构建器
/// Server builder
pub struct ServerBuilder {
  redis_config: Option<RedisConnectionConfig>,
  config: ServerConfig,
}

impl ServerBuilder {
  /// 创建新的服务器构建器
  /// Create a new server builder
  pub fn new() -> Self {
    Self {
      redis_config: None,
      config: ServerConfig::default(),
    }
  }

  /// 设置 Redis 配置
  /// Set Redis configuration
  pub fn redis_config(mut self, config: RedisConnectionConfig) -> Self {
    self.redis_config = Some(config);
    self
  }

  /// 设置服务器配置
  /// Set server configuration
  pub fn server_config(mut self, config: ServerConfig) -> Self {
    self.config = config;
    self
  }

  /// 设置并发数
  /// Set concurrency
  pub fn concurrency(mut self, concurrency: usize) -> Self {
    self.config = self.config.concurrency(concurrency);
    self
  }

  /// 添加队列
  /// Add queue
  pub fn add_queue<S: AsRef<str>>(mut self, name: S, priority: i32) -> Result<Self> {
    self.config = self.config.add_queue(name, priority)?;
    Ok(self)
  }

  /// 构建服务器
  /// Build the server
  pub async fn build(self) -> Result<Server> {
    let redis_config = self
      .redis_config
      .ok_or_else(|| Error::config("Redis configuration is required"))?;

    Server::new(redis_config, self.config).await
  }
}

impl Default for ServerBuilder {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::base::constants::DEFAULT_TIMEOUT;

  #[tokio::test]
  async fn test_handler_func() {
    let handler = HandlerFunc::new(|task: Task| {
      println!("Processing task: {}", task.get_type());
      Ok(())
    });

    let task = Task::new("test", b"payload").unwrap();
    let result = handler.process_task(task).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_async_handler_func() {
    let handler = AsyncHandlerFunc::new(|task: Task| async move {
      println!("Processing async task: {}", task.get_type());
      Ok(())
    });

    let task = Task::new("test", b"payload").unwrap();
    let result = handler.process_task(task).await;
    assert!(result.is_ok());
  }

  #[test]
  fn test_server_builder() {
    let builder = ServerBuilder::new().concurrency(4);

    assert_eq!(builder.config.concurrency, 4);
  }

  #[test]
  fn test_timeout_calculation_logic() {
    use std::time::Duration;

    // Test timeout calculation logic directly
    let now = chrono::Utc::now().timestamp();

    // Test with task timeout
    let mut task_msg = crate::proto::TaskMessage {
      deadline: now + 300,
      ..Default::default()
    };

    let timeout = if task_msg.timeout > 0 {
      Some(Duration::from_secs(task_msg.timeout as u64))
    } else if task_msg.deadline > 0 {
      let remaining = task_msg.deadline - now;
      if remaining > 0 {
        Some(Duration::from_secs(remaining as u64))
      } else {
        None
      }
    } else {
      Some(DEFAULT_TIMEOUT)
    };

    assert_eq!(timeout, Some(Duration::from_secs(300)));

    // Test with deadline
    task_msg.timeout = 0;
    task_msg.deadline = now + 600;

    let timeout = if task_msg.timeout > 0 {
      Some(Duration::from_secs(task_msg.timeout as u64))
    } else if task_msg.deadline > 0 {
      let remaining = task_msg.deadline - now;
      if remaining > 0 {
        Some(Duration::from_secs(remaining as u64))
      } else {
        None
      }
    } else {
      Some(DEFAULT_TIMEOUT)
    };

    assert!(timeout.is_some());
    assert!(timeout.unwrap().as_secs() > 590);
  }

  #[test]
  fn test_expiry_check_logic() {
    let now = chrono::Utc::now().timestamp();

    // Test not expired
    let mut task_msg = crate::proto::TaskMessage {
      deadline: now + 300,
      ..Default::default()
    };

    let is_expired = task_msg.deadline > 0 && now > task_msg.deadline;
    assert!(!is_expired);

    // Test expired
    task_msg.deadline = now - 300;
    let is_expired = task_msg.deadline > 0 && now > task_msg.deadline;
    assert!(is_expired);

    // Test no deadline
    task_msg.deadline = 0;
    let is_expired = task_msg.deadline > 0 && now > task_msg.deadline;
    assert!(!is_expired);
  }
}
