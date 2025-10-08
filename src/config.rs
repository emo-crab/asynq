//! 配置模块
//! Configuration module
//!
//! 定义了服务器和客户端的配置选项
//! Defines configuration options for server and client

use crate::error::{Error, Result};
use crate::base::constants::DEFAULT_QUEUE_NAME;
use std::collections::HashMap;
use std::time::Duration;

/// 服务器配置
/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
  /// 并发工作者数量
  /// Number of concurrent workers
  pub concurrency: usize,
  /// 队列配置，键为队列名称，值为优先级
  /// Queue configuration, key is queue name, value is priority
  pub queues: HashMap<String, i32>,
  /// 是否使用严格优先级
  /// Whether to use strict priority
  pub strict_priority: bool,
  /// 任务检查间隔
  /// Task check interval
  pub task_check_interval: Duration,
  /// 延迟任务检查间隔
  /// Delayed task check interval
  pub delayed_task_check_interval: Duration,
  /// 关闭超时时间
  /// Shutdown timeout
  pub shutdown_timeout: Duration,
  /// 健康检查间隔
  /// Health check interval
  pub health_check_interval: Duration,
  /// 组宽限期
  /// Group grace period
  pub group_grace_period: Duration,
  /// 组最大延迟
  /// Maximum group delay
  pub group_max_delay: Option<Duration>,
  /// 组最大大小
  /// Maximum group size
  pub group_max_size: Option<usize>,
  /// 清理任务间隔
  /// Janitor interval
  pub janitor_interval: Duration,
  /// 清理任务批量大小
  /// Janitor batch size
  pub janitor_batch_size: usize,
  /// 心跳间隔
  /// Heartbeat interval
  pub heartbeat_interval: Duration,
  /// 是否启用组聚合器
  /// Whether to enable group aggregator
  pub group_aggregator_enabled: bool,
}

impl Default for ServerConfig {
  fn default() -> Self {
    let mut queues = HashMap::new();
    queues.insert(DEFAULT_QUEUE_NAME.to_string(), 1);

    Self {
      concurrency: num_cpus::get(),
      queues,
      strict_priority: false,
      task_check_interval: Duration::from_secs(1),
      delayed_task_check_interval: Duration::from_secs(5),
      shutdown_timeout: Duration::from_secs(8),
      health_check_interval: Duration::from_secs(15),
      group_grace_period: Duration::from_secs(60),
      group_max_delay: None,
      group_max_size: None,
      janitor_interval: Duration::from_secs(8),
      janitor_batch_size: 100,
      heartbeat_interval: Duration::from_secs(5),
      group_aggregator_enabled: false,
    }
  }
}

impl ServerConfig {
  /// 创建新的服务器配置
  /// Create a new server configuration
  pub fn new() -> Self {
    Self::default()
  }

  /// 设置并发数
  /// Set the number of concurrent workers
  pub fn concurrency(mut self, concurrency: usize) -> Self {
    self.concurrency = concurrency.max(1);
    self
  }

  /// 设置队列配置
  /// Set the queue configuration
  pub fn queues(mut self, queues: HashMap<String, i32>) -> Self {
    if queues.is_empty() {
      let mut default_queues = HashMap::new();
      default_queues.insert(DEFAULT_QUEUE_NAME.to_string(), 1);
      self.queues = default_queues;
    } else {
      self.queues = queues;
    }
    self
  }

  /// 添加队列
  /// Add a queue
  pub fn add_queue<S: AsRef<str>>(mut self, name: S, priority: i32) -> Result<Self> {
    let name = name.as_ref();
    if name.trim().is_empty() {
      return Err(Error::InvalidQueueName {
        name: name.to_string(),
      });
    }
    if priority <= 0 {
      return Err(Error::config("Queue priority must be positive"));
    }
    self.queues.insert(name.to_string(), priority);
    Ok(self)
  }

  /// 设置严格优先级
  /// Set strict priority
  pub fn strict_priority(mut self, strict: bool) -> Self {
    self.strict_priority = strict;
    self
  }

  /// 设置任务检查间隔
  /// Set the task check interval
  pub fn task_check_interval(mut self, interval: Duration) -> Self {
    self.task_check_interval = interval;
    self
  }

  /// 设置延迟任务检查间隔
  /// Set the delayed task check interval
  pub fn delayed_task_check_interval(mut self, interval: Duration) -> Self {
    self.delayed_task_check_interval = interval;
    self
  }

  /// 设置关闭超时时间
  /// Set the shutdown timeout
  pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
    self.shutdown_timeout = timeout;
    self
  }

  /// 设置健康检查间隔
  /// Set the health check interval
  pub fn health_check_interval(mut self, interval: Duration) -> Self {
    self.health_check_interval = interval;
    self
  }

  /// 设置组宽限期
  /// Set the group grace period
  pub fn group_grace_period(mut self, grace_period: Duration) -> Result<Self> {
    if grace_period < Duration::from_secs(1) {
      return Err(Error::config(
        "Group grace period cannot be less than 1 second",
      ));
    }
    self.group_grace_period = grace_period;
    Ok(self)
  }

  /// 设置组最大延迟
  /// Set the maximum group delay
  pub fn group_max_delay(mut self, max_delay: Duration) -> Self {
    self.group_max_delay = Some(max_delay);
    self
  }

  /// 设置组最大大小
  /// Set the maximum group size
  pub fn group_max_size(mut self, max_size: usize) -> Self {
    self.group_max_size = Some(max_size);
    self
  }

  /// 设置清理任务间隔
  /// Set the janitor interval
  pub fn janitor_interval(mut self, interval: Duration) -> Self {
    self.janitor_interval = interval;
    self
  }

  /// 设置清理任务批量大小
  /// Set the janitor batch size
  pub fn janitor_batch_size(mut self, batch_size: usize) -> Self {
    self.janitor_batch_size = batch_size.max(1);
    self
  }

  /// 启用组聚合器
  /// Enable group aggregator
  pub fn enable_group_aggregator(mut self, enabled: bool) -> Self {
    self.group_aggregator_enabled = enabled;
    self
  }

  /// 验证配置
  /// Validate the configuration
  pub fn validate(&self) -> Result<()> {
    if self.concurrency == 0 {
      return Err(Error::config("Concurrency must be greater than 0"));
    }

    if self.queues.is_empty() {
      return Err(Error::config("At least one queue must be configured"));
    }

    for (name, priority) in &self.queues {
      if name.trim().is_empty() {
        return Err(Error::InvalidQueueName { name: name.clone() });
      }
      if *priority <= 0 {
        return Err(Error::config("Queue priority must be positive"));
      }
    }

    if self.group_grace_period < Duration::from_secs(1) {
      return Err(Error::config(
        "Group grace period cannot be less than 1 second",
      ));
    }

    Ok(())
  }
}

/// 客户端配置
/// Client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
  /// 连接超时时间
  /// Connection timeout
  pub connection_timeout: Duration,
  /// 请求超时时间
  /// Request timeout
  pub request_timeout: Duration,
  /// 最大重试次数
  /// Maximum number of retries
  pub max_retries: usize,
  /// 重试间隔
  /// Retry interval
  pub retry_interval: Duration,
}

impl Default for ClientConfig {
  fn default() -> Self {
    Self {
      connection_timeout: Duration::from_secs(30),
      request_timeout: Duration::from_secs(60),
      max_retries: 3,
      retry_interval: Duration::from_secs(1),
    }
  }
}

impl ClientConfig {
  /// 创建新的客户端配置
  /// Create a new client configuration
  pub fn new() -> Self {
    Self::default()
  }

  /// 设置连接超时时间
  /// Set the connection timeout
  pub fn connection_timeout(mut self, timeout: Duration) -> Self {
    self.connection_timeout = timeout;
    self
  }

  /// 设置请求超时时间
  /// Set the request timeout
  pub fn request_timeout(mut self, timeout: Duration) -> Self {
    self.request_timeout = timeout;
    self
  }

  /// 设置最大重试次数
  /// Set the maximum number of retries
  pub fn max_retries(mut self, max_retries: usize) -> Self {
    self.max_retries = max_retries;
    self
  }

  /// 设置重试间隔
  /// Set the retry interval
  pub fn retry_interval(mut self, interval: Duration) -> Self {
    self.retry_interval = interval;
    self
  }
}

/// 重试延迟函数类型
/// Retry delay function type
pub type RetryDelayFunc = Box<dyn Fn(i32, &str, &str) -> Duration + Send + Sync>;

/// 默认重试延迟函数
/// Default retry delay function
pub fn default_retry_delay(retried: i32, _error: &str, _task_type: &str) -> Duration {
  // 使用指数退避策略
  // Use exponential backoff strategy
  let base_delay = (retried as f64).powf(4.0) as i64 + 15;
  let jitter = rand::random::<i64>() % (30 * (retried as i64 + 1));
  Duration::from_secs((base_delay + jitter).max(1) as u64)
}

/// 错误处理函数类型
/// Error handler function type
pub type ErrorHandlerFunc = Box<dyn Fn(&str, &str, &str) + Send + Sync>;

/// 健康检查函数类型  
/// Health check function type
pub type HealthCheckFunc = Box<dyn Fn(Option<&str>) + Send + Sync>;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_server_config_default() {
    let config = ServerConfig::default();
    assert!(config.concurrency > 0);
    assert_eq!(config.queues.len(), 1);
    assert!(config.queues.contains_key(DEFAULT_QUEUE_NAME));
    assert!(!config.strict_priority);
  }

  #[test]
  fn test_server_config_builder() {
    let mut queues = HashMap::new();
    queues.insert("high".to_string(), 10);
    queues.insert("low".to_string(), 1);

    let config = ServerConfig::new()
      .concurrency(4)
      .queues(queues.clone())
      .strict_priority(true);

    assert_eq!(config.concurrency, 4);
    assert_eq!(config.queues, queues);
    assert!(config.strict_priority);
  }

  #[test]
  fn test_server_config_add_queue() {
    let config = ServerConfig::new().add_queue("test", 5).unwrap();

    assert!(config.queues.contains_key("test"));
    assert_eq!(config.queues.get("test"), Some(&5));
  }

  #[test]
  fn test_server_config_validation() {
    let config = ServerConfig::new();
    assert!(config.validate().is_ok());

    let invalid_config = ServerConfig {
      concurrency: 0,
      ..ServerConfig::default()
    };
    assert!(invalid_config.validate().is_err());
  }

  #[test]
  fn test_client_config_default() {
    let config = ClientConfig::default();
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.request_timeout, Duration::from_secs(60));
    assert_eq!(config.max_retries, 3);
  }

  #[test]
  fn test_default_retry_delay() {
    let delay1 = default_retry_delay(0, "error", "task");
    let delay2 = default_retry_delay(1, "error", "task");
    let delay3 = default_retry_delay(2, "error", "task");

    assert!(delay1 >= Duration::from_secs(1));
    // 由于随机性，我们只检查延迟函数是否返回合理的值
    // Due to randomness, we only check if the delay function returns reasonable values
    assert!(delay2 >= Duration::from_secs(1));
    assert!(delay3 >= Duration::from_secs(1));

    // 检查延迟计算的基本逻辑：重试次数越高，基础延迟越长
    // Check the basic logic of delay calculation: the higher the number of retries, the longer the base delay
    let base_delay_0 = default_retry_delay(0, "error", "task");
    let base_delay_5 = default_retry_delay(5, "error", "task");
    // 第5次重试的基础部分应该明显大于第0次
    // The base part of the 5th retry should be significantly larger than the 0th
    assert!(base_delay_5.as_secs() >= base_delay_0.as_secs());
  }

  #[test]
  fn test_aggregator_enabled_in_config() {
    // Test that group aggregator can be enabled
    let config = ServerConfig::new().enable_group_aggregator(true);

    assert!(config.group_aggregator_enabled);
  }

  #[test]
  fn test_aggregator_disabled_by_default() {
    // Test that group aggregator is disabled by default
    let config = ServerConfig::default();

    assert!(!config.group_aggregator_enabled);
  }

  #[test]
  fn test_aggregator_config_with_group_settings() {
    // Test that aggregator config works with group settings
    let config = ServerConfig::new()
      .group_grace_period(Duration::from_secs(30))
      .unwrap()
      .group_max_delay(Duration::from_secs(120))
      .group_max_size(50)
      .enable_group_aggregator(true);

    assert!(config.group_aggregator_enabled);
    assert_eq!(config.group_grace_period, Duration::from_secs(30));
    assert_eq!(config.group_max_delay, Some(Duration::from_secs(120)));
    assert_eq!(config.group_max_size, Some(50));
  }
}
