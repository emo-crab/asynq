//! Forwarder 模块
//! Forwarder module
//!
//! 对应 Go 版本的 forwarder.go 职责：
//! Responsibilities corresponding to the Go version's forwarder.go:
//! 定期检查调度任务和重试任务，将到期的任务转发到待处理队列
//! Periodically check scheduled tasks and retry tasks, forward due tasks to the pending queue
//!
//! 参考 Go asynq/forwarder.go
//! Reference: Go asynq/forwarder.go

use crate::base::Broker;
use crate::components::ComponentLifecycle;
use crate::error::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Forwarder 配置
/// Forwarder configuration
#[derive(Debug, Clone)]
pub struct ForwarderConfig {
  /// 检查间隔
  /// Check interval
  pub interval: Duration,
  /// 队列列表
  /// Queue list
  pub queues: Vec<String>,
}

impl Default for ForwarderConfig {
  fn default() -> Self {
    Self {
      interval: Duration::from_secs(5),
      queues: vec!["default".to_string()],
    }
  }
}

/// Forwarder - 负责转发已到期的调度任务和重试任务
/// Forwarder - responsible for forwarding due scheduled tasks and retry tasks
///
/// 对应 Go asynq 的 forwarder 组件
/// Corresponds to Go asynq's forwarder component
///
/// Forwarder 定期检查 scheduled 和 retry 集合，将已到期的任务移动到 pending 队列。
/// The Forwarder periodically checks the scheduled and retry sets, and moves due tasks to the pending queue.
pub struct Forwarder {
  broker: Arc<dyn Broker>,
  config: ForwarderConfig,
  done: Arc<AtomicBool>,
}

impl Forwarder {
  /// 创建新的 Forwarder
  /// Create a new Forwarder
  pub fn new(broker: Arc<dyn Broker>, config: ForwarderConfig) -> Self {
    Self {
      broker,
      config,
      done: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 启动 Forwarder
  /// Start the Forwarder
  ///
  /// 对应 Go 的 forwarder.start()
  /// Corresponds to Go's forwarder.start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("Forwarder: shutting down");
          break;
        }

        // 执行转发任务
        // Execute forwarding tasks
        if let Err(e) = self.forward().await {
          tracing::error!("Forwarder error: {}", e);
        }
      }
    })
  }

  /// 执行转发任务
  /// Execute forwarding tasks
  ///
  /// 对应 Go 的 forwarder.exec()
  /// Corresponds to Go's forwarder.exec()
  async fn forward(&self) -> Result<()> {
    // 转发每个队列的已到期任务
    // Forward due tasks for each queue
    if let Err(e) = self.broker.forward_if_ready(&self.config.queues).await {
      tracing::warn!("Forwarder: failed to forward ready tasks: {}", e);
      return Err(e);
    }

    Ok(())
  }

  /// 停止 Forwarder
  /// Stop the Forwarder
  ///
  /// 对应 Go 的 forwarder.shutdown()
  /// Corresponds to Go's forwarder.shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for Forwarder {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    Forwarder::start(self)
  }

  fn shutdown(&self) {
    Forwarder::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Forwarder::is_done(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_forwarder_config_default() {
    let config = ForwarderConfig::default();
    assert_eq!(config.interval, Duration::from_secs(5));
    assert_eq!(config.queues, vec!["default".to_string()]);
  }

  #[test]
  fn test_forwarder_shutdown() {
    use crate::rdb::RedisBroker;
    use crate::redis::RedisConfig;

    let redis_config = RedisConfig::from_url("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_config).unwrap());
    let config = ForwarderConfig::default();
    let forwarder = Forwarder::new(broker, config);

    assert!(!forwarder.is_done());
    forwarder.shutdown();
    assert!(forwarder.is_done());
  }
}
