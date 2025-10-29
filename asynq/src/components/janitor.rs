//! Janitor 模块
//! Janitor module
//!
//! 对应 Go 版本的 janitor.go 职责：
//! Responsibilities corresponding to the Go version's janitor.go:
//! 定期清理过期的已完成任务、死亡服务器状态等
//! Periodically clean up expired completed tasks, dead server states, etc.
//!
//! 参考 Go asynq/janitor.go
//! Reference: Go asynq/janitor.go

use crate::base::Broker;
use crate::components::ComponentLifecycle;
use crate::error::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Janitor 配置
/// Janitor configuration
#[derive(Debug, Clone)]
pub struct JanitorConfig {
  /// 清理间隔
  /// Cleanup interval
  pub interval: Duration,
  /// 批量大小
  /// Batch size
  pub batch_size: usize,
  /// 队列列表
  /// Queue list
  pub queues: Vec<String>,
}

impl Default for JanitorConfig {
  fn default() -> Self {
    Self {
      interval: Duration::from_secs(8),
      batch_size: 100,
      queues: vec!["default".to_string()],
    }
  }
}

/// Janitor - 负责定期清理过期任务和死亡服务器
/// Janitor - responsible for periodically cleaning up expired tasks and dead servers
///
/// 对应 Go asynq 的 janitor 组件
/// Corresponds to Go asynq's janitor component
pub struct Janitor {
  broker: Arc<dyn Broker>,
  config: JanitorConfig,
  done: Arc<AtomicBool>,
}

impl Janitor {
  /// 创建新的 Janitor
  /// Create a new Janitor
  pub fn new(broker: Arc<dyn Broker>, config: JanitorConfig) -> Self {
    Self {
      broker,
      config,
      done: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 启动 Janitor
  /// Start the Janitor
  ///
  /// 对应 Go 的 janitor.start()
  /// Corresponds to Go's janitor.start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("Janitor: shutting down");
          break;
        }

        // 执行清理任务
        // Execute cleanup tasks
        if let Err(e) = self.cleanup().await {
          tracing::error!("Janitor cleanup error: {}", e);
        }
      }
    })
  }

  /// 执行清理任务
  /// Execute cleanup tasks
  ///
  /// 对应 Go 的 janitor.exec()
  /// Corresponds to Go's janitor.exec()
  async fn cleanup(&self) -> Result<()> {
    // 清理每个队列的过期任务
    // Cleanup expired tasks for each queue
    for queue in &self.config.queues {
      // 清理过期的完成任务
      // Cleanup expired completed tasks
      if let Err(e) = self.broker.delete_expired_completed_tasks(queue).await {
        tracing::warn!(
          "Janitor: failed to cleanup expired completed tasks for queue {}: {}",
          queue,
          e
        );
      }
    }
    Ok(())
  }

  /// 停止 Janitor
  /// Stop the Janitor
  ///
  /// 对应 Go 的 janitor.shutdown()
  /// Corresponds to Go's janitor.shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for Janitor {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    Janitor::start(self)
  }

  fn shutdown(&self) {
    Janitor::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Janitor::is_done(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::redis::RedisConnectionType;

  #[test]
  fn test_janitor_config_default() {
    let config = JanitorConfig::default();
    assert_eq!(config.interval, Duration::from_secs(8));
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.queues, vec!["default".to_string()]);
  }

  #[tokio::test]
  async fn test_janitor_shutdown() {
    use crate::rdb::RedisBroker;
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_connection_config).await.unwrap());
    let config = JanitorConfig::default();
    let janitor = Janitor::new(broker, config);

    assert!(!janitor.is_done());
    janitor.shutdown();
    assert!(janitor.is_done());
  }
}
