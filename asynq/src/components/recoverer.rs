//! Recoverer 模块
//! Recoverer module
//!
//! 对应 Go 版本的 recoverer.go 职责：
//! Responsibilities corresponding to the Go version's recoverer.go:
//! 定期检查并恢复孤儿任务（正在处理但其工作者已崩溃的任务）
//! Periodically check and recover orphaned tasks (tasks being processed but whose workers have crashed)
//!
//! 参考 Go asynq/recoverer.go
//! Reference: Go asynq/recoverer.go

use crate::base::Broker;
use crate::components::ComponentLifecycle;
use crate::error::Result;
use chrono::Utc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Recoverer 配置
/// Recoverer configuration
#[derive(Debug, Clone)]
pub struct RecovererConfig {
  /// 恢复间隔
  /// Recovery interval
  pub interval: Duration,
  /// 队列列表
  /// Queue list
  pub queues: Vec<String>,
}

impl Default for RecovererConfig {
  fn default() -> Self {
    Self {
      interval: Duration::from_secs(8),
      queues: vec!["default".to_string()],
    }
  }
}

/// Recoverer - 负责恢复孤儿任务
/// Recoverer - responsible for recovering orphaned tasks
///
/// 对应 Go asynq 的 recoverer 组件
/// Corresponds to Go asynq's recoverer component
///
/// 孤儿任务是指在 "active" 状态中，但其对应的工作者已经崩溃或超时的任务。
/// Orphaned tasks are tasks in the "active" state whose corresponding worker has crashed or timed out.
/// Recoverer 会定期扫描这些任务并将它们重新加入队列。
/// The Recoverer periodically scans for these tasks and re-queues them.
pub struct Recoverer {
  broker: Arc<dyn Broker>,
  config: RecovererConfig,
  done: Arc<AtomicBool>,
}

impl Recoverer {
  /// 创建新的 Recoverer
  /// Create a new Recoverer
  pub fn new(broker: Arc<dyn Broker>, config: RecovererConfig) -> Self {
    Self {
      broker,
      config,
      done: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 启动 Recoverer
  /// Start the Recoverer
  ///
  /// 对应 Go 的 recoverer.start()
  /// Corresponds to Go's recoverer.start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("Recoverer: shutting down");
          break;
        }

        // 执行恢复任务
        // Execute recovery tasks
        if let Err(e) = self.recover().await {
          tracing::error!("Recoverer error: {}", e);
        }
      }
    })
  }

  /// 执行恢复任务
  /// Execute recovery tasks
  ///
  /// 对应 Go 的 recoverer.exec()
  /// Corresponds to Go's recoverer.exec()
  async fn recover(&self) -> Result<()> {
    self.recover_lease_expired_tasks().await?;
    self.recover_stale_aggregation_sets().await?;
    Ok(())
  }

  /// 恢复 lease 过期的任务
  async fn recover_lease_expired_tasks(&self) -> Result<()> {
    let cutoff = Utc::now() - chrono::Duration::seconds(30);
    let msgs = match self
      .broker
      .list_lease_expired(cutoff, &self.config.queues)
      .await
    {
      Ok(msgs) => msgs,
      Err(e) => {
        tracing::warn!("Recoverer: could not list lease expired tasks: {}", e);
        return Err(e);
      }
    };
    for msg in msgs {
      if msg.retried >= msg.retry {
        if let Err(e) = self.archive(&msg, "lease expired").await {
          tracing::warn!("Recoverer: could not archive lease expired task: {}", e);
        }
      } else if let Err(e) = self.retry(&msg, "lease expired").await {
        tracing::warn!("Recoverer: could not retry lease expired task: {}", e);
      }
    }
    Ok(())
  }

  /// 回收过期的聚合集合
  async fn recover_stale_aggregation_sets(&self) -> Result<()> {
    for q in &self.config.queues {
      if let Err(e) = self.broker.reclaim_stale_aggregation_sets(q).await {
        tracing::warn!(
          "Recoverer: could not reclaim stale aggregation sets in queue {}: {}",
          q,
          e
        );
      }
    }
    Ok(())
  }

  /// 重试任务
  async fn retry(&self, msg: &crate::proto::TaskMessage, err: &str) -> Result<()> {
    let delay = self.retry_delay_func(msg.retried, err, &msg.r#type);
    let retry_at =
      Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::seconds(1));
    self
      .broker
      .retry(msg, retry_at, err, self.is_failure_func(err))
      .await
  }

  /// 归档任务
  async fn archive(&self, msg: &crate::proto::TaskMessage, err: &str) -> Result<()> {
    self.broker.archive(msg, err).await
  }

  /// 简单的重试延迟函数
  fn retry_delay_func(&self, retried: i32, _err: &str, _task_type: &str) -> std::time::Duration {
    // 固定延迟 10 秒，可根据需要调整
    std::time::Duration::from_secs(10 * (retried as u64 + 1))
  }

  /// 简单的失败判定函数
  fn is_failure_func(&self, _err: &str) -> bool {
    true
  }

  /// 停止 Recoverer
  /// Stop the Recoverer
  ///
  /// 对应 Go 的 recoverer.shutdown()
  /// Corresponds to Go's recoverer.shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for Recoverer {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    Recoverer::start(self)
  }

  fn shutdown(&self) {
    Recoverer::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Recoverer::is_done(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_recoverer_config_default() {
    let config = RecovererConfig::default();
    assert_eq!(config.interval, Duration::from_secs(8));
    assert_eq!(config.queues, vec!["default".to_string()]);
  }

  #[test]
  fn test_recoverer_shutdown() {
    use crate::rdb::RedisBroker;
    use crate::redis::RedisConfig;

    let redis_config = RedisConfig::from_url("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_config).unwrap());
    let config = RecovererConfig::default();
    let recoverer = Recoverer::new(broker, config);

    assert!(!recoverer.is_done());
    recoverer.shutdown();
    assert!(recoverer.is_done());
  }
}
