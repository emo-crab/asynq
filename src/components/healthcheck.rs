//! Healthcheck 模块
//! Healthcheck module
//!
//! 对应 Go 版本的 healthcheck.go 职责：
//! Responsibilities corresponding to the Go version's healthcheck.go:
//! 提供服务器健康检查功能，用于监控服务器状态
//! Provides server health check functionality for monitoring server status
//!
//! 参考 Go asynq/healthcheck.go
//! Reference: Go asynq/healthcheck.go

use crate::base::Broker;
use crate::components::ComponentLifecycle;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Healthcheck 配置
/// Healthcheck configuration
#[derive(Debug, Clone)]
pub struct HealthcheckConfig {
  /// 健康检查间隔
  /// Health check interval
  pub interval: Duration,
}

impl Default for HealthcheckConfig {
  fn default() -> Self {
    Self {
      interval: Duration::from_secs(15),
    }
  }
}

/// HealthcheckFunc - 健康检查函数类型
/// HealthcheckFunc - Health check function type
pub type HealthcheckFunc = Arc<dyn Fn() -> bool + Send + Sync>;

/// Healthcheck - 负责定期执行健康检查
/// Healthcheck - responsible for periodically executing health checks
///
/// 对应 Go asynq 的 healthcheck 组件
/// Corresponds to Go asynq's healthcheck component
///
/// 健康检查用于监控服务器状态，可以用于负载均衡器或监控系统。
/// Health checks are used to monitor server status and can be used by load balancers or monitoring systems.
pub struct Healthcheck {
  broker: Arc<dyn Broker>,
  config: HealthcheckConfig,
  done: Arc<AtomicBool>,
  is_healthy: Arc<AtomicBool>,
  custom_check: Option<HealthcheckFunc>,
}

impl Healthcheck {
  /// 创建新的 Healthcheck
  /// Create a new Healthcheck
  pub fn new(broker: Arc<dyn Broker>, config: HealthcheckConfig) -> Self {
    Self {
      broker,
      config,
      done: Arc::new(AtomicBool::new(false)),
      is_healthy: Arc::new(AtomicBool::new(true)),
      custom_check: None,
    }
  }

  /// 设置自定义健康检查函数
  /// Set custom health check function
  pub fn with_custom_check(mut self, check: HealthcheckFunc) -> Self {
    self.custom_check = Some(check);
    self
  }

  /// 启动 Healthcheck
  /// Start the Healthcheck
  ///
  /// 对应 Go 的 healthcheck.start()
  /// Corresponds to Go's healthcheck.start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("Healthcheck: shutting down");
          break;
        }

        // 执行健康检查
        // Execute health check
        self.check().await;
      }
    })
  }

  /// 执行健康检查
  /// Execute health check
  ///
  /// 对应 Go 的 healthcheck.exec()
  /// Corresponds to Go's healthcheck.exec()
  async fn check(&self) {
    let mut healthy = true;

    // 检查 Redis 连接
    // Check Redis connection
    if let Err(e) = self.broker.ping().await {
      tracing::warn!("Healthcheck: Redis ping failed: {}", e);
      healthy = false;
    }

    // 执行自定义健康检查
    // Execute custom health check
    if let Some(ref check) = self.custom_check {
      if !check() {
        tracing::warn!("Healthcheck: custom check failed");
        healthy = false;
      }
    }

    self.is_healthy.store(healthy, Ordering::Relaxed);
  }

  /// 检查服务器是否健康
  /// Check if server is healthy
  pub fn is_healthy(&self) -> bool {
    self.is_healthy.load(Ordering::Relaxed)
  }

  /// 停止 Healthcheck
  /// Stop the Healthcheck
  ///
  /// 对应 Go 的 healthcheck.shutdown()
  /// Corresponds to Go's healthcheck.shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for Healthcheck {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    Healthcheck::start(self)
  }

  fn shutdown(&self) {
    Healthcheck::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Healthcheck::is_done(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_healthcheck_config_default() {
    let config = HealthcheckConfig::default();
    assert_eq!(config.interval, Duration::from_secs(15));
  }

  #[test]
  fn test_healthcheck_shutdown() {
    use crate::rdb::RedisBroker;
    use crate::redis::RedisConfig;

    let redis_config = RedisConfig::from_url("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_config).unwrap());
    let config = HealthcheckConfig::default();
    let healthcheck = Healthcheck::new(broker, config);

    assert!(!healthcheck.is_done());
    assert!(healthcheck.is_healthy());
    healthcheck.shutdown();
    assert!(healthcheck.is_done());
  }

  #[test]
  fn test_healthcheck_with_custom_check() {
    use crate::rdb::RedisBroker;
    use crate::redis::RedisConfig;

    let redis_config = RedisConfig::from_url("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_config).unwrap());
    let config = HealthcheckConfig::default();

    let custom_check: HealthcheckFunc = Arc::new(|| true);
    let healthcheck = Healthcheck::new(broker, config).with_custom_check(custom_check);

    assert!(healthcheck.is_healthy());
  }
}
