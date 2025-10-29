//! Lifecycle 模块
//! Lifecycle module
//!
//! 提供通用的组件生命周期管理 trait
//! Provides a common trait for component components management
//!
//! 此模块定义了统一的生命周期接口，用于管理各种后台组件（如 Aggregator、Forwarder 等）
//! This module defines a unified components interface for managing various background components
//! (such as Aggregator, Forwarder, etc.)

use std::sync::Arc;
use tokio::task::JoinHandle;

pub mod aggregator;
pub mod forwarder;
pub mod healthcheck;
pub mod heartbeat;
pub mod janitor;
pub mod periodic_task_manager;
pub mod processor;
pub mod recoverer;
pub mod subscriber;

/// Lifecycle trait - 组件生命周期管理接口
/// Lifecycle trait - Component components management interface
///
/// 此 trait 定义了组件的基本生命周期操作：启动、关闭和状态检查
/// This trait defines the basic components operations for components: start, shutdown, and state check
///
/// # 实现者 / Implementors
///
/// - [`Aggregator`](aggregator::Aggregator) - 聚合任务到组中进行批量处理
/// - [`Forwarder`](forwarder::Forwarder) - 转发已到期的调度任务和重试任务
/// - [`Healthcheck`](healthcheck::Healthcheck) - 执行健康检查
/// - [`Heartbeat`](heartbeat::Heartbeat) - 发送心跳以维护服务器状态
/// - [`Janitor`](janitor::Janitor) - 清理过期任务和死亡服务器
/// - [`PeriodicTaskManager`](periodic_task_manager::PeriodicTaskManager) - 管理周期性任务
/// - [`Recoverer`](recoverer::Recoverer) - 恢复孤儿任务
/// - [`Subscriber`](subscriber::Subscriber) - 订阅任务事件
///
/// # 注意 / Note
///
/// [`Processor`](processor::Processor) 没有实现此 trait，因为它具有不同的接口：
/// [`Processor`](processor::Processor) does not implement this trait because it has a different interface:
/// - 需要泛型 Handler 参数 / Requires a generic Handler parameter
/// - `start()` 方法接受 `&mut self` / `start()` method takes `&mut self`
/// - `shutdown()` 是异步的 / `shutdown()` is async
///
/// # 示例 / Example
///
/// ```rust,no_run
/// use asynq::components::ComponentLifecycle;
/// use asynq::components::janitor::{Janitor, JanitorConfig};
/// use std::sync::Arc;
/// # use asynq::redis::RedisConnectionType;
/// # use asynq::rdb::RedisBroker;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let redis_config = RedisConnectionType::single("redis://localhost:6379")?;
/// # let broker = Arc::new(RedisBroker::new(redis_config).await?);
/// let janitor = Arc::new(Janitor::new(broker, JanitorConfig::default()));
///
/// // 启动组件
/// // Start component
/// let handle = janitor.clone().start();
///
/// // 检查状态
/// // Check state
/// assert!(!janitor.is_done());
///
/// // 关闭组件
/// // Shutdown component
/// janitor.shutdown();
/// assert!(janitor.is_done());
/// # Ok(())
/// # }
/// ```
pub trait ComponentLifecycle {
  /// 启动组件
  /// Start the component
  ///
  /// 此方法启动组件的后台任务，返回一个 JoinHandle 用于等待任务完成
  /// This method starts the component's background task, returning a JoinHandle to wait for completion
  ///
  /// # 返回 / Returns
  ///
  /// 返回一个 `JoinHandle<()>`，可用于等待组件任务完成
  /// Returns a `JoinHandle<()>` that can be used to wait for the component task to complete
  fn start(self: Arc<Self>) -> JoinHandle<()>;

  /// 关闭组件
  /// Shutdown the component
  ///
  /// 此方法发送关闭信号给组件，组件会在完成当前操作后停止
  /// This method sends a shutdown signal to the component, which will stop after completing current operations
  fn shutdown(&self);

  /// 检查组件是否已完成
  /// Check if the component is done
  ///
  /// # 返回 / Returns
  ///
  /// 如果组件已停止返回 `true`，否则返回 `false`
  /// Returns `true` if the component has stopped, otherwise returns `false`
  fn is_done(&self) -> bool;
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicBool, Ordering};

  // 测试示例实现
  // Test example implementation
  struct TestComponent {
    done: Arc<AtomicBool>,
  }

  impl ComponentLifecycle for TestComponent {
    fn start(self: Arc<Self>) -> JoinHandle<()> {
      tokio::spawn(async move {
        loop {
          if self.done.load(Ordering::Relaxed) {
            break;
          }
          tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
      })
    }

    fn shutdown(&self) {
      self.done.store(true, Ordering::Relaxed);
    }

    fn is_done(&self) -> bool {
      self.done.load(Ordering::Relaxed)
    }
  }

  #[tokio::test]
  async fn test_lifecycle_trait() {
    let component = Arc::new(TestComponent {
      done: Arc::new(AtomicBool::new(false)),
    });

    assert!(!component.is_done());

    let handle = component.clone().start();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    component.shutdown();

    assert!(component.is_done());
    handle.await.unwrap();
  }
}
