//! Subscriber 模块
//! Subscriber module
//!
//! 对应 Go 版本的 subscriber.go 职责：
//! Responsibilities corresponding to the Go version's subscriber.go:
//! 订阅任务事件和通知
//! Subscribe to task events and notifications
//!
//! 参考 Go asynq/subscriber.go
//! Reference: Go asynq/subscriber.go

use crate::base::Broker;
use crate::components::ComponentLifecycle;
use crate::error::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// 订阅事件类型
/// Subscription event type
#[derive(Debug, Clone, PartialEq)]
pub enum SubscriptionEvent {
  /// 任务排队事件
  /// Task enqueued event
  TaskEnqueued {
    queue: String,
    task_id: String,
    task_type: String,
  },
  /// 任务开始处理事件
  /// Task processing started event
  TaskStarted {
    queue: String,
    task_id: String,
    task_type: String,
  },
  /// 任务完成事件
  /// Task completed event
  TaskCompleted {
    queue: String,
    task_id: String,
    task_type: String,
  },
  /// 任务失败事件
  /// Task failed event
  TaskFailed {
    queue: String,
    task_id: String,
    task_type: String,
    error: String,
  },
  /// 任务重试事件
  /// Task retried event
  TaskRetried {
    queue: String,
    task_id: String,
    task_type: String,
    retry_count: i32,
  },
  /// 任务取消事件
  /// Task cancelled event
  TaskCancelled { task_id: String },
  /// 服务器状态变化事件
  /// Server state changed event
  ServerStateChanged { server_id: String, status: String },
}

/// 订阅者配置
/// Subscriber configuration
#[derive(Debug, Clone)]
pub struct SubscriberConfig {
  /// 缓冲区大小
  /// Buffer size
  pub buffer_size: usize,
}

impl Default for SubscriberConfig {
  fn default() -> Self {
    Self { buffer_size: 100 }
  }
}

/// Subscriber - 负责订阅任务事件和通知
/// Subscriber - responsible for subscribing to task events and notifications
///
/// 对应 Go asynq 的 subscriber 组件
/// Corresponds to Go asynq's subscriber component
///
/// Subscriber 允许应用程序订阅任务生命周期事件，用于监控、日志记录或触发其他操作。
/// Subscriber allows applications to subscribe to task components events for monitoring, logging, or triggering other actions.
pub struct Subscriber {
  broker: Arc<dyn Broker>,
  #[allow(dead_code)] // Will be used for configuration in future enhancements
  config: SubscriberConfig,
  done: Arc<AtomicBool>,
  event_tx: mpsc::Sender<SubscriptionEvent>,
  event_rx: Option<mpsc::Receiver<SubscriptionEvent>>,
}

impl Subscriber {
  /// 创建新的 Subscriber
  /// Create a new Subscriber
  pub fn new(broker: Arc<dyn Broker>, config: SubscriberConfig) -> Self {
    let (event_tx, event_rx) = mpsc::channel(config.buffer_size);

    Self {
      broker,
      config,
      done: Arc::new(AtomicBool::new(false)),
      event_tx,
      event_rx: Some(event_rx),
    }
  }

  /// 获取事件接收器
  /// Get event receiver
  ///
  /// 调用此方法后，接收器的所有权将转移，后续调用将返回 None
  /// After calling this method, ownership of the receiver is transferred, subsequent calls will return None
  pub fn take_receiver(&mut self) -> Option<mpsc::Receiver<SubscriptionEvent>> {
    self.event_rx.take()
  }

  /// 启动 Subscriber
  /// Start the Subscriber
  ///
  /// 对应 Go 的 subscriber.start()
  /// Corresponds to Go's subscriber.start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tracing::info!("starting subscriber");
    tokio::spawn(async move {
      // 订阅任务取消事件
      // Subscribe to task cancellation events
      match self.broker.cancellation_pub_sub().await {
        Ok(mut stream) => {
          use futures::StreamExt;

          loop {
            tokio::select! {
              // 检查是否需要关闭
              // Check if shutdown is requested
              _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                if self.done.load(Ordering::Relaxed) {
                  tracing::debug!("Subscriber: shutting down");
                  break;
                }
              }

              // 接收取消事件
              // Receive cancellation events
              Some(result) = stream.next() => {
                tracing::debug!("Subscriber: received subscription event");
                match result {
                  Ok(task_id) => {
                    tracing::debug!("Subscriber: received cancellation for task {}", task_id);

                    // 发布取消事件
                    // Publish cancellation event
                    let event = SubscriptionEvent::TaskCancelled { task_id };
                    if let Err(e) = self.event_tx.send(event).await {
                      tracing::warn!("Subscriber: failed to forward cancellation event: {}", e);
                    }
                  }
                  Err(e) => {
                    tracing::warn!("Subscriber: error receiving cancellation event: {}", e);
                  }
                }
              }
            }
          }
        }
        Err(e) => {
          tracing::error!(
            "Subscriber: failed to subscribe to cancellation events: {}",
            e
          );

          // 如果订阅失败，仍然需要保持运行，等待关闭信号
          // If subscription fails, still need to keep running and wait for shutdown signal
          loop {
            if self.done.load(Ordering::Relaxed) {
              tracing::debug!("Subscriber: shutting down");
              break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
          }
        }
      }
    })
  }

  /// 发布事件
  /// Publish event
  ///
  /// 此方法允许其他组件发布事件到订阅者
  /// This method allows other components to publish events to subscribers
  pub async fn publish(&self, event: SubscriptionEvent) -> Result<()> {
    if let Err(e) = self.event_tx.send(event).await {
      tracing::warn!("Subscriber: failed to publish event: {}", e);
      return Err(crate::error::Error::other(format!(
        "Failed to publish event: {e}"
      )));
    }
    Ok(())
  }

  /// 停止 Subscriber
  /// Stop the Subscriber
  ///
  /// 对应 Go 的 subscriber.shutdown()
  /// Corresponds to Go's subscriber.shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for Subscriber {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    Subscriber::start(self)
  }

  fn shutdown(&self) {
    Subscriber::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Subscriber::is_done(self)
  }
}
#[cfg(feature = "default")]
#[cfg(test)]
mod tests {
  use super::*;
  use crate::backend::RedisConnectionType;

  #[test]
  fn test_subscriber_config_default() {
    let config = SubscriberConfig::default();
    assert_eq!(config.buffer_size, 100);
  }

  #[tokio::test]
  async fn test_subscriber_shutdown() {
    use crate::backend::RedisBroker;
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_connection_config).await.unwrap());
    let config = SubscriberConfig::default();
    let subscriber = Subscriber::new(broker, config);

    assert!(!subscriber.is_done());
    subscriber.shutdown();
    assert!(subscriber.is_done());
  }

  #[tokio::test]
  async fn test_subscriber_publish_receive() {
    use crate::backend::RedisBroker;
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();

    let broker = Arc::new(RedisBroker::new(redis_connection_config).await.unwrap());
    let config = SubscriberConfig::default();
    let mut subscriber = Subscriber::new(broker, config);

    let event = SubscriptionEvent::TaskEnqueued {
      queue: "default".to_string(),
      task_id: "task123".to_string(),
      task_type: "email:send".to_string(),
    };

    // 发布事件
    subscriber.publish(event.clone()).await.unwrap();

    // 接收事件
    if let Some(mut rx) = subscriber.take_receiver() {
      let received = rx.recv().await.unwrap();
      assert_eq!(received, event);
    }
  }

  /// 测试任务取消事件的发布和接收
  /// Test publishing and receiving task cancellation events
  #[tokio::test]
  #[ignore] // 需要运行 Redis 服务器才能运行此测试 / Requires running Redis server to run this test
  async fn test_subscriber_cancellation_pubsub() {
    use crate::backend::RedisBroker;
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
    let broker: Arc<dyn Broker> =
      Arc::new(RedisBroker::new(redis_connection_config).await.unwrap());

    // 创建订阅者
    let config = SubscriberConfig::default();
    let mut subscriber = Subscriber::new(Arc::clone(&broker), config);
    let mut rx = subscriber.take_receiver().unwrap();

    // 启动订阅者
    let subscriber_arc = Arc::new(subscriber);
    let handle = subscriber_arc.clone().start();

    // 等待订阅者启动
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 发布取消事件
    let task_id = "test_task_123";
    broker.publish_cancellation(task_id).await.unwrap();

    // 接收取消事件
    let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await;

    // 验证接收到的事件
    assert!(received.is_ok());
    if let Ok(Some(event)) = received {
      match event {
        SubscriptionEvent::TaskCancelled {
          task_id: received_id,
        } => {
          assert_eq!(received_id, task_id);
        }
        _ => panic!("Expected TaskCancelled event"),
      }
    }

    // 关闭订阅者
    subscriber_arc.shutdown();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
  }
}
