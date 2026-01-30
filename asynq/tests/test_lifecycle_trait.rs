//! Lifecycle trait 集成测试
//! Lifecycle trait integration tests
//!
//! 此测试文件演示如何通过 Lifecycle trait 统一管理各种组件
//! This test file demonstrates how to manage various components uniformly through the Lifecycle trait

use asynq::backend::RedisConnectionType;
use asynq::components::aggregator::{Aggregator, AggregatorConfig};
use asynq::components::forwarder::{Forwarder, ForwarderConfig};
use asynq::components::healthcheck::{Healthcheck, HealthcheckConfig};
use asynq::components::heartbeat::{Heartbeat, HeartbeatMeta};
use asynq::components::janitor::{Janitor, JanitorConfig};
use asynq::components::recoverer::{Recoverer, RecovererConfig};
use asynq::components::subscriber::{Subscriber, SubscriberConfig};
use asynq::components::ComponentLifecycle;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// 测试通过 Lifecycle trait 管理多个组件
/// Test managing multiple components through the Lifecycle trait
#[tokio::test]
#[ignore] // Requires Redis to be running
async fn test_lifecycle_trait_usage() {
  use asynq::backend::RedisBroker;
  use asynq::backend::RedisConnectionType;

  // 创建 Redis 连接
  // Create Redis connection
  let redis_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
  let broker = Arc::new(RedisBroker::new(redis_config.clone()).await.unwrap());

  // 创建各种组件
  // Create various components
  let components: Vec<Arc<dyn ComponentLifecycle>> = vec![
    // Aggregator
    Arc::new(Aggregator::new(broker.clone(), AggregatorConfig::default())),
    // Forwarder
    Arc::new(Forwarder::new(broker.clone(), ForwarderConfig::default())),
    // Healthcheck
    Arc::new(Healthcheck::new(
      broker.clone(),
      HealthcheckConfig::default(),
    )),
    // Janitor
    Arc::new(Janitor::new(broker.clone(), JanitorConfig::default())),
    // Recoverer
    Arc::new(Recoverer::new(broker.clone(), RecovererConfig::default())),
    // Subscriber
    Arc::new(Subscriber::new(broker.clone(), SubscriberConfig::default())),
    // PeriodicTaskManager - commented out as it now requires Scheduler and ConfigProvider
    // Arc::new(PeriodicTaskManager::new(
    //   scheduler,
    //   PeriodicTaskManagerConfig::default(),
    //   config_provider,
    // )),
    // Heartbeat
    Arc::new(Heartbeat::new(
      broker.clone(),
      Duration::from_secs(5),
      HeartbeatMeta {
        host: "localhost".to_string(),
        pid: 1234,
        server_uuid: "test-server".to_string(),
        concurrency: 10,
        queues: HashMap::from([("default".to_string(), 1)]),
        strict_priority: false,
        started: SystemTime::now(),
      },
      Arc::new(AtomicUsize::new(0)),
    )),
  ];

  // 启动所有组件
  // Start all components
  let mut handles = Vec::new();
  for component in &components {
    assert!(!component.is_done());
    let handle = component.clone().start();
    handles.push(handle);
  }

  // 等待一小段时间让组件运行
  // Wait a bit for components to run
  tokio::time::sleep(Duration::from_millis(100)).await;

  // 关闭所有组件
  // Shutdown all components
  for component in &components {
    component.shutdown();
  }

  // 验证所有组件都已停止
  // Verify all components have stopped
  for component in &components {
    assert!(component.is_done());
  }

  // 等待所有任务完成
  // Wait for all tasks to complete
  for handle in handles {
    let _ = handle.await;
  }
}

/// 测试单个组件的 Lifecycle trait 实现
/// Test Lifecycle trait implementation for individual components
#[tokio::test]
#[ignore] // Requires Redis to be running
async fn test_janitor_lifecycle() {
  use asynq::backend::RedisBroker;

  let redis_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
  let broker = Arc::new(RedisBroker::new(redis_config).await.unwrap());

  let janitor: Arc<dyn ComponentLifecycle> =
    Arc::new(Janitor::new(broker, JanitorConfig::default()));

  // 测试初始状态
  // Test initial state
  assert!(!janitor.is_done());

  // 启动组件
  // Start component
  let handle = janitor.clone().start();

  // 等待一小段时间
  // Wait a bit
  tokio::time::sleep(Duration::from_millis(50)).await;

  // 仍在运行
  // Still running
  assert!(!janitor.is_done());

  // 关闭组件
  // Shutdown component
  janitor.shutdown();

  // 验证已停止
  // Verify stopped
  assert!(janitor.is_done());

  // 等待任务完成
  // Wait for task to complete
  handle.await.unwrap();
}

/// 测试通过 trait object 管理组件的通用函数
/// Test generic function that manages components through trait objects
#[tokio::test]
#[ignore] // Requires Redis to be running
async fn test_generic_component_management() {
  use asynq::backend::RedisBroker;

  let redis_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
  let broker = Arc::new(RedisBroker::new(redis_config).await.unwrap());

  // 辅助函数：启动并运行一段时间后关闭组件
  // Helper function: start, run for a while, then shutdown component
  async fn manage_component(component: Arc<dyn ComponentLifecycle>, run_duration: Duration) {
    let handle = component.clone().start();
    tokio::time::sleep(run_duration).await;
    component.shutdown();
    handle.await.unwrap();
  }

  // 使用相同的函数管理不同的组件
  // Use the same function to manage different components
  let forwarder: Arc<dyn ComponentLifecycle> =
    Arc::new(Forwarder::new(broker.clone(), ForwarderConfig::default()));
  manage_component(forwarder, Duration::from_millis(50)).await;

  let recoverer: Arc<dyn ComponentLifecycle> =
    Arc::new(Recoverer::new(broker.clone(), RecovererConfig::default()));
  manage_component(recoverer, Duration::from_millis(50)).await;
}
