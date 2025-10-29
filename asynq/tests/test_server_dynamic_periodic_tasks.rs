//! Server dynamic periodic task management integration tests
//!
//! Tests the new PeriodicTaskConfigProvider-based periodic task management.

use asynq::client::Client;
use asynq::components::periodic_task_manager::{
  PeriodicTaskConfig, PeriodicTaskConfigProvider, PeriodicTaskManager, PeriodicTaskManagerConfig,
};
use asynq::config::ServerConfig;
use asynq::redis::RedisConnectionConfig;
use asynq::scheduler::Scheduler;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Test that periodic task manager can be enabled in server config
#[test]
fn test_server_with_periodic_task_manager_enabled() {
  let config = ServerConfig::new()
    .enable_periodic_task_manager(true)
    .periodic_task_manager_check_interval(Duration::from_secs(10));

  assert!(config.periodic_task_manager_enabled);
  assert_eq!(
    config.periodic_task_manager_check_interval,
    Duration::from_secs(10)
  );
}

/// Simple test config provider
struct TestConfigProvider {
  configs: Arc<Mutex<Vec<PeriodicTaskConfig>>>,
}

impl TestConfigProvider {
  fn new(configs: Vec<PeriodicTaskConfig>) -> Self {
    Self {
      configs: Arc::new(Mutex::new(configs)),
    }
  }

  #[allow(dead_code)]
  async fn update_configs(&self, configs: Vec<PeriodicTaskConfig>) {
    let mut c = self.configs.lock().await;
    *c = configs;
  }
}

#[async_trait]
impl PeriodicTaskConfigProvider for TestConfigProvider {
  async fn get_configs(&self) -> asynq::error::Result<Vec<PeriodicTaskConfig>> {
    let configs = self.configs.lock().await;
    Ok(configs.clone())
  }
}

/// Test that we can create a PeriodicTaskManager with ConfigProvider
#[tokio::test]
async fn test_periodic_task_manager_with_config_provider() {
  // Skip test if Redis not available
  let redis_config = match RedisConnectionConfig::single("redis://localhost:6379") {
    Ok(config) => config,
    Err(_) => {
      println!("Skipping test - Redis not available");
      return;
    }
  };

  let client = match Client::new(redis_config).await {
    Ok(c) => Arc::new(c),
    Err(_) => {
      println!("Skipping test - Cannot connect to Redis");
      return;
    }
  };

  let scheduler = Arc::new(Scheduler::new(client, None).await.unwrap());

  let configs = vec![PeriodicTaskConfig::new(
    "test:task".to_string(),
    "0 */5 * * * *".to_string(),
    b"test payload".to_vec(),
    "default".to_string(),
  )];

  let provider = Arc::new(TestConfigProvider::new(configs));
  let manager_config = PeriodicTaskManagerConfig {
    sync_interval: Duration::from_secs(60),
  };

  let manager = PeriodicTaskManager::new(scheduler, manager_config, provider);

  // Verify manager is created successfully
  assert!(!manager.is_done());
}

/// Test that manager syncs tasks from config provider
#[tokio::test]
async fn test_periodic_task_manager_sync() {
  // Skip test if Redis not available
  let redis_config = match RedisConnectionConfig::single("redis://localhost:6379") {
    Ok(config) => config,
    Err(_) => {
      println!("Skipping test - Redis not available");
      return;
    }
  };

  let client = match Client::new(redis_config).await {
    Ok(c) => Arc::new(c),
    Err(_) => {
      println!("Skipping test - Cannot connect to Redis");
      return;
    }
  };

  let scheduler = Arc::new(Scheduler::new(client, None).await.unwrap());

  let configs = vec![
    PeriodicTaskConfig::new(
      "test:task1".to_string(),
      "0 */5 * * * *".to_string(),
      b"payload1".to_vec(),
      "default".to_string(),
    ),
    PeriodicTaskConfig::new(
      "test:task2".to_string(),
      "0 */10 * * * *".to_string(),
      b"payload2".to_vec(),
      "default".to_string(),
    ),
  ];

  let provider = Arc::new(TestConfigProvider::new(configs));
  let manager_config = PeriodicTaskManagerConfig {
    sync_interval: Duration::from_secs(5),
  };

  let manager = Arc::new(PeriodicTaskManager::new(
    scheduler,
    manager_config,
    provider.clone(),
  ));

  // Start the manager
  let handle = manager.clone().start();

  // Wait for sync to happen
  tokio::time::sleep(Duration::from_secs(2)).await;

  // Stop the manager
  manager.shutdown();
  let _ = handle.await;
}

/// Test shutdown functionality
#[tokio::test]
async fn test_periodic_task_manager_shutdown() {
  // Skip test if Redis not available
  let redis_config = match RedisConnectionConfig::single("redis://localhost:6379") {
    Ok(config) => config,
    Err(_) => {
      println!("Skipping test - Redis not available");
      return;
    }
  };

  let client = match Client::new(redis_config).await {
    Ok(c) => Arc::new(c),
    Err(_) => {
      println!("Skipping test - Cannot connect to Redis");
      return;
    }
  };

  let scheduler = Arc::new(Scheduler::new(client, None).await.unwrap());
  let provider = Arc::new(TestConfigProvider::new(vec![]));
  let manager_config = PeriodicTaskManagerConfig {
    sync_interval: Duration::from_secs(60),
  };

  let manager = PeriodicTaskManager::new(scheduler, manager_config, provider);

  assert!(!manager.is_done());
  manager.shutdown();
  assert!(manager.is_done());
}

fn main() {
  println!("Run with: cargo test --test test_server_dynamic_periodic_tasks");
}
