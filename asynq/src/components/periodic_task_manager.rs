//! Periodic Task Manager 模块
//! Periodic Task Manager module
//!
//! 对应 Go 版本的 periodic_task_manager.go 职责：
//! Responsibilities corresponding to the Go version's periodic_task_manager.go:
//! 管理周期性任务的注册、调度和执行
//! Manage the registration, scheduling, and execution of periodic tasks
//!
//! 参考 Go asynq/periodic_task_manager.go
//! Reference: Go asynq/periodic_task_manager.go
//!
//! 注意：此模块集成 Scheduler，并通过 PeriodicTaskConfigProvider 动态同步周期性任务
//! Note: This module integrates Scheduler and dynamically syncs periodic tasks via PeriodicTaskConfigProvider

use crate::components::ComponentLifecycle;
use crate::error::Result;
use crate::scheduler::{PeriodicTask, Scheduler};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Periodic Task Config Provider trait
/// 周期性任务配置提供者特性
///
/// 对应 Go 版本的 PeriodicTaskConfigProvider 接口
/// Corresponds to Go version's PeriodicTaskConfigProvider interface
#[async_trait]
pub trait PeriodicTaskConfigProvider: Send + Sync {
  /// 获取周期性任务配置列表
  /// Get list of periodic task configurations
  ///
  /// 返回当前应该注册的所有周期性任务
  /// Returns all periodic tasks that should be registered
  async fn get_configs(&self) -> Result<Vec<PeriodicTaskConfig>>;
}

/// 周期性任务配置
/// Periodic task configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodicTaskConfig {
  /// 任务唯一标识符（用于区分不同配置）
  /// Unique task identifier (used to differentiate configurations)
  pub task: String,
  /// Cron 表达式
  /// Cron expression
  pub cron: String,
  /// 任务载荷
  /// Task payload
  pub payload: Vec<u8>,
  /// 队列名称
  /// Queue name
  pub queue: String,
}

impl PeriodicTaskConfig {
  /// 创建周期性任务配置
  /// Create periodic task configuration
  pub fn new(task: String, cron: String, payload: Vec<u8>, queue: String) -> Self {
    Self {
      task,
      cron,
      payload,
      queue,
    }
  }

  /// 转换为 PeriodicTask
  /// Convert to PeriodicTask
  pub fn to_periodic_task(&self) -> Result<PeriodicTask> {
    PeriodicTask::new(
      self.task.clone(),
      self.cron.clone(),
      self.payload.clone(),
      self.queue.clone(),
    )
    .map_err(|e| crate::error::Error::other(format!("Failed to create PeriodicTask: {e}")))
  }

  /// 生成配置的唯一键（包含 payload 的哈希）
  /// Generate unique key for configuration (including payload hash)
  pub fn config_key(&self) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    self.payload.hash(&mut hasher);
    let payload_hash = hasher.finish();

    format!(
      "{}:{}:{}:{}",
      self.task, self.cron, self.queue, payload_hash
    )
  }
}

/// Periodic Task Manager 配置
/// Periodic Task Manager configuration
#[derive(Debug, Clone)]
pub struct PeriodicTaskManagerConfig {
  /// 同步间隔
  /// Sync interval
  pub sync_interval: std::time::Duration,
}

impl Default for PeriodicTaskManagerConfig {
  fn default() -> Self {
    Self {
      sync_interval: std::time::Duration::from_secs(60),
    }
  }
}

/// Periodic Task Manager - 管理周期性任务
/// Periodic Task Manager - manages periodic tasks
///
/// 对应 Go asynq 的 PeriodicTaskManager 组件
/// Corresponds to Go asynq's PeriodicTaskManager component
///
/// 此组件负责：
/// This component is responsible for:
/// 1. 从 PeriodicTaskConfigProvider 获取任务配置
/// 1. Get task configurations from PeriodicTaskConfigProvider
/// 2. 通过 Scheduler 注册和注销周期性任务
/// 2. Register and unregister periodic tasks via Scheduler
/// 3. 根据 diff 结果动态同步任务
/// 3. Dynamically sync tasks based on diff results
pub struct PeriodicTaskManager {
  scheduler: Arc<Scheduler>,
  config: PeriodicTaskManagerConfig,
  config_provider: Arc<dyn PeriodicTaskConfigProvider>,
  registered_tasks: Arc<Mutex<HashMap<String, String>>>, // config_key -> entry_id
  done: Arc<AtomicBool>,
}

impl PeriodicTaskManager {
  /// 创建新的 Periodic Task Manager
  /// Create a new Periodic Task Manager
  pub fn new(
    scheduler: Arc<Scheduler>,
    config: PeriodicTaskManagerConfig,
    config_provider: Arc<dyn PeriodicTaskConfigProvider>,
  ) -> Self {
    Self {
      scheduler,
      config,
      config_provider,
      registered_tasks: Arc::new(Mutex::new(HashMap::new())),
      done: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 启动 Periodic Task Manager
  /// Start the Periodic Task Manager
  ///
  /// 对应 Go 的 manager.Start()
  /// Corresponds to Go's manager.Start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    // Start the scheduler first
    let scheduler = self.scheduler.clone();
    tokio::spawn(async move {
      scheduler.start().await;
    });

    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.sync_interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("PeriodicTaskManager: shutting down");
          break;
        }

        // 同步任务
        // Sync tasks
        if let Err(e) = self.sync_tasks().await {
          tracing::error!("PeriodicTaskManager sync error: {}", e);
        }
      }
    })
  }

  /// 同步任务 - 从 config provider 获取配置并更新注册
  /// Sync tasks - get configurations from config provider and update registrations
  async fn sync_tasks(&self) -> Result<()> {
    // 获取新配置
    // Get new configurations
    let new_configs = self.config_provider.get_configs().await?;
    let new_config_keys: HashSet<String> = new_configs.iter().map(|c| c.config_key()).collect();

    let mut registered = self.registered_tasks.lock().await;
    let current_keys: HashSet<String> = registered.keys().cloned().collect();

    // 计算需要添加和删除的任务
    // Calculate tasks to add and remove
    let to_add: Vec<_> = new_configs
      .iter()
      .filter(|c| !current_keys.contains(&c.config_key()))
      .collect();

    let to_remove: Vec<_> = current_keys.difference(&new_config_keys).cloned().collect();

    // 注销不再需要的任务
    // Unregister tasks that are no longer needed
    for config_key in to_remove {
      if let Some(entry_id) = registered.remove(&config_key) {
        if let Err(e) = self.scheduler.unregister(&entry_id).await {
          tracing::error!("Failed to unregister task {}: {}", config_key, e);
        } else {
          tracing::info!("PeriodicTaskManager: unregistered task {}", config_key);
        }
      }
    }

    // 注册新任务
    // Register new tasks
    for config in to_add {
      let periodic_task = match config.to_periodic_task() {
        Ok(task) => task,
        Err(e) => {
          tracing::error!("Failed to create PeriodicTask from config: {}", e);
          continue;
        }
      };

      match self.scheduler.register(periodic_task, &config.queue).await {
        Ok(entry_id) => {
          let config_key = config.config_key();
          registered.insert(config_key.clone(), entry_id);
          tracing::info!(
            "PeriodicTaskManager: registered task {} with cron '{}'",
            config_key,
            config.cron
          );
        }
        Err(e) => {
          tracing::error!("Failed to register task {}: {}", config.task, e);
        }
      }
    }

    Ok(())
  }

  /// 停止 Periodic Task Manager
  /// Stop the Periodic Task Manager
  ///
  /// 对应 Go 的 manager.Shutdown()
  /// Corresponds to Go's manager.Shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);

    // Stop the scheduler
    let scheduler = self.scheduler.clone();
    tokio::spawn(async move {
      scheduler.stop().await;
    });
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for PeriodicTaskManager {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    PeriodicTaskManager::start(self)
  }

  fn shutdown(&self) {
    PeriodicTaskManager::shutdown(self)
  }

  fn is_done(&self) -> bool {
    PeriodicTaskManager::is_done(self)
  }
}
#[cfg(feature = "default")]
#[cfg(test)]
mod tests {
  use super::*;
  use crate::backend::RedisConnectionType;
  use crate::client::Client;

  // 测试配置提供者
  // Test configuration provider
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
    async fn set_configs(&self, configs: Vec<PeriodicTaskConfig>) {
      let mut c = self.configs.lock().await;
      *c = configs;
    }
  }

  #[async_trait]
  impl PeriodicTaskConfigProvider for TestConfigProvider {
    async fn get_configs(&self) -> Result<Vec<PeriodicTaskConfig>> {
      let configs = self.configs.lock().await;
      Ok(configs.clone())
    }
  }

  #[test]
  fn test_periodic_task_manager_config_default() {
    let config = PeriodicTaskManagerConfig::default();
    assert_eq!(config.sync_interval, std::time::Duration::from_secs(60));
  }

  #[test]
  fn test_periodic_task_config() {
    let config = PeriodicTaskConfig::new(
      "test:task".to_string(),
      "* * * * * *".to_string(),
      b"test payload".to_vec(),
      "default".to_string(),
    );

    assert_eq!(config.task, "test:task");
    assert_eq!(config.cron, "* * * * * *");
    assert_eq!(config.queue, "default");
  }

  #[tokio::test]
  #[ignore] // Requires Redis to be running
  async fn test_periodic_task_manager_sync() {
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
    let client = Arc::new(Client::new(redis_connection_config).await.unwrap());
    let scheduler = Arc::new(Scheduler::new(client, None).await.unwrap());

    let initial_configs = vec![PeriodicTaskConfig::new(
      "task1".to_string(),
      "* * * * * *".to_string(),
      b"payload1".to_vec(),
      "default".to_string(),
    )];

    let provider = Arc::new(TestConfigProvider::new(initial_configs));
    let config = PeriodicTaskManagerConfig::default();
    let manager = PeriodicTaskManager::new(scheduler, config, provider.clone());

    // 执行首次同步
    // Perform initial sync
    manager.sync_tasks().await.unwrap();

    let registered = manager.registered_tasks.lock().await;
    assert_eq!(registered.len(), 1);
  }

  #[tokio::test]
  #[ignore] // Requires Redis to be running
  async fn test_periodic_task_manager_shutdown() {
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
    let client = Arc::new(Client::new(redis_connection_config).await.unwrap());
    let scheduler = Arc::new(Scheduler::new(client, None).await.unwrap());

    let provider = Arc::new(TestConfigProvider::new(vec![]));
    let config = PeriodicTaskManagerConfig::default();
    let manager = PeriodicTaskManager::new(scheduler, config, provider);

    assert!(!manager.is_done());
    manager.shutdown();
    assert!(manager.is_done());
  }
}
