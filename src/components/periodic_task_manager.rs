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
//! 注意：此模块是 scheduler.rs 的补充，提供更细粒度的周期性任务管理功能
//! Note: This module complements scheduler.rs by providing more fine-grained periodic task management

use crate::client::Client;
use crate::error::Result;
use crate::components::ComponentLifecycle;
use crate::scheduler::PeriodicTask;
use crate::task::Task;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// 周期性任务条目
/// Periodic task entry
#[derive(Debug, Clone)]
pub struct PeriodicTaskEntry {
  /// 条目 ID
  /// Entry ID
  pub id: String,
  /// 任务名称
  /// Task name
  pub task_name: String,
  /// 任务载荷
  /// Task payload
  pub payload: Vec<u8>,
  /// Cron 表达式
  /// Cron expression
  pub cron_spec: String,
  /// Cron 调度器
  /// Cron schedule
  pub schedule: Schedule,
  /// 队列名称
  /// Queue name
  pub queue: String,
  /// 下次执行时间
  /// Next execution time
  pub next_run: Option<DateTime<Utc>>,
  /// 最后执行时间
  /// Last execution time
  pub last_run: Option<DateTime<Utc>>,
}

impl PeriodicTaskEntry {
  /// 从 PeriodicTask 创建
  /// Create from PeriodicTask
  pub fn from_periodic_task(task: &PeriodicTask, entry_id: String) -> Result<Self> {
    let schedule = Schedule::from_str(&task.cron)
      .map_err(|e| crate::error::Error::other(format!("Invalid cron expression: {}", e)))?;
    let next_run = schedule.upcoming(Utc).next();

    Ok(Self {
      id: entry_id,
      task_name: task.name.clone(),
      payload: task.payload.clone(),
      cron_spec: task.cron.clone(),
      schedule,
      queue: task.queue.clone(),
      next_run,
      last_run: None,
    })
  }

  /// 计算下次执行时间
  /// Calculate next run time
  pub fn calculate_next_run(&mut self) {
    self.next_run = self.schedule.upcoming(Utc).next();
  }
}

/// Periodic Task Manager 配置
/// Periodic Task Manager configuration
#[derive(Debug, Clone)]
pub struct PeriodicTaskManagerConfig {
  /// 检查间隔
  /// Check interval
  pub check_interval: std::time::Duration,
}

impl Default for PeriodicTaskManagerConfig {
  fn default() -> Self {
    Self {
      check_interval: std::time::Duration::from_secs(60),
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
/// 1. 注册和注销周期性任务
/// 1. Register and unregister periodic tasks
/// 2. 根据 cron 表达式调度任务
/// 2. Schedule tasks according to cron expressions
/// 3. 在到期时自动将任务加入队列
/// 3. Automatically enqueue tasks when due
pub struct PeriodicTaskManager {
  client: Arc<Client>,
  config: PeriodicTaskManagerConfig,
  entries: Arc<Mutex<HashMap<String, PeriodicTaskEntry>>>,
  done: Arc<AtomicBool>,
}

impl PeriodicTaskManager {
  /// 创建新的 Periodic Task Manager
  /// Create a new Periodic Task Manager
  pub fn new(client: Arc<Client>, config: PeriodicTaskManagerConfig) -> Self {
    Self {
      client,
      config,
      entries: Arc::new(Mutex::new(HashMap::new())),
      done: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 注册周期性任务
  /// Register a periodic task
  ///
  /// 对应 Go 的 manager.Register()
  /// Corresponds to Go's manager.Register()
  pub async fn register(&self, task: PeriodicTask) -> Result<String> {
    let entry_id = Uuid::new_v4().to_string();
    let entry = PeriodicTaskEntry::from_periodic_task(&task, entry_id.clone())?;

    let mut entries = self.entries.lock().await;
    entries.insert(entry_id.clone(), entry);

    tracing::info!(
      "PeriodicTaskManager: registered task {} with cron spec '{}'",
      entry_id,
      task.cron
    );

    Ok(entry_id)
  }

  /// 注销周期性任务
  /// Unregister a periodic task
  ///
  /// 对应 Go 的 manager.Unregister()
  /// Corresponds to Go's manager.Unregister()
  pub async fn unregister(&self, entry_id: &str) -> Result<()> {
    let mut entries = self.entries.lock().await;
    if entries.remove(entry_id).is_some() {
      tracing::info!("PeriodicTaskManager: unregistered task {}", entry_id);
      Ok(())
    } else {
      Err(crate::error::Error::other(format!(
        "Task entry {} not found",
        entry_id
      )))
    }
  }

  /// 列出所有任务条目
  /// List all task entries
  pub async fn list_entries(&self) -> Vec<PeriodicTaskEntry> {
    let entries = self.entries.lock().await;
    entries.values().cloned().collect()
  }

  /// 启动 Periodic Task Manager
  /// Start the Periodic Task Manager
  ///
  /// 对应 Go 的 manager.Start()
  /// Corresponds to Go's manager.Start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.check_interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("PeriodicTaskManager: shutting down");
          break;
        }

        // 执行任务调度
        // Execute task scheduling
        if let Err(e) = self.schedule_tasks().await {
          tracing::error!("PeriodicTaskManager error: {}", e);
        }
      }
    })
  }

  /// 调度到期的任务
  /// Schedule due tasks
  async fn schedule_tasks(&self) -> Result<()> {
    let now = Utc::now();
    let mut entries = self.entries.lock().await;

    for (entry_id, entry) in entries.iter_mut() {
      // 检查是否到期
      // Check if due
      if let Some(next_run) = entry.next_run {
        if next_run <= now {
          // 创建任务
          // Create task
          let task = Task::new(&entry.task_name, &entry.payload)?;

          // 将任务加入队列
          // Enqueue task
          match self.client.enqueue(task).await {
            Ok(task_info) => {
              tracing::info!(
                "PeriodicTaskManager: enqueued task {} (name: {}, id: {})",
                entry_id,
                entry.task_name,
                task_info.id
              );
              entry.last_run = Some(now);
            }
            Err(e) => {
              tracing::error!(
                "PeriodicTaskManager: failed to enqueue task {}: {}",
                entry_id,
                e
              );
            }
          }

          // 计算下次执行时间
          // Calculate next run time
          entry.calculate_next_run();
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_periodic_task_manager_config_default() {
    let config = PeriodicTaskManagerConfig::default();
    assert_eq!(config.check_interval, std::time::Duration::from_secs(60));
  }

  #[tokio::test]
  #[ignore] // Requires Redis to be running
  async fn test_periodic_task_manager_register() {
    use crate::redis::RedisConfig;
    use crate::rdb::option::TaskOptions;

    let redis_config = RedisConfig::from_url("redis://localhost:6379").unwrap();
    let client = Arc::new(Client::new(redis_config).await.unwrap());
    let config = PeriodicTaskManagerConfig::default();
    let manager = PeriodicTaskManager::new(client, config);

    let options = TaskOptions { queue: "default".to_string(), ..Default::default() };
    let task = PeriodicTask {
      name: "test:task".to_string(),
      payload: b"test payload".to_vec(),
      cron: "* * * * *".to_string(),
      queue: "default".to_string(),
      options,
      schedule: Schedule::from_str("* * * * *").unwrap(),
      next_tick: None,
    };

    let entry_id = manager.register(task).await.unwrap();
    assert!(!entry_id.is_empty());

    let entries = manager.list_entries().await;
    assert_eq!(entries.len(), 1);

    manager.unregister(&entry_id).await.unwrap();
    let entries = manager.list_entries().await;
    assert_eq!(entries.len(), 0);
  }

  #[tokio::test]
  #[ignore] // Requires Redis to be running
  async fn test_periodic_task_manager_shutdown() {
    use crate::redis::RedisConfig;

    let redis_config = RedisConfig::from_url("redis://localhost:6379").unwrap();
    let client = Arc::new(Client::new(redis_config).await.unwrap());
    let config = PeriodicTaskManagerConfig::default();
    let manager = PeriodicTaskManager::new(client, config);

    assert!(!manager.is_done());
    manager.shutdown();
    assert!(manager.is_done());
  }
}
