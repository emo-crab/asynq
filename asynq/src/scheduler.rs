//! # 周期性任务调度器（Scheduler）
//! # Periodic Task Scheduler (Scheduler)
//!
//! 该模块实现了类似 Go 版 asynq 的周期性任务调度功能。
//! This module implements periodic task scheduling similar to the Go version of asynq.
//! 主要参考 Go 版 asynq/scheduler.go、asynq/periodic_task.go。
//! Mainly refers to Go asynq/scheduler.go and asynq/periodic_task.go.
//!
//! ## Go 版主要数据结构与方法对照：
//! ## Main data structures and methods comparison with Go version:
//!
//! Go:
//!   type Scheduler struct {
//!       mu sync.Mutex
//!       entries `map[string]*Entry`
//!       running bool
//!       ...
//!   }
//!   type Entry struct {
//!       ID string
//!       Spec string // cron 表达式
//!       TaskType string
//!       Payload []byte
//!       Queue string
//!       Next time.Time
//!       ...
//!   }
//!   func (s *Scheduler) Register(spec, taskType string, payload []byte, queue string) error
//!   func (s *Scheduler) Start()
//!   func (s *Scheduler) Stop()
//!   func (s *Scheduler) Remove(id string)
//!   func (s *Scheduler) Entries() []*Entry
//!
//! Rust:
//!   struct Scheduler { ... }
//!   struct PeriodicTask { ... }
//!   impl Scheduler {
//!       pub fn add_task(&self, task: PeriodicTask)
//!       pub fn remove_task(&self, name: &str)
//!       pub fn list_tasks(&self) -> `Vec<String>`
//!       pub fn start(&mut self)
//!       pub fn stop(&mut self)
//!   }
//!
//! 目前未实现的 Go 版功能：
//! Unimplemented Go features:
//!   - Entry ID（Rust 以 name 作为唯一标识）
//!     - Entry ID (Rust uses name as unique identifier)
//!   - 任务持久化与恢复
//!     - Task persistence and recovery
//!   - 任务去重与唯一性
//!     - Task deduplication and uniqueness
//!   - 运行中动态修改任务（可通过 add/remove 实现）
//!     - Dynamic modification of tasks at runtime (can be done via add/remove)
//!   - 任务执行日志与错误处理
//!     - Task execution logging and error handling
//!
//! 如需补充上述功能，可参考 Go 版实现进一步扩展。
//! For additional features, refer to the Go implementation for further extension。

use crate::backend::option::{OptionType, TaskOptions};
use crate::client::Client;
use crate::proto::{SchedulerEnqueueEvent, SchedulerEntry};
use crate::task::Task;
use chrono::{DateTime, Utc};
use cron::Schedule;
use prost::Message;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Represents a periodic task to be scheduled.
#[derive(Debug, Clone)]
pub struct PeriodicTask {
  /// 任务名称，作为唯一标识
  pub name: String,
  /// cron 表达式
  pub cron: String,
  /// 任务负载
  pub payload: Vec<u8>,
  /// 任务队列
  pub queue: String,
  /// 任务选项
  pub options: TaskOptions,
  /// cron 调度对象
  pub schedule: Schedule,
  /// 下次执行时间
  pub next_tick: Option<DateTime<Utc>>,
}

impl PeriodicTask {
  /// 创建一个新的 PeriodicTask 实例
  pub fn new(name: String, cron: String, payload: Vec<u8>, queue: String) -> anyhow::Result<Self> {
    let schedule = Schedule::from_str(&cron)?;
    let next_tick = schedule.upcoming(Utc).next();
    let options = TaskOptions {
      queue: queue.clone(),
      ..Default::default()
    };
    Ok(Self {
      name,
      cron,
      payload,
      queue,
      options,
      schedule,
      next_tick,
    })
  }

  /// 创建一个带选项的 PeriodicTask 实例
  pub fn new_with_options(
    name: String,
    cron: String,
    payload: Vec<u8>,
    options: TaskOptions,
  ) -> anyhow::Result<Self> {
    let schedule = Schedule::from_str(&cron)?;
    let next_tick = schedule.upcoming(Utc).next();
    let queue = options.queue.clone();
    Ok(Self {
      name,
      cron,
      payload,
      queue,
      options,
      schedule,
      next_tick,
    })
  }
}

/// Type alias for scheduler task handles (main scheduling loop, heartbeat loop)
type SchedulerHandles = Arc<tokio::sync::Mutex<Option<(JoinHandle<()>, JoinHandle<()>)>>>;

/// 周期性任务调度器
pub struct Scheduler {
  client: Arc<Client>,
  /// 调度器唯一 id
  id: String,
  /// 存储所有任务的哈希表（entry_id -> PeriodicTask）
  tasks: Arc<RwLock<HashMap<String, PeriodicTask>>>,
  /// 运行状态标志
  running: Arc<AtomicBool>,
  /// 通知机制，用于唤醒调度器
  notify: Arc<Notify>,
  /// 后台运行的任务句柄
  handles: SchedulerHandles,
  /// 心跳间隔
  heartbeat_interval: Duration,
}

impl Scheduler {
  /// 初始化 Scheduler，自动生成 id，设置心跳间隔
  pub async fn new(
    client: Arc<Client>,
    heartbeat_interval: Option<Duration>,
  ) -> anyhow::Result<Self> {
    let id = format!(
      "{}:{}:{}",
      hostname::get().unwrap_or_default().to_string_lossy(),
      std::process::id(),
      Uuid::new_v4()
    );
    Ok(Self {
      client,
      id,
      tasks: Arc::new(RwLock::new(HashMap::new())),
      running: Arc::new(AtomicBool::new(false)),
      notify: Arc::new(Notify::new()),
      handles: Arc::new(tokio::sync::Mutex::new(None)),
      heartbeat_interval: heartbeat_interval.unwrap_or(Duration::from_secs(10)),
    })
  }

  /// 添加一个周期性任务，并通过 rdb 写入 SchedulerEntry，兼容 Go 版 asynq
  /// 注册一个周期性任务，只添加到本地，不写入 redis，兼容 Go 版 Register
  pub async fn register(&self, mut task: PeriodicTask, queue: &str) -> anyhow::Result<String> {
    let entry_id = Uuid::new_v4().to_string();
    task.queue = queue.to_string();
    task.options.queue = queue.to_string();
    let schedule = Schedule::from_str(&task.cron)?;
    task.schedule = schedule;
    task.next_tick = task.schedule.upcoming(Utc).next();
    let mut guard = self
      .tasks
      .write()
      .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
    guard.insert(entry_id.clone(), task);
    drop(guard);
    self.notify.notify_one();
    Ok(entry_id)
  }

  /// 注销/移除一个周期性任务（按 entry_id），只移除本地，不直接清理 redis
  pub async fn unregister(&self, entry_id: &str) -> anyhow::Result<()> {
    let mut guard = self
      .tasks
      .write()
      .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
    guard.remove(entry_id);
    drop(guard);
    self.notify.notify_one();
    Ok(())
  }

  /// 列出所有任务的名称
  pub fn list_tasks(&self) -> Vec<String> {
    match self.tasks.read() {
      Ok(tasks) => tasks.keys().cloned().collect(),
      Err(_) => vec![],
    }
  }

  /// 启动调度器，主循环和心跳分离
  /// 这个方法现在是 pub(crate)，只能被 PeriodicTaskManager 调用
  /// 在测试环境中也可以直接调用
  #[cfg_attr(not(test), doc(hidden))]
  pub async fn start(&self) {
    // Prevent double start
    if self.running.swap(true, Ordering::SeqCst) {
      return;
    }

    let tasks = self.tasks.clone();
    let running = self.running.clone();
    let notify = self.notify.clone();
    let client = self.client.clone();
    let heartbeat_interval = self.heartbeat_interval;

    let main_handle = tokio::spawn(async move {
      loop {
        if !running.load(Ordering::Relaxed) {
          break;
        }
        let now = Utc::now();
        let mut min_next: Option<DateTime<Utc>> = None;
        let mut due_entries = Vec::new();
        {
          if let Ok(mut tasks) = tasks.write() {
            for (entry_id, task) in tasks.iter_mut() {
              let next_tick = task.next_tick;
              if let Some(next) = next_tick {
                if next <= now {
                  due_entries.push((
                    entry_id.clone(),
                    task.name.clone(),
                    task.payload.clone(),
                    task.options.clone(),
                  ));
                  task.next_tick = task.schedule.upcoming(Utc).next();
                }
                if min_next.is_none() || min_next.map(|m| next < m).unwrap_or(false) {
                  min_next = Some(next);
                }
              }
            }
          }
        }
        // 推送到期任务，并通过 broker 记录调度事件
        for (entry_id, name, payload, options) in due_entries {
          if let Ok(mut t) = Task::new(&name, &payload) {
            t.options = options;
            let _ = client.enqueue(t).await;
            // 记录调度事件
            let event = SchedulerEnqueueEvent {
              task_id: name.clone(),
              enqueue_time: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
              }),
            };

            // Record using unified SchedulerBroker interface
            let broker = client.get_scheduler_broker();
            let entry_id = entry_id.clone();
            let event_clone = event.clone();
            tokio::spawn(async move {
              let _ = broker
                .record_scheduler_enqueue_event(&event_clone, &entry_id)
                .await;
            });
          }
        }
        let sleep_dur = min_next
          .map(|t| (t - now).to_std().unwrap_or(Duration::from_secs(1)))
          .unwrap_or(heartbeat_interval);
        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => {},
            _ = notify.notified() => {},
        }
      }
    });

    // 启动心跳循环
    let heartbeat_handle = self.spawn_heartbeat();

    // Store handles
    let mut handles_guard = self.handles.lock().await;
    *handles_guard = Some((main_handle, heartbeat_handle));
  }

  /// 心跳循环，定期写入所有 entry 到后端（Redis 或 PostgreSQL）
  fn spawn_heartbeat(&self) -> JoinHandle<()> {
    let tasks = self.tasks.clone();
    let running = self.running.clone();
    let client = self.client.clone();
    let scheduler_id = self.id.clone();
    let heartbeat_interval = self.heartbeat_interval;
    tokio::spawn(async move {
      // Get unified SchedulerBroker interface
      let scheduler_broker = client.get_scheduler_broker();

      let mut ticker = tokio::time::interval(heartbeat_interval);
      loop {
        tokio::select! {
          _ = ticker.tick() => {
            if !running.load(Ordering::Relaxed) {
              break;
            }
            // 快照写入后端
            let mut all_entries = Vec::new();
            {
              if let Ok(tasks) = tasks.read() {
                for (entry_id, task) in tasks.iter() {
                  let next_tick = task.next_tick;
                  let entry = SchedulerEntry {
                    id: entry_id.clone(),
                    spec: task.cron.clone(),
                    task_type: task.name.clone(),
                    task_payload: task.payload.clone(),
                    enqueue_options: Self::stringify_options(&task.options),
                    next_enqueue_time: next_tick.map(|t| prost_types::Timestamp {
                      seconds: t.timestamp(),
                      nanos: t.timestamp_subsec_nanos() as i32,
                    }),
                    prev_enqueue_time: None,
                  };
                  all_entries.push(entry);
                }
              }
            }

            // Write using unified SchedulerBroker interface
            let _ = scheduler_broker.write_scheduler_entries(&all_entries, &scheduler_id, (heartbeat_interval * 2).as_secs()).await;
          }
          _ = async {
            while running.load(Ordering::Relaxed) {
              tokio::time::sleep(heartbeat_interval).await;
            }
          } => {
            // 清理后端
            let _ = scheduler_broker.clear_scheduler_entries(&scheduler_id).await;
            break;
          }
        }
      }
    })
  }

  /// 查询所有注册的 SchedulerEntry，兼容 Go 版 asynq Inspector
  pub async fn list_entries(&self, scheduler_id: &str) -> Vec<SchedulerEntry> {
    let scheduler_broker = self.client.get_scheduler_broker();
    let raw_map = scheduler_broker
      .scheduler_entries_script(scheduler_id)
      .await
      .unwrap_or_default();
    let mut entries = Vec::new();
    for (_id, bytes) in raw_map {
      if let Ok(entry) = SchedulerEntry::decode(&*bytes) {
        entries.push(entry);
      }
    }
    entries
  }

  /// 查询调度历史事件，兼容 Go 版 asynq Inspector
  pub async fn list_events(&self, count: usize) -> Vec<SchedulerEnqueueEvent> {
    let scheduler_broker = self.client.get_scheduler_broker();
    let raw_list = scheduler_broker
      .scheduler_events_script(count)
      .await
      .unwrap_or_default();
    let mut events = Vec::new();
    for bytes in raw_list {
      if let Ok(event) = SchedulerEnqueueEvent::decode(&*bytes) {
        events.push(event);
      }
    }
    events
  }

  /// 停止调度器并清理后端 entry
  /// 这个方法现在是 pub(crate)，只能被 PeriodicTaskManager 调用
  /// 在测试环境中也可以直接调用
  #[cfg_attr(not(test), doc(hidden))]
  pub async fn stop(&self) {
    self.running.store(false, Ordering::SeqCst);
    self.notify.notify_one();

    // Take the handles and wait for them
    let handles = {
      let mut handles_guard = self.handles.lock().await;
      handles_guard.take()
    };

    if let Some((main_handle, heartbeat_handle)) = handles {
      let _ = main_handle.await;
      let _ = heartbeat_handle.await;
    }

    // 清理后端中的 entry
    let scheduler_broker = self.client.get_scheduler_broker();
    let _ = scheduler_broker.clear_scheduler_entries(&self.id).await;
  }

  /// 生成与 Go 版一致的调度选项字符串
  /// Generate option strings compatible with Go asynq scheduler
  pub fn stringify_options(opts: &TaskOptions) -> Vec<String> {
    let option_types: Vec<OptionType> = opts.into();
    option_types.iter().map(|opt| opt.to_string()).collect()
  }

  /// 解析选项字符串为 TaskOptions
  /// Parse option strings to TaskOptions
  pub fn parse_options(opts: &[String]) -> TaskOptions {
    let mut option_types = Vec::new();
    for opt_str in opts {
      if let Ok(opt) = OptionType::parse(opt_str) {
        option_types.push(opt);
      }
    }
    option_types.into()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Duration;

  #[test]
  fn test_stringify_options() {
    let mut opts = TaskOptions {
      queue: "critical".to_string(),
      ..Default::default()
    };
    opts.max_retry = 5;
    opts.timeout = Some(Duration::from_secs(60));
    opts.retention = Some(Duration::from_secs(3600));

    let strings = Scheduler::stringify_options(&opts);

    // Check that key options are in the strings
    assert!(strings.iter().any(|s| s.contains("Queue(\"critical\")")));
    assert!(strings.iter().any(|s| s.contains("MaxRetry(5)")));
    assert!(strings.iter().any(|s| s.contains("Timeout(60)")));
    assert!(strings.iter().any(|s| s.contains("Retention(3600)")));
  }

  #[test]
  fn test_parse_options() {
    let option_strings = vec![
      "Queue(\"critical\")".to_string(),
      "MaxRetry(5)".to_string(),
      "Timeout(60)".to_string(),
      "Retention(3600)".to_string(),
    ];

    let opts = Scheduler::parse_options(&option_strings);

    assert_eq!(opts.queue, "critical");
    assert_eq!(opts.max_retry, 5);
    assert_eq!(opts.timeout, Some(Duration::from_secs(60)));
    assert_eq!(opts.retention, Some(Duration::from_secs(3600)));
  }

  #[test]
  fn test_options_roundtrip() {
    let mut original_opts = TaskOptions {
      queue: "high_priority".to_string(),
      ..Default::default()
    };
    original_opts.queue = "high_priority".to_string();
    original_opts.max_retry = 10;
    original_opts.timeout = Some(Duration::from_secs(120));
    original_opts.retention = Some(Duration::from_secs(7200));
    original_opts.task_id = Some("task-abc-123".to_string());

    // Stringify
    let strings = Scheduler::stringify_options(&original_opts);

    // Parse back
    let parsed_opts = Scheduler::parse_options(&strings);

    // Verify
    assert_eq!(parsed_opts.queue, original_opts.queue);
    assert_eq!(parsed_opts.max_retry, original_opts.max_retry);
    assert_eq!(parsed_opts.timeout, original_opts.timeout);
    assert_eq!(parsed_opts.retention, original_opts.retention);
    assert_eq!(parsed_opts.task_id, original_opts.task_id);
  }

  #[test]
  fn test_periodic_task_creation_with_options() {
    let mut opts = TaskOptions {
      queue: "scheduled".to_string(),
      ..Default::default()
    };
    opts.max_retry = 3;
    opts.timeout = Some(Duration::from_secs(30));

    let task = PeriodicTask::new_with_options(
      "email:daily".to_string(),
      "0 0 0 * * *".to_string(), // Daily at midnight (sec min hour day month dow)
      b"daily email payload".to_vec(),
      opts.clone(),
    )
    .unwrap();

    assert_eq!(task.name, "email:daily");
    assert_eq!(task.queue, "scheduled");
    assert_eq!(task.options.queue, "scheduled");
    assert_eq!(task.options.max_retry, 3);
    assert_eq!(task.options.timeout, Some(Duration::from_secs(30)));
  }

  #[test]
  fn test_periodic_task_default_creation() {
    let task = PeriodicTask::new(
      "test:task".to_string(),
      "0 */5 * * * *".to_string(), // Every 5 minutes (sec min hour day month dow)
      b"test payload".to_vec(),
      "default".to_string(),
    )
    .unwrap();

    assert_eq!(task.name, "test:task");
    assert_eq!(task.queue, "default");
    assert_eq!(task.options.queue, "default");
  }
}
