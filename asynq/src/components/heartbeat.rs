//! Heartbeat 模块
//! Heartbeat module
//!
//! 对应 Go 版本的 heartbeater（heartbeat.go）职责：
//! Responsibilities corresponding to the Go version's heartbeater (heartbeat.go):
//! 周期性写入 ServerInfo 以续租 / 汇报活跃 worker 数，并在关闭时清理服务器状态。
//! Periodically writes ServerInfo to renew lease / report active worker count, and cleans up server state on shutdown。
//!
//! 此实现包含了 workers map 用于追踪当前节点正在处理的任务。
//! This implementation includes a workers map to track tasks being processed by the current node.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::base::{Broker, ServerState};
use crate::proto::{ServerInfo, TaskMessage, WorkerInfo};

/// 静态元数据（不随心跳周期变化）
/// Static metadata (does not change with heartbeat cycles)
#[derive(Debug, Clone)]
pub struct HeartbeatMeta {
  pub host: String,
  pub pid: i32,
  pub server_uuid: String,
  pub concurrency: usize,
  pub queues: HashMap<String, i32>,
  pub strict_priority: bool,
  pub started: SystemTime,
  /// ACL 租户名称（用于多租户隔离）
  /// ACL tenant name (for multi-tenant isolation)
  pub acl_tenant: Option<String>,
}

impl From<(&HeartbeatMeta, i32)> for ServerInfo {
  fn from(value: (&HeartbeatMeta, i32)) -> Self {
    let (meta, active_worker_count) = value;
    ServerInfo {
      host: meta.host.clone(),
      pid: meta.pid,
      server_id: meta.server_uuid.clone(),
      concurrency: meta.concurrency as i32,
      queues: meta.queues.clone(),
      strict_priority: meta.strict_priority,
      status: ServerState::Active.as_str().to_string(),
      start_time: Some(prost_types::Timestamp::from(meta.started)),
      active_worker_count,
    }
  }
}

/// 活跃 worker 信息（对应 Go 版本的 workerInfo）
/// Active worker information (corresponds to Go version's workerInfo)
///
/// 包含正在处理任务的 worker 的详细信息
/// Contains detailed information about a worker processing a task
#[derive(Clone)]
pub struct WorkerInfoEntry {
  /// 正在处理的任务消息
  /// The task message the worker is processing
  pub msg: TaskMessage,
  /// worker 开始处理任务的时间
  /// The time the worker started processing the task
  pub started: SystemTime,
  /// worker 必须完成任务的截止时间
  /// Deadline by which the worker must finish processing the task
  pub deadline: SystemTime,
}

/// Worker 事件枚举（统一的事件类型）
/// Worker event enum (unified event type)
///
/// 用于通过单一通道传递 worker 启动和完成事件
/// Used to pass worker starting and finished events through a single channel
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum WorkerEvent {
  /// Worker 开始处理任务事件
  /// Worker started processing task event
  Started(WorkerInfoEntry),
  /// Worker 完成任务事件（包含任务 ID）
  /// Worker finished task event (contains task ID)
  Finished(String),
}

/// 心跳器，封装心跳循环上下文
/// Heartbeat, encapsulates heartbeat loop context
///
/// 对应 Go 版本的 heartbeater 结构体，包含 workers map 用于追踪正在处理的任务
/// Corresponds to Go version's heartbeater struct, includes workers map to track tasks being processed
pub struct Heartbeat {
  broker: Arc<dyn Broker>,
  interval: Duration,
  meta: HeartbeatMeta,
  /// 当前正在处理的任务 workers（对应 Go 的 workers map[string]*workerInfo）
  /// Currently processing task workers (corresponds to Go's workers map[string]*workerInfo)
  workers: RwLock<HashMap<String, WorkerInfoEntry>>,
  shutting_down: Arc<AtomicBool>,
  /// 接收 worker 事件的通道（统一接收启动和完成事件）
  /// Channel to receive worker events (unified for starting and finished events)
  event_rx: Mutex<Option<mpsc::Receiver<WorkerEvent>>>,
}

/// Worker 事件发送器（用于 Processor 发送事件给 Heartbeat）
/// Worker event sender (for Processor to send events to Heartbeat)
///
/// 此结构实现了 Clone，支持多生产者模式
/// This struct implements Clone, supporting multi-producer pattern
#[derive(Clone)]
pub struct WorkerEventSender {
  /// 发送 worker 事件的通道（支持多生产者）
  /// Channel to send worker events (supports multi-producer)
  event_tx: mpsc::Sender<WorkerEvent>,
}

impl WorkerEventSender {
  /// 发送 worker 开始事件
  /// Send worker started event
  pub async fn send_started(
    &self,
    entry: WorkerInfoEntry,
  ) -> Result<(), mpsc::error::SendError<WorkerEvent>> {
    self.event_tx.send(WorkerEvent::Started(entry)).await
  }

  /// 发送 worker 完成事件
  /// Send worker finished event
  pub async fn send_finished(
    &self,
    task_id: String,
  ) -> Result<(), mpsc::error::SendError<WorkerEvent>> {
    self.event_tx.send(WorkerEvent::Finished(task_id)).await
  }
}

impl Heartbeat {
  /// 创建新的心跳器
  /// Create a new heartbeat
  ///
  /// 返回 (Heartbeat, WorkerEventSender)，其中 WorkerEventSender 用于 Processor 发送事件
  /// Returns (Heartbeat, WorkerEventSender), where WorkerEventSender is used by Processor to send events
  ///
  /// WorkerEventSender 实现了 Clone，支持多生产者模式
  /// WorkerEventSender implements Clone, supporting multi-producer pattern
  pub fn new(
    broker: Arc<dyn Broker>,
    interval: Duration,
    meta: HeartbeatMeta,
  ) -> (Self, WorkerEventSender) {
    // 创建统一的 worker 事件通道
    // Create unified worker event channel
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>(100);

    let heartbeat = Self {
      broker,
      interval,
      meta,
      workers: Default::default(),
      shutting_down: Arc::new(AtomicBool::new(false)),
      event_rx: Mutex::new(Some(event_rx)),
    };

    let sender = WorkerEventSender { event_tx };

    (heartbeat, sender)
  }

  /// 启动心跳循环
  /// Start the heartbeat loop
  ///
  /// 对应 Go 的 heartbeater.start()，同时处理心跳和 worker 事件
  /// Corresponds to Go's heartbeater.start(), handles both heartbeat and worker events
  ///
  /// 如果 Heartbeat 已经启动过，返回一个立即完成的空任务
  /// If Heartbeat has already been started, returns an immediately completing empty task
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tracing::info!("starting heartbeat");

    let this = self;
    tokio::spawn(async move {
      // 取出 receiver（使用 Mutex 安全地获取）
      // Take out receiver (safely obtain using Mutex)
      let mut event_rx = {
        let mut guard = this.event_rx.lock().await;
        match guard.take() {
          Some(rx) => rx,
          None => {
            tracing::warn!("Heartbeat already started, skipping duplicate start");
            return;
          }
        }
      };

      let mut ticker = tokio::time::interval(this.interval);
      loop {
        tokio::select! {
          // 定时心跳
          // Periodic heartbeat
          _ = ticker.tick() => {
            if this.shutting_down.load(Ordering::Relaxed) {
              break;
            }
            this.beat().await;
          }
          // 接收 worker 事件（统一处理启动和完成事件）
          // Receive worker event (unified handling for starting and finished events)
          Some(event) = event_rx.recv() => {
            match event {
              WorkerEvent::Started(worker_info) => {
                let task_id = worker_info.msg.id.clone();
                this.workers.write().await.insert(task_id.clone(), worker_info);
                tracing::debug!("Worker started: task_id={}", task_id);
              }
              WorkerEvent::Finished(task_id) => {
                this.workers.write().await.remove(&task_id);
                tracing::debug!("Worker finished: task_id={}", task_id);
              }
            }
          }
        }
      }
      // 退出时清理服务器状态（冗余安全清理）
      // Clean up server state on exit (redundant safe cleanup)
      let _ = this
        .broker
        .clear_server_state(
          &this.meta.host,
          this.meta.pid,
          &this.meta.server_uuid,
          this.meta.acl_tenant.as_deref(),
        )
        .await;
    })
  }

  /// 执行心跳（对应 Go 的 heartbeater.beat()）
  /// Perform heartbeat (corresponds to Go's heartbeater.beat())
  ///
  /// 收集当前 worker 信息并写入 Redis
  /// Collect current worker info and write to Redis
  async fn beat(&self) {
    let workers = self.workers.read().await;

    // 构建 WorkerInfo 列表（对应 Go 的 ws []*base.WorkerInfo）
    // Build WorkerInfo list (corresponds to Go's ws []*base.WorkerInfo)
    let worker_infos: Vec<WorkerInfo> = workers
      .values()
      .map(|w| WorkerInfo {
        host: self.meta.host.clone(),
        pid: self.meta.pid,
        server_id: self.meta.server_uuid.clone(),
        task_id: w.msg.id.clone(),
        task_type: w.msg.r#type.clone(),
        task_payload: w.msg.payload.clone(),
        queue: w.msg.queue.clone(),
        start_time: Some(prost_types::Timestamp::from(w.started)),
        deadline: Some(prost_types::Timestamp::from(w.deadline)),
      })
      .collect();

    let active_worker_count = workers.len() as i32;
    drop(workers); // 释放读锁 / Release read lock

    let info: ServerInfo = (&self.meta, active_worker_count).into();

    if let Err(e) = self
      .broker
      .write_server_state(
        &info,
        worker_infos,
        self.interval * 2,
        self.meta.acl_tenant.as_deref(),
      )
      .await
    {
      tracing::warn!("Heartbeat write failed: {}", e);
    }
  }

  /// 请求心跳循环终止
  /// Request the termination of the heartbeat loop
  pub fn shutdown(&self) {
    self.shutting_down.store(true, Ordering::Relaxed);
  }

  /// 检查是否已停止
  /// Check if it has stopped
  pub fn is_done(&self) -> bool {
    self.shutting_down.load(Ordering::Relaxed)
  }

  /// 获取当前活跃 worker 数量
  /// Get current active worker count
  pub async fn active_worker_count(&self) -> usize {
    self.workers.read().await.len()
  }
}

impl crate::components::ComponentLifecycle for Heartbeat {
  fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
    Heartbeat::start(self)
  }

  fn shutdown(&self) {
    Heartbeat::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Heartbeat::is_done(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn create_test_task_message(id: &str) -> TaskMessage {
    TaskMessage {
      id: id.to_string(),
      r#type: "test:task".to_string(),
      payload: b"test payload".to_vec(),
      queue: "default".to_string(),
      retry: 3,
      retried: 0,
      timeout: 3600,
      deadline: 0,
      ..Default::default()
    }
  }

  #[test]
  fn test_worker_info_entry_creation() {
    let msg = create_test_task_message("task-1");
    let started = SystemTime::now();
    let deadline = started + Duration::from_secs(3600);

    let entry = WorkerInfoEntry {
      msg: msg.clone(),
      started,
      deadline,
    };

    assert_eq!(entry.msg.id, "task-1");
    assert_eq!(entry.msg.r#type, "test:task");
    assert_eq!(entry.msg.queue, "default");
  }

  #[test]
  fn test_heartbeat_meta_creation() {
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 1);
    queues.insert("critical".to_string(), 6);

    let meta = HeartbeatMeta {
      host: "localhost".to_string(),
      pid: 1234,
      server_uuid: "test-uuid".to_string(),
      concurrency: 10,
      queues,
      strict_priority: false,
      started: SystemTime::now(),
      acl_tenant: None,
    };

    assert_eq!(meta.host, "localhost");
    assert_eq!(meta.pid, 1234);
    assert_eq!(meta.concurrency, 10);
    assert!(!meta.strict_priority);
    assert_eq!(meta.queues.len(), 2);
  }

  #[test]
  fn test_server_info_from_heartbeat_meta() {
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 1);

    let meta = HeartbeatMeta {
      host: "test-host".to_string(),
      pid: 5678,
      server_uuid: "server-uuid".to_string(),
      concurrency: 5,
      queues,
      strict_priority: true,
      started: SystemTime::now(),
      acl_tenant: None,
    };

    let active_worker_count = 3;
    let server_info: ServerInfo = (&meta, active_worker_count).into();

    assert_eq!(server_info.host, "test-host");
    assert_eq!(server_info.pid, 5678);
    assert_eq!(server_info.server_id, "server-uuid");
    assert_eq!(server_info.concurrency, 5);
    assert!(server_info.strict_priority);
    assert_eq!(server_info.active_worker_count, 3);
    assert_eq!(server_info.status, "active");
  }

  #[tokio::test]
  async fn test_worker_event_sender() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<WorkerEvent>(10);
    let sender = WorkerEventSender { event_tx };

    // Test sending a worker starting event
    let msg = create_test_task_message("task-123");
    let worker_info = WorkerInfoEntry {
      msg: msg.clone(),
      started: SystemTime::now(),
      deadline: SystemTime::now() + Duration::from_secs(3600),
    };
    sender.send_started(worker_info).await.unwrap();

    // Receive and verify
    let received = event_rx.recv().await.unwrap();
    match received {
      WorkerEvent::Started(entry) => {
        assert_eq!(entry.msg.id, "task-123");
      }
      _ => panic!("Expected Started event"),
    }

    // Test sending a worker finished event
    sender.send_finished("task-456".to_string()).await.unwrap();

    // Receive and verify
    let finished = event_rx.recv().await.unwrap();
    match finished {
      WorkerEvent::Finished(task_id) => {
        assert_eq!(task_id, "task-456");
      }
      _ => panic!("Expected Finished event"),
    }
  }

  #[tokio::test]
  async fn test_worker_event_sender_clone() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<WorkerEvent>(10);
    let sender1 = WorkerEventSender { event_tx };
    let sender2 = sender1.clone();

    // Both senders should work
    sender1.send_finished("task-1".to_string()).await.unwrap();
    sender2.send_finished("task-2".to_string()).await.unwrap();

    // Receive both events
    let event1 = event_rx.recv().await.unwrap();
    let event2 = event_rx.recv().await.unwrap();

    match (event1, event2) {
      (WorkerEvent::Finished(id1), WorkerEvent::Finished(id2)) => {
        assert_eq!(id1, "task-1");
        assert_eq!(id2, "task-2");
      }
      _ => panic!("Expected two Finished events"),
    }
  }
}
