//! Heartbeat 模块
//! Heartbeat module
//!
//! 对应 Go 版本的 heartbeater（heartbeat.go）职责：
//! Responsibilities corresponding to the Go version's heartbeater (heartbeat.go):
//! 周期性写入 ServerInfo 以续租 / 汇报活跃 worker 数，并在关闭时清理服务器状态。
//! Periodically writes ServerInfo to renew lease / report active worker count, and cleans up server state on shutdown。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task::JoinHandle;

use crate::base::{Broker, ServerState};
use crate::proto::ServerInfo;

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

/// 心跳器，封装心跳循环上下文
/// Heartbeat, encapsulates heartbeat loop context
pub struct Heartbeat {
  broker: Arc<dyn Broker>,
  interval: Duration,
  meta: HeartbeatMeta,
  active_workers: Arc<AtomicUsize>,
  shutting_down: Arc<AtomicBool>,
}

impl Heartbeat {
  pub fn new(
    broker: Arc<dyn Broker>,
    interval: Duration,
    meta: HeartbeatMeta,
    active_workers: Arc<AtomicUsize>,
  ) -> Self {
    Self {
      broker,
      interval,
      meta,
      active_workers,
      shutting_down: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 启动心跳循环
  /// Start the heartbeat loop
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut ticker = tokio::time::interval(self.interval);
      loop {
        ticker.tick().await;
        if self.shutting_down.load(Ordering::Relaxed) {
          break;
        }
        let info: ServerInfo = (
          &self.meta,
          self.active_workers.load(Ordering::Relaxed) as i32,
        )
          .into();
        if let Err(e) = self
          .broker
          .write_server_state(&info, self.interval * 2)
          .await
        {
          tracing::warn!("Heartbeat write failed: {}", e);
        }
      }
      // 退出时清理服务器状态（冗余安全清理）
      // Clean up server state on exit (redundant safe cleanup)
      let _ = self
        .broker
        .clear_server_state(&self.meta.host, self.meta.pid, &self.meta.server_uuid)
        .await;
    })
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
