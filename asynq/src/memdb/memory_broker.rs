//! 内存经纪人实现
//! Memory broker implementation
//!
//! 使用内存数据结构实现任务存储和管理，不依赖任何外部服务
//! Implements task storage and management using in-memory data structures without any external service dependencies

use crate::base::constants::DEFAULT_QUEUE_NAME;
use crate::error::{Error, Result};
use crate::proto::TaskMessage;
use crate::task::Task;
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

/// 内存中的任务数据
/// In-memory task data
#[derive(Clone)]
pub struct TaskData {
  /// 编码后的任务消息
  /// Encoded task message
  pub encoded: Vec<u8>,
  /// 任务消息
  /// Task message
  pub message: TaskMessage,
}

/// 有序集合中的项目（用于 scheduled, retry, active 等队列）
/// Item in sorted set (used for scheduled, retry, active queues, etc.)
#[derive(Clone, Debug)]
pub struct SortedSetItem {
  /// 任务 ID
  /// Task ID
  pub id: String,
  /// 分数（通常是时间戳）
  /// Score (usually a timestamp)
  pub score: i64,
}

/// 聚合组数据
/// Aggregation group data
#[derive(Default)]
pub struct AggregationGroup {
  /// 组中的任务 ID 和它们的分数
  /// Task IDs in the group and their scores
  pub tasks: BTreeMap<i64, Vec<String>>,
  /// 组创建时间
  /// Group creation time
  pub created_at: i64,
}

/// 聚合集合数据
/// Aggregation set data
#[derive(Default)]
pub struct AggregationSet {
  /// 集合中的任务 ID
  /// Task IDs in the set
  pub task_ids: HashSet<String>,
  /// 过期时间戳
  /// Expiration timestamp
  pub expires_at: i64,
}

/// 队列数据结构
/// Queue data structure
#[derive(Default)]
pub struct QueueData {
  /// 待处理任务 (pending) - 有序集合，按入队时间排序
  /// Pending tasks - sorted set, ordered by enqueue time
  pub pending: BTreeMap<i64, Vec<String>>,
  /// 活跃任务 (active) - 正在处理的任务
  /// Active tasks - tasks being processed
  pub active: HashSet<String>,
  /// 调度任务 (scheduled) - 按处理时间排序
  /// Scheduled tasks - sorted by process time
  pub scheduled: BTreeMap<i64, Vec<String>>,
  /// 重试任务 (retry) - 按重试时间排序
  /// Retry tasks - sorted by retry time
  pub retry: BTreeMap<i64, Vec<String>>,
  /// 已归档任务 (archived)
  /// Archived tasks
  pub archived: BTreeMap<i64, Vec<String>>,
  /// 已完成任务 (completed)
  /// Completed tasks
  pub completed: BTreeMap<i64, Vec<String>>,
  /// 任务租约过期时间
  /// Task lease expiration times
  pub lease: HashMap<String, i64>,
  /// 是否暂停
  /// Whether paused
  pub paused: bool,
  /// 聚合组
  /// Aggregation groups
  pub groups: HashMap<String, AggregationGroup>,
  /// 聚合集合
  /// Aggregation sets
  pub aggregation_sets: HashMap<String, AggregationSet>,
  /// 处理统计 - 按日期
  /// Processing statistics - by date
  pub processed_daily: HashMap<String, i64>,
  /// 失败统计 - 按日期
  /// Failure statistics - by date
  pub failed_daily: HashMap<String, i64>,
  /// 处理总数
  /// Total processed count
  pub processed_total: i64,
  /// 失败总数
  /// Total failed count
  pub failed_total: i64,
}

/// 服务器状态数据
/// Server state data
#[derive(Clone)]
pub struct ServerStateData {
  /// 编码后的服务器信息
  /// Encoded server info
  pub info: Vec<u8>,
  /// 过期时间戳
  /// Expiration timestamp
  pub expires_at: i64,
}

/// 内存存储
/// Memory storage
#[derive(Default)]
pub struct MemoryStorage {
  /// 所有队列名称
  /// All queue names
  pub queues: HashSet<String>,
  /// 每个队列的数据
  /// Data for each queue
  pub queue_data: HashMap<String, QueueData>,
  /// 任务数据 - key: queue:task_id
  /// Task data - key: queue:task_id
  pub tasks: HashMap<String, TaskData>,
  /// 唯一键映射 - key: unique_key, value: (task_id, expires_at)
  /// Unique key mapping - key: unique_key, value: (task_id, expires_at)
  pub unique_keys: HashMap<String, (String, i64)>,
  /// 服务器状态
  /// Server states
  pub servers: HashMap<String, ServerStateData>,
  /// 任务结果
  /// Task results
  pub results: HashMap<String, Vec<u8>>,
}

impl MemoryStorage {
  /// 获取或创建队列数据
  /// Get or create queue data
  pub fn get_or_create_queue(&mut self, queue: &str) -> &mut QueueData {
    self.queues.insert(queue.to_string());
    self.queue_data.entry(queue.to_string()).or_default()
  }

  /// 生成任务键
  /// Generate task key
  pub fn task_key(queue: &str, task_id: &str) -> String {
    format!("{queue}:{task_id}")
  }
}

/// 内存经纪人实现
/// Memory broker implementation
pub struct MemoryBroker {
  /// 内存存储
  /// Memory storage
  pub(crate) storage: Arc<RwLock<MemoryStorage>>,
  /// 取消频道发送器
  /// Cancellation channel sender
  pub(crate) cancel_tx: broadcast::Sender<String>,
}

impl Default for MemoryBroker {
  fn default() -> Self {
    Self::new()
  }
}

impl MemoryBroker {
  /// 创建新的内存经纪人实例
  /// Create a new memory broker instance
  pub fn new() -> Self {
    let (cancel_tx, _) = broadcast::channel(1024);
    Self {
      storage: Arc::new(RwLock::new(MemoryStorage::default())),
      cancel_tx,
    }
  }

  /// 获取存储的引用
  /// Get storage reference
  pub fn storage(&self) -> &Arc<RwLock<MemoryStorage>> {
    &self.storage
  }

  /// 获取取消频道发送器的克隆
  /// Get a clone of the cancellation channel sender
  pub fn cancel_sender(&self) -> broadcast::Sender<String> {
    self.cancel_tx.clone()
  }

  /// 将任务消息编码为字节
  /// Encode task message to bytes
  pub(crate) fn encode_task_message(&self, msg: &TaskMessage) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    msg.encode(&mut buf)?;
    Ok(buf)
  }

  /// 从字节解码任务消息
  /// Decode task message from bytes
  pub fn decode_task_message(&self, data: &[u8]) -> Result<TaskMessage> {
    match TaskMessage::decode(data) {
      Ok(msg) => Ok(msg),
      Err(decode_err) => Err(Error::ProtoDecode(decode_err)),
    }
  }

  /// 从 Task 创建 TaskMessage
  /// Create TaskMessage from Task
  pub(crate) fn task_to_message(&self, task: &Task) -> TaskMessage {
    TaskMessage {
      r#type: task.task_type.clone(),
      payload: task.payload.clone(),
      headers: task.headers.clone(),
      id: task
        .options
        .task_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string()),
      queue: if task.options.queue.is_empty() {
        DEFAULT_QUEUE_NAME.to_string()
      } else {
        task.options.queue.clone()
      },
      retry: task.options.max_retry,
      retried: 0,
      error_msg: String::new(),
      last_failed_at: 0,
      timeout: task
        .options
        .timeout
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0),
      deadline: task.options.deadline.map(|d| d.timestamp()).unwrap_or(0),
      unique_key: String::new(),
      group_key: task.options.group.clone().unwrap_or_default(),
      retention: task
        .options
        .retention
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0),
      completed_at: 0,
    }
  }

  /// 清理过期的唯一键
  /// Clean up expired unique keys
  #[allow(dead_code)]
  pub(crate) async fn cleanup_expired_unique_keys(&self) {
    let now = Utc::now().timestamp();
    let mut storage = self.storage.write().await;
    storage
      .unique_keys
      .retain(|_, (_, expires_at)| *expires_at > now);
  }

  /// 获取当前日期字符串
  /// Get current date string
  pub(crate) fn get_date_string(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d").to_string()
  }
}
