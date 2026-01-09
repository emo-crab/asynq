//! 任务模块
//! Task module
//!
//! 定义了任务相关的数据结构和功能
//! Defines data structures and functions related to tasks

use crate::base::{keys::TaskState, Broker};
use crate::error::{Error, Result};
use crate::proto;
use crate::rdb::option::{RateLimit, RetryPolicy, TaskOptions};
use crate::inspector::Inspector;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// 任务结果写入器
/// Task result writer
///
/// 用于写入任务执行结果，对应 Go asynq 的 ResultWriter
/// Used to write task execution results, corresponding to Go asynq's ResultWriter
#[derive(Clone)]
pub struct ResultWriter {
  /// 任务 ID
  /// Task ID
  task_id: String,
  /// 队列名称
  /// Queue name
  queue: String,
  /// Broker 实例
  /// Broker instance
  broker: Arc<dyn Broker>,
}

impl std::fmt::Debug for ResultWriter {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ResultWriter")
      .field("task_id", &self.task_id)
      .field("queue", &self.queue)
      .field("broker", &"<Broker>")
      .finish()
  }
}

impl ResultWriter {
  /// 创建新的 ResultWriter
  /// Create a new ResultWriter
  pub fn new(task_id: String, queue: String, broker: Arc<dyn Broker>) -> Self {
    Self {
      task_id,
      queue,
      broker,
    }
  }

  /// 写入任务结果
  /// Write task result
  ///
  /// 将给定的数据作为任务结果写入，返回写入的字节数
  /// Writes the given data as task result, returns the number of bytes written
  pub async fn write(&self, data: &[u8]) -> Result<usize> {
    self
      .broker
      .write_result(&self.queue, &self.task_id, data)
      .await?;
    Ok(data.len())
  }

  /// 获取任务 ID
  /// Get task ID
  pub fn task_id(&self) -> &str {
    &self.task_id
  }
}

/// 表示要执行的工作单元的任务
/// Represents a task as a unit of work to be executed
///
/// # Note on Equality
/// The `PartialEq` implementation compares all fields except `result_writer`.
/// This means two tasks with different result_writers can be considered equal
/// if all other fields match, which is the expected behavior since result_writer
/// is a runtime attachment and not part of the task's logical identity.
///
///
/// # 关于相等性的说明
/// `PartialEq` 实现比较除 `result_writer` 之外的所有字段。
/// 这意味着如果所有其他字段匹配，两个具有不同 result_writer 的任务可以被认为是相等的，
/// 这是预期的行为，因为 result_writer 是运行时附件，而不是任务逻辑身份的一部分。
#[derive(Clone)]
pub struct Task {
  /// 任务类型名称
  /// Task type name
  pub task_type: String,
  /// 任务负载数据
  /// Task payload data
  pub payload: Vec<u8>,
  /// 任务头信息
  /// Task headers
  pub headers: HashMap<String, String>,
  /// 任务选项
  /// Task options
  pub options: TaskOptions,
  /// 任务结果写入器
  /// Task result writer
  ///
  /// 对于新创建的任务（通过 Task::new 创建）为 None
  /// 只有传递给 Handler::process_task 的任务才有有效的 ResultWriter
  /// None for newly created tasks (created via Task::new)
  /// Only tasks passed to Handler::process_task have a valid ResultWriter
  result_writer: Option<Arc<ResultWriter>>,
  inspector: Option<Arc<Inspector>>
}
impl Debug for Task {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Task")
      .field("task_type", &self.task_type)
      .field("payload",&self.payload)
      .field("headers",&self.headers)
      .field("options",&self.options)
      .finish()
  }
}
impl Task {
  /// 创建新任务
  /// Create a new task
  pub fn new<T: AsRef<str>>(task_type: T, payload: &[u8]) -> Result<Self> {
    let task_type = task_type.as_ref();
    if task_type.trim().is_empty() {
      return Err(Error::InvalidTaskType {
        task_type: task_type.to_string(),
      });
    }

    Ok(Self {
      task_type: task_type.to_string(),
      payload: payload.to_vec(),
      headers: Default::default(),
      options: TaskOptions::default(),
      result_writer: None,
      inspector: None,
    })
  }
  pub fn new_with_headers<T: AsRef<str>>(
    task_type: T,
    payload: &[u8],
    headers: HashMap<String, String>,
  ) -> Result<Self> {
    let mut task = Self::new(task_type, payload)?;
    task.headers = headers;
    Ok(task)
  }
  #[cfg(feature = "json")]
  /// 使用 JSON 负载创建新任务
  /// Create a new task with JSON payload
  pub fn new_with_json<T: AsRef<str>, P: Serialize>(task_type: T, payload: &P) -> Result<Self> {
    let json_payload = serde_json::to_vec(payload)?;
    Self::new(task_type, &json_payload)
  }

  /// 设置任务选项
  /// Set task options
  pub fn with_options(mut self, options: TaskOptions) -> Self {
    self.options = options;
    self
  }

  /// 设置队列名称
  /// Set queue name
  pub fn with_queue<T: AsRef<str>>(mut self, queue: T) -> Self {
    self.options.queue = queue.as_ref().to_string();
    self
  }

  /// 设置最大重试次数
  /// Set maximum retry attempts
  pub fn with_max_retry(mut self, max_retry: i32) -> Self {
    self.options.max_retry = max_retry.max(0);
    self
  }

  /// 设置任务超时
  /// Set task timeout
  pub fn with_timeout(mut self, timeout: Duration) -> Self {
    self.options.timeout = Some(timeout);
    self
  }

  /// 设置任务截止时间
  /// Set task deadline
  pub fn with_deadline(mut self, deadline: DateTime<Utc>) -> Self {
    self.options.deadline = Some(deadline);
    self
  }

  /// 设置唯一任务TTL
  /// Set unique task TTL
  pub fn with_unique_ttl(mut self, ttl: Duration) -> Self {
    self.options.unique_ttl = Some(ttl);
    self
  }

  /// 设置任务组
  /// Set task group
  pub fn with_group<T: AsRef<str>>(mut self, group: T) -> Self {
    self.options.group = Some(group.as_ref().to_string());
    self
  }

  /// 设置重试策略
  /// Set retry policy
  pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
    self.options.retry_policy = Some(policy);
    self
  }

  /// 设置速率限制
  /// Set rate limit
  pub fn with_rate_limit(mut self, rate_limit: RateLimit) -> Self {
    self.options.rate_limit = Some(rate_limit);
    self
  }

  /// 设置任务 ID
  /// Set task ID
  pub fn with_task_id<T: AsRef<str>>(mut self, id: T) -> Self {
    self.options.task_id = Some(id.as_ref().to_string());
    self
  }

  /// 设置绝对处理时间
  /// Set absolute processing time
  pub fn with_process_at(mut self, when: DateTime<Utc>) -> Self {
    self.options.process_at = Some(when);
    self
  }

  /// 设置相对延迟
  /// Set relative delay
  pub fn with_process_in(mut self, delay: Duration) -> Self {
    self.options.process_in = Some(delay);
    self
  }

  /// 设置完成结果保留时间
  /// Set retention time for completion results
  pub fn with_retention(mut self, retention: Duration) -> Self {
    self.options.retention = Some(retention);
    self
  }

  /// 设置组聚合宽限期
  /// Set group aggregation grace period
  pub fn with_group_grace_period(mut self, grace: Duration) -> Self {
    self.options.group_grace_period = Some(grace);
    self
  }

  /// 获取任务类型
  /// Get task type
  pub fn get_type(&self) -> &str {
    &self.task_type
  }

  /// 获取任务负载
  /// Get task payload
  pub fn get_payload(&self) -> &[u8] {
    &self.payload
  }
  /// 获取任务头信息
  /// Get task headers
  pub fn get_headers(&self) -> &HashMap<String, String> {
    &self.headers
  }

  /// 获取任务结果写入器
  /// Get task result writer
  ///
  /// 对于新创建的任务（通过 Task::new 创建）返回 None
  /// 只有传递给 Handler::process_task 的任务才有有效的 ResultWriter
  /// Returns None for newly created tasks (created via Task::new)
  /// Only tasks passed to Handler::process_task have a valid ResultWriter
  pub fn result_writer(&self) -> Option<&Arc<ResultWriter>> {
    self.result_writer.as_ref()
  }
  /// 获取检查客户端到任务
  /// Get task inspector
  pub fn inspector(&self) -> Option<&Arc<Inspector>> {
    self.inspector.as_ref()
  }
  /// 附加结果写入器到任务
  /// Attach result writer to task
  ///
  /// 这是一个内部方法，用于在任务处理前附加 ResultWriter
  /// This is an internal method used to attach ResultWriter before task processing
  pub(crate) fn with_result_writer(mut self, writer: Arc<ResultWriter>) -> Self {
    self.result_writer = Some(writer);
    self
  }
  /// 附加检查客户端到任务
  /// Attach inspector to task
  ///
  /// 这是一个内部方法，用于在任务处理前附加 inspector
  /// This is an internal method used to attach inspector before task processing
  pub(crate) fn with_inspector(mut self, inspector: Arc<Inspector>) -> Self {
    self.inspector = Some(inspector);
    self
  }
  #[cfg(feature = "json")]
  /// 获取任务负载作为 JSON
  /// Get task payload as JSON
  pub fn get_payload_with_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
    serde_json::from_slice(&self.payload).map_err(Into::into)
  }
}

impl PartialEq for Task {
  fn eq(&self, other: &Self) -> bool {
    // 比较所有字段，除了 result_writer
    // Compare all fields except result_writer
    self.task_type == other.task_type
      && self.payload == other.payload
      && self.headers == other.headers
      && self.options == other.options
  }
}

/// 任务信息，描述任务及其元数据
/// Task information, describing the task and its metadata
#[derive(Debug, Clone, PartialEq)]
pub struct TaskInfo {
  /// 任务标识符
  /// Task identifier
  pub id: String,
  /// 任务所属的队列名称
  /// Queue name to which the task belongs
  pub queue: String,
  /// 任务类型
  /// Task type
  pub task_type: String,
  /// 任务负载数据
  /// Task payload data
  pub payload: Vec<u8>,
  /// 任务头信息
  /// Task headers
  pub headers: HashMap<String, String>,
  /// 任务状态
  /// Task state
  pub state: TaskState,
  /// 任务最大重试次数
  /// Maximum retry attempts for the task
  pub max_retry: i32,
  /// 任务已重试次数
  /// Number of times the task has been retried
  pub retried: i32,
  /// 上次失败的错误信息
  /// Error message from the last failure
  pub last_err: Option<String>,
  /// 上次失败时间
  /// Time of the last failure
  pub last_failed_at: Option<DateTime<Utc>>,
  /// 任务超时时间
  /// Task timeout duration
  pub timeout: Option<Duration>,
  /// 任务截止时间
  /// Task deadline
  pub deadline: Option<DateTime<Utc>>,
  /// 任务组
  /// Task group
  pub group: Option<String>,
  /// 下次处理时间
  /// Next processing time
  pub next_process_at: Option<DateTime<Utc>>,
  /// 是否为孤儿任务
  /// Whether the task is an orphan
  pub is_orphaned: bool,
  /// 保留期限
  /// Retention period
  pub retention: Option<Duration>,
  /// 完成时间
  /// Completion time
  pub completed_at: Option<DateTime<Utc>>,
  /// 任务结果
  /// Task result
  pub result: Option<Vec<u8>>,
}

impl TaskInfo {
  /// 从 Protocol Buffer 消息创建任务信息
  /// Create task information from Protocol Buffer message
  pub fn from_proto(
    msg: &proto::TaskMessage,
    state: TaskState,
    next_process_at: Option<DateTime<Utc>>,
    result: Option<Vec<u8>>,
  ) -> Self {
    Self {
      id: msg.id.clone(),
      queue: msg.queue.clone(),
      task_type: msg.r#type.clone(),
      payload: msg.payload.clone(),
      headers: msg.headers.clone(),
      state,
      max_retry: msg.retry,
      retried: msg.retried,
      last_err: if msg.error_msg.is_empty() {
        None
      } else {
        Some(msg.error_msg.clone())
      },
      last_failed_at: if msg.last_failed_at == 0 {
        None
      } else {
        Some(DateTime::from_timestamp(msg.last_failed_at, 0).unwrap_or_default())
      },
      timeout: if msg.timeout == 0 {
        None
      } else {
        Some(Duration::from_secs(msg.timeout as u64))
      },
      deadline: if msg.deadline == 0 {
        None
      } else {
        Some(DateTime::from_timestamp(msg.deadline, 0).unwrap_or_default())
      },
      group: if msg.group_key.is_empty() {
        None
      } else {
        Some(msg.group_key.clone())
      },
      next_process_at,    // 需要从其他地方获取
      is_orphaned: false, // 需要从其他地方确定
      retention: if msg.retention == 0 {
        None
      } else {
        Some(Duration::from_secs(msg.retention as u64))
      },
      completed_at: if msg.completed_at == 0 {
        None
      } else {
        Some(DateTime::from_timestamp(msg.completed_at, 0).unwrap_or_default())
      },
      result, // 需要从其他地方获取
    }
  }

  /// 转换为 Protocol Buffer 消息
  /// Convert to Protocol Buffer message
  pub fn to_proto(&self) -> proto::TaskMessage {
    proto::TaskMessage {
      id: self.id.clone(),
      r#type: self.task_type.clone(),
      payload: self.payload.clone(),
      queue: self.queue.clone(),
      retry: self.max_retry,
      retried: self.retried,
      error_msg: self.last_err.clone().unwrap_or_default(),
      last_failed_at: self.last_failed_at.map(|dt| dt.timestamp()).unwrap_or(0),
      timeout: self.timeout.map(|d| d.as_secs() as i64).unwrap_or(0),
      deadline: self.deadline.map(|dt| dt.timestamp()).unwrap_or(0),
      unique_key: String::new(), // 需要单独计算
      group_key: self.group.clone().unwrap_or_default(),
      retention: self.retention.map(|d| d.as_secs() as i64).unwrap_or(0),
      completed_at: self.completed_at.map(|dt| dt.timestamp()).unwrap_or(0),
      headers: self.headers.clone(),
    }
  }
}

/// 队列统计信息
/// Queue statistics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueStats {
  /// 队列名称
  /// Queue name
  pub name: String,
  /// 活跃任务数
  /// Number of active tasks
  pub active: i64,
  /// 等待中任务数
  /// Number of pending tasks
  pub pending: i64,
  /// 已调度任务数
  /// Number of scheduled tasks
  pub scheduled: i64,
  /// 重试任务数
  /// Number of retry tasks
  pub retry: i64,
  /// 已归档任务数
  /// Number of archived tasks
  pub archived: i64,
  /// 已完成任务数
  /// Number of completed tasks
  pub completed: i64,
  /// 聚合中任务数
  /// Number of aggregating tasks
  pub aggregating: i64,
  /// 每日统计
  /// Daily statistics
  pub daily_stats: Vec<DailyStats>,
}

/// 每日统计信息
/// Daily statistics information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DailyStats {
  /// 队列名称
  /// Queue name
  pub queue: String,
  /// 处理的任务数
  /// Number of processed tasks
  pub processed: i64,
  /// 失败的任务数
  /// Number of failed tasks
  pub failed: i64,
  /// 日期
  /// Date
  pub date: DateTime<Utc>,
}

/// 队列信息 - 对应 Go 的 QueueInfo
/// Queue information - Corresponds to Go's QueueInfo
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueInfo {
  /// 队列名称
  /// Queue name
  pub queue: String,
  /// 内存使用量（字节）
  /// Memory usage (in bytes)
  pub memory_usage: i64,
  /// 延迟（任务从入队到开始处理的平均时间）
  /// Latency (average time from task enqueue to processing start)
  pub latency: Duration,
  /// 队列大小（所有状态任务的总数）
  /// Queue size (total number of tasks in all states)
  pub size: i32,
  /// 任务组数量
  /// Number of task groups
  pub groups: i32,
  /// 等待中任务数
  /// Number of pending tasks
  pub pending: i32,
  /// 活跃任务数
  /// Number of active tasks
  pub active: i32,
  /// 已调度任务数
  /// Number of scheduled tasks
  pub scheduled: i32,
  /// 重试任务数
  /// Number of retry tasks
  pub retry: i32,
  /// 已归档任务数
  /// Number of archived tasks
  pub archived: i32,
  /// 已完成任务数
  /// Number of completed tasks
  pub completed: i32,
  /// 聚合中任务数
  /// Number of aggregating tasks
  pub aggregating: i32,
  /// 今日处理任务数
  /// Number of tasks processed today
  pub processed: i32,
  /// 今日失败任务数
  /// Number of tasks failed today
  pub failed: i32,
  /// 处理任务总数
  /// Total number of processed tasks
  pub processed_total: i32,
  /// 失败任务总数
  /// Total number of failed tasks
  pub failed_total: i32,
  /// 是否暂停
  /// Whether paused
  pub paused: bool,
  /// 统计时间戳
  /// Statistics timestamp
  pub timestamp: DateTime<Utc>,
}

/// 生成唯一键 - 使用与 redis.rs 中 unique_key 相同的逻辑
/// Generate unique key - Using the same logic as unique_key in redis.rs
pub fn generate_unique_key(queue: &str, task_type: &str, payload: &[u8]) -> String {
  crate::base::keys::unique_key(queue, task_type, payload)
}

/// 生成任务 ID
/// Generate task ID
pub fn generate_task_id() -> String {
  Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::base::constants::DEFAULT_QUEUE_NAME;
  use crate::rdb::option::RetryPolicy;

  #[test]
  fn test_task_creation() {
    let task = Task::new("test_task", b"test payload").unwrap();
    assert_eq!(task.task_type, "test_task");
    assert_eq!(task.payload, b"test payload");
    assert_eq!(task.options.queue, DEFAULT_QUEUE_NAME);
  }

  #[test]
  fn test_task_with_options() {
    let task = Task::new("test_task", b"test payload")
      .unwrap()
      .with_queue("custom_queue")
      .with_max_retry(10);

    assert_eq!(task.options.queue, "custom_queue");
    assert_eq!(task.options.max_retry, 10);
  }
  #[cfg(feature = "json")]
  #[test]
  fn test_task_json_payload() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestPayload {
      message: String,
      count: i32,
    }

    let payload = TestPayload {
      message: "test".to_string(),
      count: 42,
    };

    let task = Task::new(
      "test_task",
      &serde_json::to_vec(&payload).unwrap_or_default(),
    )
    .unwrap();
    let decoded: TestPayload = serde_json::from_slice(task.get_payload()).unwrap();
    assert_eq!(decoded, payload);
  }

  #[test]
  fn test_task_state_conversion() {
    assert_eq!("active".parse::<TaskState>(), Ok(TaskState::Active));
    assert_eq!("pending".parse::<TaskState>(), Ok(TaskState::Pending));
    assert!("invalid".parse::<TaskState>().is_err());

    assert_eq!(TaskState::Active.as_str(), "active");
    assert_eq!(TaskState::Pending.as_str(), "pending");
  }

  #[test]
  fn test_unique_key_generation() {
    let key1 = generate_unique_key("queue1", "task_type", b"payload");
    let key2 = generate_unique_key("queue1", "task_type", b"payload");
    let key3 = generate_unique_key("queue2", "task_type", b"payload");

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
  }

  #[test]
  fn test_task_id_generation() {
    let id1 = generate_task_id();
    let id2 = generate_task_id();

    assert_ne!(id1, id2);
    assert!(Uuid::parse_str(&id1).is_ok());
    assert!(Uuid::parse_str(&id2).is_ok());
  }

  #[test]
  fn test_retry_policy_fixed() {
    let policy = RetryPolicy::Fixed(Duration::from_secs(30));

    assert_eq!(policy.calculate_delay(0), Duration::from_secs(30));
    assert_eq!(policy.calculate_delay(5), Duration::from_secs(30));
  }

  #[test]
  fn test_retry_policy_exponential() {
    let policy = RetryPolicy::Exponential {
      base_delay: Duration::from_secs(1),
      max_delay: Duration::from_secs(300),
      multiplier: 2.0,
      jitter: false,
    };

    assert_eq!(policy.calculate_delay(0), Duration::from_secs(1));
    assert_eq!(policy.calculate_delay(1), Duration::from_secs(2));
    assert_eq!(policy.calculate_delay(2), Duration::from_secs(4));

    // Test max delay cap
    let delay = policy.calculate_delay(10);
    assert_eq!(delay, Duration::from_secs(300));
  }

  #[test]
  fn test_retry_policy_linear() {
    let policy = RetryPolicy::Linear {
      base_delay: Duration::from_secs(10),
      max_delay: Duration::from_secs(100),
      step: Duration::from_secs(5),
    };

    assert_eq!(policy.calculate_delay(0), Duration::from_secs(10));
    assert_eq!(policy.calculate_delay(1), Duration::from_secs(15));
    assert_eq!(policy.calculate_delay(2), Duration::from_secs(20));

    // Test max delay cap
    let delay = policy.calculate_delay(100);
    assert_eq!(delay, Duration::from_secs(100));
  }

  #[test]
  fn test_rate_limit_key_generation() {
    let rate_limit = RateLimit::per_task_type(Duration::from_secs(60), 10);
    let key = rate_limit.generate_key("email:send", "high_priority");
    assert_eq!(key, "asynq:ratelimit:task:email:send");

    let rate_limit = RateLimit::per_queue(Duration::from_secs(60), 10);
    let key = rate_limit.generate_key("email:send", "high_priority");
    assert_eq!(key, "asynq:ratelimit:queue:high_priority");

    let rate_limit = RateLimit::custom("custom_key", Duration::from_secs(60), 10);
    let key = rate_limit.generate_key("email:send", "high_priority");
    assert_eq!(key, "asynq:ratelimit:custom:custom_key");
  }

  #[test]
  fn test_task_with_retry_policy() {
    let retry_policy = RetryPolicy::default_exponential();
    let task = Task::new("test:task", b"payload")
      .unwrap()
      .with_retry_policy(retry_policy.clone());

    assert_eq!(task.options.retry_policy, Some(retry_policy));
  }

  #[test]
  fn test_task_with_rate_limit() {
    let rate_limit = RateLimit::per_task_type(Duration::from_secs(60), 100);
    let task = Task::new("test:task", b"payload")
      .unwrap()
      .with_rate_limit(rate_limit.clone());

    assert_eq!(task.options.rate_limit, Some(rate_limit));
  }

  #[test]
  fn test_task_result_writer_none_on_new_task() {
    // 新创建的任务应该没有 ResultWriter
    // Newly created tasks should not have a ResultWriter
    let task = Task::new("test:task", b"payload").unwrap();
    assert!(task.result_writer().is_none());
  }

  #[tokio::test]
  async fn test_result_writer_functionality() {
    use crate::rdb::RedisBroker;
    use crate::redis::RedisConnectionType;

    // 跳过测试如果没有 Redis 连接
    // Skip test if no Redis connection
    let redis_url =
      std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    let redis_config = match RedisConnectionType::single(redis_url) {
      Ok(config) => config,
      Err(_) => {
        println!("Skipping test: Redis not available");
        return;
      }
    };

    let broker = match RedisBroker::new(redis_config).await {
      Ok(broker) => Arc::new(broker),
      Err(_) => {
        println!("Skipping test: Could not connect to Redis");
        return;
      }
    };

    // 创建 ResultWriter
    // Create ResultWriter
    let task_id = generate_task_id();
    let queue = "test_queue";
    let result_writer = ResultWriter::new(task_id.clone(), queue.to_string(), broker.clone());

    // 测试 task_id 方法
    // Test task_id method
    assert_eq!(result_writer.task_id(), task_id);

    // 测试写入结果
    // Test writing result
    let result_data = b"test result data";
    let bytes_written = result_writer.write(result_data).await.unwrap();
    assert_eq!(bytes_written, result_data.len());
  }

  #[test]
  fn test_task_with_result_writer() {
    // 创建任务
    // Create task
    let task = Task::new("test:task", b"payload").unwrap();
    assert!(task.result_writer().is_none());

    // 注意：在实际测试中，我们需要一个真实的 broker
    // 这里我们只是测试 API 的存在
    // Note: In actual tests, we would need a real broker
    // Here we're just testing the API exists
  }
}
