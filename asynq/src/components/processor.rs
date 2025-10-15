//! 处理器模块
//! Processor module
//!
//! 实现与 Go asynq processor.go 兼容的任务处理器
//! Implements task processor compatible with Go asynq processor.go
//!
//! ## 概述 / Overview
//!
//! Processor 是任务处理的核心组件，负责从 Redis 队列中取出任务并执行。
//! 它实现了与 Go 版本 asynq processor.go 相似的架构和功能。
//!
//! The Processor is the core component for task processing, responsible for dequeuing
//! tasks from Redis queues and executing them. It implements an architecture and
//! functionality similar to Go's asynq processor.go.
//!
//! ## 主要特性 / Key Features
//!
//! - **信号量并发控制**: 使用 Tokio Semaphore 限制并发工作者数量
//!   - **Semaphore-based concurrency**: Uses Tokio Semaphore to limit concurrent workers
//!
//! - **队列优先级**: 支持严格优先级和加权优先级两种模式
//!   - **Queue priority**: Supports both strict priority and weighted priority modes
//!
//! - **任务超时**: 支持任务级别和全局超时设置
//!   - **Task timeout**: Supports task-level and global timeout settings
//!
//! - **优雅关闭**: 等待所有活跃工作者完成后再关闭
//!   - **Graceful shutdown**: Waits for all active workers to finish before shutting down
//!
//! - **自动重试**: 失败任务自动重试，支持指数退避策略
//!   - **Automatic retry**: Failed tasks are automatically retried with exponential backoff
//!
//! - **任务归档**: 达到最大重试次数后自动归档任务
//!   - **Task archival**: Tasks are automatically archived after max retries
//!
//! ## 与 Go 版本的兼容性 / Compatibility with Go version
//!
//! 本实现与 Go 版本的 processor.go 在以下方面保持兼容：
//! This implementation maintains compatibility with Go's processor.go in the following aspects:
//!
//! - 相同的队列优先级算法 / Same queue priority algorithm
//! - 相同的重试延迟计算 / Same retry delay calculation
//! - 相同的任务超时处理 / Same task timeout handling
//! - 相同的并发控制机制 / Same concurrency control mechanism
//!
//! ## 使用示例 / Usage Example
//!
//! ```rust,no_run
//! use asynq::components::processor::{Processor, ProcessorParams};
//! use asynq::server::Handler;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use std::sync::atomic::AtomicUsize;
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // 创建 broker 和其他必要组件
//! // Create broker and other necessary components
//! # use asynq::redis::RedisConnectionConfig;
//! # use asynq::rdb::RedisBroker;
//! # let redis_config = RedisConnectionConfig::single("redis://localhost:6379")?;
//! # let broker = Arc::new(RedisBroker::new(redis_config)?);
//!
//! let mut queues = HashMap::new();
//! queues.insert("default".to_string(), 3);
//! queues.insert("critical".to_string(), 6);
//!
//! let params = ProcessorParams {
//!   broker: broker.clone(),
//!   queues,
//!   concurrency: 10,
//!   strict_priority: false,
//!   task_check_interval: Duration::from_secs(1),
//!   shutdown_timeout: Duration::from_secs(30),
//!   active_workers: Arc::new(AtomicUsize::new(0)),
//! };
//!
//! let mut processor = Processor::new(params);
//!
//! // 启动处理器
//! // Start processor
//! # struct MyHandler;
//! # #[async_trait::async_trait]
//! # impl Handler for MyHandler {
//! #   async fn process_task(&self, _task: asynq::task::Task) -> asynq::error::Result<()> { Ok(()) }
//! # }
//! processor.start(Arc::new(MyHandler));
//!
//! // 停止处理器
//! // Stop processor
//! processor.shutdown().await;
//! # Ok(())
//! # }
//! ```

use crate::base::Broker;
use crate::error::Error;
use crate::server::Handler;
use crate::task::Task;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 处理器参数
/// Processor parameters
pub struct ProcessorParams {
  pub broker: Arc<dyn Broker>,
  pub queues: HashMap<String, i32>,
  pub concurrency: usize,
  pub strict_priority: bool,
  pub task_check_interval: Duration,
  pub shutdown_timeout: Duration,
  pub active_workers: Arc<AtomicUsize>,
}

/// 任务取消追踪结构
/// Task cancellation tracking structure
///
/// 对应 Go asynq 的 internal/base/base.go 中的 Cancellations
/// Corresponds to Cancellations in Go asynq's internal/base/base.go
#[derive(Clone)]
pub struct CancellationMap {
  // 正在运行的任务及其取消令牌
  // Running tasks with their cancellation tokens
  tasks: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

impl CancellationMap {
  /// 创建新的 Cancellations 实例
  /// Create a new Cancellations instance
  pub fn new() -> Self {
    Self {
      tasks: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  /// 添加任务取消令牌
  /// Add task cancellation token
  pub fn add(&self, task_id: String, token: CancellationToken) {
    let mut tasks = self.tasks.lock().unwrap();
    tasks.insert(task_id, token);
  }

  /// 移除任务取消令牌
  /// Remove task cancellation token
  pub fn remove(&self, task_id: &str) {
    let mut tasks = self.tasks.lock().unwrap();
    tasks.remove(task_id);
  }

  /// 取消指定的任务
  /// Cancel specified task
  ///
  /// 对应 Go 的 cancelations.Cancel(taskID)
  /// Corresponds to Go's cancelations.Cancel(taskID)
  pub fn cancel(&self, task_id: &str) -> bool {
    tracing::info!("canceling task {}", task_id);
    let tasks = self.tasks.lock().unwrap();
    if let Some(token) = tasks.get(task_id) {
      token.cancel();
      true
    } else {
      false
    }
  }

  /// 获取正在运行的任务数量
  /// Get the number of running tasks
  pub fn len(&self) -> usize {
    let tasks = self.tasks.lock().unwrap();
    tasks.len()
  }

  /// 检查是否为空
  /// Check if empty
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }
}

impl Default for CancellationMap {
  fn default() -> Self {
    Self::new()
  }
}

/// 处理器 - 负责从队列中取出任务并处理
/// Processor - responsible for dequeuing and processing tasks
pub struct Processor {
  broker: Arc<dyn Broker>,
  queue_config: HashMap<String, i32>,
  ordered_queues: Option<Vec<String>>,
  task_check_interval: Duration,
  shutdown_timeout: Duration,

  // 信号量用于限制并发工作者数量
  // Semaphore to limit number of concurrent workers
  sema: Arc<Semaphore>,

  // 运行状态标志
  // Running state flag
  running: Arc<AtomicBool>,

  // 退出信号通道
  // Quit signal channel
  quit_tx: Option<mpsc::Sender<()>>,
  quit_rx: Option<mpsc::Receiver<()>>,

  // 中止信号通道 - 用于强制停止所有工作者
  // Abort signal channel - used to forcefully stop all workers
  abort_tx: Option<mpsc::Sender<()>>,

  // 处理器主循环句柄
  // Main processor loop handle
  handle: Option<JoinHandle<()>>,

  // 活跃工作者计数
  // Active worker count
  active_workers: Arc<AtomicUsize>,

  // 任务取消追踪
  // Task cancellation tracking
  /// 对应 Go asynq 的 cancelations *base.Cancellations
  /// Corresponds to Go asynq's cancelations *base.Cancellations
  cancellations: CancellationMap,
}

impl Processor {
  /// 创建新的处理器
  /// Create a new processor
  pub fn new(params: ProcessorParams) -> Self {
    let queues = normalize_queues(params.queues);
    let ordered_queues = if params.strict_priority {
      Some(sort_by_priority(&queues))
    } else {
      None
    };

    let (quit_tx, quit_rx) = mpsc::channel(1);
    let (abort_tx, _abort_rx) = mpsc::channel(1);

    Self {
      broker: params.broker,
      queue_config: queues,
      ordered_queues,
      task_check_interval: params.task_check_interval,
      shutdown_timeout: params.shutdown_timeout,
      sema: Arc::new(Semaphore::new(params.concurrency)),
      running: Arc::new(AtomicBool::new(false)),
      quit_tx: Some(quit_tx),
      quit_rx: Some(quit_rx),
      abort_tx: Some(abort_tx),
      handle: None,
      active_workers: params.active_workers,
      cancellations: CancellationMap::new(),
    }
  }

  /// 获取任务取消追踪器的克隆
  /// Get a clone of the task cancellation tracker
  ///
  /// 用于在服务器中接收取消事件后调用 cancel 方法
  /// Used to call the cancel method after receiving cancellation events in the server
  pub fn cancellations(&self) -> CancellationMap {
    self.cancellations.clone()
  }

  /// 启动处理器
  /// Start the processor
  pub fn start<H>(&mut self, handler: Arc<H>)
  where
    H: Handler + 'static,
  {
    self.running.store(true, Ordering::SeqCst);

    let broker = Arc::clone(&self.broker);
    let running = Arc::clone(&self.running);
    let sema = Arc::clone(&self.sema);
    let queue_config = self.queue_config.clone();
    let ordered_queues = self.ordered_queues.clone();
    let task_check_interval = self.task_check_interval;
    let active_workers = Arc::clone(&self.active_workers);
    let cancelations = self.cancellations.clone();
    let mut quit_rx = self.quit_rx.take().unwrap();

    let handle = tokio::spawn(async move {
      loop {
        // 检查是否收到退出信号
        // Check if quit signal received
        if quit_rx.try_recv().is_ok() {
          tracing::debug!("Processor received quit signal");
          break;
        }

        if !running.load(Ordering::SeqCst) {
          break;
        }

        // 尝试获取信号量令牌
        // Try to acquire semaphore permit
        let permit = match sema.clone().try_acquire_owned() {
          Ok(permit) => permit,
          Err(_) => {
            // 没有可用的工作者槽位，短暂等待
            // No available worker slots, wait briefly
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
          }
        };

        // 获取队列列表
        // Get queue list
        let queues = get_queues(&queue_config, ordered_queues.as_ref());

        // 从队列中取出任务
        // Dequeue a task from the queue
        match broker.dequeue(&queues).await {
          Ok(Some(task_msg)) => {
            // 增加活跃工作者计数
            // Increment active worker count
            active_workers.fetch_add(1, Ordering::Relaxed);

            let handler = Arc::clone(&handler);
            let broker = Arc::clone(&broker);
            let active_workers = Arc::clone(&active_workers);
            let cancelations = cancelations.clone();

            // 在新的任务中处理
            // Process in a new task
            tokio::spawn(async move {
              let _permit = permit; // 持有许可直到任务完成

              // 创建任务
              // Create task
              let task = match Task::new(&task_msg.r#type, &task_msg.payload) {
                Ok(task) => task,
                Err(e) => {
                  tracing::error!("Failed to create task: {}", e);
                  active_workers.fetch_sub(1, Ordering::Relaxed);
                  return;
                }
              };

              // 创建取消令牌
              // Create cancellation token
              let cancel_token = CancellationToken::new();
              let task_id = task_msg.id.clone();

              // 注册取消令牌
              // Register cancellation token
              cancelations.add(task_id.clone(), cancel_token.clone());

              // 计算任务超时
              // Calculate task timeout
              let timeout_duration = calculate_task_timeout(&task_msg);

              // 执行任务，支持超时和取消
              // Execute task with timeout and cancellation support
              let result = if let Some(timeout) = timeout_duration {
                tokio::select! {
                  // 任务执行
                  // Task execution
                  result = handler.process_task(task.clone()) => result,
                  // 超时
                  // Timeout
                  _ = tokio::time::sleep(timeout) => {
                    tracing::warn!("Task {} timed out after {:?}", task_id, timeout);
                    Err(Error::other("Task execution timeout"))
                  }
                  // 取消信号
                  // Cancellation signal
                  _ = cancel_token.cancelled() => {
                    tracing::info!("Task {} was cancelled", task_id);
                    Ok(())
                  }
                }
              } else {
                tokio::select! {
                  // 任务执行
                  // Task execution
                  result = handler.process_task(task.clone()) => result,
                  // 取消信号
                  // Cancellation signal
                  _ = cancel_token.cancelled() => {
                    tracing::info!("Task {} was cancelled", task_id);
                    Err(Error::other("Task cancelled"))
                  }
                }
              };

              // 移除取消令牌
              // Remove cancellation token
              cancelations.remove(&task_id);

              // 处理结果
              // Handle result
              match result {
                Ok(()) => {
                  // 任务成功
                  // Task succeeded
                  if let Err(e) = broker.done(&task_msg).await {
                    tracing::error!("Failed to mark task as done: {}", e);
                  }
                }
                Err(e) => {
                  // 任务失败，决定重试还是归档
                  // Task failed, decide retry or archive
                  if task_msg.retried < task_msg.retry {
                    // 计算重试延迟
                    // Calculate retry delay
                    let retry_delay =
                      calculate_retry_delay(task_msg.retried, task.options.retry_policy.as_ref());
                    let retry_at =
                      chrono::Utc::now() + chrono::Duration::seconds(retry_delay.as_secs() as i64);

                    if let Err(e) = broker.requeue(&task_msg, retry_at, &e.to_string()).await {
                      tracing::error!("Failed to requeue task: {}", e);
                    }
                  } else {
                    // 归档任务
                    // Archive task
                    if let Err(e) = broker.archive(&task_msg, &e.to_string()).await {
                      tracing::error!("Failed to archive task: {}", e);
                    }
                  }
                }
              }

              // 减少活跃工作者计数
              // Decrement active worker count
              active_workers.fetch_sub(1, Ordering::Relaxed);
            });
          }
          Ok(None) => {
            // 没有任务，等待后重试
            // No tasks, wait and retry
            drop(permit); // 释放许可
            tokio::time::sleep(task_check_interval).await;
          }
          Err(e) => {
            // 出队错误
            // Dequeue error
            tracing::error!("Dequeue error: {}", e);
            drop(permit); // 释放许可
            tokio::time::sleep(Duration::from_secs(1)).await;
          }
        }
      }

      tracing::debug!("Processor loop exited");
    });

    self.handle = Some(handle);
  }

  /// 停止处理器（不等待工作者完成）
  /// Stop the processor (without waiting for workers)
  pub fn stop(&mut self) {
    self.running.store(false, Ordering::SeqCst);
    if let Some(tx) = self.quit_tx.take() {
      let _ = tx.try_send(());
    }
  }

  /// 关闭处理器并等待所有工作者完成
  /// Shutdown the processor and wait for all workers to finish
  pub async fn shutdown(&mut self) {
    self.stop();

    // 启动超时定时器，之后发送中止信号
    // Start timeout timer, then send abort signal
    let abort_tx = self.abort_tx.clone();
    let shutdown_timeout = self.shutdown_timeout;
    tokio::spawn(async move {
      tokio::time::sleep(shutdown_timeout).await;
      if let Some(tx) = abort_tx {
        let _ = tx.send(()).await;
      }
    });

    // 等待处理器主循环退出
    // Wait for processor main loop to exit
    if let Some(handle) = self.handle.take() {
      let _ = handle.await;
    }

    tracing::info!("Waiting for all workers to finish...");

    // 等待所有信号量令牌被释放（即所有工作者完成）
    // Wait for all semaphore permits to be released (i.e., all workers finished)
    let sema = Arc::clone(&self.sema);
    let concurrency = sema.available_permits();
    for _ in 0..concurrency {
      let _ = sema.acquire().await;
    }

    tracing::info!("All workers have finished");
  }
}

/// 标准化队列配置，确保优先级为正数
/// Normalize queue config, ensure priorities are positive
fn normalize_queues(queues: HashMap<String, i32>) -> HashMap<String, i32> {
  queues
    .into_iter()
    .map(|(name, priority)| (name, priority.max(1)))
    .collect()
}

/// 按优先级排序队列（降序）
/// Sort queues by priority (descending)
fn sort_by_priority(queues: &HashMap<String, i32>) -> Vec<String> {
  let mut queue_vec: Vec<_> = queues.iter().collect();
  queue_vec.sort_by(|a, b| b.1.cmp(a.1)); // 降序排序
  queue_vec
    .into_iter()
    .map(|(name, _)| name.clone())
    .collect()
}

/// 获取队列列表，基于优先级
/// Get queue list based on priority
fn get_queues(
  queue_config: &HashMap<String, i32>,
  ordered_queues: Option<&Vec<String>>,
) -> Vec<String> {
  // 如果只有一个队列，直接返回
  // If only one queue, return directly
  if queue_config.len() == 1 {
    return queue_config.keys().cloned().collect();
  }

  // 如果有排序的队列（严格优先级模式），返回排序后的队列
  // If ordered queues exist (strict priority mode), return ordered queues
  if let Some(ordered) = ordered_queues {
    return ordered.clone();
  }

  // 否则，基于优先级加权随机选择
  // Otherwise, weighted random selection based on priority
  let mut names = Vec::new();
  for (name, &priority) in queue_config {
    for _ in 0..priority {
      names.push(name.clone());
    }
  }

  // 随机打乱
  // Shuffle randomly
  use rand::seq::SliceRandom;
  let mut rng = rand::rng();
  names.shuffle(&mut rng);

  // 去重并返回
  // Deduplicate and return
  let mut seen = std::collections::HashSet::new();
  let mut result = Vec::new();
  for name in names {
    if seen.insert(name.clone()) {
      result.push(name);
    }
    if result.len() == queue_config.len() {
      break;
    }
  }
  result
}

/// 计算任务超时时间
/// Calculate task timeout
fn calculate_task_timeout(task_msg: &crate::proto::TaskMessage) -> Option<Duration> {
  use crate::base::constants::DEFAULT_TIMEOUT;

  // 优先使用任务级别的超时设置
  // Prefer task-level timeout settings
  if task_msg.timeout > 0 {
    return Some(Duration::from_secs(task_msg.timeout as u64));
  }

  // 如果任务有截止时间，计算剩余时间作为超时
  // If the task has a deadline, calculate remaining time as timeout
  if task_msg.deadline > 0 {
    let now = chrono::Utc::now().timestamp();
    let remaining = task_msg.deadline - now;
    if remaining > 0 {
      return Some(Duration::from_secs(remaining as u64));
    }
  }

  // 使用默认超时
  // Use default timeout
  Some(DEFAULT_TIMEOUT)
}

/// 计算重试延迟
/// Calculate retry delay
fn calculate_retry_delay(
  retried: i32,
  retry_policy: Option<&crate::rdb::option::RetryPolicy>,
) -> Duration {
  match retry_policy {
    Some(policy) => policy.calculate_delay(retried),
    None => {
      // 默认指数退避策略 - 与 Go 版本兼容
      // Default exponential backoff strategy - compatible with Go version
      let base_delay = (retried as f64).powf(4.0) as u64 + 15;
      let jitter = rand::random::<u64>() % (30 * (retried as u64 + 1));
      Duration::from_secs(base_delay + jitter)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_normalize_queues() {
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    queues.insert("low".to_string(), 0);
    queues.insert("high".to_string(), -5);

    let normalized = normalize_queues(queues);
    assert_eq!(normalized.get("default"), Some(&3));
    assert_eq!(normalized.get("low"), Some(&1));
    assert_eq!(normalized.get("high"), Some(&1));
  }

  #[test]
  fn test_sort_by_priority() {
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    queues.insert("low".to_string(), 1);
    queues.insert("high".to_string(), 6);

    let sorted = sort_by_priority(&queues);
    assert_eq!(sorted[0], "high");
    assert_eq!(sorted[1], "default");
    assert_eq!(sorted[2], "low");
  }

  #[test]
  fn test_get_queues_single() {
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);

    let result = get_queues(&queues, None);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "default");
  }

  #[test]
  fn test_get_queues_strict_priority() {
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    queues.insert("low".to_string(), 1);
    queues.insert("high".to_string(), 6);

    let ordered = vec!["high".to_string(), "default".to_string(), "low".to_string()];
    let result = get_queues(&queues, Some(&ordered));
    assert_eq!(result, ordered);
  }

  #[test]
  fn test_cancelations_new() {
    let cancelations = CancellationMap::new();
    assert!(cancelations.is_empty());
    assert_eq!(cancelations.len(), 0);
  }

  #[test]
  fn test_cancelations_add_remove() {
    let cancelations = CancellationMap::new();
    let token = CancellationToken::new();

    cancelations.add("task1".to_string(), token.clone());
    assert_eq!(cancelations.len(), 1);
    assert!(!cancelations.is_empty());

    cancelations.remove("task1");
    assert_eq!(cancelations.len(), 0);
    assert!(cancelations.is_empty());
  }

  #[tokio::test]
  async fn test_cancelations_cancel() {
    let cancellations = CancellationMap::new();
    let token = CancellationToken::new();
    let task_id = "task1".to_string();

    // Add token
    cancellations.add(task_id.clone(), token.clone());

    // Verify token is not cancelled
    assert!(!token.is_cancelled());

    // Cancel the task
    let cancelled = cancellations.cancel(&task_id);
    assert!(cancelled);

    // Verify token is now cancelled
    assert!(token.is_cancelled());
  }

  #[tokio::test]
  async fn test_cancelations_cancel_nonexistent() {
    let cancellations = CancellationMap::new();

    // Try to cancel a non-existent task
    let cancelled = cancellations.cancel("nonexistent");
    assert!(!cancelled);
  }

  #[tokio::test]
  async fn test_cancelations_multiple_tasks() {
    let cancellations = CancellationMap::new();

    let token1 = CancellationToken::new();
    let token2 = CancellationToken::new();
    let token3 = CancellationToken::new();

    cancellations.add("task1".to_string(), token1.clone());
    cancellations.add("task2".to_string(), token2.clone());
    cancellations.add("task3".to_string(), token3.clone());

    assert_eq!(cancellations.len(), 3);

    // Cancel task2
    cancellations.cancel("task2");
    assert!(!token1.is_cancelled());
    assert!(token2.is_cancelled());
    assert!(!token3.is_cancelled());

    // Remove task1
    cancellations.remove("task1");
    assert_eq!(cancellations.len(), 2);

    // Cancel task3
    cancellations.cancel("task3");
    assert!(token3.is_cancelled());
  }
}
