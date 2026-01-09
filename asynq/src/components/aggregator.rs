//! Aggregator 模块
//! Aggregator module
//!
//! 对应 Go 版本的 aggregator.go 职责：
//! Responsibilities corresponding to the Go version's aggregator.go:
//! 聚合任务到组中以进行批量处理
//! Aggregate tasks into groups for batch processing
//!
//! 参考 Go asynq/aggregator.go
//! Reference: Go asynq/aggregator.go

use crate::base::Broker;
use crate::components::ComponentLifecycle;
use crate::error::Result;
use crate::task::Task;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// GroupAggregator trait - 聚合一组任务为一个任务
/// GroupAggregator trait - aggregates a group of tasks into one task
///
/// 对应 Go asynq 的 GroupAggregator 接口
/// Corresponds to Go asynq's GroupAggregator interface
///
/// ```go
/// type GroupAggregator interface {
///     Aggregate(group string, tasks []*Task) *Task
/// }
/// ```
pub trait GroupAggregator: Send + Sync {
  /// 聚合给定组中的任务，返回聚合后的新任务
  /// Aggregates the given tasks in a group and returns a new aggregated task
  ///
  /// # Arguments
  /// * `group` - 组名 / Group name
  /// * `tasks` - 要聚合的任务列表 / List of tasks to aggregate
  ///
  /// # Returns
  /// 聚合后的新任务。队列选项会被忽略，聚合任务总是入队到组所属的队列
  /// The aggregated new task. Queue option will be ignored, aggregated task always enqueues to the group's queue
  fn aggregate(&self, group: &str, tasks: Vec<Task>) -> Result<Task>;
}

/// 函数式 GroupAggregator 适配器
/// Functional GroupAggregator adapter
pub struct GroupAggregatorFunc<F> {
  func: F,
}

impl<F> GroupAggregatorFunc<F>
where
  F: Fn(&str, Vec<Task>) -> Result<Task> + Send + Sync,
{
  /// 创建新的函数式 GroupAggregator
  /// Create a new functional GroupAggregator
  pub fn new(func: F) -> Self {
    Self { func }
  }
}

impl<F> GroupAggregator for GroupAggregatorFunc<F>
where
  F: Fn(&str, Vec<Task>) -> Result<Task> + Send + Sync,
{
  fn aggregate(&self, group: &str, tasks: Vec<Task>) -> Result<Task> {
    (self.func)(group, tasks)
  }
}

/// Aggregator 配置
/// Aggregator configuration
pub struct AggregatorConfig {
  /// 检查间隔
  /// Check interval
  pub interval: Duration,
  /// 队列列表
  /// Queue list
  pub queues: Vec<String>,
  /// 组宽限期
  /// Group grace period
  pub grace_period: Duration,
  /// 最大延迟
  /// Maximum delay
  pub max_delay: Option<Duration>,
  /// 最大组大小
  /// Maximum group size
  pub max_size: Option<usize>,
  /// 组聚合器 - 将一组任务聚合为一个任务
  /// Group aggregator - aggregates a group of tasks into one task
  pub group_aggregator: Option<Arc<dyn GroupAggregator>>,
}

impl Default for AggregatorConfig {
  fn default() -> Self {
    Self {
      interval: Duration::from_secs(5),
      queues: vec!["default".to_string()],
      grace_period: Duration::from_secs(60),
      max_delay: None,
      max_size: None,
      group_aggregator: None,
    }
  }
}

/// GroupInfo - 组信息
/// GroupInfo - Group information
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields will be used in future enhancements
struct GroupInfo {
  /// 组名
  /// Group name
  pub group: String,
  /// 队列名
  /// Queue name
  pub queue: String,
  /// 集合 ID
  /// Set ID
  pub set_id: Option<String>,
  /// 任务数量
  /// Task count
  pub task_count: usize,
}

/// Aggregator - 负责聚合任务到组中进行批量处理
/// Aggregator - responsible for aggregating tasks into groups for batch processing
///
/// 对应 Go asynq 的 aggregator 组件
/// Corresponds to Go asynq's aggregator component
///
/// 聚合器监控任务组，当满足以下条件之一时触发批处理：
/// The aggregator monitors task groups and triggers batch processing when one of the following conditions is met:
/// 1. 组大小达到 max_size
/// 1. Group size reaches max_size
/// 2. 自第一个任务加入后经过了 max_delay 时间
/// 2. max_delay time has elapsed since the first task was added
/// 3. 宽限期 grace_period 到期
/// 3. Grace period grace_period expires
pub struct Aggregator {
  broker: Arc<dyn Broker>,
  config: AggregatorConfig,
  done: Arc<AtomicBool>,
  #[allow(dead_code)] // Will be used for tracking group state in future enhancements
  groups: Arc<RwLock<HashMap<String, GroupInfo>>>,
}

impl Aggregator {
  /// 创建新的 Aggregator
  /// Create a new Aggregator
  pub fn new(broker: Arc<dyn Broker>, config: AggregatorConfig) -> Self {
    Self {
      broker,
      config,
      done: Arc::new(AtomicBool::new(false)),
      groups: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  /// 启动 Aggregator
  /// Start the Aggregator
  ///
  /// 对应 Go 的 aggregator.start()
  /// Corresponds to Go's aggregator.start()
  pub fn start(self: Arc<Self>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(self.config.interval);
      loop {
        interval.tick().await;

        if self.done.load(Ordering::Relaxed) {
          tracing::debug!("Aggregator: shutting down");
          break;
        }

        // 执行聚合检查
        // Execute aggregation check
        if let Err(e) = self.aggregate().await {
          tracing::error!("Aggregator error: {}", e);
        }
      }
    })
  }

  /// 执行聚合检查
  /// Execute aggregation check
  ///
  /// 对应 Go 的 aggregator.exec()
  /// Corresponds to Go's aggregator.exec()
  async fn aggregate(&self) -> Result<()> {
    // 检查每个队列的聚合任务
    // Check aggregation tasks for each queue
    for queue in &self.config.queues {
      // 检查是否有满足条件的聚合集合
      let groups = self.broker.list_groups(queue).await?;
      for group in groups {
        tracing::debug!("Aggregator: found group in queue {}: {:?}", queue, group);
        // Check if there are aggregation sets that meet the conditions
        if let Ok(Some(set_id)) = self
          .broker
          .aggregation_check(
            queue,
            &group,
            self.config.grace_period,
            self.config.max_delay.unwrap_or(Duration::from_secs(30)),
            self.config.max_size.unwrap_or(10),
          )
          .await
        {
          tracing::debug!(
            "Aggregator: found aggregation set ready for processing: queue={}, set_id={}",
            queue,
            set_id
          );

          // 读取聚合集合中的任务
          // Read tasks from the aggregation set
          match self
            .broker
            .read_aggregation_set(queue, &group, &set_id)
            .await
          {
            Ok(task_messages) => {
              let task_count = task_messages.len();
              tracing::info!(
                "Aggregator: processing {} tasks from aggregation set {} in queue {}",
                task_count,
                set_id,
                queue
              );

              // 如果配置了 GroupAggregator，则调用聚合函数
              // If GroupAggregator is configured, call the aggregation function
              if let Some(aggregator) = &self.config.group_aggregator {
                // 将 TaskMessage 转换为 Task
                // Convert TaskMessage to Task
                let mut tasks = Vec::new();
                for task_msg in task_messages {
                  match Task::new_with_headers(
                    &task_msg.r#type,
                    &task_msg.payload,
                    task_msg.headers,
                  ) {
                    Ok(task) => tasks.push(task),
                    Err(e) => {
                      tracing::warn!("Aggregator: failed to create task from message: {}", e);
                    }
                  }
                }

                if !tasks.is_empty() {
                  // 调用聚合函数
                  // Call aggregation function
                  match aggregator.aggregate(&group, tasks) {
                    Ok(aggregated_task) => {
                      tracing::info!(
                        "Aggregator: aggregated {} tasks into task type '{}' for group '{}'",
                        task_count,
                        aggregated_task.get_type(),
                        group
                      );

                      // 入队聚合后的任务到原队列
                      // Enqueue the aggregated task to the original queue
                      let mut enqueue_task = aggregated_task.with_queue(queue);
                      // 保留组信息以便跟踪
                      // Preserve group info for tracking
                      if enqueue_task.options.group.is_none() {
                        enqueue_task = enqueue_task.with_group(&group);
                      }

                      if let Err(e) = self.broker.enqueue(&enqueue_task).await {
                        tracing::error!("Aggregator: failed to enqueue aggregated task: {}", e);
                      } else {
                        tracing::debug!(
                          "Aggregator: successfully enqueued aggregated task to queue '{}'",
                          queue
                        );
                      }
                    }
                    Err(e) => {
                      tracing::error!(
                        "Aggregator: failed to aggregate tasks for group '{}': {}",
                        group,
                        e
                      );
                    }
                  }
                }
              } else {
                // 如果没有配置 GroupAggregator，保持原有行为：只记录日志
                // If GroupAggregator is not configured, maintain original behavior: only log
                tracing::debug!(
                  "Aggregator: no GroupAggregator configured, tasks read but not processed"
                );
              }
            }
            Err(e) => {
              tracing::warn!(
                "Aggregator: failed to read aggregation set {}: {}",
                set_id,
                e
              );
            }
          }

          // 关闭聚合集合
          // Close the aggregation set
          if let Err(e) = self
            .broker
            .delete_aggregation_set(queue, &group, &set_id)
            .await
          {
            tracing::warn!(
              "Aggregator: failed to close aggregation set {}: {}",
              set_id,
              e
            );
          }
        }
      }
    }

    Ok(())
  }

  /// 停止 Aggregator
  /// Stop the Aggregator
  ///
  /// 对应 Go 的 aggregator.shutdown()
  /// Corresponds to Go's aggregator.shutdown()
  pub fn shutdown(&self) {
    self.done.store(true, Ordering::Relaxed);
  }

  /// 检查是否已完成
  /// Check if done
  pub fn is_done(&self) -> bool {
    self.done.load(Ordering::Relaxed)
  }
}

impl ComponentLifecycle for Aggregator {
  fn start(self: Arc<Self>) -> JoinHandle<()> {
    Aggregator::start(self)
  }

  fn shutdown(&self) {
    Aggregator::shutdown(self)
  }

  fn is_done(&self) -> bool {
    Aggregator::is_done(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::redis::RedisConnectionType;

  #[test]
  fn test_aggregator_config_default() {
    let config = AggregatorConfig::default();
    assert_eq!(config.interval, Duration::from_secs(5));
    assert_eq!(config.queues, vec!["default".to_string()]);
    assert_eq!(config.grace_period, Duration::from_secs(60));
    assert_eq!(config.max_delay, None);
    assert_eq!(config.max_size, None);
    assert!(config.group_aggregator.is_none());
  }

  #[tokio::test]
  async fn test_aggregator_shutdown() {
    use crate::rdb::RedisBroker;
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();

    let broker = Arc::new(RedisBroker::new(redis_connection_config).await.unwrap());
    let config = AggregatorConfig::default();
    let aggregator = Aggregator::new(broker, config);

    assert!(!aggregator.is_done());
    aggregator.shutdown();
    assert!(aggregator.is_done());
  }

  #[test]
  fn test_group_aggregator_func() {
    // 测试 GroupAggregatorFunc 能否正确调用函数
    // Test that GroupAggregatorFunc can correctly call the function
    let aggregator = GroupAggregatorFunc::new(|group: &str, tasks: Vec<Task>| {
      assert_eq!(group, "test-group");
      assert_eq!(tasks.len(), 3);
      Task::new("batch:process", b"aggregated")
    });

    let tasks = vec![
      Task::new("task1", b"payload1").unwrap(),
      Task::new("task2", b"payload2").unwrap(),
      Task::new("task3", b"payload3").unwrap(),
    ];

    let result = aggregator.aggregate("test-group", tasks);
    assert!(result.is_ok());
    let aggregated = result.unwrap();
    assert_eq!(aggregated.get_type(), "batch:process");
    assert_eq!(aggregated.get_payload(), b"aggregated");
  }

  #[tokio::test]
  async fn test_group_aggregator_with_config() {
    use crate::rdb::RedisBroker;
    let redis_connection_config = RedisConnectionType::single("redis://localhost:6379").unwrap();
    let broker = Arc::new(RedisBroker::new(redis_connection_config).await.unwrap());

    // 创建带有 GroupAggregator 的配置
    // Create config with GroupAggregator
    let aggregator = Arc::new(GroupAggregatorFunc::new(|group: &str, tasks: Vec<Task>| {
      let combined = format!("Aggregated {} tasks from group {}", tasks.len(), group);
      Task::new("batch:process", combined.as_bytes())
    }));

    let config = AggregatorConfig {
      interval: Duration::from_secs(5),
      queues: vec!["default".to_string()],
      grace_period: Duration::from_secs(60),
      max_delay: None,
      max_size: None,
      group_aggregator: Some(aggregator),
    };

    assert!(config.group_aggregator.is_some());

    let aggregator = Aggregator::new(broker, config);
    assert!(!aggregator.is_done());
  }
}
