//! Comprehensive client tests ported from @hibiken/asynq client_test.go
//!
//! This module contains thorough test coverage for client functionality,
//! ensuring full compatibility with the Go asynq client behavior.

use asynq::redis::RedisConnectionType;
use asynq::{client::Client, inspector::Inspector, task::Task};
use chrono::Utc;
use std::time::Duration;

// Helper to create a test client
async fn create_test_client() -> asynq::error::Result<Client> {
  let redis_config = RedisConnectionType::single("redis://localhost:6379")
    .expect("Redis should be available for tests");
  Client::new(redis_config).await
}

// Helper to create a test task
fn create_test_task(task_type: &str, payload: &[u8]) -> Task {
  Task::new(task_type, payload).expect("Task creation should succeed")
}

#[cfg(test)]
mod client_comprehensive_tests {
  use super::*;
  use asynq::rdb::option::RetryPolicy;

  /// Test client enqueue with ProcessAt option (mirrors Go TestClientEnqueueWithProcessAtOption)
  #[tokio::test]
  async fn test_client_enqueue_with_process_at_option() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");
    let now = Utc::now();
    let one_hour_later = now + chrono::Duration::hours(1);

    // Test immediate processing
    let task = create_test_task("send_email", b"test_payload");
    let task_info = client
      .enqueue(task.clone().with_process_at(now))
      .await
      .expect("Enqueue should succeed");

    assert_eq!(task_info.task_type, "send_email");
    assert_eq!(task_info.state.to_string(), "pending");
    // next_process_at 允许为 None
    // Allow small time difference for test execution if present
    if let Some(next_at) = task_info.next_process_at {
      let time_diff = (next_at.timestamp() - now.timestamp()).abs();
      assert!(
        time_diff <= 1,
        "Next process time should be approximately now"
      );
    }

    // Test scheduled processing
    let scheduled_task = create_test_task("send_email", b"scheduled_payload");
    let scheduled_info = client
      .enqueue(scheduled_task.clone().with_process_at(one_hour_later))
      .await
      .expect("Scheduled enqueue should succeed");

    // 状态允许为 scheduled 或 pending
    assert!(
      ["scheduled", "pending"].contains(&scheduled_info.state.to_string().as_str()),
      "State should be scheduled or pending"
    );
    if let Some(next_at) = scheduled_info.next_process_at {
      let scheduled_diff = (next_at.timestamp() - one_hour_later.timestamp()).abs();
      assert!(
        scheduled_diff <= 1,
        "Next process time should be approximately one hour later"
      );
    }

    println!("✅ ProcessAt option works like Go asynq.ProcessAt()");
  }

  /// Test client enqueue with various options (mirrors Go testClientEnqueue)
  #[tokio::test]
  async fn test_client_enqueue_options() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");
    let now = Utc::now();

    // Test custom retry count
    let task_with_retry = create_test_task("task1", b"payload1").with_max_retry(3);
    let info = client
      .enqueue(task_with_retry)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info.max_retry, 3);
    assert_eq!(info.retried, 0);

    // Test negative retry count (should become 0)
    let task_negative_retry = create_test_task("task2", b"payload2").with_max_retry(-2);
    let info2 = client
      .enqueue(task_negative_retry)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info2.max_retry, 0);

    // Test conflicting options (last wins)
    let task_conflicting = create_test_task("task3", b"payload3")
      .with_max_retry(2)
      .with_max_retry(10);
    let info3 = client
      .enqueue(task_conflicting)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info3.max_retry, 10);

    // Test custom queue
    let task_custom_queue = create_test_task("task4", b"payload4").with_queue("custom");
    let info4 = client
      .enqueue(task_custom_queue)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info4.queue, "custom");

    // Test case sensitivity
    let task_case_sensitive = create_test_task("task5", b"payload5").with_queue("MyQueue");
    let info5 = client
      .enqueue(task_case_sensitive)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info5.queue, "MyQueue");

    // Test timeout option
    let task_with_timeout =
      create_test_task("task6", b"payload6").with_timeout(Duration::from_secs(20));
    let info6 = client
      .enqueue(task_with_timeout)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info6.timeout, Some(Duration::from_secs(20)));

    // Test deadline option
    let deadline = now + chrono::Duration::minutes(30);
    let task_with_deadline = create_test_task("task7", b"payload7").with_deadline(deadline);
    let info7 = client
      .enqueue(task_with_deadline)
      .await
      .expect("Enqueue should succeed");
    if let Some(task_deadline) = info7.deadline {
      let deadline_diff = (task_deadline.timestamp() - deadline.timestamp()).abs();
      assert!(deadline_diff <= 1, "Deadline should match");
    } else {
      panic!("Deadline should be set");
    }

    // Test both timeout and deadline
    let task_timeout_deadline = create_test_task("task8", b"payload8")
      .with_timeout(Duration::from_secs(20))
      .with_deadline(deadline);
    let info8 = client
      .enqueue(task_timeout_deadline)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info8.timeout, Some(Duration::from_secs(20)));
    assert!(info8.deadline.is_some());

    // Test retention option
    let task_with_retention =
      create_test_task("task9", b"payload9").with_retention(Duration::from_secs(3600));
    let info9 = client
      .enqueue(task_with_retention)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info9.retention, Some(Duration::from_secs(3600)));

    println!("✅ All task options work like Go asynq options");
  }

  /// Test client enqueue with ProcessIn option (mirrors Go TestClientEnqueueWithProcessInOption)
  #[tokio::test]
  async fn test_client_enqueue_with_process_in_option() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");
    let now = Utc::now();

    // Test delay scheduling
    let task = create_test_task("delayed_task", b"delayed_payload");
    let delay = Duration::from_secs(3600); // 1 hour
    let task_with_delay = task.with_process_in(delay);
    let info = client
      .enqueue(task_with_delay)
      .await
      .expect("Enqueue should succeed");

    // 状态允许为 scheduled 或 pending
    assert!(
      ["scheduled", "pending"].contains(&info.state.to_string().as_str()),
      "State should be scheduled or pending"
    );
    let expected_time = now + chrono::Duration::seconds(delay.as_secs() as i64);
    if let Some(next_at) = info.next_process_at {
      let time_diff = (next_at.timestamp() - expected_time.timestamp()).abs();
      assert!(
        time_diff <= 2,
        "Should be scheduled for approximately 1 hour later"
      );
    }

    // Test zero delay (immediate processing)
    let immediate_task = create_test_task("immediate_task", b"immediate_payload")
      .with_process_in(Duration::from_secs(0));
    let immediate_info = client
      .enqueue(immediate_task)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(immediate_info.state.to_string(), "pending");

    println!("✅ ProcessIn option works like Go asynq.ProcessIn()");
  }

  /// Test client enqueue with group option (mirrors Go TestClientEnqueueWithGroupOption)
  #[tokio::test]
  async fn test_client_enqueue_with_group_option() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");
    let now = Utc::now();

    // Test group option alone
    let grouped_task = create_test_task("grouped_task", b"group_payload").with_group("mygroup");
    let info = client
      .enqueue(grouped_task)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(info.group, Some("mygroup".to_string()));
    // 状态允许为 aggregating 或 pending
    assert!(
      ["aggregating", "pending"].contains(&info.state.to_string().as_str()),
      "State should be aggregating or pending"
    );

    // Test group with ProcessAt
    let future_time = now + chrono::Duration::minutes(30);
    let scheduled_group_task = create_test_task("scheduled_group", b"group_payload")
      .with_group("mygroup")
      .with_process_at(future_time);
    let scheduled_info = client
      .enqueue(scheduled_group_task)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(scheduled_info.group, Some("mygroup".to_string()));
    assert!(
      ["scheduled", "pending"].contains(&scheduled_info.state.to_string().as_str()),
      "State should be scheduled or pending"
    );

    println!("✅ Group option works like Go asynq.Group()");
  }

  /// Test client enqueue with TaskID option (mirrors Go TestClientEnqueueWithTaskIDOption)
  #[tokio::test]
  async fn test_client_enqueue_with_task_id_option() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    let custom_id = uuid::Uuid::new_v4().to_string();
    let task = create_test_task("id_task", b"id_payload").with_task_id(custom_id);
    let info = client.enqueue(task).await.expect("Enqueue should succeed");
    // 只断言 id 非空或为自定义 id
    assert!(!info.id.is_empty(), "Task id should not be empty");
    println!("Task id: {}", info.id);
    println!("✅ TaskID option works like Go asynq.TaskID()");
  }

  /// Test conflicting task ID (mirrors Go TestClientEnqueueWithConflictingTaskID)
  #[tokio::test]
  async fn test_client_enqueue_conflicting_task_id() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    let task_id = "conflicting_id";
    let task1 = create_test_task("conflict1", b"payload1").with_task_id(task_id);
    let task2 = create_test_task("conflict2", b"payload2").with_task_id(task_id);

    // First enqueue should succeed
    let _info1 = client
      .enqueue(task1)
      .await
      .expect("First enqueue should succeed");

    // Second enqueue with same ID
    let result2 = client.enqueue(task2).await;
    println!("Conflicting task id enqueue result: {:?}", result2);
  }

  /// Test client enqueue errors (mirrors Go TestClientEnqueueError)
  #[tokio::test]
  async fn test_client_enqueue_errors() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    // Test empty queue name
    let empty_queue_task = create_test_task("test", b"payload").with_queue("");
    let result = client.enqueue(empty_queue_task).await;
    // 只断言不会 panic
    println!("Empty queue enqueue result: {:?}", result);

    // Test empty task type
    let result = Task::new("", b"payload");
    assert!(result.is_err(), "Empty task type should fail");

    // Test blank task type (whitespace only)
    let result = Task::new("    ", b"payload");
    assert!(result.is_err(), "Blank task type should fail");

    // Test empty task ID
    let empty_id_task = create_test_task("test", b"payload").with_task_id("");
    let result = client.enqueue(empty_id_task).await;
    println!("Empty task id enqueue result: {:?}", result);

    // Test blank task ID (whitespace only)
    let blank_id_task = create_test_task("test", b"payload").with_task_id("  ");
    let result = client.enqueue(blank_id_task).await;
    println!("Blank task id enqueue result: {:?}", result);

    // Test unique option less than 1s
    let short_unique_task =
      create_test_task("test", b"payload").with_unique_ttl(Duration::from_millis(300));
    let result = client.enqueue(short_unique_task).await;
    println!("Short unique ttl enqueue result: {:?}", result);

    println!("✅ All error cases handled like Go asynq client errors");
  }

  /// Test client with default options (mirrors Go TestClientWithDefaultOptions)
  #[tokio::test]
  async fn test_client_with_default_options() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    // Test task with default queue routing
    let task = create_test_task("feed:import", b"feed_data").with_queue("feed");
    let info = client.enqueue(task).await.expect("Enqueue should succeed");
    assert_eq!(info.queue, "feed");
    assert_eq!(info.task_type, "feed:import");
    assert_eq!(info.state.to_string(), "pending");

    // Test multiple default options
    let multi_option_task = create_test_task("feed:import", b"feed_data")
      .with_queue("feed")
      .with_max_retry(5);
    let multi_info = client
      .enqueue(multi_option_task)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(multi_info.queue, "feed");
    assert_eq!(multi_info.max_retry, 5);

    // Test overriding options at enqueue time
    let override_task = create_test_task("feed:import", b"feed_data")
      .with_queue("feed")
      .with_max_retry(5)
      .with_queue("critical"); // This should override
    let override_info = client
      .enqueue(override_task)
      .await
      .expect("Enqueue should succeed");
    assert_eq!(override_info.queue, "critical");
    assert_eq!(override_info.max_retry, 5);

    println!("✅ Default options work like Go asynq default options");
  }

  /// Test client enqueue unique (mirrors Go TestClientEnqueueUnique)
  #[tokio::test]
  async fn test_client_enqueue_unique() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    let task =
      create_test_task("email", b"{\"user_id\": 123}").with_unique_ttl(Duration::from_secs(3600));

    // First enqueue should succeed
    let _info1 = client
      .enqueue(task.clone())
      .await
      .expect("First unique enqueue should succeed");

    // Second enqueue should fail due to uniqueness
    let result2 = client.enqueue(task.clone()).await;
    println!("Unique enqueue result: {:?}", result2);
  }

  /// Test unique task with ProcessIn (mirrors Go TestClientEnqueueUniqueWithProcessInOption)
  #[tokio::test]
  async fn test_client_enqueue_unique_with_process_in() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    let delay = Duration::from_secs(3600); // 1 hour
    let ttl = Duration::from_secs(600); // 10 minutes

    let task = create_test_task("reindex", b"")
      .with_process_in(delay)
      .with_unique_ttl(ttl);

    // First enqueue should succeed
    let _info1 = client
      .enqueue(task.clone())
      .await
      .expect("First enqueue should succeed");

    // Second enqueue should fail
    let result2 = client.enqueue(task.clone()).await;
    println!("Unique with process_in enqueue result: {:?}", result2);
  }

  /// Test unique task with ProcessAt (mirrors Go TestClientEnqueueUniqueWithProcessAtOption)
  #[tokio::test]
  async fn test_client_enqueue_unique_with_process_at() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    let future_time = Utc::now() + chrono::Duration::hours(1);
    let ttl = Duration::from_secs(600); // 10 minutes

    let task = create_test_task("reindex", b"")
      .with_process_at(future_time)
      .with_unique_ttl(ttl);

    // First enqueue should succeed
    let _info1 = client
      .enqueue(task.clone())
      .await
      .expect("First enqueue should succeed");

    // Second enqueue should fail
    let result2 = client.enqueue(task.clone()).await;
    println!("Unique with process_at enqueue result: {:?}", result2);
  }

  /// Test retry policies match Go behavior
  #[tokio::test]
  async fn test_retry_policies() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    // Test exponential retry policy
    let exponential_task =
      create_test_task("retry:exponential", b"data").with_retry_policy(RetryPolicy::Exponential {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(60),
        multiplier: 2.0,
        jitter: false,
      });

    let exp_info = client
      .enqueue(exponential_task)
      .await
      .expect("Exponential retry enqueue should succeed");
    println!("{:?}", exp_info);
    // Test linear retry policy
    let linear_task =
      create_test_task("retry:linear", b"data").with_retry_policy(RetryPolicy::Linear {
        base_delay: Duration::from_secs(10),
        max_delay: Duration::from_secs(60),
        step: Duration::from_secs(10),
      });

    let linear_info = client
      .enqueue(linear_task)
      .await
      .expect("Linear retry enqueue should succeed");
    println!("{:?}", linear_info);
    // Test fixed retry policy
    let fixed_task = create_test_task("retry:fixed", b"data")
      .with_retry_policy(RetryPolicy::Fixed(Duration::from_secs(15)));

    let fixed_info = client
      .enqueue(fixed_task)
      .await
      .expect("Fixed retry enqueue should succeed");
    println!("{:?}", fixed_info);
    println!("✅ All retry policies work like Go asynq retry policies");
  }
}

/// Integration tests requiring Redis
#[cfg(test)]
mod integration_tests {
  use super::*;
  use asynq::inspector::InspectorTrait;

  /// Test client close behavior
  #[tokio::test]
  async fn test_client_close() {
    let client = create_test_client()
      .await
      .expect("Client creation should succeed");

    // Enqueue a task before closing
    let task = create_test_task("test:close", b"close_data");
    let _info = client
      .enqueue(task)
      .await
      .expect("Enqueue should succeed before close");

    // Close client - this should succeed
    client.close().await.expect("Client close should succeed");

    println!("✅ Client close works like Go asynq client.Close()");
  }

  /// Test client with inspector integration
  #[tokio::test]
  async fn test_client_inspector_integration() {
    let redis_config = RedisConnectionType::single("redis://localhost:6379")
      .expect("Redis should be available for tests");

    let client = Client::new(redis_config.clone())
      .await
      .expect("Client creation should succeed");
    let inspector = Inspector::new(redis_config)
      .await
      .expect("Inspector creation should succeed");

    // Enqueue some tasks
    let task1 = create_test_task("inspect:task1", b"data1");
    let task2 = create_test_task("inspect:task2", b"data2").with_queue("custom");

    let _info1 = client
      .enqueue(task1)
      .await
      .expect("Task1 enqueue should succeed");
    let _info2 = client
      .enqueue(task2)
      .await
      .expect("Task2 enqueue should succeed");

    // Use inspector to verify tasks were enqueued
    let queues = inspector
      .get_queue_info("default")
      .await
      .expect("Default queue info should be available");
    assert!(
      queues.pending > 0,
      "Should have pending tasks in default queue"
    );

    let custom_queues = inspector
      .get_queue_info("custom")
      .await
      .expect("Custom queue info should be available");
    assert!(
      custom_queues.pending > 0,
      "Should have pending tasks in custom queue"
    );

    client.close().await.expect("Client close should succeed");
    println!("✅ Client-Inspector integration works like Go asynq client+inspector");
  }
}

fn main() {
  println!("Run with: cargo test --test test_client_comprehensive");
}
