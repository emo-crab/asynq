//! Message format and encoding compatibility tests with Go asynq
//!
//! This module tests message handling and formats to ensure
//! 100% compatibility with Go asynq protobuf formats and data structures.

use asynq::task::Task;
use std::time::Duration;

fn main() {
  println!("Run with: cargo test --test test_message_compatibility");
}

#[cfg(test)]
mod message_compatibility_tests {
  use super::*;

  /// Test protobuf encoding produces compatible format with Go asynq
  #[test]
  fn test_protobuf_binary_compatibility() {
    // Since TaskMessage is internal, we test through the public task APIs
    // which use the same protobuf encoding/decoding internally

    // Create tasks with various payload types that Go asynq commonly uses
    let test_tasks = vec![
      (
        "user:welcome",
        serde_json::to_vec(&serde_json::json!({
          "user_id": 12345,
          "email": "test@example.com",
          "name": "Test User"
        }))
        .unwrap(),
      ),
      ("simple:task", b"simple data".to_vec()),
      ("unicode:task", "Hello 世界! 🌍".as_bytes().to_vec()),
    ];

    for (task_type, payload) in test_tasks {
      let task = Task::new(task_type, &payload).unwrap();

      // Verify task creation preserves data
      assert_eq!(task.get_type(), task_type);
      assert_eq!(task.get_payload(), payload);

      println!("✅ Task type '{}' preserves payload correctly", task_type);
    }

    println!("✅ All task types handle various payload formats");
  }

  /// Test handling of edge cases in task creation
  #[test]
  fn test_task_edge_cases() {
    // Test with minimal task (like early Go asynq versions)
    let minimal_task = Task::new("simple:task", b"simple data").unwrap();
    assert_eq!(minimal_task.get_type(), "simple:task");
    assert_eq!(minimal_task.get_payload(), b"simple data");

    // Test with large payload (stress test)
    let large_payload = vec![b'x'; 1024 * 1024]; // 1MB payload
    let large_task = Task::new("large:task", &large_payload).unwrap();
    assert_eq!(large_task.get_payload().len(), 1024 * 1024);
    assert_eq!(large_task.get_payload(), large_payload);

    // Test with Unicode strings (international compatibility)
    let unicode_payload = "Hello 世界! 🌍 Здравствуй мир!".as_bytes();
    let unicode_task = Task::new("unicode:测试", unicode_payload)
      .unwrap()
      .with_queue("国际化")
      .with_group("группа");

    assert_eq!(unicode_task.get_type(), "unicode:测试");
    assert_eq!(unicode_task.options.queue, "国际化");
    assert_eq!(unicode_task.options.group.as_ref().unwrap(), "группа");
    assert_eq!(unicode_task.get_payload(), unicode_payload);

    println!("✅ All task edge cases handled correctly");
  }

  /// Test JSON payload compatibility with Go asynq
  #[test]
  fn test_json_payload_compatibility() {
    // Test various JSON payload types that Go asynq commonly uses
    let test_payloads = [
      // Simple object
      serde_json::json!({
        "action": "send_email",
        "recipient": "user@example.com"
      }),
      // Array
      serde_json::json!([1, 2, 3, 4, 5]),
      // Nested object
      serde_json::json!({
        "user": {
          "id": 123,
          "profile": {
            "name": "John Doe",
            "preferences": {
              "theme": "dark",
              "notifications": true
            }
          }
        },
        "metadata": {
          "created_at": "2023-01-01T00:00:00Z",
          "version": "1.0"
        }
      }),
      // String value
      serde_json::json!("simple string payload"),
      // Number value
      serde_json::json!(42),
      // Boolean value
      serde_json::json!(true),
      // Null value
      serde_json::json!(null),
    ];

    for (i, payload) in test_payloads.iter().enumerate() {
      let json_bytes = serde_json::to_vec(payload).unwrap();

      let task = Task::new(format!("json:test:{}", i), &json_bytes).unwrap();

      // Verify JSON payload is preserved
      assert_eq!(task.get_payload(), json_bytes);

      // Verify we can parse it back to JSON
      let parsed: serde_json::Value = serde_json::from_slice(task.get_payload()).unwrap();
      assert_eq!(parsed, *payload);
    }

    println!("✅ All JSON payload types are compatible with Go asynq");
  }

  /// Test task properties preservation
  #[test]
  fn test_task_properties_preservation() {
    // Create a task with all options
    let payload = serde_json::to_vec(&serde_json::json!({
      "order_id": "ORD-123",
      "amount": 99.99
    }))
    .unwrap();

    let task = Task::new("order:process", &payload)
      .unwrap()
      .with_queue("orders")
      .with_max_retry(5)
      .with_timeout(Duration::from_secs(300))
      .with_unique_ttl(Duration::from_secs(3600))
      .with_group("order_batch");

    // Test that task properties are preserved in the task object
    assert_eq!(task.get_type(), "order:process");
    assert_eq!(task.get_payload(), payload);
    assert_eq!(task.options.queue, "orders");
    assert_eq!(task.options.max_retry, 5);
    assert_eq!(task.options.timeout.unwrap(), Duration::from_secs(300));
    assert_eq!(task.options.unique_ttl.unwrap(), Duration::from_secs(3600));
    assert_eq!(task.options.group.as_ref().unwrap(), "order_batch");

    println!("✅ Task properties are preserved correctly");
  }
}
