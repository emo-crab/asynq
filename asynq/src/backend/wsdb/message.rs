//! WebSocket message types for asynq client-server protocol
//!
//! This module mirrors the message protocol used by asynq-server.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request message from client to server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
  /// Enqueue a task for immediate processing
  #[serde(rename = "enqueue")]
  Enqueue(EnqueueRequest),

  /// Enqueue a unique task
  #[serde(rename = "enqueue_unique")]
  EnqueueUnique(EnqueueUniqueRequest),

  /// Schedule a task for later processing
  #[serde(rename = "schedule")]
  Schedule(ScheduleRequest),

  /// Schedule a unique task
  #[serde(rename = "schedule_unique")]
  ScheduleUnique(ScheduleUniqueRequest),

  /// Dequeue a task from specified queues
  #[serde(rename = "dequeue")]
  Dequeue(DequeueRequest),

  /// Mark a task as done
  #[serde(rename = "done")]
  Done(TaskDoneRequest),

  /// Mark a task as complete (with retention)
  #[serde(rename = "mark_complete")]
  MarkComplete(TaskDoneRequest),

  /// Retry a task
  #[serde(rename = "retry")]
  Retry(RetryRequest),

  /// Archive a task
  #[serde(rename = "archive")]
  Archive(ArchiveRequest),

  /// Ping the server
  #[serde(rename = "ping")]
  Ping,

  /// Subscribe to cancellation events
  #[serde(rename = "subscribe_cancellation")]
  SubscribeCancellation,

  /// Publish a cancellation
  #[serde(rename = "publish_cancellation")]
  PublishCancellation { task_id: String },

  /// Add a task to a group for aggregation
  #[serde(rename = "add_to_group")]
  AddToGroup(AddToGroupRequest),

  /// Add a unique task to a group for aggregation
  #[serde(rename = "add_to_group_unique")]
  AddToGroupUnique(AddToGroupUniqueRequest),

  /// List groups in a queue
  #[serde(rename = "list_groups")]
  ListGroups(ListGroupsRequest),

  /// Check if aggregation conditions are met
  #[serde(rename = "aggregation_check")]
  AggregationCheck(AggregationCheckRequest),

  /// Read tasks from an aggregation set
  #[serde(rename = "read_aggregation_set")]
  ReadAggregationSet(ReadAggregationSetRequest),

  /// Delete an aggregation set
  #[serde(rename = "delete_aggregation_set")]
  DeleteAggregationSet(DeleteAggregationSetRequest),
}

/// Response message from server to client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ServerMessage {
  /// Task info response
  #[serde(rename = "task_info")]
  TaskInfo(TaskInfoResponse),

  /// Dequeue response with optional task
  #[serde(rename = "dequeue_result")]
  DequeueResult(Option<TaskMessageResponse>),

  /// Success response
  #[serde(rename = "success")]
  Success,

  /// Pong response
  #[serde(rename = "pong")]
  Pong,

  /// Error response
  #[serde(rename = "error")]
  Error { message: String },

  /// Cancellation event
  #[serde(rename = "cancellation")]
  Cancellation { task_id: String },

  /// Groups list response
  #[serde(rename = "groups_list")]
  GroupsList(Vec<String>),

  /// Aggregation set ID response
  #[serde(rename = "aggregation_set_id")]
  AggregationSetId(Option<String>),

  /// Aggregation set (list of tasks) response
  #[serde(rename = "aggregation_set")]
  AggregationSet(Vec<TaskMessageResponse>),
}

/// Request to enqueue a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueRequest {
  /// Task type name
  pub task_type: String,
  /// Task payload (base64 encoded)
  pub payload: String,
  /// Task headers
  #[serde(default)]
  pub headers: HashMap<String, String>,
  /// Queue name (optional, defaults to "default")
  pub queue: Option<String>,
  /// Maximum retry attempts
  pub max_retry: Option<i32>,
  /// Task ID (optional, auto-generated if not provided)
  pub task_id: Option<String>,
}

/// Request to enqueue a unique task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueUniqueRequest {
  /// Base enqueue request
  #[serde(flatten)]
  pub enqueue: EnqueueRequest,
  /// TTL in seconds for uniqueness
  pub ttl_seconds: u64,
}

/// Request to schedule a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRequest {
  /// Base enqueue request
  #[serde(flatten)]
  pub enqueue: EnqueueRequest,
  /// Unix timestamp when to process the task
  pub process_at: i64,
}

/// Request to schedule a unique task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleUniqueRequest {
  /// Base schedule request
  #[serde(flatten)]
  pub schedule: ScheduleRequest,
  /// TTL in seconds for uniqueness
  pub ttl_seconds: u64,
}

/// Request to dequeue tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueRequest {
  /// Queue names to dequeue from
  pub queues: Vec<String>,
}

/// Request to mark a task as done or complete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDoneRequest {
  /// Task ID
  pub task_id: String,
  /// Queue name
  pub queue: String,
  /// Task type
  pub task_type: String,
  /// Task payload (base64 encoded)
  pub payload: String,
}

/// Request to retry a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryRequest {
  /// Task done request data
  #[serde(flatten)]
  pub task: TaskDoneRequest,
  /// Unix timestamp when to retry
  pub process_at: i64,
  /// Error message
  pub error_msg: String,
  /// Whether this is a failure
  pub is_failure: bool,
}

/// Request to archive a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveRequest {
  /// Task done request data
  #[serde(flatten)]
  pub task: TaskDoneRequest,
  /// Error message
  pub error_msg: String,
}

/// Request to add a task to a group for aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddToGroupRequest {
  /// Base enqueue request
  #[serde(flatten)]
  pub enqueue: EnqueueRequest,
  /// Group name
  pub group: String,
}

/// Request to add a unique task to a group for aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddToGroupUniqueRequest {
  /// Base enqueue request
  #[serde(flatten)]
  pub enqueue: EnqueueRequest,
  /// Group name
  pub group: String,
  /// TTL in seconds for uniqueness
  pub ttl_seconds: u64,
}

/// Request to list groups in a queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListGroupsRequest {
  /// Queue name
  pub queue: String,
}

/// Request to check aggregation conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationCheckRequest {
  /// Queue name
  pub queue: String,
  /// Group name
  pub group: String,
  /// Aggregation delay in seconds
  pub aggregation_delay_seconds: u64,
  /// Maximum delay in seconds
  pub max_delay_seconds: u64,
  /// Maximum size of aggregation
  pub max_size: usize,
}

/// Request to read tasks from an aggregation set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadAggregationSetRequest {
  /// Queue name
  pub queue: String,
  /// Group name
  pub group: String,
  /// Set ID
  pub set_id: String,
}

/// Request to delete an aggregation set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteAggregationSetRequest {
  /// Queue name
  pub queue: String,
  /// Group name
  pub group: String,
  /// Set ID
  pub set_id: String,
}

/// Task info response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfoResponse {
  /// Task ID
  pub id: String,
  /// Queue name
  pub queue: String,
  /// Task type
  pub task_type: String,
  /// Task state
  pub state: String,
}

/// Task message response (for dequeue)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMessageResponse {
  /// Task ID
  pub id: String,
  /// Queue name
  pub queue: String,
  /// Task type name
  pub task_type: String,
  /// Task payload (base64 encoded)
  pub payload: String,
  /// Task headers
  pub headers: HashMap<String, String>,
  /// Maximum retry attempts
  pub retry: i32,
  /// Number of times retried
  pub retried: i32,
  /// Error message from last failure
  pub error_msg: String,
  /// Timeout in seconds
  pub timeout: i64,
  /// Deadline timestamp
  pub deadline: i64,
  /// Group key
  pub group_key: String,
}
