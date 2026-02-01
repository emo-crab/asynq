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

  /// List tasks with expired leases
  #[serde(rename = "list_lease_expired")]
  ListLeaseExpired(ListLeaseExpiredRequest),

  /// Extend lease for a task
  #[serde(rename = "extend_lease")]
  ExtendLease(ExtendLeaseRequest),

  /// Write server state
  #[serde(rename = "write_server_state")]
  WriteServerState(WriteServerStateRequest),

  /// Clear server state
  #[serde(rename = "clear_server_state")]
  ClearServerState(ClearServerStateRequest),

  /// Write task result
  #[serde(rename = "write_result")]
  WriteResult(WriteResultRequest),
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

  /// Lease expired tasks response
  #[serde(rename = "lease_expired_tasks")]
  LeaseExpiredTasks(Vec<TaskMessageResponse>),
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

/// Request to list tasks with expired leases
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListLeaseExpiredRequest {
  /// Cutoff timestamp (Unix timestamp)
  pub cutoff: i64,
  /// Queue names to check
  pub queues: Vec<String>,
}

/// Request to extend lease for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendLeaseRequest {
  /// Queue name
  pub queue: String,
  /// Task ID
  pub task_id: String,
  /// Lease duration in seconds
  pub lease_duration_seconds: u64,
}

/// Request to write server state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteServerStateRequest {
  /// Server host
  pub host: String,
  /// Server process ID
  pub pid: i32,
  /// Server ID
  pub server_id: String,
  /// Concurrency level
  pub concurrency: i32,
  /// Queues configuration (queue name -> priority)
  pub queues: HashMap<String, i32>,
  /// Whether to use strict priority
  pub strict_priority: bool,
  /// Server status (active, stopped)
  pub status: String,
  /// Number of active workers
  pub active_worker_count: i32,
  /// TTL in seconds
  pub ttl_seconds: u64,
  /// Worker information
  pub workers: Vec<WorkerInfoData>,
}

/// Worker info data for write_server_state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfoData {
  /// Worker host
  pub host: String,
  /// Worker process ID
  pub pid: i32,
  /// Server ID
  pub server_id: String,
  /// Task ID being processed
  pub task_id: String,
  /// Task type being processed
  pub task_type: String,
  /// Task payload (base64 encoded)
  pub task_payload: String,
  /// Queue name
  pub queue: String,
}

/// Request to clear server state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearServerStateRequest {
  /// Server host
  pub host: String,
  /// Server process ID
  pub pid: i32,
  /// Server ID
  pub server_id: String,
}

/// Request to write task result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResultRequest {
  /// Queue name
  pub queue: String,
  /// Task ID
  pub task_id: String,
  /// Result data (base64 encoded)
  pub result: String,
}
