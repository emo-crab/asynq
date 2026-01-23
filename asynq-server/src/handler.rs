//! WebSocket message handlers
//!
//! This module contains the logic for handling client messages and
//! converting between the wire protocol and the broker API.

use crate::config::TenantConfig;
use crate::error::{Error, Result};
use crate::message::{
  ArchiveRequest, ClientMessage, DequeueRequest, EnqueueRequest, EnqueueUniqueRequest,
  RetryRequest, ScheduleRequest, ScheduleUniqueRequest, ServerMessage, TaskDoneRequest,
  TaskInfoResponse, TaskMessageResponse,
};
use crate::server::CURRENT_TENANT;
use asynq::base::Broker;
use asynq::proto::TaskMessage;
use asynq::task::Task;
use base64::prelude::*;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;

/// Handler for processing client messages
pub struct MessageHandler {
  broker: Arc<dyn Broker>,
}

impl MessageHandler {
  /// Create a new message handler
  pub fn new(broker: Arc<dyn Broker>) -> Self {
    Self { broker }
  }

  /// Get a reference to the broker
  pub fn broker(&self) -> &Arc<dyn Broker> {
    &self.broker
  }

  /// Apply tenant prefix to a queue name if tenant is configured
  fn apply_tenant_prefix(queue: &str) -> String {
    if let Some(tenant) = Self::get_current_tenant() {
      tenant.get_tenant_queue(queue)
    } else {
      queue.to_string()
    }
  }

  /// Get current tenant from task-local variable
  fn get_current_tenant() -> Option<TenantConfig> {
    CURRENT_TENANT.try_with(|t| t.clone()).ok().flatten()
  }

  /// Handle a client message and return a server response
  ///
  /// The tenant configuration is retrieved from the CURRENT_TENANT task-local variable
  /// which is set by the authentication middleware. Queue names will be prefixed with
  /// the tenant ID if a tenant is configured.
  pub async fn handle(
    &self,
    msg: ClientMessage,
  ) -> Result<ServerMessage> {
    match msg {
      ClientMessage::Enqueue(req) => self.handle_enqueue(req).await,
      ClientMessage::EnqueueUnique(req) => self.handle_enqueue_unique(req).await,
      ClientMessage::Schedule(req) => self.handle_schedule(req).await,
      ClientMessage::ScheduleUnique(req) => self.handle_schedule_unique(req).await,
      ClientMessage::Dequeue(req) => self.handle_dequeue(req).await,
      ClientMessage::Done(req) => self.handle_done(req).await,
      ClientMessage::MarkComplete(req) => self.handle_mark_complete(req).await,
      ClientMessage::Retry(req) => self.handle_retry(req).await,
      ClientMessage::Archive(req) => self.handle_archive(req).await,
      ClientMessage::Ping => Ok(ServerMessage::Pong),
      ClientMessage::SubscribeCancellation => {
        // Subscription is handled at the WebSocket level
        Ok(ServerMessage::Success)
      }
      ClientMessage::PublishCancellation { task_id} => {
        self.handle_publish_cancellation(&task_id).await
      }
    }
  }

  /// Handle enqueue request
  async fn handle_enqueue(
    &self,
    req: EnqueueRequest,
  ) -> Result<ServerMessage> {
    let task = self.create_task_from_request(&req)?;
    let info = self.broker.enqueue(&task).await?;
    Ok(ServerMessage::TaskInfo(TaskInfoResponse {
      id: info.id,
      queue: info.queue,
      task_type: info.task_type,
      state: info.state.as_str().to_string(),
    }))
  }

  /// Handle enqueue unique request
  async fn handle_enqueue_unique(
    &self,
    req: EnqueueUniqueRequest,
  ) -> Result<ServerMessage> {
    let task = self.create_task_from_request(&req.enqueue)?;
    let ttl = Duration::from_secs(req.ttl_seconds);
    let info = self.broker.enqueue_unique(&task, ttl).await?;
    Ok(ServerMessage::TaskInfo(TaskInfoResponse {
      id: info.id,
      queue: info.queue,
      task_type: info.task_type,
      state: info.state.as_str().to_string(),
    }))
  }

  /// Handle schedule request
  async fn handle_schedule(
    &self,
    req: ScheduleRequest,
  ) -> Result<ServerMessage> {
    let task = self.create_task_from_request(&req.enqueue)?;
    let process_at = DateTime::from_timestamp(req.process_at, 0)
      .unwrap_or_else(Utc::now);
    let info = self.broker.schedule(&task, process_at).await?;
    Ok(ServerMessage::TaskInfo(TaskInfoResponse {
      id: info.id,
      queue: info.queue,
      task_type: info.task_type,
      state: info.state.as_str().to_string(),
    }))
  }

  /// Handle schedule unique request
  async fn handle_schedule_unique(
    &self,
    req: ScheduleUniqueRequest,
  ) -> Result<ServerMessage> {
    let task = self.create_task_from_request(&req.schedule.enqueue)?;
    let process_at = DateTime::from_timestamp(req.schedule.process_at, 0)
      .unwrap_or_else(Utc::now);
    let ttl = Duration::from_secs(req.ttl_seconds);
    let info = self.broker.schedule_unique(&task, process_at, ttl).await?;
    Ok(ServerMessage::TaskInfo(TaskInfoResponse {
      id: info.id,
      queue: info.queue,
      task_type: info.task_type,
      state: info.state.as_str().to_string(),
    }))
  }

  /// Handle dequeue request
  async fn handle_dequeue(
    &self,
    req: DequeueRequest,
  ) -> Result<ServerMessage> {
    // Apply tenant prefix to all queues
    let queues: Vec<String> = req
      .queues
      .iter()
      .map(|q| Self::apply_tenant_prefix(q))
      .collect();
    let result = self.broker.dequeue(&queues).await?;
    let response = result.map(|msg| self.task_message_to_response(&msg));
    Ok(ServerMessage::DequeueResult(response))
  }

  /// Handle task done request
  async fn handle_done(
    &self,
    req: TaskDoneRequest,
  ) -> Result<ServerMessage> {
    let msg = self.create_task_message_from_request(&req)?;
    self.broker.done(&msg).await?;
    Ok(ServerMessage::Success)
  }

  /// Handle mark complete request
  async fn handle_mark_complete(
    &self,
    req: TaskDoneRequest,
  ) -> Result<ServerMessage> {
    let msg = self.create_task_message_from_request(&req)?;
    self.broker.mark_as_complete(&msg).await?;
    Ok(ServerMessage::Success)
  }

  /// Handle retry request
  async fn handle_retry(
    &self,
    req: RetryRequest,
  ) -> Result<ServerMessage> {
    let msg = self.create_task_message_from_request(&req.task)?;
    let process_at = DateTime::from_timestamp(req.process_at, 0)
      .unwrap_or_else(Utc::now);
    self
      .broker
      .retry(&msg, process_at, &req.error_msg, req.is_failure)
      .await?;
    Ok(ServerMessage::Success)
  }

  /// Handle archive request
  async fn handle_archive(
    &self,
    req: ArchiveRequest,
  ) -> Result<ServerMessage> {
    let msg = self.create_task_message_from_request(&req.task)?;
    self.broker.archive(&msg, &req.error_msg).await?;
    Ok(ServerMessage::Success)
  }

  /// Handle publish cancellation request
  async fn handle_publish_cancellation(&self, task_id: &str) -> Result<ServerMessage> {
    self.broker.publish_cancellation(task_id).await?;
    Ok(ServerMessage::Success)
  }

  /// Create a Task from an enqueue request
  fn create_task_from_request(
    &self,
    req: &EnqueueRequest,
  ) -> Result<Task> {
    let payload = BASE64_STANDARD
      .decode(&req.payload)
      .map_err(|e| Error::invalid_message(format!("Invalid base64 payload: {}", e)))?;

    let mut task = Task::new(&req.task_type, &payload)
      .map_err(|e| Error::invalid_message(format!("Invalid task: {}", e)))?;

    // Apply tenant prefix to queue name
    if let Some(ref queue) = req.queue {
      let tenant_queue = Self::apply_tenant_prefix(queue);
      task = task.with_queue(&tenant_queue);
    } else if let Some(tenant) = Self::get_current_tenant() {
      // If no queue specified, use tenant-prefixed default queue
      task = task.with_queue(tenant.get_tenant_queue("default"));
    }

    if let Some(max_retry) = req.max_retry {
      task = task.with_max_retry(max_retry);
    }
    if let Some(ref task_id) = req.task_id {
      task = task.with_task_id(task_id);
    }

    // Add headers
    for (key, value) in &req.headers {
      task.headers.insert(key.clone(), value.clone());
    }

    Ok(task)
  }

  /// Create a TaskMessage from a done request
  fn create_task_message_from_request(
    &self,
    req: &TaskDoneRequest,
  ) -> Result<TaskMessage> {
    let payload = BASE64_STANDARD
      .decode(&req.payload)
      .map_err(|e| Error::invalid_message(format!("Invalid base64 payload: {}", e)))?;

    // Apply tenant prefix to queue name
    let queue = Self::apply_tenant_prefix(&req.queue);

    Ok(TaskMessage {
      id: req.task_id.clone(),
      queue,
      r#type: req.task_type.clone(),
      payload,
      ..Default::default()
    })
  }

  /// Convert a TaskMessage to a response
  fn task_message_to_response(&self, msg: &TaskMessage) -> TaskMessageResponse {
    TaskMessageResponse {
      id: msg.id.clone(),
      queue: msg.queue.clone(),
      task_type: msg.r#type.clone(),
      payload: BASE64_STANDARD.encode(&msg.payload),
      headers: msg.headers.clone(),
      retry: msg.retry,
      retried: msg.retried,
      error_msg: msg.error_msg.clone(),
      timeout: msg.timeout,
      deadline: msg.deadline,
      group_key: msg.group_key.clone(),
    }
  }
}
