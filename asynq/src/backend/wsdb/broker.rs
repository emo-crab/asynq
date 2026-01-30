//! WebSocket Broker trait implementation
//!
//! Implements the Broker trait for WebSocketBroker.

use crate::backend::wsdb::message::{
  AddToGroupRequest, AddToGroupUniqueRequest, AggregationCheckRequest, ArchiveRequest,
  ClientMessage, DeleteAggregationSetRequest, DequeueRequest, EnqueueUniqueRequest,
  ListGroupsRequest, ReadAggregationSetRequest, RetryRequest, ScheduleRequest,
  ScheduleUniqueRequest, ServerMessage,
};
use crate::backend::wsdb::{ws_broker::CLOSE_FRAME_TIMEOUT_MS, WebSocketBroker};
use crate::base::Broker;
use crate::error::{Error, Result};
use crate::proto::{ServerInfo, TaskMessage};
use crate::task::{Task, TaskInfo};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[async_trait]
impl Broker for WebSocketBroker {
  /// Ping the server
  async fn ping(&self) -> Result<()> {
    let resp = self.send_and_receive(ClientMessage::Ping).await?;
    match resp {
      ServerMessage::Pong => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Close the connection
  async fn close(&self) -> Result<()> {
    let mut conn = self.connection.write().await;
    if let Some(mut ws_conn) = conn.take() {
      // Signal shutdown to send Close frame
      if let Some(shutdown_tx) = ws_conn.shutdown_tx.take() {
        let _ = shutdown_tx.send(());
      }
      // Give time for Close frame to be sent and acknowledged
      tokio::time::sleep(tokio::time::Duration::from_millis(CLOSE_FRAME_TIMEOUT_MS)).await;
    }
    Ok(())
  }

  /// Enqueue a task
  async fn enqueue(&self, task: &Task) -> Result<TaskInfo> {
    let req = self.task_to_enqueue_request(task);
    let resp = self.send_and_receive(ClientMessage::Enqueue(req)).await?;
    self.handle_task_info_response(resp)
  }

  /// Enqueue a unique task
  async fn enqueue_unique(&self, task: &Task, ttl: Duration) -> Result<TaskInfo> {
    let req = EnqueueUniqueRequest {
      enqueue: self.task_to_enqueue_request(task),
      ttl_seconds: ttl.as_secs(),
    };
    let resp = self
      .send_and_receive(ClientMessage::EnqueueUnique(req))
      .await?;
    self.handle_task_info_response(resp)
  }

  /// Dequeue a task
  async fn dequeue(&self, queues: &[String]) -> Result<Option<TaskMessage>> {
    let req = DequeueRequest {
      queues: queues.to_vec(),
    };
    let resp = self.send_and_receive(ClientMessage::Dequeue(req)).await?;
    match resp {
      ServerMessage::DequeueResult(Some(task_resp)) => {
        Ok(Some(self.response_to_task_message(&task_resp)?))
      }
      ServerMessage::DequeueResult(None) => Ok(None),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Mark a task as done
  async fn done(&self, msg: &TaskMessage) -> Result<()> {
    let req = self.task_message_to_done_request(msg);
    let resp = self.send_and_receive(ClientMessage::Done(req)).await?;
    match resp {
      ServerMessage::Success => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Mark a task as complete
  async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()> {
    let req = self.task_message_to_done_request(msg);
    let resp = self
      .send_and_receive(ClientMessage::MarkComplete(req))
      .await?;
    match resp {
      ServerMessage::Success => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Requeue a task
  async fn requeue(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
  ) -> Result<()> {
    self
      .retry(msg, process_at, error_msg, !error_msg.is_empty())
      .await
  }

  /// Schedule a task
  async fn schedule(&self, task: &Task, process_at: DateTime<Utc>) -> Result<TaskInfo> {
    let req = ScheduleRequest {
      enqueue: self.task_to_enqueue_request(task),
      process_at: process_at.timestamp(),
    };
    let resp = self.send_and_receive(ClientMessage::Schedule(req)).await?;
    self.handle_task_info_response(resp)
  }

  /// Schedule a unique task
  async fn schedule_unique(
    &self,
    task: &Task,
    process_at: DateTime<Utc>,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let req = ScheduleUniqueRequest {
      schedule: ScheduleRequest {
        enqueue: self.task_to_enqueue_request(task),
        process_at: process_at.timestamp(),
      },
      ttl_seconds: ttl.as_secs(),
    };
    let resp = self
      .send_and_receive(ClientMessage::ScheduleUnique(req))
      .await?;
    self.handle_task_info_response(resp)
  }

  /// Retry a task
  async fn retry(
    &self,
    msg: &TaskMessage,
    process_at: DateTime<Utc>,
    error_msg: &str,
    is_failure: bool,
  ) -> Result<()> {
    let req = RetryRequest {
      task: self.task_message_to_done_request(msg),
      process_at: process_at.timestamp(),
      error_msg: error_msg.to_string(),
      is_failure,
    };
    let resp = self.send_and_receive(ClientMessage::Retry(req)).await?;
    match resp {
      ServerMessage::Success => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Archive a task
  async fn archive(&self, msg: &TaskMessage, error_msg: &str) -> Result<()> {
    let req = ArchiveRequest {
      task: self.task_message_to_done_request(msg),
      error_msg: error_msg.to_string(),
    };
    let resp = self.send_and_receive(ClientMessage::Archive(req)).await?;
    match resp {
      ServerMessage::Success => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Forward ready tasks - Not directly supported via WebSocket
  async fn forward_if_ready(&self, _queues: &[String]) -> Result<i64> {
    // Server handles this internally
    Ok(0)
  }

  /// Add a task to a group for aggregation
  async fn add_to_group(&self, task: &Task, group: &str) -> Result<TaskInfo> {
    let req = AddToGroupRequest {
      enqueue: self.task_to_enqueue_request(task),
      group: group.to_string(),
    };
    let resp = self
      .send_and_receive(ClientMessage::AddToGroup(req))
      .await?;
    self.handle_task_info_response(resp)
  }

  /// Add a unique task to a group for aggregation
  async fn add_to_group_unique(
    &self,
    task: &Task,
    group: &str,
    ttl: Duration,
  ) -> Result<TaskInfo> {
    let req = AddToGroupUniqueRequest {
      enqueue: self.task_to_enqueue_request(task),
      group: group.to_string(),
      ttl_seconds: ttl.as_secs(),
    };
    let resp = self
      .send_and_receive(ClientMessage::AddToGroupUnique(req))
      .await?;
    self.handle_task_info_response(resp)
  }

  /// List groups in a queue
  async fn list_groups(&self, queue: &str) -> Result<Vec<String>> {
    let req = ListGroupsRequest {
      queue: queue.to_string(),
    };
    let resp = self
      .send_and_receive(ClientMessage::ListGroups(req))
      .await?;
    match resp {
      ServerMessage::GroupsList(groups) => Ok(groups),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Check if aggregation conditions are met
  async fn aggregation_check(
    &self,
    queue: &str,
    group: &str,
    aggregation_delay: Duration,
    max_delay: Duration,
    max_size: usize,
  ) -> Result<Option<String>> {
    let req = AggregationCheckRequest {
      queue: queue.to_string(),
      group: group.to_string(),
      aggregation_delay_seconds: aggregation_delay.as_secs(),
      max_delay_seconds: max_delay.as_secs(),
      max_size,
    };
    let resp = self
      .send_and_receive(ClientMessage::AggregationCheck(req))
      .await?;
    match resp {
      ServerMessage::AggregationSetId(set_id) => Ok(set_id),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Read tasks from an aggregation set
  async fn read_aggregation_set(
    &self,
    queue: &str,
    group: &str,
    set_id: &str,
  ) -> Result<Vec<TaskMessage>> {
    let req = ReadAggregationSetRequest {
      queue: queue.to_string(),
      group: group.to_string(),
      set_id: set_id.to_string(),
    };
    let resp = self
      .send_and_receive(ClientMessage::ReadAggregationSet(req))
      .await?;
    match resp {
      ServerMessage::AggregationSet(tasks) => {
        let mut result = Vec::new();
        for task_resp in tasks {
          result.push(self.response_to_task_message(&task_resp)?);
        }
        Ok(result)
      }
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Delete an aggregation set
  async fn delete_aggregation_set(&self, queue: &str, group: &str, set_id: &str) -> Result<()> {
    let req = DeleteAggregationSetRequest {
      queue: queue.to_string(),
      group: group.to_string(),
      set_id: set_id.to_string(),
    };
    let resp = self
      .send_and_receive(ClientMessage::DeleteAggregationSet(req))
      .await?;
    match resp {
      ServerMessage::Success => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Reclaim stale aggregation sets - Server handles this
  async fn reclaim_stale_aggregation_sets(&self, _queue: &str) -> Result<()> {
    Ok(())
  }

  /// Delete expired completed tasks - Server handles this
  async fn delete_expired_completed_tasks(&self, _queue: &str) -> Result<i64> {
    Ok(0)
  }

  /// List lease expired tasks - Not yet implemented
  async fn list_lease_expired(
    &self,
    _cutoff: DateTime<Utc>,
    _queues: &[String],
  ) -> Result<Vec<TaskMessage>> {
    Ok(Vec::new())
  }

  /// Extend lease - Server handles this
  async fn extend_lease(
    &self,
    _queue: &str,
    _task_id: &str,
    _lease_duration: Duration,
  ) -> Result<()> {
    Ok(())
  }

  /// Write server state - Not applicable for WebSocket client
  async fn write_server_state(&self, _server_info: &ServerInfo, _ttl: Duration) -> Result<()> {
    Ok(())
  }

  /// Clear server state - Not applicable for WebSocket client
  async fn clear_server_state(&self, _host: &str, _pid: i32, _server_id: &str) -> Result<()> {
    Ok(())
  }

  /// Subscribe to cancellation events
  async fn cancellation_pub_sub(
    &self,
  ) -> Result<Box<dyn futures::Stream<Item = Result<String>> + Unpin + Send>> {
    use futures::stream::unfold;

    // Subscribe to cancellation channel
    let receiver = self.cancel_tx.subscribe();

    // Send subscription message to server
    let _ = self
      .send_and_receive(ClientMessage::SubscribeCancellation)
      .await;

    // Create a stream from the broadcast receiver
    let stream = unfold(receiver, |mut rx| async move {
      loop {
        match rx.recv().await {
          Ok(task_id) => return Some((Ok(task_id), rx)),
          Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
          Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
        }
      }
    });

    Ok(Box::new(Box::pin(stream)))
  }

  /// Publish cancellation
  async fn publish_cancellation(&self, task_id: &str) -> Result<()> {
    let resp = self
      .send_and_receive(ClientMessage::PublishCancellation {
        task_id: task_id.to_string(),
      })
      .await?;
    match resp {
      ServerMessage::Success => Ok(()),
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Write result - Not yet implemented
  async fn write_result(&self, _queue: &str, _task_id: &str, _result: &[u8]) -> Result<()> {
    Ok(())
  }
}

/// WebSocketBroker 实现 SchedulerBroker trait
/// WebSocketBroker implements the SchedulerBroker trait
///
/// 注意：WebSocket 后端的调度器功能可能受限，因为它通常连接到远程服务器
/// Note: Scheduler functionality for WebSocket backend may be limited as it typically connects to a remote server
#[async_trait]
impl crate::base::SchedulerBroker for WebSocketBroker {
  /// 批量写入 scheduler entries
  /// Batch write scheduler entries
  async fn write_scheduler_entries(
    &self,
    _entries: &[crate::proto::SchedulerEntry],
    _scheduler_id: &str,
    _ttl_secs: u64,
  ) -> Result<()> {
    // WebSocket 后端通常不支持调度器持久化
    // WebSocket backend typically doesn't support scheduler persistence
    // 这些功能由服务器端处理
    // These features are handled by the server side
    Ok(())
  }

  /// 记录调度事件
  /// Record scheduling event
  async fn record_scheduler_enqueue_event(
    &self,
    _event: &crate::proto::SchedulerEnqueueEvent,
    _entry_id: &str,
  ) -> Result<()> {
    // WebSocket 后端通常不支持调度器事件记录
    // WebSocket backend typically doesn't support scheduler event recording
    Ok(())
  }

  /// 获取所有 SchedulerEntry
  /// Get all SchedulerEntry
  async fn scheduler_entries_script(
    &self,
    _scheduler_id: &str,
  ) -> Result<std::collections::HashMap<String, Vec<u8>>> {
    // 返回空的结果
    // Return empty result
    Ok(std::collections::HashMap::new())
  }

  /// 获取调度事件列表
  /// Get scheduling event list
  async fn scheduler_events_script(&self, _count: usize) -> Result<Vec<Vec<u8>>> {
    // 返回空的结果
    // Return empty result
    Ok(Vec::new())
  }

  /// 删除 scheduler entries 数据
  /// Delete scheduler entries data
  async fn clear_scheduler_entries(&self, _scheduler_id: &str) -> Result<()> {
    // WebSocket 后端通常不需要清理
    // WebSocket backend typically doesn't need cleanup
    Ok(())
  }
}
