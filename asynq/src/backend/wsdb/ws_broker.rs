//! WebSocket Broker implementation
//!
//! A broker that connects to an asynq-server via WebSocket for cross-process communication.

use crate::backend::wsdb::message::{
  ClientMessage, EnqueueRequest, ServerMessage, TaskDoneRequest, TaskMessageResponse,
};
use crate::base::keys::TaskState;
use crate::error::{Error, Result};
use crate::proto::TaskMessage;
use crate::task::{Task, TaskInfo};
use base64::prelude::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio_tungstenite::{
  connect_async,
  tungstenite::{client::IntoClientRequest, Message},
  MaybeTlsStream, WebSocketStream,
};
use tracing::{error, warn};

/// Timeout duration for WebSocket close frame to be sent and acknowledged
pub(crate) const CLOSE_FRAME_TIMEOUT_MS: u64 = 100;

#[allow(dead_code)]
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// WebSocket Broker
///
/// A broker implementation that connects to an asynq-server via WebSocket.
/// This enables cross-process task queue communication without Redis or PostgreSQL.
pub struct WebSocketBroker {
  /// WebSocket URL
  url: String,
  /// WebSocket connection
  pub(crate) connection: Arc<RwLock<Option<WsConnection>>>,
  /// Cancellation broadcast sender
  pub(crate) cancel_tx: broadcast::Sender<String>,
  /// Optional HTTP Basic authentication credentials (username, password)
  basic_auth: Option<(String, String)>,
}

/// WebSocket connection wrapper
pub(crate) struct WsConnection {
  /// Sender for outgoing messages
  pub(crate) sender: mpsc::Sender<ClientMessage>,
  /// Receiver for responses (request-response pattern)
  pub(crate) response_rx: Arc<Mutex<mpsc::Receiver<ServerMessage>>>,
  /// Sender to signal shutdown
  pub(crate) shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl WebSocketBroker {
  /// Create a new WebSocket broker with the specified server URL
  pub async fn new(url: &str) -> Result<Self> {
    Self::with_basic_auth(url, None, None).await
  }

  /// Create a new WebSocket broker with HTTP Basic authentication
  ///
  /// The credentials will be sent as HTTP Basic Auth in the Authorization header
  pub async fn with_basic_auth(
    url: &str,
    username: Option<String>,
    password: Option<String>,
  ) -> Result<Self> {
    let (cancel_tx, _) = broadcast::channel(1024);
    let basic_auth = match (username, password) {
      (Some(u), Some(p)) => Some((u, p)),
      _ => None,
    };
    let broker = Self {
      url: url.to_string(),
      connection: Arc::new(RwLock::new(None)),
      cancel_tx,
      basic_auth,
    };
    broker.connect().await?;
    Ok(broker)
  }

  /// Get the WebSocket URL
  pub fn url(&self) -> &str {
    &self.url
  }

  /// Connect to the WebSocket server
  async fn connect(&self) -> Result<()> {
    let ws_url = if self.url.starts_with("ws://") || self.url.starts_with("wss://") {
      self.url.clone()
    } else {
      format!("ws://{}/ws", self.url)
    };

    // Build WebSocket request with optional authentication
    let mut request = ws_url
      .into_client_request()
      .map_err(|e| Error::websocket(format!("Failed to create request: {}", e)))?;

    // Add HTTP Basic authentication header if credentials are provided
    if let Some((ref username, ref password)) = self.basic_auth {
      let credentials = format!("{}:{}", username, password);
      let encoded = BASE64_STANDARD.encode(credentials.as_bytes());
      request.headers_mut().insert(
        "Authorization",
        format!("Basic {}", encoded)
          .parse()
          .map_err(|e| Error::websocket(format!("Invalid auth credentials: {}", e)))?,
      );
    }

    let (ws_stream, _) = connect_async(request)
      .await
      .map_err(|e| Error::websocket(format!("Failed to connect: {}", e)))?;

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Channel for sending messages
    let (msg_tx, mut msg_rx) = mpsc::channel::<ClientMessage>(256);
    // Channel for receiving responses
    let (resp_tx, resp_rx) = mpsc::channel::<ServerMessage>(256);
    // Channel for shutdown signal
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let cancel_tx = self.cancel_tx.clone();

    // Spawn task to handle outgoing messages
    tokio::spawn(async move {
      loop {
        tokio::select! {
          Some(msg) = msg_rx.recv() => {
            if let Ok(json) = serde_json::to_string(&msg) {
              if ws_sender.send(Message::Text(json.into())).await.is_err() {
                break;
              }
            }
          }
          _ = &mut shutdown_rx => {
            // Send Close frame when shutdown is requested
            if let Err(e) = ws_sender.close().await {
              warn!("Failed to send WebSocket close frame: {}", e);
            }
            break;
          }
        }
      }
    });

    // Spawn task to handle incoming messages
    tokio::spawn(async move {
      while let Some(msg) = ws_receiver.next().await {
        match msg {
          Ok(Message::Text(text)) => {
            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
              match &server_msg {
                ServerMessage::Cancellation { task_id } => {
                  let _ = cancel_tx.send(task_id.clone());
                }
                _ => {
                  if resp_tx.send(server_msg).await.is_err() {
                    break;
                  }
                }
              }
            }
          }
          Ok(Message::Close(_)) => break,
          Err(e) => {
            // Don't log "Connection reset" protocol errors as they can occur during clean shutdown
            // when the sender closes while receiver is still waiting
            use tokio_tungstenite::tungstenite::Error as WsError;
            match &e {
              WsError::Protocol(_) if e.to_string().contains("Connection reset") => {
                // Expected during clean shutdown, don't log
              }
              _ => {
                error!("WebSocket error: {}", e);
              }
            }
            break;
          }
          _ => {}
        }
      }
    });

    let mut conn = self.connection.write().await;
    *conn = Some(WsConnection {
      sender: msg_tx,
      response_rx: Arc::new(Mutex::new(resp_rx)),
      shutdown_tx: Some(shutdown_tx),
    });

    Ok(())
  }

  /// Send a message and wait for a response
  pub(crate) async fn send_and_receive(&self, msg: ClientMessage) -> Result<ServerMessage> {
    let conn = self.connection.read().await;
    let conn = conn
      .as_ref()
      .ok_or_else(|| Error::websocket("Not connected"))?;

    conn
      .sender
      .send(msg)
      .await
      .map_err(|_| Error::websocket("Failed to send message"))?;

    let mut rx = conn.response_rx.lock().await;
    rx.recv()
      .await
      .ok_or_else(|| Error::websocket("Connection closed"))
  }

  /// Create an EnqueueRequest from a Task
  pub(crate) fn task_to_enqueue_request(&self, task: &Task) -> EnqueueRequest {
    EnqueueRequest {
      task_type: task.task_type.clone(),
      payload: BASE64_STANDARD.encode(&task.payload),
      headers: task.headers.clone(),
      queue: if task.options.queue.is_empty() {
        None
      } else {
        Some(task.options.queue.clone())
      },
      max_retry: Some(task.options.max_retry),
      task_id: task.options.task_id.clone(),
    }
  }

  /// Create a TaskDoneRequest from a TaskMessage
  pub(crate) fn task_message_to_done_request(&self, msg: &TaskMessage) -> TaskDoneRequest {
    TaskDoneRequest {
      task_id: msg.id.clone(),
      queue: msg.queue.clone(),
      task_type: msg.r#type.clone(),
      payload: BASE64_STANDARD.encode(&msg.payload),
    }
  }

  /// Convert TaskMessageResponse to TaskMessage
  pub(crate) fn response_to_task_message(&self, resp: &TaskMessageResponse) -> Result<TaskMessage> {
    let payload = BASE64_STANDARD
      .decode(&resp.payload)
      .map_err(|e| Error::invalid_message(format!("Invalid base64 payload: {}", e)))?;

    Ok(TaskMessage {
      id: resp.id.clone(),
      queue: resp.queue.clone(),
      r#type: resp.task_type.clone(),
      payload,
      headers: resp.headers.clone(),
      retry: resp.retry,
      retried: resp.retried,
      error_msg: resp.error_msg.clone(),
      timeout: resp.timeout,
      deadline: resp.deadline,
      group_key: resp.group_key.clone(),
      ..Default::default()
    })
  }

  /// Handle server response for task info
  pub(crate) fn handle_task_info_response(&self, resp: ServerMessage) -> Result<TaskInfo> {
    match resp {
      ServerMessage::TaskInfo(info) => {
        let state = match info.state.as_str() {
          "pending" => TaskState::Pending,
          "active" => TaskState::Active,
          "scheduled" => TaskState::Scheduled,
          "retry" => TaskState::Retry,
          "archived" => TaskState::Archived,
          "completed" => TaskState::Completed,
          "aggregating" => TaskState::Aggregating,
          _ => TaskState::Pending,
        };
        Ok(TaskInfo {
          id: info.id,
          queue: info.queue,
          task_type: info.task_type,
          payload: Vec::new(),
          headers: std::collections::HashMap::new(),
          state,
          max_retry: 0,
          retried: 0,
          last_err: None,
          last_failed_at: None,
          timeout: None,
          deadline: None,
          group: None,
          next_process_at: None,
          is_orphaned: false,
          retention: None,
          completed_at: None,
          result: None,
        })
      }
      ServerMessage::Error { message } => Err(Error::broker(message)),
      _ => Err(Error::invalid_message("Unexpected response")),
    }
  }

  /// Get the cancellation sender
  pub fn cancel_sender(&self) -> broadcast::Sender<String> {
    self.cancel_tx.clone()
  }
}
