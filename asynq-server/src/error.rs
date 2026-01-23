//! Error types for asynq-server

use thiserror::Error;

/// Result type for asynq-server
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for asynq-server
#[derive(Error, Debug)]
pub enum Error {
  /// Asynq broker error
  #[error("Broker error: {0}")]
  Broker(#[from] asynq::error::Error),

  /// WebSocket error
  #[error("WebSocket error: {0}")]
  WebSocket(String),

  /// Serialization error
  #[error("Serialization error: {0}")]
  Serialization(#[from] serde_json::Error),

  /// IO error
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  /// Invalid message
  #[error("Invalid message: {0}")]
  InvalidMessage(String),

  /// Task not found
  #[error("Task not found: {0}")]
  TaskNotFound(String),

  /// Server error
  #[error("Server error: {0}")]
  Server(String),
}

impl Error {
  /// Create a WebSocket error
  pub fn websocket<S: Into<String>>(msg: S) -> Self {
    Self::WebSocket(msg.into())
  }

  /// Create an invalid message error
  pub fn invalid_message<S: Into<String>>(msg: S) -> Self {
    Self::InvalidMessage(msg.into())
  }

  /// Create a server error
  pub fn server<S: Into<String>>(msg: S) -> Self {
    Self::Server(msg.into())
  }
}
