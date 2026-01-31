//! 错误处理模块
//! Error handling module
//!
//! 定义了 Asynq 库中使用的各种错误类型
//! Defines various error types used in the Asynq library

use thiserror::Error;

/// Asynq 库的结果类型
/// Result type for the Asynq library
pub type Result<T> = std::result::Result<T, Error>;

/// Asynq 错误类型
/// Asynq error type
#[derive(Error, Debug)]
pub enum Error {
  /// Redis connection error
  #[error("Redis connection error: {0}")]
  Redis(#[from] redis::RedisError),
  /// Redis parsing error
  #[error("Redis parsing error: {0}")]
  RedisParsing(#[from] redis::ParsingError),

  #[cfg(feature = "postgres")]
  /// SeaORM 数据库错误
  /// SeaORM database error
  #[error("SeaORM database error: {0}")]
  SeaOrm(#[from] sea_orm::DbErr),

  #[cfg(feature = "json")]
  /// 序列化错误
  /// Serialization error
  #[error("Serialization error: {0}")]
  Serialization(#[from] serde_json::Error),

  /// Protocol Buffer 编码错误
  /// Protocol buffer encoding error
  #[error("Protocol buffer encoding error: {0}")]
  ProtoEncode(#[from] prost::EncodeError),

  /// Protocol Buffer 解码错误
  /// Protocol buffer decoding error
  #[error("Protocol buffer decoding error: {0}")]
  ProtoDecode(#[from] prost::DecodeError),

  /// 任务重复错误
  /// Task already exists error
  #[error("Task already exists")]
  TaskDuplicate,

  /// 任务 ID 冲突错误
  /// Task ID conflict error
  #[error("Task ID conflicts with another task")]
  TaskIdConflict,

  /// 任务未找到错误
  /// Task not found error
  #[error("Task not found: {id}")]
  TaskNotFound { id: String },

  /// 队列错误
  /// Queue error
  #[error("Queue error: {message}")]
  Queue { message: String },

  /// 无效的队列名称
  /// Invalid queue name
  #[error("Invalid queue name: {name}")]
  InvalidQueueName { name: String },

  /// 无效的任务类型
  /// Invalid task type
  #[error("Invalid task type: {task_type}")]
  InvalidTaskType { task_type: String },

  /// 服务器已关闭
  /// Server closed
  #[error("Server closed")]
  ServerClosed,

  /// 服务器已在运行
  /// Server is already running
  #[error("Server is already running")]
  ServerRunning,

  /// 超时错误
  /// Timeout error
  #[error("Operation timeout")]
  Timeout,

  /// 取消错误
  /// Cancellation error
  #[error("Operation cancelled")]
  Cancelled,

  /// 配置错误
  /// Configuration error
  #[error("Configuration error: {message}")]
  Config { message: String },

  /// IO 错误
  /// IO error
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  /// 其他错误
  /// Other error
  #[error("Other error: {message}")]
  Other { message: String },

  /// 未实现错误
  /// Not implemented error
  #[error("Not implemented: {0}")]
  NotImplemented(String),

  /// 不支持的操作
  /// Not supported operation
  #[error("Not supported: {0}")]
  NotSupported(String),

  /// WebSocket 错误
  /// WebSocket error
  #[error("WebSocket error: {0}")]
  WebSocket(String),

  /// 无效消息
  /// Invalid message
  #[error("Invalid message: {0}")]
  InvalidMessage(String),

  /// Broker 错误
  /// Broker error
  #[error("Broker error: {0}")]
  Broker(String),
}

impl Error {
  /// 创建队列错误
  /// Create a queue error
  pub fn queue<S: Into<String>>(message: S) -> Self {
    Self::Queue {
      message: message.into(),
    }
  }

  /// 创建配置错误
  /// Create a configuration error
  pub fn config<S: Into<String>>(message: S) -> Self {
    Self::Config {
      message: message.into(),
    }
  }

  /// 创建其他错误
  /// Create another type of error
  pub fn other<S: Into<String>>(message: S) -> Self {
    Self::Other {
      message: message.into(),
    }
  }

  /// 创建不支持错误
  /// Create a not supported error
  pub fn not_supported<S: Into<String>>(message: S) -> Self {
    Self::NotSupported(message.into())
  }

  /// 创建 WebSocket 错误
  /// Create a WebSocket error
  pub fn websocket<S: Into<String>>(message: S) -> Self {
    Self::WebSocket(message.into())
  }

  /// 创建无效消息错误
  /// Create an invalid message error
  pub fn invalid_message<S: Into<String>>(message: S) -> Self {
    Self::InvalidMessage(message.into())
  }

  /// 创建 Broker 错误
  /// Create a broker error
  pub fn broker<S: Into<String>>(message: S) -> Self {
    Self::Broker(message.into())
  }

  /// 检查是否为重试错误
  /// Check if the error is retriable
  pub fn is_retriable(&self) -> bool {
    match self {
      Error::Redis(_) => return true,
      Error::RedisParsing(_) => {}
      Error::ProtoEncode(_) => {}
      Error::ProtoDecode(_) => {}
      Error::TaskDuplicate => {}
      Error::TaskIdConflict => {}
      Error::TaskNotFound { .. } => {}
      Error::Queue { .. } => {}
      Error::InvalidQueueName { .. } => {}
      Error::InvalidTaskType { .. } => {}
      Error::ServerClosed => {}
      Error::ServerRunning => {}
      Error::Cancelled => {}
      Error::Config { .. } => {}
      Error::Io(_) | Error::Timeout | Error::WebSocket(_) => {
        return true;
      }
      Error::Other { .. } => {}
      Error::NotImplemented(_) => {}
      Error::NotSupported(_) => {}
      Error::InvalidMessage(_) => {}
      Error::Broker(_) => {}
      #[cfg(feature = "postgres")]
      Error::SeaOrm(_) => {}
      #[cfg(feature = "json")]
      Error::Serialization(_) => {}
    }
    false
  }

  /// 检查是否为致命错误
  /// Check if the error is fatal
  pub fn is_fatal(&self) -> bool {
    !self.is_retriable()
  }
}

/// 特殊的跳过重试错误包装器
/// Special skip retry error wrapper
#[derive(Error, Debug)]
#[error("Skip retry: {0}")]
pub struct SkipRetryError(pub Box<dyn std::error::Error + Send + Sync>);

impl SkipRetryError {
  /// 创建新的跳过重试错误
  /// Create a new skip retry error
  pub fn new<E>(error: E) -> Self
  where
    E: std::error::Error + Send + Sync + 'static,
  {
    Self(Box::new(error))
  }
}

/// 特殊的撤销任务错误包装器
/// Special revoke task error wrapper
#[derive(Error, Debug)]
#[error("Revoke task: {0}")]
pub struct RevokeTaskError(pub Box<dyn std::error::Error + Send + Sync>);

impl RevokeTaskError {
  /// 创建新的撤销任务错误
  /// Create a new revoke task error
  pub fn new<E>(error: E) -> Self
  where
    E: std::error::Error + Send + Sync + 'static,
  {
    Self(Box::new(error))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_error_creation() {
    let err = Error::queue("test queue error");
    assert!(matches!(err, Error::Queue { .. }));

    let err = Error::config("test config error");
    assert!(matches!(err, Error::Config { .. }));

    let err = Error::other("test other error");
    assert!(matches!(err, Error::Other { .. }));
  }

  #[test]
  fn test_error_retriable() {
    assert!(Error::Timeout.is_retriable());
    assert!(!Error::TaskDuplicate.is_retriable());
    assert!(!Error::ServerClosed.is_retriable());
  }

  #[test]
  fn test_skip_retry_error() {
    let inner_err = std::io::Error::other("test error");
    let skip_err = SkipRetryError::new(inner_err);
    assert!(skip_err.to_string().contains("Skip retry"));
  }

  #[test]
  fn test_revoke_task_error() {
    let inner_err = std::io::Error::other("test error");
    let revoke_err = RevokeTaskError::new(inner_err);
    assert!(revoke_err.to_string().contains("Revoke task"));
  }
}
