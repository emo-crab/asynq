//! ServeMux - 任务路由多路复用器
//! ServeMux - Task routing multiplexer
//!
//! 提供基于任务类型的路由功能，类似于 Go 版本的 servemux.go
//! Provides task type-based routing functionality, similar to Go's servemux.go

use crate::error::{Error, Result};
use crate::server::Handler;
use crate::task::Task;
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// 处理器包装器，用于存储不同类型的处理器
/// Handler wrapper for storing different types of handlers
enum HandlerWrapper {
  Sync(Arc<dyn Fn(Task) -> Result<()> + Send + Sync>),
  Async(Arc<dyn Fn(Task) -> BoxFuture<Result<()>> + Send + Sync>),
}

type BoxFuture<T> = std::pin::Pin<Box<dyn Future<Output = T> + Send>>;

/// ServeMux - 任务路由多路复用器
/// ServeMux - Task routing multiplexer
///
/// ServeMux 根据任务类型将任务路由到对应的处理器
/// ServeMux routes tasks to corresponding handlers based on task type
///
/// # Examples
///
/// ```rust,no_run
/// use asynq::{serve_mux::ServeMux,task::Task,error::Result};
///
/// # async fn example() -> Result<()> {
/// let mut mux = ServeMux::new();
///
/// // 注册同步处理器
/// mux.handle_func("email:send", |task: Task| {
///     println!("Processing email:send task");
///     Ok(())
/// });
///
/// // 注册异步处理器
/// mux.handle_async_func("image:resize", |task: Task| async move {
///     println!("Processing image:resize task");
///     Ok(())
/// });
/// # Ok(())
/// # }
/// ```
pub struct ServeMux {
  handlers: HashMap<String, HandlerWrapper>,
}

impl ServeMux {
  /// 创建新的 ServeMux
  /// Create a new ServeMux
  pub fn new() -> Self {
    Self {
      handlers: HashMap::new(),
    }
  }

  /// 注册同步处理函数
  /// Register a synchronous handler function
  ///
  /// # Arguments
  ///
  /// * `pattern` - 任务类型模式 / Task type pattern
  /// * `func` - 处理函数 / Handler function
  ///
  /// # Examples
  ///
  /// ```rust,no_run
  /// use asynq::{serve_mux::ServeMux,task::Task,error::Result};
  ///
  /// let mut mux = ServeMux::new();
  /// mux.handle_func("email:send", |task: Task| {
  ///     println!("Processing: {}", task.get_type());
  ///     Ok(())
  /// });
  /// ```
  pub fn handle_func<F>(&mut self, pattern: &str, func: F)
  where
    F: Fn(Task) -> Result<()> + Send + Sync + 'static,
  {
    self.handlers.insert(
      pattern.to_string(),
      HandlerWrapper::Sync(Arc::new(func)),
    );
  }

  /// 注册异步处理函数
  /// Register an asynchronous handler function
  ///
  /// # Arguments
  ///
  /// * `pattern` - 任务类型模式 / Task type pattern
  /// * `func` - 异步处理函数 / Async handler function
  ///
  /// # Examples
  ///
  /// ```rust,no_run
  /// use asynq::{serve_mux::ServeMux,task::Task,error::Result};
  ///
  /// let mut mux = ServeMux::new();
  /// mux.handle_async_func("image:resize", |task: Task| async move {
  ///     println!("Processing: {}", task.get_type());
  ///     Ok(())
  /// });
  /// ```
  pub fn handle_async_func<F, Fut>(&mut self, pattern: &str, func: F)
  where
    F: Fn(Task) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
  {
    let func = Arc::new(func);
    self.handlers.insert(
      pattern.to_string(),
      HandlerWrapper::Async(Arc::new(move |task: Task| {
        let func = Arc::clone(&func);
        Box::pin(async move { func(task).await })
      })),
    );
  }

  /// 查找处理器
  /// Find handler for a task type
  fn find_handler(&self, task_type: &str) -> Option<&HandlerWrapper> {
    self.handlers.get(task_type)
  }
}

impl Default for ServeMux {
  fn default() -> Self {
    Self::new()
  }
}

#[async_trait]
impl Handler for ServeMux {
  async fn process_task(&self, task: Task) -> Result<()> {
    let task_type = task.get_type();
    
    match self.find_handler(task_type) {
      Some(HandlerWrapper::Sync(func)) => func(task),
      Some(HandlerWrapper::Async(func)) => func(task).await,
      None => Err(Error::other(format!(
        "No handler registered for task type: {}",
        task_type
      ))),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::task::Task;

  #[tokio::test]
  async fn test_serve_mux_sync_handler() {
    let mut mux = ServeMux::new();
    
    mux.handle_func("test:task", |task: Task| {
      assert_eq!(task.get_type(), "test:task");
      Ok(())
    });

    let task = Task::new("test:task", b"test payload").unwrap();
    let result = mux.process_task(task).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_serve_mux_async_handler() {
    let mut mux = ServeMux::new();
    
    mux.handle_async_func("async:task", |task: Task| async move {
      assert_eq!(task.get_type(), "async:task");
      Ok(())
    });

    let task = Task::new("async:task", b"test payload").unwrap();
    let result = mux.process_task(task).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_serve_mux_no_handler() {
    let mux = ServeMux::new();
    
    let task = Task::new("unknown:task", b"test payload").unwrap();
    let result = mux.process_task(task).await;
    assert!(result.is_err());
    
    if let Err(e) = result {
      assert!(e.to_string().contains("No handler registered"));
    }
  }

  #[tokio::test]
  async fn test_serve_mux_multiple_handlers() {
    let mut mux = ServeMux::new();
    
    mux.handle_func("email:send", |_task: Task| {
      Ok(())
    });
    
    mux.handle_async_func("image:resize", |_task: Task| async move {
      Ok(())
    });

    let task1 = Task::new("email:send", b"test").unwrap();
    assert!(mux.process_task(task1).await.is_ok());

    let task2 = Task::new("image:resize", b"test").unwrap();
    assert!(mux.process_task(task2).await.is_ok());
  }
}
