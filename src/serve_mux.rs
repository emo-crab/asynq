//! ServeMux - 任务路由多路复用器
//! ServeMux - Task routing multiplexer
//!
//! 提供基于任务类型的路由功能，类似于 Go 版本的 servemux.go
//! Provides task type-based routing functionality, similar to Go's servemux.go
//!
//! ## Pattern Matching / 模式匹配
//!
//! ServeMux supports wildcard pattern matching for flexible task routing:
//! ServeMux 支持通配符模式匹配，实现灵活的任务路由：
//!
//! - Exact match / 精确匹配: `"email:send"` matches only "email:send"
//! - Prefix wildcard / 前缀通配符: `"email:*"` matches all tasks starting with "email:"
//! - Suffix wildcard / 后缀通配符: `"*:send"` matches all tasks ending with ":send"
//! - Combined wildcards / 组合通配符: `"email:*:done"` matches tasks with specific prefix and suffix
//! - Catch-all / 捕获所有: `"*"` matches any task type
//!
//! ## Examples / 示例
//!
//! ```rust,no_run
//! use asynq::{serve_mux::ServeMux, task::Task, error::Result};
//!
//! # async fn example() -> Result<()> {
//! let mut mux = ServeMux::new();
//!
//! // Handle all email tasks / 处理所有邮件任务
//! mux.handle_func("email:*", |task: Task| {
//!     println!("Processing email task: {}", task.get_type());
//!     Ok(())
//! });
//!
//! // Handle all urgent tasks / 处理所有紧急任务
//! mux.handle_func("*:urgent", |task: Task| {
//!     println!("Processing urgent task: {}", task.get_type());
//!     Ok(())
//! });
//!
//! // Catch-all handler / 捕获所有处理器
//! mux.handle_async_func("*", |task: Task| async move {
//!     println!("Default handler for: {}", task.get_type());
//!     Ok(())
//! });
//! # Ok(())
//! # }
//! ```

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

/// 检查任务类型是否匹配模式
/// Check if a task type matches a pattern
///
/// Supports wildcards:
/// - "*" matches any string
/// - "prefix:*" matches any string starting with "prefix:"
/// - "*:suffix" matches any string ending with ":suffix"
/// - "prefix:*:suffix" matches strings with prefix and suffix
fn pattern_matches(pattern: &str, task_type: &str) -> bool {
  if pattern == "*" {
    return true;
  }

  if !pattern.contains('*') {
    return pattern == task_type;
  }

  // Handle wildcard patterns
  let parts: Vec<&str> = pattern.split('*').collect();

  if parts.len() == 2 {
    let (prefix, suffix) = (parts[0], parts[1]);

    if prefix.is_empty() {
      // Pattern like "*:suffix"
      return task_type.ends_with(suffix);
    } else if suffix.is_empty() {
      // Pattern like "prefix:*"
      return task_type.starts_with(prefix);
    } else {
      // Pattern like "prefix:*:suffix"
      return task_type.starts_with(prefix) && task_type.ends_with(suffix);
    }
  }

  // For complex patterns with multiple wildcards, use a simple approach
  // Check if task_type starts with first part and ends with last part
  if let (Some(first), Some(last)) = (parts.first(), parts.last()) {
    if task_type.starts_with(first) && task_type.ends_with(last) {
      // Check all middle parts are present in order
      let mut search_start = first.len();
      for part in &parts[1..parts.len() - 1] {
        if let Some(pos) = task_type[search_start..].find(part) {
          search_start += pos + part.len();
        } else {
          return false;
        }
      }
      return true;
    }
  }

  false
}

/// ServeMux - 任务路由多路复用器
/// ServeMux - Task routing multiplexer
///
/// ServeMux 根据任务类型将任务路由到对应的处理器，支持通配符模式匹配
/// ServeMux routes tasks to corresponding handlers based on task type, with wildcard pattern matching support
///
/// # Pattern Matching / 模式匹配
///
/// - `"email:send"` - Exact match / 精确匹配
/// - `"email:*"` - Prefix wildcard / 前缀通配符
/// - `"*:send"` - Suffix wildcard / 后缀通配符
/// - `"email:*:done"` - Combined wildcards / 组合通配符
/// - `"*"` - Catch-all / 捕获所有
///
/// # Examples
///
/// ```rust,no_run
/// use asynq::{serve_mux::ServeMux,task::Task,error::Result};
///
/// # async fn example() -> Result<()> {
/// let mut mux = ServeMux::new();
///
/// // 注册同步处理器 / Register sync handler
/// mux.handle_func("email:send", |task: Task| {
///     println!("Processing email:send task");
///     Ok(())
/// });
///
/// // 使用通配符匹配所有邮件任务 / Use wildcard to match all email tasks
/// mux.handle_func("email:*", |task: Task| {
///     println!("Processing email task: {}", task.get_type());
///     Ok(())
/// });
///
/// // 注册异步处理器 / Register async handler
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
    self
      .handlers
      .insert(pattern.to_string(), HandlerWrapper::Sync(Arc::new(func)));
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
  ///
  /// Supports exact match and wildcard patterns:
  /// - Exact match: "email:send" matches only "email:send"
  /// - Prefix wildcard: "email:*" matches "email:send", "email:deliver", etc.
  /// - Suffix wildcard: "*:send" matches "email:send", "sms:send", etc.
  /// - Full wildcard: "*" matches any task type
  fn find_handler(&self, task_type: &str) -> Option<&HandlerWrapper> {
    // Try exact match first
    if let Some(handler) = self.handlers.get(task_type) {
      return Some(handler);
    }

    // Try pattern matching with wildcards
    for (pattern, handler) in &self.handlers {
      if pattern_matches(pattern, task_type) {
        return Some(handler);
      }
    }

    None
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

    mux.handle_func("email:send", |_task: Task| Ok(()));

    mux.handle_async_func("image:resize", |_task: Task| async move { Ok(()) });

    let task1 = Task::new("email:send", b"test").unwrap();
    assert!(mux.process_task(task1).await.is_ok());

    let task2 = Task::new("image:resize", b"test").unwrap();
    assert!(mux.process_task(task2).await.is_ok());
  }

  #[test]
  fn test_pattern_matches() {
    // Exact match
    assert!(pattern_matches("email:send", "email:send"));
    assert!(!pattern_matches("email:send", "email:deliver"));

    // Full wildcard
    assert!(pattern_matches("*", "any:task"));
    assert!(pattern_matches("*", "anything"));

    // Prefix wildcard
    assert!(pattern_matches("email:*", "email:send"));
    assert!(pattern_matches("email:*", "email:deliver"));
    assert!(pattern_matches("email:*", "email:process:complex"));
    assert!(!pattern_matches("email:*", "sms:send"));

    // Suffix wildcard
    assert!(pattern_matches("*:send", "email:send"));
    assert!(pattern_matches("*:send", "sms:send"));
    assert!(!pattern_matches("*:send", "email:deliver"));

    // Prefix and suffix wildcard
    assert!(pattern_matches("email:*:done", "email:send:done"));
    assert!(pattern_matches("email:*:done", "email:process:task:done"));
    assert!(!pattern_matches("email:*:done", "email:send:failed"));
    assert!(!pattern_matches("email:*:done", "sms:send:done"));
  }

  #[tokio::test]
  async fn test_serve_mux_wildcard_patterns() {
    let mut mux = ServeMux::new();

    // Register a handler for all email tasks
    mux.handle_func("email:*", |task: Task| {
      assert!(task.get_type().starts_with("email:"));
      Ok(())
    });

    // Register a handler for all tasks ending with :send
    mux.handle_async_func("*:send", |task: Task| async move {
      assert!(task.get_type().ends_with(":send"));
      Ok(())
    });

    // Test email:send (should match "email:*" first due to exact prefix)
    let task1 = Task::new("email:send", b"test").unwrap();
    assert!(mux.process_task(task1).await.is_ok());

    // Test email:deliver (should match "email:*")
    let task2 = Task::new("email:deliver", b"test").unwrap();
    assert!(mux.process_task(task2).await.is_ok());

    // Test sms:send (should match "*:send")
    let task3 = Task::new("sms:send", b"test").unwrap();
    assert!(mux.process_task(task3).await.is_ok());

    // Test task with no matching handler
    let task4 = Task::new("report:generate", b"test").unwrap();
    assert!(mux.process_task(task4).await.is_err());
  }

  #[tokio::test]
  async fn test_serve_mux_catch_all_pattern() {
    let mut mux = ServeMux::new();

    // Register a catch-all handler
    mux.handle_func("*", |_task: Task| Ok(()));

    // Any task type should be handled
    let task1 = Task::new("any:task:type", b"test").unwrap();
    assert!(mux.process_task(task1).await.is_ok());

    let task2 = Task::new("another:completely:different:task", b"test").unwrap();
    assert!(mux.process_task(task2).await.is_ok());
  }
}
