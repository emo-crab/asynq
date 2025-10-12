//! # Asynq
//!
//! Simple, reliable & efficient distributed task queue in Rust
//!
//! Asynq 是一个分布式任务队列库，用于在 Redis 支持的异步工作者之间排队任务并处理任务。
//! Asynq is a distributed task queue library for queuing and processing tasks among async workers backed by Redis.
//! 它旨在具有可扩展性，但易于上手。
//! It is designed to be scalable yet easy to use.
//!
//! ## 特性
//! ## Features
//!
//! - 保证任务至少执行一次
//!   - Guarantees at-least-once task execution
//! - 任务调度功能
//!   - Task scheduling capability
//! - 失败任务的重试机制
//!   - Retry mechanism for failed tasks
//! - 在工作者崩溃时自动恢复任务
//!   - Automatic task recovery on worker crash
//! - 加权优先级队列
//!   - Weighted priority queues
//! - 严格优先级队列
//!   - Strict priority queues
//! - 低延迟添加任务，因为 Redis 写入速度快
//!   - Low-latency task addition due to fast Redis writes
//! - 使用唯一选项去重任务
//!   - Deduplication of tasks using unique option
//! - 支持每个任务的超时和截止时间
//!   - Supports timeout and deadline for each task
//! - 允许聚合任务组以批处理多个连续操作
//!   - Allows aggregation of task groups for batch processing
//! - 灵活的处理器接口，支持中间件
//!   - Flexible handler interface with middleware support
//! - 能够暂停队列以停止处理来自队列的任务
//!   - Ability to pause queues to stop processing tasks from a queue
//! - 周期性任务
//!   - Periodic tasks
//! - 支持 Redis Sentinels 实现高可用性
//!   - Supports Redis Sentinels for high availability
//! - 与 Prometheus 集成以收集和可视化队列指标
//!   - Integration with Prometheus for queue metrics collection and visualization
//! - Web UI 检查和远程控制队列和任务
//!   - Web UI for inspecting and remotely controlling queues and tasks
//!
//! ## 快速开始
//! ## Quick Start
//!
//! ```rust,no_run
//! use asynq::{client::Client,task::Task, redis::RedisConfig};
//! use asynq::server::{Server,Handler};
//! use async_trait::async_trait;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 创建 Redis 配置
//!     // Create Redis configuration
//!     let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;
//!     
//!     // 创建客户端
//!     // Create client
//!     let client = Client::new(redis_config).await?;
//!     
//!     // 创建任务
//!     // Create task
//!     let task = Task::new("email:deliver", b"task payload").unwrap();
//!     
//!     // 将任务加入队列
//!     // Enqueue task
//!     client.enqueue(task).await?;
//!     
//!     Ok(())
//! }
//! ```

pub mod base;
pub mod client;
pub mod components;
pub mod config;
pub mod error;
pub mod inspector;
pub mod proto;
pub mod rdb;
pub mod redis;
pub mod scheduler;
pub mod serve_mux;
pub mod server;
pub mod task;

// Re-export macros when the feature is enabled
#[cfg(feature = "macros")]
pub use asynq_macros::{
  register_async_handlers, register_handlers, task_handler, task_handler_async,
};
