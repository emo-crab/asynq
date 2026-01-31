//! 后端模块
//! Backend module
//!
//! 定义了任务存储后端的实现，包括 Redis、PostgreSQL 和 WebSocket 后端。
//! Defines task storage backend implementations, including Redis, PostgreSQL, and WebSocket backends.
//!
//! 现在支持同时启用多个后端，默认使用 Redis 后端。
//! Now supports enabling multiple backends simultaneously, with Redis as the default.
//!
//! ## 默认 Broker 和 Inspector 类型别名
//! ## Default Broker and Inspector Type Aliases
//!
//! 默认的 `Broker` 和 `Inspector` 类型别名使用 Redis 后端。
//! The default `Broker` and `Inspector` type aliases use the Redis backend.
//!
//! 如需使用其他后端，请直接使用具体类型：
//! To use other backends, use the specific types directly:
//!
//! - PostgreSQL: `PostgresBroker`, `PostgresInspector` (需要 `postgres` feature)
//! - WebSocket: `WebSocketBroker`, `WebSocketInspector` (需要 `websocket` feature)
//!
//! ## 使用示例
//! ## Usage Example
//!
//! ```rust,ignore
//! use asynq::backend::{Broker, Inspector};
//!
//! // 默认使用 Redis 后端
//! // Default uses Redis backend
//! ```

// PostgreSQL 后端 - 需要 postgres feature
// PostgreSQL backend - requires postgres feature
#[cfg(feature = "postgres")]
pub mod pgdb;

// WebSocket 后端 - 需要 websocket feature
// WebSocket backend - requires websocket feature
#[cfg(feature = "websocket")]
pub mod wsdb;

// Redis 后端 - 始终编译（默认后端）
// Redis backend - always compiled (default backend)
pub mod option;
pub mod pagination;
mod rdb;

// Re-export common types from rdb (Redis is always available as default backend)
// 重新导出 rdb 的公共类型（Redis 始终可用作为默认后端）
pub use rdb::{RedisBroker, RedisConnectionType, RedisInspector};

// Re-export types from pgdb when postgres feature is enabled
// 当启用 postgres 特性时重新导出 pgdb 的类型
#[cfg(feature = "postgres")]
pub use pgdb::PostgresBroker;
#[cfg(feature = "postgres")]
pub use pgdb::PostgresInspector;

// Re-export types from wsdb when websocket feature is enabled
// 当启用 websocket 特性时重新导出 wsdb 的类型
#[cfg(feature = "websocket")]
pub use wsdb::WebSocketBroker;
#[cfg(feature = "websocket")]
pub use wsdb::WebSocketInspector;

// =============================================================================
// 统一的 Broker 和 Inspector 类型别名
// Unified Broker and Inspector type aliases
// =============================================================================

/// 默认的 Broker 类型别名
/// Default Broker type alias
///
/// 始终使用 Redis 后端作为默认实现。
/// Always uses Redis backend as the default implementation.
///
/// 如需使用其他后端，请直接使用具体类型：
/// To use other backends, use the specific types directly:
///
/// - PostgreSQL: `PostgresBroker` (需要 `postgres` feature)
/// - WebSocket: `WebSocketBroker` (需要 `websocket` feature)
pub type Broker = RedisBroker;

/// 默认的 Inspector 类型别名
/// Default Inspector type alias
///
/// 始终使用 Redis 后端作为默认实现。
/// Always uses Redis backend as the default implementation.
///
/// 如需使用其他后端，请直接使用具体类型：
/// To use other backends, use the specific types directly:
///
/// - PostgreSQL: `PostgresInspector` (需要 `postgres` feature)
/// - WebSocket: `WebSocketInspector` (需要 `websocket` feature)
pub type Inspector = RedisInspector;
