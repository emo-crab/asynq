//! # Asynq Server
//!
//! A standalone asynq server with WebSocket API for cross-process task queue communication.
//!
//! ## Overview
//!
//! `asynq-server` provides a WebSocket-based API that allows multiple processes to communicate
//! with a centralized task queue. It supports multiple backends:
//!
//! - **Memory** (default): In-process storage, no external dependencies
//! - **Redis** (requires `redis` feature): Persistent storage with Redis
//! - **PostgresSQL** (requires `postgresql` feature): Persistent storage with PostgresSQL
//!
//! **Note:** WebSocket backend is NOT supported to avoid circular dependency.
//!
//! This design allows producers and consumers to connect via WebSocket without needing
//! direct access to Redis/PostgresSQL credentials - the server handles those connections internally.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────┐     WebSocket     ┌─────────────────────────────────┐
//! │   Producer 1    │ ─────────────────▶│                                 │
//! └─────────────────┘                   │                                 │
//!                                       │         asynq-server            │
//! ┌─────────────────┐     WebSocket     │  (Memory/Redis/PostgresSQL)      │
//! │   Producer 2    │ ─────────────────▶│                                 │
//! └─────────────────┘                   │  Credentials stay on server,   │
//!                                       │  clients only need WebSocket    │
//! ┌─────────────────┐     WebSocket     │                                 │
//! │   Consumer 1    │ ◀────────────────▶│                                 │
//! └─────────────────┘                   └─────────────────────────────────┘
//! ```
//!
//! ### Starting the server with Redis backend (requires `redis` feature)
//!
//! ```rust,ignore
//! use asynq_server::AsynqServer;
//! use redis::ConnectionInfo;
//! use std::str::FromStr;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let redis_config = ConnectionInfo::from_str("redis://127.0.0.1:6379")?;
//!     let server = AsynqServer::with_redis("127.0.0.1:8080", redis_config.into()).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! ### Starting the server with PostgresSQL backend (requires `postgresql` feature)
//!
//! ```rust,ignore
//! use asynq_server::AsynqServer;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let server = AsynqServer::with_postgres(
//!         "127.0.0.1:8080",
//!         "postgres://user:password@localhost/asynq"
//!     ).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod error;
pub mod handler;
pub mod message;
pub mod server;

pub use config::{AuthError, BackendType, MultiTenantAuth, TenantConfig};
pub use error::{Error, Result};
pub use server::AsynqServer;
