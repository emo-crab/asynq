//! Multi-Tenant WebSocket Server Example with Backend Authentication
//!
//! This example demonstrates how to configure and run the asynq-server
//! with backend-based multi-tenant authentication. Each user authenticates
//! by providing credentials that are used to connect to their own Redis
//! or PostgresSQL backend.
//!
//! ## Setup
//!
//! 1. Start a Redis server (for this example):
//!    ```bash
//!    docker run -d -p 6379:6379 redis:alpine --requirepass default_password
//!    ```
//!
//! 2. Start the server:
//!    ```bash
//!    cargo run --example multi_tenant_server --features redis
//!    ```
//!
//! 3. Clients connect with their Redis credentials:
//!    ```bash
//!    export ASYNQ_USERNAME="user1"
//!    export ASYNQ_PASSWORD="password1"
//!    cargo run --example websocket_producer --features websocket,json
//!    ```
//!
//! When a user connects, the server attempts to connect to Redis using their
//! credentials. If successful, the connection is cached and the user is authenticated.

use asynq_server::{AsynqServer, BackendType, MultiTenantAuth};
use std::str::FromStr;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // Initialize logging
  tracing_subscriber::registry()
    .with(fmt::layer())
    .with(
      EnvFilter::from_default_env()
        .add_directive("asynq_server=debug".parse()?)
        .add_directive("multi_tenant_server=debug".parse()?),
    )
    .init();

  // Create server with memory backend
  let addr = "127.0.0.1:8080";
  tracing::info!("Starting multi-tenant asynq-server on {}", addr);

  // Configure multi-tenant authentication with Redis backend
  // Template: redis://{username}:{password}@localhost:6379
  // When a user connects with username=user1 and password=pass1, the server will try:
  // redis://user1:pass1@localhost:6379
  let backend_template = "redis://{username}:{password}@localhost:6379".to_string();
  let auth = MultiTenantAuth::new(BackendType::Redis, backend_template);

  tracing::info!("🏢 Multi-tenant authentication configured with Redis backend");
  tracing::info!("   Backend template: redis://{{username}}:{{password}}@localhost:6379");
  tracing::info!("   Users authenticate by connecting to their own Redis instance");
  tracing::info!("");
  tracing::info!("How it works:");
  tracing::info!("  1. User connects with Basic Auth credentials");
  tracing::info!("  2. Server attempts to connect to Redis using those credentials");
  tracing::info!("  3. If connection succeeds, user is authenticated and cached");
  tracing::info!("  4. If connection fails, user gets 401 Unauthorized");
  tracing::info!("  5. Failed attempts are rate-limited to prevent abuse");
  tracing::info!("");
  tracing::info!("WebSocket endpoint: ws://{}/ws", addr);
  tracing::info!("Health check: http://{}/health", addr);

  // Create and run server
  let server = AsynqServer::from_str(addr)?.with_multi_tenant_auth(auth);

  server.run().await?;

  Ok(())
}
