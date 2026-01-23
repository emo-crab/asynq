//! Asynq Server binary
//!
//! A standalone asynq server with WebSocket API for cross-process task queue communication.

use asynq_server::{AsynqServer, BackendType, MultiTenantAuth};
use std::env;
use std::str::FromStr;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // Initialize logging
  tracing_subscriber::registry()
    .with(fmt::layer())
    .with(EnvFilter::from_default_env().add_directive("asynq_server=info".parse()?))
    .init();

  // Get address from environment or use default
  let addr = env::var("ASYNQ_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

  info!("Starting asynq-server on {}", addr);

  let mut server = AsynqServer::from_str(&addr)?;

  // Check for backend-based multi-tenant authentication
  if let (Ok(backend_type), Ok(backend_template)) = (
    env::var("ASYNQ_BACKEND_TYPE"),
    env::var("ASYNQ_BACKEND_TEMPLATE"),
  ) {
    let backend = match backend_type.to_lowercase().as_str() {
      "redis" => BackendType::Redis,
      "postgresql" | "postgres" => BackendType::Postgres,
      _ => {
        eprintln!("Invalid ASYNQ_BACKEND_TYPE: {}. Use 'redis' or 'postgresql'", backend_type);
        return Err(anyhow::anyhow!("Invalid backend type"));
      }
    };
    
    info!("🏢 Multi-tenant authentication enabled with {:?} backend", backend);
    info!("   Backend template: {}", backend_template);
    info!("   Users will be authenticated by connecting to their own backend");
    
    let auth = MultiTenantAuth::new(backend, backend_template);
    server = server.with_multi_tenant_auth(auth);
  } else {
    // Fall back to single-tenant authentication (backward compatible)
    let username = env::var("ASYNQ_USERNAME").ok();
    let password = env::var("ASYNQ_PASSWORD").ok();

    match (username, password) {
      (Some(user), Some(pass)) => {
        info!("🔐 Single-tenant authentication enabled");
        server = server.with_basic_auth(user, pass);
      }
      _ => {
        info!("⚠️  Authentication disabled - all connections allowed");
      }
    }
  }

  server.run().await?;

  Ok(())
}
