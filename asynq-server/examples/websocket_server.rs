//! WebSocket Server Example with HTTP Basic Authentication
//!
//! This example shows how to start an asynq-server with HTTP Basic authentication enabled.
//! Clients must provide valid username and password to connect.
//!
//! ## Running the Server
//!
//! ```bash
//! # Set the authentication credentials
//! export ASYNQ_USERNAME="admin"
//! export ASYNQ_PASSWORD="your-secret-password"
//!
//! # Run the server
//! cargo run --example websocket_server --features websocket
//! ```
//!
//! The server will listen on 127.0.0.1:8080 by default. You can change this by
//! setting the ASYNQ_SERVER_ADDR environment variable:
//!
//! ```bash
//! export ASYNQ_SERVER_ADDR="0.0.0.0:9090"
//! ```

use asynq_server::AsynqServer;
use std::env;
use std::str::FromStr;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("asynq_server=info".parse()?)
                .add_directive("websocket_server=info".parse()?),
        )
        .init();

    // Get server address from environment or use default
    let addr = env::var("ASYNQ_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    // Get authentication credentials from environment
    let username = env::var("ASYNQ_USERNAME").ok();
    let password = env::var("ASYNQ_PASSWORD").ok();
    
    let auth_enabled = username.is_some() && password.is_some();
    
    if auth_enabled {
        info!("🔐 HTTP Basic Authentication enabled");
        info!("   Clients must provide username and password in Authorization header");
    } else {
        info!("⚠️  Authentication disabled - all connections allowed");
        info!("   Set ASYNQ_USERNAME and ASYNQ_PASSWORD environment variables to enable authentication");
    }

    info!("🚀 Starting asynq-server on {}", addr);

    // Create server with memory backend (no external dependencies)
    let mut server: AsynqServer = AsynqServer::from_str(addr.as_str())?;

    // Add authentication if credentials are provided
    if let (Some(user), Some(pass)) = (username, password) {
        server = server.with_basic_auth(user, pass);
    }

    info!("📡 WebSocket endpoint: ws://{}/ws", addr);
    info!("🏥 Health check endpoint: http://{}/health", addr);
    info!("Press Ctrl+C to stop");

    // Run the server
    server.run().await?;

    Ok(())
}
