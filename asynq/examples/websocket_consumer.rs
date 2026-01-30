//! WebSocket Consumer Example with HTTP Basic Authentication
//!
//! This example demonstrates how to use the WebSocket backend to process tasks
//! with HTTP Basic authentication. A consumer connects to an asynq-server via WebSocket
//! and processes tasks from the queue.
//!
//! ## Setup
//!
//! 1. Start the asynq-server with authentication:
//!    ```bash
//!    export ASYNQ_USERNAME="admin"
//!    export ASYNQ_PASSWORD="your-secret-password"
//!    cargo run --bin asynq-server
//!    ```
//!
//! 2. Run this consumer:
//!    ```bash
//!    export ASYNQ_USERNAME="admin"
//!    export ASYNQ_PASSWORD="your-secret-password"
//!    cargo run --example websocket_consumer --features websocket,json
//!    ```
//!
//! 3. Run the producer to send tasks:
//!    ```bash
//!    export ASYNQ_USERNAME="admin"
//!    export ASYNQ_PASSWORD="your-secret-password"
//!    cargo run --example websocket_producer --features websocket,json
//!    ```

#[cfg(feature = "websocket")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "websocket")]
#[derive(Serialize, Deserialize, Debug)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}
#[cfg(feature = "websocket")]
#[derive(Serialize, Deserialize, Debug)]
struct ImagePayload {
  url: String,
  width: u32,
  height: u32,
}
#[cfg(not(feature = "websocket"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  Ok(())
}
#[cfg(feature = "websocket")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  use asynq::backend::{WebSocketBroker, WebSocketInspector};
  use asynq::serve_mux::ServeMux;
  use asynq::server::Server;
  use asynq::task::Task;
  use std::env;
  use tracing::info;
  // Initialize logging
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("asynq=debug".parse()?)
        .add_directive("websocket_consumer=debug".parse()?),
    )
    .init();
  // Get WebSocket server URL from environment or use default
  let ws_url = env::var("ASYNQ_WS_URL").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

  // Get authentication credentials from environment
  let username = env::var("ASYNQ_USERNAME").ok();
  let password = env::var("ASYNQ_PASSWORD").ok();
  // let username = Some("tenant1".to_string());
  // let password = Some("secure_pass123".to_string());
  info!(
    "Connecting to asynq-server at {} with authentication",
    ws_url
  );

  // Create a WebSocket broker with HTTP Basic authentication
  let broker =
    std::sync::Arc::new(WebSocketBroker::with_basic_auth(&ws_url, username, password).await?);
  info!("Successfully connected to asynq-server");

  // Create a ServeMux to route tasks to handlers
  let mut mux = ServeMux::new();

  // Register handler for email:send tasks
  mux.handle_async_func("email:send", |task: Task| async move {
    match task.get_payload_with_json::<EmailPayload>() {
      Ok(payload) => {
        info!("📧 Processing email task:");
        info!("   To: {}", payload.to);
        info!("   Subject: {}", payload.subject);
        info!("   Body: {}", payload.body);

        // Simulate email sending
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        info!("✅ Email sent successfully to {}", payload.to);
        Ok(())
      }
      Err(e) => {
        tracing::error!("Failed to parse email payload: {}", e);
        Err(e)
      }
    }
  });

  // Register handler for email:reminder tasks (delayed/scheduled tasks)
  mux.handle_async_func("email:reminder", |task: Task| async move {
    match task.get_payload_with_json::<EmailPayload>() {
      Ok(payload) => {
        info!("⏰ Processing delayed email reminder task:");
        info!("   To: {}", payload.to);
        info!("   Subject: {}", payload.subject);
        info!("   Body: {}", payload.body);

        // Simulate email sending
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        info!("✅ Reminder email sent successfully to {}", payload.to);
        Ok(())
      }
      Err(e) => {
        tracing::error!("Failed to parse email reminder payload: {}", e);
        Err(e)
      }
    }
  });

  // Register handler for image:resize tasks
  mux.handle_async_func("image:resize", |task: Task| async move {
    match task.get_payload_with_json::<ImagePayload>() {
      Ok(payload) => {
        info!("🖼️  Processing image resize task:");
        info!("   URL: {}", payload.url);
        info!("   Dimensions: {}x{}", payload.width, payload.height);

        // Simulate image processing
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        info!("✅ Image resized successfully");
        Ok(())
      }
      Err(e) => {
        tracing::error!("Failed to parse image payload: {}", e);
        Err(e)
      }
    }
  });

  // Register handler for report:daily tasks
  mux.handle_async_func("report:daily", |task: Task| async move {
    match task.get_payload_with_json::<EmailPayload>() {
      Ok(payload) => {
        info!("📊 Processing daily report task:");
        info!("   To: {}", payload.to);
        info!("   Subject: {}", payload.subject);

        // Simulate report generation
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        info!("✅ Daily report sent successfully");
        Ok(())
      }
      Err(e) => {
        tracing::error!("Failed to parse report payload: {}", e);
        Err(e)
      }
    }
  });

  // Configure queues with priorities
  let mut queues = std::collections::HashMap::new();
  queues.insert("critical".to_string(), 6); // Highest priority
  queues.insert("default".to_string(), 3); // Medium priority
  queues.insert("image_processing".to_string(), 2); // Lower priority
  queues.insert("low".to_string(), 1); // Lowest priority

  // Create server configuration
  let config = asynq::config::ServerConfig::new()
    .concurrency(4) // Process up to 4 tasks concurrently
    .queues(queues);

  info!("Starting consumer with 4 concurrent workers...");

  // Create WebSocket inspector (stub that directs operations to asynq-server)
  let inspector = std::sync::Arc::new(WebSocketInspector::new());

  // Create server with broker and inspector
  let mut server = Server::with_broker_and_inspector(broker, inspector, config).await?;

  info!("🚀 Consumer is ready and waiting for tasks...");
  info!("Press Ctrl+C to stop");

  // Run the server (this will block until Ctrl+C)
  server.run(mux).await?;

  Ok(())
}
