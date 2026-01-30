//! WebSocket Producer Example with HTTP Basic Authentication
//!
//! This example demonstrates how to use the WebSocket backend to enqueue tasks
//! with HTTP Basic authentication. A producer connects to an asynq-server via WebSocket
//! and sends tasks for processing.
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
//! 2. Run this producer:
//!    ```bash
//!    export ASYNQ_USERNAME="admin"
//!    export ASYNQ_PASSWORD="your-secret-password"
//!    cargo run --example websocket_producer --features websocket,json
//!    ```

use asynq::client::Client;
use asynq::task::Task;
use serde::{Deserialize, Serialize};
use std::env;
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ImagePayload {
  url: String,
  width: u32,
  height: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Initialize logging
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("asynq=debug".parse()?)
        .add_directive("websocket_producer=debug".parse()?),
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

  // Create a WebSocket client with HTTP Basic authentication
  let client = match (username, password) {
    (Some(user), Some(pass)) => Client::new_with_websocket_basic_auth(&ws_url, user, pass).await?,
    _ => Client::new_with_websocket(&ws_url).await?,
  };

  info!("Successfully connected to asynq-server");

  // Example 1: Enqueue an email task
  let email_payload = EmailPayload {
    to: "user@example.com".to_string(),
    subject: "Welcome to Asynq!".to_string(),
    body: "This is a test email sent via WebSocket with authentication.".to_string(),
  };

  let email_task = Task::new_with_json("email:send", &email_payload)?;
  let task_info = client.enqueue(email_task).await?;
  info!(
    "Email task enqueued: ID={}, Queue={}",
    task_info.id, task_info.queue
  );

  // Example 2: Enqueue an image processing task with custom queue
  let image_payload = ImagePayload {
    url: "https://example.com/image.jpg".to_string(),
    width: 800,
    height: 600,
  };

  let image_task = Task::new_with_json("image:resize", &image_payload)?
    .with_queue("image_processing")
    .with_max_retry(5);

  let task_info = client.enqueue(image_task).await?;
  info!(
    "Image task enqueued: ID={}, Queue={}",
    task_info.id, task_info.queue
  );

  // Example 3: Schedule a task for later
  let scheduled_payload = EmailPayload {
    to: "scheduled@example.com".to_string(),
    subject: "Scheduled Email".to_string(),
    body: "This email is scheduled for later delivery.".to_string(),
  };

  let scheduled_task = Task::new_with_json("email:send", &scheduled_payload)?;
  let task_info = client
    .enqueue_in(scheduled_task, std::time::Duration::from_secs(300))
    .await?;
  info!(
    "Scheduled task enqueued: ID={}, will run in 5 minutes",
    task_info.id
  );

  // Example 4: Enqueue a unique task (prevents duplicates)
  let unique_payload = EmailPayload {
    to: "unique@example.com".to_string(),
    subject: "Daily Report".to_string(),
    body: "This is your daily report.".to_string(),
  };

  let unique_task = Task::new_with_json("report:daily", &unique_payload)?;
  match client
    .enqueue_unique(unique_task, std::time::Duration::from_secs(3600))
    .await
  {
    Ok(task_info) => {
      info!("Unique task enqueued: ID={}", task_info.id);
    }
    Err(e) => {
      info!("Unique task already exists or error: {}", e);
    }
  }
  // 示例 3: 调度延迟任务
  // Example 3: Schedule delayed task
  let delayed_email_bin = serde_json::to_vec(&email_payload)?;
  let delayed_email = asynq::task::Task::new("email:reminder", &delayed_email_bin).unwrap();

  // 5 分钟后执行
  // Execute after 5 minutes
  match client
    .enqueue_in(delayed_email, std::time::Duration::from_secs(30))
    .await
  {
    Ok(task_info) => {
      info!("Delayed email task scheduled: ID = {}", task_info.id);
    }
    Err(e) => {
      error!("Failed to schedule delayed task: {e}");
    }
  }
  info!("All tasks enqueued successfully!");

  // Close the WebSocket connection properly to avoid "Connection reset without closing handshake" error
  client.close().await?;
  info!("WebSocket connection closed gracefully");

  Ok(())
}
