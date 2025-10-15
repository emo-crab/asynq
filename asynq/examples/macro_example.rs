//! Task Handler Macro Example
//!
//! This example demonstrates how to use the task handler macros to register
//! handlers in a way similar to actix-web's routing macros.
//!
//! To run this example:
//! ```bash
//! cargo run --example macro_example --features macros
//! ```

use asynq::redis::RedisConnectionConfig;
use asynq::{
  config::ServerConfig, error::Result, register_async_handlers, register_handlers,
  serve_mux::ServeMux, server::ServerBuilder, task::Task, task_handler, task_handler_async,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ImagePayload {
  src_url: String,
  width: u32,
  height: u32,
}

// Define task handlers using the macro syntax
// Similar to actix-web's #[get("/path")] but for task types

/// Handle email sending tasks
#[task_handler("email:send")]
fn handle_email_send(task: Task) -> Result<()> {
  println!("📧 [Macro Handler] Processing email:send task");

  if let Ok(payload) = serde_json::from_slice::<EmailPayload>(task.get_payload()) {
    println!("   To: {}", payload.to);
    println!("   Subject: {}", payload.subject);
    println!("   Body: {}", payload.body);
  } else {
    println!(
      "   Raw payload: {:?}",
      String::from_utf8_lossy(task.get_payload())
    );
  }

  println!("✅ Email task completed");
  Ok(())
}

/// Handle image resize tasks (async)
#[task_handler_async("image:resize")]
async fn handle_image_resize(task: Task) -> Result<()> {
  println!("🖼️  [Macro Handler] Processing image:resize task");

  if let Ok(payload) = serde_json::from_slice::<ImagePayload>(task.get_payload()) {
    println!("   Source URL: {}", payload.src_url);
    println!("   Target size: {}x{}", payload.width, payload.height);

    // Simulate async image processing
    tokio::time::sleep(Duration::from_millis(100)).await;
  } else {
    println!(
      "   Raw payload: {:?}",
      String::from_utf8_lossy(task.get_payload())
    );
  }

  println!("✅ Image resize completed");
  Ok(())
}

/// Handle payment processing (async)
#[task_handler_async("payment:process")]
async fn handle_payment(task: Task) -> Result<()> {
  println!("💰 [Macro Handler] Processing payment:process task");
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );

  // Simulate async payment processing
  tokio::time::sleep(Duration::from_millis(50)).await;

  println!("✅ Payment processed");
  Ok(())
}

/// Handle daily report generation (sync)
#[task_handler("report:daily")]
fn handle_daily_report(task: Task) -> Result<()> {
  println!("📊 [Macro Handler] Processing report:daily task");
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );
  println!("✅ Daily report generated");
  Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  // Initialize logging
  tracing_subscriber::fmt::init();

  println!("🚀 Asynq Task Handler Macro Example");
  println!("====================================");
  println!();
  println!("This example demonstrates using attribute macros for task handlers");
  println!("similar to actix-web's #[get(\"/path\")] routing macros.");
  println!();

  // Get Redis URL from environment or use default
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Redis URL: {redis_url}");

  // Create Redis configuration
  let redis_config = RedisConnectionConfig::single(redis_url)?;

  // Create queue configuration
  let mut queues = HashMap::new();
  queues.insert("critical".to_string(), 6);
  queues.insert("default".to_string(), 3);
  queues.insert("low".to_string(), 1);

  // Create server configuration
  let server_config = ServerConfig::new()
    .concurrency(2)
    .queues(queues)
    .strict_priority(false)
    .task_check_interval(Duration::from_secs(1))
    .shutdown_timeout(Duration::from_secs(10));

  println!();
  println!("⚙️  Server Configuration:");
  println!("   • Concurrency: 2 workers");
  println!("   • Queues: critical (6), default (3), low (1)");
  println!("   • Strict priority: disabled");
  println!();

  // Create ServeMux and register handlers using convenience macros
  println!("📋 Registering task handlers...");
  let mut mux = ServeMux::new();

  // Register sync handlers with a single macro call
  register_handlers!(mux, handle_email_send, handle_daily_report);
  println!("   ✓ Registered sync handlers");

  // Register async handlers with a single macro call
  register_async_handlers!(mux, handle_image_resize, handle_payment);
  println!("   ✓ Registered async handlers");
  println!();

  // Create and start the server
  let mut server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;

  println!("🔄 Server is running and waiting for tasks...");
  println!("   The following task types are registered:");
  println!("   • email:send");
  println!("   • image:resize");
  println!("   • payment:process");
  println!("   • report:daily");
  println!();
  println!("💡 Tip: Use the producer example or client to enqueue tasks");
  println!("   Press Ctrl+C to gracefully shutdown");
  println!();

  // Run the server
  server.run(mux).await?;

  println!();
  println!("👋 Server shutdown complete");

  Ok(())
}
