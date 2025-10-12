//! Pattern Matching Example
//!
//! This example demonstrates the wildcard pattern matching feature for task routing.
//! It shows how to use wildcards to route tasks based on task type patterns.
//!
//! To run this example:
//! ```bash
//! cargo run --example pattern_matching_example --features macros
//! ```

use asynq::{
  config::ServerConfig, error::Result, redis::RedisConfig, register_handlers, serve_mux::ServeMux,
  server::ServerBuilder, task::Task, task_handler,
};
use std::collections::HashMap;
use std::time::Duration;

// Handle all email-related tasks with a prefix wildcard
#[task_handler("email:*")]
fn handle_email_tasks(task: Task) -> Result<()> {
  println!("📧 [Email Handler] Processing: {}", task.get_type());
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );
  println!("   ✓ Handled by email:* pattern");
  Ok(())
}

// Handle all SMS-related tasks with a prefix wildcard
#[task_handler("sms:*")]
fn handle_sms_tasks(task: Task) -> Result<()> {
  println!("📱 [SMS Handler] Processing: {}", task.get_type());
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );
  println!("   ✓ Handled by sms:* pattern");
  Ok(())
}

// Handle all tasks ending with :urgent using suffix wildcard
#[task_handler("*:urgent")]
fn handle_urgent_tasks(task: Task) -> Result<()> {
  println!("🚨 [Urgent Handler] Processing: {}", task.get_type());
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );
  println!("   ✓ Handled by *:urgent pattern");
  Ok(())
}

// Handle all notification completion tasks with prefix and suffix wildcards
#[task_handler("notification:*:complete")]
fn handle_notification_complete(task: Task) -> Result<()> {
  println!("✅ [Notification Complete] Processing: {}", task.get_type());
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );
  println!("   ✓ Handled by notification:*:complete pattern");
  Ok(())
}

// Catch-all handler for any unmatched tasks
#[task_handler("*")]
fn handle_fallback(task: Task) -> Result<()> {
  println!("🔄 [Fallback Handler] Processing: {}", task.get_type());
  println!(
    "   Payload: {:?}",
    String::from_utf8_lossy(task.get_payload())
  );
  println!("   ✓ Handled by * (catch-all) pattern");
  Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
  // Initialize logging
  tracing_subscriber::fmt::init();

  println!("╔══════════════════════════════════════════════════════════════╗");
  println!("║         Pattern Matching Example - Task Router              ║");
  println!("╚══════════════════════════════════════════════════════════════╝");
  println!();

  // Configure Redis connection
  let redis_config = RedisConfig::new("redis://127.0.0.1:6379")?;

  // Configure queue priorities
  let mut queues = HashMap::new();
  queues.insert("default".to_string(), 5);
  queues.insert("urgent".to_string(), 10);

  // Create server configuration
  let server_config = ServerConfig::new()
    .concurrency(2)
    .queues(queues)
    .strict_priority(false)
    .task_check_interval(Duration::from_secs(1))
    .shutdown_timeout(Duration::from_secs(5));

  println!("⚙️  Server Configuration:");
  println!("   • Concurrency: 2 workers");
  println!("   • Queues: default (5), urgent (10)");
  println!();

  // Create ServeMux and register handlers with patterns
  println!("📋 Registering task handlers with patterns...");
  let mut mux = ServeMux::new();

  // Register handlers in order of specificity
  // More specific patterns should come before more general ones
  register_handlers!(
    mux,
    handle_urgent_tasks,          // *:urgent (suffix wildcard)
    handle_notification_complete, // notification:*:complete (prefix + suffix)
    handle_email_tasks,           // email:* (prefix wildcard)
    handle_sms_tasks,             // sms:* (prefix wildcard)
    handle_fallback               // * (catch-all - should be last)
  );

  println!("   ✓ Registered patterns:");
  println!("      - *:urgent (all urgent tasks)");
  println!("      - notification:*:complete (notification completion tasks)");
  println!("      - email:* (all email tasks)");
  println!("      - sms:* (all SMS tasks)");
  println!("      - * (catch-all fallback)");
  println!();

  // Create and start server
  println!("🚀 Starting server...");
  let mut server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;

  println!("   ✓ Server started successfully");
  println!();
  println!("════════════════════════════════════════════════════════════════");
  println!("Server is running. Send tasks to see pattern matching in action!");
  println!();
  println!("Example tasks to enqueue from another terminal:");
  println!("  - email:send      → Matches 'email:*'");
  println!("  - email:deliver   → Matches 'email:*'");
  println!("  - sms:send        → Matches 'sms:*'");
  println!("  - payment:urgent  → Matches '*:urgent'");
  println!("  - notification:email:complete → Matches 'notification:*:complete'");
  println!("  - report:generate → Matches '*' (catch-all)");
  println!();
  println!("Press Ctrl+C to stop the server");
  println!("════════════════════════════════════════════════════════════════");

  // Run the server and wait for shutdown
  server.run(mux).await?;

  println!();
  println!("👋 Server shutdown complete");

  Ok(())
}
