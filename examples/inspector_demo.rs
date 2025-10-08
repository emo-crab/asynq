//! Inspector API 示例
//! Inspector API demonstration
//!
//! 演示如何使用 Inspector API 监控和管理任务与队列，兼容 Go asynq Inspector。
//! Demonstrates how to use the Inspector API to monitor and manage tasks and queues, fully compatible with Go asynq Inspector.

use asynq::rdb::RedisBroker;
use asynq::redis::RedisConfig;
use std::sync::Arc;
use asynq::base::keys::TaskState;
use asynq::inspector::Inspector;
use asynq::proto::ServerInfo;
use asynq::rdb::inspect::Pagination;
use asynq::task::{DailyStats, QueueInfo};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  println!("🔍 Asynq Inspector API Demo");
  println!("==========================");

  // Create Redis configuration (this would connect to a real Redis instance)
  let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;

  // Create rdb and inspector
  let broker: Arc<RedisBroker> = Arc::new(RedisBroker::new(redis_config)?);
  let inspector = Inspector::from_broker(broker);

  println!("\n📊 Queue Information:");
  println!("--------------------");

  // Get all queues (Go: inspector.Queues())
  match inspector.get_queues().await {
    Ok(queues) => {
      for queue_name in &queues {
        println!("🔵 Queue: {}", queue_name);

        // Get detailed queue info (Go: inspector.GetQueueInfo())
        match inspector.get_queue_info(queue_name).await {
          Ok(info) => print_queue_info(&info),
          Err(e) => println!("   ❌ Error getting queue info: {}", e),
        }
      }
    }
    Err(e) => println!("❌ Error getting queues: {}", e),
  }

  println!("\n🖥️  Server Information:");
  println!("----------------------");

  // Get all servers (Go: inspector.Servers())
  match inspector.get_servers().await {
    Ok(servers) => {
      for server in &servers {
        print_server_info(server);
      }
    }
    Err(e) => println!("❌ Error getting servers: {}", e),
  }

  println!("\n📈 Historical Data:");
  println!("------------------");

  // Get history for the default queue (Go: inspector.History())
  match inspector.get_history("default", 7).await {
    Ok(history) => {
      for stats in &history {
        print_daily_stats(stats);
      }
    }
    Err(e) => println!("❌ Error getting history: {}", e),
  }

  println!("\n📋 Task Management:");
  println!("------------------");

  // List tasks by different states (Go: inspector.ListPendingTasks(), etc.)
  let states_to_check = [
    ("pending", TaskState::Pending),
    ("active", TaskState::Active),
    ("scheduled", TaskState::Scheduled),
    ("retry", TaskState::Retry),
    ("archived", TaskState::Archived),
    ("completed", TaskState::Completed),
  ];

  for (state_name, state) in &states_to_check {
    match inspector.list_tasks("default", *state, Pagination::default()).await {
      Ok(tasks) => {
        println!("📝 {} tasks: {} found", state_name, tasks.len());
      }
      Err(e) => println!("❌ Error listing {} tasks: {}", state_name, e),
    }
  }

  // Demonstrate convenience methods that match Go API exactly
  println!("\n🔧 Convenience Methods (matching Go API):");
  println!("------------------------------------------");

  // These methods match Go inspector methods exactly:
  // inspector.ListPendingTasks() → list_pending_tasks()
  // inspector.ListActiveTasks() → list_active_tasks()
  // inspector.RunAllArchivedTasks() → run_all_archived_tasks()
  // etc.

  match inspector.list_pending_tasks("default").await {
    Ok(tasks) => println!("✅ list_pending_tasks(): {} tasks", tasks.len()),
    Err(e) => println!("❌ list_pending_tasks() error: {}", e),
  }

  match inspector.list_active_tasks("default").await {
    Ok(tasks) => println!("✅ list_active_tasks(): {} tasks", tasks.len()),
    Err(e) => println!("❌ list_active_tasks() error: {}", e),
  }

  println!("\n✨ Inspector API is fully compatible with Go asynq!");
  println!("   All Go inspector methods have Rust equivalents.");

  Ok(())
}

fn print_queue_info(info: &QueueInfo) {
  println!("   📊 Size: {}, Groups: {}", info.size, info.groups);
  println!(
    "   📈 Pending: {}, Active: {}, Scheduled: {}",
    info.pending, info.active, info.scheduled
  );
  println!(
    "   🔄 Retry: {}, Archived: {}, Completed: {}",
    info.retry, info.archived, info.completed
  );
  println!(
    "   💾 Memory: {} bytes, Latency: {:?}",
    info.memory_usage, info.latency
  );
  println!("   ⏸️  Paused: {}", info.paused);
}

fn print_server_info(server: &ServerInfo) {
  println!(
    "🖥️  Server: {} ({}:{})",
    server.server_id, server.host, server.pid
  );
  println!(
    "   🔧 Concurrency: {}, Status: {}",
    server.concurrency, server.status
  );
  println!("   📅 Started: {}", server.status);
  println!("   👥 Active Workers: {}", server.active_worker_count);
  println!("   🔄 Strict Priority: {}", server.strict_priority);
}

fn print_daily_stats(stats: &DailyStats) {
  println!(
    "📅 {}: {} processed, {} failed (Queue: {})",
    stats.date.format("%Y-%m-%d"),
    stats.processed,
    stats.failed,
    stats.queue
  );
}
