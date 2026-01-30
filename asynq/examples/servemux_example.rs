//! ServeMux 示例 - 演示如何使用 ServeMux 路由不同的任务类型
//! ServeMux Example - Demonstrates how to use ServeMux to route different task types

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}

#[derive(Serialize, Deserialize)]
struct ImageResizePayload {
  src_url: String,
  width: u32,
  height: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  use asynq::backend::RedisConnectionType;

  use asynq::serve_mux::ServeMux;

  use asynq::task::Task;

  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  // 从环境变量读取 Redis URL，默认使用 localhost
  // Read Redis URL from environment variable, default to localhost
  let redis_url =
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
  println!("🔗 Using Redis URL: {redis_url}");

  // 创建 Redis 配置
  // Create Redis configuration
  let redis_config = RedisConnectionType::single(redis_url)?;

  // 创建队列配置
  // Create queue configuration
  let mut queues = std::collections::HashMap::new();
  queues.insert("critical".to_string(), 6);
  queues.insert("default".to_string(), 3);
  queues.insert("image_processing".to_string(), 2);
  queues.insert("low".to_string(), 1);

  // 创建服务器配置
  // Create server configuration
  let server_config = asynq::config::ServerConfig::new()
    .concurrency(2) // 2 个并发工作者，与 Go 示例一致
    .queues(queues)
    .strict_priority(false)
    .task_check_interval(std::time::Duration::from_secs(1))
    .shutdown_timeout(std::time::Duration::from_secs(10));

  // 创建 ServeMux
  // Create ServeMux
  let mut mux = ServeMux::new();

  // 注册不同的任务类型处理器
  // Register handlers for different task types

  // email:send - 同步处理器
  mux.handle_func("email:send", |task: Task| {
    println!("📧 Rust Consumer: Processing email:send task");
    if let Ok(payload) = serde_json::from_slice::<EmailPayload>(task.get_payload()) {
      println!("   To: {}", payload.to);
      println!("   Subject: {}", payload.subject);
    } else {
      println!(
        "   Payload: {:?}",
        String::from_utf8_lossy(task.get_payload())
      );
    }
    Ok(())
  });

  // image:resize - 异步处理器
  mux.handle_async_func("image:resize", |task: Task| async move {
    println!("🖼️  Rust Consumer: Processing image:resize task");
    if let Ok(payload) = serde_json::from_slice::<ImageResizePayload>(task.get_payload()) {
      println!("   Source: {}", payload.src_url);
      println!("   Dimensions: {}x{}", payload.width, payload.height);
    } else {
      println!(
        "   Payload: {:?}",
        String::from_utf8_lossy(task.get_payload())
      );
    }
    Ok(())
  });

  // payment:process - 异步处理器
  mux.handle_async_func("payment:process", |task: Task| async move {
    println!("💰 Rust Consumer: Processing payment:process task");
    println!(
      "   Payload: {:?}",
      String::from_utf8_lossy(task.get_payload())
    );
    Ok(())
  });

  // report:daily - 同步处理器
  mux.handle_func("report:daily", |task: Task| {
    println!("📊 Rust Consumer: Processing report:daily task");
    println!(
      "   Payload: {:?}",
      String::from_utf8_lossy(task.get_payload())
    );
    Ok(())
  });

  // batch:process - 同步处理器
  mux.handle_func("batch:process", |task: Task| {
    println!("🔄 Rust Consumer: Processing batch:process task");
    println!(
      "   Payload: {:?}",
      String::from_utf8_lossy(task.get_payload())
    );
    Ok(())
  });

  // 创建服务器
  // Create server
  let mut server = asynq::server::ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;

  // 使用 ServeMux 运行服务器
  // Run server with ServeMux
  println!("🚀 Rust Consumer: Starting server with ServeMux...");
  println!("🔄 Server is running and waiting for tasks...");
  println!("Press Ctrl+C to gracefully shutdown");

  server.run(mux).await?;

  println!("👋 Server shutdown complete");

  Ok(())
}
