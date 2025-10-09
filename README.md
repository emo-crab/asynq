# Asynq - Rust Distributed Task Queue

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)
![Go Compatible](https://img.shields.io/badge/go%20compatible-hibiken%2Fasynq-green.svg)

Asynq is a simple, reliable, and efficient distributed task queue library written in Rust, backed by Redis, inspired by [hibiken/asynq](https://github.com/hibiken/asynq).

**🔗 Fully Compatible with Go asynq**: This implementation is fully compatible with the Go version of [hibiken/asynq](https://github.com/hibiken/asynq), allowing seamless interoperation with Go services.

## 🌟 Features

- ✅ **Guaranteed at-least-once execution** - Tasks won't be lost
- ⏰ **Task scheduling** - Support for delayed and scheduled tasks
- 🔄 **Automatic retry** - Configurable retry policies for failed tasks
- 🛡️ **Fault recovery** - Automatic task recovery on worker crashes
- 🎯 **Priority queues** - Support for weighted and strict priority
- ⚡ **Low latency** - Fast Redis writes with low task enqueue latency
- 🔒 **Task deduplication** - Support for unique task options
- ⏱️ **Timeout control** - Per-task timeout and deadline support
- 📦 **Task aggregation** - Support for batch processing of multiple tasks
- 🔌 **Flexible interface** - Support for middleware and custom handlers
- ⏸️ **Queue pause** - Ability to pause/resume specific queues
- 🕒 **Periodic tasks** - Support for cron-style scheduled tasks
- 🏠 **High availability** - Support for Redis Cluster
- 🖥️ **Web UI** - Web-based management interface for queues and tasks
- 🔄 **Go compatible** - Fully compatible with Go version asynq, can be deployed together
- 🎯 **Macro support** - Attribute macros for easy handler registration (optional feature)

## 🚀 Quick Start

### Add Dependencies

Add to your `Cargo.toml`:

```toml
[dependencies]
asynq = { version = "0.1", features = ["json"] }
## Enable macro support (optional)
# asynq = { version = "0.1", features = ["json", "macros"] }
## or dev channel
#asynq = { git = "https://github.com/emo-crab/asynq", branch = "main" }
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### Basic Usage

#### Producer (Enqueue Tasks)

```rust
use asynq::{client::Client, task::Task, redis::RedisConfig};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct EmailPayload {
    to: String,
    subject: String,
    body: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Redis configuration
    let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;

    // Create client
    let client = Client::new(redis_config).await?;

    // Create task
    let payload = EmailPayload {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
        body: "Welcome to our service!".to_string(),
    };

    let task = Task::new_with_json("email:send", &payload)?;

    // Enqueue task
    let task_info = client.enqueue(task).await?;
    println!("Task enqueued with ID: {}", task_info.id);

    Ok(())
}
```

#### Consumer (Process Tasks)

```rust
use asynq::{server::Server,server::Handler,task::Task, redis::RedisConfig, config::ServerConfig};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
struct EmailPayload {
    to: String,
    subject: String,
    body: String,
}

struct EmailProcessor;

#[async_trait]
impl Handler for EmailProcessor {
    async fn process_task(&self, task: Task) -> asynq::error::Result<()> {
        match task.get_type() {
            "email:send" => {
                let payload: EmailPayload = task.get_payload_with_json()?;
                println!("Sending email to: {}", payload.to);
                // Implement actual email sending logic
                Ok(())
            }
            _ => {
                Err(asynq::error::Error::other("Unknown task type"))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Redis configuration
    let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;

    // Configure queues
    let mut queues = HashMap::new();
    queues.insert("critical".to_string(), 6);
    queues.insert("default".to_string(), 3);
    queues.insert("low".to_string(), 1);

    // Server configuration
    let config = ServerConfig::new()
        .concurrency(4)
        .queues(queues);

    // Create server
    let mut server = Server::new(redis_config, config).await?;

    // Start server
    server.run(EmailProcessor).await?;

    Ok(())
}
```

#### Using ServeMux for Task Routing

ServeMux provides Go-like task routing functionality, automatically routing tasks to different handlers based on task type:

```rust
use asynq::{serve_mux::ServeMux, task::Task, redis::RedisConfig, config::ServerConfig, server::ServerBuilder};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;

    // Create ServeMux
    let mut mux = ServeMux::new();

    // Register synchronous handler
    mux.handle_func("email:send", |task: Task| {
        println!("Processing email:send {:?}",task);
        Ok(())
    });

    // Register asynchronous handler
    mux.handle_async_func("image:resize", |task: Task| async move {
        println!("Processing image:resize {:?}",task);
        // Async processing logic
        Ok(())
    });

    mux.handle_func("payment:process", |task: Task| {
        println!("Processing payment {:?}",task);
        Ok(())
    });

    // Configure server
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    let config = ServerConfig::new().concurrency(4).queues(queues);

    // Create and run server
    let mut server = ServerBuilder::new()
        .redis_config(redis_config)
        .server_config(config)
        .build()
        .await?;

    // ServeMux implements Handler trait, can be passed directly to server.run()
    server.run(mux).await?;

    Ok(())
}
```

**Features:**
- 🎯 Automatically route tasks to corresponding handlers based on task type
- ⚡ Support for both synchronous (`handle_func`) and asynchronous (`handle_async_func`) handlers
- 🔄 Fully compatible with Go version ServeMux
- 🛡️ Type-safe with compile-time checking
- 📝 Clean API, easy to use

See `examples/servemux_example.rs` for more examples.

### Task Handler Macros (Optional Feature)

When the `macros` feature is enabled, you can use attribute macros similar to actix-web's routing macros for cleaner handler definition:

```rust
use asynq::{
    serve_mux::ServeMux, 
    task::Task, 
    task_handler, 
    task_handler_async,
    register_handlers,
    register_async_handlers,
    redis::RedisConfig, 
    config::ServerConfig, 
    server::ServerBuilder
};
use std::collections::HashMap;

// Define handlers with attribute macros
#[task_handler("email:send")]
fn handle_email(task: Task) -> asynq::error::Result<()> {
    println!("Processing email:send");
    Ok(())
}

#[task_handler_async("image:resize")]
async fn handle_image(task: Task) -> asynq::error::Result<()> {
    println!("Processing image:resize");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;
    
    // Create ServeMux and register handlers with convenience macros
    let mut mux = ServeMux::new();
    register_handlers!(mux, handle_email);
    register_async_handlers!(mux, handle_image);
    
    // Configure and run server
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    let config = ServerConfig::new().concurrency(4).queues(queues);
    
    let mut server = ServerBuilder::new()
        .redis_config(redis_config)
        .server_config(config)
        .build()
        .await?;
    
    server.run(mux).await?;
    Ok(())
}
```

**Macro Features:**
- 🎯 **Declarative syntax**: Define handlers with clean attribute syntax
- 📝 **Reduced boilerplate**: Pattern strings are stored with the function
- 🔧 **Convenient registration**: Use `register_handlers!` and `register_async_handlers!` macros
- 🌐 **Familiar pattern**: Similar to actix-web's `#[get("/path")]` routing macros

See `examples/macro_example.rs` for a complete example.

## 📚 Advanced Usage

### Delayed Tasks

```rust
use std::time::Duration;
// Execute after 5 minutes delay
client.enqueue_in(task, Duration::from_secs(300)).await?;
```

### Unique Tasks (Deduplication)

```rust
use std::time::Duration;

// Keep unique within 1 hour
let unique_task = Task::new_with_json("report:daily", &payload)?;
client.enqueue_unique(unique_task, Duration::from_secs(3600)).await?;
```

### Task Groups (Batch Processing)

```rust
// Add tasks to group for aggregation
for i in 1..=10 {
    let item_task = Task::new_with_json("batch:process", &serde_json::json!({"item": i}))?;
    client.add_to_group(item_task, "daily_batch").await?;
}
```

### Task Options

```rust
let task = Task::new_with_json("image:resize", &payload)?
    .with_queue("image_processing")     // Specify queue
    .with_max_retry(5)                  // Maximum retry attempts
    .with_timeout(Duration::from_secs(300)) // Timeout
    .with_unique_ttl(Duration::from_secs(3600)); // Uniqueness TTL
```

### Priority Queues

```rust
let mut queues = HashMap::new();
queues.insert("critical".to_string(), 6);  // Highest priority
queues.insert("default".to_string(), 3);   // Medium priority
queues.insert("low".to_string(), 1);       // Low priority

let config = ServerConfig::new()
    .queues(queues)
    .strict_priority(true); // Strict priority mode
```

## 🏗️ Architecture Design

Asynq uses a modular design with main components:

```
asynq/
├── src/
│   ├── lib.rs              # Library entry and public API
│   ├── client.rs           # Client implementation
│   ├── server.rs           # Server implementation
│   ├── serve_mux.rs         # ServeMux routing (compatible with Go serve_mux.go)
│   ├── processor.rs        # Processor implementation (compatible with Go processor.go)
│   ├── task.rs             # Task data structures
│   ├── error.rs            # Error handling
│   ├── config.rs           # Configuration management
│   ├── redis.rs            # Redis connection management
│   ├── inspector.rs        # Queue inspector
│   └── broker/             # Storage backend abstraction
│       ├── mod.rs          # Broker trait definition
│       └── redis_broker.rs # Redis implementation
├── proto/
│   └── asynq.proto         # Protocol Buffer definitions
└── examples/
    ├── producer.rs         # Producer example
    ├── consumer.rs         # Consumer example
    ├── servemux_example.rs # ServeMux usage example
    └── processor_example.rs # Processor example
```

### Core Components

- **Client**: Responsible for enqueueing tasks
- **Server**: Responsible for dequeuing and processing tasks
- **ServeMux**: Task routing multiplexer, routes tasks to different handlers by type (compatible with Go servemux.go)
- **Processor**: Task processor core, handles concurrency control and task execution (compatible with Go asynq processor.go)
- **Aggregator**: Task aggregator, aggregates tasks from the same group into batch tasks (compatible with Go asynq aggregator.go)
- **Broker**: Storage backend abstraction, currently supports Redis
- **Task**: Task data structure containing type, payload, and options
- **Handler**: Task handler trait that users need to implement
- **Inspector**: Queue and task inspection and management tool

### Processor Features

The Processor module implements task processing architecture compatible with Go asynq processor.go:

- ✅ **Semaphore concurrency control**: Uses Tokio Semaphore for precise control of concurrent workers
- ✅ **Queue priority**: Supports both strict priority and weighted priority modes
- ✅ **Task timeout**: Supports task-level and global timeout settings
- ✅ **Graceful shutdown**: Waits for all active workers to complete before shutdown
- ✅ **Automatic retry**: Failed tasks automatically retry with exponential backoff
- ✅ **Task archiving**: Tasks automatically archived after reaching max retry count

### GroupAggregator Features

The GroupAggregator module implements task aggregation functionality compatible with Go asynq aggregator.go:

- ✅ **Task grouping**: Set group label for tasks using `with_group()`
- ✅ **Batch aggregation**: Automatically aggregate tasks from the same group into a single batch task
- ✅ **Flexible triggers**: Supports three trigger conditions: grace period, max group size, max delay
- ✅ **Custom aggregation**: Customize aggregation logic via `GroupAggregator` trait
- ✅ **Functional interface**: Quickly create aggregators using `GroupAggregatorFunc`

Example usage:

```rust
use asynq::components::aggregator::GroupAggregatorFunc;

// Define aggregation function
let aggregator = GroupAggregatorFunc::new(|group, tasks| {
    // Merge multiple tasks into a single batch task
    let combined = tasks.iter()
        .map(|t| t.get_payload())
        .collect::<Vec<_>>()
        .join(&b"\n"[..]);
    Task::new("batch:process", &combined)
});

// Set on server
server.set_group_aggregator(aggregator);
```

See [GROUP_AGGREGATOR.md](docs/GROUP_AGGREGATOR.md) for more details.

## 🛠️ Configuration Options

### Server Configuration

```rust
use asynq::config::ServerConfig;
use std::time::Duration;

let config = ServerConfig::new()
    .concurrency(8)                                          // Number of concurrent workers
    .task_check_interval(Duration::from_secs(1))            // Task check interval
    .delayed_task_check_interval(Duration::from_secs(5))    // Delayed task check interval
    .shutdown_timeout(Duration::from_secs(10))              // Shutdown timeout
    .health_check_interval(Duration::from_secs(15))         // Health check interval
    .group_grace_period(Duration::from_secs(60))?           // Group aggregation grace period
    .group_max_delay(Duration::from_secs(300))              // Group max delay
    .group_max_size(100)                                    // Group max size
    .janitor_interval(Duration::from_secs(8))               // Janitor interval
    .janitor_batch_size(50);                                // Janitor batch size
```

### Redis Configuration

```rust
use asynq::redis::{RedisConfig, PoolConfig};
use std::time::Duration;

// Basic configuration
let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?;

// Advanced configuration
let pool_config = PoolConfig {
    max_size: 20,
    min_idle: Some(5),
    connection_timeout: Duration::from_secs(30),
    idle_timeout: Some(Duration::from_secs(600)),
    max_lifetime: Some(Duration::from_secs(1800)),
};

let redis_config = RedisConfig::from_url("redis://127.0.0.1:6379")?
    .with_pool_config(pool_config);
```

## 📊 Monitoring and Management

### Queue Inspector

```rust
use asynq::base::keys::TaskState;
use asynq::inspector::Inspector;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let inspector = Inspector::new(broker);

    // Get queue statistics
    let stats = inspector.get_queue_stats("default").await?;
    println!("Pending: {}, Active: {}", stats.pending, stats.active);

    // List tasks
    let tasks = inspector.list_tasks("default", TaskState::Pending, 1, 10).await?;

    // Requeue archived task
    inspector.requeue_archived_task("default", "task-id").await?;

    // Pause queue
    inspector.pause_queue("default").await?;

    Ok(())
}
```

## 🔧 Development Guide

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/emo-crab/asynq.git
cd asynq
```

2. Install dependencies:
```bash
cargo build
```

3. Start Redis:
```bash
docker run -d -p 6379:6379 redis:alpine
```

4. Run examples:
```bash
# Terminal 1: Start consumer
cargo run --example consumer

# Terminal 2: Run producer
cargo run --example producer
```

### Run Tests

```bash
# Unit tests
cargo test

# Integration tests (requires Redis)
cargo test --features integration-tests
```

## 🤝 Contributing

We welcome contributions of all kinds! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### Development Principles

- Use Rust features and best practices
- Keep the API simple and easy to use
- Provide comprehensive documentation
- Ensure code quality and test coverage
- Follow semantic versioning

## 📝 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Thanks to [hibiken/asynq](https://github.com/hibiken/asynq) for design inspiration
- Thanks to the Rust community for excellent library support

## 📞 Contact

If you have any questions or suggestions, please:

- Submit an [Issue](https://github.com/emo-crab/asynq/issues)
- Create a [Pull Request](https://github.com/emo-crab/asynq/pulls)
- Join our discussions

---

⭐ If this project helps you, please give us a star!
