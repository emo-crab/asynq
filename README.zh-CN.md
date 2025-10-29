# Asynq - Rust 分布式任务队列

[English](README.md) | [中文](README.md)

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)
![Go Compatible](https://img.shields.io/badge/go%20compatible-hibiken%2Fasynq-green.svg)

Asynq 是一个用 Rust 编写的简单、可靠、高效的分布式任务队列库，基于 Redis 存储，灵感来自 [hibiken/asynq](https://github.com/hibiken/asynq)。

**🔗 完全兼容 Go 版本 asynq**: 本实现与 Go 版本的 [hibiken/asynq](https://github.com/hibiken/asynq) 完全兼容，可以与 Go 服务无缝协作。

## 🌟 特性

- ✅ **保证至少执行一次** - 任务不会丢失
- ⏰ **任务调度** - 支持延迟任务和定时任务
- 🔄 **自动重试** - 失败任务可配置重试策略
- 🛡️ **故障恢复** - 工作者崩溃时自动恢复任务
- 🎯 **优先级队列** - 支持加权和严格优先级
- ⚡ **低延迟** - Redis 写入速度快，任务入队延迟低
- 🔒 **任务去重** - 支持唯一任务选项
- ⏱️ **超时控制** - 每个任务支持超时和截止时间
- 📦 **任务聚合** - 支持批量处理多个任务
- 🔌 **灵活接口** - 支持中间件和自定义处理器
- ⏸️ **队列暂停** - 可以暂停/恢复特定队列
- 🕒 **周期性任务** - 支持 cron 风格的定时任务
- 🏠 **高可用性** - 支持 Redis Cluster
- 🖥️ **Web UI** - 提供队列和任务的 Web 管理界面
- 🔄 **Go 兼容** - 与 Go 版本 asynq 完全兼容，可混合部署
- 🎯 **宏支持** - 提供类似 actix-web 的属性宏，方便注册处理器（可选功能）

## 🚀 快速开始

### 添加依赖

在你的 `Cargo.toml`中添加：

```toml
[dependencies]
asynq = { version = "0.1", features = ["json"] }
## 启用宏支持（可选）
# asynq = { version = "0.1", features = ["json", "macros"] }
## or dev channel
#asynq = { git = "https://github.com/emo-crab/asynq", branch = "main" }
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### 基本用法

#### 生产者 (发送任务)

```rust
use asynq::{client::Client, task::Task, redis::RedisConnectionType};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct EmailPayload {
    to: String,
    subject: String,
    body: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建 Redis 配置
    let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379")?;
    
    // 创建客户端
    let client = Client::new(redis_config).await?;
    
    // 创建任务
    let payload = EmailPayload {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
        body: "Welcome to our service!".to_string(),
    };

    let task = Task::new_with_json("email:send", &payload)?;
    
    // 发送任务到队列
    let task_info = client.enqueue(task).await?;
    println!("Task enqueued with ID: {}", task_info.id);

    Ok(())
}
```

#### 消费者 (处理任务)

```rust
use asynq::{server::Server,server::Handler,task::Task, redis::RedisConnectionType, config::ServerConfig};
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
                // 执行实际的邮件发送逻辑
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
    // Redis 配置
    let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379")?;
    
    // 配置队列
    let mut queues = HashMap::new();
    queues.insert("critical".to_string(), 6);
    queues.insert("default".to_string(), 3);
    queues.insert("low".to_string(), 1);
    
    // 服务器配置
    let config = ServerConfig::new()
        .concurrency(4)
        .queues(queues);
    
    // 创建服务器
    let mut server = Server::new(redis_config, config).await?;
    
    // 启动服务器
    server.run(EmailProcessor).await?;

    Ok(())
}
```

#### 使用 ServeMux 路由任务

ServeMux 提供了类似 Go 版本的任务路由功能，可以根据任务类型自动路由到不同的处理器：

```rust
use asynq::{serve_mux::ServeMux, task::Task, redis::RedisConnectionType, config::ServerConfig, server::ServerBuilder};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379")?;
    
    // 创建 ServeMux
    let mut mux = ServeMux::new();
    
    // 注册同步处理器
    mux.handle_func("email:send", |task: Task| {
        println!("Processing email:send {:?}",task);
        Ok(())
    });
    
    // 注册异步处理器
    mux.handle_async_func("image:resize", |task: Task| async move {
        println!("Processing image:resize {:?}",task);
        // 异步处理逻辑
        Ok(())
    });

    mux.handle_func("payment:process", |task: Task| {
        println!("Processing payment {:?}",task);
        Ok(())
    });
    
    // 配置服务器
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    let config = ServerConfig::new().concurrency(4).queues(queues);
    
    // 创建并运行服务器
    let mut server = ServerBuilder::new()
        .redis_config(redis_config)
        .server_config(config)
        .build()
        .await?;
    
    // ServeMux 实现了 Handler trait，可以直接传递给 server.run()
    server.run(mux).await?;

    Ok(())
}
```

**特点:**
- 🎯 自动根据任务类型路由到对应的处理器
- ⚡ 支持同步 (`handle_func`) 和异步 (`handle_async_func`) 处理器
- 🔄 与 Go 版本的 ServeMux 完全兼容
- 🛡️ 类型安全，编译时检查
- 📝 简洁的 API，易于使用

更多示例请参考 `examples/servemux_example.rs`。

### 任务处理器宏（可选功能）

启用 `macros` 功能后，可以使用类似 actix-web 路由宏的属性宏，更简洁地定义处理器：

```rust
use asynq::{
    serve_mux::ServeMux, 
    task::Task, 
    task_handler, 
    task_handler_async,
    register_handlers,
    register_async_handlers,
    redis::RedisConnectionType, 
    config::ServerConfig, 
    server::ServerBuilder
};
use std::collections::HashMap;

// 使用属性宏定义处理器
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
    let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379")?;
    
    // 创建 ServeMux 并使用便捷宏注册处理器
    let mut mux = ServeMux::new();
    register_handlers!(mux, handle_email);
    register_async_handlers!(mux, handle_image);
    
    // 配置并运行服务器
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

**宏特性:**
- 🎯 **声明式语法**: 使用简洁的属性语法定义处理器
- 📝 **减少样板代码**: 模式字符串与函数自动关联
- 🔧 **便捷注册**: 使用 `register_handlers!` 和 `register_async_handlers!` 宏
- 🌐 **熟悉的模式**: 类似 actix-web 的 `#[get("/path")]` 路由宏

完整示例请参考 `examples/macro_example.rs`。

## 📚 高级用法

### 延迟任务

```rust
use std::time::Duration;
// 延迟 5 分钟执行
client.enqueue_in(task, Duration::from_secs(300)).await?;
```

### 唯一任务 (去重)

```rust
use std::time::Duration;

// 在 1 小时内保持唯一性
let unique_task = Task::new_with_json("report:daily", &payload)?;
client.enqueue_unique(unique_task, Duration::from_secs(3600)).await?;
```

### 任务组 (批处理)

```rust
// 将任务添加到组中进行聚合
for i in 1..=10 {
    let item_task = Task::new_with_json("batch:process", &serde_json::json!({"item": i}))?;
    client.add_to_group(item_task, "daily_batch").await?;
}
```

### 任务选项

```rust
let task = Task::new_with_json("image:resize", &payload)?
    .with_queue("image_processing")     // 指定队列
    .with_max_retry(5)                  // 最大重试次数
    .with_timeout(Duration::from_secs(300)) // 超时时间
    .with_unique_ttl(Duration::from_secs(3600)); // 唯一性TTL
```

### 优先级队列

```rust
let mut queues = HashMap::new();
queues.insert("critical".to_string(), 6);  // 最高优先级
queues.insert("default".to_string(), 3);   // 中等优先级
queues.insert("low".to_string(), 1);       // 低优先级

let config = ServerConfig::new()
    .queues(queues)
    .strict_priority(true); // 严格优先级模式
```

## 🏗️ 架构设计

Asynq 采用模块化设计，主要组件包括：

```
asynq/
├── src/
│   ├── lib.rs              # 库入口和公共API
│   ├── client.rs           # 客户端实现
│   ├── server.rs           # 服务器实现
│   ├── serve_mux.rs         # ServeMux 路由实现 (兼容 Go servemux.go)
│   ├── processor.rs        # 处理器实现 (兼容 Go processor.go)
│   ├── task.rs             # 任务相关数据结构
│   ├── error.rs            # 错误处理
│   ├── config.rs           # 配置管理
│   ├── redis.rs            # Redis连接管理
│   ├── inspector.rs        # 队列检查器
│   └── broker/             # 存储后端抽象层
│       ├── mod.rs          # Broker特征定义
│       └── redis_broker.rs # Redis实现
├── proto/
│   └── asynq.proto         # Protocol Buffer定义
└── examples/
    ├── producer.rs         # 生产者示例
    ├── consumer.rs         # 消费者示例
    ├── servemux_example.rs # ServeMux 使用示例
    └── processor_example.rs # 处理器示例
```

### 核心组件

- **Client**: 负责将任务加入队列
- **Server**: 负责从队列中取出任务并处理
- **ServeMux**: 任务路由多路复用器，根据任务类型路由到不同处理器（兼容 Go servemux.go）
- **Processor**: 任务处理器核心，负责并发控制和任务执行（兼容 Go asynq processor.go）
- **Aggregator**: 任务聚合器，将同组任务聚合为批处理任务（兼容 Go asynq aggregator.go）
- **Broker**: 存储后端抽象层，目前支持Redis
- **Task**: 任务数据结构，包含类型、负载和选项
- **Handler**: 任务处理器特征，用户需要实现此接口
- **Inspector**: 队列和任务的检查和管理工具

### Processor 特性

Processor 模块实现了与 Go asynq processor.go 兼容的任务处理架构：

- ✅ **信号量并发控制**: 使用 Tokio Semaphore 精确控制并发工作者数量
- ✅ **队列优先级**: 支持严格优先级和加权优先级两种模式
- ✅ **任务超时**: 支持任务级别和全局超时设置
- ✅ **优雅关闭**: 等待所有活跃工作者完成后再关闭
- ✅ **自动重试**: 失败任务自动重试，支持指数退避策略
- ✅ **任务归档**: 达到最大重试次数后自动归档任务

### GroupAggregator 特性

GroupAggregator 模块实现了与 Go asynq aggregator.go 兼容的任务聚合功能：

- ✅ **任务分组**: 通过 `with_group()` 为任务设置组标签
- ✅ **批量聚合**: 自动将同组任务聚合成单个批处理任务
- ✅ **灵活触发**: 支持宽限期、最大组大小、最大延迟三种触发条件
- ✅ **自定义聚合**: 通过 `GroupAggregator` trait 自定义聚合逻辑
- ✅ **函数式接口**: 使用 `GroupAggregatorFunc` 快速创建聚合器

示例用法：

```rust
use asynq::components::aggregator::GroupAggregatorFunc;

// 定义聚合函数
let aggregator = GroupAggregatorFunc::new(|group, tasks| {
    // 将多个任务合并为一个批处理任务
    let combined = tasks.iter()
        .map(|t| t.get_payload())
        .collect::<Vec<_>>()
        .join(&b"\n"[..]);
    Task::new("batch:process", &combined)
});

// 设置到服务器
server.set_group_aggregator(aggregator);
```

更多详情请参阅 [GROUP_AGGREGATOR.md](docs/GROUP_AGGREGATOR.md)

## 🛠️ 配置选项

### 服务器配置

```rust
use asynq::config::ServerConfig;
use std::time::Duration;

let config = ServerConfig::new()
    .concurrency(8)                                          // 并发工作者数量
    .task_check_interval(Duration::from_secs(1))            // 任务检查间隔
    .delayed_task_check_interval(Duration::from_secs(5))    // 延迟任务检查间隔
    .shutdown_timeout(Duration::from_secs(10))              // 关闭超时
    .health_check_interval(Duration::from_secs(15))         // 健康检查间隔
    .group_grace_period(Duration::from_secs(60))?           // 组聚合宽限期
    .group_max_delay(Duration::from_secs(300))              // 组最大延迟
    .group_max_size(100)                                    // 组最大大小
    .janitor_interval(Duration::from_secs(8))               // 清理间隔
    .janitor_batch_size(50);                                // 清理批量大小
```

### Redis 配置

```rust
use asynq::redis::RedisConnectionType;
// 基本配置
let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379")?;
let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
let redis_config = RedisConnectionType::cluster(nodes)?;
```

## 📊 监控和管理

### 队列检查器

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

## 🔧 开发指南

### 本地开发

1. 克隆仓库：
```bash
git clone https://github.com/emo-crab/asynq.git
cd asynq
```

2. 安装依赖：
```bash
cargo build
```

3. 启动 Redis：
```bash
docker run -d -p 6379:6379 redis:alpine
```

4. 运行示例：
```bash
# 终端1: 启动消费者
cargo run --example consumer

# 终端2: 运行生产者
cargo run --example producer
```

### 运行测试

```bash
# 单元测试
cargo test

# 集成测试（需要Redis）
cargo test --features integration-tests
```

## 🤝 贡献

我们欢迎各种形式的贡献！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详细信息。

### 开发原则

- 使用 Rust 的特性和最佳实践
- 保持API的简洁和易用
- 提供完整的中文文档
- 确保代码质量和测试覆盖率
- 遵循语义化版本

## 📝 许可证

本项目采用[MIT License](LICENSE-MIT) OR [GPL License](LICENSE-GPL)。

## 🙏 致谢

- 感谢 [hibiken/asynq](https://github.com/hibiken/asynq) 提供的设计灵感
- 感谢 Rust 社区提供的优秀库支持

## 📞 联系

如果你有任何问题或建议，请：

- 提交 [Issue](https://github.com/emo-crab/asynq/issues)
- 发起 [Pull Request](https://github.com/emo-crab/asynq/pulls)
- 加入我们的讨论

---

⭐ 如果这个项目对你有帮助，请给我们一个 star！