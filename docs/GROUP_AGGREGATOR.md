# Group Aggregator 使用指南
# Group Aggregator Usage Guide

## 概述 / Overview

Group Aggregator 允许你将多个相关任务聚合成一个批处理任务，这对于优化资源使用和提高处理效率非常有用。

Group Aggregator allows you to aggregate multiple related tasks into a single batch processing task, which is useful for optimizing resource usage and improving processing efficiency.

## 对应 Go asynq 接口 / Corresponds to Go asynq Interface

本实现对应 Go asynq 的 `GroupAggregator` 接口：

This implementation corresponds to Go asynq's `GroupAggregator` interface:

```go
type GroupAggregator interface {
    Aggregate(group string, tasks []*Task) *Task
}
```

## 使用步骤 / Usage Steps

### 1. 定义聚合函数 / Define Aggregation Function

创建一个函数来聚合同组的任务：

Create a function to aggregate tasks from the same group:

```rust
use asynq::Task;

fn aggregate_tasks(group: &str, tasks: Vec<Task>) -> asynq::Result<Task> {
    println!("Aggregating {} tasks from group '{}'", tasks.len(), group);
    
    // 合并任务的 payload
    // Combine task payloads
    let mut combined_payload = String::new();
    for task in tasks {
        if let Ok(payload_str) = std::str::from_utf8(task.get_payload()) {
            combined_payload.push_str(payload_str);
            combined_payload.push('\n');
        }
    }
    
    // 创建聚合后的任务
    // Create aggregated task
    Task::new("batch:process", combined_payload.as_bytes())
}
```

### 2. 配置服务器 / Configure Server

启用组聚合器并设置聚合参数：

Enable group aggregator and set aggregation parameters:

```rust
use asynq::{config::ServerConfig, components::aggregator::GroupAggregatorFunc};
use std::collections::HashMap;
use std::time::Duration;

// 配置服务器
// Configure server
let mut queues = HashMap::new();
queues.insert("default".to_string(), 1);

let server_config = ServerConfig::default()
    .concurrency(2)
    .queues(queues)
    .enable_group_aggregator(true)
    .group_grace_period(Duration::from_secs(10))
    .group_max_size(5)
    .group_max_delay(Duration::from_secs(30));
```

### 3. 设置聚合器 / Set Aggregator

创建并设置 GroupAggregator：

Create and set the GroupAggregator:

```rust
use asynq::server::ServerBuilder;

let mut server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;

// 设置组聚合器
// Set group aggregator
let aggregator = GroupAggregatorFunc::new(aggregate_tasks);
server.set_group_aggregator(aggregator);
```

### 4. 创建带组标签的任务 / Create Tasks with Group Labels

在客户端创建任务时指定组标签：

Specify group labels when creating tasks on the client side:

```rust
use asynq::{Client, Task};

let client = Client::new(redis_config).await?;

// 创建属于 "daily-digest" 组的任务
// Create tasks belonging to the "daily-digest" group
let task = Task::new("email:send", payload)?
    .with_queue("default")
    .with_group("daily-digest")
    .with_group_grace_period(Duration::from_secs(5));

client.enqueue(task).await?;
```

## 聚合触发条件 / Aggregation Trigger Conditions

聚合会在以下任一条件满足时触发：

Aggregation is triggered when any of the following conditions is met:

1. **Grace Period（宽限期）**: 从第一个任务加入组后经过指定时间
   - Grace period: Specified time elapsed since the first task joined the group

2. **Max Size（最大组大小）**: 组中的任务数达到指定上限
   - Max size: Number of tasks in the group reaches the specified limit

3. **Max Delay（最大延迟）**: 任务在组中等待的最长时间
   - Max delay: Maximum time a task waits in the group

## 完整示例 / Complete Examples

### 消费者示例 / Consumer Example

运行带有组聚合器的消费者：

Run consumer with group aggregator:

```bash
cargo run --example group_aggregator_example
```

### 生产者示例 / Producer Example

创建带组标签的任务：

Create tasks with group labels:

```bash
cargo run --example group_task_producer
```

## 注意事项 / Notes

1. **队列选项被忽略**: 聚合后的任务总是入队到原组所属的队列
   - Queue option ignored: Aggregated task is always enqueued to the group's original queue

2. **错误处理**: 如果聚合函数返回错误，原始任务不会被处理
   - Error handling: If aggregation function returns error, original tasks are not processed

3. **并发安全**: GroupAggregator 必须实现 `Send + Sync`
   - Concurrency safety: GroupAggregator must implement `Send + Sync`

## 参考 / References

- Go asynq GroupAggregator: https://github.com/hibiken/asynq/blob/master/aggregator.go
- 示例代码 / Example code: `examples/group_aggregator_example.rs`
- 测试代码 / Test code: `src/components/aggregator.rs` (tests module)
