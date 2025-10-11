# 任务处理器宏特性

## 概述

本文档提供基于宏的任务处理器注册特性的快速参考。

## 启用功能

在依赖中添加 `macros` 特性：

```toml
[dependencies]
asynq = { version = "0.1", features = ["macros"] }
```

## 基本用法

### 1. 使用属性定义处理器

```rust
use asynq::{Task, error::Result, task_handler, task_handler_async};

// 同步处理器
#[task_handler("email:send")]
fn handle_email(task: Task) -> Result<()> {
    println!("处理邮件任务");
    Ok(())
}

// 异步处理器
#[task_handler_async("image:resize")]
async fn handle_image(task: Task) -> Result<()> {
    println!("处理图片任务");
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}
```

### 2. 使用 ServeMux 注册处理器

```rust
use asynq::{serve_mux::ServeMux, register_handlers, register_async_handlers};

let mut mux = ServeMux::new();

// 注册同步处理器
register_handlers!(mux, handle_email);

// 注册异步处理器
register_async_handlers!(mux, handle_image);
```

## 可用宏

### 属性宏

- `#[task_handler("pattern")]` - 标记同步函数为任务处理器
- `#[task_handler_async("pattern")]` - 标记异步函数为任务处理器

### 注册宏

- `register_handlers!(mux, handler1, handler2, ...)` - 注册一个或多个同步处理器
- `register_async_handlers!(mux, handler1, handler2, ...)` - 注册一个或多个异步处理器

## 模式匹配

任务处理器支持灵活的模式匹配，使用通配符实现精细的任务路由：

### 模式类型

1. **精确匹配**: `"email:send"` - 只匹配 "email:send"
2. **前缀通配符**: `"email:*"` - 匹配所有以 "email:" 开头的任务（例如 "email:send"、"email:deliver"）
3. **后缀通配符**: `"*:send"` - 匹配所有以 ":send" 结尾的任务（例如 "email:send"、"sms:send"）
4. **前缀和后缀**: `"email:*:done"` - 匹配类似 "email:send:done"、"email:process:done" 的任务
5. **捕获所有**: `"*"` - 匹配任何任务类型

### 模式匹配示例

```rust
use asynq::{Task, error::Result, task_handler};

// 处理所有与邮件相关的任务
#[task_handler("email:*")]
fn handle_all_emails(task: Task) -> Result<()> {
    // 处理 email:send、email:deliver、email:bounce 等
    println!("处理邮件任务: {}", task.get_type());
    Ok(())
}

// 处理所有不同渠道的发送操作
#[task_handler("*:send")]
fn handle_all_sends(task: Task) -> Result<()> {
    // 处理 email:send、sms:send、push:send 等
    println!("处理发送任务: {}", task.get_type());
    Ok(())
}

// 所有未匹配任务的捕获处理器
#[task_handler("*")]
fn handle_fallback(task: Task) -> Result<()> {
    println!("处理未匹配的任务: {}", task.get_type());
    Ok(())
}
```

### 模式匹配优先级

当多个模式可以匹配同一个任务时：
1. 首先尝试精确匹配
2. 按注册顺序尝试通配符模式
3. 使用第一个匹配的处理器

```rust
let mut mux = ServeMux::new();

// 更具体的模式应该先注册
register_handlers!(mux, 
    handle_specific_email,      // "email:send" (精确)
    handle_all_emails,          // "email:*" (前缀通配符)
    handle_all_sends,           // "*:send" (后缀通配符)
    handle_fallback             // "*" (捕获所有)
);
```

## 完整示例

参见 `examples/macro_example.rs` 和 `examples/pattern_matching_example.rs` 获取完整的工作示例。

运行示例：
```bash
cargo run --example macro_example --features macros
cargo run --example pattern_matching_example --features macros
```

## 优点

1. **声明式**: 处理器模式与函数一起声明
2. **类型安全**: 在编译时验证模式字符串
3. **人性化**: 相比手动注册更简洁的语法
4. **灵活路由**: 通配符模式实现精细的任务类型分发
5. **可选**: 不需要时可以禁用该功能
6. **熟悉**: 类似 actix-web 的路由宏

## 对比

### 不使用宏（传统方式）

```rust
let mut mux = ServeMux::new();

mux.handle_func("email:send", |task: Task| {
    println!("处理邮件");
    Ok(())
});

mux.handle_async_func("image:resize", |task: Task| async move {
    println!("处理图片");
    Ok(())
});
```

### 使用宏（新方式）

```rust
#[task_handler("email:send")]
fn handle_email(task: Task) -> Result<()> {
    println!("处理邮件");
    Ok(())
}

#[task_handler_async("image:resize")]
async fn handle_image(task: Task) -> Result<()> {
    println!("处理图片");
    Ok(())
}

let mut mux = ServeMux::new();
register_handlers!(mux, handle_email);
register_async_handlers!(mux, handle_image);
```

## 技术细节

- 宏会为模式字符串创建编译时常量
- 模式常量命名为 `__<函数名>_PATTERN`
- 注册宏使用过程宏在编译时生成正确的常量名
- 无运行时开销 - 所有模式关联都在编译时解析
- 除了过程宏使用的标准 `quote` crate 外，没有额外的外部依赖

## 与 Go 版本的兼容性

此实现的任务路由机制与 Go 版本的 hibiken/asynq 完全兼容。通过通配符模式匹配，您可以：

- 在 Rust 服务中处理特定类型的任务
- 在 Go 服务中处理其他类型的任务
- 使用捕获所有处理器作为后备机制
- 在混合语言环境中灵活分配任务处理
