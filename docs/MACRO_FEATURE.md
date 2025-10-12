# Task Handler Macro Feature

## Overview

This document provides a quick reference for the new macro-based task handler registration feature.

## Enabling the Feature

Add the `macros` feature to your dependency:

```toml
[dependencies]
asynq = { version = "0.1", features = ["macros"] }
```

## Basic Usage

### 1. Define handlers with attributes

```rust
use asynq::{Task, error::Result, task_handler, task_handler_async};

// Synchronous handler
#[task_handler("email:send")]
fn handle_email(task: Task) -> Result<()> {
    println!("Processing email task");
    Ok(())
}

// Asynchronous handler
#[task_handler_async("image:resize")]
async fn handle_image(task: Task) -> Result<()> {
    println!("Processing image task");
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}
```

### 2. Register handlers with ServeMux

```rust
use asynq::{serve_mux::ServeMux, register_handlers, register_async_handlers};

let mut mux = ServeMux::new();

// Register sync handlers
register_handlers!(mux, handle_email);

// Register async handlers  
register_async_handlers!(mux, handle_image);
```

## Available Macros

### Attribute Macros

- `#[task_handler("pattern")]` - Marks a synchronous function as a task handler
- `#[task_handler_async("pattern")]` - Marks an asynchronous function as a task handler

### Registration Macros

- `register_handlers!(mux, handler1, handler2, ...)` - Registers one or more sync handlers
- `register_async_handlers!(mux, handler1, handler2, ...)` - Registers one or more async handlers

## Pattern Matching

Task handlers support flexible pattern matching with wildcards for refined task routing:

### Pattern Types

1. **Exact Match**: `"email:send"` - Matches only "email:send"
2. **Prefix Wildcard**: `"email:*"` - Matches all tasks starting with "email:" (e.g., "email:send", "email:deliver")
3. **Suffix Wildcard**: `"*:send"` - Matches all tasks ending with ":send" (e.g., "email:send", "sms:send")
4. **Prefix and Suffix**: `"email:*:done"` - Matches tasks like "email:send:done", "email:process:done"
5. **Catch-All**: `"*"` - Matches any task type

### Pattern Matching Examples

```rust
use asynq::{Task, error::Result, task_handler};

// Handle all email-related tasks
#[task_handler("email:*")]
fn handle_all_emails(task: Task) -> Result<()> {
    // Handles email:send, email:deliver, email:bounce, etc.
    println!("Handling email task: {}", task.get_type());
    Ok(())
}

// Handle all send operations across different channels
#[task_handler("*:send")]
fn handle_all_sends(task: Task) -> Result<()> {
    // Handles email:send, sms:send, push:send, etc.
    println!("Handling send task: {}", task.get_type());
    Ok(())
}

// Catch-all handler for unmatched tasks
#[task_handler("*")]
fn handle_fallback(task: Task) -> Result<()> {
    println!("Handling unmatched task: {}", task.get_type());
    Ok(())
}
```

### Pattern Matching Priority

When multiple patterns could match a task:
1. Exact matches are tried first
2. Wildcard patterns are tried in registration order
3. The first matching handler is used

```rust
let mut mux = ServeMux::new();

// More specific patterns should be registered first
register_handlers!(mux, 
    handle_specific_email,      // "email:send" (exact)
    handle_all_emails,          // "email:*" (prefix wildcard)
    handle_all_sends,           // "*:send" (suffix wildcard)
    handle_fallback             // "*" (catch-all)
);
```

## Complete Example

See `examples/macro_example.rs` for a complete working example.

Run it with:
```bash
cargo run --example macro_example --features macros
```

## Benefits

1. **Declarative**: Handler patterns are declared right with the function
2. **Type-safe**: Pattern strings are validated at compile time
3. **Ergonomic**: Cleaner syntax compared to manual registration
4. **Flexible Routing**: Wildcard patterns enable refined task type dispatching
5. **Optional**: Feature can be disabled if not needed
6. **Familiar**: Similar to actix-web's routing macros

## Comparison

### Without Macros (Traditional)

```rust
let mut mux = ServeMux::new();

mux.handle_func("email:send", |task: Task| {
    println!("Processing email");
    Ok(())
});

mux.handle_async_func("image:resize", |task: Task| async move {
    println!("Processing image");
    Ok(())
});
```

### With Macros (New)

```rust
#[task_handler("email:send")]
fn handle_email(task: Task) -> Result<()> {
    println!("Processing email");
    Ok(())
}

#[task_handler_async("image:resize")]
async fn handle_image(task: Task) -> Result<()> {
    println!("Processing image");
    Ok(())
}

let mut mux = ServeMux::new();
register_handlers!(mux, handle_email);
register_async_handlers!(mux, handle_image);
```

## Technical Details

- The macros create compile-time constants for pattern strings
- Pattern constants are named as `__<function_name>_PATTERN`
- The registration macros use procedural macros to generate the correct constant names at compile time
- No runtime overhead - all pattern associations are resolved at compile time
- No external dependencies beyond the standard `quote` crate used for proc macros
