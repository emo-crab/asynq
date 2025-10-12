# Asynq Macros

Procedural macros for the `asynq` task queue library, providing a convenient way to define and register task handlers using attribute macros similar to actix-web's routing macros.

## Features

This crate provides the following macros:

- `#[task_handler("pattern")]` - Define a synchronous task handler
- `#[task_handler_async("pattern")]` - Define an asynchronous task handler

## Usage

Add `asynq` with the `macros` feature to your `Cargo.toml`:

```toml
[dependencies]
asynq = { version = "0.1", features = ["macros"] }
```

Then use the macros to define your task handlers:

```rust
use asynq::{task::Task, error::Result, task_handler, task_handler_async};

// Define a synchronous task handler
#[task_handler("email:send")]
fn handle_email_send(task: Task) -> Result<()> {
    println!("Sending email...");
    Ok(())
}

// Define an asynchronous task handler
#[task_handler_async("image:resize")]
async fn handle_image_resize(task: Task) -> Result<()> {
    println!("Resizing image...");
    // Async operations here
    Ok(())
}
```

Register the handlers with a `ServeMux`:

```rust
use asynq::{serve_mux::ServeMux, register_handlers, register_async_handlers};

let mut mux = ServeMux::new();

// Register sync handlers
register_handlers!(mux, handle_email_send);

// Register async handlers
register_async_handlers!(mux, handle_image_resize);
```

## Benefits

- **Declarative syntax**: Define handlers with a clean attribute syntax
- **Type safety**: Pattern strings are validated at compile time
- **Reduced boilerplate**: Automatic pattern association with handlers
- **Similar to web frameworks**: Familiar pattern for developers coming from actix-web or similar frameworks
