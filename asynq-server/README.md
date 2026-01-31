# Asynq Server

A standalone WebSocket server for [Asynq](https://github.com/emo-crab/asynq) that provides cross-process task queue
communication with multi-tenant authentication support.

## Features

- 🔌 **WebSocket API** - Clients connect via WebSocket, no direct database access needed
- 🏢 **Multi-Tenant Support** - Isolate tasks per tenant with automatic queue prefixing
- ⚡ **Dynamic Tenant Management** - Add/remove tenants at runtime via REST API
- 🔐 **Flexible Authentication** - No auth, single-tenant, or multi-tenant modes
- 🗄️ **Multiple Backends** - Memory (default), Redis, or PostgresSQL
- 🚀 **High Performance** - Built with Axum and Tokio async runtime
- 🔄 **Backward Compatible** - Works with existing single-tenant setups

## Quick Start

### No Authentication (Development)

```bash
cargo run --bin asynq-server
```

## License

Licensed under MIT OR GPL-3.0, same as the Asynq project.

## Links

- [Main Asynq Repository](https://github.com/emo-crab/asynq)
- [Multi-Tenant Guide](examples/MULTI_TENANT_README.md)
- [WebSocket Authentication Guide](../WEBSOCKET_AUTH_EXAMPLES.md)
