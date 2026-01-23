# Asynq Server

A standalone WebSocket server for [Asynq](https://github.com/emo-crab/asynq) that provides cross-process task queue communication with multi-tenant authentication support.

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

### Single-Tenant Authentication (Production)

```bash
export ASYNQ_USERNAME="admin"
export ASYNQ_PASSWORD="your-secret-password"
cargo run --bin asynq-server
```

### Multi-Tenant Authentication (Enterprise)

Create a tenant configuration file `tenants.json`:

```json
[
  {
    "id": "tenant1",
    "username": "tenant1_user",
    "password": "tenant1_password",
    "queue_prefix": "tenant1"
  },
  {
    "id": "tenant2",
    "username": "tenant2_user",
    "password": "tenant2_password",
    "queue_prefix": "tenant2"
  }
]
```

Start the server:

```bash
export ASYNQ_TENANT_CONFIG=tenants.json
cargo run --bin asynq-server
```

## Multi-Tenant Architecture

```
┌──────────────────┐    tenant1:default    ┌────────────────────────┐
│  Tenant 1 Client │ ──────────────────────▶│                        │
│  (user: tenant1) │                        │                        │
└──────────────────┘                        │                        │
                                            │   asynq-server         │
┌──────────────────┐    tenant2:default    │   (Multi-tenant)       │
│  Tenant 2 Client │ ──────────────────────▶│                        │
│  (user: tenant2) │                        │  Queue Isolation:      │
└──────────────────┘                        │  - tenant1:default     │
                                            │  - tenant1:email       │
┌──────────────────┐    acme:default       │  - tenant2:default     │
│  Acme Client     │ ──────────────────────▶│  - acme:default        │
│  (user: acme)    │                        │                        │
└──────────────────┘                        └────────────────────────┘
```

### How Multi-Tenant Works

1. **Authentication**: Each tenant has unique username/password credentials
2. **Queue Isolation**: Tasks are automatically prefixed with tenant ID (e.g., `tenant1:default`)
3. **Complete Isolation**: Tenants cannot access each other's tasks
4. **Automatic Routing**: Server handles queue prefixing transparently

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ASYNQ_SERVER_ADDR` | Server listen address | `127.0.0.1:8080` |
| `ASYNQ_TENANT_CONFIG` | Path to multi-tenant config JSON | - |
| `ASYNQ_USERNAME` | Single-tenant username | - |
| `ASYNQ_PASSWORD` | Single-tenant password | - |

### Tenant Configuration Format

```json
[
  {
    "id": "string",           // Required: Unique tenant identifier
    "username": "string",     // Required: Authentication username
    "password": "string",     // Required: Authentication password
    "queue_prefix": "string"  // Optional: Queue prefix (defaults to id)
  }
]
```

## Dynamic Tenant Management

Tenants can be added or removed at runtime using the REST API (only available in multi-tenant mode).

### API Endpoints

#### List All Tenants
```bash
GET /api/tenants
```

Response:
```json
{
  "tenants": ["tenant1", "tenant2", "acme_corp"],
  "count": 3
}
```

#### Add a New Tenant
```bash
POST /api/tenants
Content-Type: application/json

{
  "id": "new_tenant",
  "username": "new_user",
  "password": "new_password",
  "queue_prefix": "new_tenant"
}
```

Response (201 Created):
```json
{
  "message": "Tenant added successfully",
  "tenant_id": "new_tenant",
  "username": "new_user"
}
```

#### Remove a Tenant
```bash
DELETE /api/tenants/{username}
```

Response:
```json
{
  "message": "Tenant removed successfully",
  "tenant_id": "tenant1",
  "username": "tenant1_user"
}
```

### Example: Adding Tenants Dynamically

```bash
# Start server with multi-tenant mode (can start with empty config)
export ASYNQ_TENANT_CONFIG=tenants.json
cargo run --bin asynq-server

# Add a new tenant dynamically via API
curl -X POST http://localhost:8080/api/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "id": "dynamic_tenant",
    "username": "dynamic_user",
    "password": "dynamic_password",
    "queue_prefix": "dynamic"
  }'

# List all tenants
curl http://localhost:8080/api/tenants

# Remove a tenant
curl -X DELETE http://localhost:8080/api/tenants/dynamic_user
```

**Note**: Tenants added dynamically are only stored in memory and will be lost on server restart unless you also update the configuration file.

## Backends

### Memory (Default)

No external dependencies, perfect for development:

```bash
cargo run --bin asynq-server
```

### Redis

Requires Redis running and `redis` feature:

```bash
cargo run --bin asynq-server --features redis
```

Configure via code (not command-line):

```rust
use asynq_server::AsynqServer;
use asynq::redis::RedisConnectionType;

let redis_config = RedisConnectionType::single("redis://127.0.0.1:6379")?;
let server = AsynqServer::with_redis("127.0.0.1:8080", redis_config).await?;
server.run().await?;
```

### PostgresSQL

Requires PostgresSQL and `postgresql` feature:

```bash
cargo run --bin asynq-server --features postgresql
```

Configure via code:

```rust
let server = AsynqServer::with_postgres(
    "127.0.0.1:8080",
    "postgres://user:password@localhost/asynq"
).await?;
```

## Client Usage

### Rust Client (WebSocket)

```rust
use asynq::client::Client;
use asynq::task::Task;

// Connect with tenant credentials
let client = Client::new_with_websocket_basic_auth(
    "127.0.0.1:8080",
    "tenant1_user".to_string(),
    "tenant1_password".to_string()
).await?;

// Enqueue task (automatically routed to tenant1:default)
let task = Task::new_with_json("email:send", &payload)?;
client.enqueue(task).await?;
```

### Consumer

```rust
use asynq::server::Server;
use asynq::wsdb::WebSocketBroker;

// Connect with tenant credentials
let broker = Arc::new(
    WebSocketBroker::with_basic_auth(
        "127.0.0.1:8080",
        Some("tenant1_user".to_string()),
        Some("tenant1_password".to_string())
    ).await?
);

let mut server = Server::with_broker_and_inspector(
    broker,
    inspector,
    config
).await?;

// Process tasks from tenant1's queues
server.run(mux).await?;
```

## Examples

Run the multi-tenant example:

```bash
cd asynq-server
cargo run --example multi_tenant_server
```

See full documentation: [examples/MULTI_TENANT_README.md](examples/MULTI_TENANT_README.md)

## API Endpoints

### WebSocket & Health
- `GET /health` - Health check endpoint
- `GET /ws` - WebSocket upgrade endpoint

### Tenant Management (Multi-tenant mode only)
- `GET /api/tenants` - List all configured tenants
- `POST /api/tenants` - Add a new tenant dynamically
- `DELETE /api/tenants/{username}` - Remove a tenant by username

## Security Best Practices

1. **Use Strong Passwords**: Generate secure credentials
   ```bash
   openssl rand -base64 32
   ```

2. **Protect Config Files**: Secure tenant configuration files
   ```bash
   chmod 600 tenants.json
   ```

3. **Use TLS in Production**: Deploy with `wss://` instead of `ws://`

4. **Rotate Credentials**: Regularly update tenant passwords

5. **Monitor Access**: Log authentication attempts

6. **Network Isolation**: Use firewalls to restrict access

## Development

### Build

```bash
cargo build --bin asynq-server
```

### Test

```bash
cargo test
```

### Run with Debug Logging

```bash
RUST_LOG=asynq_server=debug cargo run --bin asynq-server
```

## Troubleshooting

### 401 Unauthorized

**Issue**: Client cannot connect

**Solution**: 
- Verify credentials match server configuration
- Check tenant configuration is loaded
- Review server logs

### Tasks Not Appearing

**Issue**: Queued tasks not visible

**Solution**:
- Verify queue names include tenant prefix
- Check tenant authentication is working
- Ensure client and consumer use same tenant

### Configuration Not Loading

**Issue**: Tenant config file not loaded

**Solution**:
- Validate JSON syntax: `jq < tenants.json`
- Check `ASYNQ_TENANT_CONFIG` path
- Review startup logs for errors

## License

Licensed under MIT OR GPL-3.0, same as the Asynq project.

## Links

- [Main Asynq Repository](https://github.com/emo-crab/asynq)
- [Multi-Tenant Guide](examples/MULTI_TENANT_README.md)
- [WebSocket Authentication Guide](../WEBSOCKET_AUTH_EXAMPLES.md)
