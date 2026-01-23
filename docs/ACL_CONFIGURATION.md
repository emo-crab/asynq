# ACL Configuration Guide

This guide explains how to use the ACL (Access Control List) feature in asynq to enable multi-tenant task queue isolation.

## Overview

When ACL is enabled (by setting a tenant name), asynq automatically prefixes queue names with a tenant identifier (username). This ensures that different tenants can only access their own tasks in a shared Redis instance.

**Important**: The `default` queue is intentionally excluded from prefixing and serves as a shared public queue accessible by all tenants. All other queue names will be prefixed.

For example, with tenant `tenant1`:
- Queue `default` → Redis key: `asynq:{default}:pending` (shared)
- Queue `critical` → Redis key: `asynq:{tenant1:critical}:pending` (tenant-specific)

## Configuration

### Server Configuration

To enable ACL in the server (consumer):

```rust
use asynq::config::ServerConfig;
use asynq::server::ServerBuilder;

let server_config = ServerConfig::new()
    .acl_tenant("tenant1")  // Set the tenant name (ACL is automatically enabled)
    .concurrency(4)
    // ... other configurations
    ;

let server = ServerBuilder::new()
    .redis_config(redis_config)
    .server_config(server_config)
    .build()
    .await?;
```

### Client Configuration

To enable ACL in the client (producer):

```rust
use asynq::config::ClientConfig;
use asynq::client::Client;

let client_config = ClientConfig::new()
    .acl_tenant("tenant1");  // Set the tenant name (ACL is automatically enabled)

let client = Client::with_config(redis_config, client_config).await?;
```

## Automatic Configuration from Redis URL

Both the consumer and producer examples include automatic ACL configuration when using Redis URLs with authentication:

```bash
# Redis URL format: redis://username:password@host:port
export REDIS_URL="redis://tenant1:secure_pass123@localhost:6379"

# When the acl feature is enabled, the username is automatically extracted
cargo run --example=consumer --features=acl
cargo run --example=producer --features=acl
```

## Example Usage

### 1. Create Redis ACL User

First, create a Redis user with appropriate permissions:

```bash
redis-cli
> ACL SETUSER tenant1 on >secure_pass123 ~asynq:{tenant1}:* +@all
```

### 2. Run Consumer with ACL

```bash
export REDIS_URL="redis://tenant1:secure_pass123@localhost:6379"
cargo run --manifest-path=asynq/Cargo.toml --example=consumer --features=acl
```

Output:
```
🚀 Starting Asynq worker server...
🔗 Using Redis URL: redis://tenant1:secure_pass123@localhost:6379
🔐 ACL enabled with tenant: tenant1
🔄 Server is running and waiting for tasks...
```

### 3. Run Producer with ACL

```bash
export REDIS_URL="redis://tenant1:secure_pass123@localhost:6379"
cargo run --manifest-path=asynq/Cargo.toml --example=producer --features=acl
```

Output:
```
🔗 Using Redis URL: redis://tenant1:secure_pass123@localhost:6379
🔐 ACL enabled with tenant: tenant1
Email task enqueued: ID = ...
```

## Queue Name Transformation

When ACL is enabled, queue names are automatically prefixed, **except for the default queue**:

| Original Queue | With ACL (tenant1) | Notes |
|---------------|--------------------| ------|
| `default`     | `default` (unchanged) | Shared public queue for all tenants |
| `critical`    | `tenant1:critical` | Tenant-specific queue |
| `low`         | `tenant1:low`      | Tenant-specific queue |

The Redis keys become:
- `asynq:{default}:pending` - Shared across all tenants
- `asynq:{tenant1:critical}:pending` - Tenant-specific
- etc.

**Important**: The `default` queue is intentionally left unprefixed as it serves as a shared public queue accessible by all tenants. If you need tenant-specific default behavior, create a custom queue (e.g., `myqueue`) instead.

## Multi-Tenant Isolation

Different tenants can run simultaneously without interfering with each other:

**Tenant 1:**
```bash
export REDIS_URL="redis://tenant1:pass1@localhost:6379"
cargo run --example=consumer --features=acl  # Processes tenant1:* queues
```

**Tenant 2:**
```bash
export REDIS_URL="redis://tenant2:pass2@localhost:6379"
cargo run --example=consumer --features=acl  # Processes tenant2:* queues
```

Each tenant can only access their own tasks due to Redis ACL rules.

## Backward Compatibility

When ACL is not enabled (no tenant name set), the system works exactly as before:

```rust
// ACL disabled (default - no tenant name)
let config = ServerConfig::new();
// Queue names remain unchanged: "default", "critical", etc.
```

## API Design

Setting the tenant name automatically enables ACL functionality. There is no separate flag to enable/disable ACL:

```rust
// ACL enabled - tenant name is set
let config_with_acl = ServerConfig::new()
    .acl_tenant("tenant1");  // ACL is now active

// ACL disabled - no tenant name
let config_without_acl = ServerConfig::new();
```

## Best Practices

1. **Use meaningful tenant names**: Use usernames or organization identifiers as tenant names
2. **Set up Redis ACL properly**: Ensure each tenant user has appropriate Redis ACL permissions
3. **Consistent tenant names**: Both producer and consumer should use the same tenant name
4. **Use environment variables**: Store Redis URLs with credentials in environment variables, not in code

## Troubleshooting

### Error: "No permissions to access a key"

This error occurs when:
1. Tenant name is not set but Redis requires authentication
2. The Redis ACL user doesn't have permission to access the required keys

**Solution**: 
- Set tenant name in both client and server configurations
- Ensure Redis ACL user has permissions for `~asynq:{tenant}:*` keys

### Tasks not being processed

**Check**:
1. Both producer and consumer are using the same tenant name
2. Tenant name is set in both configurations
3. Queue names match between producer and consumer

## Reference

For more information about the ACL module, see:
- `asynq/examples/acl_example.rs` - Complete ACL module demonstration
- `asynq/src/acl/mod.rs` - ACL module implementation
- Redis ACL documentation: https://redis.io/docs/management/security/acl/
