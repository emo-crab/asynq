# PostgresSQL 多租户功能

## 概述

PostgresSQL 多租户功能允许在同一个数据库中安全隔离不同租户的任务数据。通过行级安全策略 (Row-Level Security, RLS) 和数据库用户隔离，每个租户只能访问自己的数据。

## Overview

PostgresSQL multi-tenancy feature allows secure isolation of task data for different tenants in the same database. Through Row-Level Security (RLS) policies and database user isolation, each tenant can only access their own data.

## 主要特性 / Key Features

### 1. 数据隔离 / Data Isolation
- **行级安全策略 (RLS)**: 使用 PostgresSQL RLS 策略自动过滤租户数据
- **Row-Level Security**: Uses PostgresSQL RLS policies to automatically filter tenant data
- **数据库用户隔离**: 每个租户拥有独立的数据库用户
- **Database User Isolation**: Each tenant has their own database user

### 2. 向后兼容 / Backward Compatible
- 可选的租户ID字段，不影响现有非多租户部署
- Optional tenant_id field, doesn't affect existing non-tenant deployments
- 默认行为保持不变
- Default behavior remains unchanged

### 3. ACL 管理器 / ACL Manager
- 统一的 `AclManager` trait 接口
- Unified `AclManager` trait interface
- 支持 Redis ACL 和 PostgresSQL RLS
- Supports both Redis ACL and PostgresSQL RLS

## 架构设计 / Architecture

```
┌─────────────────────────────────────────┐
│         Application Layer               │
│  (Client/Server with tenant_id)         │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│         PostgresBroker                  │
│  (tenant_id: Option<String>)            │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│         Database Tables                 │
│  ┌──────────────────────────────────┐   │
│  │ asynq_tasks                      │   │
│  │ - id, queue, task_type, ...      │   │
│  │ - tenant_id: Option<String> ◄────┼───┼─ RLS Policy
│  └──────────────────────────────────┘   │
│  ┌──────────────────────────────────┐   │
│  │ asynq_queues                     │   │
│  │ - name, paused, ...              │   │
│  │ - tenant_id: Option<String> ◄────┼───┼─ RLS Policy
│  └──────────────────────────────────┘   │
│  ... (servers, workers, stats, ...)     │
└─────────────────────────────────────────┘
```

## 使用方法 / Usage

### 1. 创建租户用户 / Create Tenant User

```rust
use asynq::acl::{PostgresAclManager, AclConfig, NodeConfig};
use sea_orm::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 连接数据库（使用管理员账户）
    // Connect to database (using admin account)
    let db = Database::connect("postgresql://admin:password@localhost/asynq_db").await?;
    
    // 创建 ACL 管理器
    // Create ACL manager
    let acl_manager = PostgresAclManager::new(db, Some("postgres".to_string()));
    
    // 配置租户
    // Configure tenant
    let node_config = NodeConfig::new(
        "postgresql://localhost/asynq_db",
        "tenant_user1",      // 租户用户名 / Tenant username
        "secure_password",   // 租户密码 / Tenant password
        0,                   // (ignored for PostgresSQL)
    );
    
    let acl_config = AclConfig::new(node_config);
    
    // 创建租户用户和 RLS 策略
    // Create tenant user and RLS policies
    acl_manager.create_tenant_user(&acl_config).await?;
    
    println!("Tenant user created successfully!");
    Ok(())
}
```

### 2. 使用租户 Broker / Use Tenant Broker

```rust
use asynq::pgdb::PostgresBroker;
use asynq::task::Task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 方式1：创建带租户的 Broker
    // Method 1: Create Broker with tenant
    let broker = PostgresBroker::new_with_tenant(
        "postgresql://tenant_user1:secure_password@localhost/asynq_db",
        Some("tenant_user1".to_string())
    ).await?;
    
    // 方式2：从现有连接创建
    // Method 2: Create from existing connection
    let db = sea_orm::Database::connect(
        "postgresql://tenant_user1:secure_password@localhost/asynq_db"
    ).await?;
    let broker2 = PostgresBroker::from_connection_with_tenant(
        db,
        Some("tenant_user1".to_string())
    );
    
    // 发送任务（自动添加 tenant_id）
    // Enqueue task (automatically adds tenant_id)
    let task = Task::new("email:send", b"payload");
    broker.enqueue(&task).await?;
    
    Ok(())
}
```

### 3. 服务器配置 / Server Configuration

```rust
use asynq::server::Server;
use asynq::config::ServerConfig;
use asynq::base::Broker;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建带租户的 Broker
    // Create Broker with tenant
    let broker = PostgresBroker::new_with_tenant(
        "postgresql://tenant_user1:secure_password@localhost/asynq_db",
        Some("tenant_user1".to_string())
    ).await?;
    
    // 配置服务器
    // Configure server
    let mut queues = HashMap::new();
    queues.insert("default".to_string(), 3);
    
    let config = ServerConfig::new()
        .concurrency(4)
        .queues(queues)
        .acl_tenant("tenant_user1".to_string()); // 设置租户名 / Set tenant name
    
    // 创建并运行服务器
    // Create and run server
    let mut server = Server::new(broker, config).await?;
    
    // 服务器只会处理 tenant_user1 的任务
    // Server will only process tasks for tenant_user1
    server.run(handler).await?;
    
    Ok(())
}
```

### 4. 管理租户 / Manage Tenants

```rust
use asynq::acl::AclManager;

// 列出所有租户用户
// List all tenant users
let users = acl_manager.list_tenant_users().await?;
for user in users {
    println!("Tenant user: {}", user);
}

// 检查租户是否存在
// Check if tenant exists
let exists = acl_manager.tenant_user_exists("tenant_user1").await?;
println!("Tenant exists: {}", exists);

// 更新租户配置（如密码）
// Update tenant configuration (e.g., password)
let updated_config = AclConfig::new(NodeConfig::new(
    "postgresql://localhost/asynq_db",
    "tenant_user1",
    "new_password",
    0,
));
acl_manager.update_tenant_user(&updated_config).await?;

// 删除租户
// Delete tenant
acl_manager.delete_tenant_user("tenant_user1").await?;
```

## RLS 策略说明 / RLS Policy Explanation

### 自动应用的策略 / Automatically Applied Policies

对每个表，系统会创建以下 RLS 策略：
For each table, the system creates the following RLS policies:

```sql
-- 为租户 'tenant_user1' 创建策略
-- Create policy for tenant 'tenant_user1'
CREATE POLICY tenant_isolation_policy_tenant_user1 
ON asynq_tasks 
FOR ALL 
TO tenant_user1 
USING (tenant_id = 'tenant_user1');
```

### 策略效果 / Policy Effect

1. **SELECT**: 租户只能查询自己的数据
   - Tenant can only query their own data

2. **INSERT**: 需要手动设置 `tenant_id`（Broker 自动处理）
   - Need to manually set `tenant_id` (Broker handles automatically)

3. **UPDATE/DELETE**: 只能修改/删除自己的数据
   - Can only modify/delete own data

## 数据库架构 / Database Schema

### 实体表 / Entity Tables

所有表都包含 `tenant_id` 字段：
All tables include `tenant_id` field:

```sql
-- 任务表 / Tasks table
CREATE TABLE asynq_tasks (
    id VARCHAR PRIMARY KEY,
    queue VARCHAR NOT NULL,
    task_type VARCHAR NOT NULL,
    payload BYTEA,
    state VARCHAR(50),
    -- ... other fields ...
    tenant_id VARCHAR  -- 租户 ID / Tenant ID
);

CREATE INDEX idx_asynq_tasks_tenant_id ON asynq_tasks(tenant_id) 
WHERE tenant_id IS NOT NULL;

CREATE INDEX idx_asynq_tasks_tenant_queue_state ON asynq_tasks(tenant_id, queue, state) 
WHERE tenant_id IS NOT NULL;
```

### 索引优化 / Index Optimization

为提高查询性能，创建了以下索引：
For better query performance, the following indexes are created:

1. `idx_asynq_tasks_tenant_id` - 单列租户索引
   - Single column tenant index
2. `idx_asynq_tasks_tenant_queue_state` - 复合索引（租户+队列+状态）
   - Composite index (tenant + queue + state)

## 安全最佳实践 / Security Best Practices

### 1. 使用独立的数据库用户 / Use Separate Database Users
```rust
// ✅ 推荐：每个租户独立的数据库连接
// Recommended: Separate database connection for each tenant
let tenant1_broker = PostgresBroker::new_with_tenant(
    "postgresql://tenant1:pass1@localhost/asynq_db",
    Some("tenant1".to_string())
).await?;

let tenant2_broker = PostgresBroker::new_with_tenant(
    "postgresql://tenant2:pass2@localhost/asynq_db",
    Some("tenant2".to_string())
).await?;

// ❌ 不推荐：共享数据库连接
// Not recommended: Shared database connection
```

### 2. 密码管理 / Password Management
```rust
// 使用环境变量存储敏感信息
// Use environment variables for sensitive information
let password = std::env::var("TENANT_PASSWORD")?;
let config = NodeConfig::new(
    "postgresql://localhost/asynq_db",
    "tenant_user1",
    &password,
    0,
);
```

### 3. 权限最小化 / Minimal Permissions
```rust
// ACL 管理器只授予必要的权限
// ACL manager only grants necessary permissions
// - SELECT, INSERT, UPDATE, DELETE on tables
// - USAGE on sequences
// - No superuser or CREATE DATABASE privileges
```

## 迁移指南 / Migration Guide

### 从非多租户到多租户 / From Non-Tenant to Multi-Tenant

```sql
-- 1. 为现有表添加 tenant_id 列
-- Add tenant_id column to existing tables
ALTER TABLE asynq_tasks ADD COLUMN tenant_id VARCHAR;
ALTER TABLE asynq_queues ADD COLUMN tenant_id VARCHAR;
ALTER TABLE asynq_servers ADD COLUMN tenant_id VARCHAR;
ALTER TABLE asynq_workers ADD COLUMN tenant_id VARCHAR;
ALTER TABLE asynq_schedulers ADD COLUMN tenant_id VARCHAR;

-- Stats 表只添加 tenant_id 列（不修改主键以保持兼容性）
-- Stats table only adds tenant_id column (doesn't modify primary key for compatibility)
ALTER TABLE asynq_stats ADD COLUMN tenant_id VARCHAR;
-- Note: Stats queries will filter by tenant_id manually. Keeping primary key as (queue, date) maintains backward compatibility.

-- 2. 创建索引
-- Create indexes
CREATE INDEX idx_asynq_tasks_tenant_id ON asynq_tasks(tenant_id) 
WHERE tenant_id IS NOT NULL;
CREATE INDEX idx_asynq_stats_tenant_id ON asynq_stats(tenant_id) 
WHERE tenant_id IS NOT NULL;
-- ... (其他索引 / other indexes)

-- 3. 为现有数据设置默认租户（可选）
-- Set default tenant for existing data (optional)
UPDATE asynq_tasks SET tenant_id = 'default_tenant' WHERE tenant_id IS NULL;
UPDATE asynq_queues SET tenant_id = 'default_tenant' WHERE tenant_id IS NULL;
-- ...
```

## 性能考虑 / Performance Considerations

### 1. 索引策略 / Index Strategy
- 复合索引优先于单列索引用于常见查询
- Composite indexes prioritized for common queries
- 使用部分索引（WHERE tenant_id IS NOT NULL）减少索引大小
- Use partial indexes to reduce index size

### 2. 查询优化 / Query Optimization
```rust
// Broker 自动在查询中添加 tenant_id 过滤
// Broker automatically adds tenant_id filter in queries
// PostgresSQL 查询规划器可以有效利用索引
// PostgresSQL query planner can effectively use indexes
```

### 3. 连接池 / Connection Pooling
```rust
// 为每个租户使用独立的连接池
// Use separate connection pool for each tenant
let opt = ConnectOptions::new(&tenant_db_url)
    .max_connections(10)  // 根据负载调整 / Adjust based on load
    .to_owned();
```

## 限制和注意事项 / Limitations and Notes

### 1. 跨租户操作 / Cross-Tenant Operations
- 不支持跨租户的任务调度和查询
- Cross-tenant task scheduling and queries are not supported
- 每个租户需要独立的 Broker 实例
- Each tenant needs a separate Broker instance

### 2. 数据库级别隔离 / Database-Level Isolation
- 使用 RLS 策略，但数据存储在同一个数据库
- Uses RLS policies, but data is stored in the same database
- 如需完全物理隔离，考虑使用独立数据库
- Consider separate databases for complete physical isolation

### 3. 管理员权限 / Admin Privileges
- 创建租户需要数据库管理员权限
- Creating tenants requires database admin privileges
- 建议使用专门的管理服务
- Recommend using a dedicated management service

## 故障排除 / Troubleshooting

### 问题：无法创建租户用户 / Issue: Cannot Create Tenant User
```
错误 / Error: permission denied to create role
```
**解决方案 / Solution**: 确保连接使用的是有 CREATE ROLE 权限的管理员账户
**Solution**: Ensure connection uses admin account with CREATE ROLE privilege

### 问题：查询不到数据 / Issue: No Data in Queries
```
租户用户可以连接，但查询返回空结果
Tenant user can connect, but queries return empty results
```
**解决方案 / Solution**: 
1. 检查 RLS 策略是否正确创建 / Check if RLS policies are created correctly
2. 确认 tenant_id 字段正确设置 / Confirm tenant_id field is set correctly
3. 验证 broker 使用了正确的 tenant_id / Verify broker uses correct tenant_id

## 示例项目 / Example Projects

完整的示例代码请参考：
For complete example code, see:

- `examples/postgres_multi_tenant_example.rs` - 基础示例 / Basic example
- `examples/postgres_acl_management.rs` - ACL 管理示例 / ACL management example

## 相关资源 / Related Resources

- [PostgresSQL Row-Level Security Documentation](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [SeaORM Documentation](https://www.sea-ql.org/SeaORM/)
- [Asynq GitHub Repository](https://github.com/emo-crab/asynq)
