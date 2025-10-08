//! Example demonstrating the new components: Janitor, Recoverer, Forwarder, Healthcheck
//! 示例：展示新组件的使用：Janitor、Recoverer、Forwarder、Healthcheck
//!
//! This example shows how the new components work together to provide
//! a complete asynq-compatible server architecture.
//!
//! 此示例展示了新组件如何协同工作，提供完整的 asynq 兼容服务器架构。

use asynq::{
    rdb::RedisBroker,
    redis::RedisConfig,
    client::Client,
};
use std::sync::Arc;
use std::time::Duration;
use asynq::components::aggregator::{Aggregator, AggregatorConfig};
use asynq::components::forwarder::{Forwarder, ForwarderConfig};
use asynq::components::healthcheck::{Healthcheck, HealthcheckConfig};
use asynq::components::janitor::{Janitor, JanitorConfig};
use asynq::components::periodic_task_manager::{PeriodicTaskManager, PeriodicTaskManagerConfig};
use asynq::components::recoverer::{Recoverer, RecovererConfig};
use asynq::components::subscriber::{Subscriber, SubscriberConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 Starting asynq components example...\n");

    // 创建 Redis 配置
    // Create Redis configuration
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    println!("📡 Connecting to Redis: {}", redis_url);
    
    let redis_config = RedisConfig::from_url(&redis_url)?;
    let mut broker = RedisBroker::new(redis_config.clone())?;
    broker.init_scripts().await?;
    let broker: Arc<dyn asynq::base::Broker> = Arc::new(broker);

    println!("✅ Connected to Redis\n");

    // 1. Janitor - 清理过期任务和死亡服务器
    // 1. Janitor - Clean up expired tasks and dead servers
    println!("🧹 Starting Janitor component...");
    let janitor_config = JanitorConfig {
        interval: Duration::from_secs(10),
        batch_size: 100,
        queues: vec!["default".to_string(), "critical".to_string()],
    };
    let janitor = Arc::new(Janitor::new(Arc::clone(&broker), janitor_config));
    let janitor_handle = janitor.clone().start();
    println!("   ✓ Janitor started (interval: 10s)\n");

    // 2. Recoverer - 恢复孤儿任务
    // 2. Recoverer - Recover orphaned tasks
    println!("🔄 Starting Recoverer component...");
    let recoverer_config = RecovererConfig {
        interval: Duration::from_secs(8),
        queues: vec!["default".to_string(), "critical".to_string()],
    };
    let recoverer = Arc::new(Recoverer::new(Arc::clone(&broker), recoverer_config));
    let recoverer_handle = recoverer.clone().start();
    println!("   ✓ Recoverer started (interval: 8s)\n");

    // 3. Forwarder - 转发调度任务
    // 3. Forwarder - Forward scheduled tasks
    println!("⏩ Starting Forwarder component...");
    let forwarder_config = ForwarderConfig {
        interval: Duration::from_secs(5),
        queues: vec!["default".to_string(), "critical".to_string()],
    };
    let forwarder = Arc::new(Forwarder::new(Arc::clone(&broker), forwarder_config));
    let forwarder_handle = forwarder.clone().start();
    println!("   ✓ Forwarder started (interval: 5s)\n");

    // 4. Healthcheck - 健康检查
    // 4. Healthcheck - Health check
    println!("🏥 Starting Healthcheck component...");
    let healthcheck_config = HealthcheckConfig {
        interval: Duration::from_secs(15),
    };
    let healthcheck = Arc::new(Healthcheck::new(Arc::clone(&broker), healthcheck_config));
    let healthcheck_handle = healthcheck.clone().start();
    println!("   ✓ Healthcheck started (interval: 15s)\n");

    // 5. Aggregator - 聚合任务
    // 5. Aggregator - Aggregate tasks
    println!("📦 Starting Aggregator component...");
    let aggregator_config = AggregatorConfig {
        interval: Duration::from_secs(5),
        queues: vec!["default".to_string()],
        grace_period: Duration::from_secs(60),
        max_delay: Some(Duration::from_secs(300)),
        max_size: Some(100),
        group_aggregator: None,
    };
    let aggregator = Arc::new(Aggregator::new(Arc::clone(&broker), aggregator_config));
    let aggregator_handle = aggregator.clone().start();
    println!("   ✓ Aggregator started (interval: 5s)\n");

    // 6. Subscriber - 订阅事件
    // 6. Subscriber - Subscribe to events
    println!("📢 Starting Subscriber component...");
    let subscriber_config = SubscriberConfig {
        buffer_size: 100,
    };
    let subscriber = Subscriber::new(Arc::clone(&broker), subscriber_config);
    let subscriber_arc = Arc::new(subscriber);
    let subscriber_handle = subscriber_arc.clone().start();
    println!("   ✓ Subscriber started (buffer size: 100)\n");

    // 7. Periodic Task Manager - 管理周期性任务
    // 7. Periodic Task Manager - Manage periodic tasks
    println!("⏰ Starting Periodic Task Manager...");
    let client = Arc::new(Client::new(redis_config).await?);
    let ptm_config = PeriodicTaskManagerConfig {
        check_interval: Duration::from_secs(60),
    };
    let ptm = Arc::new(PeriodicTaskManager::new(client, ptm_config));
    let ptm_handle = ptm.clone().start();
    println!("   ✓ Periodic Task Manager started (interval: 60s)\n");

    println!("🎉 All components started successfully!\n");
    println!("📊 Component Status:");
    println!("   • Janitor:       Running");
    println!("   • Recoverer:     Running");
    println!("   • Forwarder:     Running");
    println!("   • Healthcheck:   Healthy = {}", healthcheck.is_healthy());
    println!("   • Aggregator:    Running");
    println!("   • Subscriber:    Running");
    println!("   • Task Manager:  Running\n");

    println!("⏳ Running for 30 seconds...");
    println!("   (Components are working in the background)\n");

    // 运行 30 秒
    // Run for 30 seconds
    tokio::time::sleep(Duration::from_secs(30)).await;

    // 优雅关闭所有组件
    // Gracefully shutdown all components
    println!("🛑 Shutting down components...");
    
    janitor.shutdown();
    recoverer.shutdown();
    forwarder.shutdown();
    healthcheck.shutdown();
    aggregator.shutdown();
    subscriber_arc.shutdown();
    ptm.shutdown();

    // 等待所有任务完成
    // Wait for all tasks to complete
    let _ = tokio::time::timeout(Duration::from_secs(5), janitor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), recoverer_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), forwarder_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), healthcheck_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), aggregator_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), subscriber_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), ptm_handle).await;

    println!("✅ All components shut down successfully!\n");
    println!("👋 Example completed!");

    Ok(())
}
