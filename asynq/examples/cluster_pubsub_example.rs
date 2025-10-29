//! Redis Cluster PubSub 示例
//! Redis Cluster PubSub Example
//!
//! 演示如何在 Redis Cluster 模式下使用 PubSub 功能
//! Demonstrates how to use PubSub functionality in Redis Cluster mode

#[cfg(feature = "cluster")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  use asynq::rdb::RedisBroker;
  use asynq::redis::RedisConnectionType;
  // 初始化日志
  // Initialize logging
  tracing_subscriber::fmt::init();

  println!("Redis Cluster PubSub 示例");
  println!("Redis Cluster PubSub Example\n");

  // 创建集群配置，启用 RESP3 协议以支持 PubSub
  // Create cluster configuration with RESP3 enabled for PubSub support
  let cluster_config = RedisConnectionType::cluster(vec![
    "redis://127.0.0.1:7000".to_string(),
    "redis://127.0.0.1:7001".to_string(),
    "redis://127.0.0.1:7002".to_string(),
  ])?; // 启用 RESP3 协议 / Enable RESP3 protocol

  println!("配置信息 / Configuration:");
  println!();

  // 创建 broker
  // Create broker
  let broker = RedisBroker::new(cluster_config).await?;

  println!("✓ Broker 创建成功 / Broker created successfully");
  println!();

  // 获取 PubSub 连接
  // Get PubSub connection
  // 注意：在集群模式下，PubSub 只能创建一次
  // Note: In cluster mode, PubSub can only be created once
  match broker.get_pubsub().await {
    Ok(pubsub) => {
      println!("✓ PubSub 连接创建成功 / PubSub connection created successfully");
      println!();
      println!("说明 / Notes:");
      println!("  - 使用 RESP3 协议和 push_sender 实现 / Uses RESP3 protocol with push_sender");
      println!("  - 通过 tokio channel 接收推送消息 / Receives push messages via tokio channel");
      println!("  - 订阅应通过连接对象的 subscribe() 方法完成 / Subscription should be done via connection.subscribe()");
      println!();

      // 将 PubSub 转换为消息流
      // Convert PubSub to message stream
      let _stream = pubsub.into_on_message();
      println!("✓ 消息流创建成功 / Message stream created successfully");
    }
    Err(e) => {
      println!("✗ 创建 PubSub 连接失败 / Failed to create PubSub connection: {e}");
      println!();
      println!("可能的原因 / Possible reasons:");
      println!("  - Redis Cluster 未运行 / Redis Cluster is not running");
      println!("  - 节点地址不正确 / Node addresses are incorrect");
      println!("  - 未启用 RESP3 协议 / RESP3 protocol is not enabled");
      return Err(e.into());
    }
  }

  println!();
  println!("示例完成 / Example completed");

  Ok(())
}
#[cfg(not(feature = "cluster"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  Ok(())
}
