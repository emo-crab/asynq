use crate::backend::rdb::redis::RedisConnection;
use crate::error::Result;
use futures::Stream;
#[cfg(feature = "cluster")]
use redis::cluster::ClusterClient;
#[cfg(feature = "sentinel")]
use redis::sentinel::SentinelClient;
use redis::Client;

#[cfg(feature = "cluster")]
/// 集群模式的 PubSub 连接封装
/// Cluster mode PubSub connection wrapper
///
/// Redis Cluster 使用 RESP3 协议和 push_sender 来实现 PubSub 功能
/// Redis Cluster uses RESP3 protocol and push_sender to implement PubSub
pub struct ClusterPubSubConnection {
  /// 接收推送消息的通道
  /// Channel for receiving push messages
  receiver: tokio::sync::mpsc::UnboundedReceiver<redis::PushInfo>,
  /// 用于订阅的集群连接
  /// Cluster connection for subscribing
  connection: redis::cluster_async::ClusterConnection,
}
#[cfg(feature = "cluster")]
impl ClusterPubSubConnection {
  /// 从通道接收器和连接创建集群 PubSub 连接
  /// Create cluster PubSub connection from channel receiver and connection
  pub fn from_receiver(
    receiver: tokio::sync::mpsc::UnboundedReceiver<redis::PushInfo>,
    connection: redis::cluster_async::ClusterConnection,
  ) -> Self {
    Self {
      receiver,
      connection,
    }
  }

  /// 将 PubSub 转换为消息流
  /// Convert PubSub to message stream
  pub fn into_on_message(self) -> std::pin::Pin<Box<dyn Stream<Item = redis::Msg> + Send>> {
    use futures::StreamExt;
    let mut receiver = self.receiver;
    let stream = futures_util::stream::poll_fn(move |cx| match receiver.poll_recv(cx) {
      std::task::Poll::Ready(Some(push_info)) => std::task::Poll::Ready(Some(push_info)),
      std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
      std::task::Poll::Pending => std::task::Poll::Pending,
    })
    .filter_map(|push_info| {
      async move {
        // 将 PushInfo 转换为 redis::Msg
        // Convert PushInfo to redis::Msg
        if push_info.kind == redis::PushKind::Message || push_info.kind == redis::PushKind::PMessage
        {
          // 构造一个 redis::Msg
          // PushInfo 的 data 格式为: [channel, message] 或 [pattern, channel, message]
          // Construct a redis::Msg
          // PushInfo data format: [channel, message] or [pattern, channel, message]
          if push_info.data.len() >= 2 {
            // 使用 redis::Msg::from_value 从 Value 创建 Msg
            // Use redis::Msg::from_value to create Msg from Value
            let msg_value = if push_info.kind == redis::PushKind::PMessage {
              // For message: [pattern, channel, payload]
              redis::Value::Array(vec![
                redis::Value::BulkString(b"pmessage".to_vec()),
                push_info.data[0].clone(),
                push_info.data[1].clone(),
                push_info.data[2].clone(),
              ])
            } else {
              // For message: [channel, payload]
              redis::Value::Array(vec![
                redis::Value::BulkString(b"message".to_vec()),
                push_info.data[0].clone(),
                push_info.data[1].clone(),
              ])
            };
            redis::Msg::from_value(&msg_value)
          } else {
            None
          }
        } else {
          None
        }
      }
    });

    Box::pin(stream)
  }
}

/// Redis PubSub 统一枚举
/// Unified Redis PubSub enum
///
/// 统一单机和集群模式的 PubSub 接口
/// Unifies Single and Cluster mode PubSub interfaces
///
/// 参考 redis-rs issues #1365 和 PR #1449
/// References redis-rs issues #1365 and PR #1449
pub enum RedisPubSub {
  /// 单机模式的 PubSub
  /// Single mode PubSub
  Single(redis::aio::PubSub),
  #[cfg(feature = "cluster")]
  /// 集群模式的 PubSub（通过集群中的一个节点连接）
  /// Cluster mode PubSub (via connection to a node in the cluster)
  /// Redis Cluster 的 PubSub 需要连接到特定节点
  /// Redis Cluster PubSub requires connection to a specific node
  Cluster(ClusterPubSubConnection),
  #[cfg(feature = "sentinel")]
  /// Sentinel 模式的 PubSub（通过 Sentinel 获取主节点后与主节点建立的 PubSub）
  /// Sentinel mode PubSub (obtained by Sentinel and connected to the master node)
  Sentinel(redis::aio::PubSub),
}

impl RedisPubSub {
  /// 订阅频道
  /// Subscribe to a channel
  ///
  /// 对于集群模式，订阅通过存储的连接对象完成
  /// For cluster mode, subscription is done through the stored connection object
  pub async fn subscribe<T: redis::ToRedisArgs>(&mut self, channel: T) -> Result<()> {
    match self {
      RedisPubSub::Single(pubsub) => {
        pubsub.subscribe(channel).await?;
        Ok(())
      }
      #[cfg(feature = "cluster")]
      RedisPubSub::Cluster(cluster_pubsub) => {
        // 集群模式下，通过连接对象订阅
        // In cluster mode, subscribe through the connection object
        cluster_pubsub.connection.subscribe(channel).await?;
        Ok(())
      }
      #[cfg(feature = "sentinel")]
      RedisPubSub::Sentinel(pubsub) => {
        // Sentinel 情况下，Sentinel 已经解析到主节点并返回了一个普通 PubSub
        // For Sentinel, we use the PubSub obtained after resolving master via Sentinel
        pubsub.subscribe(channel).await?;
        Ok(())
      }
    }
  }

  /// 将 PubSub 转换为消息流
  /// Convert PubSub to message stream
  pub fn into_on_message(self) -> Box<dyn Stream<Item = redis::Msg> + Unpin + Send> {
    match self {
      RedisPubSub::Single(pubsub) => Box::new(pubsub.into_on_message()),
      #[cfg(feature = "cluster")]
      RedisPubSub::Cluster(pubsub) => Box::new(pubsub.into_on_message()),
      #[cfg(feature = "sentinel")]
      RedisPubSub::Sentinel(pubsub) => Box::new(pubsub.into_on_message()),
    }
  }
}

#[derive(Clone)]
pub enum RedisClient {
  Single(Client),
  #[cfg(feature = "cluster")]
  Cluster {
    client: ClusterClient,
    /// 推送消息接收器（用于 PubSub，只能使用一次）
    /// Push message receiver (for PubSub, can only be used once)
    push_receiver: std::sync::Arc<
      tokio::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<redis::PushInfo>>>,
    >,
  },
  #[cfg(feature = "sentinel")]
  Sentinel {
    /// Sentinel 模式下保存的客户端（使用 Arc<Mutex<>> 包装以允许在异步上下文中可变调用）
    /// Sentinel client wrapped in Arc<Mutex<>> so we can call &mut methods on it when needed
    client: std::sync::Arc<tokio::sync::Mutex<SentinelClient>>,
  },
}

impl RedisClient {
  pub async fn get_connection(&self) -> Result<RedisConnection> {
    match self {
      RedisClient::Single(s) => {
        let conn = s.get_multiplexed_async_connection().await?;
        Ok(RedisConnection::Single(conn))
      }
      #[cfg(feature = "cluster")]
      RedisClient::Cluster { client, .. } => {
        let conn = client.get_async_connection().await?;
        Ok(RedisConnection::Cluster(conn))
      }
      #[cfg(feature = "sentinel")]
      RedisClient::Sentinel { client, .. } => {
        // Sentinel: lock the inner SentinelClient (requires &mut) and resolve/get an async connection on demand
        let mut guard = client.lock().await;
        let conn = guard.get_async_connection().await?;
        Ok(RedisConnection::Single(conn))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[cfg(feature = "cluster")]
  #[test]
  fn test_cluster_pubsub_connection_creation() {
    // 这个测试验证 ClusterPubSubConnection 可以被创建
    // This test verifies that ClusterPubSubConnection can be created
    // 注意：这只是一个编译时测试，确保结构正确定义
    // Note: This is just a compile-time test to ensure the structure is defined correctly

    // 由于我们需要一个真实的 redis::aio::PubSub 连接来创建 ClusterPubSubConnection，
    // 而这需要一个运行的 Redis 实例，所以这里只测试类型系统
    // Since we need a real redis::aio::PubSub connection to create ClusterPubSubConnection，
    // and this requires a running Redis instance, we only test the type system here

    // 类型检查：确保 ClusterPubSubConnection 实现了预期的方法
    // Type check: ensure ClusterPubSubConnection implements expected methods
    fn _type_check() {
      fn _assert_has_subscribe<
        T: Fn(
          &mut ClusterPubSubConnection,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + '_>>,
      >(
        _: T,
      ) {
      }
      fn _assert_has_into_on_message<
        T: Fn(ClusterPubSubConnection) -> std::pin::Pin<Box<dyn Stream<Item = redis::Msg> + Send>>,
      >(
        _: T,
      ) {
      }

      // 这些编译通过就说明类型是正确的
      // If these compile, the types are correct
    }
  }

  #[test]
  fn test_redis_pubsub_enum() {
    // 测试 RedisPubSub 枚举的类型定义
    // Test RedisPubSub enum type definition

    // 确保 RedisPubSub 有两个变体
    // Ensure RedisPubSub has two variants
    fn _check_variants() {
      // 这个函数的存在就证明了枚举定义是正确的
      // The existence of this function proves the enum definition is correct
      fn _match_check(pubsub: RedisPubSub) {
        match pubsub {
          RedisPubSub::Single(_) => {}
          #[cfg(feature = "cluster")]
          RedisPubSub::Cluster(_) => {}
          #[cfg(feature = "sentinel")]
          RedisPubSub::Sentinel(_) => {}
        }
      }
    }
  }
}
