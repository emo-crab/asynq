//! Redis 配置和连接管理模块
//! Redis configuration and connection management module

use crate::error::Result;
use async_trait::async_trait;
use redis::{Cmd, ConnectionInfo, IntoConnectionInfo, Pipeline, RedisFuture, Value};

pub enum RedisConnection {
  Single(redis::aio::MultiplexedConnection),
  #[cfg(feature = "cluster")]
  Cluster(redis::cluster_async::ClusterConnection),
}
#[async_trait]
impl redis::aio::ConnectionLike for RedisConnection {
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    match self {
      RedisConnection::Single(c) => c.req_packed_command(cmd),
      #[cfg(feature = "cluster")]
      RedisConnection::Cluster(c) => c.req_packed_command(cmd),
    }
  }

  fn req_packed_commands<'a>(
    &'a mut self,
    cmd: &'a Pipeline,
    offset: usize,
    count: usize,
  ) -> RedisFuture<'a, Vec<Value>> {
    match self {
      RedisConnection::Single(c) => c.req_packed_commands(cmd, offset, count),
      #[cfg(feature = "cluster")]
      RedisConnection::Cluster(c) => c.req_packed_commands(cmd, offset, count),
    }
  }

  /// Gets the current Redis database number
  /// # Notes
  /// * Always returns 0 as database selection is not supported in cluster mode
  /// # Returns
  /// * `i64` - The database number (always 0)
  fn get_db(&self) -> i64 {
    0
  }
}

/// Redis 连接类型
/// Redis connection types
#[derive(Debug, Clone)]
pub enum RedisConnectionConfig {
  /// 单机连接
  /// Standalone connection
  Single(ConnectionInfo),
  #[cfg(feature = "cluster")]
  /// 集群连接
  /// Cluster connection
  Cluster(Vec<ConnectionInfo>),
}

impl RedisConnectionConfig {
  /// 创建单机连接
  /// Create a standalone connection
  pub fn single<T: IntoConnectionInfo>(connection_info: T) -> Result<Self> {
    Ok(Self::Single(connection_info.into_connection_info()?))
  }
  #[cfg(feature = "cluster")]
  /// 创建集群连接
  /// Create a cluster connection
  pub fn cluster<T: IntoConnectionInfo>(nodes: Vec<T>) -> Result<Self> {
    let nodes = nodes
      .into_iter()
      .map(|x| x.into_connection_info())
      .filter_map(|x| x.ok())
      .collect::<Vec<_>>();
    if nodes.is_empty() {
      return Err(crate::error::Error::other(
        "At least one valid node is required for cluster connection",
      ));
    }
    Ok(Self::Cluster(nodes))
  }
}
