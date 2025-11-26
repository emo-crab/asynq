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
#[derive(Clone)]
pub enum RedisConnectionType {
  /// 单机连接
  /// Standalone connection
  Single {
    connection_info: ConnectionInfo,
    #[cfg(feature = "tls")]
    tls_certs: Option<redis::TlsCertificates>,
  },
  #[cfg(feature = "cluster")]
  /// 集群连接
  /// Cluster connection
  Cluster(Vec<ConnectionInfo>),
  #[cfg(feature = "sentinel")]
  /// Sentinel 连接：包含主节点名和 sentinel 节点列表
  /// Sentinel connection: contains master name and sentinel nodes list
  Sentinel {
    master_name: String,
    sentinels: Vec<ConnectionInfo>,
    redis_connection_info: Option<redis::RedisConnectionInfo>,
    #[cfg(feature = "tls")]
    tls_certs: Option<redis::TlsCertificates>,
  },
}

impl RedisConnectionType {
  /// 创建单机连接
  /// Create a standalone connection
  pub fn single<T: IntoConnectionInfo>(connection_info: T) -> Result<Self> {
    Ok(Self::Single {
      connection_info: connection_info.into_connection_info()?,
      #[cfg(feature = "tls")]
      tls_certs: None,
    })
  }
  #[cfg(feature = "tls")]
  /// 创建单机连接并指定 tls_mode（仅当开启 `tls` feature 时可用）
  /// Create a standalone connection with explicit tls_mode (available when `tls` feature enabled)
  pub fn single_with_tls<T: IntoConnectionInfo>(
    connection_info: T,
    tls_certs: redis::TlsCertificates,
  ) -> Result<Self> {
    Ok(Self::Single {
      connection_info: connection_info.into_connection_info()?,
      tls_certs: Some(tls_certs),
    })
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
  #[cfg(feature = "sentinel")]
  /// 创建 sentinel 连接
  /// Create a sentinel connection with master name and sentinel nodes
  pub fn sentinel<T: IntoConnectionInfo>(
    master_name: &str,
    sentinels: Vec<T>,
    redis_connection_info: Option<redis::RedisConnectionInfo>,
  ) -> Result<Self> {
    let nodes = sentinels
      .into_iter()
      .map(|x| x.into_connection_info())
      .filter_map(|x| x.ok())
      .collect::<Vec<_>>();
    if nodes.is_empty() {
      return Err(crate::error::Error::other(
        "At least one valid sentinel node is required for sentinel connection",
      ));
    }
    Ok(Self::Sentinel {
      master_name: master_name.to_string(),
      sentinels: nodes,
      redis_connection_info,
      #[cfg(feature = "tls")]
      tls_certs: None,
    })
  }
  #[cfg(all(feature = "sentinel", feature = "tls"))]
  /// 创建 sentinel 连接并指定 tls_mode（仅当开启 `sentinel` 和 `tls` feature 时可用）
  pub fn sentinel_with_tls<T: IntoConnectionInfo>(
    master_name: &str,
    sentinels: Vec<T>,
    redis_connection_info: Option<redis::RedisConnectionInfo>,
    tls_certs: redis::TlsCertificates,
  ) -> Result<Self> {
    let nodes = sentinels
      .into_iter()
      .map(|x| x.into_connection_info())
      .filter_map(|x| x.ok())
      .collect::<Vec<_>>();
    if nodes.is_empty() {
      return Err(crate::error::Error::other(
        "At least one valid sentinel node is required for sentinel connection",
      ));
    }
    Ok(Self::Sentinel {
      master_name: master_name.to_string(),
      sentinels: nodes,
      redis_connection_info,
      tls_certs: Some(tls_certs),
    })
  }
}

// Manual Debug implementation to avoid requiring `Debug` for `redis::TlsMode`.
impl std::fmt::Debug for RedisConnectionType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      RedisConnectionType::Single {
        connection_info, ..
      } => {
        #[cfg(feature = "tls")]
        {
          f.debug_struct("Single")
            .field("connection_info", connection_info)
            .field("tls_certs", &"<tls-certs>")
            .finish()
        }
        #[cfg(not(feature = "tls"))]
        {
          f.debug_struct("Single")
            .field("connection_info", connection_info)
            .finish()
        }
      }
      #[cfg(feature = "cluster")]
      RedisConnectionType::Cluster(nodes) => f.debug_tuple("Cluster").field(nodes).finish(),
      #[cfg(feature = "sentinel")]
      RedisConnectionType::Sentinel {
        master_name,
        sentinels,
        redis_connection_info,
        ..
      } => {
        #[cfg(feature = "tls")]
        {
          f.debug_struct("Sentinel")
            .field("master_name", master_name)
            .field("sentinels", sentinels)
            .field("redis_connection_info", redis_connection_info)
            .field("tls_certs", &"<tls-certs>")
            .finish()
        }
        #[cfg(not(feature = "tls"))]
        {
          f.debug_struct("Sentinel")
            .field("master_name", master_name)
            .field("sentinels", sentinels)
            .field("redis_connection_info", redis_connection_info)
            .finish()
        }
      }
    }
  }
}
