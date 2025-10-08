//! Redis 配置和连接管理模块
//! Redis configuration and connection management module

use crate::error::{Error, Result};
use async_trait::async_trait;
use redis::{Client, Cmd, ConnectionInfo, IntoConnectionInfo, Pipeline, RedisFuture, Value};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

/// Redis 连接配置
/// Redis connection configuration
#[derive(Debug, Clone)]
pub struct RedisConfig {
  /// Redis 连接信息
  /// Redis connection info
  pub connection_info: ConnectionInfo,
  /// 连接池配置
  /// Connection pool configuration
  pub pool_config: PoolConfig,
}

impl RedisConfig {
  /// 创建新的 Redis 配置
  /// Create a new Redis configuration
  pub fn new<T: IntoConnectionInfo>(connection_info: T) -> Result<Self> {
    let connection_info = connection_info
      .into_connection_info()
      .map_err(|e| Error::other(format!("Invalid Redis connection info: {}", e)))?;

    Ok(Self {
      connection_info,
      pool_config: PoolConfig::default(),
    })
  }

  /// 从 Redis URL 创建配置
  /// Create configuration from Redis URL
  pub fn from_url(url: &str) -> Result<Self> {
    Self::new(url)
  }

  /// 设置连接池配置
  /// Set connection pool configuration
  pub fn with_pool_config(mut self, pool_config: PoolConfig) -> Self {
    self.pool_config = pool_config;
    self
  }
}

/// 连接池配置
/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
  /// 连接池最大大小
  /// Maximum size of the connection pool
  pub max_size: u32,
  /// 最小空闲连接数
  /// Minimum number of idle connections
  pub min_idle: Option<u32>,
  /// 连接超时时间
  /// Connection timeout duration
  pub connection_timeout: Duration,
  /// 空闲超时时间
  /// Idle timeout duration
  pub idle_timeout: Option<Duration>,
  /// 连接最大生命周期
  /// Maximum lifetime of a connection
  pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
  fn default() -> Self {
    Self {
      max_size: 10,
      min_idle: Some(1),
      connection_timeout: Duration::from_secs(30),
      idle_timeout: Some(Duration::from_secs(600)), // 10 分钟 / 10 minutes
      max_lifetime: Some(Duration::from_secs(1800)), // 30 分钟 / 30 minutes
    }
  }
}

/// Redis 连接管理器
/// Redis connection manager
#[derive(Debug)]
pub struct RedisConnectionManager {
  pub client: Client,
}

impl RedisConnectionManager {
  /// 创建新的连接管理器
  /// Create a new connection manager
  pub fn new(connection_info: ConnectionInfo) -> Result<Self> {
    let client = Client::open(connection_info)?;
    Ok(Self { client })
  }
}

/// Redis 集群配置
/// Redis Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
  /// 集群节点地址列表
  /// List of cluster node addresses
  pub nodes: Vec<String>,
  /// 认证密码
  /// Authentication password
  pub password: Option<String>,
  /// 连接超时时间
  /// Connection timeout duration
  pub connection_timeout: Duration,
  /// 读取超时时间
  /// Response timeout duration
  pub response_timeout: Duration,
  /// 使用 RESP3 协议以支持 PubSub
  /// Use RESP3 protocol to support PubSub
  pub use_resp3: bool,
}

impl ClusterConfig {
  /// 创建新的集群配置
  /// Create a new cluster configuration
  pub fn new(nodes: Vec<String>) -> Self {
    Self {
      nodes,
      password: None,
      connection_timeout: Duration::from_secs(5),
      response_timeout: Duration::from_secs(3),
      use_resp3: false,
    }
  }

  /// 设置密码
  /// Set password
  pub fn with_password<T: AsRef<str>>(mut self, password: T) -> Self {
    self.password = Some(password.as_ref().to_string());
    self
  }

  /// 设置连接超时时间
  /// Set connection timeout duration
  pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
    self.connection_timeout = timeout;
    self
  }

  /// 设置响应超时时间
  /// Set response timeout duration
  pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
    self.response_timeout = timeout;
    self
  }

  /// 启用 RESP3 协议以支持 PubSub
  /// Enable RESP3 protocol to support PubSub
  pub fn with_resp3(mut self, use_resp3: bool) -> Self {
    self.use_resp3 = use_resp3;
    self
  }
}
pub enum RedisConnection {
  Single(redis::aio::MultiplexedConnection),
  Cluster(redis::cluster_async::ClusterConnection),
}
#[async_trait]
impl redis::aio::ConnectionLike for RedisConnection {
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    match self {
      RedisConnection::Single(c) => c.req_packed_command(cmd),
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
impl Deref for RedisConnection {
  type Target = dyn redis::aio::ConnectionLike;

  fn deref(&self) -> &Self::Target {
    match self {
      RedisConnection::Single(s) => s,
      RedisConnection::Cluster(c) => c,
    }
  }
}

impl DerefMut for RedisConnection {
  fn deref_mut(&mut self) -> &mut Self::Target {
    match self {
      RedisConnection::Single(s) => s,
      RedisConnection::Cluster(c) => c,
    }
  }
}

/// Redis 连接类型
/// Redis connection types
#[derive(Debug, Clone)]
pub enum RedisConnectionConfig {
  /// 单机连接
  /// Standalone connection
  Single(RedisConfig),
  /// 集群连接
  /// Cluster connection
  Cluster(ClusterConfig),
}

impl RedisConnectionConfig {
  /// 创建单机连接
  /// Create a standalone connection
  pub fn single<T: IntoConnectionInfo>(connection_info: T) -> Result<Self> {
    Ok(Self::Single(RedisConfig::new(connection_info)?))
  }

  /// 创建集群连接
  /// Create a cluster connection
  pub fn cluster(nodes: Vec<String>) -> Self {
    Self::Cluster(ClusterConfig::new(nodes))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_redis_config_creation() {
    let config = RedisConfig::from_url("redis://127.0.0.1:6379").unwrap();
    assert_eq!(config.connection_info.addr.to_string(), "127.0.0.1:6379");
  }

  #[test]
  fn test_cluster_config() {
    let nodes = vec!["127.0.0.1:7000".to_string(), "127.0.0.1:7001".to_string()];
    let config = ClusterConfig::new(nodes.clone())
      .with_password("password")
      .with_connection_timeout(Duration::from_secs(10));

    assert_eq!(config.nodes, nodes);
    assert_eq!(config.password, Some("password".to_string()));
    assert_eq!(config.connection_timeout, Duration::from_secs(10));
  }
}
