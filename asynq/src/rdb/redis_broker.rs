//! Redis 经纪人实现
//! Redis broker implementation
//!
//! 实现基于 Redis 的任务存储和管理
//! Implements task storage and management based on Redis

use crate::base::constants::DEFAULT_QUEUE_NAME;
use crate::base::keys;
use crate::error::{Error, Result};
use crate::proto::{SchedulerEnqueueEvent, SchedulerEntry, TaskMessage};
use crate::rdb::redis_scripts::{RedisArg, ScriptManager};
#[cfg(feature = "cluster")]
use crate::rdb::universal_client::ClusterPubSubConnection;
use crate::rdb::universal_client::{RedisClient, RedisPubSub};
use crate::redis::{RedisConnection, RedisConnectionType};
use crate::task::Task;
use prost::Message;
#[cfg(feature = "sentinel")]
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::AsyncCommands;
use redis::Client;
use uuid::Uuid;

/// Redis 经纪人实现
/// Redis broker implementation
pub struct RedisBroker {
  client: RedisClient,
  pub(crate) script_manager: ScriptManager,
}

impl RedisBroker {
  /// 从RedisConnection创建新的Redis经纪人实例
  /// Create a new Redis broker instance from RedisConnection
  pub async fn new(conn: RedisConnectionType) -> Result<Self> {
    match conn {
      RedisConnectionType::Single {
        connection_info,
        #[cfg(feature = "tls")]
        tls_certs,
      } => {
        // If a tls_mode was provided in RedisConnectionType, apply it to the
        // ConnectionInfo addr so redis::Client will create a TLS connection.
        // This is a no-op when the `tls` feature is not enabled.
        let client = {
          #[cfg(feature = "tls")]
          {
            // When tls feature is enabled, if a tls_mode was supplied by the
            // higher-level RedisConnectionType, prefer building a TLS-enabled
            // client using the redis crate's TLS helpers. Otherwise fall back
            // to the normal Client::open.
            let ci = connection_info.clone();
            if let Some(tls) = tls_certs {
              // Attempt to build a client with TLS configuration provided by
              // callers (they can construct a `redis::TlsCertificates` in examples).
              // This uses the redis crate API to attach rustls-based cert/key
              // and optional root CA bytes to the client.
              Client::build_with_tls(ci, tls)?
            } else {
              Client::open(ci)?
            }
          }
          #[cfg(not(feature = "tls"))]
          {
            Client::open(connection_info)?
          }
        };
        let mut broker = Self {
          client: RedisClient::Single(client),
          script_manager: ScriptManager::default(),
        };
        broker.init_scripts().await?;
        Ok(broker)
      }
      #[cfg(feature = "cluster")]
      RedisConnectionType::Cluster(config) => {
        // 如果配置了使用 RESP3，创建 push_sender 通道
        // If RESP3 is configured, create push_sender channel
        let (push_receiver, cluster_client) = {
          let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
          let client = redis::cluster::ClusterClientBuilder::new(config)
            .use_protocol(redis::ProtocolVersion::RESP3)
            .push_sender(tx)
            .build()?;
          (
            std::sync::Arc::new(tokio::sync::Mutex::new(Some(rx))),
            client,
          )
        };
        let mut broker = Self {
          client: RedisClient::Cluster {
            client: cluster_client,
            push_receiver,
          },
          script_manager: ScriptManager::default(),
        };
        broker.init_scripts().await?;
        Ok(broker)
      }
      #[cfg(feature = "sentinel")]
      RedisConnectionType::Sentinel {
        master_name,
        sentinels,
        redis_connection_info,
        #[cfg(feature = "tls")]
        tls_certs,
      } => {
        // Use redis::SentinelClient to resolve the master and get a Client.
        // See https://docs.rs/redis/latest/redis/sentinel/index.html
        // Build a SentinelClient and wrap it in Arc<Mutex<>> so we can call its &mut async methods
        // If any sentinel ConnectionInfo contains a username/password, use that to populate
        // the SentinelNodeConnectionInfo so the resolved master connection will use auth.
        let mut node_conn_info: Option<SentinelNodeConnectionInfo> = None;
        if let Some(conn_info) = redis_connection_info {
          node_conn_info = Some(SentinelNodeConnectionInfo {
            #[cfg(feature = "tls")]
            tls_mode: None, // node_conn_info is used for resolved master; we will pass tls via builder when possible
            redis_connection_info: Some(conn_info),
          });
        }
        // If TLS certificates are provided, use SentinelClientBuilder to attach them
        #[cfg(feature = "tls")]
        let sentinel_client = {
          if let Some(certs) = tls_certs {
            // SentinelClientBuilder expects ConnectionAddr values for sentinels
            let addrs: Vec<redis::ConnectionAddr> = sentinels.iter().map(|ci| ci.addr.clone()).collect();
            let mut builder = redis::sentinel::SentinelClientBuilder::new(
              addrs,
              master_name.clone(),
              SentinelServerType::Master,
            )?;
            builder = builder.set_client_to_redis_certificates(certs);
            builder.build()?
          } else {
            // No certs provided; fall back to simple builder
            SentinelClient::build(sentinels, master_name.clone(), node_conn_info, SentinelServerType::Master)?
          }
        };
        let client = std::sync::Arc::new(tokio::sync::Mutex::new(sentinel_client));

        let mut broker = Self {
          client: RedisClient::Sentinel { client },
          script_manager: ScriptManager::default(),
        };
        broker.init_scripts().await?;
        Ok(broker)
      }
    }
  }

  /// 获取异步连接
  /// Get asynchronous connection
  pub async fn get_async_connection(&self) -> Result<RedisConnection> {
    let async_conn = self.client.get_connection().await?;
    Ok(async_conn)
  }

  /// 获取 PubSub 连接
  /// Get PubSub connection
  pub async fn get_pubsub(&self) -> Result<RedisPubSub> {
    match &self.client {
      RedisClient::Single(client) => {
        let pubsub = client.get_async_pubsub().await?;
        Ok(RedisPubSub::Single(pubsub))
      }
      #[cfg(feature = "cluster")]
      RedisClient::Cluster {
        client,
        push_receiver,
      } => {
        // 对于集群模式，从 push_receiver 取出接收器并获取连接
        // For cluster mode, take receiver from push_receiver and get connection
        let mut receiver_option = push_receiver.lock().await;
        if let Some(receiver) = receiver_option.take() {
          // 获取一个集群连接用于订阅
          // Get a cluster connection for subscription
          let connection = client.get_async_connection().await?;
          let cluster_pubsub = ClusterPubSubConnection::from_receiver(receiver, connection);
          Ok(RedisPubSub::Cluster(cluster_pubsub))
        } else {
          Err(Error::other(
            "Cluster PubSub receiver has already been taken. You can only create one PubSub connection for cluster mode."
          ))
        }
      }
      #[cfg(feature = "sentinel")]
      RedisClient::Sentinel { client, .. } => {
        // Lock the sentinel client and obtain an async pubsub from the resolved master
        let mut guard = client.lock().await;
        let master_client = guard.async_get_client().await?;
        let pubsub = master_client.get_async_pubsub().await?;
        Ok(RedisPubSub::Sentinel(pubsub))
      }
    }
  }

  /// 初始化脚本管理器，预加载所有脚本
  /// Initialize script manager and preload all scripts
  pub(crate) async fn init_scripts(&mut self) -> Result<()> {
    let mut conn = self.get_async_connection().await?;
    self.script_manager.load_scripts(&mut conn).await?;
    Ok(())
  }

  /// 将任务消息编码为字节
  /// Encode task message to bytes
  pub(crate) fn encode_task_message(&self, msg: &TaskMessage) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    msg.encode(&mut buf)?;
    Ok(buf)
  }

  /// 从字节解码任务消息,这里不需要修复任何编码错误
  /// Decode task message from bytes, no need to fix any encoding errors here
  pub fn decode_task_message(&self, data: &[u8]) -> Result<TaskMessage> {
    match TaskMessage::decode(data) {
      Ok(msg) => Ok(msg),
      // 不要处理这里的错误，直接返回错误就可以了
      // Do not handle errors here, just return the error
      Err(decode_err) => Err(Error::ProtoDecode(decode_err)),
    }
  }

  /// 从Task创建TaskMessage
  /// Create TaskMessage from Task
  pub(crate) fn task_to_message(&self, task: &Task) -> TaskMessage {
    TaskMessage {
      r#type: task.task_type.clone(),
      payload: task.payload.clone(),
      headers: task.headers.clone(),
      id: task
        .options
        .task_id
        .clone()
        .unwrap_or(Uuid::new_v4().to_string()),
      queue: if task.options.queue.clone().is_empty() {
        DEFAULT_QUEUE_NAME.to_string()
      } else {
        task.options.queue.clone()
      },
      retry: task.options.max_retry,
      retried: 0,
      error_msg: String::new(),
      last_failed_at: 0,
      timeout: task
        .options
        .timeout
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0),
      deadline: task.options.deadline.map(|d| d.timestamp()).unwrap_or(0),
      unique_key: String::new(),
      group_key: task.options.group.clone().unwrap_or_default(),
      retention: task
        .options
        .retention
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0),
      completed_at: 0,
    }
  }

  /// 批量写入 scheduler entries，兼容 Go 版 asynq
  /// Batch write scheduler entries, compatible with Go version asynq
  pub async fn write_scheduler_entries(
    &self,
    entries: &[SchedulerEntry],
    scheduler_id: &str,
    ttl_secs: u64,
  ) -> Result<()> {
    let mut args: Vec<RedisArg> = Vec::new();
    args.push(RedisArg::Int(ttl_secs as i64));
    for entry in entries {
      let mut buf = Vec::new();
      entry
        .encode(&mut buf)
        .map_err(|e| Error::other(format!("prost encode error: {e}")))?;
      args.push(RedisArg::Bytes(buf));
    }
    let key = keys::scheduler_entries_key(scheduler_id);
    let mut conn = self.get_async_connection().await?;
    let _: () = self
      .script_manager
      .eval_script(
        &mut conn,
        "write_scheduler_entries",
        std::slice::from_ref(&key),
        &args,
      )
      .await?;
    // 新增：ZADD 到全局 ZSET，score 为过期时间戳
    // New: ZADD to global ZSET, score is the expiration timestamp
    let zset_key = keys::ALL_SCHEDULERS;
    let expire_at = chrono::Utc::now().timestamp() + ttl_secs as i64;
    let _: () = conn.zadd(zset_key, &key, expire_at).await?;

    Ok(())
  }

  /// 记录调度事件，兼容 Go 版 asynq
  /// Record scheduling event, compatible with Go version asynq
  pub async fn record_scheduler_enqueue_event(
    &self,
    event: &SchedulerEnqueueEvent,
    entry_id: &str,
  ) -> Result<()> {
    let mut buf = Vec::new();
    event
      .encode(&mut buf)
      .map_err(|e| Error::other(format!("prost encode error: {e}")))?;
    let key = keys::scheduler_history_key(entry_id);
    let mut conn = self.get_async_connection().await?;
    let args = vec![
      RedisArg::Int(event.enqueue_time.map(|x| x.seconds).unwrap_or(0)),
      RedisArg::Bytes(buf),
      RedisArg::Int(1000),
    ];
    self
      .script_manager
      .eval_script::<()>(&mut conn, "record_scheduler_enqueue_event", &[key], &args)
      .await?;
    Ok(())
  }

  /// 通过脚本获取所有 SchedulerEntry，兼容 Go 版 asynq
  /// Get all SchedulerEntry through script, compatible with Go version asynq
  pub async fn scheduler_entries_script(
    &self,
    scheduler_id: &str,
  ) -> Result<std::collections::HashMap<String, Vec<u8>>> {
    let key = keys::scheduler_entries_key(scheduler_id);
    let mut conn = self.get_async_connection().await?;
    // 调用脚本，假设脚本返回 [key1, value1, key2, value2, ...]
    // Call the script, assuming the script returns [key1, value1, key2, value2, ...]
    let result: Vec<Vec<u8>> = self
      .script_manager
      .eval_script(&mut conn, "get_scheduler_entries", &[key], &[])
      .await?;
    let mut map = std::collections::HashMap::new();
    let mut iter = result.chunks_exact(2);
    while let Some([k, v]) = iter.next() {
      let key_str = String::from_utf8_lossy(k).to_string();
      map.insert(key_str, v.clone());
    }
    Ok(map)
  }

  /// 通过脚本获取调度事件列表，兼容 Go 版 asynq
  /// Get scheduling event list through script, compatible with Go version asynq
  pub async fn scheduler_events_script(&self, count: usize) -> Result<Vec<Vec<u8>>> {
    let key = keys::SCHEDULER_EVENTS.to_string();
    let mut conn = self.get_async_connection().await?;
    let args = vec![RedisArg::Int(count as i64)];
    let result: Vec<Vec<u8>> = self
      .script_manager
      .eval_script(&mut conn, "get_scheduler_events", &[key], &args)
      .await?;
    Ok(result)
  }

  /// 删除 scheduler entries 数据，兼容 Go 版 asynq
  /// Delete scheduler entries data, compatible with Go version asynq
  pub async fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()> {
    let key = keys::scheduler_entries_key(scheduler_id);
    let zset_key = keys::ALL_SCHEDULERS;
    let mut conn = self.get_async_connection().await?;
    // ZREM 全局调度器 ZSET
    // ZREM global scheduler ZSET
    let _: () = conn
      .zrem(zset_key, &key)
      .await
      .map_err(|e| Error::other(format!("redis ZREM error: {e}")))?;
    // DEL 具体 entry 列表
    // DEL specific entry list
    let _: () = conn
      .del(&key)
      .await
      .map_err(|e| Error::other(format!("redis DEL error: {e}")))?;
    Ok(())
  }
}
