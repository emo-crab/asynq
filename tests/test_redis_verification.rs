//! Test to verify that Redis ZSETs are properly populated
//! This test validates that both asynq:servers and asynq:workers are correctly registered

use redis::{aio::MultiplexedConnection, AsyncCommands};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use asynq::base::{keys, Broker};
use asynq::proto::ServerInfo;
use asynq::{rdb::RedisBroker, redis::RedisConfig};

#[tokio::test]
async fn test_redis_zsets_population() -> Result<(), Box<dyn std::error::Error>> {
  // Skip test if no Redis available
  let redis_config = match RedisConfig::from_url("redis://localhost:6379") {
    Ok(config) => config,
    Err(_) => {
      println!("Skipping test - Redis not available");
      return Ok(());
    }
  };

  let broker = RedisBroker::new(redis_config.clone())?;

  // Test connection
  broker.ping().await?;

  // Create client for direct Redis access
  let client = redis::Client::open(redis_config.connection_info)?;
  let mut conn: MultiplexedConnection = client.get_multiplexed_tokio_connection().await?;

  // Clean up any existing data
  let _: () = conn.del("asynq:servers").await?;
  let _: () = conn.del("asynq:workers").await?;

  // Test server info
  let hostname = "test-verification-host";
  let pid = 54321;
  let server_uuid = Uuid::new_v4().to_string();
  let server_key = keys::server_info_key(hostname, pid, &server_uuid);

  let mut queues = HashMap::new();
  queues.insert("default".to_string(), 1);

  let server_info = ServerInfo {
    host: hostname.to_string(),
    pid,
    server_id: server_uuid.clone(),
    concurrency: 5,
    queues,
    strict_priority: false,
    status: "active".to_string(),
    start_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    active_worker_count: 0,
  };

  println!("🔧 Registering server: {}", server_key);

  // Register server
  broker
    .write_server_state(&server_info, Duration::from_secs(300))
    .await?;

  // Check asynq:servers ZSET
  let servers_zset: Vec<String> = conn.zrange("asynq:servers", 0, -1).await?;
  println!("📊 asynq:servers ZSET: {:?}", servers_zset);

  assert_eq!(
    servers_zset.len(),
    1,
    "Should have exactly 1 server in ZSET"
  );
  assert_eq!(servers_zset[0], server_key, "Server ID should match");

  // Check asynq:workers ZSET
  let workers_zset: Vec<String> = conn.zrange("asynq:workers", 0, -1).await?;
  println!("📊 asynq:workers ZSET: {:?}", workers_zset);

  assert_eq!(
    workers_zset.len(),
    1,
    "Should have exactly 1 worker key in ZSET"
  );
  let workers_key = keys::workers_key(hostname, pid, &server_uuid);
  assert_eq!(
    workers_zset[0], workers_key,
    "Worker key should match expected format"
  );

  // Check that server key exists and contains data
  let server_exists: bool = conn.exists(&server_key).await?;
  assert!(server_exists, "Server key should exist in Redis");

  // Check that workers key behavior (gets created but may not exist if no workers)
  // Since no workers are passed to the script, the key may not exist or be empty
  let workers_len: i32 = conn.llen(&workers_key).await.unwrap_or(0);
  assert_eq!(
    workers_len, 0,
    "Workers list should be empty when no workers are registered"
  );

  // Test rdb's get_servers method
  let retrieved_servers = broker.get_servers().await?;
  assert_eq!(
    retrieved_servers.len(),
    1,
    "Should retrieve exactly 1 server"
  );
  assert_eq!(retrieved_servers[0].server_id, server_uuid);
  assert_eq!(retrieved_servers[0].host, hostname);
  assert_eq!(retrieved_servers[0].pid, pid);

  println!("✅ All Redis ZSET verification tests passed!");

  // Clean up
  let _: () = conn.del("asynq:servers").await?;
  let _: () = conn.del("asynq:workers").await?;
  let _: () = conn.del(&server_key).await?;
  let _: () = conn.del(&workers_key).await?;

  Ok(())
}

fn main() {
  println!("Run with: cargo test --test test_redis_verification");
}
