//! Test server registration functionality
//!
//! This test verifies that the server registration properly:
//! 1. Adds server to asynq:servers ZSET
//! 2. Stores server info with correct key format
//! 3. Retrieves server info correctly

use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use asynq::base::{keys, Broker};
use asynq::proto::ServerInfo;
use asynq::{rdb::RedisBroker, redis::RedisConnectionType};

#[tokio::test]
async fn test_server_registration() -> Result<(), Box<dyn std::error::Error>> {
  // Skip test if no Redis available
  let redis_config = match RedisConnectionType::single("redis://localhost:6379") {
    Ok(config) => config,
    Err(_) => {
      println!("Skipping test - Redis not available");
      return Ok(());
    }
  };

  let broker = RedisBroker::new(redis_config).await?;

  // Test server info
  let hostname = "test-host";
  let pid = 12345;
  let server_uuid = Uuid::new_v4().to_string();
  let server_id = format!("{}:{}:{}", hostname, pid, server_uuid); // Full server ID

  let mut queues = HashMap::new();
  queues.insert("default".to_string(), 1);

  let server_info = ServerInfo {
    host: hostname.to_string(),
    pid,
    server_id: server_uuid.clone(), // Only the UUID part goes here
    concurrency: 10,
    queues,
    strict_priority: false,
    status: "active".to_string(),
    start_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    active_worker_count: 0,
  };

  // Test write_server_state (should register server)
  broker
    .write_server_state(&server_info, Duration::from_secs(300))
    .await?;

  // Test get_servers (should return our registered server)
  let servers = broker.get_servers().await?;
  println!("Found {} servers", servers.len());

  let found_server = servers.iter().find(|s| s.server_id == server_uuid);
  assert!(
    found_server.is_some(),
    "Server should be found in server list"
  );

  let found_server = found_server.unwrap();
  assert_eq!(found_server.host, hostname);
  assert_eq!(found_server.pid, pid);
  assert_eq!(found_server.concurrency, 10);

  // Test get_server_info (should return specific server info)
  let retrieved_info = broker
    .get_server_info(&keys::server_info_key(
      &found_server.host,
      found_server.pid,
      &found_server.server_id,
    ))
    .await?;
  assert!(
    retrieved_info.is_some(),
    "Server info should be retrievable"
  );

  let retrieved_info = retrieved_info.unwrap();
  assert_eq!(retrieved_info.server_id, server_uuid);
  assert_eq!(retrieved_info.host, hostname);
  assert_eq!(retrieved_info.pid, pid);

  // Test cleanup
  broker
    .clear_server_state(hostname, pid, &server_uuid)
    .await?;

  // Verify server is removed
  let servers_after_cleanup = broker.get_servers().await?;
  let found_after_cleanup = servers_after_cleanup
    .iter()
    .find(|s| s.server_id == server_id);
  assert!(
    found_after_cleanup.is_none(),
    "Server should be removed after cleanup"
  );

  println!("✅ All server registration tests passed!");
  Ok(())
}

fn main() {
  // This is here to make the file a standalone test
  println!("Run with: cargo test --test test_server_registration");
}
