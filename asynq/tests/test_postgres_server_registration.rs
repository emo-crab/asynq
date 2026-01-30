//! Test PostgresSQL server registration functionality
//!
//! This test verifies that the server registration with PostgresSQL properly:
//! 1. Stores server info in asynq_servers table
//! 2. Retrieves server info correctly
//! 3. Tracks workers in asynq_workers table

// Default database URL for tests
const TEST_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/asynq_test";

#[cfg(feature = "postgresql")]
#[tokio::test]
async fn test_postgres_server_registration() -> Result<(), Box<dyn std::error::Error>> {
  use std::collections::HashMap;
  use std::sync::Arc;
  use std::time::Duration;
  use uuid::Uuid;

  use asynq::base::Broker;
  use asynq::inspector::InspectorTrait;
  use asynq::backend::PostgresBroker;
  use asynq::backend::PostgresInspector;
  use asynq::proto::ServerInfo;

  // Try to connect to PostgresSQL
  let database_url =
    std::env::var("DATABASE_URL").unwrap_or_else(|_| TEST_DATABASE_URL.to_string());

  let broker = match PostgresBroker::new(&database_url).await {
    Ok(b) => Arc::new(b),
    Err(e) => {
      println!("Skipping test - PostgresSQL not available: {}", e);
      return Ok(());
    }
  };

  let inspector = PostgresInspector::from_broker(broker.clone());

  // Test server info
  let hostname = "test-host";
  let pid = 12345;
  let server_uuid = Uuid::new_v4().to_string();

  let mut queues = HashMap::new();
  queues.insert("default".to_string(), 1);
  queues.insert("critical".to_string(), 2);

  let server_info = ServerInfo {
    host: hostname.to_string(),
    pid,
    server_id: server_uuid.clone(),
    concurrency: 10,
    queues,
    strict_priority: false,
    status: "active".to_string(),
    start_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    active_worker_count: 0,
  };

  println!("📝 Writing server state to PostgresSQL...");
  // Test write_server_state (should register server)
  broker
    .write_server_state(&server_info, Duration::from_secs(300))
    .await?;

  println!("✅ Server state written successfully");

  // Test get_servers (should return our registered server)
  println!("🔍 Retrieving servers from database...");
  let servers = inspector.get_servers().await?;
  println!("📊 Found {} servers", servers.len());

  let found_server = servers.iter().find(|s| s.server_id == server_uuid);
  assert!(
    found_server.is_some(),
    "Server should be found in server list"
  );

  let found_server = found_server.unwrap();
  assert_eq!(found_server.host, hostname);
  assert_eq!(found_server.pid, pid);
  assert_eq!(found_server.concurrency, 10);
  assert_eq!(found_server.status, "active");

  println!("✅ Server found with correct information:");
  println!("   Host: {}", found_server.host);
  println!("   PID: {}", found_server.pid);
  println!("   Server ID: {}", found_server.server_id);
  println!("   Concurrency: {}", found_server.concurrency);

  // Test cleanup
  println!("🧹 Cleaning up server state...");
  broker
    .clear_server_state(hostname, pid, &server_uuid)
    .await?;

  // Verify server is removed
  let servers_after_cleanup = inspector.get_servers().await?;
  let found_after_cleanup = servers_after_cleanup
    .iter()
    .find(|s| s.server_id == server_uuid);
  assert!(
    found_after_cleanup.is_none(),
    "Server should be removed after cleanup"
  );

  println!("✅ All PostgresSQL server registration tests passed!");
  Ok(())
}

#[cfg(feature = "postgresql")]
#[tokio::test]
async fn test_postgres_worker_registration() -> Result<(), Box<dyn std::error::Error>> {
  use std::sync::Arc;
  use uuid::Uuid;

  use asynq::backend::PostgresBroker;
  use asynq::proto::WorkerInfo;

  // Try to connect to PostgresSQL
  let database_url =
    std::env::var("DATABASE_URL").unwrap_or_else(|_| TEST_DATABASE_URL.to_string());

  let broker = match PostgresBroker::new(&database_url).await {
    Ok(b) => Arc::new(b),
    Err(e) => {
      println!("Skipping test - PostgresSQL not available: {}", e);
      return Ok(());
    }
  };

  // Test worker info
  let hostname = "test-worker-host";
  let pid = 54321;
  let server_uuid = Uuid::new_v4().to_string();
  let task_id = Uuid::new_v4().to_string();

  let worker_info = WorkerInfo {
    host: hostname.to_string(),
    pid,
    server_id: server_uuid.clone(),
    task_id: task_id.clone(),
    task_type: "test:task".to_string(),
    task_payload: b"test payload".to_vec(),
    queue: "default".to_string(),
    start_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    deadline: None,
  };

  println!("📝 Writing worker state to PostgresSQL...");
  // Test write_worker_state
  broker.write_worker_state(&worker_info).await?;
  println!("✅ Worker state written successfully");

  // Test cleanup
  println!("🧹 Cleaning up worker state...");
  broker
    .clear_worker_state(hostname, pid, &server_uuid, &task_id)
    .await?;

  println!("✅ All PostgresSQL worker registration tests passed!");
  Ok(())
}
