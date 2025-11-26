use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}
#[cfg(feature = "sentinel")]
#[tokio::main]
async fn main() -> Result<()> {
  // Assumptions:
  // - Sentinel is running on localhost:26379
  // - The monitored master name is `mymaster` (change below if different)
  let master_name = "mymaster";
  // sentinel addresses (change as needed)
  let sentinels = vec!["redis://localhost:26379"];
  let redis_connection_info = Some(RedisConnectionInfo {
    db: 0,
    username: None,
    password: Some("mypassword".to_string()),
    protocol: Default::default(),
  });
  let redis_config = RedisConnectionType::sentinel(master_name, sentinels, redis_connection_info)?;
  let client = Client::new(redis_config).await?;
  // Create task
  let payload = EmailPayload {
    to: "user@example.com".to_string(),
    subject: "Welcome!".to_string(),
    body: "Welcome to our service!".to_string(),
  };

  let task = Task::new_with_json("email:send", &payload)?;

  // Enqueue task
  let task_info = client.enqueue(task).await?;
  println!("Task enqueued with ID: {}", task_info.id);
  Ok(())
}
#[cfg(not(feature = "sentinel"))]
#[tokio::main]
async fn main() -> Result<()> {
Ok(())
}