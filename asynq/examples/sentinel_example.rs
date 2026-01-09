use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EmailPayload {
  to: String,
  subject: String,
  body: String,
}
#[cfg(all(feature = "sentinel", feature = "json"))]
#[tokio::main]
async fn main() -> Result<()> {
  // Assumptions:
  // - Sentinel is running on localhost:26379
  // - The monitored master name is `mymaster` (change below if different)
  let master_name = "mymaster";
  // sentinel addresses (change as needed)
  let sentinels = vec!["redis://localhost:26379"];
  let redis_connection_info = Some(
    redis::RedisConnectionInfo::default()
      .set_db(0)
      .set_password("mypassword"),
  );
  let redis_config =
    asynq::redis::RedisConnectionType::sentinel(master_name, sentinels, redis_connection_info)?;
  let client = asynq::client::Client::new(redis_config).await?;
  // Create task
  let payload = EmailPayload {
    to: "user@example.com".to_string(),
    subject: "Welcome!".to_string(),
    body: "Welcome to our service!".to_string(),
  };

  let task = asynq::task::Task::new_with_json("email:send", &payload)?;

  // Enqueue task
  let task_info = client.enqueue(task).await?;
  println!("Task enqueued with ID: {}", task_info.id);
  Ok(())
}
#[cfg(not(all(feature = "sentinel", feature = "json")))]
#[tokio::main]
async fn main() -> Result<()> {
  Ok(())
}
