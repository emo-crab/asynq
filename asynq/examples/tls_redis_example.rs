use anyhow::Result;
use asynq::redis::RedisConnectionType;
use std::fs::File;

// This example is compiled/run only when the crate is built with the `tls` feature.
// Run with: `cargo run -p asynq --example tls_redis_example --features tls`.
#[cfg(feature = "tls")]
#[tokio::main]
async fn main() -> Result<()> {
  // Expect first CLI arg to be the Redis URL, e.g. rediss://127.0.0.1:6380
  let mut args = std::env::args().skip(1);
  let redis_url = args
    .next()
    .unwrap_or_else(|| "rediss://127.0.0.1:6380".to_string());

  // Paths for cert, key and ca can be provided by env vars or left empty to use system roots
  let cert_path = Some("asynq/examples//certs/redis.crt");
  let key_path = Some("asynq/examples//certs/redis.key");
  let ca_path = Some("asynq/examples//certs/ca.crt");
  // Helper: read file to Vec<u8>
  fn read_bytes(path: &str) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
  }
  // Build optional client_tls and root_cert
  let client_tls = if let (Some(cert), Some(key)) = (cert_path, key_path) {
    Some(redis::ClientTlsConfig {
      client_cert: read_bytes(cert)?,
      client_key: read_bytes(key)?,
    })
  } else {
    None
  };
  let root_cert = if let Some(ca) = ca_path {
    Some(read_bytes(ca)?)
  } else {
    None
  };
  let tls_certs = redis::TlsCertificates {
    client_tls,
    root_cert,
  };
  // Build a RedisConnectionType with TLS certs
  let conn = RedisConnectionType::single_with_tls(redis_url.as_str(), tls_certs)?;
  // Create a broker via the client APIs in asynq
  let broker = asynq::rdb::redis_broker::RedisBroker::new(conn).await?;
  // Get a connection and PING
  let mut conn = broker.get_async_connection().await?;
  let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
  println!("PING -> {pong}");
  Ok(())
}
#[cfg(not(feature = "tls"))]
fn main() {}
