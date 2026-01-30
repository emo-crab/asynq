//! Multi-tenant configuration with dynamic backend authentication
//!
//! This module provides configuration support for multi-tenant authentication
//! where tenants are authenticated by attempting to connect to their own
//! Redis or PostgresSQL backend using their credentials.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Tenant configuration with authentication credentials
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantConfig {
  /// Tenant unique identifier
  pub id: String,
  /// Authentication username for this tenant
  pub username: String,
  /// Authentication password for this tenant
  pub password: String,
  /// Optional queue prefix for this tenant (defaults to tenant ID)
  pub queue_prefix: Option<String>,
  /// Backend connection string (for caching)
  pub backend_connection: Option<String>,
}

impl TenantConfig {
  /// Create a new tenant configuration
  pub fn new(id: String, username: String, password: String) -> Self {
    Self {
      id,
      username,
      password,
      queue_prefix: None,
      backend_connection: None,
    }
  }

  /// Set a custom queue prefix for this tenant
  pub fn with_queue_prefix(mut self, prefix: String) -> Self {
    self.queue_prefix = Some(prefix);
    self
  }

  /// Set backend connection string
  pub fn with_backend_connection(mut self, connection: String) -> Self {
    self.backend_connection = Some(connection);
    self
  }

  /// Get the queue prefix for this tenant
  pub fn get_queue_prefix(&self) -> &str {
    self.queue_prefix.as_deref().unwrap_or(&self.id)
  }

  /// Get the full queue name with tenant prefix
  pub fn get_tenant_queue(&self, queue: &str) -> String {
    format!("{}:{}", self.get_queue_prefix(), queue)
  }
}

/// Failed authentication attempt record for rate limiting
#[derive(Debug, Clone)]
struct FailedAttempt {
  count: u32,
  last_attempt: Instant,
}

/// Backend type for connection attempts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
  Redis,
  Postgres,
}

/// Multi-tenant authentication manager with backend-based authentication
///
/// This manager authenticates tenants by attempting to connect to their
/// own backend (Redis or PostgresSQL) using their credentials. Successful
/// connections are cached to avoid repeated connection attempts.
#[derive(Clone)]
pub struct MultiTenantAuth {
  /// Map from username to cached tenant configuration (thread-safe)
  tenants: Arc<RwLock<HashMap<String, TenantConfig>>>,
  /// Map from username to failed authentication attempts (for rate limiting)
  failed_attempts: Arc<RwLock<HashMap<String, FailedAttempt>>>,
  /// Backend type to use for authentication
  backend_type: BackendType,
  /// Backend connection template (e.g., "redis://{host}:6379" or "postgresql://{host}/db")
  backend_template: String,
  /// Maximum failed attempts before rate limiting kicks in
  max_failed_attempts: u32,
  /// Time window for rate limiting (in seconds)
  rate_limit_window: Duration,
}

impl std::fmt::Debug for MultiTenantAuth {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MultiTenantAuth")
      .field("backend_type", &self.backend_type)
      .field("backend_template", &self.backend_template)
      .field("max_failed_attempts", &self.max_failed_attempts)
      .field("rate_limit_window", &self.rate_limit_window)
      .finish()
  }
}

impl MultiTenantAuth {
  /// Create a new multi-tenant authentication manager with backend-based auth
  ///
  /// # Arguments
  /// * `backend_type` - The type of backend to use (Redis or PostgresSQL)
  /// * `backend_template` - Connection string template where credentials will be substituted
  ///   - For Redis: "redis://{username}:{password}@{host}:6379"
  ///   - For PostgresSQL: "postgresql://{username}:{password}@{host}/database"
  pub fn new(backend_type: BackendType, backend_template: String) -> Self {
    Self {
      tenants: Arc::new(RwLock::new(HashMap::new())),
      failed_attempts: Arc::new(RwLock::new(HashMap::new())),
      backend_type,
      backend_template,
      max_failed_attempts: 5,
      rate_limit_window: Duration::from_secs(300), // 5 minutes
    }
  }

  /// Set maximum failed attempts before rate limiting
  pub fn with_max_failed_attempts(mut self, max: u32) -> Self {
    self.max_failed_attempts = max;
    self
  }

  /// Set rate limit time window
  pub fn with_rate_limit_window(mut self, window: Duration) -> Self {
    self.rate_limit_window = window;
    self
  }

  /// Check if a username is rate limited
  fn is_rate_limited(&self, username: &str) -> bool {
    match self.failed_attempts.read() {
      Ok(attempts) => {
        if let Some(attempt) = attempts.get(username) {
          if attempt.count >= self.max_failed_attempts {
            let elapsed = attempt.last_attempt.elapsed();
            if elapsed < self.rate_limit_window {
              return true;
            }
          }
        }
        false
      }
      Err(_) => {
        // If lock is poisoned, be conservative and don't rate limit
        false
      }
    }
  }

  /// Record a failed authentication attempt
  fn record_failed_attempt(&self, username: &str) {
    if let Ok(mut attempts) = self.failed_attempts.write() {
      let entry = attempts
        .entry(username.to_string())
        .or_insert(FailedAttempt {
          count: 0,
          last_attempt: Instant::now(),
        });

      // Reset count if outside the rate limit window
      if entry.last_attempt.elapsed() >= self.rate_limit_window {
        entry.count = 1;
      } else {
        entry.count += 1;
      }
      entry.last_attempt = Instant::now();
    }
    // If lock is poisoned, silently fail - rate limiting is best effort
  }

  /// Clear failed attempts for a username (called on successful auth)
  fn clear_failed_attempts(&self, username: &str) {
    if let Ok(mut attempts) = self.failed_attempts.write() {
      attempts.remove(username);
    }
    // If lock is poisoned, silently fail - clearing attempts is best effort
  }

  /// Build connection string from template and credentials
  fn build_connection_string(&self, username: &str, password: &str) -> String {
    self
      .backend_template
      .replace("{username}", username)
      .replace("{password}", password)
  }

  /// Attempt to authenticate by connecting to the backend
  ///
  /// Returns Ok(TenantConfig) if authentication succeeds, Err otherwise.
  /// On success, the tenant is cached for future requests.
  pub async fn authenticate(
    &self,
    username: &str,
    password: &str,
  ) -> Result<TenantConfig, AuthError> {
    // Check rate limiting first
    if self.is_rate_limited(username) {
      return Err(AuthError::RateLimited);
    }

    // Check if already cached
    {
      if let Ok(tenants) = self.tenants.read() {
        if let Some(tenant) = tenants.get(username) {
          if tenant.password == password {
            return Ok(tenant.clone());
          }
        }
      }
      // If lock is poisoned, continue to backend authentication
    }

    // Attempt to connect to backend with credentials
    let connection_string = self.build_connection_string(username, password);

    match self.try_backend_connection(&connection_string).await {
      Ok(()) => {
        // Success! Cache this tenant
        let tenant = TenantConfig {
          id: username.to_string(),
          username: username.to_string(),
          password: password.to_string(),
          queue_prefix: Some(username.to_string()),
          backend_connection: Some(connection_string.clone()),
        };

        if let Ok(mut tenants) = self.tenants.write() {
          tenants.insert(username.to_string(), tenant.clone());
        }
        // If caching fails due to poisoned lock, still return success

        // Clear any failed attempts
        self.clear_failed_attempts(username);

        Ok(tenant)
      }
      Err(e) => {
        // Failed - record the attempt
        self.record_failed_attempt(username);
        Err(e)
      }
    }
  }

  /// Try to connect to the backend to verify credentials
  async fn try_backend_connection(&self, connection_string: &str) -> Result<(), AuthError> {
    match self.backend_type {
      BackendType::Redis => self.try_redis_connection(connection_string).await,
      BackendType::Postgres => self.try_postgresql_connection(connection_string).await,
    }
  }

  /// Try to connect to Redis
  async fn try_redis_connection(&self, connection_string: &str) -> Result<(), AuthError> {
    use asynq::backend::RedisBroker;

    match asynq::backend::RedisConnectionType::single(connection_string) {
      Ok(config) => match RedisBroker::new(config).await {
        Ok(_broker) => Ok(()),
        Err(_) => Err(AuthError::ConnectionFailed),
      },
      Err(_) => Err(AuthError::InvalidCredentials),
    }
  }

  /// Try to connect to PostgresSQL
  #[cfg(feature = "postgresql")]
  async fn try_postgresql_connection(&self, connection_string: &str) -> Result<(), AuthError> {
    use asynq::backend::PostgresBroker;

    match PostgresBroker::new(connection_string).await {
      Ok(_broker) => Ok(()),
      Err(_) => Err(AuthError::ConnectionFailed),
    }
  }

  #[cfg(not(feature = "postgresql"))]
  async fn try_postgresql_connection(&self, _connection_string: &str) -> Result<(), AuthError> {
    Err(AuthError::BackendNotAvailable)
  }

  /// Add a tenant to the cache manually (for pre-configured tenants)
  pub fn add_tenant(&self, config: TenantConfig) {
    if let Ok(mut tenants) = self.tenants.write() {
      tenants.insert(config.username.clone(), config);
    }
    // If lock is poisoned, silently fail
  }

  /// Remove a tenant from the cache
  pub fn remove_tenant(&self, username: &str) -> Option<TenantConfig> {
    self
      .tenants
      .write()
      .ok()
      .and_then(|mut tenants| tenants.remove(username))
  }

  /// List all cached tenants
  pub fn list_tenants(&self) -> Vec<String> {
    self
      .tenants
      .read()
      .map(|tenants| tenants.values().map(|t| t.id.clone()).collect())
      .unwrap_or_default()
  }

  /// Check if multi-tenant auth is enabled (has any cached tenants)
  pub fn is_enabled(&self) -> bool {
    // Always enabled when using backend-based auth
    true
  }

  /// Get the number of cached tenants
  pub fn tenant_count(&self) -> usize {
    self
      .tenants
      .read()
      .map(|tenants| tenants.len())
      .unwrap_or(0)
  }
}

/// Authentication error types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
  /// User is rate limited due to too many failed attempts
  RateLimited,
  /// Invalid credentials format
  InvalidCredentials,
  /// Failed to connect to backend
  ConnectionFailed,
  /// Backend feature not enabled
  BackendNotAvailable,
}

impl std::fmt::Display for AuthError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      AuthError::RateLimited => write!(f, "Too many failed authentication attempts"),
      AuthError::InvalidCredentials => write!(f, "Invalid credentials"),
      AuthError::ConnectionFailed => write!(f, "Failed to connect to backend"),
      AuthError::BackendNotAvailable => write!(f, "Backend not available"),
    }
  }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_tenant_config_creation() {
    let tenant = TenantConfig::new(
      "tenant1".to_string(),
      "user1".to_string(),
      "pass1".to_string(),
    );
    assert_eq!(tenant.id, "tenant1");
    assert_eq!(tenant.username, "user1");
    assert_eq!(tenant.get_queue_prefix(), "tenant1");
  }

  #[test]
  fn test_tenant_config_with_custom_prefix() {
    let tenant = TenantConfig::new(
      "tenant1".to_string(),
      "user1".to_string(),
      "pass1".to_string(),
    )
    .with_queue_prefix("custom".to_string());
    assert_eq!(tenant.get_queue_prefix(), "custom");
    assert_eq!(tenant.get_tenant_queue("default"), "custom:default");
  }

  #[test]
  fn test_multi_tenant_auth_caching() {
    let auth = MultiTenantAuth::new(
      BackendType::Redis,
      "redis://{username}:{password}@localhost:6379".to_string(),
    );
    assert!(auth.is_enabled()); // Always enabled with backend auth

    // Manually add a tenant to cache
    let tenant1 = TenantConfig::new(
      "tenant1".to_string(),
      "user1".to_string(),
      "pass1".to_string(),
    );
    auth.add_tenant(tenant1);

    assert_eq!(auth.tenant_count(), 1);

    // List tenants
    let tenants = auth.list_tenants();
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0], "tenant1");

    // Remove tenant
    let removed = auth.remove_tenant("user1");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id, "tenant1");
    assert_eq!(auth.tenant_count(), 0);
  }

  #[test]
  fn test_rate_limiting() {
    let auth = MultiTenantAuth::new(
      BackendType::Redis,
      "redis://{username}:{password}@localhost:6379".to_string(),
    )
    .with_max_failed_attempts(3)
    .with_rate_limit_window(Duration::from_secs(60));

    // Simulate failed attempts
    for _ in 0..3 {
      auth.record_failed_attempt("test_user");
    }

    // Should now be rate limited
    assert!(auth.is_rate_limited("test_user"));

    // Clear failed attempts
    auth.clear_failed_attempts("test_user");
    assert!(!auth.is_rate_limited("test_user"));
  }

  #[test]
  fn test_connection_string_building() {
    let auth = MultiTenantAuth::new(
      BackendType::Redis,
      "redis://{username}:{password}@localhost:6379".to_string(),
    );

    let conn_str = auth.build_connection_string("testuser", "testpass");
    assert_eq!(conn_str, "redis://testuser:testpass@localhost:6379");
  }
}
