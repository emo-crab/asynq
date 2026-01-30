//! Asynq Server implementation
//!
//! This module contains the main server implementation using Axum and WebSocket.

use crate::config::{MultiTenantAuth, TenantConfig};
use crate::error::{Error, Result};
use crate::handler::MessageHandler;
use crate::message::{ClientMessage, ServerMessage};
#[cfg(feature = "postgresql")]
use asynq::backend::PostgresBroker;
use asynq::backend::RedisBroker;
use asynq::backend::RedisConnectionType;
use asynq::base::Broker;
use asynq::components::forwarder::{Forwarder, ForwarderConfig};
use axum::{
  extract::{
    ws::{Message, WebSocket, WebSocketUpgrade},
    Request, State,
  },
  http::{header, StatusCode},
  middleware::{self, Next},
  response::{IntoResponse, Response},
  routing::get,
  Router,
};
use base64::prelude::*;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task_local;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

// Task-local variable for storing current tenant information
task_local! {
  pub static CURRENT_TENANT: Option<TenantConfig>;
}

/// Basic authentication credentials
#[derive(Clone, Debug)]
pub struct BasicAuth {
  pub username: String,
  pub password: String,
}

/// Authentication mode for the server
#[derive(Clone, Debug)]
pub enum AuthMode {
  /// No authentication required
  None,
  /// Single-tenant authentication (backward compatible)
  Single(BasicAuth),
  /// Multi-tenant authentication
  Multi(MultiTenantAuth),
}

/// Shared state for the server
pub struct AppState {
  /// Message handler with the broker
  pub handler: MessageHandler,
  /// Cancellation broadcast sender
  pub cancel_tx: broadcast::Sender<String>,
  /// Authentication mode (None, Single, or Multi)
  pub auth_mode: AuthMode,
}

/// Asynq Server
///
/// A standalone server that provides WebSocket API for cross-process
/// task queue communication. Supports multiple backends:
/// - Memory (default, no external dependencies)
/// - Redis (requires `redis` feature)
/// - PostgresSQL (requires `postgresql` feature)
///
/// Supports three authentication modes:
/// - No authentication (default)
/// - Single-tenant authentication (backward compatible)
/// - Multi-tenant authentication (isolates tasks per tenant)
///
/// Note: WebSocket backend is NOT supported to avoid circular dependency.
pub struct AsynqServer {
  /// Server address
  addr: SocketAddr,
  /// The broker instance (any implementation except WebSocket)
  broker: Arc<dyn Broker>,
  /// Authentication mode
  auth_mode: AuthMode,
}
impl AsynqServer {
  /// Create a new AsynqServer with a custom broker
  ///
  /// This allows using any Broker implementation (except WebSocket) as the backend.
  /// Useful for using Redis or PostgresSQL as the backing store.
  pub fn with_broker<A: Into<SocketAddr>>(addr: A, broker: Arc<dyn Broker>) -> Self {
    Self {
      addr: addr.into(),
      broker,
      auth_mode: AuthMode::None,
    }
  }

  /// Set HTTP Basic authentication for this server (single-tenant mode)
  ///
  /// When set, clients must provide matching username and password via HTTP Basic Auth
  /// in the Authorization header to connect to the WebSocket. If not set, all connections are allowed.
  ///
  /// This is the backward-compatible single-tenant authentication mode.
  pub fn with_basic_auth(mut self, username: String, password: String) -> Self {
    self.auth_mode = AuthMode::Single(BasicAuth { username, password });
    self
  }

  /// Set multi-tenant authentication for this server
  ///
  /// When set, clients must provide credentials via HTTP Basic Auth that match one of the
  /// configured tenants. Tasks will be isolated per tenant using queue prefixes.
  pub fn with_multi_tenant_auth(mut self, auth: MultiTenantAuth) -> Self {
    self.auth_mode = AuthMode::Multi(auth);
    self
  }

  /// Create a new AsynqServer with Redis backend
  pub async fn with_redis<A: Into<SocketAddr>>(
    addr: A,
    redis_connection: RedisConnectionType,
  ) -> Result<Self> {
    let broker = Arc::new(
      RedisBroker::new(redis_connection)
        .await
        .map_err(|e| Error::server(format!("Failed to connect to Redis: {}", e)))?,
    );
    Ok(Self {
      addr: addr.into(),
      broker,
      auth_mode: AuthMode::None,
    })
  }

  /// Create a new AsynqServer with PostgresSQL backend
  #[cfg(feature = "postgresql")]
  pub async fn with_postgres<A: Into<SocketAddr>>(addr: A, database_url: &str) -> Result<Self> {
    let broker = Arc::new(
      PostgresBroker::new(database_url)
        .await
        .map_err(|e| Error::server(format!("Failed to connect to PostgresSQL: {}", e)))?,
    );
    Ok(Self {
      addr: addr.into(),
      broker,
      auth_mode: AuthMode::None,
    })
  }

  /// Get the broker instance
  pub fn broker(&self) -> &Arc<dyn Broker> {
    &self.broker
  }

  /// Run the server
  pub async fn run(self) -> Result<()> {
    let (cancel_tx, _) = broadcast::channel::<String>(1024);

    let state = Arc::new(AppState {
      handler: MessageHandler::new(self.broker.clone()),
      cancel_tx: cancel_tx.clone(),
      auth_mode: self.auth_mode,
    });

    // Start the cancellation forwarder
    let broker_clone = self.broker.clone();
    let cancel_tx_clone = cancel_tx.clone();
    tokio::spawn(async move {
      if let Ok(mut stream) = broker_clone.cancellation_pub_sub().await {
        while let Some(result) = stream.next().await {
          if let Ok(task_id) = result {
            let _ = cancel_tx_clone.send(task_id);
          }
        }
      }
    });

    // Start the task forwarder to move scheduled/retry tasks to pending queue
    // This is essential for processing delayed tasks created with enqueue_in
    let forwarder_config = ForwarderConfig {
      interval: std::time::Duration::from_secs(5),
      queues: vec!["default".to_string()],
    };
    let forwarder = Arc::new(Forwarder::new(self.broker.clone(), forwarder_config));
    let forwarder_handle = forwarder.clone().start();
    info!("Task forwarder started for scheduled task processing");

    let app = Router::new()
      .route("/ws", get(websocket_handler))
      .route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth_middleware,
      ))
      .route("/health", get(health_handler))
      .layer(CorsLayer::permissive())
      .layer(TraceLayer::new_for_http())
      .with_state(state);

    let listener = tokio::net::TcpListener::bind(self.addr)
      .await
      .map_err(Error::Io)?;

    info!("Asynq server listening on {}", self.addr);

    // Run the server with graceful shutdown support
    let result = axum::serve(listener, app)
      .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
        info!("Received shutdown signal, stopping server...");
      })
      .await
      .map_err(Error::Io);

    // Cleanup: Stop the forwarder when server shuts down
    forwarder.shutdown();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), forwarder_handle).await;
    info!("Task forwarder stopped");

    result
  }
}

/// Authentication middleware
///
/// This middleware authenticates the request and sets the CURRENT_TENANT task-local variable
/// if authentication succeeds. The tenant information is then accessible throughout the
/// request handling chain without needing to pass it through function parameters.
async fn auth_middleware(
  State(state): State<Arc<AppState>>,
  mut req: Request,
  next: Next,
) -> Response {
  // Extract authentication from headers
  let auth_header = req
    .headers()
    .get(header::AUTHORIZATION)
    .and_then(|h| h.to_str().ok());

  // Determine tenant based on auth mode
  let tenant_option = match &state.auth_mode {
    AuthMode::None => {
      // No authentication required
      None
    }
    AuthMode::Single(credentials) => {
      // Single-tenant authentication (backward compatible)
      if let Some(auth_str) = auth_header {
        if let Some(encoded) = auth_str.strip_prefix("Basic ") {
          if let Ok(decoded_bytes) = BASE64_STANDARD.decode(encoded) {
            if let Ok(decoded_str) = String::from_utf8(decoded_bytes) {
              if let Some((username, password)) = decoded_str.split_once(':') {
                if username == credentials.username && password == credentials.password {
                  // Single-tenant mode doesn't need tenant config
                  None
                } else {
                  warn!("Invalid credentials for single-tenant mode");
                  return StatusCode::UNAUTHORIZED.into_response();
                }
              } else {
                return StatusCode::UNAUTHORIZED.into_response();
              }
            } else {
              return StatusCode::UNAUTHORIZED.into_response();
            }
          } else {
            return StatusCode::UNAUTHORIZED.into_response();
          }
        } else {
          return StatusCode::UNAUTHORIZED.into_response();
        }
      } else {
        return StatusCode::UNAUTHORIZED.into_response();
      }
    }
    AuthMode::Multi(multi_auth) => {
      // Multi-tenant authentication with backend verification
      if let Some(auth_str) = auth_header {
        if let Some(encoded) = auth_str.strip_prefix("Basic ") {
          if let Ok(decoded_bytes) = BASE64_STANDARD.decode(encoded) {
            if let Ok(decoded_str) = String::from_utf8(decoded_bytes) {
              if let Some((username, password)) = decoded_str.split_once(':') {
                match multi_auth.authenticate(username, password).await {
                  Ok(tenant) => {
                    info!(
                      "Tenant '{}' authenticated successfully via backend",
                      tenant.id
                    );
                    Some(tenant)
                  }
                  Err(crate::config::AuthError::RateLimited) => {
                    warn!("Authentication rate limited for user: {}", username);
                    return StatusCode::TOO_MANY_REQUESTS.into_response();
                  }
                  Err(e) => {
                    warn!("Authentication failed for user {}: {}", username, e);
                    return StatusCode::UNAUTHORIZED.into_response();
                  }
                }
              } else {
                return StatusCode::UNAUTHORIZED.into_response();
              }
            } else {
              return StatusCode::UNAUTHORIZED.into_response();
            }
          } else {
            return StatusCode::UNAUTHORIZED.into_response();
          }
        } else {
          return StatusCode::UNAUTHORIZED.into_response();
        }
      } else {
        return StatusCode::UNAUTHORIZED.into_response();
      }
    }
  };

  // Store tenant in request extensions for WebSocket upgrade handler
  req.extensions_mut().insert(tenant_option.clone());

  // Run the request with the tenant context set
  CURRENT_TENANT.scope(tenant_option, next.run(req)).await
}

/// Health check handler
async fn health_handler() -> impl IntoResponse {
  "OK"
}

/// WebSocket upgrade handler
///
/// Authentication is handled by the middleware, so we just extract the tenant
/// from request extensions and upgrade to WebSocket.
async fn websocket_handler(
  ws: WebSocketUpgrade,
  State(state): State<Arc<AppState>>,
  req: Request,
) -> impl IntoResponse {
  // Extract tenant from request extensions (set by auth middleware)
  let tenant_option = req
    .extensions()
    .get::<Option<TenantConfig>>()
    .cloned()
    .flatten();

  // Upgrade to WebSocket with tenant context
  ws.on_upgrade(move |socket| handle_socket(socket, state, tenant_option))
}

/// Handle a WebSocket connection
///
/// The tenant context is maintained via the CURRENT_TENANT task-local variable
/// which was set by the authentication middleware.
async fn handle_socket(
  socket: WebSocket,
  state: Arc<AppState>,
  tenant_config: Option<TenantConfig>,
) {
  // Run the socket handling with the tenant context
  CURRENT_TENANT
    .scope(tenant_config.clone(), async move {
      let (mut sender, mut receiver) = socket.split();

      // Subscribe to cancellation events
      let mut cancel_rx = state.cancel_tx.subscribe();

      // Use a channel to send outgoing messages
      let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<ServerMessage>(256);

      // Task to send outgoing messages
      let send_task = tokio::spawn(async move {
        loop {
          tokio::select! {
            Some(msg) = out_rx.recv() => {
              if let Ok(json) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json.into())).await.is_err() {
                  break;
                }
              }
            }
            Ok(task_id) = cancel_rx.recv() => {
              let msg = ServerMessage::Cancellation { task_id };
              if let Ok(json) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json.into())).await.is_err() {
                  break;
                }
              }
            }
            else => break,
          }
        }
      });

      // Handle incoming messages
      while let Some(msg) = receiver.next().await {
        let response = match msg {
          Ok(Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
            Ok(client_msg) => match state.handler.handle(client_msg).await {
              Ok(server_msg) => Some(server_msg),
              Err(e) => {
                error!("Handler error: {}", e);
                Some(ServerMessage::error(e.to_string()))
              }
            },
            Err(e) => {
              warn!("Invalid message: {}", e);
              Some(ServerMessage::error(format!("Invalid message: {}", e)))
            }
          },
          Ok(Message::Binary(data)) => match serde_json::from_slice::<ClientMessage>(&data) {
            Ok(client_msg) => match state.handler.handle(client_msg).await {
              Ok(server_msg) => Some(server_msg),
              Err(e) => {
                error!("Handler error: {}", e);
                Some(ServerMessage::error(e.to_string()))
              }
            },
            Err(e) => {
              warn!("Invalid binary message: {}", e);
              None
            }
          },
          Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => None,
          Ok(Message::Close(_)) => break,
          Err(e) => {
            error!("WebSocket error: {}", e);
            break;
          }
        };

        if let Some(response) = response {
          if out_tx.send(response).await.is_err() {
            break;
          }
        }
      }

      // Cleanup
      drop(out_tx);
      send_task.abort();

      if let Some(tenant) = tenant_config {
        info!("WebSocket connection closed for tenant '{}'", tenant.id);
      } else {
        info!("WebSocket connection closed");
      }
    })
    .await
}
