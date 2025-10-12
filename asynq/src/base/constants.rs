use std::time::Duration;

/// 默认队列名称
pub const DEFAULT_QUEUE_NAME: &str = "default";

/// 默认最大重试次数
pub const DEFAULT_MAX_RETRY: i32 = 3;
pub const DEFAULT_MAX_ARCHIVE_SIZE: i64 = 10240;
pub const DEFAULT_ARCHIVED_EXPIRATION_IN_DAYS: i64 = 90;
/// 默认任务超时时间
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60); // 30 分钟

/// 版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const TIME_LAYOUT_YMD: &str = "%Y-%m-%d";
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_constants() {
    assert_eq!(DEFAULT_QUEUE_NAME, "default");
    assert_eq!(DEFAULT_MAX_RETRY, 3);
    assert_eq!(DEFAULT_TIMEOUT, Duration::from_secs(1800));
  }
}
