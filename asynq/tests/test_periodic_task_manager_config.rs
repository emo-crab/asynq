//! PeriodicTaskManager 配置集成测试
//! PeriodicTaskManager configuration integration tests
//!
//! 此测试文件验证 PeriodicTaskManager 能够根据配置开关正确启动或禁用
//! This test file verifies that PeriodicTaskManager can be correctly enabled or disabled based on the configuration switch

use asynq::config::ServerConfig;
use std::time::Duration;

/// 测试默认情况下 PeriodicTaskManager 是禁用的
/// Test that PeriodicTaskManager is disabled by default
#[test]
fn test_periodic_task_manager_disabled_by_default() {
  let config = ServerConfig::default();
  assert!(!config.periodic_task_manager_enabled);
}

/// 测试可以启用 PeriodicTaskManager
/// Test that PeriodicTaskManager can be enabled
#[test]
fn test_enable_periodic_task_manager() {
  let config = ServerConfig::new().enable_periodic_task_manager(true);
  assert!(config.periodic_task_manager_enabled);
}

/// 测试可以禁用 PeriodicTaskManager
/// Test that PeriodicTaskManager can be disabled
#[test]
fn test_disable_periodic_task_manager() {
  let config = ServerConfig::new()
    .enable_periodic_task_manager(true)
    .enable_periodic_task_manager(false);
  assert!(!config.periodic_task_manager_enabled);
}

/// 测试可以配置 PeriodicTaskManager 的检查间隔
/// Test that PeriodicTaskManager check interval can be configured
#[test]
fn test_configure_periodic_task_manager_check_interval() {
  let custom_interval = Duration::from_secs(30);
  let config = ServerConfig::new()
    .periodic_task_manager_check_interval(custom_interval)
    .enable_periodic_task_manager(true);

  assert!(config.periodic_task_manager_enabled);
  assert_eq!(config.periodic_task_manager_check_interval, custom_interval);
}

/// 测试 PeriodicTaskManager 配置默认值
/// Test PeriodicTaskManager configuration default values
#[test]
fn test_periodic_task_manager_default_values() {
  let config = ServerConfig::default();
  assert!(!config.periodic_task_manager_enabled);
  assert_eq!(
    config.periodic_task_manager_check_interval,
    Duration::from_secs(60)
  );
}

/// 测试可以链式调用配置方法
/// Test that configuration methods can be chained
#[test]
fn test_periodic_task_manager_builder_pattern() {
  let config = ServerConfig::new()
    .concurrency(4)
    .enable_periodic_task_manager(true)
    .periodic_task_manager_check_interval(Duration::from_secs(120));

  assert_eq!(config.concurrency, 4);
  assert!(config.periodic_task_manager_enabled);
  assert_eq!(
    config.periodic_task_manager_check_interval,
    Duration::from_secs(120)
  );
}

/// 测试 PeriodicTaskManager 与其他组件配置的兼容性
/// Test compatibility of PeriodicTaskManager configuration with other components
#[test]
fn test_periodic_task_manager_with_other_components() {
  let config = ServerConfig::new()
    .enable_group_aggregator(true)
    .enable_periodic_task_manager(true)
    .group_grace_period(Duration::from_secs(30))
    .unwrap()
    .periodic_task_manager_check_interval(Duration::from_secs(45));

  assert!(config.group_aggregator_enabled);
  assert!(config.periodic_task_manager_enabled);
  assert_eq!(config.group_grace_period, Duration::from_secs(30));
  assert_eq!(
    config.periodic_task_manager_check_interval,
    Duration::from_secs(45)
  );
}
