// mod test_redis_broker
use asynq::rdb::RedisBroker;
use asynq::redis::RedisConnectionConfig;

// RedisBroker 单元测试
// ...以下为原 tests 模块内容...

#[tokio::test]
async fn test_redis_broker_creation() {
  let config = RedisConnectionConfig::single("redis://127.0.0.1:6379").unwrap();
  let broker = RedisBroker::new(config).await;
  assert!(broker.is_ok());
}

// ...其余测试内容同原 tests 模块，全部迁移...

// RedisBroker 连接与构造相关方法
// ...实现内容将在后续插入...
