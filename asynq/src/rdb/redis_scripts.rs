//! Redis Lua 脚本模块
//!
//! 提供与 hibiken/asynq Go 版本兼容的 Redis Lua 脚本，确保操作的原子性
//! 需要保证与 Go 版本的脚本保持同步,以避免潜在的兼容性问题
//! 目前已经核对并同步了大部分脚本，lua脚本不再频繁变动

use crate::error::{Error, Result};
use crate::redis::RedisConnection;
use phf::phf_map;
use redis::{RedisWrite, ToRedisArgs};

#[derive(Clone, Debug)]
pub enum RedisArg {
  Int(i64),
  Str(String),
  Bytes(Vec<u8>),
  Bool(bool),
  Float(f64),
  // 你可以根据需要继续扩展
}

impl ToRedisArgs for RedisArg {
  fn write_redis_args<W>(&self, out: &mut W)
  where
    W: ?Sized + RedisWrite,
  {
    match self {
      RedisArg::Int(i) => i.write_redis_args(out),
      RedisArg::Str(s) => s.write_redis_args(out),
      RedisArg::Bytes(b) => b.write_redis_args(out),
      RedisArg::Bool(b) => (*b as i64).write_redis_args(out),
      RedisArg::Float(f) => f.write_redis_args(out),
    }
  }
}
/// Redis Lua 脚本集合 - 与 Go 版本完全兼容
pub mod scripts {
  /// 入队任务脚本 - 基于 Go 版本的 enqueueCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:pending
  /// `ARGV[1]` -> task message data
  /// `ARGV[2]` -> task ID
  /// `ARGV[3]` -> current unix time in nsec
  /// Returns 1 if successfully enqueued, 0 if task ID already exists
  pub const ENQUEUE: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 1 then
            return 0
        end
        redis.call("HSET", KEYS[1],
                   "msg", ARGV[1],
                   "state", "pending",
                   "pending_since", ARGV[3])
        redis.call("LPUSH", KEYS[2], ARGV[2])
        return 1
    "#;

  /// 入队唯一任务脚本 - 基于 Go 版本的 enqueueUniqueCmd
  /// Enqueue unique task script - based on Go version's enqueueUniqueCmd
  /// `KEYS[1]` -> unique key
  /// `KEYS[2]` -> asynq:{`<qname>`}:t:`<task_id>`
  /// `KEYS[3]` -> asynq:{`<qname>`}:pending
  /// `ARGV[1]` -> 任务ID / Task ID
  /// `ARGV[2]` -> 唯一锁TTL / Uniqueness lock TTL
  /// `ARGV[3]` -> 任务消息数据 / Task message data
  /// `ARGV[4]` -> 当前时间（纳秒）/ Current unix time in nsec
  /// 返回值：1=成功，0=ID冲突，-1=唯一键已存在
  /// Returns: 1 if successfully enqueued, 0 if task ID conflicts, -1 if unique key exists
  pub const ENQUEUE_UNIQUE: &str = r#"
        local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
        if not ok then
          return -1
        end
        if redis.call("EXISTS", KEYS[2]) == 1 then
          return 0
        end
        redis.call("HSET", KEYS[2],
                   "msg", ARGV[3],
                   "state", "pending",
                   "pending_since", ARGV[4],
                   "unique_key", KEYS[1])
        redis.call("LPUSH", KEYS[3], ARGV[1])
        return 1
    "#;

  /// 调度任务脚本 - 基于 Go 版本的 scheduleCmd
  /// Schedule task script - based on Go version's scheduleCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:scheduled
  /// `ARGV[1]` -> 任务消息数据 / Task message data
  /// `ARGV[2]` -> 执行时间（Unix时间戳）/ process_at time in Unix time
  /// `ARGV[3]` -> 任务ID / Task ID
  /// 返回值：1=成功，0=ID已存在
  /// Returns: 1 if successfully enqueued, 0 if task ID already exists
  pub const SCHEDULE: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 1 then
            return 0
        end
        redis.call("HSET", KEYS[1],
                   "msg", ARGV[1],
                   "state", "scheduled")
        redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
        return 1
    "#;

  /// 重试任务脚本 - Go: retryCmd
  /// Retry task script - Go: retryCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:active
  /// `KEYS[3]` -> asynq:{`<qname>`}:lease
  /// `KEYS[4]` -> asynq:{`<qname>`}:retry
  /// `KEYS[5]` -> asynq:{`<qname>`}:processed:`<yyyy-mm-dd>`
  /// `KEYS[6]` -> asynq:{`<qname>`}:failed:`<yyyy-mm-dd>`
  /// `KEYS[7]` -> asynq:{`<qname>`}:processed
  /// `KEYS[8]` -> asynq:{`<qname>`}:failed
  /// `ARGV[1]` -> 任务ID / Task ID
  /// `ARGV[2]` -> 更新后的任务消息数据 / Updated task message data
  /// `ARGV[3]` -> 重试时间（Unix时间戳）/ retry at (unix timestamp)
  /// `ARGV[4]` -> 统计过期时间戳 / stats expiration timestamp
  /// `ARGV[5]` -> 是否失败 / is_failure (1 or 0)
  /// `ARGV[6]` -> 最大int64值 / max int64 value
  pub const RETRY: &str = r#"
        if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
        redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry")
        if tonumber(ARGV[5]) == 1 then
            local n = redis.call("INCR", KEYS[5])
            if tonumber(n) == 1 then
                redis.call("EXPIREAT", KEYS[5], ARGV[4])
            end
            local m = redis.call("INCR", KEYS[6])
            if tonumber(m) == 1 then
                redis.call("EXPIREAT", KEYS[6], ARGV[4])
            end
            local total = redis.call("GET", KEYS[7])
            if tonumber(total) == tonumber(ARGV[6]) then
                redis.call("SET", KEYS[7], 1)
                redis.call("SET", KEYS[8], 1)
            else
                redis.call("INCR", KEYS[7])
                redis.call("INCR", KEYS[8])
            end
        end
        return redis.status_reply("OK")
    "#;

  /// 归档任务脚本 - 与 Go 版本兼容 archiveCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:active
  /// `KEYS[3]` -> asynq:{`<qname>`}:lease
  /// `KEYS[4]` -> asynq:{`<qname>`}:archived
  /// `KEYS[5]` -> asynq:{`<qname>`}:processed:`<yyyy-mm-dd>`
  /// `KEYS[6]` -> asynq:{`<qname>`}:failed:`<yyyy-mm-dd>`
  /// `KEYS[7]` -> asynq:{`<qname>`}:processed
  /// `KEYS[8]` -> asynq:{`<qname>`}:failed
  /// `KEYS[9]` -> asynq:{`<qname>`}:t:
  /// -------
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> updated base.TaskMessage value
  /// `ARGV[3]` -> died_at UNIX timestamp
  /// `ARGV[4]` -> cutoff timestamp (e.g., 90 days ago)
  /// `ARGV[5]` -> max number of tasks in archive (e.g., 100)
  /// `ARGV[6]` -> stats expiration timestamp
  /// `ARGV[7]` -> max int64 value
  pub const ARCHIVE: &str = r#"
        if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
        local old = redis.call("ZRANGE", KEYS[4], "-inf", ARGV[4], "BYSCORE")
        if #old > 0 then
            for _, id in ipairs(old) do
                redis.call("DEL", KEYS[9] .. id)
            end
            redis.call("ZREM", KEYS[4], unpack(old))
        end

        local extra = redis.call("ZRANGE", KEYS[4], 0, -ARGV[5])
        if #extra > 0 then
            for _, id in ipairs(extra) do
                redis.call("DEL", KEYS[9] .. id)
            end
            redis.call("ZREM", KEYS[4], unpack(extra))
        end

        redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "archived")
        local n = redis.call("INCR", KEYS[5])
        if tonumber(n) == 1 then
            redis.call("EXPIREAT", KEYS[5], ARGV[6])
        end
        local m = redis.call("INCR", KEYS[6])
        if tonumber(m) == 1 then
            redis.call("EXPIREAT", KEYS[6], ARGV[6])
        end
        local total = redis.call("GET", KEYS[7])
        if tonumber(total) == tonumber(ARGV[7]) then
            redis.call("SET", KEYS[7], 1)
            redis.call("SET", KEYS[8], 1)
        else
            redis.call("INCR", KEYS[7])
            redis.call("INCR", KEYS[8])
        end
        return redis.status_reply("OK")
    "#;

  /// 转发调度任务脚本 - 与 Go 版本兼容
  /// 参数: `KEYS[1]` = 调度键, `KEYS[2]` = 队列键, `ARGV[1]` = 当前时间戳
  pub const FORWARD_SCHEDULED: &str = r#"
        local scheduled_key = KEYS[1]
        local queue_key = KEYS[2]
        local now = tonumber(ARGV[1])

        -- 获取到期的调度任务
        local tasks = redis.call("ZRANGEBYSCORE", scheduled_key, "-inf", now, "LIMIT", 0, 100)

        if #tasks == 0 then
            return 0
        end

        -- 批量移动任务到队列
        for _, task in ipairs(tasks) do
            redis.call("LPUSH", queue_key, task)
            redis.call("ZREM", scheduled_key, task)
        end

        return #tasks
    "#;

  /// 转发重试任务脚本 - 与 Go 版本兼容
  /// 参数: `KEYS[1]` = 重试键, `KEYS[2]` = 队列键, `ARGV[1]` = 当前时间戳
  pub const FORWARD_RETRY: &str = r#"
        local retry_key = KEYS[1]
        local queue_key = KEYS[2]
        local now = tonumber(ARGV[1])

        -- 获取到期的重试任务
        local tasks = redis.call("ZRANGEBYSCORE", retry_key, "-inf", now, "LIMIT", 0, 100)

        if #tasks == 0 then
            return 0
        end

        -- 批量移动任务到队列
        for _, task in ipairs(tasks) do
            redis.call("LPUSH", queue_key, task)
            redis.call("ZREM", retry_key, task)
        end

        return #tasks
    "#;

  /// 清理过期任务脚本 - 与 Go 版本兼容
  /// 参数: `KEYS[1]` = 完成键, `ARGV[1]` = 当前时间戳
  pub const CLEANUP_COMPLETED: &str = r#"
        local completed_key = KEYS[1]
        local now = tonumber(ARGV[1])

        -- 删除过期的完成任务
        local removed = redis.call("ZREMRANGEBYSCORE", completed_key, "-inf", now)

        return removed
    "#;

  /// 速率限制脚本 - 滑动窗口实现
  /// 参数: `KEYS[1]` = 速率限制键, `ARGV[1]` = 窗口大小(秒), `ARGV[2]` = 限制数量, `ARGV[3]` = 当前时间戳
  pub const RATE_LIMIT: &str = r#"
        local key = KEYS[1]
        local window = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local window_start = now - window

        -- 清理过期的请求记录
        redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)

        -- 获取当前窗口内的请求数量
        local current_count = redis.call("ZCARD", key)

        if current_count < limit then
            -- 未达到限制，记录当前请求
            redis.call("ZADD", key, now, now)
            redis.call("EXPIRE", key, window + 1)
            return {1, limit - current_count - 1}  -- {allowed, remaining}
        else
            -- 达到限制
            return {0, 0}
        end
    "#;

  /// 恢复孤儿任务脚本 - 与 Go 版本兼容
  /// 参数: `KEYS[1]` = 活跃键, `KEYS[2]` = 队列键, `ARGV[1]` = 超时时间戳
  pub const RECOVER_ORPHANED: &str = r#"
        local active_key = KEYS[1]
        local queue_key = KEYS[2]
        local cutoff = tonumber(ARGV[1])

        -- 获取所有活跃任务
        local tasks = redis.call("SMEMBERS", active_key)
        local recovered = 0

        for _, task in ipairs(tasks) do
            -- 解析任务消息以获取开始时间
            -- 这里简化处理，实际应该解析 protobuf 消息
            -- 如果任务超时，将其重新加入队列
            redis.call("SREM", active_key, task)
            redis.call("LPUSH", queue_key, task)
            recovered = recovered + 1
        end

        return recovered
    "#;

  /// 延长租约脚本 - Go: ExtendLease
  /// `KEYS[1]` -> lease key
  /// `ARGV[1]` -> task_id
  /// `ARGV[2]` -> lease duration (seconds)
  pub const EXTEND_LEASE: &str = r#"
        local lease_key = KEYS[1]
        local task_id = ARGV[1]
        local duration = tonumber(ARGV[2])

        -- Check if task is in lease
        local exists = redis.call("SISMEMBER", lease_key, task_id)
        if exists == 0 then
            return 0  -- Task not found in lease
        end

        -- Extend lease expiration
        redis.call("EXPIRE", lease_key, duration)

        return 1
    "#;

  /// 回收陈旧聚合集合脚本 - Go: ReclaimStaleAggregationSets
  /// `KEYS[1]` -> all aggregation sets key
  /// `KEYS[2]` -> group key prefix
  /// `ARGV[1]` -> cutoff timestamp
  pub const RECLAIM_STALE_AGGREGATION_SETS: &str = r#"
        local all_sets_key = KEYS[1]
        local group_prefix = KEYS[2]
        local cutoff = tonumber(ARGV[1])

        -- Get all aggregation sets
        local sets = redis.call("SMEMBERS", all_sets_key)
        local reclaimed = 0

        for _, set_id in ipairs(sets) do
            -- Extract timestamp from set_id (assuming format "set_<timestamp>")
            local timestamp = tonumber(string.match(set_id, "set_(%d+)"))
            if timestamp and timestamp < cutoff then
                -- This set is stale, reclaim it
                local set_key = group_prefix .. ":" .. set_id

                -- Move tasks back to group
                local tasks = redis.call("SMEMBERS", set_key)
                for _, task in ipairs(tasks) do
                    redis.call("ZADD", group_prefix, timestamp, task)
                end

                -- Remove the set
                redis.call("DEL", set_key)
                redis.call("SREM", all_sets_key, set_id)
                reclaimed = reclaimed + 1
            end
        end

        return reclaimed
    "#;

  // ==== 从 rdb.go 添加缺失的脚本 ====

  /// 出队任务脚本 - Go: dequeueCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:pending
  /// `KEYS[2]` -> asynq:{`<qname>`}:paused
  /// `KEYS[3]` -> asynq:{`<qname>`}:active
  /// `KEYS[4]` -> asynq:{`<qname>`}:lease
  /// `ARGV[1]` -> lease expiration unix time
  /// `ARGV[2]` -> task key prefix
  pub const DEQUEUE: &str = r#"
        if redis.call("EXISTS", KEYS[2]) == 0 then
            local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
            if id then
                local key = ARGV[2] .. id
                redis.call("HSET", key, "state", "active")
                redis.call("HDEL", key, "pending_since")
                redis.call("ZADD", KEYS[4], ARGV[1], id)
                return redis.call("HGET", key, "msg")
            end
        end
        return nil
    "#;

  /// 完成任务脚本 - Go: doneCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:active
  /// `KEYS[2]` -> asynq:{`<qname>`}:lease
  /// `KEYS[3]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[4]` -> asynq:{`<qname>`}:processed:`<yyyy-mm-dd>`
  /// `KEYS[5]` -> asynq:{`<qname>`}:processed
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> stats expiration timestamp
  /// `ARGV[3]` -> max int64 value
  pub const DONE: &str = r#"
        if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("DEL", KEYS[3]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        local n = redis.call("INCR", KEYS[4])
        if tonumber(n) == 1 then
            redis.call("EXPIREAT", KEYS[4], ARGV[2])
        end
        local total = redis.call("GET", KEYS[5])
        if tonumber(total) == tonumber(ARGV[3]) then
            redis.call("SET", KEYS[5], 1)
        else
            redis.call("INCR", KEYS[5])
        end
        return redis.status_reply("OK")
    "#;

  /// 完成唯一任务脚本 - Go: doneUniqueCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:active
  /// `KEYS[2]` -> asynq:{`<qname>`}:lease
  /// `KEYS[3]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[4]` -> asynq:{`<qname>`}:processed:`<yyyy-mm-dd>`
  /// `KEYS[5]` -> asynq:{`<qname>`}:processed
  /// `KEYS[6]` -> unique key
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> stats expiration timestamp
  /// `ARGV[3]` -> max int64 value
  pub const DONE_UNIQUE: &str = r#"
        if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("DEL", KEYS[3]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        local n = redis.call("INCR", KEYS[4])
        if tonumber(n) == 1 then
            redis.call("EXPIREAT", KEYS[4], ARGV[2])
        end
        local total = redis.call("GET", KEYS[5])
        if tonumber(total) == tonumber(ARGV[3]) then
            redis.call("SET", KEYS[5], 1)
        else
            redis.call("INCR", KEYS[5])
        end
        if redis.call("GET", KEYS[6]) == ARGV[1] then
          redis.call("DEL", KEYS[6])
        end
        return redis.status_reply("OK")
    "#;
  /// 标志完成任务脚本 - Go: markAsCompleteCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:active
  /// `KEYS[2]` -> asynq:{`<qname>`}:lease
  /// `KEYS[3]` -> asynq:{`<qname>`}:completed
  /// `KEYS[4]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[5]` -> asynq:{`<qname>`}:processed:`<yyyy-mm-dd>`
  /// `KEYS[6]` -> asynq:{`<qname>`}:processed
  ///
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> stats expiration timestamp
  /// `ARGV[3]` -> task expiration time in unix time
  /// `ARGV[4]` -> task message data
  /// `ARGV[5]` -> max int64 value
  pub const MARK_AS_COMPLETE: &str = r#"
        if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
          return redis.error_reply("INTERNAL")
        end
        redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
        local n = redis.call("INCR", KEYS[5])
        if tonumber(n) == 1 then
            redis.call("EXPIREAT", KEYS[5], ARGV[2])
        end
        local total = redis.call("GET", KEYS[6])
        if tonumber(total) == tonumber(ARGV[5]) then
            redis.call("SET", KEYS[6], 1)
        else
            redis.call("INCR", KEYS[6])
        end
        return redis.status_reply("OK")
  "#;
  /// 标记唯一任务完成脚本 - Go: markAsCompleteUniqueCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:active
  /// `KEYS[2]` -> asynq:{`<qname>`}:lease
  /// `KEYS[3]` -> asynq:{`<qname>`}:completed
  /// `KEYS[4]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[5]` -> asynq:{`<qname>`}:processed:`<yyyy-mm-dd>`
  /// `KEYS[6]` -> asynq:{`<qname>`}:processed
  /// `KEYS[7]` -> unique key
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> stats expiration timestamp
  /// `ARGV[3]` -> completed at timestamp + retention
  /// `ARGV[4]` -> task message data
  /// `ARGV[5]` -> max int64 value
  pub const MARK_AS_COMPLETE_UNIQUE: &str = r#"
        if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
          return redis.error_reply("INTERNAL")
        end
        redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
        local n = redis.call("INCR", KEYS[5])
        if tonumber(n) == 1 then
            redis.call("EXPIREAT", KEYS[5], ARGV[2])
        end
        local total = redis.call("GET", KEYS[6])
        if tonumber(total) == tonumber(ARGV[5]) then
            redis.call("SET", KEYS[6], 1)
        else
            redis.call("INCR", KEYS[6])
        end
        if redis.call("GET", KEYS[7]) == ARGV[1] then
          redis.call("DEL", KEYS[7])
        end
        return redis.status_reply("OK")
    "#;

  /// 重新入队脚本 - Go: requeueCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:active
  /// `KEYS[2]` -> asynq:{`<qname>`}:lease
  /// `KEYS[3]` -> asynq:{`<qname>`}:pending
  /// `KEYS[4]` -> asynq:{`<qname>`}:t:<task_id>
  /// `ARGV[1]` -> task ID
  pub const REQUEUE: &str = r#"
        if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
          return redis.error_reply("NOT FOUND")
        end
        redis.call("RPUSH", KEYS[3], ARGV[1])
        redis.call("HSET", KEYS[4], "state", "pending")
        return redis.status_reply("OK")
    "#;

  /// 添加到组脚本 - Go: addToGroupCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:g:<group_key>
  /// `KEYS[3]` -> asynq:{`<qname>`}:groups
  /// `ARGV[1]` -> task message data
  /// `ARGV[2]` -> task ID
  /// `ARGV[3]` -> current time in Unix time
  /// `ARGV[4]` -> group key
  pub const ADD_TO_GROUP: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 1 then
            return 0
        end
        redis.call("HSET", KEYS[1],
                   "msg", ARGV[1],
                   "state", "aggregating",
                   "group", ARGV[4])
        redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
        redis.call("SADD", KEYS[3], ARGV[4])
        return 1
    "#;

  /// 添加唯一任务到组脚本 - Go: addToGroupUniqueCmd
  /// `KEYS[1]` -> unique key
  /// `KEYS[2]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[3]` -> asynq:{`<qname>`}:g:<group_key>
  /// `KEYS[4]` -> asynq:{`<qname>`}:groups
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> 唯一锁TTL / Uniqueness lock TTL
  /// `ARGV[3]` -> task message data
  /// `ARGV[4]` -> current time in Unix time
  /// `ARGV[5]` -> group key
  pub const ADD_TO_GROUP_UNIQUE: &str = r#"
        local ok = redis.call("SET", KEYS[4], ARGV[2], "NX", "EX", ARGV[5])
        if not ok then
          return -1
        end
        if redis.call("EXISTS", KEYS[1]) == 1 then
            return 0
        end
        redis.call("HSET", KEYS[1],
                   "msg", ARGV[1],
                   "state", "aggregating",
                   "group", ARGV[4])
        redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
        redis.call("SADD", KEYS[3], ARGV[4])
        return 1
    "#;

  /// 调度唯一任务脚本 - Go: scheduleUniqueCmd
  /// `KEYS[1]` -> unique key
  /// `KEYS[2]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[3]` -> asynq:{`<qname>`}:scheduled
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> 唯一锁TTL / Uniqueness lock TTL
  /// `ARGV[3]` -> task message data
  /// `ARGV[4]` -> process_at time in Unix time
  ///
  /// Output:
  /// Returns 1 if successfully scheduled
  /// Returns 0 if task ID already exists
  /// Returns -1 if task unique key already exists
  pub const SCHEDULE_UNIQUE: &str = r#"
        local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
        if not ok then
          return -1
        end
        if redis.call("EXISTS", KEYS[2]) == 1 then
          return 0
        end
        redis.call("HSET", KEYS[2],
                   "msg", ARGV[4],
                   "state", "scheduled",
                   "unique_key", KEYS[1])
        redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
        return 1
    "#;

  /// 转发脚本 - Go: forwardCmd
  /// `KEYS[1]` -> source queue (e.g. asynq:{`<qname>`:scheduled or asynq:{`<qname>`}:retry})
  /// `KEYS[2]` -> asynq:{`<qname>`}:pending
  /// `ARGV[1]` -> current unix time in seconds
  /// `ARGV[2]` -> task key prefix
  /// `ARGV[3]` -> current unix time in nsec
  /// `ARGV[4]` -> group key prefix
  /// Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
  pub const FORWARD: &str = r#"
        local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
        for _, id in ipairs(ids) do
            local taskKey = ARGV[2] .. id
            local group = redis.call("HGET", taskKey, "group")
            if group and group ~= '' then
                redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
                redis.call("ZREM", KEYS[1], id)
                redis.call("HSET", taskKey,
                           "state", "aggregating")
            else
                redis.call("LPUSH", KEYS[2], id)
                redis.call("ZREM", KEYS[1], id)
                redis.call("HSET", taskKey,
                           "state", "pending",
                           "pending_since", ARGV[3])
            end
        end
        return table.getn(ids)
    "#;
  /// AGGREGATION_CHECK checks the given group for whether to create an aggregation set.
  /// An aggregation set is created if one of the aggregation criteria is met:
  /// 1) group has reached or exceeded its max size
  /// 2) group's oldest task has reached or exceeded its max delay
  /// 3) group's latest task has reached or exceeded its grace period
  ///
  /// if aggregation criteria is met, the command moves those tasks from the group
  /// and put them in an aggregation set. Additionally, if the creation of aggregation set
  /// empties the group, it will clear the group name from the all groups set.
  ///
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:`<group_name>`
  /// `KEYS[2]` -> asynq:{`<qname>`}:g:`<group_name>`:<aggregation_set_id>
  /// `KEYS[3]` -> asynq:{`<qname>`}:aggregation_sets
  /// `KEYS[4]` -> asynq:{`<qname>`}:groups
  /// -------
  /// `ARGV[1]` -> max group size
  /// `ARGV[2]` -> max group delay in unix time
  /// `ARGV[3]` -> start time of the grace period
  /// `ARGV[4]` -> aggregation set expire time
  /// `ARGV[5]` -> current time in unix time
  /// `ARGV[6]` -> group name
  ///
  /// Output:
  /// Returns 0 if no aggregation set was created
  /// Returns 1 if an aggregation set was created
  ///
  /// Time Complexity:
  /// O(log(N) + M) with N being the number tasks in the group zset
  /// and M being the max size.
  pub const AGGREGATION_CHECK: &str = r#"
        local size = redis.call("ZCARD", KEYS[1])
        if size == 0 then
            return 0
        end
        local maxSize = tonumber(ARGV[1])
        if maxSize ~= 0 and size >= maxSize then
            local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
            for i=1, table.getn(res)-1, 2 do
                redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
            end
            redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
            redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
            if size == maxSize then
                redis.call("SREM", KEYS[4], ARGV[6])
            end
            return 1
        end
        local maxDelay = tonumber(ARGV[2])
        local currentTime = tonumber(ARGV[5])
        if maxDelay ~= 0 then
            local oldestEntry = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
            local oldestEntryScore = tonumber(oldestEntry[2])
            local maxDelayTime = currentTime - maxDelay
            if oldestEntryScore <= maxDelayTime then
                local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
                for i=1, table.getn(res)-1, 2 do
                    redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
                end
                redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
                redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
                if size <= maxSize or maxSize == 0 then
                    redis.call("SREM", KEYS[4], ARGV[6])
                end
                return 1
            end
        end
        local latestEntry = redis.call("ZREVRANGE", KEYS[1], 0, 0, "WITHSCORES")
        local latestEntryScore = tonumber(latestEntry[2])
        local gracePeriodStartTime = currentTime - tonumber(ARGV[3])
        if latestEntryScore <= gracePeriodStartTime then
            local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
            for i=1, table.getn(res)-1, 2 do
                redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
            end
            redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
            redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
            if size <= maxSize or maxSize == 0 then
                redis.call("SREM", KEYS[4], ARGV[6])
            end
            return 1
        end
        return 0
  "#;
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:`<group_name>`:<aggregation_set_id>
  /// ------
  /// `ARGV[1]` -> task key prefix
  ///
  /// Output:
  /// Array of encoded task messages
  ///
  /// Time Complexity:
  /// O(N) with N being the number of tasks in the aggregation set.
  pub const READ_AGGREGATION_SET: &str = r#"
        local msgs = {}
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            local key = ARGV[1] .. id
            table.insert(msgs, redis.call("HGET", key, "msg"))
        end
        return msgs
  "#;
  /// `KEYS[1]` -> asynq:{`<qname>`}:aggregation_sets
  /// -------
  /// `ARGV[1]` -> current time in unix time
  pub const RECLAIM_STATE_AGGREGATION_SETS: &str = r#"
        local staleSetKeys = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
        for _, key in ipairs(staleSetKeys) do
            local idx = string.find(key, ":[^:]*$")
            local groupKey = string.sub(key, 1, idx-1)
            local res = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
            for i=1, table.getn(res)-1, 2 do
                redis.call("ZADD", groupKey, tonumber(res[i+1]), res[i])
            end
            redis.call("DEL", key)
        end
        redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
        return redis.status_reply("OK")
  "#;
  /// 删除过期完成任务脚本 - Go: deleteExpiredCompletedTasksCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:completed
  /// `ARGV[1]` -> current time in unix time
  /// `ARGV[2]` -> task key prefix
  /// `ARGV[3]` -> batch size (i.e. maximum number of tasks to delete)
  ///
  /// Returns the number of tasks deleted.
  pub const DELETE_EXPIRED_COMPLETED_TASKS: &str = r#"
        local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))
        for _, id in ipairs(ids) do
            redis.call("DEL", ARGV[2] .. id)
            redis.call("ZREM", KEYS[1], id)
        end
        return table.getn(ids)
    "#;

  /// 列出租约过期脚本 - Go: listLeaseExpiredCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:lease
  /// `ARGV[1]` -> cutoff in unix time
  /// `ARGV[2]` -> task key prefix
  pub const LIST_LEASE_EXPIRED: &str = r#"
        local res = {}
        local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
        for _, id in ipairs(ids) do
            local key = ARGV[2] .. id
            local v = redis.call("HGET", key, "msg")
            if v then
                table.insert(res, v)
            end
        end
        return res
    "#;

  /// 写入服务器状态脚本 - Go: writeServerStateCmd
  /// `KEYS[1]` -> asynq:servers:{<host:pid:sid>}
  /// `KEYS[2]` -> asynq:workers:{<host:pid:sid>}
  /// `ARGV[1]` -> TTL in seconds
  /// `ARGV[2]` -> server info
  /// `ARGV[3]` -> worker info
  pub const WRITE_SERVER_STATE: &str = r#"
        redis.call("SETEX", KEYS[1], ARGV[1], ARGV[2])
        redis.call("DEL", KEYS[2])
        for i = 3, table.getn(ARGV)-1, 2 do
            redis.call("HSET", KEYS[2], ARGV[i], ARGV[i+1])
        end
        redis.call("EXPIRE", KEYS[2], ARGV[1])
        return redis.status_reply("OK")
    "#;

  /// 清除服务器状态脚本 - Go: clearServerStateCmd
  /// `KEYS[1]` -> asynq:servers:{<host:pid:sid>}
  /// `KEYS[2]` -> asynq:workers:{<host:pid:sid>}
  pub const CLEAR_SERVER_STATE: &str = r#"
        redis.call("DEL", KEYS[1])
        redis.call("DEL", KEYS[2])
        return redis.status_reply("OK")
    "#;

  /// 写入调度器条目脚本 - Go: writeSchedulerEntriesCmd
  /// `KEYS[1]` -> asynq:schedulers:{<scheduler_id>}
  /// `ARGV[1]` -> TTL in seconds
  /// ARGV[2..] -> scheduler entries
  pub const WRITE_SCHEDULER_ENTRIES: &str = r#"
        redis.call("DEL", KEYS[1])
        for i=2, table.getn(ARGV) do
            redis.call("LPUSH", KEYS[1], ARGV[i])
        end
        redis.call("EXPIRE", KEYS[1], ARGV[1])
        return redis.status_reply("OK")
    "#;

  /// 记录调度器入队事件脚本 - Go: recordSchedulerEnqueueEventCmd
  /// `KEYS[1]` -> asynq:scheduler_history:`<entryID>`
  /// `ARGV[1]` -> enqueued_at timestamp
  /// `ARGV[2]` -> serialized SchedulerEnqueueEvent data
  /// `ARGV[3]` -> max number of events to be persisted
  pub const RECORD_SCHEDULER_ENQUEUE_EVENT: &str = r#"
        redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -ARGV[3])
        redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
        return redis.status_reply("OK")
    "#;

  // ==== 从 inspect.go 添加缺失的脚本 ====

  /// 内存使用脚本 - Go: memoryUsageCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:pending
  /// `KEYS[2]` -> asynq:{`<qname>`}:active
  /// `KEYS[3]` -> asynq:{`<qname>`}:scheduled
  /// `KEYS[4]` -> asynq:{`<qname>`}:retry
  /// `KEYS[5]` -> asynq:{`<qname>`}:archived
  /// `KEYS[6]` -> asynq:{`<qname>`}:completed
  /// `ARGV[1]` -> task key prefix
  /// `ARGV[2]` -> sample size
  pub const MEMORY_USAGE: &str = r#"
        local sample_size = tonumber(ARGV[2])
        if sample_size <= 0 then
            return redis.error_reply("sample size must be a positive number")
        end
        local memusg = 0
        for i=1,2 do
            local ids = redis.call("LRANGE", KEYS[i], 0, sample_size - 1)
            local sample_total = 0
            if (table.getn(ids) > 0) then
                for _, id in ipairs(ids) do
                    local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
                    sample_total = sample_total + bytes
                end
                local n = redis.call("LLEN", KEYS[i])
                local avg = sample_total / table.getn(ids)
                memusg = memusg + (avg * n)
            end
            local m = redis.call("MEMORY", "USAGE", KEYS[i])
            if (m) then
                memusg = memusg + m
            end
        end
        for i=3,6 do
            local ids = redis.call("ZRANGE", KEYS[i], 0, sample_size - 1)
            local sample_total = 0
            if (table.getn(ids) > 0) then
                for _, id in ipairs(ids) do
                    local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
                    sample_total = sample_total + bytes
                end
                local n = redis.call("ZCARD", KEYS[i])
                local avg = sample_total / table.getn(ids)
                memusg = memusg + (avg * n)
            end
            local m = redis.call("MEMORY", "USAGE", KEYS[i])
            if (m) then
                memusg = memusg + m
            end
        end
        return memusg
    "#;

  /// 历史统计脚本 - Go: historicalStatsCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:processed:`<date>`
  /// `KEYS[2]` -> asynq:{`<qname>`}:failed:`<date>`
  pub const HISTORICAL_STATS: &str = r#"
        local res = {}
        for _, key in ipairs(KEYS) do
            local n = redis.call("GET", key)
            if not n then
                n = 0
            end
            table.insert(res, tonumber(n))
        end
        return res
    "#;

  /// 获取任务信息脚本 - Go: getTaskInfoCmd
  /// Input:
  /// `KEYS[1]` -> task key (asynq:{`<qname>`}:t:`<task_id>`)
  /// `ARGV[1]` -> task id
  /// `ARGV[2]` -> current time in Unix time (seconds)
  /// `ARGV[3]` -> queue key prefix (asynq:{`<qname>`}:)
  ///
  /// Output:
  /// Tuple of {msg, state, nextProcessAt, result}
  /// msg: encoded task message
  /// state: string describing the state of the task
  /// nextProcessAt: unix time in seconds, zero if not applicable.
  /// result: result data associated with the task
  ///
  /// If the task key doesn't exist, it returns error with a message "NOT FOUND"
  pub const GET_TASK_INFO: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 0 then
            return redis.error_reply("NOT FOUND")
        end
        local msg, state, result = unpack(redis.call("HMGET", KEYS[1], "msg", "state", "result"))
        if state == "scheduled" or state == "retry" then
            return {msg, state, redis.call("ZSCORE", ARGV[3] .. state, ARGV[1]), result}
        end
        if state == "pending" then
            return {msg, state, ARGV[2], result}
        end
        return {msg, state, 0, result}
    "#;

  /// 组统计脚本 - Go: groupStatsCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:<group_key>
  /// `ARGV[1]` -> group key
  pub const GROUP_STATS: &str = r#"
        local res = {}
        local group_names = redis.call("SMEMBERS", KEYS[1])
        for _, gname in ipairs(group_names) do
            local size = redis.call("ZCARD", ARGV[1] .. gname)
            table.insert(res, gname)
            table.insert(res, size)
        end
        return res
    "#;

  /// 列出消息脚本 - Go: listMessagesCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:pending or active
  /// `ARGV[1]` -> task key prefix
  /// `ARGV[2]` -> page size
  /// `ARGV[3]` -> page number (0-based)
  pub const LIST_MESSAGES: &str = r#"
        local ids = redis.call("LRange", KEYS[1], ARGV[1], ARGV[2])
        local data = {}
        for _, id in ipairs(ids) do
            local key = ARGV[3] .. id
            local msg, result = unpack(redis.call("HMGET", key, "msg","result"))
            table.insert(data, msg)
            table.insert(data, result)
        end
        return data
    "#;

  /// 列出有序集合条目脚本 - Go: listZSetEntriesCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:scheduled, retry, archived, or completed
  /// `ARGV[1]` -> task key prefix
  /// `ARGV[2]` -> page size
  /// `ARGV[3]` -> page number (0-based)
  pub const LIST_ZSET_ENTRIES: &str = r#"
        local data = {}
        local id_score_pairs = redis.call("ZRANGE", KEYS[1], ARGV[1], ARGV[2], "WITHSCORES")
        for i = 1, table.getn(id_score_pairs), 2 do
            local id = id_score_pairs[i]
            local score = id_score_pairs[i+1]
            local key = ARGV[3] .. id
            local msg, res = unpack(redis.call("HMGET", key, "msg", "result"))
            table.insert(data, msg)
            table.insert(data, score)
            table.insert(data, res)
        end
        return data
    "#;

  /// 运行所有聚合任务脚本 - Go: runAllAggregatingCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:<group_key>
  /// `KEYS[2]` -> asynq:{`<qname>`}:pending
  /// `KEYS[3]` -> asynq:{`<qname>`}:groups
  /// `ARGV[1]` -> task key prefix
  /// `ARGV[2]` -> group key
  pub const RUN_ALL_AGGREGATING: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("LPUSH", KEYS[2], id)
            redis.call("HSET", ARGV[1] .. id, "state", "pending")
        end
        redis.call("DEL", KEYS[1])
        redis.call("SREM", KEYS[3], ARGV[2])
        return table.getn(ids)
    "#;

  /// 运行任务脚本 - Go: runTaskCmd
  /// runTaskCmd is a Lua script that updates the given task to pending state.
  ///
  /// Input:
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:pending
  /// `KEYS[3]` -> asynq:{`<qname>`}:groups
  /// --
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> queue key prefix; asynq:{`<qname>`}:
  /// `ARGV[3]` -> group key prefix
  ///
  /// Output:
  /// Numeric code indicating the status:
  /// Returns 1 if task is successfully updated.
  /// Returns 0 if task is not found.
  /// Returns -1 if task is in active state.
  /// Returns -2 if task is in pending state.
  /// Returns error reply if unexpected error occurs.
  pub const RUN_TASK: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 0 then
            return 0
        end
        local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
        if state == "active" then
            return -1
        elseif state == "pending" then
            return -2
        elseif state == "aggregating" then
            local n = redis.call("ZREM", ARGV[3] .. group, ARGV[1])
            if n == 0 then
                return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[3] .. group))
            end
            if redis.call("ZCARD", ARGV[3] .. group) == 0 then
                redis.call("SREM", KEYS[3], group)
            end
        else
            local n = redis.call("ZREM", ARGV[2] .. state, ARGV[1])
            if n == 0 then
                return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[2] .. state))
            end
        end
        redis.call("LPUSH", KEYS[2], ARGV[1])
        redis.call("HSET", KEYS[1], "state", "pending")
        return 1
    "#;

  /// 运行所有任务脚本 - Go: runAllCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:scheduled, retry, or archived
  /// `KEYS[2]` -> asynq:{`<qname>`}:pending
  /// `ARGV[1]` -> task key prefix
  pub const RUN_ALL: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("LPUSH", KEYS[2], id)
            redis.call("HSET", ARGV[1] .. id, "state", "pending")
        end
        redis.call("DEL", KEYS[1])
        return table.getn(ids)
    "#;

  /// 归档所有聚合任务脚本 - Go: archiveAllAggregatingCmd
  /// archiveAllAggregatingCmd archives all tasks in the given group.
  ///
  /// Input:
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:`<group_name>`
  /// `KEYS[2]` -> asynq:{`<qname>`}:archived
  /// `KEYS[3]` -> asynq:{`<qname>`}:groups
  /// -------
  /// `ARGV[1]` -> current timestamp
  /// `ARGV[2]` -> cutoff timestamp (e.g., 90 days ago)
  /// `ARGV[3]` -> max number of tasks in archive (e.g., 100)
  /// `ARGV[4]` -> task key prefix (asynq:{`<qname>`}:t:)
  /// `ARGV[5]` -> group name
  ///
  /// Output:
  /// integer: Number of tasks archived
  pub const ARCHIVE_ALL_AGGREGATING: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("ZADD", KEYS[2], ARGV[1], id)
            redis.call("HSET", ARGV[4] .. id, "state", "archived")
        end
        redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
        redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
        redis.call("DEL", KEYS[1])
        redis.call("SREM", KEYS[3], ARGV[5])
        return table.getn(ids)
    "#;

  /// 归档所有等待任务脚本 - Go: archiveAllPendingCmd
  /// archiveAllPendingCmd is a Lua script that moves all pending tasks from
  /// the given queue to archived state.
  ///
  /// Input:
  /// `KEYS[1]` -> asynq:{`<qname>`}:pending
  /// `KEYS[2]` -> asynq:{`<qname>`}:archived
  /// --
  /// `ARGV[1]` -> current timestamp
  /// `ARGV[2]` -> cutoff timestamp (e.g., 90 days ago)
  /// `ARGV[3]` -> max number of tasks in archive (e.g., 100)
  /// `ARGV[4]` -> task key prefix (asynq:{`<qname>`}:t:)
  ///
  /// Output:
  /// integer: Number of tasks archived
  pub const ARCHIVE_ALL_PENDING: &str = r#"
        local ids = redis.call("LRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("ZADD", KEYS[2], ARGV[1], id)
            redis.call("HSET", ARGV[4] .. id, "state", "archived")
        end
        redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
        redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
        redis.call("DEL", KEYS[1])
        return table.getn(ids)
    "#;

  /// 归档任务脚本 - Go: archiveTaskCmd
  /// archiveTaskCmd is a Lua script that archives a task given a task id.
  ///
  /// Input:
  /// `KEYS[1]` -> task key (asynq:{`<qname>`}:t:<task_id>)
  /// `KEYS[2]` -> archived key (asynq:{`<qname>`}:archived)
  /// `KEYS[3]` -> all groups key (asynq:{`<qname>`}:groups)
  /// --
  /// `ARGV[1]` -> id of the task to archive
  /// `ARGV[2]` -> current timestamp
  /// `ARGV[3]` -> cutoff timestamp (e.g., 90 days ago)
  /// `ARGV[4]` -> max number of tasks in archived state (e.g., 100)
  /// `ARGV[5]` -> queue key prefix (asynq:{`<qname>`}:)
  /// `ARGV[6]` -> group key prefix (asynq:{`<qname>`}:g:)
  ///
  /// Output:
  /// Numeric code indicating the status:
  /// Returns 1 if task is successfully archived.
  /// Returns 0 if task is not found.
  /// Returns -1 if task is already archived.
  /// Returns -2 if task is in active state.
  /// Returns error reply if unexpected error occurs.
  pub const ARCHIVE_TASK: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 0 then
            return 0
        end
        local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
        if state == "active" then
            return -2
        end
        if state == "archived" then
            return -1
        end
        if state == "pending" then
            if redis.call("LREM", ARGV[5] .. state, 1, ARGV[1]) == 0 then
                return redis.error_reply("task id not found in list " .. tostring(ARGV[5] .. state))
            end
        elseif state == "aggregating" then
            if redis.call("ZREM", ARGV[6] .. group, ARGV[1]) == 0 then
                return redis.error_reply("task id not found in zset " .. tostring(ARGV[6] .. group))
            end
            if redis.call("ZCARD", ARGV[6] .. group) == 0 then
                redis.call("SREM", KEYS[3], group)
            end
        else
            if redis.call("ZREM", ARGV[5] .. state, ARGV[1]) == 0 then
                return redis.error_reply("task id not found in zset " .. tostring(ARGV[5] .. state))
            end
        end
        redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
        redis.call("HSET", KEYS[1], "state", "archived")
        redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[3])
        redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[4])
        return 1
    "#;

  /// 归档所有任务脚本 - Go: archiveAllCmd
  /// archiveAllCmd is a Lua script that archives all tasks in either scheduled
  /// or retry state from the given queue.
  ///
  /// Input:
  /// `KEYS[1]` -> ZSET to move task from (e.g., asynq:{`<qname>`}:retry)
  /// `KEYS[2]` -> asynq:{`<qname>`}:archived
  /// --
  /// `ARGV[1]` -> current timestamp
  /// `ARGV[2]` -> cutoff timestamp (e.g., 90 days ago)
  /// `ARGV[3]` -> max number of tasks in archive (e.g., 100)
  /// `ARGV[4]` -> task key prefix (asynq:{`<qname>`}:t:)
  ///
  /// Output:
  /// integer: number of tasks archived
  pub const ARCHIVE_ALL: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("ZADD", KEYS[2], ARGV[1], id)
            redis.call("HSET", ARGV[4] .. id, "state", "archived")
        end
        redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
        redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
        redis.call("DEL", KEYS[1])
        return table.getn(ids)
    "#;

  /// 删除任务脚本 - Go: deleteTaskCmd
  /// Input:
  /// `KEYS[1]` -> asynq:{`<qname>`}:t:<task_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:groups
  /// --
  /// `ARGV[1]` -> task ID
  /// `ARGV[2]` -> queue key prefix
  /// `ARGV[3]` -> group key prefix
  ///
  /// Output:
  /// Numeric code indicating the status:
  /// Returns 1 if task is successfully deleted.
  /// Returns 0 if task is not found.
  /// Returns -1 if task is in active state.
  pub const DELETE_TASK: &str = r#"
        if redis.call("EXISTS", KEYS[1]) == 0 then
            return 0
        end
        local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
        if state == "active" then
            return -1
        end
        if state == "pending" then
            if redis.call("LREM", ARGV[2] .. state, 0, ARGV[1]) == 0 then
                return redis.error_reply("task is not found in list: " .. tostring(ARGV[2] .. state))
            end
        elseif state == "aggregating" then
            if redis.call("ZREM", ARGV[3] .. group, ARGV[1]) == 0 then
                return redis.error_reply("task is not found in zset: " .. tostring(ARGV[3] .. group))
            end
            if redis.call("ZCARD", ARGV[3] .. group) == 0 then
                redis.call("SREM", KEYS[2], group)
            end
        else
            if redis.call("ZREM", ARGV[2] .. state, ARGV[1]) == 0 then
                return redis.error_reply("task is not found in zset: " .. tostring(ARGV[2] .. state))
            end
        end
        local unique_key = redis.call("HGET", KEYS[1], "unique_key")
        if unique_key and unique_key ~= "" and redis.call("GET", unique_key) == ARGV[1] then
            redis.call("DEL", unique_key)
        end
        return redis.call("DEL", KEYS[1])
    "#;

  /// 删除所有任务脚本 - Go: deleteAllCmd
  /// deleteAllCmd deletes tasks from the given zset.
  ///
  /// Input:
  /// `KEYS[1]` -> zset holding the task ids.
  /// --
  /// `ARGV[1]` -> task key prefix
  ///
  /// Output:
  /// integer: number of tasks deleted
  pub const DELETE_ALL: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            local task_key = ARGV[1] .. id
            local unique_key = redis.call("HGET", task_key, "unique_key")
            if unique_key and unique_key ~= "" and redis.call("GET", unique_key) == id then
                redis.call("DEL", unique_key)
            end
            redis.call("DEL", task_key)
        end
        redis.call("DEL", KEYS[1])
        return table.getn(ids)
    "#;

  /// 删除所有聚合任务脚本 - Go: deleteAllAggregatingCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:<group_key>
  /// `KEYS[2]` -> asynq:{`<qname>`}:groups
  /// `ARGV[1]` -> task key prefix
  /// `ARGV[2]` -> group key
  pub const DELETE_ALL_AGGREGATING: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("DEL", ARGV[1] .. id)
        end
        redis.call("SREM", KEYS[2], ARGV[2])
        redis.call("DEL", KEYS[1])
        return table.getn(ids)
    "#;

  /// 删除所有等待任务脚本 - Go: deleteAllPendingCmd
  /// `KEYS[1]` -> asynq:{`<qname>`}:pending
  /// `ARGV[1]` -> task key prefix
  pub const DELETE_ALL_PENDING: &str = r#"
        local ids = redis.call("LRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids) do
            redis.call("DEL", ARGV[1] .. id)
        end
        redis.call("DEL", KEYS[1])
        return table.getn(ids)
    "#;

  /// 强制删除队列脚本 - Go: removeQueueForceCmd
  /// removeQueueForceCmd removes the given queue regardless of
  /// whether the queue is empty.
  /// It only check whether active queue is empty before removing.
  ///
  /// Input:
  /// `KEYS[1]` -> asynq:{`<qname>`}
  /// `KEYS[2]` -> asynq:{`<qname>`}:active
  /// `KEYS[3]` -> asynq:{`<qname>`}:scheduled
  /// `KEYS[4]` -> asynq:{`<qname>`}:retry
  /// `KEYS[5]` -> asynq:{`<qname>`}:archived
  /// `KEYS[6]` -> asynq:{`<qname>`}:lease
  /// --
  /// `ARGV[1]` -> task key prefix
  ///
  /// Output:
  /// Numeric code to indicate the status.
  /// Returns 1 if successfully removed.
  /// Returns -2 if the queue has active tasks.
  pub const REMOVE_QUEUE_FORCE: &str = r#"
        local active = redis.call("LLEN", KEYS[2])
        if active > 0 then
            return -2
        end
        for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
            redis.call("DEL", ARGV[1] .. id)
        end
        redis.call("DEL", KEYS[1])
        redis.call("DEL", KEYS[2])
        redis.call("DEL", KEYS[3])
        redis.call("DEL", KEYS[4])
        redis.call("DEL", KEYS[5])
        redis.call("DEL", KEYS[6])
        return 1
    "#;

  /// 删除队列脚本 - Go: removeQueueCmd
  /// removeQueueCmd removes the given queue.
  /// It checks whether queue is empty before removing.
  ///
  /// Input:
  /// `KEYS[1]` -> asynq:{`<qname>`}:pending
  /// `KEYS[2]` -> asynq:{`<qname>`}:active
  /// `KEYS[3]` -> asynq:{`<qname>`}:scheduled
  /// `KEYS[4]` -> asynq:{`<qname>`}:retry
  /// `KEYS[5]` -> asynq:{`<qname>`}:archived
  /// `KEYS[6]` -> asynq:{`<qname>`}:lease
  /// --
  /// `ARGV[1]` -> task key prefix
  ///
  /// Output:
  /// Numeric code to indicate the status
  /// Returns 1 if successfully removed.
  /// Returns -1 if queue is not empty
  pub const REMOVE_QUEUE: &str = r#"
        local ids = {}
        for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
            table.insert(ids, id)
        end
        for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
            table.insert(ids, id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
            table.insert(ids, id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
            table.insert(ids, id)
        end
        for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
            table.insert(ids, id)
        end
        if table.getn(ids) > 0 then
            return -1
        end
        for _, id in ipairs(ids) do
            redis.call("DEL", ARGV[1] .. id)
        end
        for _, id in ipairs(ids) do
            redis.call("DEL", ARGV[1] .. id)
        end
        redis.call("DEL", KEYS[1])
        redis.call("DEL", KEYS[2])
        redis.call("DEL", KEYS[3])
        redis.call("DEL", KEYS[4])
        redis.call("DEL", KEYS[5])
        redis.call("DEL", KEYS[6])
        return 1
    "#;

  /// 列出服务器键脚本 - Go: listServerKeysCmd
  /// `KEYS[1]` -> asynq:servers
  pub const LIST_SERVER_KEYS: &str = r#"
        local now = tonumber(ARGV[1])
        local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
        redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
        return keys
    "#;

  /// 列出工作者脚本 - Go: listWorkersCmd
  /// `KEYS[1]` -> asynq:workers:{<host:pid:sid>}
  pub const LIST_WORKERS: &str = r#"
        local now = tonumber(ARGV[1])
        local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
        redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
        return keys
    "#;

  /// 列出调度器键脚本 - Go: listSchedulerKeysCmd
  /// `KEYS[1]` -> asynq:schedulers
  pub const LIST_SCHEDULER_KEYS: &str = r#"
        local now = tonumber(ARGV[1])
        local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
        redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
        return keys
    "#;
  /// `KEYS[1]` -> asynq:{`<qname>`}:g:`<group_name>`:<aggregation_set_id>
  /// `KEYS[2]` -> asynq:{`<qname>`}:aggregation_sets
  /// -------
  /// `ARGV[1]` -> task key prefix
  ///
  /// Output:
  /// Redis status reply
  ///
  /// Time Complexity:
  /// max(O(N), O(log(M))) with N being the number of tasks in the aggregation set
  /// and M being the number of elements in the all-aggregation-sets list.
  pub const DELETE_AGGREGATION_SET: &str = r#"
        local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
        for _, id in ipairs(ids)  do
            redis.call("DEL", ARGV[1] .. id)
        end
        redis.call("DEL", KEYS[1])
        redis.call("ZREM", KEYS[2], KEYS[1])
        return redis.status_reply("OK")
  "#;
  /// Go currentStatsCmd
  /// `KEYS[1]` ->  asynq:`<qname>`:pending
  /// `KEYS[2]` ->  asynq:`<qname>`:active
  /// `KEYS[3]` ->  asynq:`<qname>`:scheduled
  /// `KEYS[4]` ->  asynq:`<qname>`:retry
  /// `KEYS[5]` ->  asynq:`<qname>`:archived
  /// `KEYS[6]` ->  asynq:`<qname>`:completed
  /// `KEYS[7]` ->  asynq:`<qname>`:processed:`<yyyy-mm-dd>`
  /// `KEYS[8]` ->  asynq:`<qname>`:failed:`<yyyy-mm-dd>`
  /// `KEYS[9]` ->  asynq:`<qname>`:processed
  /// `KEYS[10]` -> asynq:`<qname>`:failed
  /// `KEYS[11]` -> asynq:`<qname>`:paused
  /// `KEYS[12]` -> asynq:`<qname>`:groups
  /// --------
  /// `ARGV[1]` -> task key prefix
  /// `ARGV[2]` -> group key prefix
  pub const CURRENT_STATS: &str = r#"
        local res = {}
        local pendingTaskCount = redis.call("LLEN", KEYS[1])
        table.insert(res, KEYS[1])
        table.insert(res, pendingTaskCount)
        table.insert(res, KEYS[2])
        table.insert(res, redis.call("LLEN", KEYS[2]))
        table.insert(res, KEYS[3])
        table.insert(res, redis.call("ZCARD", KEYS[3]))
        table.insert(res, KEYS[4])
        table.insert(res, redis.call("ZCARD", KEYS[4]))
        table.insert(res, KEYS[5])
        table.insert(res, redis.call("ZCARD", KEYS[5]))
        table.insert(res, KEYS[6])
        table.insert(res, redis.call("ZCARD", KEYS[6]))
        for i=7,10 do
            local count = 0
            local n = redis.call("GET", KEYS[i])
            if n then
                count = tonumber(n)
            end
            table.insert(res, KEYS[i])
            table.insert(res, count)
        end
        table.insert(res, KEYS[11])
        table.insert(res, redis.call("EXISTS", KEYS[11]))
        table.insert(res, "oldest_pending_since")
        if pendingTaskCount > 0 then
            local id = redis.call("LRANGE", KEYS[1], -1, -1)[1]
            table.insert(res, redis.call("HGET", ARGV[1] .. id, "pending_since"))
        else
            table.insert(res, 0)
        end
        local group_names = redis.call("SMEMBERS", KEYS[12])
        table.insert(res, "group_size")
        table.insert(res, table.getn(group_names))
        local aggregating_count = 0
        for _, gname in ipairs(group_names) do
            aggregating_count = aggregating_count + redis.call("ZCARD", ARGV[2] .. gname)
        end
        table.insert(res, "aggregating_count")
        table.insert(res, aggregating_count)
        return res
"#;
}

/// Redis 脚本管理器
#[derive(Debug, Default)]
pub struct ScriptManager {
  /// 脚本SHA缓存
  script_sha1: std::collections::HashMap<&'static str, String>,
}

impl ScriptManager {
  /// 预加载所有脚本
  pub async fn load_scripts(&mut self, conn: &mut RedisConnection) -> Result<()> {
    // use redis::AsyncCommands;

    for (name, script) in ALL_SCRIPT.entries() {
      let sha = self.load_script(conn, script).await?;
      self.script_sha1.insert(name, sha);
    }

    Ok(())
  }

  pub async fn load_script(&self, conn: &mut RedisConnection, script: &str) -> Result<String> {
    // use redis::AsyncCommands;
    let sha: String = redis::cmd("SCRIPT")
      .arg("LOAD")
      .arg(script)
      .query_async(conn)
      .await?;
    Ok(sha)
  }
  /// 获取脚本SHA
  pub fn get_script_sha(&self, name: &str) -> Option<&String> {
    self.script_sha1.get(name)
  }

  /// 执行脚本（支持二进制参数）
  pub async fn eval_script_with_binary_args<T>(
    &self,
    conn: &mut RedisConnection,
    script_name: &str,
    keys: &[String],
    string_args: &[String],
    binary_args: &[Vec<u8>],
  ) -> Result<T>
  where
    T: redis::FromRedisValue,
  {
    // 首先尝试使用 EVALSHA 如果脚本已加载
    if let Some(sha) = self.get_script_sha(script_name) {
      let mut cmd = redis::cmd("EVALSHA");
      cmd.arg(sha).arg(keys.len()).arg(keys);

      // 添加字符串参数
      for arg in string_args {
        cmd.arg(arg);
      }

      // 添加二进制参数
      for arg in binary_args {
        cmd.arg(arg);
      }

      match cmd.query_async::<T>(conn).await {
        Ok(result) => return Ok(result),
        Err(e) if e.to_string().contains("NOSCRIPT") => {
          // 脚本被清理了，继续使用 EVAL
        }
        Err(e) => return Err(e.into()),
      }
    }

    // 如果脚本未加载或 EVALSHA 失败，使用 EVAL
    let script = match script_name {
      "write_server_state" => scripts::WRITE_SERVER_STATE,
      "clear_server_state" => scripts::CLEAR_SERVER_STATE,
      _ => return Err(Error::other(format!("Script not loaded: {script_name}"))),
    };

    let mut cmd = redis::cmd("EVAL");
    cmd.arg(script).arg(keys.len()).arg(keys);

    // 添加字符串参数
    for arg in string_args {
      cmd.arg(arg);
    }

    // 添加二进制参数
    for arg in binary_args {
      cmd.arg(arg);
    }

    let result: T = cmd.query_async(conn).await?;
    Ok(result)
  }

  /// 执行脚本
  pub async fn eval_script<T>(
    &self,
    conn: &mut RedisConnection,
    script_name: &str,
    keys: &[String],
    args: &[RedisArg],
  ) -> Result<T>
  where
    T: redis::FromRedisValue,
  {
    if let Some(sha) = self.get_script_sha(script_name) {
      // 尝试使用 EVALSHA
      match redis::cmd("EVALSHA")
        .arg(sha)
        .arg(keys.len())
        .arg(keys)
        .arg(args)
        .query_async::<T>(conn)
        .await
      {
        Ok(result) => Ok(result),
        Err(e) => {
          if e.to_string().contains("NoScript") || e.to_string().contains("NOT: FOUND") {
            // 如果脚本不存在，重新加载并执行
            if let Some(script) = ALL_SCRIPT.get(script_name) {
              self.load_script(conn, script).await?;
              let result = redis::cmd("EVAL")
                .arg(script)
                .arg(keys.len())
                .arg(keys)
                .arg(args)
                .query_async::<T>(conn)
                .await?;
              Ok(result)
            } else {
              Err(Error::other(format!(
                "Script not found in KEYWORDS: {script_name}"
              )))
            }
          } else {
            Err(Error::other(format!("Script execution failed: {e}")))
          }
        }
      }
    } else {
      Err(Error::other(format!("Script not loaded: {script_name}")))
    }
  }
}

static ALL_SCRIPT: phf::Map<&'static str, &'static str> = phf_map! {
    "enqueue" => scripts::ENQUEUE,
    "enqueue_unique" => scripts::ENQUEUE_UNIQUE,
    "schedule" => scripts::SCHEDULE,
    "dequeue" => scripts::DEQUEUE,
    "done" => scripts::DONE,
    "retry" => scripts::RETRY,
    "archive" => scripts::ARCHIVE,
    "forward_scheduled" => scripts::FORWARD_SCHEDULED,
    "forward_retry" => scripts::FORWARD_RETRY,
    "cleanup_completed" => scripts::CLEANUP_COMPLETED,
    "rate_limit" => scripts::RATE_LIMIT,
    "recover_orphaned" => scripts::RECOVER_ORPHANED,
    "mark_as_complete" => scripts::MARK_AS_COMPLETE,
    "extend_lease" => scripts::EXTEND_LEASE,
    "aggregation_check" => scripts::AGGREGATION_CHECK,
    "read_aggregation_set" => scripts::READ_AGGREGATION_SET,
    "reclaim_stale_aggregation_sets" => scripts::RECLAIM_STALE_AGGREGATION_SETS,
    "done_unique" => scripts::DONE_UNIQUE,
    "mark_as_complete_unique" => scripts::MARK_AS_COMPLETE_UNIQUE,
    "requeue" => scripts::REQUEUE,
    "add_to_group" => scripts::ADD_TO_GROUP,
    "add_to_group_unique" => scripts::ADD_TO_GROUP_UNIQUE,
    "schedule_unique" => scripts::SCHEDULE_UNIQUE,
    "forward" => scripts::FORWARD,
    "delete_expired_completed_tasks" => scripts::DELETE_EXPIRED_COMPLETED_TASKS,
    "list_lease_expired" => scripts::LIST_LEASE_EXPIRED,
    "write_server_state" => scripts::WRITE_SERVER_STATE,
    "clear_server_state" => scripts::CLEAR_SERVER_STATE,
    "write_scheduler_entries" => scripts::WRITE_SCHEDULER_ENTRIES,
    "record_scheduler_enqueue_event" => scripts::RECORD_SCHEDULER_ENQUEUE_EVENT,
    "memory_usage" => scripts::MEMORY_USAGE,
    "historical_stats" => scripts::HISTORICAL_STATS,
    "get_task_info" => scripts::GET_TASK_INFO,
    "group_stats" => scripts::GROUP_STATS,
    "list_messages" => scripts::LIST_MESSAGES,
    "list_zset_entries" => scripts::LIST_ZSET_ENTRIES,
    "run_all_aggregating" => scripts::RUN_ALL_AGGREGATING,
    "run_task" => scripts::RUN_TASK,
    "run_all" => scripts::RUN_ALL,
    "archive_all_aggregating" => scripts::ARCHIVE_ALL_AGGREGATING,
    "archive_all_pending" => scripts::ARCHIVE_ALL_PENDING,
    "archive_task" => scripts::ARCHIVE_TASK,
    "archive_all" => scripts::ARCHIVE_ALL,
    "delete_task" => scripts::DELETE_TASK,
    "delete_all" => scripts::DELETE_ALL,
    "delete_all_aggregating" => scripts::DELETE_ALL_AGGREGATING,
    "delete_all_pending" => scripts::DELETE_ALL_PENDING,
    "remove_queue_force" => scripts::REMOVE_QUEUE_FORCE,
    "remove_queue" => scripts::REMOVE_QUEUE,
    "list_server_keys" => scripts::LIST_SERVER_KEYS,
    "list_workers" => scripts::LIST_WORKERS,
    "list_scheduler_keys" => scripts::LIST_SCHEDULER_KEYS,
    "reclaim_state_aggregation_sets" => scripts::RECLAIM_STATE_AGGREGATION_SETS,
    "delete_aggregation_set" => scripts::DELETE_AGGREGATION_SET,
    "current_stats" => scripts::CURRENT_STATS,
};
