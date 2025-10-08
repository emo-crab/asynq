use chrono::{DateTime, Utc};
use std::fmt;
use std::time::Duration;
use crate::base::constants::{DEFAULT_MAX_RETRY, DEFAULT_QUEUE_NAME};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionType {
  MaxRetryOpt(u32),         // 最大重试次数
  QueueOpt(String),         // 队列名称
  TimeoutOpt(u64),          // 超时时间
  DeadlineOpt(u64),         // 截止时间
  UniqueOpt(String),        // 唯一标识
  ProcessAtOpt(u64),        // 处理时间
  ProcessInOpt(u64),        // 处理间隔
  TaskIDOpt(String),        // 任务ID
  RetentionOpt(u64),        // 保留时间
  GroupOpt(String),         // 组名
  RateLimitOpt(u32),        // 速率限制
  GroupGracePeriodOpt(u64), // 组聚合宽限期
}

impl fmt::Display for OptionType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      OptionType::MaxRetryOpt(v) => write!(f, "MaxRetry({})", v),
      OptionType::QueueOpt(v) => write!(f, "Queue(\"{}\")", v),
      OptionType::TimeoutOpt(v) => write!(f, "Timeout({})", v),
      OptionType::DeadlineOpt(v) => write!(f, "Deadline({})", v),
      OptionType::UniqueOpt(v) => write!(f, "Unique({})", v),
      OptionType::ProcessAtOpt(v) => write!(f, "ProcessAt({})", v),
      OptionType::ProcessInOpt(v) => write!(f, "ProcessIn({})", v),
      OptionType::TaskIDOpt(v) => write!(f, "TaskID(\"{}\")", v),
      OptionType::RetentionOpt(v) => write!(f, "Retention({})", v),
      OptionType::GroupOpt(v) => write!(f, "Group(\"{}\")", v),
      OptionType::RateLimitOpt(v) => write!(f, "RateLimit({})", v),
      OptionType::GroupGracePeriodOpt(v) => write!(f, "GroupGracePeriod({})", v),
    }
  }
}

impl From<&TaskOptions> for Vec<OptionType> {
  fn from(opts: &TaskOptions) -> Self {
    let mut v = Vec::new();
    v.push(OptionType::MaxRetryOpt(opts.max_retry as u32));
    v.push(OptionType::QueueOpt(opts.queue.clone()));
    if let Some(ref id) = opts.task_id {
      v.push(OptionType::TaskIDOpt(id.clone()));
    }
    if let Some(timeout) = opts.timeout {
      v.push(OptionType::TimeoutOpt(timeout.as_secs()));
    }
    if let Some(deadline) = opts.deadline {
      v.push(OptionType::DeadlineOpt(deadline.timestamp() as u64));
    }
    if let Some(ref ttl) = opts.unique_ttl {
      v.push(OptionType::UniqueOpt(ttl.as_secs().to_string()));
    }
    if let Some(process_at) = opts.process_at {
      v.push(OptionType::ProcessAtOpt(process_at.timestamp() as u64));
    }
    if let Some(process_in) = opts.process_in {
      v.push(OptionType::ProcessInOpt(process_in.as_secs()));
    }
    if let Some(retention) = opts.retention {
      v.push(OptionType::RetentionOpt(retention.as_secs()));
    }
    if let Some(ref group) = opts.group {
      v.push(OptionType::GroupOpt(group.clone()));
    }
    if let Some(ref rate_limit) = opts.rate_limit {
      v.push(OptionType::RateLimitOpt(rate_limit.limit));
    }
    if let Some(grace) = opts.group_grace_period {
      v.push(OptionType::GroupGracePeriodOpt(grace.as_secs()));
    }
    v
  }
}

impl OptionType {
  /// 从字符串解析选项类型，兼容 Go asynq 格式
  /// Parse option type from string, compatible with Go asynq format
  pub fn parse(s: &str) -> Result<Self, OptionTypeParseError> {
    let s = s.trim();
    
    // 匹配格式: OptionName(value) 或 OptionName("value")
    if let Some(pos) = s.find('(') {
      let name = &s[..pos];
      let rest = &s[pos+1..];
      
      if let Some(end_pos) = rest.rfind(')') {
        let value = &rest[..end_pos].trim();
        
        match name {
          "MaxRetry" => {
            let v = value.parse::<u32>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid MaxRetry value: {}", value)))?;
            Ok(OptionType::MaxRetryOpt(v))
          },
          "Queue" => {
            // Remove quotes if present
            let v = value.trim_matches('"').to_string();
            Ok(OptionType::QueueOpt(v))
          },
          "Timeout" => {
            let v = value.parse::<u64>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid Timeout value: {}", value)))?;
            Ok(OptionType::TimeoutOpt(v))
          },
          "Deadline" => {
            let v = value.parse::<u64>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid Deadline value: {}", value)))?;
            Ok(OptionType::DeadlineOpt(v))
          },
          "Unique" => {
            Ok(OptionType::UniqueOpt(value.to_string()))
          },
          "ProcessAt" => {
            let v = value.parse::<u64>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid ProcessAt value: {}", value)))?;
            Ok(OptionType::ProcessAtOpt(v))
          },
          "ProcessIn" => {
            let v = value.parse::<u64>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid ProcessIn value: {}", value)))?;
            Ok(OptionType::ProcessInOpt(v))
          },
          "TaskID" => {
            let v = value.trim_matches('"').to_string();
            Ok(OptionType::TaskIDOpt(v))
          },
          "Retention" => {
            let v = value.parse::<u64>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid Retention value: {}", value)))?;
            Ok(OptionType::RetentionOpt(v))
          },
          "Group" => {
            let v = value.trim_matches('"').to_string();
            Ok(OptionType::GroupOpt(v))
          },
          "RateLimit" => {
            let v = value.parse::<u32>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid RateLimit value: {}", value)))?;
            Ok(OptionType::RateLimitOpt(v))
          },
          "GroupGracePeriod" => {
            let v = value.parse::<u64>()
              .map_err(|_| OptionTypeParseError::InvalidValue(format!("Invalid GroupGracePeriod value: {}", value)))?;
            Ok(OptionType::GroupGracePeriodOpt(v))
          },
          _ => Err(OptionTypeParseError::InvalidValue(format!("Unknown option type: {}", name)))
        }
      } else {
        Err(OptionTypeParseError::InvalidValue(format!("Missing closing parenthesis: {}", s)))
      }
    } else {
      Err(OptionTypeParseError::InvalidValue(format!("Invalid option format: {}", s)))
    }
  }
}

#[derive(Debug)]
pub enum OptionTypeParseError {
  InvalidValue(String),
}

/// 任务选项
#[derive(Debug, Clone, PartialEq)]
pub struct TaskOptions {
  /// 任务 ID，如果为空则自动生成
  pub task_id: Option<String>,
  /// 队列名称
  pub queue: String,
  /// 最大重试次数
  pub max_retry: i32,
  /// 任务超时
  pub timeout: Option<Duration>,
  /// 任务截止时间
  pub deadline: Option<DateTime<Utc>>,
  /// 唯一任务的 TTL
  pub unique_ttl: Option<Duration>,
  /// 处理时间（用于延迟任务）
  pub process_at: Option<DateTime<Utc>>,
  /// 延迟处理时间（相对于当前时间）
  pub process_in: Option<Duration>,
  /// 保留期限
  pub retention: Option<Duration>,
  /// 任务组
  pub group: Option<String>,
  /// 重试策略
  pub retry_policy: Option<RetryPolicy>,
  /// 速率限制配置
  pub rate_limit: Option<RateLimit>,
  /// 组聚合宽限期
  pub group_grace_period: Option<Duration>,
}

impl From<Vec<OptionType>> for TaskOptions {
  fn from(opts: Vec<OptionType>) -> Self {
    let mut task_opts = TaskOptions::default();
    
    for opt in opts {
      match opt {
        OptionType::MaxRetryOpt(v) => task_opts.max_retry = v as i32,
        OptionType::QueueOpt(v) => task_opts.queue = v,
        OptionType::TimeoutOpt(v) => task_opts.timeout = Some(Duration::from_secs(v)),
        OptionType::DeadlineOpt(v) => {
          task_opts.deadline = Some(DateTime::from_timestamp(v as i64, 0).unwrap_or_default());
        },
        OptionType::UniqueOpt(v) => {
          if let Ok(secs) = v.parse::<u64>() {
            task_opts.unique_ttl = Some(Duration::from_secs(secs));
          }
        },
        OptionType::ProcessAtOpt(v) => {
          task_opts.process_at = Some(DateTime::from_timestamp(v as i64, 0).unwrap_or_default());
        },
        OptionType::ProcessInOpt(v) => task_opts.process_in = Some(Duration::from_secs(v)),
        OptionType::TaskIDOpt(v) => task_opts.task_id = Some(v),
        OptionType::RetentionOpt(v) => task_opts.retention = Some(Duration::from_secs(v)),
        OptionType::GroupOpt(v) => task_opts.group = Some(v),
        OptionType::RateLimitOpt(v) => {
          // Create a default rate limit with the given limit value
          task_opts.rate_limit = Some(RateLimit::per_task_type(Duration::from_secs(60), v));
        },
        OptionType::GroupGracePeriodOpt(v) => {
          task_opts.group_grace_period = Some(Duration::from_secs(v));
        },
      }
    }
    
    task_opts
  }
}

impl Default for TaskOptions {
  fn default() -> Self {
    Self {
      task_id: None,
      queue: DEFAULT_QUEUE_NAME.to_string(),
      max_retry: DEFAULT_MAX_RETRY,
      timeout: None,
      deadline: None,
      unique_ttl: None,
      process_at: None,
      process_in: None,
      retention: None,
      group: None,
      retry_policy: None,
      rate_limit: None,
      group_grace_period: None,
    }
  }
}

/// 重试策略
#[derive(Debug, Clone)]
pub enum RetryPolicy {
  /// 固定延迟
  Fixed(Duration),
  /// 指数退避
  Exponential {
    /// 基础延迟
    base_delay: Duration,
    /// 最大延迟
    max_delay: Duration,
    /// 乘数
    multiplier: f64,
    /// 是否添加随机抖动
    jitter: bool,
  },
  /// 线性退避
  Linear {
    /// 基础延迟
    base_delay: Duration,
    /// 最大延迟
    max_delay: Duration,
    /// 步进值
    step: Duration,
  },
  /// 自定义延迟函数（重试次数 -> 延迟时间）
  Custom(fn(i32) -> Duration),
}

impl PartialEq for RetryPolicy {
  fn eq(&self, other: &Self) -> bool {
    use RetryPolicy::*;
    match (self, other) {
      (Fixed(a), Fixed(b)) => a == b,
      (
        Exponential {
          base_delay: a1,
          max_delay: a2,
          multiplier: a3,
          jitter: a4,
        },
        Exponential {
          base_delay: b1,
          max_delay: b2,
          multiplier: b3,
          jitter: b4,
        },
      ) => a1 == b1 && a2 == b2 && a3 == b3 && a4 == b4,
      (
        Linear {
          base_delay: a1,
          max_delay: a2,
          step: a3,
        },
        Linear {
          base_delay: b1,
          max_delay: b2,
          step: b3,
        },
      ) => a1 == b1 && a2 == b2 && a3 == b3,
      (Custom(_), Custom(_)) => false, // Never compare function pointers
      _ => false,
    }
  }
}

/// 速率限制配置
/// Rate limit configuration
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimit {
  /// 限制类型
  /// Type of rate limit
  pub limit_type: RateLimitType,
  /// 窗口大小（秒）
  /// Window size (seconds)
  pub window: Duration,
  /// 限制数量
  /// Limit count
  pub limit: u32,
  /// 限制键（如果为空则使用任务类型）
  /// Limit key (if None, use task type)
  pub key: Option<String>,
}

/// 速率限制类型
/// Rate limit type
#[derive(Debug, Clone, PartialEq)]
pub enum RateLimitType {
  /// 按任务类型限制
  /// Per task type
  PerTaskType,
  /// 按队列限制
  /// Per queue
  PerQueue,
  /// 按自定义键限制
  /// Custom key
  Custom,
}
/// 速率限制实现
/// Rate limit implementation
impl RateLimit {
  /// 创建按任务类型的速率限制
  pub fn per_task_type(window: Duration, limit: u32) -> Self {
    Self {
      limit_type: RateLimitType::PerTaskType,
      window,
      limit,
      key: None,
    }
  }

  /// 创建按队列的速率限制
  pub fn per_queue(window: Duration, limit: u32) -> Self {
    Self {
      limit_type: RateLimitType::PerQueue,
      window,
      limit,
      key: None,
    }
  }

  /// 创建自定义键的速率限制
  pub fn custom<T: AsRef<str>>(key: T, window: Duration, limit: u32) -> Self {
    Self {
      limit_type: RateLimitType::Custom,
      window,
      limit,
      key: Some(key.as_ref().to_string()),
    }
  }

  /// 生成速率限制键
  pub fn generate_key(&self, task_type: &str, queue: &str) -> String {
    match &self.limit_type {
      RateLimitType::PerTaskType => format!("asynq:ratelimit:task:{}", task_type),
      RateLimitType::PerQueue => format!("asynq:ratelimit:queue:{}", queue),
      RateLimitType::Custom => format!(
        "asynq:ratelimit:custom:{}",
        self.key.as_ref().unwrap_or(&"default".to_string())
      ),
    }
  }
}

/// 重试策略实现
impl RetryPolicy {
  /// 计算重试延迟
  pub fn calculate_delay(&self, retry_count: i32) -> Duration {
    match self {
      RetryPolicy::Fixed(delay) => *delay,
      RetryPolicy::Exponential {
        base_delay,
        max_delay,
        multiplier,
        jitter,
      } => {
        let mut delay = base_delay.as_secs_f64() * multiplier.powi(retry_count);
        if delay > max_delay.as_secs_f64() {
          delay = max_delay.as_secs_f64();
        }

        if *jitter {
          // 添加 ±25% 的随机抖动
          use rand::Rng;
          let mut rng = rand::rng();
          let jitter_factor = rng.random_range(0.75..=1.25);
          delay *= jitter_factor;
        }

        Duration::from_secs_f64(delay)
      }
      RetryPolicy::Linear {
        base_delay,
        max_delay,
        step,
      } => {
        let delay = base_delay.as_secs() + (step.as_secs() * retry_count as u64);
        Duration::from_secs(delay.min(max_delay.as_secs()))
      }
      RetryPolicy::Custom(func) => func(retry_count),
    }
  }

  /// 默认指数退避策略
  pub fn default_exponential() -> Self {
    Self::Exponential {
      base_delay: Duration::from_secs(1),
      max_delay: Duration::from_secs(3600), // 1小时
      multiplier: 2.0,
      jitter: true,
    }
  }

  /// 默认线性退避策略
  pub fn default_linear() -> Self {
    Self::Linear {
      base_delay: Duration::from_secs(30),
      max_delay: Duration::from_secs(1800), // 30分钟
      step: Duration::from_secs(60),        // 每次增加1分钟
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_option_type_display() {
    assert_eq!(OptionType::QueueOpt("critical".to_string()).to_string(), "Queue(\"critical\")");
    assert_eq!(OptionType::MaxRetryOpt(5).to_string(), "MaxRetry(5)");
    assert_eq!(OptionType::TimeoutOpt(60).to_string(), "Timeout(60)");
    assert_eq!(OptionType::TaskIDOpt("abc123".to_string()).to_string(), "TaskID(\"abc123\")");
    assert_eq!(OptionType::GroupOpt("batch".to_string()).to_string(), "Group(\"batch\")");
  }

  #[test]
  fn test_option_type_parse() {
    // Test Queue parsing
    let opt = OptionType::parse("Queue(\"critical\")").unwrap();
    assert_eq!(opt, OptionType::QueueOpt("critical".to_string()));

    // Test MaxRetry parsing
    let opt = OptionType::parse("MaxRetry(5)").unwrap();
    assert_eq!(opt, OptionType::MaxRetryOpt(5));

    // Test Timeout parsing
    let opt = OptionType::parse("Timeout(60)").unwrap();
    assert_eq!(opt, OptionType::TimeoutOpt(60));

    // Test TaskID parsing
    let opt = OptionType::parse("TaskID(\"abc123\")").unwrap();
    assert_eq!(opt, OptionType::TaskIDOpt("abc123".to_string()));

    // Test Group parsing
    let opt = OptionType::parse("Group(\"batch\")").unwrap();
    assert_eq!(opt, OptionType::GroupOpt("batch".to_string()));

    // Test Retention parsing
    let opt = OptionType::parse("Retention(300)").unwrap();
    assert_eq!(opt, OptionType::RetentionOpt(300));
  }

  #[test]
  fn test_option_type_roundtrip() {
    let options = vec![
      OptionType::QueueOpt("critical".to_string()),
      OptionType::MaxRetryOpt(5),
      OptionType::TimeoutOpt(60),
      OptionType::TaskIDOpt("task-123".to_string()),
      OptionType::RetentionOpt(3600),
      OptionType::GroupOpt("batch".to_string()),
    ];

    for opt in &options {
      let s = opt.to_string();
      let parsed = OptionType::parse(&s).unwrap();
      assert_eq!(*opt, parsed, "Failed roundtrip for option: {}", s);
    }
  }

  #[test]
  fn test_task_options_to_vec_option_type() {
    let mut opts = TaskOptions { queue: "critical".to_string(), ..Default::default() };
    opts.max_retry = 5;
    opts.timeout = Some(Duration::from_secs(60));
    opts.task_id = Some("task-123".to_string());

    let opt_vec: Vec<OptionType> = (&opts).into();
    
    // Check that key options are present
    assert!(opt_vec.contains(&OptionType::QueueOpt("critical".to_string())));
    assert!(opt_vec.contains(&OptionType::MaxRetryOpt(5)));
    assert!(opt_vec.contains(&OptionType::TimeoutOpt(60)));
    assert!(opt_vec.contains(&OptionType::TaskIDOpt("task-123".to_string())));
  }

  #[test]
  fn test_vec_option_type_to_task_options() {
    let opt_vec = vec![
      OptionType::QueueOpt("critical".to_string()),
      OptionType::MaxRetryOpt(5),
      OptionType::TimeoutOpt(60),
      OptionType::TaskIDOpt("task-123".to_string()),
      OptionType::RetentionOpt(3600),
    ];

    let task_opts: TaskOptions = opt_vec.into();
    
    assert_eq!(task_opts.queue, "critical");
    assert_eq!(task_opts.max_retry, 5);
    assert_eq!(task_opts.timeout, Some(Duration::from_secs(60)));
    assert_eq!(task_opts.task_id, Some("task-123".to_string()));
    assert_eq!(task_opts.retention, Some(Duration::from_secs(3600)));
  }

  #[test]
  fn test_stringify_and_parse_options() {
    let mut opts = TaskOptions { queue: "critical".to_string(), ..Default::default() };
    opts.max_retry = 5;
    opts.timeout = Some(Duration::from_secs(60));
    opts.retention = Some(Duration::from_secs(3600));

    // Convert to OptionType vec
    let opt_vec: Vec<OptionType> = (&opts).into();
    
    // Convert to strings
    let strings: Vec<String> = opt_vec.iter().map(|o| o.to_string()).collect();
    
    // Parse back
    let parsed_opts: Vec<OptionType> = strings
      .iter()
      .filter_map(|s| OptionType::parse(s).ok())
      .collect();
    
    // Convert to TaskOptions
    let result_opts: TaskOptions = parsed_opts.into();
    
    // Verify key fields match
    assert_eq!(result_opts.queue, opts.queue);
    assert_eq!(result_opts.max_retry, opts.max_retry);
    assert_eq!(result_opts.timeout, opts.timeout);
    assert_eq!(result_opts.retention, opts.retention);
  }
}
