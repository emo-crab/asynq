//! SeaORM 实体模块
//! SeaORM entity module
//!
//! 定义了与 PostgresSQL 表对应的实体模型
//! Defines entity models corresponding to PostgresSQL tables

pub mod prelude;
pub mod queues;
pub mod scheduler_entries;
pub mod scheduler_events;
pub mod schedulers;
pub mod servers;
pub mod stats;
pub mod tasks;
pub mod workers;

pub use prelude::*;
