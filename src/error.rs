//! Cache error types

use redis::RedisError;

/// Cache-related errors
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Redis error: {0}")]
    Redis(#[from] RedisError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Backend error: {0}")]
    Backend(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Not found")]
    NotFound,
}
