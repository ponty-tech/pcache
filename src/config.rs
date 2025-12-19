//! Cache configuration

use std::time::Duration;

/// Configuration for the three-layer cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries in L1 cache
    pub l1_max_capacity: u64,
    /// Time-to-live for L1 cache entries
    pub l1_ttl: Duration,
    /// Time-to-live for L2 (Redis) cache entries
    pub l2_ttl: Duration,
    /// Enable Redis pub/sub for cache invalidation
    pub enable_pubsub: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_max_capacity: 1000,
            l1_ttl: Duration::from_secs(300), // 5 minutes
            l2_ttl: Duration::from_secs(900), // 15 minutes
            enable_pubsub: true,
        }
    }
}
