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
    /// TTL for negative cache entries (backend returned None) in L2.
    /// When set, "not found" results are cached in Redis to prevent
    /// repeated backend lookups for non-existent keys.
    /// `None` disables negative caching (default).
    pub negative_ttl: Option<Duration>,
    /// Also remember "not found" results in L1 (in memory), for `negative_ttl`.
    /// Without it every lookup of a missing key costs a Redis round-trip.
    ///
    /// Opt-in because it changes when a newly created key becomes visible: on
    /// other instances the in-memory entry is only cleared by a pub/sub
    /// invalidation (which needs `enable_pubsub` and a `KeyFormatter` that
    /// implements `parse_invalidation_payload`) or by its TTL. An entry may be
    /// refreshed from a still-live L2 sentinel, so without invalidation a
    /// missing key can stay hidden for up to twice `negative_ttl`.
    /// Has no effect unless `negative_ttl` is set.
    pub negative_l1: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_max_capacity: 1000,
            l1_ttl: Duration::from_secs(300), // 5 minutes
            l2_ttl: Duration::from_secs(900), // 15 minutes
            enable_pubsub: true,
            negative_ttl: None,
            negative_l1: false,
        }
    }
}
