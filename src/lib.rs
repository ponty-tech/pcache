//! pcache - Three-layer caching library
//!
//! This library provides a generic three-tier caching strategy:
//! - L1: In-memory Moka cache (fastest)
//! - L2: Redis cache (medium speed, shared across instances)
//! - L3: Pluggable backend (database, HTTP, etc.)
//!
//! The cache supports:
//! - Automatic fallback between layers
//! - Redis pub/sub for cache invalidation across instances
//! - Configurable TTLs and capacity
//! - Generic key and value types
//! - Backend-agnostic L3 fetching

mod config;
mod error;
pub mod tenant_cache;
mod three_layer_cache;

pub use config::CacheConfig;
pub use error::CacheError;
pub use tenant_cache::{TenantBackend, TenantCache, TenantInfo};
pub use three_layer_cache::{
    CacheKey, Cacheable, DataFetcher, KeyFormatter, ThreeLayerCache, shutdown_pubsub_hub,
};

// Re-export async_trait for convenience
pub use async_trait::async_trait;
