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
#[cfg(feature = "python")]
mod python;
pub mod collection_cache;
pub mod system_function_cache;
pub mod tenant_cache;
mod three_layer_cache;

pub use collection_cache::{CollectionBackend, CollectionCache};
pub use config::CacheConfig;
pub use error::CacheError;
pub use system_function_cache::{SystemFunctionBackend, SystemFunctionCache, SystemFunctions};
pub use tenant_cache::{TenantBackend, TenantCache, TenantInfo};
pub use three_layer_cache::{
    CacheKey, Cacheable, DataFetcher, KeyFormatter, ThreeLayerCache, shutdown_pubsub_hub,
};

// Re-export async_trait for convenience
pub use async_trait::async_trait;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
fn pcache(m: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(m)?;
    Ok(())
}
