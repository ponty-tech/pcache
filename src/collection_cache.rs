//! Collection cache for bulk/aggregate values
//!
//! This module provides a cache for a single collection value (e.g., all tenants
//! with settings). Unlike `TenantCache` or `SystemFunctionCache` which cache
//! per-key, this caches one bulk result with L1 (Moka) + L2 (Redis) + L3
//! (pluggable backend) and cross-instance invalidation via Redis pub/sub.
//!
//! ## Example
//!
//! The Redis key and invalidation channel are configurable at construction time:
//!
//! - L2 (Redis): configurable, e.g. `cache:collection:tenant_settings`
//! - Invalidation channel: configurable, e.g. `cache:invalidate:collection:tenant_settings`

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// The fixed internal key used for the single cache entry.
const SENTINEL_KEY: &str = "_";

/// Trait for fetching an entire collection from the backend (L3).
///
/// Unlike per-key caches, this trait takes no key — the entire collection
/// is treated as a single cached value.
#[async_trait]
pub trait CollectionBackend: Send + Sync + 'static {
    /// The type of the collection value (e.g., `Vec<TenantWithSettings>`)
    type Value: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// Fetch the entire collection from the backend.
    async fn fetch_all(
        &self,
    ) -> Result<Option<Self::Value>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Adapts a [`CollectionBackend`] to the [`DataFetcher`] trait expected by
/// [`ThreeLayerCache`]. Ignores both the context and key arguments.
struct CollectionFetcher<B: CollectionBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: CollectionBackend> DataFetcher<String, B::Value, ()> for CollectionFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        _key: &String,
    ) -> Result<Option<B::Value>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch_all().await
    }
}

/// Key formatter for collection caches.
///
/// Since a collection cache stores exactly one entry, `format_key` ignores
/// the key argument and returns the configured `redis_key`.
struct CollectionKeyFormatter {
    redis_key: &'static str,
    channel: &'static str,
}

impl KeyFormatter<String> for CollectionKeyFormatter {
    fn format_key(&self, _key: &String) -> String {
        self.redis_key.to_owned()
    }

    fn invalidation_channel(&self) -> &'static str {
        self.channel
    }

    fn parse_invalidation_payload(&self, _payload: &str) -> Option<String> {
        Some(SENTINEL_KEY.to_owned())
    }
}

/// Three-layer cache for a single collection value.
///
/// Caches one bulk result (e.g., "all tenants with settings") with
/// L1 (Moka) + L2 (Redis) + L3 (pluggable backend).
/// Supports cross-instance invalidation via Redis pub/sub.
pub struct CollectionCache<B: CollectionBackend> {
    inner: ThreeLayerCache<String, B::Value, (), CollectionFetcher<B>, CollectionKeyFormatter>,
}

impl<B: CollectionBackend> Clone for CollectionCache<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: CollectionBackend> CollectionCache<B> {
    /// Create a new CollectionCache.
    ///
    /// - `redis_key`: The Redis key used for L2 storage (e.g., `"cache:collection:tenants"`)
    /// - `invalidation_channel`: The Redis pub/sub channel for cross-instance invalidation
    pub async fn new(
        redis_client: redis::Client,
        backend: B,
        config: CacheConfig,
        redis_key: &'static str,
        invalidation_channel: &'static str,
    ) -> Result<Self, redis::RedisError> {
        let backend = Arc::new(backend);

        let inner = ThreeLayerCache::new(
            redis_client,
            (),
            config,
            CollectionFetcher { backend },
            CollectionKeyFormatter {
                redis_key,
                channel: invalidation_channel,
            },
        )
        .await?;

        Ok(Self { inner })
    }

    /// Get the cached collection value.
    ///
    /// Returns `Ok(None)` if the backend returned `None`.
    pub async fn get(&self) -> Result<Option<Arc<B::Value>>, CacheError> {
        self.inner.get(&SENTINEL_KEY.to_owned()).await
    }

    /// Invalidate the cached collection across all layers and instances.
    pub async fn invalidate(&self) -> Result<(), CacheError> {
        self.inner.invalidate(&SENTINEL_KEY.to_owned()).await
    }
}
