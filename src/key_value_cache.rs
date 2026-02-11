//! Generic key-value cache
//!
//! A three-layer cache that maps a string key to a set of string key-value pairs.
//! Redis key format and invalidation channel are configurable at construction time,
//! allowing different services to use different L2 keys while sharing an invalidation
//! channel for cross-service L1 cache clearing.

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A cached entry: a string identifier with associated key-value data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueEntry {
    /// The lookup key (e.g. account ID, tenant ID, …)
    pub id: String,
    /// The key-value data
    pub data: HashMap<String, String>,
}

impl KeyValueEntry {
    /// Look up a value by key, returns None if not present.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|s| s.as_str())
    }
}

/// Trait for fetching a key-value entry from the backend (L3).
///
/// Implementations can use MySQL, HTTP, or any other data source.
#[async_trait]
pub trait KeyValueBackend: Send + Sync + 'static {
    /// Fetch the entry for the given ID, or None if it doesn't exist.
    async fn fetch(
        &self,
        id: &str,
    ) -> Result<Option<KeyValueEntry>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Adapter from KeyValueBackend to the generic DataFetcher trait.
struct KeyValueFetcher<B: KeyValueBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: KeyValueBackend> DataFetcher<String, KeyValueEntry, ()> for KeyValueFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        id: &String,
    ) -> Result<Option<KeyValueEntry>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch(id).await
    }
}

/// Key formatter with configurable prefix/suffix and invalidation channel.
struct KeyValueKeyFormatter {
    key_prefix: &'static str,
    key_suffix: &'static str,
    channel: &'static str,
}

impl KeyFormatter<String> for KeyValueKeyFormatter {
    fn format_key(&self, id: &String) -> String {
        format!("{}{}{}", self.key_prefix, id, self.key_suffix)
    }

    fn invalidation_channel(&self) -> &'static str {
        self.channel
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<String> {
        Some(payload.to_owned())
    }
}

/// Three-layer key-value cache.
///
/// Caches string-keyed entries with L1 (Moka) + L2 (Redis) + L3 (pluggable backend).
/// Supports cross-instance invalidation via Redis pub/sub.
///
/// Redis key format and invalidation channel are configurable at construction time.
pub struct KeyValueCache<B: KeyValueBackend> {
    inner: ThreeLayerCache<String, KeyValueEntry, (), KeyValueFetcher<B>, KeyValueKeyFormatter>,
}

impl<B: KeyValueBackend> Clone for KeyValueCache<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: KeyValueBackend> KeyValueCache<B> {
    /// Create a new KeyValueCache.
    ///
    /// # Arguments
    ///
    /// * `redis_client` - Redis connection client
    /// * `backend` - L3 backend for fetching entries
    /// * `config` - Cache configuration (TTLs, capacity, pub/sub)
    /// * `redis_key_prefix` - Prefix for L2 Redis keys (e.g. `"cache:account:"`)
    /// * `redis_key_suffix` - Suffix for L2 Redis keys (e.g. `":settings"`)
    /// * `invalidation_channel` - Redis pub/sub channel for cross-instance invalidation
    pub async fn new(
        redis_client: redis::Client,
        backend: B,
        config: CacheConfig,
        redis_key_prefix: &'static str,
        redis_key_suffix: &'static str,
        invalidation_channel: &'static str,
    ) -> Result<Self, redis::RedisError> {
        let backend = Arc::new(backend);

        let inner = ThreeLayerCache::new(
            redis_client,
            (),
            config,
            KeyValueFetcher { backend },
            KeyValueKeyFormatter {
                key_prefix: redis_key_prefix,
                key_suffix: redis_key_suffix,
                channel: invalidation_channel,
            },
        )
        .await?;

        Ok(Self { inner })
    }

    /// Get the entry for the given ID, using three-layer caching.
    pub async fn get(&self, id: &str) -> Result<Option<Arc<KeyValueEntry>>, CacheError> {
        self.inner.get(&id.to_owned()).await
    }

    /// Invalidate the cache entry for the given ID.
    pub async fn invalidate(&self, id: &str) -> Result<(), CacheError> {
        self.inner.invalidate(&id.to_owned()).await
    }
}
