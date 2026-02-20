//! Client cache for OAuth2 client lookups
//!
//! This module provides a cache for OAuth2 clients keyed by client ID.
//! The client type is defined by the consumer via the `ClientBackend` associated type.
//!
//! ## Cache Keys
//!
//! - L2 (Redis): `cache:client:{client_id}`
//! - Invalidation channel: `cache:invalidate:client`

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// Trait for fetching OAuth2 clients from the backend (L3).
///
/// The `Client` associated type allows the consumer (e.g. hermes) to define
/// its own client model while the cache struct lives in pcache.
#[async_trait]
pub trait ClientBackend: Send + Sync + 'static {
    /// The client type to cache (must be serializable for Redis L2 storage)
    type Client: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// Fetch a client by ID from the backend
    async fn fetch_by_id(
        &self,
        id: &str,
    ) -> Result<Option<Self::Client>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Adapts a [`ClientBackend`] to the [`DataFetcher`] trait expected by
/// [`ThreeLayerCache`].
struct ClientFetcher<B: ClientBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: ClientBackend> DataFetcher<String, B::Client, ()> for ClientFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        id: &String,
    ) -> Result<Option<B::Client>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch_by_id(id).await
    }
}

/// Key formatter for client cache.
struct ClientKeyFormatter;

impl KeyFormatter<String> for ClientKeyFormatter {
    fn format_key(&self, key: &String) -> String {
        format!("cache:client:{key}")
    }

    fn invalidation_channel(&self) -> &'static str {
        "cache:invalidate:client"
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<String> {
        Some(payload.to_owned())
    }
}

/// Three-layer cache for OAuth2 clients.
///
/// Caches clients by ID with L1 (Moka) + L2 (Redis) + L3 (pluggable backend).
/// Supports cross-instance invalidation via Redis pub/sub.
pub struct ClientCache<B: ClientBackend> {
    inner: ThreeLayerCache<String, B::Client, (), ClientFetcher<B>, ClientKeyFormatter>,
}

impl<B: ClientBackend> Clone for ClientCache<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: ClientBackend> ClientCache<B> {
    /// Create a new ClientCache with the given backend.
    pub fn new(
        redis_client: redis::Client,
        redis_conn: redis::aio::ConnectionManager,
        backend: B,
        config: CacheConfig,
    ) -> Self {
        let backend = Arc::new(backend);

        let inner = ThreeLayerCache::new(
            redis_client,
            redis_conn,
            (),
            config,
            ClientFetcher { backend },
            ClientKeyFormatter,
        );

        Self { inner }
    }

    /// Get a client by ID, using three-layer caching.
    pub async fn get(&self, id: &str) -> Result<Option<Arc<B::Client>>, CacheError> {
        self.inner.get(&id.to_owned()).await
    }

    /// Invalidate cache for a specific client.
    pub async fn invalidate(&self, client_id: &str) -> Result<(), CacheError> {
        self.inner.invalidate(&client_id.to_owned()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_key_formatter_format_key() {
        let formatter = ClientKeyFormatter;
        assert_eq!(
            formatter.format_key(&"ponty-frontend".to_owned()),
            "cache:client:ponty-frontend"
        );
        assert_eq!(
            formatter.format_key(&"my-app-123".to_owned()),
            "cache:client:my-app-123"
        );
    }

    #[test]
    fn client_key_formatter_format_key_with_special_chars() {
        let formatter = ClientKeyFormatter;
        assert_eq!(
            formatter.format_key(&"client-with-dashes".to_owned()),
            "cache:client:client-with-dashes"
        );
        assert_eq!(
            formatter.format_key(&"client_with_underscores".to_owned()),
            "cache:client:client_with_underscores"
        );
    }

    #[test]
    fn client_key_formatter_invalidation_channel() {
        let formatter = ClientKeyFormatter;
        assert_eq!(formatter.invalidation_channel(), "cache:invalidate:client");
    }

    #[test]
    fn client_key_formatter_parse_invalidation_payload() {
        let formatter = ClientKeyFormatter;
        assert_eq!(
            formatter.parse_invalidation_payload("client-xyz"),
            Some("client-xyz".to_owned())
        );
        assert_eq!(
            formatter.parse_invalidation_payload(""),
            Some(String::new())
        );
    }

    #[test]
    fn client_key_formatter_parse_invalidation_payload_special_chars() {
        let formatter = ClientKeyFormatter;
        assert_eq!(
            formatter.parse_invalidation_payload("client:with:colons"),
            Some("client:with:colons".to_owned())
        );
    }
}
