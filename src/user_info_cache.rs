//! User info cache for account-level user information
//!
//! This module provides a cache for user information keyed by account ID (i32).
//! The user info type is defined by the consumer via the `UserInfoBackend` associated type.
//!
//! ## Cache Keys
//!
//! - L2 (Redis): `cache:account:{account_id}:userinfo`
//! - Invalidation channel: `cache:invalidate:account:userinfo`

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// Trait for fetching user information from the backend (L3).
///
/// The `UserInfo` associated type allows the consumer (e.g. hermes) to define
/// its own user info model while the cache struct lives in pcache.
#[async_trait]
pub trait UserInfoBackend: Send + Sync + 'static {
    /// The user info type to cache (must be serializable for Redis L2 storage)
    type UserInfo: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// Fetch user info by account ID from the backend
    async fn fetch_by_account_id(
        &self,
        account_id: i32,
    ) -> Result<Option<Self::UserInfo>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Adapts a [`UserInfoBackend`] to the [`DataFetcher`] trait expected by
/// [`ThreeLayerCache`].
struct UserInfoFetcher<B: UserInfoBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: UserInfoBackend> DataFetcher<i32, B::UserInfo, ()> for UserInfoFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        account_id: &i32,
    ) -> Result<Option<B::UserInfo>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch_by_account_id(*account_id).await
    }
}

/// Key formatter for user info cache.
struct UserInfoKeyFormatter;

impl KeyFormatter<i32> for UserInfoKeyFormatter {
    fn format_key(&self, key: &i32) -> String {
        format!("cache:account:{key}:userinfo")
    }

    fn invalidation_channel(&self) -> &'static str {
        "cache:invalidate:account:userinfo"
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<i32> {
        payload.parse::<i32>().ok()
    }
}

/// Three-layer cache for user information.
///
/// Caches user info by account ID with L1 (Moka) + L2 (Redis) + L3 (pluggable backend).
/// Supports cross-instance invalidation via Redis pub/sub.
pub struct UserInfoCache<B: UserInfoBackend> {
    inner: ThreeLayerCache<i32, B::UserInfo, (), UserInfoFetcher<B>, UserInfoKeyFormatter>,
}

impl<B: UserInfoBackend> Clone for UserInfoCache<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: UserInfoBackend> UserInfoCache<B> {
    /// Create a new UserInfoCache with the given backend.
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
            UserInfoFetcher { backend },
            UserInfoKeyFormatter,
        );

        Self { inner }
    }

    /// Get user info by account ID, using three-layer caching.
    pub async fn get(&self, account_id: i32) -> Result<Option<Arc<B::UserInfo>>, CacheError> {
        self.inner.get(&account_id).await
    }

    /// Invalidate user info cache for a specific account.
    pub async fn invalidate(&self, account_id: i32) -> Result<(), CacheError> {
        self.inner.invalidate(&account_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_info_key_formatter_format_key() {
        let formatter = UserInfoKeyFormatter;
        assert_eq!(
            formatter.format_key(&42),
            "cache:account:42:userinfo"
        );
        assert_eq!(
            formatter.format_key(&12345),
            "cache:account:12345:userinfo"
        );
    }

    #[test]
    fn user_info_key_formatter_invalidation_channel() {
        let formatter = UserInfoKeyFormatter;
        assert_eq!(
            formatter.invalidation_channel(),
            "cache:invalidate:account:userinfo"
        );
    }

    #[test]
    fn user_info_key_formatter_parse_invalidation_payload_valid() {
        let formatter = UserInfoKeyFormatter;
        assert_eq!(
            formatter.parse_invalidation_payload("42"),
            Some(42)
        );
        assert_eq!(
            formatter.parse_invalidation_payload("12345"),
            Some(12345)
        );
    }

    #[test]
    fn user_info_key_formatter_parse_invalidation_payload_invalid() {
        let formatter = UserInfoKeyFormatter;
        assert_eq!(
            formatter.parse_invalidation_payload("not-a-number"),
            None
        );
        assert_eq!(
            formatter.parse_invalidation_payload(""),
            None
        );
    }
}
