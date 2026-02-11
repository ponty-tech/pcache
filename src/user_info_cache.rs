//! User info cache for per-account settings
//!
//! This module provides a cache for user information (account settings) per account.
//! Each account has a set of key-value settings (e.g., locale, notification_interval).
//!
//! ## Cache Keys
//!
//! Redis key format and invalidation channel are configurable at construction time,
//! allowing different services to use different L2 keys while sharing an invalidation
//! channel for cross-service L1 cache clearing.

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// User info - account settings as key-value pairs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    /// Account ID
    pub account_id: String,
    /// Map of setting key to value
    pub settings: HashMap<String, String>,
}

impl UserInfo {
    /// Get a setting value by key (returns None if not found)
    pub fn get_setting(&self, key: &str) -> Option<&str> {
        self.settings.get(key).map(|s| s.as_str())
    }
}

/// Trait for fetching user info from the backend (L3)
///
/// Implementations can use MySQL, HTTP, or any other backend.
#[async_trait]
pub trait UserInfoBackend: Send + Sync + 'static {
    /// Fetch user info (account settings) for an account
    async fn fetch(
        &self,
        account_id: &str,
    ) -> Result<Option<UserInfo>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Wrapper to make UserInfoBackend work with ThreeLayerCache
struct UserInfoFetcher<B: UserInfoBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: UserInfoBackend> DataFetcher<String, UserInfo, ()> for UserInfoFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        account_id: &String,
    ) -> Result<Option<UserInfo>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch(account_id).await
    }
}

/// Key formatter for user info cache with configurable key prefix/suffix and channel
struct UserInfoKeyFormatter {
    key_prefix: &'static str,
    key_suffix: &'static str,
    channel: &'static str,
}

impl KeyFormatter<String> for UserInfoKeyFormatter {
    fn format_key(&self, account_id: &String) -> String {
        format!("{}{}{}", self.key_prefix, account_id, self.key_suffix)
    }

    fn invalidation_channel(&self) -> &'static str {
        self.channel
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<String> {
        Some(payload.to_owned())
    }
}

/// Three-layer cache for user info (account settings)
///
/// Caches per-account settings with L1 (Moka) + L2 (Redis) + L3 (pluggable backend).
/// Supports cross-instance invalidation via Redis pub/sub.
///
/// Redis key format and invalidation channel are configurable at construction time.
pub struct UserInfoCache<B: UserInfoBackend> {
    inner: ThreeLayerCache<String, UserInfo, (), UserInfoFetcher<B>, UserInfoKeyFormatter>,
}

impl<B: UserInfoBackend> Clone for UserInfoCache<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: UserInfoBackend> UserInfoCache<B> {
    /// Create a new UserInfoCache with the given backend and configurable keys
    ///
    /// # Arguments
    ///
    /// * `redis_client` - Redis connection client
    /// * `backend` - L3 backend for fetching user info
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
            UserInfoFetcher { backend },
            UserInfoKeyFormatter {
                key_prefix: redis_key_prefix,
                key_suffix: redis_key_suffix,
                channel: invalidation_channel,
            },
        )
        .await?;

        Ok(Self { inner })
    }

    /// Get user info for an account, using three-layer caching
    pub async fn get(&self, account_id: &str) -> Result<Option<Arc<UserInfo>>, CacheError> {
        self.inner.get(&account_id.to_owned()).await
    }

    /// Invalidate cache for a specific account
    pub async fn invalidate(&self, account_id: &str) -> Result<(), CacheError> {
        self.inner.invalidate(&account_id.to_owned()).await
    }
}
