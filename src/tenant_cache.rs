//! Unified tenant cache with dual-key lookup
//!
//! This module provides a tenant cache that supports lookup by:
//! - `tenant_id` (primary key)
//! - `slug` (secondary key, extracted from subdomain)
//!
//! Both keys map to the same `TenantInfo` struct which includes both basic
//! tenant data and all tenant settings. This cache is shared between hermes
//! (MySQL backend) and proteus (HTTP/Helios backend).

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Tenant information with settings, cached by both id and slug
///
/// This struct is the unified tenant cache entry used by both hermes and proteus.
/// It includes basic tenant information plus all tenant settings as key-value pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantInfo {
    /// Tenant ID (primary key)
    pub id: String,
    /// Tenant slug (used in subdomains)
    pub slug: String,
    /// Tenant display name
    pub name: String,
    /// Whether the tenant is active (inactive_at is NULL)
    #[serde(default = "default_active")]
    pub active: bool,
    /// Tenant settings as key-value pairs
    #[serde(default)]
    pub settings: HashMap<String, serde_json::Value>,
}

fn default_active() -> bool {
    true
}

impl TenantInfo {
    /// Get a setting value as a string reference (for String values only)
    pub fn get_setting_str(&self, key: &str) -> Option<&str> {
        self.settings.get(key).and_then(|v| v.as_str())
    }

    /// Get a setting value as a string (converts non-string types)
    pub fn get_setting(&self, key: &str) -> Option<String> {
        self.settings.get(key).and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            _ => None,
        })
    }

    /// Get a setting as an i64 (returns None if not found or not parseable)
    pub fn get_setting_i64(&self, key: &str) -> Option<i64> {
        self.settings.get(key).and_then(|v| match v {
            serde_json::Value::Number(n) => n.as_i64(),
            serde_json::Value::String(s) => s.parse::<i64>().ok(),
            _ => None,
        })
    }

    /// Get a setting as a bool (returns None if not found or not parseable)
    pub fn get_setting_bool(&self, key: &str) -> Option<bool> {
        self.settings.get(key).and_then(|v| match v {
            serde_json::Value::Bool(b) => Some(*b),
            serde_json::Value::String(s) => s.parse::<bool>().ok(),
            serde_json::Value::Number(n) => n.as_i64().map(|i| i != 0),
            _ => None,
        })
    }
}

/// Trait for fetching tenant data from the backend (L3)
///
/// Implementations can use MySQL (hermes) or HTTP (proteus) as the backend.
#[async_trait]
pub trait TenantBackend: Send + Sync + 'static {
    /// Fetch tenant by ID
    async fn fetch_by_id(
        &self,
        id: &str,
    ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>>;

    /// Fetch tenant by slug
    async fn fetch_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Wrapper to make TenantBackend work with ThreeLayerCache for ID lookups
pub struct TenantByIdFetcher<B: TenantBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: TenantBackend> DataFetcher<String, TenantInfo, ()> for TenantByIdFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        id: &String,
    ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch_by_id(id).await
    }
}

/// Wrapper to make TenantBackend work with ThreeLayerCache for slug lookups
pub struct TenantBySlugFetcher<B: TenantBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: TenantBackend> DataFetcher<String, TenantInfo, ()> for TenantBySlugFetcher<B> {
    async fn fetch(
        &self,
        _ctx: &(),
        slug: &String,
    ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch_by_slug(slug).await
    }
}

/// Key formatter for tenant-by-id cache
pub struct TenantByIdKeyFormatter;

impl KeyFormatter<String> for TenantByIdKeyFormatter {
    fn format_key(&self, id: &String) -> String {
        format!("cache:tenant:id:{}", id)
    }

    fn invalidation_channel(&self) -> &'static str {
        "cache:invalidate:tenant:id"
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<String> {
        Some(payload.to_owned())
    }
}

/// Key formatter for tenant-by-slug cache
pub struct TenantBySlugKeyFormatter;

impl KeyFormatter<String> for TenantBySlugKeyFormatter {
    fn format_key(&self, slug: &String) -> String {
        format!("cache:tenant:slug:{}", slug)
    }

    fn invalidation_channel(&self) -> &'static str {
        "cache:invalidate:tenant:slug"
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<String> {
        Some(payload.to_owned())
    }
}

/// Unified tenant cache with dual-key lookup
///
/// Supports lookup by both `tenant_id` and `slug`, with automatic
/// cross-population of caches.
pub struct TenantCache<B: TenantBackend> {
    by_id: ThreeLayerCache<String, TenantInfo, (), TenantByIdFetcher<B>, TenantByIdKeyFormatter>,
    by_slug:
        ThreeLayerCache<String, TenantInfo, (), TenantBySlugFetcher<B>, TenantBySlugKeyFormatter>,
}

impl<B: TenantBackend> Clone for TenantCache<B> {
    fn clone(&self) -> Self {
        Self {
            by_id: self.by_id.clone(),
            by_slug: self.by_slug.clone(),
        }
    }
}

impl<B: TenantBackend> TenantCache<B> {
    /// Create a new TenantCache with the given backend
    pub async fn new(
        redis_client: redis::Client,
        backend: B,
        config: CacheConfig,
    ) -> Result<Self, redis::RedisError> {
        let backend = Arc::new(backend);

        let by_id = ThreeLayerCache::new(
            redis_client.clone(),
            (),
            config.clone(),
            TenantByIdFetcher {
                backend: Arc::clone(&backend),
            },
            TenantByIdKeyFormatter,
        )
        .await?;

        let by_slug = ThreeLayerCache::new(
            redis_client,
            (),
            config,
            TenantBySlugFetcher { backend },
            TenantBySlugKeyFormatter,
        )
        .await?;

        Ok(Self { by_id, by_slug })
    }

    /// Get tenant by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<Arc<TenantInfo>>, CacheError> {
        self.by_id.get(&id.to_owned()).await
    }

    /// Get tenant by slug
    pub async fn get_by_slug(&self, slug: &str) -> Result<Option<Arc<TenantInfo>>, CacheError> {
        self.by_slug.get(&slug.to_owned()).await
    }

    /// Check if a tenant is active by ID
    pub async fn is_tenant_active(&self, id: &str) -> Result<bool, CacheError> {
        match self.get_by_id(id).await? {
            Some(tenant) => Ok(tenant.active),
            None => Ok(false),
        }
    }

    /// Invalidate tenant cache by ID
    ///
    /// Note: This invalidates the by-id cache. The by-slug cache will be
    /// invalidated via pub/sub if the payload includes the slug.
    pub async fn invalidate_by_id(&self, id: &str) -> Result<(), CacheError> {
        self.by_id.invalidate(&id.to_owned()).await
    }

    /// Invalidate tenant cache by slug
    pub async fn invalidate_by_slug(&self, slug: &str) -> Result<(), CacheError> {
        self.by_slug.invalidate(&slug.to_owned()).await
    }

    /// Invalidate tenant cache by both ID and slug
    pub async fn invalidate(&self, id: &str, slug: &str) -> Result<(), CacheError> {
        self.by_id.invalidate(&id.to_owned()).await?;
        self.by_slug.invalidate(&slug.to_owned()).await
    }

    /// Extract slug from hostname
    ///
    /// Given "myapp.pontydev.se" and suffix ".pontydev.se", returns "myapp"
    pub fn extract_slug_from_host(host: &str, domain_suffix: &str) -> Option<String> {
        // Remove port if present
        let host = host.split(':').next().unwrap_or(host);

        // Domain suffix should include the leading dot (e.g., ".pontydev.se")
        // Check with or without leading dot to avoid allocation
        let (suffix, suffix_with_dot);
        let suffix_to_check: &str = if domain_suffix.starts_with('.') {
            domain_suffix
        } else {
            suffix_with_dot = format!(".{}", domain_suffix);
            suffix = suffix_with_dot.as_str();
            suffix
        };

        if let Some(slug) = host.strip_suffix(suffix_to_check)
            && !slug.is_empty()
            && !slug.contains('.')
        {
            return Some(slug.to_owned());
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_slug_from_host() {
        // Basic case
        assert_eq!(
            TenantCache::<MockBackend>::extract_slug_from_host("myapp.pontydev.se", ".pontydev.se"),
            Some("myapp".to_owned())
        );

        // With port
        assert_eq!(
            TenantCache::<MockBackend>::extract_slug_from_host(
                "myapp.pontydev.se:443",
                ".pontydev.se"
            ),
            Some("myapp".to_owned())
        );

        // Domain suffix without leading dot
        assert_eq!(
            TenantCache::<MockBackend>::extract_slug_from_host("myapp.pontydev.se", "pontydev.se"),
            Some("myapp".to_owned())
        );

        // No subdomain (just the domain)
        assert_eq!(
            TenantCache::<MockBackend>::extract_slug_from_host("pontydev.se", ".pontydev.se"),
            None
        );

        // Wrong domain
        assert_eq!(
            TenantCache::<MockBackend>::extract_slug_from_host("myapp.example.com", ".pontydev.se"),
            None
        );

        // Nested subdomain
        assert_eq!(
            TenantCache::<MockBackend>::extract_slug_from_host(
                "sub.myapp.pontydev.se",
                ".pontydev.se"
            ),
            None
        );
    }

    // Mock backend for tests
    struct MockBackend;

    #[async_trait]
    impl TenantBackend for MockBackend {
        async fn fetch_by_id(
            &self,
            _id: &str,
        ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(None)
        }

        async fn fetch_by_slug(
            &self,
            _slug: &str,
        ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(None)
        }
    }
}
