//! System function cache for per-tenant feature flags
//!
//! This module provides a cache for system functions (feature flags) per tenant.
//! Each tenant has a set of functions that can be active or inactive (e.g., TFA enabled).
//!
//! ## Cache Keys
//!
//! - L2 (Redis): `cache:tenant:{tenant_id}:system_function`
//! - Invalidation channel: `cache:invalidate:system_function`

use crate::{CacheConfig, CacheError, DataFetcher, KeyFormatter, ThreeLayerCache};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// System functions - function name to active status mapping for a tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemFunctions {
    /// Tenant ID this set of functions belongs to
    pub tenant_id: String,
    /// Map of function name to active status
    pub functions: HashMap<String, bool>,
}

impl SystemFunctions {
    /// Check if a function is active (returns false if not found or inactive)
    pub fn is_active(&self, function_name: &str) -> bool {
        self.functions.get(function_name).copied().unwrap_or(false)
    }
}

/// Trait for fetching system functions from the backend (L3)
///
/// Implementations can use MySQL, HTTP, or any other backend.
#[async_trait]
pub trait SystemFunctionBackend: Send + Sync + 'static {
    /// Fetch system functions for a tenant
    async fn fetch(
        &self,
        tenant_id: &str,
    ) -> Result<Option<SystemFunctions>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Wrapper to make SystemFunctionBackend work with ThreeLayerCache
struct SystemFunctionFetcher<B: SystemFunctionBackend> {
    backend: Arc<B>,
}

#[async_trait]
impl<B: SystemFunctionBackend> DataFetcher<String, SystemFunctions, ()>
    for SystemFunctionFetcher<B>
{
    async fn fetch(
        &self,
        _ctx: &(),
        tenant_id: &String,
    ) -> Result<Option<SystemFunctions>, Box<dyn std::error::Error + Send + Sync>> {
        self.backend.fetch(tenant_id).await
    }
}

/// Key formatter for system function cache
struct SystemFunctionKeyFormatter;

impl KeyFormatter<String> for SystemFunctionKeyFormatter {
    fn format_key(&self, tenant_id: &String) -> String {
        format!("cache:tenant:{tenant_id}:system_function")
    }

    fn invalidation_channel(&self) -> &'static str {
        "cache:invalidate:system_function"
    }

    fn parse_invalidation_payload(&self, payload: &str) -> Option<String> {
        Some(payload.to_owned())
    }
}

/// Three-layer cache for system functions
///
/// Caches per-tenant feature flags with L1 (Moka) + L2 (Redis) + L3 (pluggable backend).
/// Supports cross-instance invalidation via Redis pub/sub.
pub struct SystemFunctionCache<B: SystemFunctionBackend> {
    inner: ThreeLayerCache<
        String,
        SystemFunctions,
        (),
        SystemFunctionFetcher<B>,
        SystemFunctionKeyFormatter,
    >,
}

impl<B: SystemFunctionBackend> Clone for SystemFunctionCache<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: SystemFunctionBackend> SystemFunctionCache<B> {
    /// Create a new SystemFunctionCache with the given backend
    pub async fn new(
        redis_client: redis::Client,
        backend: B,
        config: CacheConfig,
    ) -> Result<Self, redis::RedisError> {
        let backend = Arc::new(backend);

        let inner = ThreeLayerCache::new(
            redis_client,
            (),
            config,
            SystemFunctionFetcher { backend },
            SystemFunctionKeyFormatter,
        )
        .await?;

        Ok(Self { inner })
    }

    /// Get system functions for a tenant, using three-layer caching
    pub async fn get(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Arc<SystemFunctions>>, CacheError> {
        self.inner.get(&tenant_id.to_owned()).await
    }

    /// Invalidate cache for a specific tenant
    pub async fn invalidate(&self, tenant_id: &str) -> Result<(), CacheError> {
        self.inner.invalidate(&tenant_id.to_owned()).await
    }
}
