//! Generic three-layer caching system
//!
//! This module provides a reusable three-tier caching strategy:
//! - L1: In-memory Moka cache (fastest)
//! - L2: Redis cache (medium speed, shared across instances)
//! - L3: Pluggable backend (database, HTTP, etc.)
//!
//! The cache supports:
//! - Automatic fallback between layers
//! - Redis pub/sub for cache invalidation across instances
//! - Configurable TTLs and capacity
//! - Generic key and value types

use futures::StreamExt;
use moka::future::Cache;
use redis::AsyncCommands;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    sync::{Arc, OnceLock},
    thread,
    time::{Duration, Instant},
};

use tokio::runtime::Builder;
use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{debug, error, warn};

use crate::{CacheConfig, CacheError};

type Callback = Arc<dyn Fn(Option<String>) + Send + Sync + 'static>;

enum ControlMsg {
    Register {
        channel: &'static str,
        callback: Box<dyn Fn(Option<String>) + Send + Sync + 'static>,
    },
}

/// Shared hub for Redis pub/sub using a single connection that can subscribe to multiple channels.
/// Individual caches register callbacks for their specific invalidation channels.
struct PubSubHub {
    tx: mpsc::UnboundedSender<ControlMsg>,
    _handle: std::thread::JoinHandle<()>,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
}

/// Public helper to shutdown hub if initialized (used by test cleanup)
pub fn shutdown_pubsub_hub() {
    if let Some(hub) = PUBSUB_HUB.get() {
        hub.shutdown();
    }
}

static PUBSUB_HUB: OnceLock<PubSubHub> = OnceLock::new();

impl PubSubHub {
    /// Gracefully signal shutdown and join background thread
    pub fn shutdown(&self) {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Start the hub background task and return a handle
    fn start(redis_client: redis::Client) -> PubSubHub {
        let (tx, mut rx) = mpsc::unbounded_channel::<ControlMsg>();

        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_flag_thread = Arc::clone(&shutdown_flag);
        let handle = thread::spawn(move || {
            match Builder::new_current_thread().enable_all().build() {
                Ok(rt) => {
                    rt.block_on(async move {
                        let mut pubsub = match redis_client.get_async_pubsub().await {
                            Ok(c) => c,
                            Err(e) => {
                                error!(
                                    "PubSubHub: failed to create Redis pubsub connection: {}",
                                    e
                                );
                                return;
                            }
                        };

                        let mut callbacks: HashMap<&'static str, Vec<Callback>> = HashMap::new();
                        let mut subscribed: HashSet<&'static str> = HashSet::new();

                        // Health / reconnection state
                        let mut last_health_check = Instant::now();
                        let health_interval = Duration::from_secs(30);

                        while !shutdown_flag_thread.load(std::sync::atomic::Ordering::SeqCst) {
                            // Drain registration queue
                            while let Ok(cmd) = rx.try_recv() {
                                let ControlMsg::Register { channel, callback } = cmd;
                                {
                                    let entry = callbacks.entry(channel).or_default();
                                    let cb_arc: Callback =
                                        Arc::new(move |p: Option<String>| (callback)(p));
                                    entry.push(cb_arc);

                                    if !subscribed.contains(channel) {
                                        // Exponential backoff on initial subscribe failures
                                        let mut attempt = 0u32;
                                        let max_attempts = 5u32;
                                        let mut delay_ms = 100u64;
                                        while attempt < max_attempts {
                                            match pubsub.subscribe(channel).await {
                                                Ok(()) => {
                                                    debug!(
                                                        "PubSubHub: subscribed to channel {}",
                                                        channel
                                                    );
                                                    subscribed.insert(channel);
                                                    break;
                                                }
                                                Err(e) => {
                                                    attempt += 1;
                                                    warn!(
                                                        "PubSubHub: subscribe attempt {} failed for {}: {}",
                                                        attempt, channel, e
                                                    );
                                                    if attempt == max_attempts {
                                                        warn!(
                                                            "PubSubHub: initial subscribe failed for channel {} after {} attempts, will retry during health check",
                                                            channel, max_attempts
                                                        );
                                                        break;
                                                    }
                                                    tokio::time::sleep(Duration::from_millis(
                                                        delay_ms,
                                                    ))
                                                    .await;
                                                    delay_ms = (delay_ms.saturating_mul(2)).min(2000);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Periodic health check & automatic re-subscribe
                            if last_health_check.elapsed() >= health_interval {
                                last_health_check = Instant::now();

                                let needs_reconnect =
                                    match redis::aio::ConnectionManager::new(redis_client.clone())
                                        .await
                                    {
                                        Ok(mut conn) => {
                                            match redis::cmd("PING")
                                                .query_async::<String>(&mut conn)
                                                .await
                                            {
                                                Ok(_) => false,
                                                Err(e) => {
                                                    warn!(
                                                        "PubSubHub: PING failed ({}); attempting reconnection",
                                                        e
                                                    );
                                                    true
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "PubSubHub: could not create connection manager for health check: {}",
                                                e
                                            );
                                            true
                                        }
                                    };

                                if needs_reconnect {
                                    match redis_client.get_async_pubsub().await {
                                        Ok(new_pubsub) => {
                                            pubsub = new_pubsub;
                                            // New connection has no subscriptions; clear so
                                            // they're re-established by the loop below.
                                            subscribed.clear();
                                        }
                                        Err(e) => {
                                            error!(
                                                "PubSubHub: reconnection failed: {}",
                                                e
                                            );
                                        }
                                    }
                                }

                                // Subscribe any channels that have callbacks but aren't
                                // subscribed. Handles both initial subscription failures
                                // and post-reconnection recovery.
                                let pending: Vec<&'static str> = callbacks
                                    .keys()
                                    .filter(|ch| !subscribed.contains(*ch))
                                    .copied()
                                    .collect();

                                for channel in pending {
                                    match pubsub.subscribe(channel).await {
                                        Ok(()) => {
                                            debug!(
                                                "PubSubHub: subscribed to channel {}",
                                                channel
                                            );
                                            subscribed.insert(channel);
                                        }
                                        Err(e) => {
                                            warn!(
                                                "PubSubHub: health check failed to subscribe to {}: {}",
                                                channel, e
                                            );
                                        }
                                    }
                                }
                            }

                            // Poll for messages
                            let poll_deadline = Instant::now() + Duration::from_millis(250);
                            loop {
                                if let Some(msg) = pubsub.on_message().next().await {
                                    match msg.get_payload::<String>() {
                                        Ok(payload) => {
                                            let ch_name = msg.get_channel_name();
                                            if let Some(listeners) = callbacks.get(ch_name) {
                                                for cb in listeners {
                                                    cb(Some(payload.clone()));
                                                }
                                            } else {
                                                debug!(
                                                    "PubSubHub: no listeners for channel {}",
                                                    ch_name
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "PubSubHub: failed to decode pub/sub payload: {}",
                                                e
                                            );
                                        }
                                    }
                                } else {
                                    tokio::time::sleep(Duration::from_millis(25)).await;
                                }
                                if Instant::now() >= poll_deadline {
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("PubSubHub: failed to build dedicated runtime: {}", e);
                }
            }
        });

        PubSubHub {
            tx,
            _handle: handle,
            shutdown_flag,
        }
    }

    /// Register a channel-specific callback
    fn register_channel(
        &self,
        channel: &'static str,
        callback: Box<dyn Fn(Option<String>) + Send + Sync + 'static>,
    ) {
        let _ = self.tx.send(ControlMsg::Register { channel, callback });
    }
}

/// Trait for types that can be cached
pub trait Cacheable: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}
impl<T> Cacheable for T where T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

/// Trait for cache key types
pub trait CacheKey: Display + Hash + Eq + Clone + Send + Sync + 'static {}
impl<T> CacheKey for T where T: Display + Hash + Eq + Clone + Send + Sync + 'static {}

/// Trait for fetching data from the L3 backend.
///
/// The context type `C` allows different backends:
/// - For MySQL: `C = sqlx::MySqlPool`
/// - For HTTP: `C = reqwest::Client` or a custom HTTP context
///
/// The error type `E` should be convertible to `Box<dyn Error + Send + Sync>`.
#[async_trait::async_trait]
pub trait DataFetcher<K, V, C>: Send + Sync {
    /// Fetch a value from the backend by key
    async fn fetch(
        &self,
        ctx: &C,
        key: &K,
    ) -> Result<Option<V>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for generating Redis cache keys and parsing invalidation payloads
pub trait KeyFormatter<K>: Send + Sync + 'static {
    /// Build the L2 cache key for Redis
    fn format_key(&self, key: &K) -> String;

    /// The Redis pub/sub channel for invalidations
    fn invalidation_channel(&self) -> &'static str;

    /// Parse an invalidation payload into the key type.
    fn parse_invalidation_payload(&self, _payload: &str) -> Option<K> {
        None
    }
}

/// Generic three-layer cache
pub struct ThreeLayerCache<K, V, C, F, KF>
where
    K: CacheKey,
    V: Cacheable,
    C: Send + Sync + 'static,
    F: DataFetcher<K, V, C>,
    KF: KeyFormatter<K>,
{
    inner: Arc<ThreeLayerCacheInner<K, V, C>>,
    fetcher: Arc<F>,
    key_formatter: Arc<KF>,
}

/// Represents an in-flight L3 fetch that other requests can wait on
type InFlightFetch<V> = tokio::sync::watch::Receiver<Option<Result<Option<Arc<V>>, String>>>;
type InFlightSender<V> = tokio::sync::watch::Sender<Option<Result<Option<Arc<V>>, String>>>;

/// Guard that ensures in-flight entries are cleaned up even on panic/cancel.
///
/// When dropped, removes the key from the in-flight map and notifies waiters
/// with an error if no result was sent.
struct InFlightGuard<K: CacheKey, V: Cacheable> {
    key: K,
    in_flight: Arc<RwLock<HashMap<K, InFlightFetch<V>>>>,
    tx: Option<InFlightSender<V>>,
}

impl<K: CacheKey, V: Cacheable> InFlightGuard<K, V> {
    fn new(
        key: K,
        in_flight: Arc<RwLock<HashMap<K, InFlightFetch<V>>>>,
        tx: InFlightSender<V>,
    ) -> Self {
        Self {
            key,
            in_flight,
            tx: Some(tx),
        }
    }

    /// Complete the fetch with a result, consuming the guard.
    fn complete(mut self, result: Result<Option<Arc<V>>, String>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(result));
        }
    }
}

impl<K: CacheKey, V: Cacheable> Drop for InFlightGuard<K, V> {
    fn drop(&mut self) {
        // If tx is still Some, we're being dropped without calling complete()
        // This means a panic or cancellation occurred
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(Err("Fetch was cancelled or panicked".to_owned())));
        }

        // Always clean up the in-flight entry
        // Use try_write to avoid blocking in drop; if we can't get the lock,
        // spawn a task to clean up asynchronously
        let key = self.key.clone();
        let in_flight = Arc::clone(&self.in_flight);

        tokio::spawn(async move {
            let mut guard = in_flight.write().await;
            guard.remove(&key);
        });
    }
}

struct ThreeLayerCacheInner<K, V, C>
where
    K: CacheKey,
    V: Cacheable,
    C: Send + Sync + 'static,
{
    l1_cache: Cache<K, Arc<V>>,
    redis: redis::aio::ConnectionManager,
    backend_ctx: C,
    config: CacheConfig,
    invalidation_tx: broadcast::Sender<K>,
    /// Track in-flight L3 fetches for request coalescing
    in_flight: Arc<RwLock<HashMap<K, InFlightFetch<V>>>>,
}

impl<K, V, C, F, KF> Clone for ThreeLayerCache<K, V, C, F, KF>
where
    K: CacheKey,
    V: Cacheable,
    C: Send + Sync + 'static,
    F: DataFetcher<K, V, C>,
    KF: KeyFormatter<K>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            fetcher: Arc::clone(&self.fetcher),
            key_formatter: Arc::clone(&self.key_formatter),
        }
    }
}

impl<K, V, C, F, KF> ThreeLayerCache<K, V, C, F, KF>
where
    K: CacheKey,
    V: Cacheable,
    C: Send + Sync + 'static,
    F: DataFetcher<K, V, C>,
    KF: KeyFormatter<K>,
{
    /// Create a new three-layer cache
    pub async fn new(
        redis_client: redis::Client,
        backend_ctx: C,
        config: CacheConfig,
        fetcher: F,
        key_formatter: KF,
    ) -> Result<Self, redis::RedisError> {
        let l1_cache = Cache::builder()
            .max_capacity(config.l1_max_capacity)
            .time_to_live(config.l1_ttl)
            .build();

        let (invalidation_tx, _) = broadcast::channel(100);
        let redis_manager = redis::aio::ConnectionManager::new(redis_client.clone()).await?;

        let cache = Self {
            inner: Arc::new(ThreeLayerCacheInner {
                l1_cache,
                redis: redis_manager,
                backend_ctx,
                config: config.clone(),
                invalidation_tx,
                in_flight: Arc::new(RwLock::new(HashMap::new())),
            }),
            fetcher: Arc::new(fetcher),
            key_formatter: Arc::new(key_formatter),
        };

        // Start pub/sub listener if enabled
        if config.enable_pubsub {
            cache.register_pubsub(&redis_client);
        }

        Ok(cache)
    }

    /// Register this cache with the shared pub/sub hub
    fn register_pubsub(&self, redis_client: &redis::Client) {
        PUBSUB_HUB
            .get_or_init(|| PubSubHub::start(redis_client.clone()))
            .register_channel(self.key_formatter.invalidation_channel(), {
                let l1_cache = self.inner.l1_cache.clone();
                let key_formatter = Arc::clone(&self.key_formatter);
                // Spawn a background thread for local invalidations
                {
                    let l1_cache_local = l1_cache.clone();
                    let mut local_rx = self.inner.invalidation_tx.subscribe();
                    thread::spawn(move || {
                        if let Ok(rt) = Builder::new_current_thread().enable_all().build() {
                            rt.block_on(async move {
                                while let Ok(key) = local_rx.recv().await {
                                    debug!("Local invalidation for key: {}", key);
                                    l1_cache_local.invalidate(&key).await;
                                }
                            });
                        } else {
                            error!("Failed to build runtime for local invalidation thread");
                        }
                    });
                }
                Box::new(move |payload: Option<String>| {
                    let l1_cache = l1_cache.clone();
                    let key_formatter = Arc::clone(&key_formatter);
                    if let Some(pl) = payload {
                        debug!(
                            "Cache invalidation received on channel '{}': {}",
                            key_formatter.invalidation_channel(),
                            pl
                        );
                        if let Some(k) = key_formatter.parse_invalidation_payload(&pl) {
                            tokio::spawn({
                                let l1_cache = l1_cache;
                                let k = k;
                                async move {
                                    l1_cache.invalidate(&k).await;
                                    debug!("L1 cache invalidated for key: {}", k);
                                }
                            });
                        } else {
                            warn!(
                                "Unable to parse invalidation payload into cache key: {}",
                                pl
                            );
                        }
                    }
                })
            });
    }

    /// Get value by key using three-layer caching
    pub async fn get(&self, key: &K) -> Result<Option<Arc<V>>, CacheError> {
        // Try L1 cache first
        if let Some(value) = self.inner.l1_cache.get(key).await {
            debug!("Cache hit L1 for key: {}", key);
            return Ok(Some(value));
        }

        debug!("Cache miss L1 for key: {}", key);

        // Try L2 (Redis) cache
        let redis_key = self.key_formatter.format_key(key);
        let mut redis_conn = self.inner.redis.clone();

        let cached: Option<String> = match redis_conn.get::<_, Option<String>>(&redis_key).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "L2 (Redis) GET error for key {} ({}). Falling back to backend.",
                    key, e
                );
                None
            }
        };

        if let Some(json) = cached {
            debug!("Cache hit L2 for key: {}", key);
            match serde_json::from_str::<V>(&json) {
                Ok(value) => {
                    let arc_value = Arc::new(value);
                    // Populate L1 cache
                    self.inner
                        .l1_cache
                        .insert(key.clone(), Arc::clone(&arc_value))
                        .await;
                    return Ok(Some(arc_value));
                }
                Err(e) => {
                    warn!(
                        "Failed to deserialize cached value for key {}: {}. Deleting corrupt entry.",
                        key, e
                    );
                    if let Err(del_err) = redis_conn.del::<_, ()>(&redis_key).await {
                        warn!(
                            "Failed to delete corrupt L2 entry for key {} (Redis key: {}): {}",
                            key, redis_key, del_err
                        );
                    }
                }
            }
        } else {
            debug!("Cache miss L2 for key: {}", key);
        }

        // Check if there's already an in-flight fetch for this key
        {
            let in_flight = self.inner.in_flight.read().await;
            if let Some(rx) = in_flight.get(key) {
                let mut rx = rx.clone();
                drop(in_flight); // Release the read lock

                debug!("Waiting for in-flight L3 fetch for key: {}", key);

                // Wait for the in-flight fetch to complete
                loop {
                    if let Some(result) = rx.borrow().as_ref() {
                        return match result {
                            Ok(value) => Ok(value.clone()),
                            Err(e) => Err(CacheError::Backend(e.clone().into())),
                        };
                    }
                    if rx.changed().await.is_err() {
                        // Sender dropped without sending - treat as error
                        return Err(CacheError::Backend("In-flight fetch was cancelled".into()));
                    }
                }
            }
        }

        // No in-flight fetch, start one with request coalescing
        let (tx, rx) = tokio::sync::watch::channel(None);

        // Register this fetch as in-flight
        let guard = {
            let mut in_flight = self.inner.in_flight.write().await;
            // Double-check: another task might have started a fetch while we waited for the write lock
            if let Some(existing_rx) = in_flight.get(key) {
                let mut rx = existing_rx.clone();
                drop(in_flight);

                debug!("Waiting for in-flight L3 fetch for key (race): {}", key);
                loop {
                    if let Some(result) = rx.borrow().as_ref() {
                        return match result {
                            Ok(value) => Ok(value.clone()),
                            Err(e) => Err(CacheError::Backend(e.clone().into())),
                        };
                    }
                    if rx.changed().await.is_err() {
                        return Err(CacheError::Backend("In-flight fetch was cancelled".into()));
                    }
                }
            }
            in_flight.insert(key.clone(), rx);
            InFlightGuard::new(key.clone(), Arc::clone(&self.inner.in_flight), tx)
        };

        // Fetch from L3 (backend)
        // The guard ensures cleanup happens even on panic/cancellation
        let result = match self.fetcher.fetch(&self.inner.backend_ctx, key).await {
            Ok(Some(value)) => {
                debug!("Cache miss - fetched from backend for key: {}", key);

                // Serialize once for Redis
                let json = serde_json::to_string(&value)?;

                // Populate L2 (Redis) - best effort
                let ttl_seconds = self.inner.config.l2_ttl.as_secs();
                if let Err(e) = redis_conn
                    .set_ex::<_, _, ()>(&redis_key, json, ttl_seconds)
                    .await
                {
                    warn!(
                        "L2 (Redis) SETEX error for key {} (Redis key: {}): {}. Continuing.",
                        key, redis_key, e
                    );
                }

                // Populate L1
                let arc_value = Arc::new(value);
                self.inner
                    .l1_cache
                    .insert(key.clone(), Arc::clone(&arc_value))
                    .await;

                Ok(Some(arc_value))
            }
            Ok(None) => {
                debug!("Value not found for key: {}", key);
                Ok(None)
            }
            Err(e) => {
                error!("Backend error for key {}: {}", key, e);
                Err(CacheError::Backend(e))
            }
        };

        // Complete the guard with the result - this notifies waiters and cleans up
        guard.complete(match &result {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(e.to_string()),
        });

        result
    }

    /// Invalidate cache for a specific key
    pub async fn invalidate(&self, key: &K) -> Result<(), CacheError> {
        debug!("Invalidating cache for key: {}", key);

        // Invalidate L1 cache
        self.inner.l1_cache.invalidate(key).await;

        // Invalidate L2 cache (Redis) - best effort
        let redis_key = self.key_formatter.format_key(key);
        let mut conn = self.inner.redis.clone();
        if let Err(e) = conn.del::<_, ()>(&redis_key).await {
            warn!(
                "L2 (Redis) DEL error for key {} (Redis key: {}): {}. Continuing.",
                key, redis_key, e
            );
        }

        // Publish invalidation message to other instances - best effort
        if self.inner.config.enable_pubsub {
            let channel = self.key_formatter.invalidation_channel();
            let payload = key.to_string();

            if let Err(e) = conn.publish::<_, _, ()>(channel, &payload).await {
                warn!(
                    "Redis PUBLISH error on channel {} for key {}: {}. Continuing.",
                    channel, key, e
                );
            } else {
                debug!(
                    "Cache invalidation published on channel '{}': {}",
                    channel, payload
                );
            }
        }

        // Notify local listeners (non-blocking)
        let _ = self.inner.invalidation_tx.send(key.clone());

        Ok(())
    }
}
