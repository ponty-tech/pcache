use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::collection_cache::CollectionBackend;
use crate::system_function_cache::SystemFunctionBackend;
use crate::tenant_cache::TenantBackend;
use crate::{
    CacheConfig, CollectionCache, SystemFunctionCache, SystemFunctions, TenantCache, TenantInfo,
    shutdown_pubsub_hub,
};

// ============ JSON conversion helpers ============

/// Convert a serde_json::Value to a Python object
fn json_value_to_py(py: Python<'_>, v: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match v {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, val) in obj {
                dict.set_item(k, json_value_to_py(py, val)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Convert a Python dict/None to TenantInfo via JSON serialization
fn py_to_tenant_info(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Option<TenantInfo>> {
    if obj.is_none() {
        return Ok(None);
    }
    let json_mod = py.import("json")?;
    let json_str: String = json_mod.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&json_str)
        .map(Some)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Convert a Python dict/None to SystemFunctions via JSON serialization
fn py_to_system_functions(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<SystemFunctions>> {
    if obj.is_none() {
        return Ok(None);
    }
    let json_mod = py.import("json")?;
    let json_str: String = json_mod.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&json_str)
        .map(Some)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ============ Data Classes ============

/// Tenant information with settings
#[pyclass(name = "TenantInfo")]
#[derive(Clone)]
pub struct PyTenantInfo {
    inner: Arc<TenantInfo>,
}

#[pymethods]
impl PyTenantInfo {
    #[getter]
    fn id(&self) -> &str {
        &self.inner.id
    }

    #[getter]
    fn slug(&self) -> &str {
        &self.inner.slug
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn active(&self) -> bool {
        self.inner.active
    }

    #[getter]
    fn settings(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.inner.settings {
            dict.set_item(k, json_value_to_py(py, v)?)?;
        }
        Ok(dict.into_any().unbind())
    }

    fn get_setting(&self, key: &str) -> Option<String> {
        self.inner.get_setting(key)
    }

    fn get_setting_i64(&self, key: &str) -> Option<i64> {
        self.inner.get_setting_i64(key)
    }

    fn get_setting_bool(&self, key: &str) -> Option<bool> {
        self.inner.get_setting_bool(key)
    }

    fn __repr__(&self) -> String {
        format!(
            "TenantInfo(id='{}', slug='{}', name='{}', active={})",
            self.inner.id, self.inner.slug, self.inner.name, self.inner.active
        )
    }
}

/// System functions (feature flags) for a tenant
#[pyclass(name = "SystemFunctions")]
#[derive(Clone)]
pub struct PySystemFunctions {
    inner: Arc<SystemFunctions>,
}

#[pymethods]
impl PySystemFunctions {
    #[getter]
    fn tenant_id(&self) -> &str {
        &self.inner.tenant_id
    }

    #[getter]
    fn functions(&self) -> HashMap<String, bool> {
        self.inner.functions.clone()
    }

    fn is_active(&self, function_name: &str) -> bool {
        self.inner.is_active(function_name)
    }

    fn __repr__(&self) -> String {
        format!(
            "SystemFunctions(tenant_id='{}', count={})",
            self.inner.tenant_id,
            self.inner.functions.len()
        )
    }
}

/// Cache configuration
#[pyclass(name = "CacheConfig")]
#[derive(Clone)]
pub struct PyCacheConfig {
    inner: CacheConfig,
}

#[pymethods]
impl PyCacheConfig {
    #[new]
    #[pyo3(signature = (l1_max_capacity=1000, l1_ttl_seconds=300, l2_ttl_seconds=900, enable_pubsub=true))]
    fn new(
        l1_max_capacity: u64,
        l1_ttl_seconds: u64,
        l2_ttl_seconds: u64,
        enable_pubsub: bool,
    ) -> Self {
        Self {
            inner: CacheConfig {
                l1_max_capacity,
                l1_ttl: Duration::from_secs(l1_ttl_seconds),
                l2_ttl: Duration::from_secs(l2_ttl_seconds),
                enable_pubsub,
            },
        }
    }
}

// ============ Backend Wrappers ============

/// Wraps a Python object that implements the TenantBackend protocol:
///   async def fetch_by_id(self, id: str) -> dict | None
///   async def fetch_by_slug(self, slug: str) -> dict | None
struct PyTenantBackendWrapper {
    py_backend: Py<PyAny>,
    event_loop: Py<PyAny>,
}

/// Run a Python async method from a tokio context.
///
/// Schedules the coroutine on the captured asyncio event loop via
/// `asyncio.run_coroutine_threadsafe` and blocks (releasing the GIL)
/// until the result is available.
async fn call_python_async(
    py_obj: Py<PyAny>,
    method: &str,
    arg: String,
    event_loop: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let method = method.to_owned();

    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let coro = py_obj.call_method1(py, method.as_str(), (&arg,))?;
            let asyncio = py.import("asyncio")?;
            let cf = asyncio.call_method1(
                "run_coroutine_threadsafe",
                (coro.bind(py), event_loop.bind(py)),
            )?;
            // cf.result() blocks, releasing the GIL during the wait
            cf.call_method1("result", (30.0,))
                .map(|r| r.unbind())
        })
    })
    .await
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
}

/// Run a Python async method that takes no arguments from a tokio context.
///
/// Like `call_python_async` but for zero-argument methods (e.g., `fetch_all()`).
async fn call_python_async_no_args(
    py_obj: Py<PyAny>,
    method: &str,
    event_loop: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let method = method.to_owned();

    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let coro = py_obj.call_method0(py, method.as_str())?;
            let asyncio = py.import("asyncio")?;
            let cf = asyncio.call_method1(
                "run_coroutine_threadsafe",
                (coro.bind(py), event_loop.bind(py)),
            )?;
            cf.call_method1("result", (30.0,))
                .map(|r| r.unbind())
        })
    })
    .await
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
}

/// Convert a Python object (dict, list, None, etc.) to serde_json::Value
fn py_to_json_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Option<serde_json::Value>> {
    if obj.is_none() {
        return Ok(None);
    }
    let json_mod = py.import("json")?;
    let json_str: String = json_mod.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&json_str)
        .map(Some)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[async_trait::async_trait]
impl TenantBackend for PyTenantBackendWrapper {
    async fn fetch_by_id(
        &self,
        id: &str,
    ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>> {
        let (backend, event_loop) = Python::attach(|py| {
            (
                self.py_backend.clone_ref(py),
                self.event_loop.clone_ref(py),
            )
        });
        let result = call_python_async(backend, "fetch_by_id", id.to_owned(), event_loop).await?;
        Python::attach(|py| py_to_tenant_info(py, result.bind(py)))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn fetch_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<TenantInfo>, Box<dyn std::error::Error + Send + Sync>> {
        let (backend, event_loop) = Python::attach(|py| {
            (
                self.py_backend.clone_ref(py),
                self.event_loop.clone_ref(py),
            )
        });
        let result =
            call_python_async(backend, "fetch_by_slug", slug.to_owned(), event_loop).await?;
        Python::attach(|py| py_to_tenant_info(py, result.bind(py)))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Wraps a Python object that implements the SystemFunctionBackend protocol:
///   async def fetch(self, tenant_id: str) -> dict | None
struct PySysFnBackendWrapper {
    py_backend: Py<PyAny>,
    event_loop: Py<PyAny>,
}

#[async_trait::async_trait]
impl SystemFunctionBackend for PySysFnBackendWrapper {
    async fn fetch(
        &self,
        tenant_id: &str,
    ) -> Result<Option<SystemFunctions>, Box<dyn std::error::Error + Send + Sync>> {
        let (backend, event_loop) = Python::attach(|py| {
            (
                self.py_backend.clone_ref(py),
                self.event_loop.clone_ref(py),
            )
        });
        let result =
            call_python_async(backend, "fetch", tenant_id.to_owned(), event_loop).await?;
        Python::attach(|py| py_to_system_functions(py, result.bind(py)))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

// ============ Cache Classes ============

/// Three-layer tenant cache with dual-key lookup (by ID and slug)
#[pyclass(name = "TenantCache")]
pub struct PyTenantCache {
    inner: TenantCache<PyTenantBackendWrapper>,
}

#[pymethods]
impl PyTenantCache {
    /// Create a new TenantCache.
    ///
    /// Args:
    ///     redis_url: Redis connection URL (e.g. "redis://localhost:6379")
    ///     backend: Python object with async fetch_by_id(id) and fetch_by_slug(slug) methods
    ///     config: CacheConfig instance
    #[classmethod]
    fn create<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        redis_url: String,
        backend: Py<PyAny>,
        config: PyCacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        let event_loop = py
            .import("asyncio")?
            .call_method0("get_running_loop")?
            .unbind();

        future_into_py(py, async move {
            let redis_client = redis::Client::open(redis_url.as_str())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let wrapper = PyTenantBackendWrapper {
                py_backend: backend,
                event_loop,
            };

            let cache = TenantCache::new(redis_client, wrapper, config.inner)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(PyTenantCache { inner: cache })
        })
    }

    /// Get tenant by ID, returns TenantInfo or None
    fn get_by_id<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            match cache.get_by_id(&id).await {
                Ok(Some(info)) => Ok(Some(PyTenantInfo { inner: info })),
                Ok(None) => Ok(None),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    /// Get tenant by slug, returns TenantInfo or None
    fn get_by_slug<'py>(&self, py: Python<'py>, slug: String) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            match cache.get_by_slug(&slug).await {
                Ok(Some(info)) => Ok(Some(PyTenantInfo { inner: info })),
                Ok(None) => Ok(None),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    /// Check if a tenant is active by ID
    fn is_tenant_active<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            cache
                .is_tenant_active(&id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Invalidate tenant cache by ID
    fn invalidate_by_id<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            cache
                .invalidate_by_id(&id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Invalidate tenant cache by slug
    fn invalidate_by_slug<'py>(
        &self,
        py: Python<'py>,
        slug: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            cache
                .invalidate_by_slug(&slug)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Invalidate tenant cache by both ID and slug
    fn invalidate<'py>(
        &self,
        py: Python<'py>,
        id: String,
        slug: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            cache
                .invalidate(&id, &slug)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

/// Three-layer system function cache
#[pyclass(name = "SystemFunctionCache")]
pub struct PySystemFunctionCache {
    inner: SystemFunctionCache<PySysFnBackendWrapper>,
}

#[pymethods]
impl PySystemFunctionCache {
    /// Create a new SystemFunctionCache.
    ///
    /// Args:
    ///     redis_url: Redis connection URL (e.g. "redis://localhost:6379")
    ///     backend: Python object with async fetch(tenant_id) method
    ///     config: CacheConfig instance
    #[classmethod]
    fn create<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        redis_url: String,
        backend: Py<PyAny>,
        config: PyCacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        let event_loop = py
            .import("asyncio")?
            .call_method0("get_running_loop")?
            .unbind();

        future_into_py(py, async move {
            let redis_client = redis::Client::open(redis_url.as_str())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let wrapper = PySysFnBackendWrapper {
                py_backend: backend,
                event_loop,
            };

            let cache = SystemFunctionCache::new(redis_client, wrapper, config.inner)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(PySystemFunctionCache { inner: cache })
        })
    }

    /// Get system functions for a tenant, returns SystemFunctions or None
    fn get<'py>(&self, py: Python<'py>, tenant_id: String) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            match cache.get(&tenant_id).await {
                Ok(Some(functions)) => Ok(Some(PySystemFunctions { inner: functions })),
                Ok(None) => Ok(None),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    /// Invalidate system function cache for a tenant
    fn invalidate<'py>(
        &self,
        py: Python<'py>,
        tenant_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            cache
                .invalidate(&tenant_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

/// Wraps a Python object that implements the CollectionBackend protocol:
///   async def fetch_all(self) -> Any | None
struct PyCollectionBackendWrapper {
    py_backend: Py<PyAny>,
    event_loop: Py<PyAny>,
}

#[async_trait::async_trait]
impl CollectionBackend for PyCollectionBackendWrapper {
    type Value = serde_json::Value;

    async fn fetch_all(
        &self,
    ) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let (backend, event_loop) = Python::attach(|py| {
            (
                self.py_backend.clone_ref(py),
                self.event_loop.clone_ref(py),
            )
        });
        let result = call_python_async_no_args(backend, "fetch_all", event_loop).await?;
        Python::attach(|py| py_to_json_value(py, result.bind(py)))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Three-layer collection cache for bulk values
#[pyclass(name = "CollectionCache")]
pub struct PyCollectionCache {
    inner: CollectionCache<PyCollectionBackendWrapper>,
}

#[pymethods]
impl PyCollectionCache {
    /// Create a new CollectionCache.
    ///
    /// Args:
    ///     redis_url: Redis connection URL (e.g. "redis://localhost:6379")
    ///     backend: Python object with async fetch_all() method
    ///     config: CacheConfig instance
    ///     redis_key: Redis key for L2 storage (e.g. "cache:collection:tenant_settings")
    ///     invalidation_channel: Redis pub/sub channel for cross-instance invalidation
    #[classmethod]
    fn create<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        redis_url: String,
        backend: Py<PyAny>,
        config: PyCacheConfig,
        redis_key: String,
        invalidation_channel: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let event_loop = py
            .import("asyncio")?
            .call_method0("get_running_loop")?
            .unbind();

        future_into_py(py, async move {
            let redis_client = redis::Client::open(redis_url.as_str())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let wrapper = PyCollectionBackendWrapper {
                py_backend: backend,
                event_loop,
            };

            // Leak the strings to get &'static str required by KeyFormatter.
            // Cache instances are created once at app startup and live for
            // the lifetime of the process, so this is intentional.
            let redis_key: &'static str = Box::leak(redis_key.into_boxed_str());
            let invalidation_channel: &'static str =
                Box::leak(invalidation_channel.into_boxed_str());

            let cache = CollectionCache::new(
                redis_client,
                wrapper,
                config.inner,
                redis_key,
                invalidation_channel,
            )
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(PyCollectionCache { inner: cache })
        })
    }

    /// Get the cached collection value, returns a Python object or None
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            match cache.get().await {
                Ok(Some(value)) => Python::attach(|py| json_value_to_py(py, &value))
                    .map(Some)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                Ok(None) => Ok(None),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    /// Invalidate the cached collection across all layers and instances
    fn invalidate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cache = self.inner.clone();
        future_into_py(py, async move {
            cache
                .invalidate()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

// ============ Module Functions ============

/// Gracefully shutdown the Redis pub/sub hub.
/// Call this during application shutdown.
#[pyfunction]
fn shutdown() {
    shutdown_pubsub_hub();
}

// ============ Module Registration ============

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTenantInfo>()?;
    m.add_class::<PySystemFunctions>()?;
    m.add_class::<PyCacheConfig>()?;
    m.add_class::<PyTenantCache>()?;
    m.add_class::<PySystemFunctionCache>()?;
    m.add_class::<PyCollectionCache>()?;
    m.add_function(wrap_pyfunction!(shutdown, m)?)?;
    Ok(())
}
