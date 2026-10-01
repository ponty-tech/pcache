# Changelog

## [Unreleased]

### Security
- Bump `pyo3` and `pyo3-async-runtimes` from 0.27 to 0.29 (RUSTSEC-2026-0176, RUSTSEC-2026-0177), `crossbeam-epoch` to 0.9.21 (RUSTSEC-2026-0204) and `event-listener` to 5.4.2 (RUSTSEC-2026-0221). Python classes keep their `FromPyObject` behaviour through an explicit `from_py_object`.

## [Unreleased]

### Added
- `CacheConfig::negative_l1` (Python: `negative_l1=False`): opt-in in-memory negative caching, so a lookup of a missing key no longer costs a Redis round-trip while `negative_ttl` lasts. Adds a field to `CacheConfig`, so Rust callers that build it with a struct literal must set it.

### Changed
- Bump `redis` from 1.0.3 to 1.0.4
- Bump `futures` from 0.3.31 to 0.3.32
- Bump `actions/download-artifact` from 4 to 7
- Bump `actions/checkout` from 4 to 6
- Bump `actions/setup-python` from 5 to 6
- Bump `actions/upload-artifact` from 4 to 6
- Bump `google-github-actions/auth` from 2 to 3

## [0.5.2] - 2026-03-02

### Fixed
- Downgrade PubSubHub reconnection failure log from `error!` to `warn!` to avoid unnecessary Sentry alerts on transient Redis outages. The hub retries every 30 seconds indefinitely, so a single failure is not actionable.

## [0.5.1] - 2026-02-21

### Added
- Bridge Rust `tracing` logs to Python `logging` via `pyo3-log` when the `python` feature is enabled.

## [0.5.0] - 2026-02-19

### Added
- `ClientCache` for caching OAuth2 client lookups.
- `UserInfoCache` for caching per-account user info.
- Shared `ConnectionManager` to reuse a single Redis connection across all cache instances.
- Dependabot for cargo, pip, and GitHub Actions dependencies.

### Fixed
- Handle poisoned mutex in `InFlightGuard::Drop` to ensure cleanup even after panics.
