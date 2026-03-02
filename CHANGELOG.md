# Changelog

## [Unreleased]

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
