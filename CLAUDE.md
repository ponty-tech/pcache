# pcache

Three-layer caching library (L1: moka, L2: Redis, L3: pluggable backend) with Redis pub/sub cross-instance invalidation.

## Releasing

When releasing a new version, **all three steps** are required:

1. Bump version in `Cargo.toml`
2. Bump version in `pyproject.toml`
3. Create a git tag matching the version (e.g., `git tag v0.3.4`)

Both version files must stay in sync. The tag triggers the GitHub Actions workflow to build and publish to Google Artifact Registry.
