<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Contributing to CROWDB

## Development status

CROWDB is under active development. It has not reached alpha, is not recommended for production, and must be tested with disposable data. Compatibility is not yet maintained for persisted data, WAL, metadata, or other on-disk formats. A change may deliberately replace an unreleased format without migration support when its requirement says so.

The root [VERSION](VERSION) file defines the server version; `pixi run check-version` checks the corresponding manifests and lockfiles. The portable SDK has its own version. Change versions only as part of release work.

## Community and security

Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Report vulnerabilities through [SECURITY.md](SECURITY.md), not a public issue.

## Development environment

CROWDB uses [Pixi](https://pixi.sh) to pin Rust, C++, Node.js, and native build dependencies. Run builds, tests, linters, and project executables through Pixi.

```bash
# Build C++, the Rust workspace, and the web UI.
pixi run build

# Run host test suites.
pixi run test-suite

# Check version metadata.
pixi run check-version

# Check Rust formatting and lint.
pixi run rs-fmt-check
pixi run rs-lint

# Check TypeScript and browser behavior when the UI changes.
pixi run ts-lint
pixi run test-console-ui
```

For changed C++ files, run `clang-format --dry-run --Werror` and `tree-lint` through Pixi, passing the affected file paths. `tree-fmt` rewrites all C++ trees listed in its task; it is not limited to changed files.

Playwright uses an installed system browser; do not install one locally. See [pixi.toml](pixi.toml) for tasks and environments, and [tools/README.md](tools/README.md) for test selection, scripts, and CI coverage.

Container acceptance is a separate Linux amd64 gate requiring Docker:

```sh
pixi run test-single-node-container
```

## Before writing code

- Search existing requirements, designs, tests, and neighboring components.
- For architectural or externally visible behavior, agree on the requirement or design before implementation.
- Preserve the existing authority and recovery model; do not create a local fallback that can diverge from Group 0 or another durable authority.
- Do not add a lock to a hot path without discussing contention, ordering, progress, and complexity trade-offs.
- Never commit credentials, tokens, private keys, production data, or generated runtime directories.
- Add dependencies through the owning package manager and avoid newly published versions until they have had time for ecosystem review.

Public [design articles](doc/design/README.md) explain architecture and advantages in English; update them only for an explicit documentation task. Keep usage instructions in the owning README and public documents self-contained.

## Code and tests

### Rust

- Workspace crates deny unsafe code by default. Keep necessary unsafe code confined to the existing FFI and low-level boundaries.
- Put integration tests in each crate's `tests/` directory.
- Put shared integration-test helpers in `tests/common/` and name helper types with a `Test` prefix.
- Use structured `tracing` fields and established domain identifiers.
- Follow the existing `foo.rs` plus `foo/` module layout; do not add `mod.rs`.

### C++

- Follow the repository `.clang-format` and `.clang-tidy` configuration.
- Keep public subsystem headers under the matching `include/<library>/` tree and private headers with their implementation.
- Add GoogleTest coverage under the owning component's `tests/` directory.

### Web UI

- Follow existing React and TypeScript component patterns.
- Add focused unit tests and update Playwright E2E coverage for visible behavior.
- Verify the real backend path when UI behavior depends on service state.

A bug fix should normally add a failing regression test first, then fix the root cause. Do not weaken assertions, add retries, disable durability, or bypass security boundaries to make a test pass.

## Pull requests

1. Create a focused branch from `main`.
2. Keep the change aligned with one requirement or one coherent maintenance purpose.
3. Add or update tests and affected behavior specs or usage instructions with the implementation. Public architecture overviews are a separate documentation task.
4. Run the relevant Pixi gates; run the full suite for cross-component changes.
5. Review generated files and the complete diff for secrets and unrelated edits.
6. Open a pull request explaining the problem, design choice, verification, and known limitations.

Use concise, single-line commit subjects. Keep unrelated refactors in separate commits or pull requests. Do not force-push shared branches or bypass hooks and release gates.

## Releases

Git history records development changes. Only maintainers publish releases; summarize notable changes and compatibility in the release notes. Version and image tags alone do not promise production readiness or compatibility.

## License

By contributing, you agree that your contribution is licensed under the Apache License 2.0.
