<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Contributing to CROWDB

Thank you for contributing to CROWDB.

## Development status

CROWDB is under active development at version `0.0.0-dev`. It has not reached
alpha, is not recommended for production, and must be tested with disposable
data. Compatibility is not yet maintained for persisted data, WAL, metadata, or
other on-disk formats. A change may deliberately replace an unreleased format
without migration support when its requirement says so.

The root [VERSION](VERSION) file is the project version source of truth. Cargo,
Pixi, the web package, and lockfiles must match it. Do not bump the version in an
ordinary contribution unless the pull request is explicitly release work.

## Community and security

Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Report vulnerabilities through
[SECURITY.md](SECURITY.md), not a public issue.

## Development environment

CROWDB uses [Pixi](https://pixi.sh) to pin Rust, C++, Node.js, and native build
dependencies. Run builds, tests, linters, and project executables through Pixi.

```bash
# Build C++, the Rust workspace, and the web UI.
pixi run build

# Run the complete local test suite.
pixi run test-suite

# Check version metadata.
pixi run check-version

# Check Rust formatting and lint.
pixi run rs-fmt-check
pixi run rs-lint

# Format and lint changed C++ code.
pixi run tree-fmt
pixi run tree-lint

# Check TypeScript and browser behavior when the UI changes.
pixi run ts-lint
pixi run test-console-ui
```

Playwright uses an installed system browser; do not install a repository-local
browser. See `pixi.toml` for focused component tasks.

## Before writing code

- Search existing requirements, designs, tests, and neighboring components.
- For architectural or externally visible behavior, agree on the requirement or
  design before implementation.
- Preserve the existing authority and recovery model; do not create a local
  fallback that can diverge from Group 0 or another durable authority.
- Do not add a lock to a hot path without discussing contention, ordering,
  progress, and complexity trade-offs.
- Never commit credentials, tokens, private keys, production data, or generated
  runtime directories.
- Add dependencies through the owning package manager and avoid newly published
  versions until they have had time for ecosystem review.

Start documentation work at `doc/doc_index.md`. Permanent architecture belongs
under `doc/design/`, user behavior in `doc/user-manual/user-guide.md`, future
contracts in `doc/backlog/`, and temporary execution plans in `doc/working/`.

## Code and tests

### Rust

- Workspace crates deny unsafe code by default. Keep necessary unsafe code
  confined to the existing FFI and low-level boundaries.
- Put integration tests in each crate's `tests/` directory.
- Put shared integration-test helpers in `tests/common/` and name helper types
  with a `Test` prefix.
- Use structured `tracing` fields and established domain identifiers.
- Follow the existing `foo.rs` plus `foo/` module layout; do not add `mod.rs`.

### C++

- Follow the repository `.clang-format` and `.clang-tidy` configuration.
- Keep public subsystem headers under the matching `include/<library>/` tree and
  private headers with their implementation.
- Add GoogleTest coverage under the owning component's `tests/` directory.

### Web UI

- Follow existing React and TypeScript component patterns.
- Add focused unit tests and update Playwright E2E coverage for visible behavior.
- Verify the real backend path when UI behavior depends on service state.

A bug fix should normally add a failing regression test first, then fix the root
cause. Do not weaken assertions, add retries, disable durability, or bypass
security boundaries to make a test pass.

## Pull requests

1. Create a focused branch from `main`.
2. Keep the change aligned with one requirement or one coherent maintenance
   purpose.
3. Add or update tests and documentation with the implementation.
4. Run the relevant Pixi gates; run the full suite for cross-component changes.
5. Review generated files and the complete diff for secrets and unrelated edits.
6. Open a pull request explaining the problem, design choice, verification, and
   known limitations.

Use concise, single-line commit subjects. Keep unrelated refactors in separate
commits or pull requests. Do not force-push shared branches or bypass hooks and
release gates.

## Changelog and releases

CROWDB begins public change history with its first Docker preview. Before that
baseline, Git history and requirement documents are the development record; do
not fabricate historical releases. Release preparation creates the first
versioned changelog entry. After that baseline, user-visible changes belong
under `Unreleased` and move to a dated section during release.

Only maintainers publish releases. A version or image tag is not a production or
compatibility promise unless the release notes explicitly make that promise.

## License

By contributing, you agree that your contribution is licensed under the Apache
License 2.0.
