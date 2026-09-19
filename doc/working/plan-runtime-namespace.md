<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified Runtime Namespace Plan

Upstream: [R176](../backlog/R176-runtime-namespace.md)

Goal: make one namespace own stable ports and all generated paths for every
multi-process test or local cluster, then reduce cleanup to explicit lifecycle
roots without using the system temporary directory.

## Phase 0: Full-stack failure baseline

- [x] **Split S3 full-stack reporting**: keep environment setup outside the
  count, expose the eight boto3 functions plus restart and benchmark cases,
  correct phase names, and refactor orchestration below lint limits. Files:
  `app/crowdb-access-server/Cargo.toml`, S3 full-stack test and Python scripts.
- [x] **Prove restart identity bug**: reproduce stale DiskDB/chunk-KV
  endpoints, make harness restart reuse existing config, identity, paths, and
  ports, and pass all 17 named cases. Files: DiskDB and chunk-KV harnesses.

## Phase 1: Namespace core

- [~] **Define runtime hierarchy**: expose the repository-local
  `.crowdb-runtime/{ephemeral,persistent,artifacts,ports}` roots and a versioned
  namespace manifest with per-service directories. Files: protocol/test-harness
  runtime modules and tests.
- [ ] **Make claims owner-aware**: move tests to the workspace-global registry,
  record namespace owner plus process-start identity, atomically reclaim dead
  ephemeral claims, and retain persistent claims. Files: protocol port
  allocator and tests.
- [ ] **Add stable assignments**: map `ServicePort` plus logical instance to one
  persisted port, detect conflicting owners, and make restart lookup allocation
  free. Files: protocol namespace and tests.

## Phase 2: Shared process harness

- [ ] **Namespace KV clusters**: give `KvCluster` one namespace and place each
  node's data, config, and logs below its service identity; preserve assignment
  across crash/restart. Files: test-harness cluster and tests.
- [ ] **Namespace storage services**: migrate DiskDB, DiskIO, ChunkDB, and
  chunk-KV constructors to namespace assignments and service roots; distinguish
  start, restart, and replacement. Files: test-harness service modules.
- [ ] **Migrate harness consumers**: update process-spawning tests in KV server,
  DiskDB, ChunkDB, access-server, console-shared, CLI, and web; retain port zero
  only when an in-process listener owns the bound socket. Files: Rust E2E tests.

## Phase 3: Persistent and local deployments

- [ ] **Persist console assignments**: store namespace identity and port map in
  local cluster records; stop preserves them, restart validates/reuses them,
  and deletion or explicit release removes them. Files: console-shared cluster
  and S3 lifecycle, CLI adapters, tests.
- [ ] **Unify default local paths**: migrate console, web, benchmarks, CLI logs,
  and local service defaults from `runtime-data`, `log`, `cli-log`, and
  `temp-data` into the appropriate runtime namespace class. Files: console/web
  config and deployment modules, benchmark scripts and tests.

## Phase 4: Non-Rust producers and cleanup

- [ ] **Migrate C++ test paths**: route tree, RPC, common, and DiskIO test data
  and logs through the workspace runtime root; remove hard-coded system-temp
  paths while keeping failed-run diagnostics. Files: C++ test helpers and
  runners.
- [ ] **Migrate tools**: replace sanitizer, profiling, regression, and helper
  `/tmp` files with namespaced workspace artifacts. Files: `tools/` scripts and
  pixi task environment.
- [ ] **Consolidate ignore rules**: ignore `.crowdb-runtime/` and remove obsolete
  generated-path entries only after their producers are migrated. Files:
  `.gitignore`.
- [ ] **Make cleanup manifest-driven**: make `clean-env` terminate recorded
  ephemeral processes and remove ephemeral/stale resources; make `clean`
  preserve persistent namespaces and remove filename-pattern and `/tmp` sweeps.
  Files: `pixi.toml`, cleanup helper and tests.

## Phase 5: Design, gates, and closure

- [ ] **Update permanent design**: document runtime ownership, stable restart,
  persistent conflict behavior, workspace locality, and cleanup safety as
  current architecture. Files: test strategy and console design.
- [ ] **Run acceptance and quality gates**: protocol, harness, console, CLI,
  web, named boto3 suite, clean behavior, fmt, clippy, and `rs-lint`; investigate
  failures from first divergence under the requirement retry rules.
- [ ] **Close requirement**: remove the backlog entry, R176 detail, and this
  plan after all acceptance evidence passes.

## Files

- `lib/crowdb-protocol/src/port/`
- `lib/crowdb-test-harness/src/`
- process-spawning tests under `app/*/tests/` and `lib/*/tests/`
- `lib/crowdb-console-shared/src/`
- `app/crowdb-cli/`, `app/crowdb-web/`
- C++ test helpers under `lib/` and `app/crowdb-diskio/`
- `tools/`, `pixi.toml`, `.gitignore`
- permanent test and console designs

## Tests

- Unit: manifest round-trip, owner liveness, stale-claim reclamation, assignment
  stability, conflict reporting, path classification.
- Integration: concurrent namespace exclusion, service restart identity,
  successful cleanup versus failed-run preservation, persistent survival across
  ordinary clean.
- E2E: parallel process-spawning suites, S3 17-case full stack, persistent CLI
  stop/restart CRUD, and cleanup tasks with seeded ephemeral/persistent roots.
