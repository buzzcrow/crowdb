<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Regression Benchmark Layout Plan

Upstream: `doc/design/console/design-crowdb-console.md`,
`doc/design/kv/design-crowdb-kv-server.md`

Goal: give every regression benchmark the same inspectable file layout,
reuse compatible clusters safely, and retain complete service metrics.

## Phase 1: Deployment layout

- [x] **Stable CLI invocation directories**: prefix every CLI-created log
  directory with `cli-` and cover the naming contract with tests. Files:
  `app/crowdb-cli/src/main.rs`, `app/crowdb-cli/tests/`.
- [x] **One-rack combined topology**: keep the default three-node combined
  deployment in rack 1 and remove implicit benchmark rack reassignment. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`, tests.
- [x] **Per-server roots**: place KV, DiskDB, ChunkDB, and DiskIO beneath stable
  server directories under each node; keep KV `waldata/` and `ctdata/` directly
  under its server root. Files: `lib/crowdb-console-shared/src/ops/cluster.rs`,
  `lib/crowdb-console-shared/src/lifecycle.rs`, tests.
- [x] **Stable service names**: use `chunkdb-1..N` for directory/server names
  while retaining internal instance IDs in service metadata. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`, tests.
- [x] **Document deployment contract**: specify stable server roots, PID usage,
  one-rack defaults, and config lifetime. Files:
  `doc/design/console/design-crowdb-console.md`.

## Phase 2: Regression lifecycle and layout

- [x] **Common regression helpers**: centralize run-root creation, persistent
  `console.toml`, CLI invocation, teardown, and log assertions. Files:
  `tools/bench-regression-common.sh` and regression scripts.
- [x] **Unify retained artifacts**: migrate KV read/write/scan, RPC, DiskDB,
  ChunkDB, ChunkIO, and disk/chunk sentinels to the common run layout. Files:
  `tools/bench-*-regression.sh`.
- [x] **Share compatible clusters**: deploy once for cases with identical
  server tunables, clean between cases, and redeploy only when deploy-time
  settings change. Files: regression scripts.
- [x] **Full-stack reset boundary**: add clean/restart orchestration for KV,
  DiskDB, ChunkDB, and DiskIO so cached in-memory state cannot leak between
  cases. Files: console shared operations, CLI cluster commands, server
  lifecycle code, tests.
- [x] **Config lifetime**: retain the run-root `console.toml` through teardown
  as diagnostic state and ensure commands do not place it inside an invocation
  directory. Files: common helper, regression scripts, documentation.

## Phase 3: Metrics completeness

- [x] **Service metric wiring**: make DiskDB and ChunkDB flush Rust plus C++ RPC
  counters and make DiskIO flush service/RPC counters in addition to system
  metrics. Files: server metric runners and C++/Rust metric bridges.
- [x] **Content-aware gates**: require expected metric sections and counters,
  not merely non-empty files, for every participating client/server. Files:
  common helper and regression scripts.
- [x] **Empty auxiliary logs**: determine whether zero-byte RPC/stdout logs are
  useful; avoid creating them when disabled or document their lifecycle.
  Files: shared logging/lifecycle code and tests.

## Phase 4: Verification and cleanup

- [x] **Unit tests**: cover CLI slug naming, stable server paths, service IDs,
  and clean/restart state transitions.
- [x] **Integration tests**: deploy the local combined stack, inspect its tree,
  run two cleaned workloads on one cluster, and verify all metric sections.
- [x] **Regression smoke tests**: run short cases for every sentinel through
  `pixi run` and inspect retained structures.
- [x] **Quality gates**: run Rust formatting, clippy, affected tests, shell
  syntax checks, and `git diff --check` through the required tooling.
- [~] **Finish documentation**: reconcile the permanent design with verified
  behavior and delete this working plan once every item is complete.

## Files

- `app/crowdb-cli/src/main.rs`
- `app/crowdb-cli/src/commands/cluster.rs`
- `app/crowdb-cli/tests/`
- `lib/crowdb-console-shared/src/lifecycle.rs`
- `lib/crowdb-console-shared/src/ops/cluster.rs`
- `lib/crowdb-console-shared/tests/`
- `doc/design/console/design-crowdb-console.md`
- `tools/bench-regression-common.sh`
- `tools/bench-*-regression.sh`

## Tests

### Unit

- CLI invocation slug and directory-name tests.
- Local deployment server-root and stable-ID tests.
- Service clean/restart transition tests.

### Integration

- Combined deployment tree and one-rack topology.
- Two full-stack workload cases sharing one clean cluster.
- Rust, C++ RPC, storage, and system metric content validation.

### E2E

- Short smoke execution of each regression sentinel with retained artifact
  inspection.
