<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified File-Backed Service Configuration Plan

Upstream: [`R138`](../backlog/R138-service-rpc-workers-config-file.md),
[`working design`](design-unified-service-config.md), and the component root
designs indexed by [`doc_index.md`](../doc_index.md).

Goal: make all four server RPC worker settings file-backed under one startup
precedence contract and establish a complete diskio TOML schema.

## Phase 1: Contract

- [x] **Research configuration ownership**: trace schemas, CLI overlays,
  listener construction, templates, deploy generation, reload behavior, and
  tests for KV, diskdb, chunkdb, diskio, and reusable libraries. Files: app/,
  lib/, and indexed component designs.
- [x] **Define local and future contracts**: update R138, write the working
  design, and add the deferred group-0 configuration backlog. Files:
  `doc/backlog/R138-service-rpc-workers-config-file.md`,
  `doc/backlog/R139-group0-service-config.md`, `doc/backlog/backlog.md`,
  `doc/working/design-unified-service-config.md`.

## Phase 2: Rust Services

- [~] **Add typed server worker fields**: add defaults and validation to KV,
  diskdb, and chunkdb configs. Files: `lib/crowdb-kv/src/common/config.rs`,
  `app/crowdb-diskdb/src/ddb_config.rs`,
  `app/crowdb-chunkdb/src/chunkdb_config.rs`.
- [ ] **Fix Rust startup precedence**: distinguish absent CLI values and wire
  merged config values into RPC listeners. Files: `app/crowdb-kv-server/src/`,
  `app/crowdb-diskdb/src/main.rs`, `app/crowdb-chunkdb/src/main.rs`.
- [ ] **Add Rust templates and tests**: track all service examples and verify
  defaults, explicit values, invalid zero, and CLI precedence. Files:
  `app/crowdb-{kv-server,diskdb,chunkdb}/conf/` and affected `tests/`.

## Phase 3: Diskio

- [ ] **Implement complete TOML loading**: add toml++, typed section mapping,
  transactional errors, and final validation. Files: `pixi.toml`,
  `app/crowdb-diskio/CMakeLists.txt`, `app/crowdb-diskio/src/dio_config.*`.
- [ ] **Merge CLI over file**: add `--config`, pre-load the file, retain
  explicit CLI override semantics, and update help. Files:
  `app/crowdb-diskio/src/dio_config.*`.
- [ ] **Add diskio template and tests**: exercise full mapping, errors,
  precedence, and tracked template loading. Files:
  `app/crowdb-diskio/conf/crowdb_diskio_config.toml`,
  `app/crowdb-diskio/tests/dio_config_test.cpp`.

## Phase 4: Deployment and Verification

- [ ] **Generate deploy configs**: create/pass diskio config and include
  server workers in generated chunkdb/diskdb files and restart launch specs.
  Files: `lib/crowdb-console-shared/src/lifecycle.rs` and tests.
- [ ] **Run affected tests separately**: run common, KV core/server, diskdb,
  chunkdb, diskio C++, and console-shared tasks through pixi. Files: none.
- [ ] **Review implementation**: inspect the full diff, callers, static reload
  behavior, error paths, and hot-path impact. Files: all changed code/tests.

## Phase 5: Permanent Design and Cleanup

- [ ] **Fold permanent documentation**: add the cross-service configuration
  design, update component designs/index/user guide, and delete the working
  design. Files: `doc/design/config/`, component designs,
  `doc/doc_index.md`, `doc/user-manual/user-guide.md`, working design.
- [ ] **Run final gates**: run Rust fmt/clippy, C++ format/tree-lint, and the
  full test suite through pixi. Files: none.
- [ ] **Close requirement**: delete R138 and this completed plan, remove its
  backlog entry, and commit cleanup separately. Files: `doc/backlog/`,
  `doc/working/`.

## Consolidated File List

- `pixi.toml`
- `lib/crowdb-kv/src/common/config.rs`
- `app/crowdb-kv-server/src/{cli.rs,main.rs,store_registry.rs}`
- `app/crowdb-kv-server/conf/crowdb_kv_server_config.toml`
- `app/crowdb-diskdb/src/{ddb_config.rs,main.rs}`
- `app/crowdb-diskdb/conf/crowdb_diskdb_config.toml`
- `app/crowdb-chunkdb/src/{chunkdb_config.rs,main.rs}`
- `app/crowdb-chunkdb/conf/crowdb_chunkdb_config.toml`
- `app/crowdb-diskio/{CMakeLists.txt,src/dio_config.*,tests/dio_config_test.cpp}`
- `app/crowdb-diskio/conf/crowdb_diskio_config.toml`
- `lib/crowdb-console-shared/src/lifecycle.rs` and affected tests
- `doc/backlog/{backlog.md,R138-*,R139-*}`
- `doc/design/config/design-crowdb-config.md`
- component designs, `doc/doc_index.md`, and `doc/user-manual/user-guide.md`

## Tests

- Unit: config deserialize/default/validation tests in KV, diskdb, chunkdb,
  and diskio.
- Integration: Rust CLI precedence and console generated-config/restart tests.
- E2E: affected server tasks; no new external topology scenario is needed
  because this phase is startup-only.
