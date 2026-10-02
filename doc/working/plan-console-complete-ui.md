<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Complete Console UI Plan

Upstream: [R203](../backlog/R203-console-complete-ui.md).
Goal: five domains with explicit scope, real operations/diagnostics, persistent
standalone bootstrap, and the same UI in Container with topology writes disabled.

## Baseline and scope

- User authorized implementation after spec; record decisions needing review in
  the backlog rather than interrupting the user's six-hour absence.
- Preserve the user's running localhost cluster and its default directory.
  Browser inspection is read-only; tests must use isolated runtime roots/ports.
- Initial Iceberg scope is Namespace/Table metadata CRUD. Row DML and object-to-
  Chunk reverse lookup remain human decisions in R203; other work proceeds.
- Existing startup/persistence edits are uncommitted and need integration/lint
  verification; three initial startup tests passed before the last scope changes.

## Phase 1 — startup and shell

- [x] **Standalone recovery**: finish atomic local persistence and process recovery,
  validate Group 0 authority and multi-node/DiskDB behavior, retain configured
  process identity. Files: `app/crowdb-web/src/{main,state,standalone}.rs`,
  `src/standalone/recovery.rs`, `src/diskdb/lifecycle.rs`, `src/mgmt/cluster_init.rs`,
  `tests/standalone_startup_test.rs`. Acceptance: A1–A5.
- [ ] **Five-domain shell**: typed domain identities, per-domain scope and status,
  health/empty/error distinction, header and pane reuse. Files: `ui/src/App.tsx`,
  `types/index.ts`, `contexts/`, `shell/`, `views/`. Acceptance: A1, A6, A10, A23.
- [ ] **KV/Cluster boundaries**: Cluster layout stops at physical services; KV left
  tree and init/operator scope own logical resources; guard Group 0 writes and
  demo. Files: `App.tsx`, `topology/`, `panels/KvOperatorPanel.tsx`, Web KV routes.
  Acceptance: A7–A9, A22–A23.

## Phase 2 — Chunk diagnostics

- [ ] **Typed Chunk queries**: connect routed ListChunks/detail to Web, explicit
  prefix/paging and placement enrichment. Files: chunkdb client, Web Chunk routes,
  console shared ops. Acceptance: A12, A14.
- [ ] **Chunk view**: list/scope sidebar, actual types and stable Strip sequence,
  Mirror/EC fragment cards, bounded rendering and cross-domain links. Files:
  `views/ChunkView.tsx`, new Chunk domain components/types/API. Acceptance: A13–A15.

## Phase 3 — native Access operations

- [ ] **Access adapter**: discovered endpoint, capability/role handling, typed
  Iceberg REST and signed S3 requests, bounded transfer and error semantics.
  Files: new Web access domain modules, shared access operations, UI API/types.
  Acceptance: A16–A20.
- [ ] **Iceberg UI**: Namespace/Table tree, metadata forms, Schema/Snapshots/Files,
  truthful capability gaps, explicit scope and demo resources. Files: new Iceberg
  view/domain components and tests. Acceptance: A16–A17, A23.
- [ ] **S3 UI**: Bucket/prefix tree, paged object CRUD, bounded preview and streamed
  upload/download, multipart/cancel state and demo. Files: new S3 domain view/
  components/tests. Acceptance: A18–A20, A23.

## Phase 4 — managed deployment and verification

- [ ] **Container integration**: convert confirmed managed snapshots to shared UI
  models, server-enforced capability boundaries and real logical/data operations.
  Files: Web managed routes, UI mode loader, Container web acceptance.
  Acceptance: A11, A21.
- [ ] **Focused gates and commits**: measure affected baseline E2E specs before
  changes; run isolated acceptance, Rust fmt/clippy, TS lint and affected E2E;
  commit coherent verified tasks. Preserve live persistent services.
- [ ] **Permanent design and cleanup**: update Console/UI architecture for delivered
  scope. Remove R203/index/plan only after its full accepted scope is delivered.

## Verification

- Unit: scope transitions, prefix/type parsing, Strip layout identity, capability
  combination, safe error/value presentation, request bodies.
- Integration: `standalone_startup_test`, `managed_mode_test`, DiskDB lifecycle,
  Chunk query adapters, Access protocol/credential/streaming paths.
- E2E: shell/embedding, Cluster physical boundary, KV init/operator, Chunk scope/
  placement, Iceberg and S3 functions, Container read-only hardware with writable
  authorized data. Use existing installed system browser.
- Commands are recorded in R203; append actual outcomes here per phase. No full
  UI suite unless specifically needed/authorized; focused specs first.

## Results

- Focused standalone/managed/DiskDB startup verification: 10 passed, including
  three-member Group 0 recovery, DiskDB replay with preserved custom ports, and
  stale local Rack cache replacement from Group 0. Invalid test profile corrected
  from `fast-test` to existing `e2e`; built missing DiskDB binary before rerun.
- Shell baseline on installed `/usr/bin/microsoft-edge`: embedding 1.6s,
  domain transition 0.372s, Docker writes 0.377s; all 5 tests passed. An initial
  attempt pointed at absent Google Chrome; no browser installation was needed.

- Initial `pixi run cargo test -p crowdb-web --test standalone_startup_test --
  --nocapture`: 3 passed; later sealed-intent/DiskDB recovery edits still need
  focused verification.
- Running UI inspected: Cluster duplicates logical Store/Group/Replica under KV
  Server; KV already uses logical navigation; Chunk currently repeats capacity
  topology. Preserve existing successful operations while unifying these scopes.
