<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Tentative BusyBlock Scanner Plan

Upstream: [R80](../backlog/R80-diskdb-rebalance.md).

Goal: reconcile only durable tentative BusyBlock records with their ChunkDB
owner without creating a second DiskDB task record or treating elapsed time as
cleanup permission.

## Phase 1 — Owner-disposition contract

- [~] **Protocol and routing**: define a versioned exact-segment owner query
  and `Referenced`/`TaskPending`/`Absent` response; route to the ChunkDB owner
  and retain on transient failures. Files: `lib/crowdb-protocol/`,
  `lib/crowdb-chunkdb-client/`, `app/crowdb-chunkdb/src/service/`.
- [ ] **Owner state**: answer `Referenced` from current metadata and
  `TaskPending` from durable repair checkpoints. Files:
  `app/crowdb-chunkdb/src/{lifecycle,task,service}/`, tests.

## Phase 2 — Independent DiskDB scanner

- [ ] **Tentative enumeration**: add `BusyBlockOwnerScanner` as an independent
  `BgRunner` task, not an extension of ghost/integrity `ScannerTask`; enumerate
  tentative BusyBlock records and apply idempotent confirm/retain/free actions.
  Files: `app/crowdb-diskdb/src/`, allocation persistence, metrics.
- [ ] **Grace and restart**: persist first-absent state for each exact
  incarnation, apply the configurable 86,400-second deleted-owner grace, and
  retain on routing/RPC failures. Files: DiskDB persistence/config/tests.

## Phase 3 — Verification

- [ ] **Fault matrix**: cover referenced, task-pending, absent,
  deleted-before/after grace, stale incarnation, transient owner error, and
  scanner restart. Files: `app/crowdb-diskdb/tests/`, ChunkDB fixtures.

## Gates

- `pixi run test-diskdb`
- `pixi run test-chunkdb`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`
