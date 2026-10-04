<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Durable split abort and recovery fencing Plan

Upstream: [R209](../backlog/R209-chunk-kv-split-recovery-fencing.md),
[`design-crowdb-chunk-kv.md`](../design/chunkds/design-crowdb-chunk-kv.md),
[`design-crowdb-chunk-kv-server.md`](../design/chunkds/design-crowdb-chunk-kv-server.md)

Goal: make pre-publication split abort cleanup restart-safe and keep child
artifacts from becoming authority.

## Phase 1 — local cleanup contract

- [x] **Abort cleanup API**: add one idempotent storage/service operation that
  clears parent ingress, retires a prepared child, and releases its exact pin;
  use it for both live abort and recovery. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `app/crowdb-chunk-kv-server/src/serving/worker.rs`,
  `app/crowdb-chunk-kv-server/src/serving/transition_runtime.rs`.
- [x] **Partition-local epoch rule**: enforce monotonic epochs for an existing
  partition ID and reject recycled IDs at split planning/publication boundaries.
  Files: `lib/crowdb-chunk-kv/src/types.rs`,
  `lib/crowdb-protocol/src/chunk_kv.rs`.

## Phase 2 — durable recovery

- [x] **Abort/catalog proof**: ensure cleanup only runs when the catalog still
  proves non-publication; committed splits follow completion and never rollback.
  Files: `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/catalog.rs`,
  `app/crowdb-chunk-kv-server/src/catalog/transition.rs`.
- [x] **Restart reconciliation**: recover every split phase and make repeated
  cleanup/commit idempotent. Files:
  `app/crowdb-chunk-kv-server/src/serving/transition_runtime.rs`,
  `app/crowdb-chunk-kv-server/src/control_store.rs`.

## Phase 3 — verification

- [x] **Recovery tests**: reran split recovery, transition worker, catalog,
  and server acceptance suites covering durable phase recovery and cleanup.
- [x] **Gates**: ran affected crate tests, `pixi run rs-fmt-check`, and
  `pixi run rs-lint`.
