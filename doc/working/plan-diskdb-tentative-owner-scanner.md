<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Tentative Owner Scanner Plan

Upstream: [R80](../backlog/R80-diskdb-rebalance.md).

Goal: safely reconcile durable tentative DiskDB blocks through their owning
ChunkDB instance without treating elapsed time as permission to free data.

## Phase 1 — Owner-disposition contract

- [~] **Versioned owner query**: define the exact segment incarnation request
  and extensible `Referenced`/`TaskPending`/`Absent` response; route it to the
  chunk owner and retain blocks on any transient result. Files: protocol,
  ChunkDB service/client, DiskDB scanner client.
- [ ] **Owner source of truth**: derive `Referenced` from current chunk
  metadata and `TaskPending` from a durable repair/relocation task checkpoint.
  Files: ChunkDB lifecycle/task store and tests.

## Phase 2 — DiskDB reconciliation

- [ ] **Busy-block scan pass**: add an independent `BusyBlockOwnerScanner`
  background component (not an extension of the ghost/integrity `ScannerTask`)
  that enumerates only durable BusyBlock records still marked tentative after
  their normal confirm window, queries their owner, idempotently confirms
  referenced blocks, retains pending/transient records, and frees only absent
  ones. The allocation itself has only its BusyBlock record, not a separate
  DiskDB task record. Files: `app/crowdb-diskdb/src/`, allocation persistence,
  metrics.
- [ ] **Deleted-owner grace**: track first unseen time per exact incarnation
  and apply configurable 86,400-second grace only to missing/deleted owners.
  Files: scanner progress persistence, `DdbConfig`, scanner tests.

## Phase 3 — Verification

- [ ] **Fault matrix**: cover referenced, pending, absent, deleted-before/after
  grace, stale incarnation, retryable owner failure, and scanner restart.
  Files: `app/crowdb-diskdb/tests/`, ChunkDB integration fixtures.

## Files

- `app/crowdb-diskdb/src/busy_block_owner_scanner.rs` and `ddb_config.rs`
- `app/crowdb-chunkdb/src/{lifecycle,task,service}/`
- `lib/crowdb-{protocol,chunkdb-client}/`
- `app/crowdb-diskdb/tests/`

## Gates

- `pixi run test-diskdb`
- `pixi run test-chunkdb`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`
