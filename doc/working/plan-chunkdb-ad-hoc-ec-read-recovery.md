<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Ad-hoc EC Read Recovery Plan

Upstream: [R98](../backlog/R98-chunkdb-ad-hoc-ec-read-recovery.md).

Goal: retain slice-level EC recovery for small reads while letting ChunkDB
coalesce, reuse, and durably publish safe full-block reconstructions.

## Phase 1 — Reader policy and request contract

- [ ] **Full-block eligibility**: add explicit full-fragment threshold, memory,
  concurrency, and queue limits to `ChunkReadPolicy`; retain current slice
  decode for smaller ranges. Files: `lib/crowdb-chunk-client/src/chunk/` and
  config/tests.
- [ ] **Routed ad-hoc request**: add FlatBuffers/protocol types for expected
  revision, strip sequence, failed segment incarnation, and operation id;
  implement ChunkDB service and client transport/routing. Files:
  `lib/crowdb-protocol/`, `lib/crowdb-chunkdb-client/`,
  `app/crowdb-chunkdb/src/service/`.

## Phase 2 — ChunkDB operation manager

- [ ] **Validation and coalescing**: implement the bounded in-memory manager
  keyed by exact chunk/strip/segment incarnation; validate current metadata,
  share byte results, and expose no durable task record. Files:
  `app/crowdb-chunkdb/src/repair.rs` or dedicated ad-hoc recovery module,
  metrics, unit tests.
- [ ] **Durable handoff**: attach an accepted operation to deterministic
  `RepairStrip` admission, checkpoint target phases, and reuse the current
  allocate → rebuild/fsync → CAS publish → confirm sequence. Files:
  `app/crowdb-chunkdb/src/{repair,lifecycle,task}/`.

## Phase 3 — Client integration and verification

- [ ] **Foreground integration**: request full-block recovery only when the
  policy permits; otherwise keep marker-and-background repair. Files:
  `lib/crowdb-chunk-client/src/chunk/{chunk_reader,strip_reader}.rs`.
- [ ] **Failure matrix**: cover slice decode, coalescing, stale revisions,
  saturation, pre-CAS crash cleanup, and post-CAS confirmation. Files:
  `lib/crowdb-chunk-client/tests/`, `app/crowdb-chunkdb/tests/`.

## Files

- `lib/crowdb-chunk-client/src/chunk/{chunk_reader,strip_reader}.rs`
- `lib/crowdb-{protocol,chunkdb-client}/`
- `app/crowdb-chunkdb/src/{repair,lifecycle,service,task}/`
- `lib/crowdb-chunk-client/tests/` and `app/crowdb-chunkdb/tests/`

## Gates

- `pixi run test-chunk-client`
- `pixi run test-chunkdb`
- `pixi run rs-fmt -- --check`
- `pixi run cargo clippy -p crowdb-chunk-client -p crowdb-chunkdb --all-targets -- -D warnings`
