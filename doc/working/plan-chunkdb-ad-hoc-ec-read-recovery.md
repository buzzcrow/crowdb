<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Ad-hoc EC Read Recovery Plan

Upstream: [R171](../backlog/R171-chunkdb-ad-hoc-ec-read-recovery.md),
[ChunkIO Reader](../design/chunkio/design-crowdb-chunkio-reader.md), and
[ChunkDB repair](../design/chunkdb/design-crowdb-chunkdb.md#73-physical-validation-and-degraded-placement-repair).

Goal: preserve small-range EC slice recovery while allowing bounded, routed,
full-fragment recovery to reuse the existing fenced `RepairStrip` publication
flow.

## Review Findings

- `ChunkReader` already records exact failed `Segment` identities before it
  returns reconstructed bytes. `StripReader` reconstructs only the requested
  shard-relative range under a client-wide scratch-memory semaphore, so the
  small-read acceptance invariant has an existing implementation and test
  seam.
- `RepairCoordinator` deterministically admits marker-backed `RepairStrip`
  tasks. `RepairStripTaskHandler` already checkpoints one tentative target,
  writes and fsyncs it, CAS-publishes the exact old strip, and confirms that
  target after publication. R171 must attach to this task identity rather than
  introduce another durable task or target allocation path.
- ChunkDB's current RPC surface carries metadata only. R171 needs a versioned,
  chunk-routed request and a response that can carry a rebuilt fragment to
  waiting readers before the repair task publishes it. The new operation must
  validate its expected revision, strip sequence, and exact segment incarnation
  on the owner before it joins or starts work.
- Existing `repair` configuration bounds only durable background work. The
  client policy and a server-local manager need separate, explicit admission,
  memory, waiter, and retained-result bounds. The manager can use an async
  shared-result primitive; it must not add a hot-path lock or become a durable
  ownership authority.

## Decisions

- Client ad-hoc recovery is a core v1 feature. The client tracks matching
  partial recovery failures by `(chunk, revision, strip, failed segment
  incarnation)`, with bounded retained bytes and in-flight entries. At a
  1-MiB request or half of the fragment size, whichever is smaller, it submits
  strip-level recovery to ChunkDB. A complete-fragment request submits
  immediately. All same-key client callers await the same shared future and
  receive the same rebuilt bytes.
- ChunkDB coalesces cluster-wide strip recovery using the same key. Its
  default is 32 concurrent jobs; it owns a separate bounded reconstructed-byte
  budget and shares one result future with every same-key routed request.
- A repair marker and `RepairStrip` task are created only after a confirmed
  integrity failure: returned data cannot be parsed, or a written frame fails
  checksum verification. Network, timeout, unavailable, and unknown I/O
  errors do not mark a segment corrupt and must not trigger repair admission.
  They retain ordinary read error/retry behavior.
- The client recovery result is returned as soon as reconstruction completes;
  ChunkDB continues the existing durable target checkpoint, CAS publication,
  and confirmation flow independently. Process-local futures and bytes are
  never recovery authority after a crash.

- DiskDB owns the physical corruption state. R171 adds a fenced
  `MarkBlockCorrupt` operation whose precondition is the exact `Segment`
  incarnation; ChunkDB invokes it only after verified frame corruption, before
  it records `unavailable_segments` and admits `RepairStrip`.
- The client process budget is 256 MiB. ChunkDB retains the proposed 512 MiB
  decode/result budget. Both reject new ad-hoc recovery when saturated rather
  than queueing it indefinitely or marking a block corrupt.

## Implementation

- [x] **Resolve recovery policy**: use client-side matching-failure
  accumulation and same-future coalescing; submit at 1 MiB or half-fragment,
  use 32 default ChunkDB jobs, and admit repair only after verified corruption.
  Files: this plan, `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `app/crowdb-chunkdb/src/chunkdb_config.rs`.
- [~] **Define routed recovery protocol**: add versioned request/result types,
  FlatBuffer tables, wire conversion, client transport, and typed stale/healed/
  insufficient-shard outcomes. Files: `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
  `lib/crowdb-protocol/src/types/chunkdb.rs`, `lib/crowdb-chunkdb-client/`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`.
- [ ] **Mark verified-corrupt blocks**: if selected, add a fenced DiskDB block
  state mutation and invoke it only after frame parsing or write-frame checksum
  proves corruption; then persist the ChunkDB unavailable marker and admit
  `RepairStrip`. Files: `lib/crowdb-protocol/`, `lib/crowdb-diskdb-client/`,
  `app/crowdb-diskdb/`, `app/crowdb-chunkdb/`.
- [ ] **Add client eligibility and fallback**: track bounded matching failures
  and shared futures, send eligible full-fragment recovery at the configured
  threshold, return shared bytes to waiters, and preserve normal read handling
  on saturation or non-integrity I/O failure. Files:
  `lib/crowdb-chunk-client/src/chunk/`.
- [ ] **Add ChunkDB coalescing manager**: key in-flight work by chunk, strip,
  and failed segment incarnation; reuse the existing `RepairStrip` checkpoint
  and publication sequence without a second allocation. Files:
  `app/crowdb-chunkdb/src/repair.rs`, new focused recovery-manager module,
  `app/crowdb-chunkdb/src/main.rs`.
- [ ] **Expose metrics**: add aggregate counters for slice fallback,
  full-block starts, coalesced waiters, reused bytes, limit rejection, stale
  requests, publication result, and background fallback. Files:
  `lib/crowdb-chunk-client/src/metrics.rs`, `app/crowdb-chunkdb/src/metrics.rs`.
- [ ] **Verify acceptance cases**: cover small slice non-amplification,
  client and cluster duplicate coalescing, threshold submission, integrity-only
  corruption marking, early-byte full rebuild, stale rejection, saturation
  fallback, and both crash boundaries. Files:
  `lib/crowdb-chunk-client/tests/`, `app/crowdb-chunkdb/tests/`.

## Files

- `doc/working/plan-chunkdb-ad-hoc-ec-read-recovery.md`
- `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`
- `lib/crowdb-chunk-client/src/metrics.rs`
- `lib/crowdb-protocol/src/fbs/chunkdb.fbs`
- `lib/crowdb-protocol/src/types/chunkdb.rs`
- `lib/crowdb-chunkdb-client/src/`
- `app/crowdb-chunkdb/src/repair.rs`
- `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`
- `app/crowdb-chunkdb/src/chunkdb_config.rs`
- `app/crowdb-chunkdb/src/metrics.rs`
- focused Chunk Client and ChunkDB integration/E2E tests

## Tests

- Unit/integration: `pixi run test-chunk-client`, `pixi run test-chunkdb`.
- E2E: targeted Chunk Client and ChunkDB recovery tests, prefixed with
  `pixi run clean-env &&` when they start services.
- Gates: `pixi run rs-fmt -- --check` and
  `pixi run cargo clippy -p crowdb-chunk-client -p crowdb-chunkdb --all-targets -- -D warnings`.
