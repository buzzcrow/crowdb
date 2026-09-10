<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Stream Plan

Source: [`design-chunk-stream.md`](design-chunk-stream.md) and
[`../backlog/R141-chunk-stream.md`](../backlog/R141-chunk-stream.md).

Goal: provide a fenced, bounded, mirrored logical byte stream for the R142 WAL
without placing rollover metadata on the append hot path.

## Phase 1: Types and Storage Contracts

- [x] Add `crowdb-chunk-stream` to the workspace with typed errors, names,
  bindings, manifests, extent pages, active descriptors, and validation.
- [x] Add injected registry, metadata, and chunk-storage async traits plus
  in-memory `test-util` implementations.
- [x] Add protocol registry/manifest/extent key and value types without WAL
  framing or partition-server dependencies.

## Phase 2: Ordered Append Worker

- [x] Implement bounded request/byte admission, one MPSC worker, immediate
  first-request submission, and bounded drain of already queued requests.
- [x] Implement vectored batch assembly, three-mirror durability, one cursor
  advance, exact per-request ranges, and typed batch failure.
- [x] Resolve ambiguous cursor publication by durable cursor plus checksum and
  stall the writer if neither commit nor absence can be proven.

## Phase 3: Rollover and Recovery

- [x] Allocate/prep one successor, prevent cross-chunk appends, seal the old
  chunk, update one extent page, and generation-fence manifest publication.
- [~] Reopen the highest complete generation, derive the active tail, reject
  stale epochs, and recover sealed-but-unpublished rollover state. Production
  orphan reporting remains open.
- [x] Cover the authoritative generation before publication and the complete
  new generation after publication through generation-CAS tests.

## Phase 4: Read, Trim, and Observation

- [~] Implement logarithmic target-page lookup, checked physical translation,
  and bounded ordered read windows. Production multi-range prefetch/coalescing
  remains open.
- [x] Publish logical trim before bounded idempotent strip cleanup and preserve
  boundary strips.
- [x] Add watchdog observations and metrics without changing completion
  ownership.

## Phase 5: Gates and Documentation

- [x] Run format, clippy, crate tests, chunk-client tests, and server tests
  through `pixi run`.
- [x] Fold the implemented contract into `doc/design/chunkio/` and update
  `doc/doc_index.md`. Keep temporary documents while production issues remain.
- [ ] Remove the completed backlog detail and row in a separate cleanup commit.

## Tests

- Unit: extent validation/lookup, queue bounds, aggregation, rollover limits,
  ambiguous outcomes, trim rounding, fencing, watchdog lifetime, typed errors.
- Integration: registry separation, no per-append metadata write, multi-chunk
  replay, crash recovery, trim/reclaim ordering, out-of-order reads.
- E2E: higher-epoch ownership reopen and exact byte continuity.

## Open Issues

- Production group-0/metadata adapters and the R143 activation lifecycle remain
  open while the injected contracts and core stream state machine are built.
- The chunk-client mirror writer is still a placeholder; production wiring must
  use the fenced cursor APIs without weakening the three-replica contract.
- Production reader integration still needs adjacent-range coalescing and
  bounded out-of-order prefetch over the chunk-reader implementation.
- Orphan enumeration and retained-generation cleanup need production metadata
  watermarks; core reopen already repairs sealed unpublished rollover state.
- Hardware benchmark thresholds and metadata scale-out remain deferred.
