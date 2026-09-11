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
- [x] Reopen the highest complete generation, derive the active tail, reject
  stale epochs, and recover sealed-but-unpublished rollover state with the
  injected stores. Production restart and orphan wiring remains below.
- [x] Cover the authoritative generation before publication and the complete
  new generation after publication through generation-CAS tests.

## Phase 4: Read, Trim, and Observation

- [x] Implement logarithmic target-page lookup, checked physical translation,
  and bounded ordered read windows with the injected store. Production
  multi-range prefetch/coalescing remains below.
- [x] Publish logical trim before bounded idempotent strip cleanup and preserve
  boundary strips.
- [x] Add watchdog observations and metrics without changing completion
  ownership.

## Phase 5: Gates and Documentation

- [x] Run format, clippy, crate tests, chunk-client tests, and server tests
  through `pixi run`.
- [x] Fold the implemented contract into `doc/design/chunkds/` and update
  `doc/doc_index.md`. Keep temporary documents while production issues remain.
- [ ] Remove the completed backlog detail and row in a separate cleanup commit.

## Phase 6: Production Chunk IO and Placement

- [x] **Add the direct mirror chunk writer**: implement a one-chunk
  `MirrorChunkWriter` in `crowdb-chunk-client` that consumes owned `Bytes`,
  appends mirror strips asynchronously, advances the fenced acknowledged
  cursor, and never constructs the EC pipeline. Files:
  `lib/crowdb-chunk-client/src/chunk/`, `lib/crowdb-chunk-client/tests/`.
- [x] **Wire stream chunk storage**: implement the production
  `StreamChunkStore` adapter over `MirrorChunkWriter` and `ChunkReader`, enforce
  the 256-MiB hard chunk limit, and allocate a fresh chunk on every writer
  reopen. Files: `lib/crowdb-chunk-stream/src/`,
  `lib/crowdb-chunk-stream/tests/`.
- [x] **Add configured binding placement**: expose group-0 registry plus an
  explicit metadata-group selection (default group 1), retain readable binding
  keys and binary metadata keys, assemble shared production clients in the
  chunk-KV server, and let R143 drive group creation/activation.
  Files: `lib/crowdb-protocol/src/{chunk_stream.rs,key/chunk_stream.rs}`,
  `lib/crowdb-chunk-stream/src/`, `app/crowdb-chunk-kv-server/src/`.
- [x] **Implement seekable prefetch readers**: add finite and `ToEnd` read
  hints, an 8-MiB default retained buffer, cached-byte delivery concurrent with
  up to eight bounded physical reads, ordered output, EOF, and a
  provenance-aware form yielding the physical chunk for R142 validation. Files:
  `lib/crowdb-chunk-stream/src/`, `lib/crowdb-chunk-stream/tests/`.
- [x] **Add chunk-bound append**: accept R142 body+CRC bytes, append the chosen
  chunk's canonical ID per request after rollover selection, include trailer
  bytes in admission/range accounting, and return the same ID in
  `AppendResult`. Files: `lib/crowdb-chunk-stream/src/`,
  `lib/crowdb-chunk-stream/tests/`.
- [~] **Add stream ownership metadata**: generate time-ordered 128-bit stream
  names, allocate chunks with the stream owner kind/key, report empty and
  unreachable chunk candidates to R146, and keep superseded metadata on its
  own watermark-driven cleanup path. Files: `lib/crowdb-protocol/src/`,
  `lib/crowdb-chunk-client/src/`, `lib/crowdb-chunk-stream/src/`,
  `doc/backlog/R146-chunk-orphan-sealing.md`.
- [~] **Implement metadata publication**: use R101 CAS for the stable
  manifest/head key and fresh immutable versioned COW keys for changed extent
  pages, with logarithmic logical-offset discovery. Cover page rollover,
  takeover in either CAS ordering, crash-created orphan pages, nonzero first
  page indices, and monotonic-epoch stale-writer rejection. Files:
  `lib/crowdb-protocol/src/`,
  `lib/crowdb-chunk-stream/src/`, `lib/crowdb-chunk-stream/tests/`.
- [ ] **Benchmark concurrency and bounds**: use the NullDisk-backed harness for
  concurrent streams/readers, queue saturation, 256-MiB rollover, random seek,
  sequential replay, and prefetch memory. Record selected thresholds in the
  permanent design. Files: `lib/crowdb-chunk-client/src/benchmark.rs`,
  `lib/crowdb-chunk-stream/benches/`, `doc/design/chunkds/`.

## Tests

- Unit: extent validation/lookup, queue bounds, aggregation, rollover limits,
  ambiguous outcomes, trim rounding, fencing, watchdog lifetime, typed errors.
- Integration: registry separation, no per-append metadata write, multi-chunk
  replay, crash recovery, trim/reclaim ordering, out-of-order reads.
- E2E: higher-epoch ownership reopen and exact byte continuity.
