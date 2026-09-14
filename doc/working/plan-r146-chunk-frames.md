<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R146 Chunk Frames Plan

Upstream: `doc/backlog/R146-chunk-orphan-sealing.md`.

Goal: move persistent chunk users to public self-validating frames, one
deadline-indexed liveness task, bounded stale-write rejection, and
integrity-aware protection reads.

## Phase 1: Public frame protocol

- [x] **Rust frame codec and locations**: add explicit frame encode/parse/CRC32C
  and single/combined location and subrange helpers. Files:
  `lib/crowdb-protocol/src/frame.rs`, `lib/crowdb-protocol/src/lib.rs`,
  `lib/crowdb-protocol/Cargo.toml`, `lib/crowdb-protocol/tests/frame_test.rs`.
- [x] **C++ frame companion**: expose the identical frame parser and encoder
  to tree and DiskIO C++. Files: protocol C++ header, `lib/crowdb-rpc` CMake
  integration, and focused C++ test.
- [x] **Cross-language vectors**: add a shared valid fixture consumed by Rust
  and C++ plus malformed-frame checks in both codecs. Files: protocol test
  fixtures and consumer tests.

## Phase 2: DiskIO time boundary

- [x] **Wire timestamp and response**: add write creation wall time and
  `OldRequest` through FlatBuffers, Rust wrappers, and C++ generated schema
  callsites. Files: `lib/crowdb-protocol/src/fbs/diskio.fbs`, generated wrapper
  users, `lib/crowdb-diskio-client`.
- [x] **DiskIO stale-write guard**: validate the shared age/skew policy before
  `AlignedWriter::submit_ordered`; add configuration and unit coverage. Files:
  `app/crowdb-diskio/src/dio_config*`, `app/crowdb-diskio/src/rpc/dio_server.*`,
  `app/crowdb-diskio/tests/dio_server_test.cpp`.

## Phase 3: Liveness task

- [~] **Conditional task primitive**: extend KV batch support with compare
  conditions and make task claim/renew conflict-safe. Files:
  `lib/crowdb-kv-client`, `lib/crowdb-kv`, protocol KV wire, and task tests.
- [~] **Deadline-indexed FinalizeChunk task**: add a due-first liveness index,
  creation/renewal batch, owner self-fence, final frame scan and seal/delete.
  Files: `lib/crowdb-protocol/src/key/chunk_task.rs`,
  `app/crowdb-chunkdb/src/task/*`, lifecycle handler, RPC and integration tests.

## Phase 4: Chunk writers and locations

- [~] **Repo writers**: `RepoSmall` writes and reads use one verified frame
  per object and compact physical/logical locations. `RepoLarge` uses
  continuous EC byte packing, frames its partial unit tail before sealing, and
  supports bounded frame range reads. Migrate legacy small-object tests and
  publish shared locations only after durable writes. Files:
  `lib/crowdb-chunk-client/src/writer/*`, client tests.
- [ ] **Stream**: frame journal records and remove duplicate physical checksum
  fields while preserving journal payload semantics. Files:
  `lib/crowdb-chunk-stream/src/*`, stream tests.
- [ ] **Tree**: change default persistent page capacity to 65,502 bytes;
  frame pages, rotate before a page crosses a chunk, and replace scalar
  `ChunkPageRef` metadata with the shared location form. Files:
  `lib/crowdb-tree/src/backend/chunk/*`, C++ tests.

## Phase 5: Verified reads and repair

- [~] **Verified source-aware strip reads**: RepoSmall locations are parsed
  and CRC/chunk-ID verified before payload exposure. Retain source provenance through
  frame verification, attempt a connected target at most three times, and feed
  checksum corruption into mirror/EC fallback and repair. Files:
  `lib/crowdb-chunk-client/src/chunk/strip_reader.rs`, `chunk_reader.rs`,
  `error.rs`, and reader tests.

## Phase 6: Completion

- [ ] **Acceptance and cleanup**: run R146 gates, remove R146 from the backlog
  and this plan, and update permanent design only where architecture changed.
  Files: `doc/backlog/R146-chunk-orphan-sealing.md`,
  `doc/backlog/backlog.md`, `doc/working/plan-r146-chunk-frames.md`.

## Consolidated files

- Protocol: `lib/crowdb-protocol`, `lib/crowdb-rpc/CMakeLists.txt`.
- I/O: `lib/crowdb-diskio-client`, `app/crowdb-diskio`.
- Lifecycle: `lib/crowdb-kv-client`, `lib/crowdb-kv`, `app/crowdb-chunkdb`.
- Users: `lib/crowdb-chunk-client`, `lib/crowdb-chunk-stream`,
  `lib/crowdb-tree`.

## Tests

- Unit: protocol frame/key/wire tests; DiskIO server; task transition tests.
- Integration: chunkdb liveness; repo/stream/tree framed writes; verified
  mirror and EC reads with repair observation.
- E2E: owner crash, chunkdb restart, stale delayed write rejection.
