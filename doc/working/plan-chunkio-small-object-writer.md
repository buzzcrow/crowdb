<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Small-Object Shared-Chunk Writer Plan

Upstream: [working design](design-chunkio-small-object-writer.md),
[requirement](../backlog/R106-chunkio-small-object-writer.md), and
[chunk IO design](../design/chunkio/design-crowdb-chunkio.md).

Goal: implement bounded small-object aggregation into fenced shared mirror
chunks with independent durable locations and elastic lock-free routing.

## Phase 1: Durable Shared-Chunk Primitives

- [x] **Cursor protocol model**: add writer epoch, acknowledged cursor,
  closed-strip sequence, lease fields, and advance request/response types.
  Files: `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
  `lib/crowdb-protocol/src/fbs/msg_type.fbs`,
  `lib/crowdb-protocol/src/fb_wrappers/chunkdb.rs`.
- [x] **Cursor client transport**: expose and route `advance_chunk_write` through
  crowdb-rpc and the chunk allocator seam. Files:
  `lib/crowdb-chunkdb-client/src/client.rs`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`,
  `lib/crowdb-chunk-client/src/traits.rs`,
  `lib/crowdb-chunk-client/src/client.rs`.
- [x] **Cursor lifecycle**: validate and persist fenced monotonic advances under
  the per-chunk guard; seal expired writer leases at the persisted cursor.
  Files: `app/crowdb-chunkdb/src/lifecycle/handler.rs`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/service.rs`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/mutations.rs`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/wire.rs`,
  `app/crowdb-chunkdb/src/main.rs`.
- [x] **Relative disk writes**: add aligned, bounded segment-relative writes and
  preserve the zero-offset convenience. Files:
  `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`,
  `lib/crowdb-chunk-client/src/disk_io/routing.rs`, affected test writers.
- [x] **Primitive verification**: cover FlatBuffer round trips, client/server
  advance transport, fencing, orphan sealing, and relative-write validation.
  Files: `lib/crowdb-protocol/tests/`, `lib/crowdb-chunkdb-client/tests/`,
  `app/crowdb-chunkdb/tests/`, `lib/crowdb-chunk-client/tests/`.

## Phase 2: Ingress and Shared Pool

- [x] **Policy and errors**: add validated defaults, object-size errors, and the
  validation exception to the writer contract. Files:
  `lib/crowdb-chunk-client/src/config.rs`,
  `lib/crowdb-chunk-client/src/error.rs`,
  `lib/crowdb-chunk-client/src/io.rs`.
- [x] **Atomic metrics**: add small-object, byte, batch, queue, pipeline,
  scaling, and tail-waste accounting with lock-free snapshots. Files:
  `lib/crowdb-chunk-client/src/metrics.rs`.
- [x] **Pool admission and routing**: implement whole-object permits, lazy
  startup, atomic snapshots, bounded MPSC routes, power-of-two selection, and
  stale-route retry. Files:
  `lib/crowdb-chunk-client/src/writer/small_pool.rs`,
  `lib/crowdb-chunk-client/src/writer.rs`.
- [x] **Object ingress state**: implement fragment retention, exact-size
  validation, submit-once completion, empty finish, abort, and terminal calls.
  Files: `lib/crowdb-chunk-client/src/writer/small_object.rs`.
- [x] **Client ownership and API**: make clones share one pool, expose async
  small-write preparation and shutdown, and support parts-based policy tests.
  Files: `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-chunk-client/src/lib.rs`.
- [x] **Ingress verification**: cover fragmented input, overflow/underflow,
  limit/budget rejection, empty/abort behavior, terminal state, whole-budget
  waiting, and clone pool sharing. Files:
  `lib/crowdb-chunk-client/tests/small_object_test.rs`.

## Phase 3: Pipeline and Elasticity

- [x] **Batch assembly**: aggregate whole objects to byte/object/deadline bounds,
  align and zero-pad physical buffers, and retain exact descriptors. Files:
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`.
- [x] **Chunk owner and commit barrier**: allocate Repo mirror chunks, write all
  replicas, advance the fenced cursor, fan out locations, append strips, prepare
  replacements, and seal/delete on rotation or retirement. Files:
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`.
- [x] **Failure fan-out**: unpublish failed workers and complete their batch and
  accepted queue entries exactly once without replay. Files:
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`,
  `lib/crowdb-chunk-client/src/writer/small_pool.rs`.
- [x] **Elastic manager**: implement initialization publication, queue-delay
  scale-out, idle scale-in, cooldown/bounds, receiver-owned close/drain, worker
  failure replacement, and shutdown joins. Files:
  `lib/crowdb-chunk-client/src/writer/small_manager.rs`.
- [x] **Pipeline verification**: cover aggregation, independent locations,
  commit barriers, acknowledged-prefix preservation, strip/chunk rotation,
  oversized-alone and sparse batches, ownership, scale behavior, drain races,
  failures, shutdown, and metrics. Files:
  `lib/crowdb-chunk-client/tests/small_object_test.rs`.
- [x] **E2E shared-object verification**: write several objects through real
  services, query the shared chunk, and read each exact replica range. Files:
  `lib/crowdb-chunk-client/tests/small_object_e2e.rs`.

## Phase 4: Acceptance and Documentation

- [x] **Affected test tasks**: list pixi tasks and run each affected acceptance
  task separately, including the requirement's focused small-object command.
  Files: none.
- [~] **Formal design**: fold the working design into a permanent chunkio
  small-object sub-design and update the documentation index. Files:
  `doc/design/chunkio/design-crowdb-chunkio-small-object-writer.md`,
  `doc/design/chunkio/design-crowdb-chunkio.md`, `doc/doc_index.md`.
- [ ] **Final cleanup**: delete the working design, completed plan, R106 detail,
  and backlog entry in a separate commit. Files:
  `doc/working/design-chunkio-small-object-writer.md`,
  `doc/working/plan-chunkio-small-object-writer.md`,
  `doc/backlog/R106-chunkio-small-object-writer.md`, `doc/backlog/backlog.md`.
- [ ] **Pre-push gate**: run format check, `rs-lint`, and the full ordered test
  suite through pixi. Files: none.

## Consolidated Files

- Protocol: `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
  `lib/crowdb-protocol/src/fbs/msg_type.fbs`,
  `lib/crowdb-protocol/src/fb_wrappers/chunkdb.rs`.
- Chunkdb client/server: `lib/crowdb-chunkdb-client/src/`,
  `app/crowdb-chunkdb/src/lifecycle/`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`,
  `app/crowdb-chunkdb/src/main.rs`.
- Chunk IO: `lib/crowdb-chunk-client/src/{client,config,error,io,metrics,traits}.rs`,
  `lib/crowdb-chunk-client/src/disk_io/`,
  `lib/crowdb-chunk-client/src/writer/`.
- Tests: `lib/crowdb-protocol/tests/`, `lib/crowdb-chunkdb-client/tests/`,
  `app/crowdb-chunkdb/tests/`, `lib/crowdb-chunk-client/tests/`.
- Docs: `doc/design/chunkio/`, `doc/doc_index.md`, `doc/backlog/`,
  `doc/working/`.

## Tests

- Unit: protocol round trips, relative-write validation, ingress state,
  reservations, deterministic routing, stale close, batch bounds, scaling,
  failures, and metrics.
- Integration: fenced lifecycle, orphan sealing, aggregation and locations,
  cursor barrier, rotations, worker drain/replacement, and shutdown.
- E2E: real-service shared mirror write plus exact range readback.
- Gates: focused affected tasks, `pixi run -- cargo fmt --all -- --check`,
  `pixi run rs-lint`, and `pixi run test-suite`.
