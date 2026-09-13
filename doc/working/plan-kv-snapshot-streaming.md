<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV New-Member Snapshot Streaming Plan

Upstream requirement: `doc/backlog/R151-kv-snapshot-streaming.md`

Goal: bootstrap an unpublished new member through bounded, resumable snapshot
chunks and remove the whole-buffer peer snapshot path.

## Phase 1 — Engine session foundation

- [x] **Stream portable export encoding**: encode the existing portable tuple
  format incrementally from a stable crowdb-tree snapshot, calculate total
  length and final CRC without materializing the byte stream, and retain only
  one output chunk. Files: `lib/crowdb-tree/include/crowdb-tree/snapshot/snapshot_io.h`,
  `lib/crowdb-tree/src/snapshot/snapshot_io.cpp`,
  `lib/crowdb-tree/tests/integration/snapshot_export_test.cpp`.
- [x] **Parse portable imports incrementally**: parse arbitrary feed
  boundaries with rolling CRC, bounded field carry, truncation/trailing-data
  rejection, and delayed engine installation at finish. Files:
  `lib/crowdb-tree/include/crowdb-tree/snapshot/snapshot_io.h`,
  `lib/crowdb-tree/src/snapshot/snapshot_io.cpp`,
  `lib/crowdb-tree/tests/integration/snapshot_export_test.cpp`.
- [~] **Extend the C session API**: add export metadata getters and an
  offset-aware next operation; make import state explicit and keep both end
  functions null-safe/idempotent at the Rust ownership boundary. Relevant
  symbols: `ct_snapshot_export_begin`, `ct_snapshot_export_next`,
  `ct_snapshot_export_end`, `ct_snapshot_import_begin`,
  `ct_snapshot_import_feed`, `ct_snapshot_import_finish`,
  `ct_snapshot_import_end`. Files: `lib/crowdb-tree/include/crowdb-tree/c_api.h`,
  `lib/crowdb-tree/src/c_api.cpp`.
- [ ] **Add Rust RAII owners**: introduce `SnapshotExportSession` and
  `SnapshotImportSession` in crowdb-tree FFI. Export exposes `at_slot`, total
  bytes, final CRC32C, chunk bytes, current offset, and `read(offset)`;
  import exposes `feed`, `finish`, and `abort`, with `Drop` closing unfinished
  handles. The safe wrapper must not expose raw handle aliasing. Files:
  `lib/crowdb-tree/ffi/src/sys.rs`, `lib/crowdb-tree/ffi/src/snapshot.rs`,
  `lib/crowdb-tree/ffi/tests/ffi_test.rs`.
- [ ] **Define engine-neutral sessions**: replace whole-buffer
  `KVEngine::snapshot_export` and `snapshot_import` with object-safe begin
  methods returning non-clone session owners and shared metadata/error types.
  Implement deterministic sessions for `InMemKV` and `CrowdbTreeEngine`; keep
  temporary whole-buffer helpers only inside migration tests. Files:
  `lib/crowdb-kv/src/kv/kv_engine.rs`,
  `lib/crowdb-kv/src/kv/in_mem_kv.rs`,
  `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs`,
  `lib/crowdb-kv/tests/kv_test/conformance_test.rs`.

## Phase 2 — Snapshot RPC protocol and source registry

- [ ] **Define bounded unary messages**: replace the two legacy snapshot
  message types with Begin, Read, Finish, and Abort pairs. Use two `u64`
  fields for boot nonce/session number; carry group, engine-format tag,
  source slot/term/epoch, chunk limit, total length, final CRC, echoed offset,
  payload CRC, end marker, and typed session/offset/backpressure errors. Run
  the repository's FlatBuffer generation task and update safe wrappers. Files:
  `lib/crowdb-protocol/src/fbs/msg_type.fbs`,
  `lib/crowdb-protocol/src/fbs/kv_consensus.fbs`, generated bindings,
  `lib/crowdb-protocol/src/fb_wrappers/kv_consensus.rs`, protocol tests.
- [ ] **Add validated limits**: add snapshot chunk bytes, source-session
  capacity, idle lease, and receiver restart budget to the existing static KV
  configuration. Defaults: 1 MiB, four sessions, 30 seconds, and three fresh
  Begin attempts. Reject zero values and chunk sizes above 1 MiB so responses
  remain below the fixed 4 MiB RPC ceiling. Files:
  `lib/crowdb-kv/src/common/config.rs`, server configuration plumbing,
  configuration fixtures and tests.
- [ ] **Build source session actors**: create one bounded actor per admitted
  export so the non-clone engine session never crosses concurrent calls.
  Guard total capacity with owned permits and index sessions by boot nonce plus
  monotonic counter. Store immutable metadata, current offset, last response,
  and lease deadline. Files: `lib/crowdb-kv/src/rpc/snapshot_registry.rs`,
  `lib/crowdb-kv/src/rpc/px_rpc_service.rs`, registry unit tests.
- [ ] **Serve session lifecycle RPCs**: Begin captures engine metadata and
  starts the actor; Read accepts current offset or exact previous retry;
  Read and Finish expire the session if the source membership epoch changed;
  Finish otherwise requires the final offset; Abort and Finish are idempotent.
  Reap idle sessions and shut all actors down with the service. Keep registry
  work only on snapshot message handlers. Files: snapshot registry,
  `lib/crowdb-kv/src/rpc/px_rpc_service.rs`, RPC service tests.
- [ ] **Instrument the source**: register active/capacity-rejected/expired/
  aborted/completed sessions, encoded bytes/chunks, exact retries, integrity
  failures, and export latency through existing metric handles. Files:
  snapshot registry/service and metrics tests.

## Phase 3 — Receiver and unpublished join commit

- [ ] **Add typed pull transport**: implement Begin, Read, Finish, and Abort
  calls over `PxRpcTransport`'s existing connection pool. Preserve the session
  identity across a connection generation change and map expired identity,
  invalid offset, integrity failure, and backpressure separately. Files:
  `lib/crowdb-kv/src/rpc/px_rpc_transport.rs`, transport tests.
- [ ] **Stream into the importer**: change `PxGroup::join_via_snapshot` to
  validate and feed one payload at a time, advance its local offset only after
  successful feed, retry the same offset after disconnect, and restart with a
  fresh importer when the source lease is gone. Validate accumulated length
  and CRC before finish; Abort best-effort on all exits. Files:
  `lib/crowdb-kv/src/cluster/group_membership.rs`, snapshot join tests.
- [ ] **Make publication failure-safe**: keep the group local to
  `join_group_via_snapshot` until import and learner seeding succeed. On any
  failure, shut down and drop the fresh group so a partially installed engine
  cannot be retried or registered. Confirm store insertion remains after
  `seed_resume_frontier` and `set_next_slot`; no remotes or election driver
  may start earlier. Files: `app/crowdb-kv-server/src/mgmt/group_ops.rs`,
  management integration tests.
- [ ] **Verify tail continuation**: after successful publication, wire the
  existing remotes and prove ordinary heartbeat/`FetchGap` repair begins above
  the imported slot. Do not invoke snapshot streaming from the running
  follower's `catchup_snapshot_threshold` branch. Files: snapshot join E2E
  tests, `lib/crowdb-kv/src/cluster/group_fetchgap.rs` assertions if needed.
- [ ] **Instrument the receiver**: add imported bytes/chunks, reconnect
  retries, resumed offsets, identity restarts, integrity failures, aborted
  joins, and import latency. Files: group membership and server metrics tests.

## Phase 4 — Compatibility removal and documentation

- [ ] **Remove whole-buffer APIs**: delete `SnapshotReply`, `send_snapshot`,
  legacy service handling, and the `KVEngine`/FFI convenience methods that
  concatenate the stream after all callers and tests use sessions. Files:
  `lib/crowdb-kv/src/rpc/px_rpc_transport.rs`,
  `lib/crowdb-kv/src/rpc/px_rpc_service.rs`, KV engine implementations,
  crowdb-tree FFI wrappers and tests.
- [ ] **Remove legacy protocol claims**: delete `ESnapshotRequest`/
  `ESnapshotResponse`, remove comments and configuration describing a 64 MiB
  exception, and verify normal 4 MiB framing remains unchanged. Files:
  protocol schema/wrappers/tests, RPC/server configuration.
- [ ] **Update permanent architecture**: describe bounded new-member transfer,
  source lease/retry semantics, unpublished import commit, and the explicit
  deferral of live follower replacement. Correct the snapshot chunk maximum
  to 1 MiB for this transport. Files:
  `doc/design/kv/design-crowdb-kv-state-machine.md`,
  `doc/design/kv/design-crowdb-kv-rpc.md`,
  `doc/design/kv/design-crowdb-kv-slot.md`.
- [ ] **Run focused and final gates**: execute all requirement commands, add a
  >64 MiB integration fixture without checking a large blob into the tree,
  and retain complete output for any retry.
- [ ] **Close the requirement**: after every acceptance case passes, remove
  the backlog entry and this working plan according to the requirement
  workflow.

## Consolidated Files

- Crowdb-tree snapshot C API, Rust FFI session wrappers, and tests.
- KV engine snapshot traits, in-memory/crowdb-tree implementations, and
  conformance tests.
- KV consensus FlatBuffers, generated bindings, safe wrappers, and tests.
- Snapshot source registry/actors, RPC service, transport, and metrics.
- Group membership join lifecycle and management publication tests.
- KV state-machine, RPC, and slot architecture documents.

## Verification

- Unit: C/Rust RAII cleanup, metadata, deterministic retry, offset errors,
  session capacity/expiry/idempotent close, configuration limits, incremental
  parser boundaries, and typed transport errors.
- Integration: >64 MiB bounded transfer, reconnect at acknowledged offset,
  expired-identity restart, source-epoch fencing, corrupt/wrong/truncated
  stream rejection, failed group absence, and legacy-message removal.
- E2E: successful unpublished join, learner/engine frontier agreement, WAL-tail
  repair above `at_slot`, and concurrent source traffic progress during a
  large transfer.
