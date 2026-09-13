<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Snapshot Streaming Plan

Upstream requirement: `doc/backlog/R151-kv-snapshot-streaming.md`

Goal: replace whole-buffer snapshot install with bounded, resumable unary
chunks while preserving stable export, atomic activation, and shared-worker
progress.

## Phase 1 — Bounded engine sessions

- [x] **Stream crowdb-tree portable export**: replace the prebuilt serialized
  string with an immutable snapshot cursor, incremental CRC32C encoder, stable
  total-length/final-CRC metadata, and a one-chunk retry cache. Preserve the
  existing byte format. Files: `lib/crowdb-tree/include/crowdb-tree/snapshot/snapshot_io.h`,
  `lib/crowdb-tree/src/snapshot/snapshot_io.cpp`, crowdb-tree snapshot tests.
- [~] **Stage crowdb-tree portable import**: incrementally parse header,
  entries, and trailer with bounded carry; maintain rolling CRC32C; keep parsed
  logical entries as staging state; reject truncation, trailing data, malformed
  lengths, and CRC mismatch before activation. Files: crowdb-tree snapshot I/O
  header/source and tests.
- [ ] **Make successful activation atomic**: build the replacement tree off
  the published root and perform one fenced root/state publication so readers
  observe either the old or new engine, never a partly rebuilt tree. Retire old
  pages through the existing epoch mechanism and serialize with the existing
  writer lock. Files: `lib/crowdb-tree/include/crowdb-tree/btree/tree.h`,
  `lib/crowdb-tree/src/btree/crowdb-tree.cpp`, integration tests.
- [ ] **Expose C session metadata**: add export slot, total bytes, final CRC,
  chunk-size, and offset-aware next calls without transferring ownership of
  whole buffers. Keep end calls idempotent for valid handles. Files:
  `lib/crowdb-tree/include/crowdb-tree/c_api.h`, `lib/crowdb-tree/src/c_api.cpp`.
- [ ] **Add Rust RAII wrappers**: introduce non-clone export/import session
  owners whose `Drop` closes the C handle; expose metadata, offset reads, feed,
  finish, and abort semantics. Retain whole-buffer convenience methods only
  until callers migrate. Files: `lib/crowdb-tree/ffi/src/sys.rs`,
  `lib/crowdb-tree/ffi/src/snapshot.rs`, `lib/crowdb-tree/ffi/tests/ffi_test.rs`.
- [ ] **Add engine-neutral sessions**: define object-safe `SnapshotExporter`
  and `SnapshotImporter` traits plus metadata/chunk/error types; replace
  `KVEngine::snapshot_export`/`snapshot_import` with begin methods. Implement
  deterministic incremental sessions for `InMemKV` and crowdb-tree. Files:
  `lib/crowdb-kv/src/kv/kv_engine.rs`, `lib/crowdb-kv/src/kv/in_mem_kv.rs`,
  `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs`, KV conformance tests.

## Phase 2 — Protocol and bounded exporter ownership

- [ ] **Define unary stream messages**: allocate begin/read/finish/abort
  request/response message types. Carry identity, byte offset, limits, source
  fencing, total length, final CRC32C, per-chunk CRC32C, end state, and typed
  error codes. Remove the legacy snapshot tables after migration. Files:
  `lib/crowdb-protocol/src/fbs/msg_type.fbs`,
  `lib/crowdb-protocol/src/fbs/kv_consensus.fbs`, generated protocol wrappers.
- [ ] **Add validated static limits**: add snapshot chunk bytes, exporter
  session cap, and idle lease to server/group configuration with defaults of
  1 MiB, four, and 30 seconds. Enforce the existing 64 KiB–64 MiB chunk range,
  nonzero capacity, and nonzero lease. Thread server overrides through startup
  configuration. Files: `lib/crowdb-kv/src/common/config.rs`,
  `app/crowdb-kv-server/src/config.rs`, config tests and sample configuration.
- [ ] **Build the lock-free session registry**: store an immutable ArcSwap
  registry per `PxRpcService`; pair a server boot nonce with a monotonic
  session counter for reconnect-safe identities;
  CAS insert/remove/reap operations enforce capacity without eviction. Each
  entry owns a bounded actor channel and live lease state. Files:
  `lib/crowdb-kv/src/rpc/snapshot_registry.rs`,
  `lib/crowdb-kv/src/rpc/px_rpc_service.rs`, registry unit tests.
- [ ] **Serve begin/read/finish/abort**: begin captures group/leader term and
  membership epoch and starts one actor owning the exporter. Read verifies the
  live fence and supports current offset or exact previous-chunk retry. Finish
  requires final acknowledgement; abort/finish/expiry close exactly once.
  Responses never exceed the configured chunk size and request tasks await the
  actor rather than spawning unbounded work. Files: snapshot registry/service,
  RPC helpers, service tests.
- [ ] **Add exporter metrics**: register and update active/capacity-rejected,
  bytes/chunks, retries, expired/aborted/completed, integrity/fence failures,
  export latency, and request queue-wait counters using existing lock-free
  metric handles. Files: snapshot registry, RPC service, metrics tests/design.

## Phase 3 — Receiver, fencing, and catch-up policy

- [ ] **Implement the pull client**: add typed transport calls for all four
  operations over the existing connection pool. Validate every echoed field,
  payload CRC, offset, bound, end marker, total length, and final CRC. Retry a
  retryable disconnect at the last successfully fed offset; restart from begin
  only when the session identity expired. Files:
  `lib/crowdb-kv/src/rpc/px_rpc_transport.rs`, transport tests.
- [ ] **Install through staged engine state**: make `join_via_snapshot` pull
  and feed chunks directly, check local install generation/topology before
  finish, abort best-effort on every failure, activate once, and seed the
  learner only after matching `at_slot`. Files:
  `lib/crowdb-kv/src/cluster/group_membership.rs`, learner/group state and
  snapshot join tests.
- [ ] **Support one live install per follower**: add atomic install generation
  and in-progress admission, reset learner bookkeeping to the activated slot,
  discard obsolete gaps, and wake the apply loop to replay accepted/chosen tail
  entries. Superseded and stale-term/epoch transfers must not activate. Files:
  `lib/crowdb-kv/src/cluster/group.rs`, learner/local-replica modules,
  integration tests.
- [ ] **Route large and unavailable gaps**: retain `FetchGap` at or below
  `catchup_snapshot_threshold`; above it, launch the admitted live installer.
  Distinguish not-yet-chosen from unavailable accepted history in the RPC and
  select snapshot install on the latter. Resume `FetchGap` for the post-snapshot
  tail. Files: `lib/crowdb-kv/src/cluster/group_fetchgap.rs`, group FetchGap
  handler, consensus schema/transport/service, routing tests.
- [ ] **Add receiver/fairness metrics**: expose installs, active state,
  imported bytes/chunks, reconnect retries, resumed/restarted offsets,
  aborted/integrity/fence failures, import latency, and snapshot request queue
  wait. Verify consensus RPC counters progress during a large transfer. Files:
  group/RPC metrics, app server E2E tests, observability design.

## Phase 4 — Compatibility removal and verification

- [ ] **Remove the single-frame protocol**: delete legacy message types,
  `SnapshotReply`, whole-buffer service/transport code, the special 64 MiB RPC
  allowance, and stale comments/configuration. Files: protocol, KV RPC,
  crowdb-rpc/server configuration, tests and design docs.
- [ ] **Run engine gates**: run crowdb-tree C++ tests, `pixi run cargo test -p
  crowdb-tree-ffi`, `pixi run tree-lint`, and changed C++ formatting checks.
- [ ] **Run KV gates**: run `pixi run cargo test -p crowdb-kv` and server
  spawning tests through `pixi run clean-env &&` followed by their pixi task.
  Include >64 MiB, reconnect/resume, corruption/ordering, cleanup, gap-policy,
  topology-fence, and shared-worker fairness cases.
- [ ] **Run final formatting and lint**: run `pixi run cargo fmt --all --
  --check` and `pixi run cargo clippy -p crowdb-tree-ffi -p crowdb-kv -p
  crowdb-kv-server --all-targets -- -D warnings`, then `pixi run rs-lint`.
- [ ] **Update permanent architecture**: document bounded engine buffering,
  protocol lifecycle, catch-up routing, fencing, configuration, and metrics.
  Files: `doc/design/kv/design-crowdb-kv-state-machine.md`,
  `doc/design/kv/design-crowdb-kv-rpc.md`,
  `doc/design/kv/design-crowdb-kv-slot.md`,
  `doc/design/kv/design-crowdb-kv-observability.md`.
- [ ] **Close the requirement**: delete the R151 backlog detail, remove its
  backlog index entry, and delete this working plan after every acceptance
  case and gate passes.

## Consolidated Files

- crowdb-tree snapshot encoder/parser, tree activation, C API, and tests.
- crowdb-tree Rust FFI session wrappers and tests.
- KV engine traits/implementations and conformance tests.
- consensus FlatBuffers and generated wrappers.
- RPC snapshot registry, service, transport, and metrics.
- group/learner catch-up and live-install state.
- KV/server configuration and tests.
- permanent KV state-machine, RPC, slot, and observability design.

## Verification

- Unit: deterministic chunk retries, incremental parser boundaries, RAII drop,
  registry capacity/expiry/idempotent cleanup, configuration validation,
  learner reset, typed offset/integrity/fence errors.
- Integration: >64 MiB bounded transfer, reconnect from acknowledged offset,
  corrupt/missing/duplicate/reordered/wrong-identity rejection with old engine
  preserved, large/unavailable gap routing, tail replay, topology fencing.
- E2E: concurrent writes, Heartbeats, Accepts, and large snapshot installation
  on shared workers with bounded retained transfer memory and observable
  consensus progress.
