<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R66: WAL — Durable Buffered-File io_uring Backend

**Problem**: The current requirement assumed the production WAL performs file
writes and `fdatasync` through Tokio's blocking pool. Current code is more
serious: `IoBackend::File` is the default selected by `crowdb-kv-server`, but
`FileBackendFile::fdatasync` is a no-op. `pipeline_writer::write_batch` then
resolves append acknowledgements as if its flush were durable. A process crash
can therefore lose records that were counted as durable by the Paxos
acknowledgement contract in `doc/design/kv/design-crowdb-kv-wal.md` §5. The
real `sync_all` occurs only during an explicit close or maintenance flush and
does not repair an earlier acknowledgement.

After restoring the fallback's correctness, Tokio regular-file writes and
syncs use blocking filesystem work, adding scheduler hops and sharing the
runtime blocking pool with tree maintenance. An io_uring backend remains a
reasonable Linux optimization, but several assumptions in the old scope do
not match current code:

- `DiskIOUring` has no standalone C ABI or Rust owner; each crowdb-tree handle
  privately owns its ring, so sharing that instance with a store WAL would
  require a wider lifecycle redesign.
- It supports read, contiguous write, and fsync callbacks, while the WAL hot
  path uses vectored writes and also needs open, rename, unlink, directory,
  length, and truncate operations.
- WAL records and segment tails are not 4 KiB aligned. Passing them directly
  through `O_DIRECT` is invalid, and the existing `DiskIOUring` does not supply
  the block backend's read-modify-write alignment layer.
- `IoBackend::detect` is not used by the server; `parse_wal_backend` constructs
  the configured backend directly.

The root contract is `doc/design/kv/design-crowdb-kv-wal.md`. Concrete failure
scenarios are a follower returning `Accepted` before data reaches stable
storage, a single-node group acknowledging a write lost on power failure, and
io_uring teardown racing a borrowed write buffer or file descriptor.

**Solution**: Restore durability first, then add an explicitly selectable
buffered-file io_uring backend for the WAL write/read/sync data path.

1. Change the portable `File` backend's `fdatasync` to await a real
   `sync_data`. This is the always-available correctness fallback and remains
   the default backend. `wal_skip_fsync` remains the only explicit option that
   may bypass per-batch durability and stays documented as benchmark-only.
2. Extend the existing C++ `DiskIOUring` with the operations and lifecycle
   state needed by WAL: a vectored positional write submission, a validity
   probe, error-preserving fsync flags for data-only versus full sync, and
   close/unregister behavior that does not release an fd or callback state
   until its completions are drained. Buffers and iovec arrays remain owned by
   the awaiting Rust operation until its CQE resolves; cancellation suppresses
   delivery but does not free borrowed storage early.
3. Add a small standalone C ABI beside the crowdb-tree ABI and a safe RAII
   adapter in `crowdb-tree-ffi`. One adapter owns one single-pipeline
   `DiskIOUring`, registers regular-file descriptors, converts negative CQE
   results to `io::Error`, and resolves Rust futures from completion callbacks.
   Unsafe code stays confined to the existing FFI modules. Construction fails
   cleanly when liburing is absent or the runtime kernel rejects ring setup.
4. Add `IoBackend::Uring` and `WalFileInner::Uring`. A server configuration of
   `--wal-backend uring` creates one ring owned by the server's shared
   `Arc<IoBackend>` and therefore shared by all stores/groups using that
   backend. Segment data reads, vectored writes, `fdatasync`, and `fsync` use
   CQE-backed futures. Namespace and metadata operations that are not on the
   append durability path may continue through the portable filesystem
   implementation.
5. Open uring WAL segment files as buffered regular files, not `O_DIRECT`.
   Preserve arbitrary record offsets, zero-copy vectored batches, partial-I/O
   retry behavior, and the current on-disk format. The aligned
   `BlockDevice`/RMW backend is unchanged and is not routed through io_uring by
   this requirement.
6. Expose `uring` as an explicit Linux-capable server option. Startup must fail
   with an actionable error when it was explicitly requested but unavailable;
   it must not silently weaken durability or choose a different backend.
   Automatic default selection and sharing a crowdb-tree-owned ring are
   deferred until production benchmarks justify the extra policy and ownership
   complexity.
7. Add backend conformance, failure, cancellation, shutdown, and recovery
   tests, then compare `File` and `Uring` WAL batch latency with fsync enabled.
   Update the WAL design and benchmark history with correctness semantics,
   supported platforms, topology, and measured results.

`O_DIRECT` WAL support, converting namespace operations to io_uring, changing
the WAL record format, changing batch policy, and making uring the automatic
default are not part of this requirement.

**Dependencies**:

- The existing `DiskIOUring` and Linux liburing build are the implementation
  base. Builds without liburing retain only the corrected `File` backend.
- The existing `WalFile` abstraction and pipeline writer remain the semantic
  boundary; no Paxos API change is required.
- The uring adapter stays in `crowdb-tree-ffi` because that crate already
  compiles and links `crowdb-common/cpp`. Moving generic C++ FFI ownership into
  the Rust `crowdb-common` crate is a later build-boundary cleanup, not a
  prerequisite.

**Acceptance**:

- Setup the default `File` backend with fsync enabled and inject/observe its
  sync operation; append a batch; assert the acknowledgement resolves only
  after a real `sync_data` success and returns an error on sync failure.
  Invariant: every non-skipped WAL acknowledgement is covered by a durable
  flush. Integration test.
- Setup the uring adapter on a supported Linux kernel; submit vectored writes,
  positional reads, data sync, and full sync; assert exact bytes, offsets,
  partial-I/O completion handling, and errno propagation. Invariant: CQE
  completion is the sole future-success point. Integration test.
- Setup a pending uring operation, drop its Rust future and then close its WAL
  file/backend; assert no use-after-free, double callback, fd reuse race, hang,
  or leaked in-flight operation. Invariant: buffers, callbacks, descriptors,
  and ring destruction have a single ordered lifetime. Integration test.
- Setup a server with explicit `uring` on supported and unsupported hosts;
  assert supported startup reports uring and unsupported startup fails clearly,
  while the unchanged default reports `File`. Invariant: backend selection is
  explicit and never silently changes durability. Integration test.
- Setup identical fsync-enabled `File` and `Uring` WAL workloads; restart and
  replay each, assert identical durable records, then record throughput and
  append/fsync latency. Invariant: the optimization preserves WAL format and
  recovery behavior. E2E test.
- Setup a build without liburing; build and run the portable WAL tests; assert
  no uring symbol is required and `File` remains durable. Invariant: the Linux
  optimization does not break the portable fallback. Integration test.

Verification commands:

- `pixi run test-common-ct`
- `pixi run test-tree-ffi`
- `pixi run test-kv-core`
- `pixi run test-kv-server`
- `pixi run -- cargo bench -p crowdb-kv --bench wal -- --save-baseline r66-uring`
- `pixi run -- cargo fmt --all --check`
- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run rs-lint`
