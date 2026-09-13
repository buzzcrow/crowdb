<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Durable Buffered-File WAL io_uring Plan

Upstream requirement: `doc/backlog/R66-kv-wal-io-uring.md`

Root design: `doc/design/kv/design-crowdb-kv-wal.md`

Goal: restore the default WAL durability barrier and add an explicitly selected
Linux io_uring backend without changing record format or aligned block I/O.

## Phase 1 — Correctness Baseline

- [ ] **Restore portable data sync**: replace the `FileBackendFile::fdatasync`
  no-op with awaited `tokio::fs::File::sync_data` and correct its comments.
  Files: `lib/crowdb-kv/src/wal/file_backend.rs`.
- [ ] **Prove acknowledgement ordering**: add a failure-injectable file/backend
  seam or focused test hook that holds and fails data sync; assert pipeline
  acknowledgements remain pending until success and fail on sync error. Files:
  `lib/crowdb-kv/src/wal/{file_backend,pipeline_writer}.rs`,
  `lib/crowdb-kv/tests/wal_test/`.
- [ ] **Audit skip-fsync gates**: verify only `wal_skip_fsync` and the documented
  macOS benchmark policy bypass runtime data sync; reconcile misleading durable
  comments before uring work. Files: `lib/crowdb-kv/src/wal/`,
  `lib/crowdb-kv/src/common/config.rs`, `app/crowdb-kv-server/src/cli.rs`.

## Phase 2 — C++ Ring Operations and Lifetime

- [ ] **Add engine readiness reporting**: expose whether all configured rings,
  eventfds, and polling threads initialized successfully; construction failure
  must not produce an apparently usable engine. Files:
  `lib/crowdb-common/cpp/include/crowdb-common/diskio_uring.h`,
  `lib/crowdb-common/cpp/src/diskio_uring.cpp`.
- [ ] **Add vectored positional write**: implement `submit_writev` with retained
  iovec storage and partial-write completion/retry semantics matching
  `FileBackendFile::write_vectored_at`. Files: common `diskio_uring` header,
  implementation, and C++ tests.
- [ ] **Separate sync modes**: pass data-only/full-sync flags into
  `io_uring_prep_fsync` and test both completion/error paths. Files: common
  `diskio_uring` header, implementation, and tests.
- [ ] **Fix fd and ring teardown ordering**: make unregister/close wait for or
  cancel and drain every fd completion before registration reuse, and drain
  callback ownership before stopping polling threads. Cover old kernels where
  cancel-by-fd is unavailable. Files: common `diskio_uring` header,
  implementation, and tests.

## Phase 3 — Standalone Rust Adapter

- [ ] **Define an opaque C ABI**: add create/destroy, readiness, fd
  register/unregister, read, writev, data-sync, full-sync, cancel, and eventfd
  functions with a callback/context completion contract. Files:
  `lib/crowdb-tree/include/crowdb-tree/c_api.h`,
  `lib/crowdb-tree/src/c_api.cpp` or a focused C ABI translation unit.
- [ ] **Add scoped unsafe bindings**: declare the ABI in `sys.rs`; keep all raw
  pointers, callbacks, and descriptor borrowing inside a new uring adapter
  module. Files: `lib/crowdb-tree/ffi/src/{sys,uring}.rs`,
  `lib/crowdb-tree/ffi/src/lib.rs`.
- [ ] **Implement owned completion futures**: retain write buffers/iovecs and
  read destinations through CQE, translate negative errno, support dropped
  futures without early free, and share the existing eventfd pump pattern.
  Files: `lib/crowdb-tree/ffi/src/{uring,reactor}.rs`.
- [ ] **Test adapter cancellation and destruction**: cover synchronous submit
  failure, normal CQE, dropped future, file close, ring close, and concurrent
  operations under ASan-capable C++ tests where applicable. Files:
  `lib/crowdb-tree/ffi/tests/`, `lib/crowdb-common/cpp/tests/diskio_uring_test.cpp`.

## Phase 4 — WAL Backend and Server Selection

- [ ] **Implement buffered uring files**: own a `std::fs::File`, register its fd,
  and implement read/read-exact, write/writev, data sync, and full sync through
  the adapter; keep metadata/namespace calls on the portable path. Files:
  `lib/crowdb-kv/src/wal/{uring_backend,wal_file,io_backend}.rs`,
  `lib/crowdb-kv/src/wal.rs`.
- [ ] **Preserve pipeline semantics**: route existing `write_raw_vectored` and
  flush calls unchanged through `WalFile`; verify offsets, partial writes,
  rotation, seal, replay, and shutdown. Files:
  `lib/crowdb-kv/src/wal/{segment,pipeline_writer,replay}.rs` and tests.
- [ ] **Add explicit configuration**: accept `uring` in server CLI/config,
  construct one ring in the shared `IoBackend`, reject unavailable explicit
  selection, and report the chosen backend. Files:
  `app/crowdb-kv-server/src/{cli,store_registry,main}.rs`,
  `lib/crowdb-kv/src/common/config.rs`.
- [ ] **Add backend conformance**: run the same file-operation, WAL append,
  rotation, replay, and failure cases against File and Uring where supported;
  keep compile-time portable coverage. Files: `lib/crowdb-kv/tests/wal_test/`,
  `app/crowdb-kv-server/tests/`.

## Phase 5 — Evidence and Closure

- [ ] **Benchmark with durability enabled**: compare identical File and Uring
  batch sizes, payloads, segment settings, and physical device; record
  append/fsync latency, throughput, CPU, and ring pressure counters. Files:
  `lib/crowdb-kv/benches/wal.rs`,
  `lib/crowdb-kv/benches/wal_bench_history.md`.
- [ ] **Update architecture**: document the corrected fallback, buffered uring
  topology, explicit selection/failure policy, and deferred O_DIRECT/default
  work. Files: `doc/design/kv/design-crowdb-kv-wal.md`.
- [ ] **Run focused gates**: run all R66 verification commands, including the
  portable build path, and fix ordinary failures before closure. Files:
  affected workspace.

## Consolidated File List

- `lib/crowdb-common/cpp/{include/crowdb-common/diskio_uring.h,src/diskio_uring.cpp,tests/}`
- `lib/crowdb-tree/{include/crowdb-tree/c_api.h,src/c_api.cpp}`
- `lib/crowdb-tree/ffi/src/{lib,sys,uring,reactor}.rs` and FFI tests
- `lib/crowdb-kv/src/wal/{file_backend,uring_backend,io_backend,wal_file,segment,pipeline_writer,replay}.rs`
- `lib/crowdb-kv/src/common/config.rs` and WAL tests/benchmarks
- `app/crowdb-kv-server/src/{cli,store_registry,main}.rs` and tests
- `doc/design/kv/design-crowdb-kv-wal.md`
- `lib/crowdb-kv/benches/wal_bench_history.md`

## Tests

Unit tests:

- CQE errno and partial-I/O mapping.
- Explicit backend parsing and unavailable-backend errors.
- File sync acknowledgement seam.

Integration tests:

- C++ and Rust ring initialization, writev/read/fsync, cancellation, and drop.
- File/Uring WAL conformance, rotation, replay, and injected failure.
- Server startup selection and portable no-liburing build.

E2E tests:

- Fsync-enabled WAL restart/replay parity and same-device File/Uring benchmark.
