<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA registered host-buffer foundation Plan

Upstream: [R238](../backlog/R238-rdma-registered-buffer-foundation.md),
[R5](../backlog/R5-rdma-alloc.md), and
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: provide bounded registered host buffers and an explicit lease/ownership
contract for every later RDMA RPC and data-path component.

## Progress

Capacity classes, ownership states, lease metadata, bounded atomic admission,
lock-free freelists, quarantine exclusion, and C/Rust metadata accessors are
implemented in `buffer.h`, `buffer.cpp`, `c_api.h`, and the FFI buffer wrapper.
The pool is device-independent so TCP-only builds retain the bounded contract.
The verbs MR arena and basic send/receive handoff are complete. Its allocation
and recycle path uses bounded atomic class freelists and an atomic arena bump
pointer; owner transitions, premature-release quarantine, inflight shutdown
quarantine, and FFI ownership accessors now use the common contract.

The verbs pool now pre-registers one region and carves aligned capacity-class
slices from it. Each lease records the region MR handle and lkey, and
quarantined leases are retained out of the reusable free lists until pool
shutdown.

## Phase 1: inventory and contract

- [x] **Map existing buffer ownership**: document the current `Buffer`,
  `BufferPool`, `OutFrame`, callback, and FFI ownership transitions before
  adding registration. Files: `lib/crowdb-rpc/include/crowdb-rpc/buffer.h`,
  `lib/crowdb-rpc/include/crowdb-rpc/transport.h`,
  `lib/crowdb-rpc/include/crowdb-rpc/connection.h`,
  `lib/crowdb-rpc/ffi/src/sys.rs`.
- [x] **Define capacity classes**: add named constants/configuration for 64 KiB,
  256 KiB, and 1 MiB data capacities plus the 1 KiB control capacity. Reject
  zero, overflowed, and unsupported capacities before pool admission. Files:
  `lib/crowdb-rpc/include/crowdb-rpc/buffer.h`, configuration headers, and
  corresponding Rust FFI types.
- [x] **Define lease metadata**: specify request ID, stream ID, sequence,
  logical offset, actual length, capacity, registration identity, and release
  state. Keep the handle opaque across FFI. Files: buffer and FFI headers,
  `lib/crowdb-rpc/ffi/src/sys.rs`.

## Phase 2: registered pool

- [x] **Unify device registration owner**: make the verbs-backed arena use the
  common registration identity and failure contract while retaining one MR per
  arena. Registration failure must leave TCP allocation usable. Files:
  `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/rdma_transport.h` and
  `lib/crowdb-rpc/src/transport/rdma/rdma_transport.cpp`.
- [x] **Unify class freelists**: carve all four classes from bounded registered
  regions, preserve the byte and slot budgets, and avoid a separate pool path
  with different recycle semantics. Files: same pool files and pool tests.
- [x] **Complete ownership transitions**: make acquire, post, completion, disk
  handoff, caller release, and quarantine explicit at every async boundary;
  reject a second release or recycle before completion. Files: pool,
  transport completion paths, and ownership tests.
- [x] **Complete shutdown and quarantine**: drain known operations, keep
  uncertain buffers out of reuse until connection quiescence, and deregister
  only after device access ends. Files: pool, connection shutdown, and tests.
- [x] **Review hot-path synchronization**: keep allocation/recycle lock-free;
  lifecycle destruction is separate from the atomic freelists. Files: pool
  implementation.

## Phase 3: FFI lease surface

- [x] **Expose lease accessors**: return opaque handle, data pointer, actual
  length, capacity, sequence, and logical offset without exposing verbs structs.
  Files: `lib/crowdb-rpc/ffi/src/sys.rs`, `client.rs`, `server.rs`, and C ABI
  declarations.
- [x] **Enforce release ordering**: caller release returns a buffer only after
  the C++ owner confirms completion; Rust must not retain a dereferenceable
  view after release. Files: FFI wrapper and ownership tests.

## Tests

Unit tests:

- class rounding/selection and overflow rejection;
- byte-budget admission and per-class counts;
- every valid ownership transition and invalid double release;
- quarantine exclusion and shutdown accounting;
- lease metadata and opaque-handle accessors.

Integration tests:

- registration failure falls back to the ordinary TCP pool;
- a completed receive lease is not reusable until explicit release;
- a disk-owned lease is not reusable until disk completion is reported;
- concurrent acquire/release does not exceed the configured byte budget.

E2E tests:

- run the TCP-only OCI profile without RDMA devices and confirm the service
  remains usable;
- run the HCA local-loop profile and confirm registered buffers survive a complete
  send, receive, and release cycle.

## Consolidated files

- `lib/crowdb-rpc/include/crowdb-rpc/buffer.h`
- `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/rdma_transport.h`
- `lib/crowdb-rpc/src/transport/rdma/rdma_transport.cpp`
- `lib/crowdb-rpc/ffi/src/sys.rs`
- `lib/crowdb-rpc/ffi/src/client.rs`
- `lib/crowdb-rpc/ffi/src/server.rs`
- `lib/crowdb-rpc/tests/` buffer and lease tests

## Verification gates

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run tree-lint
```

Completion requires all ownership, budget, shutdown, and TCP-only tests to
pass. R238 is complete once the recorded gates below pass; R239 transport work
may consume the stable lease contract.
