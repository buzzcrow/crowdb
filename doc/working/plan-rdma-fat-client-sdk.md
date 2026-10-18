<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA fat client SDK and FFI lease Plan

Upstream: [R242](../backlog/R242-rdma-fat-client-sdk.md),
[root RDMA plan](plan-rdma.md), [R238](../backlog/R238-rdma-registered-buffer-foundation.md),
[R239](../backlog/R239-rdma-rpc-transport.md), and
[R241](../backlog/R241-rdma-chunk-kv-access.md).

Goal: expose Access Server semantics through a native fat-client SDK that can
consume the same RDMA stream directly, with caller-owned host buffer leases
and an explicit host-to-GPU copy simulation.

## Phase 1: access-core split

- [ ] **Map Access Server boundary**: identify HTTP-only parsing, authorization,
  access planning, generation validation, aggregation, Chunk-KV calls, DiskIO
  calls, and response publication. Files: `app/crowdb-access-server/` and
  access design modules.
- [ ] **Extract access core**: move shared planning and operation semantics
  behind a native interface while keeping HTTP frontend behavior unchanged.
  The core must select the logical connection path through R239 and preserve
  TCP fallback.
- [ ] **Define SDK operations**: specify open/read/write/range/metadata
  operations, request ID, generation, cancellation, deadlines, and stream
  sequence without exposing C++ transport or verbs types.

## Phase 2: FFI lease interface

- [ ] **Define opaque lease ABI**: expose handle, actual length, capacity,
  request ID, sequence, logical offset, error/end status, and release. Use
  fixed-width types and explicit null/error behavior.
- [ ] **Implement callback bridge**: C++ completion resolves ownership and
  invokes the Rust FFI callback; Rust forwards to the future/stream consumer.
  The callback must not block the RPC worker or retain a borrowed pointer past
  release.
- [ ] **Implement release/drop**: make release idempotency and misuse visible,
  return caller-owned buffers to R238 only after caller access ends, and keep
  shutdown safe while leases remain held.
- [ ] **Implement cancellation**: stop new work, drain or fail submitted
  requests, preserve in-flight buffer lifetime, and report cancellation once.

## Phase 3: host and GPU-copy flow

- [ ] **Host path**: deliver registered host leases directly to SDK callers;
  do not copy payload into a second SDK buffer.
- [ ] **GPU-copy simulation**: provide an explicit copy hook or SDK operation
  that holds the host lease until the caller confirms host-to-GPU copy
  completion. Do not expose a GPU pointer in the base ABI.
- [ ] **Buffer pressure**: enforce SDK-held byte budget; pause producer/admission
  when callers retain all leases and resume after release.

## Phase 4: frontend parity

- [ ] **HTTP parity**: run identical access requests through HTTP frontend and
  native SDK and compare bytes, metadata, generation, errors, and cancellation.
- [ ] **Transport parity**: run SDK operations over RDMA and TCP with the same
  request ID and verify fallback before payload submission.
- [ ] **Multi-message stream**: expose ordered 64 KiB metadata or 256 KiB
  aggregate messages as one SDK stream without copying them into a monolithic
  result.

## Tests

Unit tests:

- opaque ABI layout, fixed-width fields, null/error behavior;
- lease accessors, single release, release-after-shutdown, and misuse;
- callback does not block and does not publish unresolved ownership;
- cancellation resolves exactly once;
- host-to-GPU copy dependency keeps lease alive until completion.

Integration tests:

- native SDK uses the same access-core semantics as HTTP frontend;
- caller holds all leases and observes bounded admission/backpressure;
- multi-message metadata and aggregate responses preserve order and offsets;
- RDMA-disabled host uses TCP without changing SDK result;
- pre-payload RDMA failure retries eligible request with shared request ID;
- partial RDMA failure fails stream without accepting a TCP suffix.

E2E tests:

- single-node HCA local-loop OCI client/Access Server/DiskIO flow;
- TCP-only OCI profile with no RDMA devices;
- single-node local-loop multi-container SDK pressure and multiple logical streams;
- explicit host-to-GPU copy simulation with release after copy completion.

## Consolidated files

- `app/crowdb-access-server/`
- Access core and native SDK modules
- `lib/crowdb-rpc/ffi/src/client.rs`
- `lib/crowdb-rpc/ffi/src/server.rs`
- `lib/crowdb-rpc/ffi/src/sys.rs`
- SDK integration tests and OCI E2E runner

## Verification gates

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

Completion requires caller-controlled lease tests, HTTP/SDK parity, TCP/RDMA
parity, and safe shutdown with outstanding leases.
