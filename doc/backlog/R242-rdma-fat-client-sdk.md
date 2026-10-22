<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R242: Access SDK — RDMA fat client and buffer leases

## Problem

The current Access Server boundary adds an application-to-server socket and
async hop. A fat client should expose access semantics directly while reusing
the access core and allowing the same RDMA path used by the server. Returned
buffers need an explicit lifetime so the SDK can avoid copies without
recycling memory while the caller still reads it.

## Solution

1. Split the access core from the HTTP frontend and add a native SDK frontend.
   Both frontends use the same planner, authorization, generation, aggregation,
   Chunk-KV, DiskIO, and TCP/RDMA selection semantics.
2. Define an FFI-safe opaque buffer lease containing request identity, sequence,
   logical offset, actual length, capacity class, and release operation. The
   caller owns a completed host lease until release; release returns it to the
   registered pool.
3. Deliver completion from the C++ RPC worker to Rust without exposing verbs
   objects. Rust must not dereference a released lease, and C++ must not
   recycle a caller-owned lease from a completion callback.
4. Support host memory first. A caller requesting GPU delivery receives host
   leases and performs an explicit host-to-GPU copy; a future allocator can
   supply GPU-capable memory behind the same lease contract.
5. Preserve bounded admission, cancellation, shutdown, and RDMA-to-TCP
   fallback for SDK operations.

## Dependencies

- Accepted registered buffers/RDMA RPC and
  [Chunk-KV/access integration](../design/rpc/design-crowdb-rpc-rdma.md#7-service-interactions).
- Existing Access Server access-core and FFI conventions.
- R220 remains a separate Dataset SDK plan; this requirement supplies the
  transport and lease contract it may reuse.

## Acceptance

- Given a native SDK access request, when the operation completes through RDMA,
  the caller receives the exact ordered host buffers and releases each lease.
  Invariant: caller-owned lifetime. Integration test.
- Given the caller holds all available leases, when another buffer is needed,
  admission applies bounded backpressure and resumes after release. Invariant:
  bounded SDK memory. Integration test.
- Given a request with host-to-GPU delivery selected, when the operation
  completes, host data remains valid until the explicit copy completes and
  release follows the copy. Invariant: copy dependency lifetime. Integration test.
- Given RDMA is unavailable or fails before payload, when the same SDK request
  executes, TCP produces the same access result and request identity.
  Invariant: frontend transport parity. Integration test.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
```
