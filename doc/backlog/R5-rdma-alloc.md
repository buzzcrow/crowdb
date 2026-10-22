<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R5: RDMA — Host-buffer RPC and data transport

## Status

The registered host-buffer foundation, common RDMA RPC transport and
HTTP/ChunkIO/DiskIO datapath are accepted. Remaining child requirements R241
and R242 retain the Chunk-KV and fat-client goals.

## Problem

CROWDB needs an RDMA path for RPC and data transfer while retaining TCP as a
one-way fallback. The path must move bounded registered host buffers through
Access Server, Chunk-KV, DiskIO, and the native fat client without an
intermediate CPU payload copy. The caller must receive explicit buffer leases,
and unsupported RDMA must not affect TCP operation. The formal architecture is
defined in [`design-crowdb-rpc-rdma.md`](../design/rpc/design-crowdb-rpc-rdma.md).

## Solution

1. Reuse the accepted [registered-buffer and RDMA RPC contract](../design/rpc/design-crowdb-rpc-rdma.md):
   bounded 64 KiB/256 KiB/1 MiB classes, explicit ownership and quarantine,
   TCP preflight, per-CQ polling/event workers and one-way fallback.
2. Retain the verified transport and benchmark invariants when adding services.
3. Reuse the accepted [HTTP/ChunkIO/DiskIO stream](../design/rpc/design-crowdb-rpc-rdma.md#7-service-interactions),
   preserving existing aggregation, aligned 1 MiB receive data, disk completion
   ownership, caller leases and partial-result failure without a TCP suffix.
4. Implement Access Server to Chunk-KV integration in
   [R241](R241-rdma-chunk-kv-access.md), using 64 KiB metadata and 256 KiB
   aggregated result messages.
5. Implement the native fat client and FFI lease API in
   [R242](R242-rdma-fat-client-sdk.md), with host memory first and explicit
   host-to-GPU copy for the initial GPU simulation.

The base transfer mechanism is SEND/RECV. Large client buffers are split into
ordered messages with request ID, sequence, and logical offset. RDMA READ and
WRITE are outside this requirement.

## Dependencies

- R4 bounded memory pool principles.
- Existing common RPC framing and request-number system.
- Existing ChunkIO aggregation, Access Server, Chunk-KV, DiskIO, and FFI
  ownership contracts.
- R170 remains an optional cuObject adapter and must reuse the stream contract.

## Acceptance

- Given a host with no RDMA capability, when the services start, TCP remains
  usable and no RDMA attempt is required. Invariant: optional transport.
  Integration test.
- Given RDMA capability and configured registered memory, when an access stream
  runs, all buffers remain within the budget and are released only by their
  owner. Invariant: bounded ownership. E2E test.
- Given an RDMA setup failure before payload submission, when an idempotent
  request is retried, TCP returns the same logical result. Invariant: one-way
  fallback. E2E test.
- Given a result larger than its selected class, when it is split into ordered
  messages, the SDK, Chunk-KV, or DiskIO consumer reconstructs exact bytes.
  Invariant: stream equivalence. E2E test.
- Given an SDK caller holds returned leases, when the pool is exhausted, new
  work applies bounded backpressure and resumes after release. Invariant:
  caller-controlled lifetime. Integration test.

## Open Questions

- Which service-discovery record supplies the coarse RDMA capability and
  capability generation? The TCP handshake remains the authoritative dynamic
  endpoint exchange.
- Which deployments enable the RDMA transport by default after hardware
  capability is detected? Unsupported deployments remain TCP-only.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
```
