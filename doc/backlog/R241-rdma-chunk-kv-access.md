<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R241: Access server — RDMA Chunk-KV integration

## Problem

Access Server and Chunk-KV need RDMA for access operations, but their message
sizes differ from the S3/DiskIO datapath. Metadata is usually small, while an
aggregated result may be larger and must be returned as multiple messages
without changing access semantics.

## Solution

1. Route Access Server to Chunk-KV through R239 while retaining the existing
   access planner, request correlation, generation checks, and error mapping.
2. Configure 64 KiB data capacity for metadata and 256 KiB for aggregated
   Chunk-KV results. The transport is created with these defaults from config;
   the access/aggregation layer selects the class for each operation.
3. Represent a result larger than its selected capacity as ordered messages
   with request ID, stream ID, sequence, logical offset, and end/error flags.
4. Use the same TCP/RDMA request identity and one-way RDMA-to-TCP fallback.
   Retry only when the existing operation is idempotent or duplicate-suppressed.
5. Keep aggregation above RDMA. The RDMA layer does not add flush timers,
   coalescing, or a second buffer-copy path.

## Dependencies

- R238 and R239.
- Existing Chunk-KV client/server and Access Server access semantics.
- Existing ChunkIO aggregation and request-number system.

## Acceptance

- Given metadata smaller than 64 KiB, when Access Server selects RDMA, the
  result matches TCP and uses the metadata class. Invariant: semantic parity.
  Integration test.
- Given an aggregated Chunk-KV response larger than 256 KiB, when it is
  returned, ordered messages reconstruct the exact response. Invariant:
  bounded message capacity and ordering. Integration test.
- Given an RDMA setup failure before payload, when an idempotent access request
  is retried, TCP returns one response with the same request identity.
  Invariant: shared correlation and fallback. Integration test.
- Given a partial RDMA response, when the connection fails, the access result
  is failed and no TCP suffix is accepted. Invariant: no mixed transport
  result. Integration test.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
```
