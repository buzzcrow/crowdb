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
   The common RPC transport splits and assembles the already encoded control
   message. Chunk-KV keeps its existing serializers and delivers one complete
   logical request/response to the existing handler/correlation layer. Use the
   negotiated control/data layout: small fragment control accompanies bounded
   64 KiB/256 KiB/1 MiB data containing the encoded logical control bytes.
   Negotiate support and the logical-message limit before payload submission.
4. Use the same TCP/RDMA request identity and one-way RDMA-to-TCP fallback.
   Retry only when the existing operation is idempotent or duplicate-suppressed.
5. Keep application aggregation above RDMA. Do not add an application flush
   timer, another aggregate producer, or application payload assembly copy.
   Existing contiguous RPC transport coalescing remains bounded by both peers'
   slots. Control/data streams send one message per SEND. Reassembly copies
   each control fragment once into one bounded logical control owner, which
   remains charged until its final native/FFI caller release.
6. Bound logical control bytes independently of registered memory. Configure
   a maximum logical message and per-direction send/receive byte budgets;
   defaults cover the client's existing 16 MiB response budget. Sender
   admission counts the retained original encoding. Receiver admission reserves
   the complete logical length before assembly; exhausted admission retains
   only the existing receive-window fragments and resumes on caller release.
   Never allocate replacement receive storage or return a partial control
   message. Disconnect/cancellation/partial-message expiry releases assembly
   state while completed caller-owned results retain their admission.

## Dependencies

- Accepted [registered buffers and RDMA RPC](../design/rpc/design-crowdb-rpc-rdma.md).
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
