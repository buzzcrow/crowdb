<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R240: Access server — RDMA datapath stream

## Problem

The S3/HTTP and DiskIO path currently has no RDMA stream contract. The path
must preserve existing ChunkIO aggregation, use a 1 MiB data capacity, and
transfer ownership from HTTP input to DiskIO without an intermediate CPU
payload copy. DiskIO must not recycle a receive buffer while asynchronous disk
I/O still uses it.

## Solution

1. Feed already aggregated ChunkIO buffers to R239. RDMA does not aggregate or
   flush; it preserves actual payload length and stream sequence.
2. Use a 1 KiB control block plus a 1 MiB data capacity for the datapath. A
   logical object is an ordered sequence of messages when larger than one
   message.
3. Let Access Server fill its registered send buffer from the HTTP path. Let
   DiskIO own its registered receive buffer after CQ completion and submit that
   buffer directly to asynchronous disk I/O.
4. Repost a DiskIO receive buffer only after disk completion. Apply bounded
   receive-window backpressure when DiskIO or the SDK consumer is slow.
5. Preserve R239 fallback: before payload submission the operation may use TCP;
   after partial RDMA transfer the stream fails without TCP byte splicing.

## Dependencies

- R238 registered buffers.
- R239 RDMA RPC transport.
- Existing ChunkIO aggregation, Access Server S3, DiskIO asynchronous I/O, and
  integrity contracts.

## Acceptance

- Given an aggregated HTTP upload, when it is sent to DiskIO, the receiver
  writes the registered buffer directly and reposts it only after disk
  completion. Invariant: buffer lifetime covers disk I/O. E2E test.
- Given an object larger than 1 MiB, when it is transferred, ordered messages
  reconstruct the exact object using sequence and logical offset. Invariant:
  stream ordering. E2E test.
- Given a full DiskIO write queue, when receive buffers are consumed, sender
  backpressure remains bounded and no replacement buffer is allocated.
  Invariant: bounded receive window. Integration test.
- Given an RDMA failure after payload submission, when the operation ends, no
  TCP bytes are appended to the partial object. Invariant: fallback isolation.
  E2E test.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo test -p crowdb-diskio-client --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
```
