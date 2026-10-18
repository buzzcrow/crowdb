<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA datapath stream Plan

Upstream: [R240](../backlog/R240-rdma-datapath-stream.md),
[root RDMA plan](plan-rdma.md), [R238](../backlog/R238-rdma-registered-buffer-foundation.md),
and [R239](../backlog/R239-rdma-rpc-transport.md).

Goal: connect existing HTTP/ChunkIO aggregation to the RDMA RPC stream and
DiskIO receive loop without introducing another aggregation or CPU payload
copy.

## Phase 1: map existing data flow

- [ ] **Trace upload flow**: identify where HTTP body bytes enter ChunkIO,
  become aggregated buffers, are submitted to DiskIO, and are released after
  durable completion. Files: `app/crowdb-access-server/`,
  `lib/crowdb-chunk-client/`, `lib/crowdb-diskio-client/`, and ChunkIO designs.
- [ ] **Trace download flow**: identify how SDK/HTTP consumers receive data,
  how read-ahead aggregation chooses message boundaries, and when a caller
  releases each buffer.
- [ ] **Define transport adapter**: add the smallest Access/DiskIO adapter that
  accepts an already formed buffer, actual length, request identity, sequence,
  and logical offset. Do not add a second flush timer or aggregation queue.

## Phase 2: upload to DiskIO

- [ ] **Allocate 1 MiB datapath class**: select the configured 1 MiB data
  capacity plus 1 KiB control buffer for S3/HTTP/DiskIO streams.
- [ ] **Fill registered send buffer**: make the HTTP/ChunkIO producer write
  directly into the send-owned registered buffer and submit its actual length.
  The producer must stop when the send/registered budget is exhausted.
- [ ] **Receive ownership handoff**: on RDMA receive completion, transfer the
  DiskIO buffer to the asynchronous disk writer. Keep request, sequence,
  logical offset, and expected length attached to the write.
- [ ] **Repost after disk completion**: only reset and repost the receive buffer
  after the disk write and integrity result are complete. Propagate disk errors
  to the stream and do not silently repost corrupted data.

## Phase 3: download to SDK/HTTP

- [ ] **Prepost receive buffers**: use the R238 pool and R239 receive window;
  publish a `BufferLease` only after header/class/length validation.
- [ ] **Preserve caller ownership**: keep the lease caller-owned until explicit
  release. Do not let CQ completion or a stream callback recycle it.
- [ ] **Implement multi-message assembly**: deliver messages in sequence and
  logical offset order, preserving end/error flags and cancellation behavior.
- [ ] **Keep TCP parity**: use the same access result and error mapping when the
  stream selects TCP before RDMA payload submission.

## Phase 4: pressure and failure behavior

- [ ] **Disk queue pressure**: stop reposting when the asynchronous disk queue
  reaches its byte bound; let receive-window exhaustion backpressure the
  sender.
- [ ] **SDK pressure**: stop publishing new leases when held bytes reach the
  configured limit; resume after release.
- [ ] **Partial stream failure**: fail the logical object/request after partial
  RDMA transfer and quarantine uncertain buffers; never append TCP bytes.
- [ ] **Cancellation**: stop new aggregation submissions, drain known WRs,
  preserve in-flight buffer lifetimes, and return only buffers proven safe.

## Tests

Unit tests:

- actual length below 1 MiB does not send padded bytes;
- sequence/offset/end/error parsing and multi-message assembly;
- DiskIO repost is rejected before disk completion;
- SDK lease release is the only return transition for caller-owned buffers;
- no second aggregation or flush timer is invoked by the RDMA adapter.

Integration tests:

- HTTP/ChunkIO aggregate enters a registered send buffer directly;
- DiskIO receive completion hands the same buffer to asynchronous disk I/O;
- full disk queue stops repost and resumes after completion;
- all SDK leases held causes bounded producer backpressure;
- TCP and RDMA return identical bytes, lengths, and errors;
- cancellation and partial RDMA failure leave no reusable uncertain buffer.

E2E tests:

- single-node HCA local-loop OCI upload into DiskIO and download back to a client;
- object larger than 1 MiB reconstructed from ordered messages;
- TCP-only OCI profile produces the same object and metadata;
- single-node local-loop multi-container profile reports bounded pressure without claiming physical
  endpoint isolation.

## Consolidated files

- `app/crowdb-access-server/`
- `lib/crowdb-chunk-client/`
- `lib/crowdb-diskio-client/`
- `lib/crowdb-rpc/`
- `lib/crowdb-chunkio/` or the existing ChunkIO owner module
- relevant S3/DiskIO integration tests and OCI E2E runner

## Verification gates

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo test -p crowdb-diskio-client --all-targets
pixi run -- cargo test -p crowdb-chunk-client --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

Do not hand off to R242 until both upload and download have explicit lease
ownership tests and the DiskIO receive loop proves repost-after-disk-completion.
