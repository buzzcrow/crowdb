<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R161: access server / S3 — Admission control, memory bounds, and backpressure

## Problem

Streaming avoids whole-object buffering only if every queue and retained owner
is bounded. Slow clients, slow storage, fragmented bodies, and metadata stalls
can otherwise consume all native buffers or descriptors and starve unrelated
requests.

The resource model is
`doc/design/accessserver/design-crowdb-access-server.md` §8 and
`doc/design/accessserver/design-crowdb-access-server-s3.md` §4.

## Solution

1. Add service, tenant, and request credits for connections, admitted
   operations, request/response native bytes, buffer views, writer/reader
   windows, metadata work, and cleanup work. Account an owner until its final
   asynchronous consumer releases it.
2. Apply backpressure end to end: stop polling request bodies when write
   credits are exhausted, stop chunk prefetch when response credits are
   exhausted, and stop accepting new work when service admission is full.
   Never block a Tokio worker or allocate an unbounded fallback.
3. Validate `max_buffer_views_per_rpc` below all platform, RPC, and send-queue
   hard limits. Compute an effective frame limit from configured maximum,
   remaining send-queue capacity, and flush policy; flush before overflow and
   coalesce only when one logical storage operation cannot fit.
4. Separate header, body-idle, body-total, metadata, storage, and drain
   deadlines. Cancellation returns all non-durable credits; durable upload or
   cleanup work retains explicitly accounted ownership until reconciliation.
5. Expose rejection, wait duration, queue depth, retained bytes/views,
   queue-limit flush, and coalesced-byte metrics without per-object labels.

## Dependencies

- Depends on streaming paths R155 and R157 for full integration.
- R160 supplies process/plugin wiring; local safety limits remain mandatory
  before full scale-out.
- Uses native owner lifetime support in `crowdb-rpc-ffi` and chunk clients.

## Acceptance

- Given PUTs and GETs larger than all memory budgets with slow peers, when load
  exceeds each credit independently, assert producers pause and measured
  ownership never exceeds the configured bound. Invariant: object size and
  peer speed cannot create unbounded memory. E2E test.
- Given view limits at the hard boundary and a nearly full send queue, when a
  fragmented operation is sent, assert config above the boundary is rejected,
  frames flush before overflow, and any coalesce is counted. Invariant: runtime
  tuning cannot violate transport limits. Integration test.
- Given timeout or cancellation at every pipeline stage, when work terminates,
  assert volatile credits return exactly once and durable reconciler ownership
  remains counted. Invariant: cancellation neither leaks nor releases live
  memory early. Integration test.
- Given one tenant exhausts its quota, when another tenant submits bounded
  work, assert service reserve/fairness policy admits it or returns the
  configured deterministic rejection. Invariant: one tenant cannot silently
  consume unaccounted global capacity. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run test-rpc-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
