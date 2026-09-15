<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R161: access server / S3 — Pipeline-local admission and backpressure

## Problem

Small-object concurrency commonly reaches 1,000–3,000 requests. A global or
per-tenant semaphore, whole-object reservation, or multi-route load selection
would add shared synchronization to every body frame. Unbounded queues would
instead let slow storage consume process memory.

The resource model is
`doc/design/chunkio/design-crowdb-chunkio-small-object-writer.md` §§2–3.

## Solution

1. Hash the complete tenant/bucket/object key to one shared-object pipeline.
   Read only that route's atomic queued bytes and bounded queue state; do not
   reserve `Content-Length` or acquire a global/per-tenant semaphore.
2. Charge actual immutable frame bytes while owned by the handler, queue, or
   worker. Transfer that charge with ownership and release it exactly once.
3. Before polling Hyper for another body frame, if the selected route is full,
   wait for its notification with a short periodic fallback. Leaving the
   socket unread supplies natural TCP backpressure. Slightly stale observations
   are acceptable; the bounded channel remains the hard object-count limit.
4. Scale out when a route crosses configured queued-byte or object thresholds,
   up to the configured pipeline count. Selection stays one direct hash, not
   power-of-two choice.
5. Default the pool for about 1,000 concurrent 1 MiB objects. Document roughly
   3.25 GiB and 5.25 GiB configurations for 3,000 and 5,000 respectively.
   GET uses the chunk reader's independently bounded pull window.

## Dependencies

- Uses shared-object writer routes and the S3 streaming bridge.
- Native-buffer and vectored-send accounting remains a separate transport
  extension and cannot add object-path coordination.

## Acceptance

- Given 1,000 concurrent maximum-size small objects, when storage drains
  normally, assert all complete within the default budget and no global
  semaphore is acquired. Invariant: ordinary admission is route-local.
  Integration test.
- Given one selected pipeline is full, when its client continues sending,
  assert the handler stops polling Hyper until notification or fallback detects
  capacity. Invariant: storage pressure reaches TCP. Integration test.
- Given frame ownership moves handler to queue to worker, when success,
  failure, or cancellation completes, assert actual-byte accounting returns to
  its prior value exactly once. Invariant: accounting follows ownership.
  Integration test.
- Given a hot route, when queued thresholds are crossed, assert scale-out may
  add a pipeline but the current object stays on its original route.
  Invariant: one object maps to one chunk pipeline. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
