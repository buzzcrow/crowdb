<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R79: diskdb — Durable Concurrent-Free Coalescing

**Problem**: Current diskdb already batches all distinct segments in one
`FreeBlocks` RPC into one `DdbKvClient::persist_free_batch` call. The remaining
round-trip overhead is across concurrent RPCs, not within a request. The old
requirement predates the persist-only free model and incorrectly says free
clears the bitmap and deletes `BusyBlockKey`; current code only writes immutable,
incarnation-qualified `FreeBlockKey` facts and increments the in-memory
compaction backlog after persistence.

The proposed minimum-size buffer was unsafe at the API boundary. It returned
success before a free fact was durable, allowed a sub-threshold tail to remain
forever because there was no timer, and re-enqueued failed writes behind an
already successful response. A crash would not create scanner-repairable
drift: both the busy record and conservative bitmap would still say busy, so
the intended free would be indistinguishable from a live allocation and could
leak permanently. Graceful shutdown cannot repair an acknowledgement already
observed by a caller.

It also proposed a `Mutex<Vec<_>>` on the hot free path, conflicting with the
repository lock-free hot-path rule. The current configuration fields already
exist but are unused and describe `free_flush_max_batch` as a minimum trigger.
The root free and compaction contract is
`doc/design/diskdb/design-crowdb-diskdb-zone-management.md` §4 and the summary
in `doc/design/diskdb/design-crowdb-diskdb.md`.

Concrete workloads are mass object deletion and chunk GC issuing many
simultaneous `FreeBlocks` RPCs to the same bound data group. The optimization
must reduce their KV proposals without weakening “successful response means
all requested free facts are durable.”

**Solution**: Opportunistically coalesce concurrently queued free RPCs into
bounded KV batches. Flush immediately under a single lock-free drainer; use no
timer and never acknowledge buffered state.

1. Add a `FreeBatch` service component backed by a lock-free MPSC queue, an
   atomic drainer-ownership flag, and per-request completion channels. A queued
   request retains its disk group, bind, deduplicated segment/free records, and
   completion sender. No mutex is acquired on enqueue, drain ownership, or
   completion.
2. Interpret `free_flush_max_batch` as the maximum number of free-record
   operations in one KV proposal, not a minimum wait threshold. The caller
   that wins the drainer CAS immediately removes queued work. It groups only
   requests with the same current bind, preserves each request as an atomic
   unit, and stops before the maximum unless one request alone exceeds it; an
   oversized request is persisted alone rather than split.
3. While one KV write is in flight, later RPCs accumulate naturally. The
   drainer loops over that backlog without sleeping, then releases ownership
   with a lost-wakeup-safe empty-check/CAS handoff. At low concurrency the
   first request flushes immediately, so no request waits for a future free.
4. Resolve every request in a successful combined proposal only after
   `persist_free_batch` succeeds. Then, and only then, remove matching
   tentative allocations, increment each zone's
   `uncompacted_free_record_count`, and record disk/free metrics exactly once
   per distinct segment. On KV error, resolve every covered request with the
   error and do not mutate in-memory accounting or silently re-enqueue it;
   caller retry remains idempotent because free facts are incarnation-qualified.
5. When `free_batch_enabled` is false, retain the current direct request-level
   batch path. A dynamic transition affects new submissions only; already
   queued work completes under its captured policy. Rename configuration
   comments and user-facing documentation to the maximum-batch semantics while
   retaining the existing field names and defaults for config compatibility.
6. During shutdown, close admission before stopping RPC/runtime services,
   reject new submissions, and await the active drainer and queued request
   completions. Shutdown does not provide missing durability—the normal
   response contract already does—but it prevents accepted requests from being
   abandoned during orderly service termination.
7. Add counters for input requests/records, output KV batches/records,
   coalescing ratio, queue depth, oversize requests, failures, and drain
   latency. Add deterministic concurrency, failure, dynamic-config, and
   shutdown tests plus a diskdb benchmark case that demonstrates fewer KV
   proposals under concurrent frees.

Timer-based delay, success-before-persist, bitmap clearing on free, deleting
busy records on free, cross-bind atomicity, splitting one RPC across proposals,
and changing compaction semantics are not part of this requirement.

**Dependencies**:

- The landed persist-only free path, incarnation-qualified `FreeBlockKey`, and
  `DdbKvClient::persist_free_batch` are required.
- The existing `free_batch_enabled` and `free_flush_max_batch` fields are
  reused with corrected semantics. No config migration is required because the
  feature defaults off and has not been implemented.
- Compaction remains the only bitmap clearer. If a disk group's bind changes
  after enqueue, persistence uses the captured bind and normal KV/routing error
  handling returns failure; a batch never mixes old and new binds.

**Acceptance**:

- Setup one free request below the configured maximum with batching enabled;
  submit and await it without any later request; assert one KV batch is issued
  immediately and success arrives only after persistence. Invariant: no timer
  or minimum threshold can strand a request. Integration test.
- Setup many concurrent requests for one bind while the first persistence is
  held in flight; release it; assert subsequent requests are combined up to the
  maximum, no request is split, every free fact is present once, and all
  waiters resolve. Invariant: concurrent work coalesces with bounded proposal
  size and no lost wakeup. Integration test.
- Setup concurrent requests for different binds and one oversized request;
  drain them; assert no proposal crosses a bind and the oversized request is
  sent alone and atomically. Invariant: data-group and request atomicity are
  preserved. Integration test.
- Setup a failed or outcome-unknown KV batch; await covered requests; assert
  all return failure, none is secretly re-enqueued, and tentative state,
  backlog counters, and free metrics remain unchanged until an idempotent retry
  succeeds. Invariant: in-memory effects follow durable persistence exactly
  once. Integration test.
- Setup batching disabled, then toggle it on and off during queued work; assert
  direct mode matches current behavior and captured submissions finish without
  loss or duplicate accounting. Invariant: dynamic configuration changes only
  new admission. Integration test.
- Setup queued and in-flight frees, begin graceful shutdown, and race new
  submissions; assert admitted requests finish, new requests are rejected, and
  shutdown returns with queue depth and in-flight count at zero. Invariant:
  orderly lifecycle never abandons admitted work. Integration test.
- Setup the concurrent-free benchmark with batching disabled and enabled;
  assert both persist identical free facts with zero errors and enabled mode
  reduces KV batch proposals, then record the coalescing ratio and latency.
  Invariant: batching is an evidenced optimization with identical semantics.
  E2E test.

Verification commands:

- `pixi run test-diskdb`
- `pixi run test-diskdb-client`
- `pixi run -- bash tools/bench-diskdb-regression.sh`
- `pixi run -- cargo fmt --all --check`
- `pixi run rs-lint`
