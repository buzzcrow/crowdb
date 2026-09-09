<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# EC Implementation Follow-ups

This file records issues discovered while implementing R113, R136, and R137.
Resolved items are removed; any remaining item includes its observed impact,
current safe behavior, and the work needed to close it.

## Active

- **Consumed-reservation recycling fence:** current DiskIO requests carry only
  physical addressing, not `allocation_ts`. Until end-to-end generation fencing
  exists, lease expiry may reclaim only never-consumed reservations. A consumed
  reservation must be confirmed or remain persistently tracked; it must never
  be recycled merely due to elapsed time.
- **Completed-group crash takeover:** parity failure in a live writer admits the
  existing durable conversion task, but a process crash after the eighth mirror
  confirmation and before that admission can leave a complete reservation
  group for the generic metadata scanner to discover later. Add a reservation
  scanner that directly converts complete groups into deterministic tasks and
  cancels stale incomplete groups after applying the consumed-block fence.
- **Global reservation admission bound:** each writer has a bounded one-group
  prefetch and pipeline count is bounded, but ChunkDB has no independent
  cluster-wide reserved-block/byte quota. Add atomic admission accounting and
  gauges so many clients cannot collectively exhaust tentative capacity.
- **Benchmark reset readiness:** `cluster clean` can report a KV leader before
  every ChunkDB range owner has republished its binding. A benchmark started in
  that window fails an initial allocation with `chunk bucket not in owned
  ranges`. Add a range-ownership readiness barrier to the local cluster tool.
- **Foreground EC throughput attribution:** the complete 20-second real-EC
  matrix passes correctness/accounting sentinels, but the higher-concurrency
  cases run below the R113 mirror-only checkpoint because they include parity
  CPU, parity DiskIO, and initial 28-block reservation latency. Retain a longer
  steady-state A/B with parity byte counters and separate startup timing before
  setting a strict EC-enabled TPS threshold.
