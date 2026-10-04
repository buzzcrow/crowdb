<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI Follow-up Plan

Persistent issue list requested by the user. Remove completed tasks and record
new defects with their authoritative behavior and focused acceptance.

Design: [Console UI specification](../design/console/design-crowdb-console-ui.md).
Verification: [Console acceptance](../../app/crowdb-web/ui/e2e/README.md).

## Current tasks

No unfinished tasks remain in the approved UI scope.

## Completed acceptance

- Ordinary real-backend browser suite: 57/57 passed, about 2.4 minutes.
- Main owned three-node native chain: 21 collected, 18 passed, three explicit
  fixture-specific skips; 95.52 seconds including 8.426-second owned teardown.
- The separate production split browser case passed in 29.1 seconds. The
  combined weighted fixture passed in 326.42 seconds, reached 4/4/4 ownership,
  proved actual byte-weighted transfer and checked all 512 exact values.
  Owned teardown completed in 5.762 seconds.
- Separate actual 100-extent Journal replacement passed; its browser case took
  2.9 seconds and the full chain 42.90 seconds. Prerequisite arrival remains
  covered by the ordinary lifecycle suite rather than counted as a native skip.
- Docker release policy, image smoke and full container acceptance passed:
  interrupted bootstrap, real managed UI, public client writes, topology-write
  rejection, all seven services' crash/hang recovery, persisted-volume restart,
  restart exhaustion, changed identity/corrupt configuration rejection,
  anonymous volume and monitor death. Host-native mode is separate evidence.
- TypeScript, Rust formatting/workspace Clippy, affected Rust tests, C++/FFI
  suites and tree lint passed. Tests own their resources and clean up on failure.

## Retained behavior

- Ownership displays two independent 1024-slot maps: Storage Group and Serving
  ChunkDB. Group/CDB selection shows its bitmap; Node/Rack selection combines
  owners per layer, with gray outside scope and explicit unknown membership.
  These are Chunk ownership slots, distinct from Chunk-KV partitions.
- Page, Journal, Iceberg and S3 inspection remains bounded and tied to exact
  catalog/owner/reference generations. Navigation restores selection and windows;
  deleted resources, stale cursors, outages and late responses cannot enable
  actions using stale state.
- The persisted balance cooldown is one minute. Healthy movable data and an
  idle healthy owner must make actual placement progress within 40 seconds.
  Request, heartbeat, lease and independent transfer safety budgets are unchanged.

## Metrics decision

Metrics entries, panels, polling and unused UI types have been removed.
If metrics publishing is requested later, clients connect their own time-series
database. Publishing is a future product task and has no UI implementation here.
