<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Stability Review Plan

Run: https://github.com/buzzcrow/crowdb/actions/runs/41816831055

All four failed jobs were repaired and locally verified on 2026-11-04:
AccessTests completes every original stage; ContainerKV passes 50 fixtures and
58 client phases with isolation; Console native operations passes 19 owners;
Console native data passes all 14 original owners. Rust fmt, all-target clippy,
314 fixture controls and CI task coverage pass. Original failures remain in the
evidence; this records local verification, not a later remote CI result.

As requested, this persistent plan retains only unfinished follow-up work.
Completed CI execution and review tasks have been removed.

CI evidence from repository root:
`.crowdb-runtime/artifacts/ci/run-41816831055/`.

The Dataset import follow-up in
[run 41829384144](https://github.com/buzzcrow/crowdb/actions/runs/41829384144/job/126583192879)
is repaired and locally verified: preparation and browser execution each retain
360 seconds in the same fixture, instead of sharing one client budget. All five
Dataset owners pass at CI concurrency, including full production placement,
512 records, 12 partitions and three owners. Original browser assertions remain
unchanged. All 316 runner controls, Rust format, clippy and CI coverage pass.
Evidence from repository root:
`.crowdb-runtime/artifacts/ci/job-126583192879/`.

## Remaining task: deferred historical attribution

- [ ] **P13 first aggregate WAL release error (deferred)**: identify its exact
  historical partition/phase and durable release outcome if further attribution
  is required. Saved logs map six failing streams but do not identify the first
  aggregate branch. A local server reproduction needs typed partition, stream,
  epoch, lifecycle, release phase and first stalled cause. Preserve lease
  exclusion and replay. Existing real-worker controls pass and establish
  `WriteStalled` before release with zero payload IO after authority revocation;
  they do not justify a production fencing change.
  Evidence from repository root:
  `.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z/repair/readiness-probe-budget-2s/wal-drain-research-20261104/findings.md`.
