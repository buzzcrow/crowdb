<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Stability Review Plan

The three requested CI repairs and local verification finished on 2026-11-04.
The user authorized pushing the completed changes on 2026-11-04.

- ContainerKV: 50 fixtures and 58 client phases pass, including two-slot isolation.
  Explicit CAS conflicts remain visible; an unknown publication outcome can no
  longer become a retryable conflict through a failed reconciliation read.
- Deployment services: 33 fixtures and 41 client phases pass. Strict bucket-row
  classification fixes the reproduced multipart metadata collision. The original
  restart HTTP 500 lacks its offending row, so historical attribution stays qualified.
- Console native-data: the original ordered 14-case baseline passes. The fixed
  image passes the original browser case and all nine affected node cases; retain
  five Dataset baseline passes whose relevant inputs did not change. This is
  incremental coverage, not a claim that all 14 ran on the fixed image.
  Complete plans keep observing current processes. All 381 UI tests and types pass.

CI evidence from repository root:
`.crowdb-runtime/artifacts/ci/new-failures-20261104/local-verification/`.
`console-fixed-coverage.json` records exact case membership and image qualification.
Original failures and repair RED/GREEN evidence remain in the three job directories.

## Remaining task: deferred historical attribution

- [ ] **P13 first aggregate WAL release error**: Identify its exact historical
  partition/phase and durable release outcome if further attribution is required.
  Six failing streams map to exact partitions, but the saved aggregate error does
  not identify the first branch. A real-worker reproduction establishes that
  idle liveness failure followed by authority revocation returns `WriteStalled`
  before owner release with zero payload IO; healthy idle release and transient
  recovery both pass. This warning alone does not establish an unfinished payload
  write or corruption. No production fencing change is justified by that evidence.
  The next investigation needs typed partition/stream/epoch/lifecycle/release-phase
  and first stalled cause in a local server reproduction. Preserve lease
  exclusion and replay. The three controls, fmt and focused clippy pass.
  Evidence from repository root:
  `.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z/repair/readiness-probe-budget-2s/wal-drain-research-20261104/findings.md`.

Completed execution, probe review and resource-closure tasks have been removed.
