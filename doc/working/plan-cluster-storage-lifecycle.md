<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Cluster Storage Lifecycle Plan

Upstream: [R235](../backlog/R235-console-cluster-storage-lifecycle.md).
Goal: implement shared initialization and Group-0-authoritative OCI disk management
in one Cluster workspace, preserving existing data and bootstrap exclusion.

## Execution

- [x] **Runtime storage inputs**: inspect `container/oci-image/runtime.py` and
  `run-node.py`; map permitted file/device inputs into the common disk directory,
  validate duplicates/types, retain startup inputs, and scope local cleanup.
  Tests: `container/oci-image/tests`, container helper tests. Acceptance A01-A02.
- [~] **Discovery and effective admission**: add monitor disk inventory and
  Group-0-gated registration, default file creation and geometry validation.
  Files: `container/crowdb-monitor/src/node`, `bootstrap/disk_files.rs`,
  `lib/crowdb-protocol/src/mgmt.rs`, `lib/crowdb-console-shared/src/ops/hardware`,
  `app/crowdb-web/src/physical`. Acceptance A03-A05.
- [ ] **Backend initialization**: inspect existing single/bootstrap ownership,
  reuse monitor execution for UI trigger, UUID-min automatic initiation and
  three-/one-voter policy. Persist policy and reuse accepted manifests.
  Files: monitor node runtime, console bootstrap/admission, web managed routes.
  Acceptance A06-A10.
- [ ] **Offline/remove semantics**: preserve removed disk metadata and allocation
  identity, prove status application before remove, gate cascades, support explicit
  rejoin without blank initialization. Files: hardware ops, protocol hardware
  records, DiskDB/DiskIO status observation, web routes. Acceptance A11-A13.
- [ ] **UI baseline**: measure affected current root-runner browser flow before
  visible edits; record exact per-test timings here. Inspect tree/view/history,
  managed candidate components and existing physical/capacity flow registrations.
- [ ] **Unified workspace**: consolidate navigation and selection, common center
  scope, candidates and status actions, responsive properties drawer, init policy
  and progress. Files: `app/crowdb-web/ui/src/{shell,views,menus,managed}` and
  associated hooks/styles/tests. Acceptance A14-A18.
- [ ] **Verification and documentation**: run focused component/runtime/browser
  tests, then fmt/lint separately; update permanent Console/deployment/OCI guides.
  Commit coherent passing tasks; remove requirement/index entry/plan only after
  full acceptance is verified.

## Verification

- Unit/integration: `pixi run test-oci-image`, monitor crate tests, console and
  storage component gates, UI components/types. Add tests under crate `tests/`.
- E2E: use root container runner and installed browser; preface server-spawning
  tests with `pixi run clean-env &&`. Record failing cases and complete errors.
- Gates: `pixi run rs-fmt-check`, `pixi run rs-lint`, plus C++ format/tree-lint and
  C++ tests if C++ changes. Run only relevant gates per coherent commit.

## Progress and evidence

- Initial working tree contains the authored R235 and backlog-index changes only.
- Read implementation/coding/UI/plan skills and enumerated Pixi tasks.
- Existing OCI runner accepts raw devices but maps original paths; common disk
  targets and external files need implementation. Existing bootstrap protection
  must remain authoritative; no additional distributed scheduler is planned.
- Runtime input implementation passes all 22 `test-oci-image` cases, including
  common file/device targets, duplicate inode/device detection and retained mapping
  records. Shell syntax passes. Actual container/device acceptance remains in the
  integration phase; these unit tests mock runtime invocation.
- Local helper now uses a named data volume, restarts existing stopped containers
  with original mounts and cleans only the selected container, retaining volumes.
