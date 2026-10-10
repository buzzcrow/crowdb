<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Multi-node Deployment Plan

Upstream: [requirement](../backlog/R227-console-multi-node-deployment.md),
[design](../design/deploy/design-crowdb-deploy.md).
Goal: system-Docker node containers discover peers and bootstrap/manage one
selected cluster from any UI, with durable identity and fail-closed authority.

## Review and scope

- Base: fetched origin/main at 435bee75f; branch task-deploy.
- Existing design edits preserved in stash and applied to the new branch.
- Runtime scope: Docker first; containerd and other OCI runtimes deferred.
- One image and shared monitor/UI implementation. Single-node is an automatic
  bootstrap/virtual-disk startup policy with read-only UI and backend enforcement;
  multi-node is manual bootstrap. Do not create parallel service stacks.
- Review gaps: system/init retries check only replica identity; preview starts
  services before UI; no node discovery management endpoint exists.
- Identity fingerprints must cover operation, cluster and complete initial
  membership, not merely local replica ID. Generic store/group handlers must
  not bypass system-resource ownership.
- Management grants need execution-time quorum validation and fencing; caching
  an earlier successful read is insufficient. Destructive cleanup needs durable
  terminal operation state and delayed-command rejection.
- SSH approval must validate monitor-to-host identity association; credentials
  never enter persisted drafts, logs or Group-0 public records.

## Tasks

- [x] **Node identity**: persist UUID outside preview bootstrap state, reject
  malformed/symlinked identity and preserve it on recreation. Files:
  container/crowdb-monitor/src/node.rs, node/identity.rs, tests/node_identity_test.rs.
- [x] **Discovery foundation**: management-interface-scoped DNS-SD, bounded
  candidate cache, conflict/foreign-cluster classification and read-only handshake.
  Verified actual loopback multicast discovery, cloned UUID conflict and goodbye.
  Files: container/crowdb-monitor/src/node/, lib/crowdb-protocol/src/mgmt/.
- [ ] **Discovery completion**: seed fallback, dynamic cluster binding,
  physical hardware/rack handshake and real Docker bridge tests. Files:
  container/crowdb-monitor/src/node/, tests/.
- [x] **Bootstrap exclusion foundation**: operation/config identity, durable pre-store-0
  acceptance, all-member preparation and same-operation recovery. Files:
  app/crowdb-kv-server/src/mgmt/system_init.rs, recovery/, protocol management
  schema, lib/crowdb-console-shared/src/ops/cluster/bootstrap/.
  Verified concurrent prepare, persistence across restart, generic-path rejection,
  same-operation init retry and no init calls after one member rejects prepare.
- [ ] **Bootstrap operation integration**: persist UI operation/config identity,
  route node startup through prepared bootstrap and add fenced terminal cleanup
  plus dynamic monitor cluster binding. Files: shared bootstrap, Web management,
  monitor node lifecycle and KV-server cleanup.
- [ ] **Node mode and Docker SSH**: monitor/UI-first mode, persistent SSH identity,
  system Docker harness and independent roots/devices; reuse automatic single-node
  bootstrap as a startup policy and enforce read-only UI. Files:
  container/crowdb-monitor/src/node/, container/single-node-container/.
- [ ] **Admission and authority**: authenticated SSH preparation, conditional UUID
  mapping allocation, operation progress and execution-time authority checks.
  Files: lib/crowdb-console-shared/src/ops/, app/crowdb-web/src/mgmt/.
- [x] **Candidate observations in Web/UI**: local-only bounded monitor proxy,
  standalone `--runtime-dir` and `--node-monitor`, node console mode and separate
  Cluster sidebar candidate list. Retain observations with a stale marker on
  monitor loss; discovery never changes racks or grants admission. Files:
  app/crowdb-web/src/node.rs, state.rs, main.rs, ui/src/shell/CandidateNodes.tsx,
  tests/node_candidates_test.rs, ui/e2e/flows/12-cluster-discovery.spec.ts.
- [~] **UI lifecycle**: admission and explicit initialization,
  publication/unavailable states, cluster selection and cleanup recovery. Files:
  app/crowdb-web/ui/src/, ui/e2e/flows/.
- [ ] **Verify and clean up**: focused crate tests, Docker integration and affected
  UI specs, fmt/clippy, permanent documentation, then requirement/index/plan cleanup.

## Files

- Monitor: container/crowdb-monitor/src/, tests/, Cargo.toml.
- Protocol: lib/crowdb-protocol/src/mgmt.rs and owning children.
- KV server: app/crowdb-kv-server/src/mgmt/, recovery/, tests/.
- Console: lib/crowdb-console-shared/src/ops/cluster/, tests/.
- Web: app/crowdb-web/src/, ui/src/, ui/e2e/flows/, tests/.
- Packaging: container/single-node-container/ and pixi task definitions.

## Verification

- Integration: UUID persistence/conflicts; bootstrap races and restart;
  admission failure/retry; ownership unavailability and delayed commands.
- Docker: real multicast on a dedicated bridge; three node roots and UI ports;
  SSH trust survives replacement; system Docker only.
- E2E: candidates, racks, publication gating, multiple UIs and recovery.
- Gates: pixi run cargo test -p crowdb-monitor; affected crate tests;
  pixi run rs-fmt-check; pixi run rs-lint; affected Playwright specs.
- Server-spawning suites are prefixed with pixi run clean-env.

## Verification results

- Monitor full test suite passed after identity/discovery foundation (including
  actual multicast, HTTP handshake and all existing single-node tests).
- Workspace rs-lint passed for the discovery foundation; subsequent bootstrap
  changes require a fresh gate.
- KV-server focused clippy passed after system-store ownership changes.
- System Docker client/daemon available, version 28.3.3.

- KV-server system_init_test: 7 passed, including new prepare race/restart cases.
- Console bootstrap_intent_test: 5 passed; bootstrap_prepare_test: 1 passed.
- Protocol full suite passed; workspace rs-lint passed with bootstrap API and
  shared all-member preparation path.
- Discovery regression tests and focused monitor clippy passed after SIGTERM
  shutdown and multi-interface self-observation handling adjustments.
- Registry-owned bootstrap execution gate: system_init_test passed (7 tests).
- Web candidate proxy: node_candidates_test passed (3 tests), with local-origin
  validation and discovery before any KV server exists.
- Candidate browser test uses actual monitor and isolated Web roots, without
  response interception. Passed in 3.5 seconds; baseline 2026-10-10.
- Existing Cluster lifecycle spec passed (4 tests; 3.1s, 1.4s, 5.1s, 2.3s),
  within twice the freshly measured baseline.
- Rust fmt/workspace clippy and UI build/E2E TypeScript lint passed.
