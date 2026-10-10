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
- [~] **Bootstrap operation integration**: persist UI operation/config identity,
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
- [ ] **UI lifecycle**: admission and explicit initialization,
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

- Real Docker bridge acceptance passed: three independent volumes and monitors,
  no peer list, mutual Ed25519 SSH, identified bootstrap and all three UIs
  reading the same Group-0 cluster/topology. Later cleanup/admission extensions
  need fresh Docker verification.
- Full monitor suite passed after shared node discovery/automatic startup changes.
- KV system_init_test passed 8 tests including explicit cleanup, delayed-operation
  fencing, new-operation reuse and restart.
- Current additions: sealed bootstrap, private monitor control, SSH operation
  tagging/cancellation, Group-0 CAS admission allocation, UI retry/cleanup,
  per-node bindings and automatic single-node discovery/read-only enforcement.
- Remaining verification/implementation: cleanup with unreachable members,
  post-bootstrap admission/cancellation, quorum partitions and deployment grants,
  remote managed-service lifecycle, cluster selection/reconfiguration, production
  resource inputs, Docker host-network and single-node regression, full gates.

- Later-node admission exposed wildcard RPC listeners in discovered topology.
  Client normalization now resolves only wildcard local listeners through the
  reporting management origin. Real RPC regression topology_endpoint_test passed.
- system_init_test passed all 8 cleanup/exclusion cases after generic cleanup fencing.
- Focused Cluster lifecycle/discovery browser verification passed 5 tests (2.8s,
  1.5s, 5.1s, 2.5s, 2.7s); cluster filtering is covered.
- Service intents now persist in Group 0; target monitor validates current intent,
  retains execution, recovers committed requests and reports application probes.
  Cross-UI lifecycle, DiskDB and KV routing require refreshed Docker verification.
- Four-node Docker extension currently awaits rerun after RPC topology fix.
  Single-node full container regression awaits rerun with container-internal curl.

- Refreshed complete monitor suite passed. Explicit seed expiry and live bindings
  use real HTTP and multicast transports.
- Four-node bridge suite passed with application RPC health, shared service intents,
  cross-UI service lifecycle, minority management rejection and explicit cleanup.
- Host-network production launcher passed digest pinning, explicit secret mount,
  resource boundaries, bootstrap, container replacement and identity/key recovery.
- Native S3 multipart browser reproduction passed after restoring the KV client
  FFI build; the full console gate is running again.
- Bootstrap recovery now copies validated public fixed inputs and authenticated
  private service credentials to each participant before starting its KV replica.
  Other UIs recover the same operation independently of the initiating UI.

## Resumed verification

- Command: `pixi run bash -c 'export CROWDB_CONTAINER_IMAGE=crowdb-node:r227; bash container/single-node-container/tests/container-e2e.sh'`.
- Five root-cause-driven runs remain unsuccessful:
  - Initial discovery identity creation raced automatic bootstrap's empty-root check.
    Identity creation is now synchronous before the single-node bootstrap task.
  - The persistent-root symlink assertion ran as root after adding the SSH entry
    point. It now checks using the container's service user.
  - The single-node internal Access health listener required container-local curl;
    curl is now packaged and that private listener is probed inside the container.
  - A KV client build omitted `ffi`, removing DiskIO C ABI symbols. Packaging now
    preserves that feature and checks unresolved symbols before image assembly.
  - The fifth run passed boot, client writes/readback, browser checks, crash/hang
    recovery for every service, persisted-volume restart and restart-budget checks,
    then failed the corrupt-manifest diagnostic assertion.
- Exact final boundary: `verify_invalid_manifest_rejected` observes nonzero
  container exit, then requires `docker logs ... | grep -F 'Manifest('`.
  Actual log: `Error: "preview manifest failed: bootstrap manifest cannot be decoded: expected value at line 1 column 1"`.
  The invalid manifest is rejected; the old diagnostic spelling is absent.
- Root cause: the same-image single/manual command wrapper now returns the
  contextual display error rather than the former debug enum representation.
  After user authorization, the assertion retains nonzero exit and checks this
  concrete decoding diagnostic. The invalid-profile case similarly checks its
  contextual decode diagnostic.
- Proposed continuation: retain the nonzero-exit assertion, match the concrete
  manifest decoding diagnostic, rerun the complete container suite, then finish
  multi-node browser/recovery coverage, module/documentation cleanup and full gates.
  Alternative: restore the former CLI diagnostic representation, preserving the
  old test spelling, then repeat the same acceptance gate.
- The implement-requirement skill requires recording/committing this state and
  requesting a human decision after five failed root-cause-driven attempts.
- R227 remains open. Recent full workspace clippy passed; focused KV bootstrap
  tests passed (8), the prior four-node application-health suite and host-network
  replacement suite passed. Latest monitor/protocol rerun is finishing. The full
  console gate was stopped before completion; full console UI has not run.
- All implementation edits are retained in the worktree. The most recent browser
  coverage and cross-UI manifest recovery additions still require execution and
  must not be described as verified or complete.

- User confirmed continuing R227; R228 is unrelated and remains untouched.
- Latest monitor and protocol full rerun completed successfully.
- Latest release build and workspace rs-lint passed. Full test-console is running;
  shared-console and CLI tests passed and Web tests are in progress.
- First resumed single-node run failed because the shell source was edited while
  running, producing an unexpected EOF. Run a fixed temporary script copy and
  pin the image digest for the next full execution.
- First real Docker UI run found missing static assets: manual Web startup omitted
  the profile's UI directory. Added explicit --ui-root and Web log-directory
  arguments; image rebuild and browser rerun are pending.
- Monitor acceptance now distinguishes reserved identity from complete preparation.
  Automatic KV startup waits for complete manifest/credential persistence.
- Added Group-0 regression cases for bootstrap registry retries preserving later
  admissions, service retries retaining operation identity, and cross-node service
  ID collision rejection. Their focused execution is pending.
- Remaining contract gaps to implement/verify: authenticated endpoint/rack updates
  with stable IDs; cancellation after target KV preparation; interrupted bootstrap
  recovery from a second UI; disjoint-cluster cleanup and voting catch-up; data-group
  operation during Group-0 loss; partial cleanup with unreachable nodes; final
  permanent docs and full gates. R227 must remain open until these are handled.

## Blocked

- Resumed Docker command: `pixi run bash -c 'export CROWDB_CONTAINER_IMAGE=<pinned-image-id>; python container/single-node-container/tests/node-containers.py'`.
- Five root-cause-driven Docker browser runs failed after user authorization:
  1. Manual Web startup omitted the static UI directory. Added --ui-root.
  2. The admission dialog was trapped in the fixed sidebar stacking context.
     Moved admission/recovery dialogs into a portal.
  3. Preparation restart coverage used the previous randomly published host port.
     Refresh Docker's actual port mapping after restart.
  4. The body portal escaped the console's scoped CSS. Portal now targets the
     nearest .crowdb-console root, retaining embedding styles and ownership.
  5. Real mutual SSH preparation for the third node exceeds the 3-second UI
     assertion budget. No assertion timeout increase or sixth run was made.
- Latest image: sha256:66c08eb74c3daeec89ebfab6a52b54e3cdb486e592f848a656c1271614eb5ac2.
- Exact latest boundary: 12-cluster-discovery.spec.ts:91 expects the admission
  dialog count to reach zero within 3000 ms; it remains open with Verifying SSH.
  Trace records successful first/second admission responses at 368 ms / 2048 ms;
  the third request has not completed when the assertion ends. This establishes
  the first slow boundary, but does not prove whether the third operation would
  eventually succeed.
- Proposed continuation: instrument the SSH preparation stages, reuse authenticated
  sessions for each peer's key/host setup, and avoid repeated handshakes while
  preserving all-pair bidirectional strict-key proof. Rerun the same browser gate
  at its existing assertion budget, then finish the remaining contract gaps.
- Alternative: expose admission as a durable asynchronous UI operation with visible
  per-peer progress, testing the pending state and final admission separately.
  This expands the UI/API implementation but supports larger clusters naturally.
- Required human continuation follows implement-requirement's five-attempt rule.
  Implementation edits and regression assertions remain preserved.
- Other gates: deployment_test passed 4 tests; workspace rs-lint passed; monitor
  runtime/integration suites passed and isolated monitor doc-test rerun passed.
  Full test-console failed native balance seed_values with Deadline; isolated
  reproduction is running. Full UI has two failures so far; duplicate-rack passes
  alone in 1.7 seconds. Single-node container regression is still running through
  crash/hang recovery with its pinned image and fixed script copy.
