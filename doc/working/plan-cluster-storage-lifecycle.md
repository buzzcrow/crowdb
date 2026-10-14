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
- [x] **Disk observations**: expose bounded monitor inventory, reject duplicate
  resources and mismatched launcher identities. Inventory tests and Clippy pass;
  real raw-device/containerd acceptance remains in integration verification.
- [ ] **Effective admission**: add Group-0-gated registration, default file
  creation and geometry validation; merge observations with authority records.
  Files: `container/crowdb-monitor/src/node`, `bootstrap/disk_files.rs`,
  `lib/crowdb-protocol/src/mgmt.rs`, `lib/crowdb-console-shared/src/ops/hardware`,
  `app/crowdb-web/src/physical`. Acceptance A03-A05.
- [~] **Backend initialization**: inspect existing single/bootstrap ownership,
  reuse monitor execution for UI trigger, UUID-min automatic initiation and
  three-/one-voter policy. Persist policy and reuse accepted manifests.
  Files: monitor node runtime, console bootstrap/admission, web managed routes.
  Acceptance A06-A10.
- [ ] **Offline/remove semantics**: preserve removed disk metadata and allocation
  identity, prove status application before remove, gate cascades, support explicit
  rejoin without blank initialization. Files: hardware ops, protocol hardware
  records, DiskDB/DiskIO status observation, web routes. Acceptance A11-A13.
- [x] **UI baseline**: measure affected current root-runner browser flow before
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
- Monitor now exposes read-only direct-child disk observations through private
  control and authenticated local/remote Console endpoints. Missing supplied
  inputs, mismatched types, aliases and symlinks remain ineligible; discovery
  neither creates files nor registers disks. Oversized inventories return a
  bounded explicit error. Group-0 merging and effective admission remain pending.
- `cargo test -p crowdb-monitor --test disk_inventory_test`: four cases pass.
  Affected monitor/protocol/web formatting and all-target Clippy pass.
- Rootless build prerequisite resolved after the operator installed `uidmap`;
  the existing subordinate ID ranges work and the rootless namespace probe
  passes. OCI build has reached BuildKit; browser baseline remains pending.
- Shared disk input validation now rejects capacities that truncate allocation
  units or overflow the protocol zone count. The new geometry regression and
  existing hardware tests pass (nine cases); affected all-target Clippy passes.
- Explicit OCI inputs now carry host-observed backing identity; discovery rejects
  an unverified or different inode/device at the same target path. Five inventory
  tests and 22 OCI tests pass, as does affected all-target Clippy. A disposable
  Docker bind-mount probe verified host/container identity equality on this host;
  real block-device and containerd identity acceptance remains pending.
- Monitor initialization now durably accepts one fixed bootstrap request, owns
  execution outside the request connection, resumes interrupted work, records
  failures and rejects changed inputs. Existing peer preparation, bootstrap
  publication and binding remain the authority. Cleanup rejects an active task.
  Two lifecycle tests and affected all-target Clippy pass; HTTP/native container
  verification is in progress. Auto join and single-mode convergence remain open
  implementation tasks, not completed acceptance.
- Node-mode init now responds with accepted progress and exposes
  `/api/cluster/initialization`; callers must observe completion. Existing UI
  success wording and recovery progress still need updating after the baseline.
- Existing Web node-candidate and management-route tests pass (four cases),
  as do the six native fixture observation tests. Native fixture now asserts
  `202` acceptance before observing actual Group-0 publication.
- Full `cargo test -p crowdb-monitor` passes. Current UI component baseline:
  35 files / 186 tests pass in 18.13 s; TypeScript check passes. Existing ReactFlow
  `act(...)` warning is present before visible edits. Browser baseline is separate
  and remains pending the OCI build.
- Offline implementation must keep the disk alive through fsync completion:
  `DiskioServer::handle_fsync` currently does not capture its shared disk handle.
  `Group0Sync` also unregisters the fd before outstanding references drain.
  Preserve fd registration until quiescence, then expose applied exclusion;
  do not infer it solely from absence in the active disk map or heartbeat loss.
- Rootless OCI build and import now succeed. Exact three-node browser baseline
  reached fixture bootstrap but failed before browser execution: monitor SSH used
  Console's process-default trust store instead of admission's pinned peer keys
  (`ssh: Unknown server key`). Use strict OpenSSH peer control with the existing
  node key and pinned `ssh/known_hosts`; re-run the same acceptance after rebuild.
- Container replacement now rejects omitted or changed retained mappings before
  launching a runtime. Additive mappings remain allowed; all 23 OCI helper tests
  pass and preserved mapping records/bytes are checked by regression coverage.
- Strict admitted-peer control fixes the native bootstrap failure. Exact
  `console::native_browser::hardware_capacity_and_dashed_status` passes with the
  rebuilt release image; Playwright baseline is 1.3 s (2026-10-14), root case
  12.22 s. The fixture observes both monitor completion and published authority,
  and reports monitor failure immediately. Seven fixture observation tests pass.
- Cluster center now observes monitor initialization phases, fixed voters and
  operation errors, retains stale observations, and retries the same members.
  Closing the component only stops polling. Node-mode submission says accepted
  rather than successful. All 189 UI tests and TypeScript pass. Rebuilt-image
  browser regression passes in 1.6 s versus the 1.3 s baseline, preserving all
  hardware/capacity assertions and checking monitor completion in the center.
- Startup mode is persisted with atomic first-writer publication. Conflicting
  standard/single starts reject without rewriting policy or bootstrapping data.
  Existing single-profile manifests retain identity and validated profile/config
  checks; legacy standard acceptance cannot be reinterpreted as single mode.
  Full monitor tests pass, including concurrent mode publication. Shared execution
  convergence and automatic admission remain separate unfinished work.
- Auto join now persists an enabled-by-default standard-node policy, gates work on
  authenticated admission and Group-0 availability, selects at most three initial
  candidates in UUID order, and dispatches accepted execution to the minimum-UUID
  monitor. Later admission uses the existing nonvoting path. A pinned target with
  no binding blocks replacement bootstrap. The Cluster panel exposes the setting,
  target, waiting count and authentication failures; stale observations disable
  policy mutation. Five backend regression tests and 192 UI tests pass; focused
  all-target Clippy passes. Browser policy persistence and automatic bootstrap
  acceptance remain pending; target selection and shared single execution remain
  unfinished. Manual acceptance fixtures explicitly disable automatic scheduling.
- Bridge recovery fault injection now stops only the peer SSH listener, producing
  immediate connection refusal instead of a stopped container's ARP timeout. The
  original partial-preparation/recovery assertions remain; rerun is pending the
  next committed release image. Native fixture observation tests pass (eight).
- Real three-node automatic bootstrap reached active authority and verified the
  minimum-UUID monitor's accepted operation. The downstream hardware case then
  failed at ordinary Group 1 creation (`409 group 0/1 already exists`): the new
  worker unnecessarily requested a data group. Automatic initialization now
  requests only Store 0 / Group 0; the release fixture asserts that accepted
  choice. Rebuilt-image rerun is pending; no assertion was weakened.
- Bridge admission profile passes on image `9ddf0082`: browser preparation 7.4 s,
  cross-UI resume of the fixed operation 4.9 s, independent cluster cleanup 3.6 s,
  authenticated node update 11.3 s. The profile also verifies cancellation fencing,
  later nonvoting admission, remote service lifecycle, minority authority gating,
  snapshot/WAL catch-up and cleanup. Artifact: run-b5ae534af16044099d11e4db5f0036a4.
- Rebuilt-image automatic-bootstrap and hardware browser acceptance passes on
  `f9c0ec96` (run-a586ef734f9241c08ab1c5990a103f9b): each fresh node defaults enabled,
  three authenticated nodes initialize without an init submission, accepted
  membership follows UUID order, the minimum UUID monitor owns execution, and
  every replica reports exactly three voters. No ordinary data group is created.
  Browser policy toggles survive reload and preserve all prior capacity/status
  assertions. Playwright 2.4 s versus initial 1.3 s (below 2x); root case 13.91 s.
- Navigation baseline before consolidation (2026-10-15), exact canvas scope on
  image `f9c0ec96`: Fit All 1.6 s, nested collapse/context menus 4.6 s, pan reset
  0.962 s, view switching/autofit 2.0 s. All four pass; artifact
  run-5e30721f7c524643a91d2d2b0ed8af7d. Shell baseline is running separately.
- Exact shell baseline passes before consolidation: unavailable-backend 0.428 s,
  embedding/readonly/module opt-out 3.1 s, seven-domain switching 1.1 s. Artifact
  run-2c7360fe19e9485ba61c6bcbccae2b17. Native tree baseline is running separately.
- Native shell/tree baseline passes in 4.5 s before tree consolidation; artifact
  run-1d088c046a9246388c6cfdce37eefcc8. Cross-view node navigation baseline follows.
- Cross-view node baseline passes: replica navigation 4.0 s and node-to-store
  navigation 1.7 s; artifact run-1dbce2b41b6b40839a61728cbf04b520.
- Single-profile system replica creation now calls the same
  `initialize_system_replicas` prepare/create/wire driver as standard accepted
  bootstrap. Its existing manifest identity and profile metadata/storage stages
  remain intact. Ready restart verifies durable KV bootstrap acceptance as well as
  replica geometry. Four monitor KV tests pass, including lost response without
  duplicate creation and foreign-bootstrap rejection; seven shared intent,
  prepare-barrier and replay tests pass. Full single release acceptance remains
  pending; profile-specific metadata publication is still separate.
- Auto join target selection now permits an explicit initial local draft. A pinned
  target can be cleared only in an unbound workspace with a matching completed
  cleanup record; loss of binding alone is insufficient. Six backend tests pass.
- Rebuilt `e733a2d0` release reaches single-mode readiness. Its existing host
  regression incorrectly creates an unrelated random manifest after the monitor
  has initialized; strict acceptance now correctly rejects it. Update the fixture
  to export actual acceptance and assert unchanged identity across restart, one
  voter per group, foreign-manifest rejection and acknowledged data preservation.
- Browser auto-join regression exposed a controlled-checkbox race: the POST
  succeeds (200, 9.8 ms) but React restores the old check before the reply. Keep
  the pending choice disabled with explicit Saving status; restore authoritative
  state on failure. All 194 UI component tests and TypeScript pass, including
  delayed failure and intervening poll coverage. Release browser rerun pending.
- Full fixture unit gate: 136 pass, one pre-existing Dataset selection-count
  failure (`selection_test.py:107`, expected 2, actual 3). The extra SDK case
  was introduced by `257508ada5`; the assertion predates this work. Rust E2E
  all-test Clippy and formatting pass. Existing `ts-lint` points at removed
  `ui/tsconfig.e2e.json`; direct native-client typecheck also encounters old
  cross-source imports in untouched catalog specs. UI application types pass.
- Single release acceptance now passes on `69a0cb32` before and after container
  replacement (run-d276efeb561848638aa9b975ab657f09): original ready manifest
  preserved, system acceptance matches it, each group has one voter and its
  configured leader, foreign acceptance rejected, acknowledged KV value retained.
- Rebuilt auto-join browser acceptance passes in 2.4 s with explicit save-complete
  observation before reload/navigation. Automatic three-node bootstrap and all
  existing capacity/status assertions pass (run-352f07693d664ab1a11a15133decfa0e).
