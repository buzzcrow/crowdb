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
- [ ] **Backend initialization**: inspect existing single/bootstrap ownership,
  reuse monitor execution for UI trigger, UUID-min automatic initiation and
  three-/one-voter policy. Persist policy and reuse accepted manifests.
  Files: monitor node runtime, console bootstrap/admission, web managed routes.
  Acceptance A06-A10.
- [~] **Offline/remove semantics**: preserve removed disk metadata and allocation
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
- DiskIO retirement now detaches the active map before descriptor unregistration,
  retains handles until old snapshots/request callbacks drain, and delays reopen
  of the same identity. Fsync callbacks retain their disk just like reads/writes.
  New descriptor-lifetime and independent-drain tests are in the C++ gate;
  changed-source tree-lint exits 0 (existing warnings). This is the quiescence
  foundation only; authoritative Offline tokens, owner acknowledgements and
  retained removal metadata remain unfinished.
- Retirement gates pass: changed C++ formatting and tree-lint; full `test-cpp`
  (543 Tree, 56 RPC, 57 DiskIO, 31 Common, 55 Tree FFI, 25 RPC FFI cases).
  Artifact: unit/cpp-6de3ce8355cb4297b6b5e135be920f81. The initial new-test
  initializer typo was corrected before this passing run.
- Inventory now reports backing filesystem capacity/available bytes once per
  filesystem, with per-file references and separate virtual/physical allocation.
  Failed observations remain unknown with an error. Six inventory tests,
  monitor/protocol all-target Clippy and Rust formatting pass.
- Standard monitor now durably accepts private file-preparation requests for its
  confirmed Group-0 node binding and allocates off the control/heartbeat loop.
  Generated paths, retained inode identity, explicit sparse mode, host reserve,
  immutable completed capacity and visible failures are enforced. Interrupted
  unreceipted files are rejected rather than adopted; cleanup waits for active
  preparation. Six new filesystem regressions and the full monitor suite pass;
  monitor/protocol all-target Clippy and Rust formatting pass. Clippy first found
  an MSRV-incompatible helper, oversized dispatcher and test import placement;
  all corrected. File preparation is not admission: Group-0 metadata registration,
  DiskIO readiness, web orchestration and dialogs remain to be connected.
- DiskIO now applies authoritative Offline during successful Group-0 discovery,
  retaining in-flight handles but excluding new requests. It validates every
  membership identity/status before applying exclusions; malformed observations
  do not remove disks. Three new membership tests and full `test-cpp` pass
  (543 Tree, 56 RPC, 60 DiskIO, 31 Common, 55 Tree FFI, 25 RPC FFI cases), artifact
  unit/cpp-60619b7fc26c471c99d247a47e33130d. Changed-source formatting and tree-lint
  pass (existing warnings in the sync source). Per-revision owner/DiskIO exclusion
  acknowledgements and guarded removal remain unfinished.
- Removal implementation must also fence `HardwareClient::set_disk_status`:
  its current read/default/write can recreate a missing disk with empty geometry
  or overwrite concurrent status. A removed marker alone is insufficient while
  direct status writes and cascade deletion bypass the authoritative lifecycle.
- Disk status setters now require a present record and use that disk revision
  for CAS; matching status is a read-only no-op. A revision-bearing read is
  available for exclusion observations. Two real Group-0 regression tests pass,
  covering absent/deleted records, preserved geometry and no-op revisions;
  kv-client all-target/all-feature Clippy passes. Cross-record removal fencing
  and lifecycle-aware cascade guards are still pending.
- Rootless OCI release `ca41589b` builds/imports successfully. Native automatic
  bootstrap/hardware acceptance passes, including new private-control assertions:
  unbound file preparation rejected, bound preallocation completes with matching
  inventory identity and actual allocation, no fabricated Group-0 membership,
  changed-intent replay rejected. Browser 2.3 s (initial baseline 1.3 s), root
  case 12.54 s; artifact run-3f1918ea27be451497ddf712c5ee0078. Eight native fixture
  observation tests pass. The first direct Python invocation omitted PYTHONPATH;
  corrected with `PYTHONPATH=e2e-test` before the passing run.
- Fresh baseline before the navigation/tree consolidation on `ca41589b`:
  shell embedding 2.8 s, unavailable backend 0.453 s, domain toggle 1.1 s
  (run-ff5a36671a3343dab80e233a5ed0902f); native shell/tree 6.0 s
  (run-acb8eb1a94bb41a08ce0c525ea1cafc2). All pass. Shared Cluster view,
  physical Storage branches and canonical Capacity links are now in progress.
- Cluster now owns the Capacity view and canonicalizes old Capacity links.
  Selection/history and one Services/Storage tree are shared; physical groups
  no longer depend on DiskDB ownership. Storage expansion remains lazy. All
  196 UI component tests and application TypeScript pass. Release browser
  regression is pending; lifecycle dialogs and independent center/inspection
  selection remain unfinished.
- First consolidated shell run: embedding 2.8 s and unavailable-backend 0.452 s
  pass; domain toggle fails because the existing fixed error banner covers the
  new center Topology button. Trace confirms pointer interception, not failed
  navigation. Move the error into normal center layout; keep the actual browser
  click as the regression assertion. Artifact run-39995fe2eeef4f4c9aa2916831e12c69.
- Rebuilt `e466c5ab` passes the exact failed toggle in 1.3 s (baseline 1.1 s),
  artifact run-c1cfcc86c13a4cbc8f114a791f33da81. Native tree/shared selection and
  expansion pass in 5.1 s (baseline 6.0 s), run-c694ae8637fc4fa4a4e9094aa9b383f2.
  Automatic bootstrap/hardware capacity passes in 2.1 s, with file preparation
  checks retained (run-ddb03e2bc84940eb9d1149e34729db2b). Disk Inspector/physical
  ownership and both groups pass in 5.0 s (run-ad2da3869fef4d0a8c0a790f56672611).
  Four canvas regressions pass: Fit All 1.6 s, nested collapse/menu 4.9 s,
  pan 1.0 s, view switching 2.0 s (run-99e2109a829e474db8ebb5423e5793ec).
  Six-node service/storage/data flow is running before the next UI stage.
- Six-node flow stopped at an outdated synchronous-init assertion: expected 201,
  received durable 202 with operation ID and preparing phase. Update it to assert
  accepted identity/members, observe that same operation through active (fail fast
  on failed), then verify the ordinary group leader. Preserve every data assertion.
  First attempt: run-2cb131295a9f43c8928bf530675baa2b.
- Six-node flow now passes, including fixed accepted initialization, three storage
  nodes, three later diskless nodes, all services, and real KV/S3/Iceberg round
  trips. Browser about 1.2 min against prior 68.0 s; step timings retain deployment
  and disk-load costs. Artifact run-b3989028aadf41d6a01618796094652a. Navigation
  consolidation is verified; center/inspection separation and responsive layout
  are the next UI task.
- Cluster now distinguishes the center navigation scope from property selection.
  Capacity row/card bodies inspect in place; names enter, tree/breadcrumbs follow
  the scope, and closing properties preserves the work area. Topology names and
  expand/collapse controls are separate from card inspection. History captures
  both identities. All 198 component tests and TypeScript pass before the final
  selection-highlight change; release browser verification follows.
- Final component/type gate for independent inspection passes (198 tests),
  including scope retention on Close and distinct row Inspect/Open callbacks.
  Property selection has a separate visible/accessible indicator from tree scope.
- Release `9af1e624` verifies independent inspection: four canvas cases pass
  (1.8/6.3/1.2/2.1 s, run-c11694fadb5847529d82f1c7a730824b), and native disk
  navigation passes in 5.7 s (run-e9e9c65d60564adb80ff40fdd513784d). Closing
  properties preserves the center; breadcrumb and Back restore disk scope.
- Reset selection regression passes in 1.6 s
  (run-2e7ae0e07d8d445e871ff1e67e825a1a). Native S3 multipart, exact data round
  trip and Chunk-to-Cluster disk navigation pass in 8.2 s against recorded 5.3 s
  baseline (run-836840b7f7534834af3fee1d76abb225). The physical cross-link now
  asserts canonical Cluster scope and the actual DiskGroup breadcrumb.
- Responsive Cluster panel constraints and dismissible properties drawer are
  implemented. Below 1100 px properties no longer consume center width; keyboard
  Escape restores prior focus. Navigation can collapse and overlays below 720 px.
  All 198 UI component tests and TypeScript pass. Exact inspector selection
  baseline is 1.6 s; expanded resize/focus browser regression awaits release build.
- Responsive browser acceptance passes on `428c2d0d` in 2.4 s versus 1.6 s:
  keyboard resize, panel-to-drawer transition, retained center, Escape/focus
  restoration, navigation collapse/reopen and reset all pass. Artifact:
  run-e809432a6db340c39d98c28a339daf98. Rootless build/import remains successful.
- Fresh hardware browser baseline before observation retention passes in 2.1 s
  (run-ded1f9031e0b4e4bb775be7ffdafc2b1). Capacity now retains each successful
  source independently on refresh failure and exposes source timestamps/errors.
  Runtime observations are scoped by group/disk; another scope never inherits
  stale usage. All 199 UI tests and TypeScript pass, including failed refresh,
  recovery and scope isolation. Browser failure-injection verification follows.
- First observation browser regression stopped at the fixture's explicit ban on
  response interception (run-af494b4809bf4f2f908265555da0ed87). Replace route abort
  with actual browser offline/online transitions; retain all row/totals/error
  assertions and the existing real hardware lifecycle checks.
- Real browser offline/online regression passes on `d030e11e`: hardware disk
  rows and totals survive failed reads, source timestamps explicitly become stale,
  and refreshing after reconnect clears the error without losing rows. Playwright
  2.9 s versus fresh 2.1 s; run-8706ac7e1d4c4d4eb4638612e636896f.
- Effective admission next uses a versioned Group-0 backing record separate from
  `DiskValue`: backend, backing identity, provisioning operation, media (including
  unknown), placement and retained geometry. Existing DiskType numeric meanings
  stay unchanged. Publication must be atomic with membership, and duplicate
  backing checks must share the hierarchy mutation fence. Do not wire creation
  into the UI until publication, retry reconciliation and deletion guards exist.
- The backing-record schema now passes two protocol regressions and all-target
  protocol Clippy: round-trip retains unknown media separately from legacy type;
  unsupported version, missing identity, sparse block device and inconsistent
  geometry reject. Records use the original rack/node/group/disk placement key.
  This is schema groundwork only; no admission path publishes it yet.
- `HardwareClient::admit_disk_storage` now atomically publishes Init disk,
  DiskGroup membership and versioned backing record under the existing rack
  revision fence. Same-intent replay preserves live status; removed/inconsistent
  identities require recovery. Node-scoped bounded duplicate scans reject aliases,
  duplicate IDs and legacy occupied paths. Four live Group-0 regressions pass,
  including concurrent aliases; protocol regressions and both crates' all-target,
  all-feature Clippy pass. Monitor/API wiring remains pending deletion guards.
- Rack-fenced status setters now invalidate stale membership deletion, require
  present records, and keep Offline sticky until validated recovery. Direct and
  cascade compatibility deletion reject retained backing records; legacy cascades
  commit once under the rack fence rather than deleting children incrementally.
  Console authoritative disk/group routes apply the same retained-record guard.
- New direct-delete regression initially exposed mismatched disk-ID key encoding:
  backing keys used display hyphens while DiskKey uses 32 hex digits. Exact replay
  reproduced it; all backing lookup/deletion now use `DiskStorageRecord::key_for`
  derived from canonical DiskKey. Six admission/removal regressions, three status
  regressions, nine Console hardware/geometry tests and affected all-feature
  Clippy pass. These are deletion guards, not completed exclusion acknowledgements.
- Managed file admission now accepts immutable placement/geometry and queues up to
  four monitor-owned workers. Completed preparation publishes Init membership and
  backing identity together; an actual scoped DiskIO read gates initial Up.
  Waiting operations resume from paged durable receipts after monitor restart.
  New administrative DiskIO discovery is scoped to one disk; normal routing
  ignores provisional Init and excluded Offline disks.
- DiskDB defers authoritative Init until verified activation. Current focused
  verification passes: seven storage admission/activation tests, three status
  tests, two scoped DiskIO discovery tests plus existing topology test, eight file
  preparation tests, two Web file capability tests and three candidate tests.
  Full monitor suite, DiskDB keepalive/metadata/initialization (seven cases),
  five affected crates' all-target Clippy, workspace fmt and eight Python
  observation tests pass. Native queued/restart/DiskIO acceptance is added and
  awaits a committed-source OCI rebuild; UI creation remains unwired.
- First native file admission acceptance failed before browser execution on
  `e051440a` (run-2c3708098ede4691ab5a2258cea758c8): bootstrap wrote Node
  records but left Rack.node_ids empty. The strict admission fence correctly
  rejected the inconsistent parent and retained the prepared file. Fix the
  bootstrap publisher to include sorted per-rack node membership; do not weaken
  admission validation. Live-authority bootstrap regression plus existing
  prepare/replay tests pass (six cases).
- DiskIO now receives the retained backing record and exact disk revision through
  one rack-fenced bounded inventory observation. Open and refresh compare actual
  descriptor/path type, inode/device identity and capacity; symlinks, replacement,
  truncation and changed geometry fail closed. Eight admission/observation and
  three status tests pass, including exact wire revisions and corrupt geometry;
  affected all-target/all-feature Rust Clippy passes. All 61 DiskIO tests pass;
  full C++ and tree-lint gates are running before the next release build.
- Retained scopes now reject compatibility replacement of Rack/Node/DiskGroup/
  Disk records. First metadata binding remains allowed, but replacement or
  deletion of binding/owner is blocked; idempotent writes preserve the record.
  Owner renewal/publication shares the Rack revision fence and cannot overwrite
  retained group membership. The full KV client suite passes, as do affected
  Console hardware/bootstrap and DiskDB metadata/ownership tests. Ten focused
  admission tests include all-page duplicate detection over 320 retained records.
- Full C++ gate passes: 543 tree, 56 RPC, 61 DiskIO, 31 common, 55 tree-FFI and
  25 RPC-FFI tests. Full tree-lint exits 0 with existing warnings; new comparison
  warnings were fixed and touched-file lint exits 0. Rust fmt and affected
  all-target/all-feature Clippy pass. A cleanup command initially overlapped C++
  test runtime creation and exited before running the new pagination test; after
  the C++ gate completed, serialized cleanup/test execution passes.
- Second native run (5ff67405, run-b1f9de57dde5486ebff23b439acef200) proves
  both file operations reach waiting-for-diskio, but immediately reading the
  legacy disk list returns one cached entry. Bound storage GET routes now use
  Group-0 hardware authority directly. A component test publishes a second disk
  with no Console cache refresh and verifies both group/disk list and exact
  resource routes. Native restart and DiskIO activation still await rerun.
- Broader Web verification exposed the newly shared readonly middleware's empty
  error body. Return a JSON 403 with the readonly reason before handler execution;
  Auto join's old handler-specific 400 expectation is updated to this shared
  authorization contract. Four affected Web suites (14 cases), Clippy and fmt
  pass. Scratch Node storage UI is prepared outside the repository while waiting
  for the unchanged browser baseline; no unverified UI has entered a release.
- Third native run (f792bce8, run-d1de0944c130428db1bd1dcd9ec33ebf) verifies
  concurrent creation, protected membership, node restart and durable resume.
  Activation waits because DiskIO rejects the full JSON payload: the newly
  nested backing receipt contains raw unsigned UUID words that overflow folly's
  signed JSON parser. Outer IDs were already strings; project nested receipt IDs
  as decimal strings at the same FFI boundary. Group-0 serialization is unchanged.
  Regression now exercises u64::MAX in both words and both nested/outer identities;
  all ten focused tests, all-feature Clippy and fmt pass. No timeout increases.
- Fourth native run (a6627fa6, run-4e14e9fc58334ec7b6d76133980fe8ff) passes
  the complete file-admission fixture, including actual DiskIO reads and replay;
  `file-admission.json` records both Registered operations and retained identities.
  Browser starts but its first capacity read fails: `build_hardware_client`
  requires a warmed monitor cache and local PIDs, unavailable after node restart.
  Bound manual mode now resolves through persisted binding seeds and the shared
  KV client, independent of browser/cache warmup. The empty-cache component
  regression verifies hardware capacity directly; affected Web suites (14 cases),
  Clippy and fmt pass. Full browser rerun remains required before visible changes.

- Fifth native run (`a8275727`, `run-31a16b6d6554476e9363c3ff5552cac8`)
  passes the complete hardware acceptance, durable file creation/restart/activation
  and all existing browser assertions. Current exact browser baseline is 3.3 s
  (2026-10-15); image config `c3ab23eb482890308854c687cdd59a314a6bf2ad40a4f069457a8bf1162aba91`.
  Node workspace and managed-file dialog work now proceeds against that baseline.
- Node center now shows services/storage and durable file operations, with an
  explicit Create File Disk dialog (16 GiB preallocated default, sparse advanced,
  immutable retry), candidate counts and honest stale observations. Registered
  disk properties read retained backing identity/geometry from Group 0 with exact
  decimal strings. All 213 UI tests/types and the live authority regression pass.
  Browser continuation/creation/properties assertions are added; rebuilt-image
  execution remains required. New test helper was split after Clippy's line limit.
- Native workspace acceptance passes on `ff6c5313` with image config
  `4df579c1e261b261c545c18cd6f3cc5ff20cf1d366709ce4d79e0b676536e0d6`
  (`run-9ae4dae44375462cbbedde32cbb1afee`). Existing assertions remain; added
  actual UI file creation, monitor continuation outside Node scope, registered
  membership/backing properties and candidate navigation. Playwright 6.7 s
  versus 3.3 s baseline; all added stepTimer boundaries are below 2 s, with
  extra mutation/lifecycle/return/properties coverage accounting for the increase.
- DiskGroup admission now validates media class and allocation-unit size before
  file preparation and again under the atomic Rack fence. Unknown is distinct
  from legacy HDD; file and raw backends can share a compatible class. Retained
  removed receipts still constrain reuse. Four compatibility/race regressions
  and ten admission tests pass. A legacy-sibling fixture explicitly declares
  HDD before retained admission instead of mixing it with unknown media.
- Failed DiskDB load write-back now shares the hierarchy Rack fence as well as
  checking the exact owner generation; it cannot recreate removed membership.
  Existing four ownership regressions and the concurrent-removal regression pass.
  Web backing regression now uses an independent compatible group; it passes.
  Affected all-target/all-feature Clippy and fmt pass.
- The full monitor suite initially passed functional tests but rustdoc reported
  missing dependent crates after concurrent feature variants rewrote the shared
  KV-client output. Cleaning only that crate's generated artifacts and running
  the same full suite serially passes, including rustdoc. Serialize these Cargo
  feature variants through the remaining gates.
- DiskDB administrative exclusion now closes a lock-free per-disk metadata-write
  gate before waiting for admitted persistence. Scoped KV clients retain the same
  disk gates across async writes; stale loader status cannot reopen the disk.
  Cancelled or unknown writes poison quiescence instead of permitting removal.
  Production fenced writes reject disk IDs absent from their captured membership.
  Rejected multi-disk batches release unsubmitted permits without poisoning peers.
  Full all-feature DiskDB suite and rustdoc pass; four focused initialization/
  exclusion tests, affected all-target Clippy and fmt pass. Owner test fixtures
  now contain their actual disks, including the successor's recovered membership.
  Exact-revision Group-0 acknowledgements, removal/rejoin and browser actions
  remain unfinished; this gate is not itself a removal acknowledgement.
- Group-0 now stores versioned exact-disk-revision exclusion evidence, with
  reporter instance and exact DiskDB owner generation. Non-Offline, stale or
  wrong-location confirmations reject. Located DiskIO registrations pin physical
  identity with initial instance CAS, then share the Rack fence; clearing/moving
  identity and deleting its retained registration are rejected. TTL expiry is
  not evidence of quiescence. Three live-authority regressions plus protocol
  schema coverage, the full KV client suite/rustdoc, Clippy and fmt pass.
  Runtime services still need to publish evidence after actual drainage; no
  removal endpoint is enabled from these records yet.
- DiskDB keepalive publishes exact-revision exclusion evidence only from a
  closed, drained disk with the observed owner generation and unchanged Group-0
  geometry/status. Authority work is capped at 32 disks per tick with round-robin
  progress. A live regression holds an admitted write across Offline, verifies
  no premature evidence, completes it, then checks exact disk/owner revisions.
  All five initialization/exclusion tests, all-target Clippy and fmt pass.
  DiskIO still needs its corresponding publisher before removal can be enabled.
- Startup reconstruction now retains the original disk's shared write gate.
  A captured Up snapshot cannot create a fresh writable disk after Offline;
  already excluded disks skip baseline construction. Startup/recovery/exclusion
  regressions (15 cases), all-target DiskDB Clippy and fmt pass.
- DiskIO registers before opening media, then confirms Offline only after old
  snapshots/callbacks release the disk and descriptor retirement completes.
  Exact revisions are batched at most 16 per sync through a bounded, timed C ABI
  call; unconfirmed batches rotate and reconcile idempotently. The wire uses
  decimal strings for all 64-bit identities/revisions. Two C++ regressions cover
  drainage, old confirmations, batch fairness and unsigned limits; the Rust
  wire-boundary regression passes. Full C++ gate passes (543 tree, 56 RPC,
  63 DiskIO, 31 common, 55 tree-FFI, 25 RPC-FFI). Full tree-lint exits 0;
  focused touched-file lint has only three existing Group0Sync style warnings.
  KV all-target/all-feature Clippy and Rust/C++ formatting pass. Native runtime
  confirmation still awaits a committed-source image build. Before enabling
  Remove, persist DiskIO's configured node/group scope so a stale owned-group
  advertisement cannot hide a just-started participant.
- DiskIO now persists an immutable versioned configured node/group scope before
  opening media; removal observes every matching retained participant, including
  expired heartbeats and empty owned-group advertisements. Concurrent first
  registration and removal share the Rack fence. Five exclusion regressions,
  full KV-client/rustdoc, affected Clippy and the full C++ gate pass; tree-lint
  exits 0 with the existing warnings.
- Atomic removal requires exact Offline revision and current owner-generation/
  DiskIO drainage evidence. It retains backing, allocation records, owner and
  binding, removes membership and commits a same-revision reconciliation receipt.
  Three live removal regressions cover occupied metadata, replay, expired
  participants and concurrent registration. Protocol schema tests pass.
- Domain-monitor owner placement now also shares the Rack fence, closing a
  direct owner-CAS bypass. Existing placement assertions plus revision checks
  pass. Scoped HTTP lifecycle/Offline/Remove routes expose unmet conditions and
  decimal-string revisions; readonly rejects before mutation. Four web cases
  pass, including stale canonical revisions and receipt replay. Web/KV/server
  all-target/all-feature Clippy and formatting pass.
- Added post-browser native acceptance that allocates through the real DiskDB,
  fsyncs and reads a marker through DiskIO, then verifies Offline/Remove preserves
  underlying bytes and committed allocation metadata, rejects old access and
  permits allocation on another disk. The owned read-only marker observer has
  nine passing Python tests; E2E all-target Clippy passes. Actual container run
  awaits rebuilding this committed source. Rejoin/Online and UI actions remain
  unfinished; these backend gates do not complete A11-A13.
- First rebuilt-image hardware run failed before new removal coverage at the
  original status assertion: HTTP 502 `set_disk_status: compare-and-set key is
  busy` (`run-61548971e2c54f1aade2e3159d43c236`, Playwright 4.6 s). Root cause:
  every scoped DiskIO heartbeat rewrote Rack while one-shot hardware mutations
  exposed normal contention. Initial scope publication still shares Rack; later
  immutable-scope heartbeats CAS only the instance. A crash after initial location
  pinning leaves scope unpublished and still requires Rack-fenced publication.
  Hardware status mutation now reobserves and revalidates after CAS conflicts.
  Concurrent status and no-Rack-heartbeat regressions pass; existing assertions
  and timeouts remain unchanged. Native retry awaits rebuilt sources.
- Managed file geometry now rejects a partial last zone before durable acceptance
  or byte creation. New receipts reject unrepresented capacity; legacy DiskValue
  formats remain unchanged. Monitor preparation (nine cases), protocol storage
  schema (four), and HTTP 48-GiB rejection pass.
- Retained backing now has bounded per-node pages (maximum 64), node-scoped
  validated cursors, exact receipt identity and stable Rack observation. New
  `/api/nodes/:id/removed-disks` reads Removed candidates from Group 0 rather than
  inferring them from absent membership or current filesystem visibility. Paging,
  foreign cursor and HTTP removed-identity regressions pass. Four status, eleven
  admission, five exclusion, three removal and four web cases pass.
- Rebuilt hardware browser assertions pass at 6.5 s
  (`run-281dce267b68477b90d42c73e1736c86`), establishing the current baseline
  before lifecycle UI changes. Its new post-browser allocation returned NoSpace.
  DiskDB diagnostics showed all three Up disks with zone capacity zero: the
  16-MiB fixture has only 16 one-MiB units, rounded down by the existing 64-unit
  bitmap-word requirement. New admission now rejects non-word-aligned zones;
  the managed file API/UI require multiples of 64 MiB, and the fixture uses
  64 MiB. The default 16-GiB geometry is unchanged. No allocator semantics or
  assertions were weakened. Monitor/protocol/HTTP geometry regressions pass.
- Node storage now exposes an authority-backed lifecycle dialog. Offline is
  separate from Remove, pending owner/DiskIO conditions remain visible, exact
  revisions remain strings, and removal requires explicit acknowledgement of
  unavailable data and retained media. Unknown responses require authority
  refresh before any same-revision retry; committed receipt reconciliation does
  not repeat removal. Reads stop on close; server mutations are not cancelled.
- Removed candidates now merge bounded Group-0 pages with discovery, show retained
  original placement/properties even when missing, and avoid double-counting
  registered/generated paths. Partial counts are marked `+`; failed refresh keeps
  last-known candidates. Removal returns focus to Candidates. Component coverage
  includes exact unsigned revisions, both response-loss paths, unmet conditions,
  candidate pagination/node isolation and the allocation geometry boundary.
  All 219 UI tests/types, affected Rust Clippy and fmt pass. Extended existing
  native hardware browser flow covers both UI transitions/properties/focus;
  full rebuilt-image acceptance is still pending.
- Full native hardware acceptance passes on committed image `bc45d1d7`
  (`run-4ea2b66540e843d0baeb5866a7445d12`, config
  `sha256:d53d10afb05285cdb2aa44127fc29e77b93c84e2bb1b75b10cbfd948e47d50f3`).
  Browser 9.0 s versus 6.5 s baseline; the additional actual owner/DiskIO
  exclusion boundary is 2.428 s, with assertions still at 3 s. Actual occupied
  removal prints allocation/bytes retained, old I/O fails and another disk
  successfully allocates. No response interception, wait inflation or retries.
- Follow-up UI correctness: mutation refresh waits out older in-flight membership
  and retained-candidate reads, then observes anew. Reinspecting an already selected
  disk and committed mutation refresh its backing properties. New regressions
  cover all three cases; existing native flow now keeps the Inspector on the disk
  across removal. This small extension awaits focused browser verification.
- Explicit Rejoin now has a versioned Group-0 receipt tied to the exact prior
  removal. The Rack-fenced SDK restores the original ID, placement and geometry
  Offline; it rechecks compatibility and aliases, retains all allocations and
  does not reopen status or initial activation. Same-source replay preserves the
  current status/revision. Two live SDK cases plus five protocol cases pass.
  The initial new test incorrectly expected a creation replay to fail; the
  established replay is intentionally a no-op, now asserted to preserve Offline
  and the exact revision rather than initializing anything.
- Monitor retained-medium control checks bound cluster/node/placement and exact
  removal or Offline authority, verifies discovery/mapping, opens the same
  no-follow identity read/write, reads the first/last block without changing
  bytes, and rechecks authority. Three media tests cover unchanged bytes, missing,
  truncation, same-size replacement, symlinks, aliases and changed mappings.
  Rejoin HTTP requires that private check before restoring membership; unavailable
  verification and readonly cannot restore membership. A committed operation
  exposes its original removal source for lost-response reconciliation.
- Candidates now offer explicit Rejoin only for the original observed identity,
  backend and capacity. The lifecycle dialog retains operation identity through
  unknown responses, restores Offline and returns focus to the registered row.
  Rejoin does not authorize Online. All 225 UI cases/types pass, including response
  loss and invalid-media eligibility. Web/monitor/KV/protocol/E2E all-target,
  all-feature Clippy and Rust fmt pass. Native browser/occupied flow now also
  asserts original-ID Offline Rejoin, retained allocations and continued I/O
  exclusion; rebuilt-image acceptance is pending.

## Remaining recovery implementation detail

- Add a strict read-only retained-zone loader before enabling Online. Current
  ordinary journal/full-scan recovery permits missing snapshots, skips malformed
  records and may write a fallback snapshot; those paths cannot validate a
  retained occupied disk. Require an existing valid geometry-matching snapshot
  and strictly decode/range-check paged busy/free records. Preserve conservative
  occupancy, seed allocation timestamps and never create a fresh baseline.
- Persist a disk-specific Online intent at the exact Offline revision after the
  original medium check. Reuse the owning DiskDB keepalive for execution; no new
  scheduler, lock or initiator election. Wait for current owner/DiskIO exclusion
  proofs and local closed/drained gate; validate the retained metadata read-only.
  Successful activation must CAS the exact Offline revision, current owner
  generation, backing and Rack into Up together with its reconciliation receipt.
  Invalid state remains Offline with a recorded reason.
- Old disk Arc snapshots keep their closed gates. After an authorized activation,
  replace the current runtime disk with a new Init instance and strictly reload
  before allocations; do not reopen an old gate. A later Offline tick closes the
  same new instance before publishing its new-revision proof. Startup and disk-add
  reconstruction must honor retained rejoin/recovery markers and never use blank
  fallback for those disks. Ordinary legacy initialization remains unchanged.
- Existing `read_zone_records_bounded` strictly decodes records at a cutoff but
  requests unlimited scans. Retained validation needs explicit advancing pages
  and bounds; existing `read_zone_records` silently skips invalid records and is
  unsuitable. `ZoneBitmap::from_value` already validates decompression/geometry.
- Add corrupt/missing snapshot, malformed/out-of-range/overlapping records,
  stale-owner/revision and restart regressions, then extend the real occupied
  native flow through Online and read back the fsynced marker without changing
  disk identity. Rejoin-Offline alone does not complete A13.
- Rejoin-image native run `run-1aa3141766e749368b467581b585894f` failed at
  the retained-candidate focus assertion after confirmed removal (13.5 s browser).
  The newly retained Inspector selection exposed a real effect-ordering bug:
  requestAnimationFrame could focus Candidates before Dialog's passive cleanup
  restored its original, soon-removed trigger. Focus now waits for modal unmount
  in the workspace's passive effect, then targets Candidates or the restored row.
  A component regression keeps the old row mounted and proves Dialog cleanup
  cannot steal the final focus. All 226 UI cases/types pass; browser retry pending.
- Further safety audit before Online: a stable DiskDB instance ID currently
  reuses its exact data-group ownership fence after process restart. Unknown
  metadata requests from the old process can outlive it remotely. A fresh closed
  local gate alone is therefore insufficient evidence of drainage across restart.
  Fence each process incarnation before any recovery/allocation or exclusion ACK.
  Preserve the logical owner ID; allocate a stable process-session epoch once
  per boot in Group 0 and advance each owner-map generation once for that epoch.
  Claim the existing data-group fence before observing member disks. Old process
  epochs cannot reclaim owners, write metadata or publish heartbeats after the
  new process claims authority. The data-group fence and disk formats stay
  unchanged. Same-instance restart and superseded-startup regressions pass.

- Rejoin native acceptance passes on committed image `424a9887`
  (`run-093e333f298b415aaa7e918e1174acff`): browser 9.7 s, including retained
  candidate focus, original-ID Offline Rejoin and refreshed properties. Occupied
  allocation, fsynced bytes and BusyBlock records survive Remove/Rejoin; old I/O
  remains excluded and another disk allocates. This does not validate Online.
- Process-session isolation is implemented without new locks. A boot freezes its
  predecessor identity; stable epoch is separate from heartbeat record revision.
  Conditional batches must mutate their precondition key, so session-aware
  heartbeats atomically rewrite the unchanged session and liveness record. A new
  boot may supersede an interrupted pending claim; the old claimant cannot revive.
  Four live SDK tests and the complete DiskDB suite pass, including same-instance
  restart fencing. Affected SDK/DiskDB/server/protocol all-target, all-feature
  Clippy and workspace formatting pass. Native image verification remains pending.
- Strict retained-zone validation now requires a CRC/geometry-valid existing
  snapshot, strictly decoded live facts, valid ranges/states/timestamps and
  nonoverlapping busy records. It retains uncertain occupied bits and seeds the
  incarnation source. Fixed-cutoff scans use advancing 128-record pages, with
  explicit 131072-record/32-MiB per-zone bounds; incomplete scans fail closed.
  Four focused tests pass, including real multi-page reads and unchanged snapshot
  bytes after malformed input. DiskDB all-target/all-feature Clippy and fmt pass.
  The loader is not yet wired into Online execution or retained startup.
- Online now has a versioned, exact-Offline-revision Group-0 authorization and
  Pending/Failed/Complete outcome. HTTP verifies the original medium before
  acceptance; current DiskDB executes off the UI connection, one bounded job at
  a time, after all exclusion proofs. Completion shares Rack CAS with current
  owner generation, backing and membership. Remove/Rejoin and owner restart
  invalidate old authorization. Watch subscriptions include lifecycle requests
  and exclusion acknowledgements. No new locks or on-medium formats are added.
- Retained startup and background disk-add load honor durable Rejoin/Online
  markers and never enter fresh initialization or permissive fallback. Validated
  Online constructs a new Init disk and strict reload; old Arc snapshots keep
  closed gates. A partial-snapshot overlap regression exposed range_set rollback;
  retained reconstruction now merges individual busy bits without clearing any.
  Five live activation tests and five strict loader tests pass. Six protocol
  cases and the HTTP medium-failure regression pass. Full affected Clippy/fmt
  pass; the complete DiskDB suite also passes. Committed-image acceptance is
  pending.
- Native occupied-disk flow now requests Online after retained Offline Rejoin,
  waits for authoritative validation, and reads the original fsynced marker
  through DiskIO, retaining the exact BusyBlock record. Actual container result
  is pending. Visible Online controls will follow a fresh browser baseline.
- Full occupied-disk native acceptance passes on image `8777d182`, config
  `sha256:497012cfdedd2eefb56f36c6268a9a83d5a606428791f61bad05d8c367cc3fa5`,
  artifact `run-cee7a8d38c994a318ea2fb46ee03ef24`. Remove/Rejoin/validated Online
  retains identity, allocations and fsynced bytes, then reads those bytes through
  real DiskIO. Browser before visible Online changes is 10.1 s; native client
  29.70 s. Temporary DiskNotExist responses precede actual read success while
  DiskIO applies Up; they are printed as pending, never reported as recovery.
- Online UI now exposes explicit original-medium validation, durable Pending and
  Failed states, exact-revision response-loss reconciliation, retry and focus
  restoration to the original registered row. Acceptance is not shown as Online.
  All 230 UI cases and application types pass, including failure/reopen, lost
  acceptance and row status/focus. Lifecycle reads reobserve a changed disk
  revision rather than mix Offline membership with completed Online metadata.
  Web lifecycle regression, affected all-target/all-feature Clippy and fmt pass.
  The existing hardware browser flow now also triggers and verifies Online;
  rebuilt-image verification remains pending.
- Visible Online acceptance passes on committed image `04a8176a`, config
  `sha256:5ac47f36b018f9df725a76b09ce4e6b49846f97e113b5f6400e9fcf8a3c81413`,
  artifact `run-6b634ac077c644228f40c871ecb510f5`. The actual dialog triggers
  202 acceptance, observes completion, refreshes Up and returns focus to the
  registered disk. Browser 13.4 s versus 10.1 s baseline; no timeout inflation,
  interception or retries. The occupied native flow again reads retained bytes
  after Online and preserves exact allocation metadata (client 34.68 s).
- Next hardening: reserve disk IDs globally, including compatibility writers,
  before Rack-fenced admission; do not release reservations on Remove. Validate
  legacy locations without unbounded response retention. Unknown physical legacy
  records must not bypass removal through direct deletion or parent cascades.
  Existing compatibility add already rejects restoring a removed disk in the
  same retained scope (`allow_initial=false`); preserve this guard.
- Global disk-ID reservations now precede managed admission and compatibility
  publication, including Console's direct Rack-fenced writer. Each versioned
  reservation fixes one canonical location and survives Remove; interruption
  reserves identity without creating membership. Missing reservations check
  legacy/retained locations using advancing 128-key pages, bounded at 65536 keys
  with no retained full-inventory response. Different Rack CAS domains cannot
  concurrently publish the same ID. Five live regression tests pass.
- Legacy physical records with a device path now block direct and parent/cascade
  deletion even without a retained sidecar. Console's compatibility DELETE also
  rejects them; a live HTTP regression checks unchanged disk and group records.
  Full SDK tests/rustdoc, Console hardware/bootstrap tests, two Web lifecycle
  cases and affected all-target/all-feature Clippy/fmt pass. One older concurrency
  regression initially used a physical path for metadata-only compatibility
  deletion; it now explicitly uses an unmapped record, retaining every concurrency
  and non-resurrection assertion. Actual physical deletion is separately rejected.
  Updated committed-image acceptance remains part of final verification.
- Remaining admission work must also retire the Console's unverified legacy
  physical Create shortcut once supplied-medium provisioning is connected. The
  trusted low-level SDK bootstrap API still supports existing disk formats.
- Capacity REST projections now expose explicit usage coverage. Unloaded or
  partial geometry cannot claim known occupancy; the UI renders unknown used/free
  values at disk and aggregate scopes. Known zero remains valid. No data-group
  wire or on-medium format changes are needed. Group-0 heartbeat summaries also
  carry coverage, defaulting old missing flags to unknown. Allocatable capacity
  remains separately reported, so one unknown Offline disk does not hide healthy
  allocation candidates. Twenty-five protocol cases, fourteen DiskDB usage/query
  cases, 231 UI cases/types and affected all-target/all-feature Clippy pass.
- Native lifecycle acceptance now restarts the actual retained disk's owner while
  Offline, verifies a changed process PID, stable logical instance ID, advanced
  owner generation and unknown occupancy, then continues validated Online and
  original-byte reads. Rebuilt-image verification is pending.

## Remaining node-admission integration

- Accept one immutable node storage/service plan on the authenticated monitor,
  tied to discovery UUID and admission operation before initial binding. Resolve
  numeric node/cluster authority only from the matching confirmed admission;
  unbound or nonquorate nodes wait without effective disk registration. Include
  diskless selection, selected existing supplied inputs and the no-input default
  of one preallocated 16-GiB file with 1-GiB reserve. Never create fallback files
  when supplied inputs are invalid, missing or not authorized.
- Extend the existing candidate form with Node/Services/Storage sections. Verify
  credentials and read the private inventory before the final initialization
  summary; display individual media and require explicit raw-device agreement.
  Final submission persists the plan once. Preserve the existing default service
  choices and dependency waiting states. Background execution must not depend on
  the browser; investigate reusing the node's owned REST backend for existing
  typed deployment operations rather than duplicating launch logic.
- Reuse or create a compatible node-local group per media class, preserving all
  existing placements. Resolve metadata binding against an existing ordinary KV
  group and persist its choice; do not silently create Group 1 or expand voters.
  Unbound ownership remains a visible prerequisite, distinct from media access.
- Supplied-file/raw provisioning needs a durable bounded full-zero verification
  before fresh admission; absence of a recognized signature is not blank proof.
  Read-only validation must reject nonzero, foreign, in-use, replaced or aliased
  media. Keep fixed expected path/type/identity/capacity and per-disk progress.
  Use exclusive raw-device access and preserve its exclusion through DiskIO fd
  retirement. Cross-node resource claims must account for authoritative physical
  host identity as well as node-local backing identity.
- Extend existing owned Docker/containerd acceptance with disposable loop-backed
  media; create, verify and detach only the fixture's own loop resource. The
  existing containerd host fixture supplies an isolated privileged runtime host;
  the CROWDB node itself remains unprivileged and cannot use a runtime socket.
- Retire Console's unverified legacy physical Create shortcut when this path is
  available. Keep trusted existing bootstrap formats readable without a header.
  Also finish Missing/replaced-media health publication, scoped Cluster/Rack
  workspaces, remaining zone navigation, permanent docs and full acceptance.

## Current verification and physical health

- Committed image `7b283051` passed native hardware lifecycle acceptance in
  `run-9ab937107441447787b41a1b65970db1`: browser 12.6 s, native client 33.15 s.
  Actual Offline owner restart advanced its generation, reported unknown usage,
  preserved allocation records and completed strict Online with original bytes.
- Fresh bridge-admission baseline on the same image passed in
  `run-4cfd63d8787e47a78a43b0b7046f76bf`: prepare/init 7.7 s, cross-UI recovery
  4.8 s, independent cluster cleanup 2.3 s, authenticated node update 11.4 s.
  Other shared-rack, minority, remote-service and delayed-admission checks pass.
- Monitor physical health now checks one eight-receipt page per background pass,
  with one worker and five-second spacing. Exact retained identity, geometry,
  no-follow descriptor and first/last reads reuse the read-only recovery check.
  Missing/replaced/unreadable media is quarantined Offline at the captured disk
  revision under Rack CAS; stale observations cannot close a later lifecycle.
  Records, placement and bytes survive; restored paths never reopen automatically.
- Two live SDK physical-failure fencing cases, three retained-medium cases,
  affected all-target/all-feature Clippy and Python compilation pass. Initial
  checks caught a test name shadowing error and ordinary Clippy layout/style
  issues; fixed without weakening assertions or lint configuration.
- Actual missing-media acceptance is added to the owned native hardware fixture:
  admit a separate file, move only that fixture file out of the disk directory,
  observe authoritative exclusion, restart the monitor, prove membership remains
  with no replacement, restore the same inode and prove it remains Offline.
  Updated-image verification remains pending; no completion claimed yet.

- Image `e83e96c1` / config
  `sha256:4a894592525bb660daa0bbf74fff4bda64adbb37832bdabf90722cbb078eb6a5`
  completed the real missing-file quarantine/restart/restore proof, retaining the
  exact backing and membership. Artifact: `run-9a44ad9f5f0345beb2e726f69ad97021`.
  The subsequent original browser assertion failed at capacity: batch POST
  returned two added disks and no errors, but GET capacity contained only one.
- First divergence traced through actual network bodies to the compatibility
  batch handler. It called best-effort SDK writes, discarded Rack-CAS errors and
  unconditionally returned 201. Concurrent monitor/owner changes exposed this;
  the capacity assertion correctly detected an incomplete publication.
- Single/batch Console creation now uses confirmed Group-0 authority. Batch
  validates all inputs, reserves global identities, commits all new disk records
  and parent membership in one Rack-CAS batch, and reconciles identical replay.
  Rejections cannot create partial membership. Local projections merge only the
  confirmed records/parent and tolerate canonical media aliases on replay.
- New live HTTP regression covers concurrent status churn, all disk records and
  parent membership, exact capacity disk count, identical replay, duplicate IDs
  and a conflicting last member leaving the first new member absent. Both new
  cases and both existing lifecycle API cases pass, with affected all-target/
  all-feature Clippy and repository Rust fmt. Initial replay failure revealed
  the cache's canonical media spelling; fixed the confirmed projection merge
  without relaxing the replay assertion. Container rerun remains pending.


### Next node-plan implementation boundary

- Keep one accepted `NodeSetup` intent on the target monitor, matched to its
  discovery UUID and admission operation. A specific bounded worker may ask the
  owned local Web backend to prepare existing typed service configurations; the
  monitor's existing desired-intent reconciliation starts/observes the processes.
  No browser executor owns accepted work and no generic workflow engine is added.
- Persist each complete prepared `NodeServiceIntent` before publication, including
  chosen ports and Chunk-KV bootstrap IDs. Publish a fresh Start only when its
  desired key is absent; exact operation/configuration replay is read-only.
  Never overwrite a newer operator Stop/Restart/Delete during plan reconciliation.
  Existing explicit service controls retain their normal supersession behavior.
- Treat installed/registered-but-stopped services as explicit prerequisites rather
  than restarting them through default-plan retries. Resolve existing KV binding
  as the Paxos step; do not deploy another KV instance or create Group 1.
- Node plan state must include the no-input managed-file default, explicit
  diskless and supplied-media selections, per-medium results, and separate
  waiting states for authority, binding, ownership and service readiness.
- Also reject timed-out retained pages explicitly in `disk_storage_page`; do not
  interpret a partial observation as a complete inventory. This remains a pending
  hardening hunk, not an implemented claim.

- UI follow-up identified during the health integration: `NodeStorage.confirmed`
  currently overlays successful local lifecycle results indefinitely. Retire that
  overlay against fresh authoritative lifecycle revisions so a later physical
  quarantine or another Console mutation cannot remain painted Up (or Offline).
  Preserve the immediate confirmed result/focus behavior, bounded observation
  concurrency, and add both component and actual browser regression assertions.

- Image `02260a4c` / config
  `sha256:30b2d481fbd5f644faa196a1c605db34fb5e79ba91bc9d7c03dfa652a448719d`
  passed missing-medium proof again and the original atomic batch/capacity
  assertions. Artifact `run-d54accc5a818482897bc63f376847158`. Browser reached
  validated Online but failed waiting for its POST; per-test time 14.4 s.
- Trace showed the Online click completed but no POST was sent. Its dialog
  disabled Confirm whenever the 250-ms exclusion poll was in flight, including
  between pointer-down and pointer-up. Existing confirmed revision remains safe
  for a conditional mutation; the handler now keeps that action available while
  reading. Failed observations still disable mutations until fresh authority.
  An observation generation prevents a read begun before submission from
  overwriting the accepted mutation result or error afterward.
- Added deferred-read regression: Confirm remains enabled, emits exactly one
  Online request at the original full-width revision, and an older completed
  read cannot erase Pending validation. All 232 UI cases and TypeScript pass.
  Original native assertions/timeouts remain unchanged. Updated OCI rerun pending.

- Image `3cf6fcee` / config
  `sha256:81fe210de43dd229b0bfc9c164744ef77254456f08948f1888865dcffe1d3748`
  passed the entire native hardware scope in
  `run-e6386ab2df2f4b9b9d0da30a619f10ee`. Browser 10.7 s (original baseline
  10.1 s), native lifecycle client 38.22 s including the additional missing-file
  restart proof. Every original browser assertion remains. Atomic batch capacity,
  actual missing-medium quarantine/no replacement, restored inode still Offline,
  validated Online UI/focus, occupied removal, same-owner process restart,
  unknown occupancy and original committed-byte reads all pass.

- Fresh bridge baseline on committed image `3cf6fcee` passed in
  `run-c33b85e8fc294014befb1c39c5a099ca`: preparation/init 7.7 s,
  cross-UI recovery 4.9 s, independent cleanup 2.6 s, authenticated node update
  10.8 s. All original authority, cancellation and service lifecycle proofs pass.
- Node-plan service publication now has `publish_planned_start`: only a fixed
  canonical Start operation may create an absent desired record. Exact full
  replay retains its revision; different operations/configuration and newer
  explicit Stop/Restart/Delete remain authoritative. Monitor's existing desired
  reconciliation can execute the accepted record without browser delivery.
  Four live publication/race/fencing cases and three existing identity cases
  pass; affected all-target/all-feature Clippy and Rust fmt pass. This is a
  verified building block; the node-plan worker/UI integration remains pending.

- Added typed `mgmt::node::setup` choices/progress contract: one fixed discovery
  UUID/admission operation, explicit Automatic/Diskless/Supplied storage, exact
  supplied identity/capacity and initialization agreement, six selected service
  kinds and bounded listener overrides. Omitted choices default to all services
  and a preallocated 16-GiB file with 1-GiB reserve. Progress excludes prepared
  configuration and credentials. Serialized request size is capped at 32 KiB;
  duplicate media/services/listener reservations fail before acceptance.
  Four contract cases and all-target/all-feature protocol Clippy pass.
  Monitor journal/worker and public API integration remain pending.
- Raw-media provisioning must distinguish observed physical bytes from usable
  whole-zone geometry, retaining any unused tail rather than rejecting ordinary
  sector-sized devices or resizing them. Introduce a compatible optional backing
  capacity field when implementing that path, with strict retained identity
  validation. Automatic DiskGroup selection must avoid global ID collisions
  because allocation/service APIs address groups by numeric ID.
