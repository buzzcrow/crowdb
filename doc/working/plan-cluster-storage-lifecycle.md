<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Cluster Storage Lifecycle Plan

Upstream: [R235](../backlog/R235-console-cluster-storage-lifecycle.md).
Retention exception: keep this execution evidence with R235 for the user's consolidated review.

Current user scope: complete R235 only; do not start R236, R237 or other backlog work.

Goal: implement shared initialization and Group-0-authoritative OCI disk management
in one Cluster workspace, preserving existing data and bootstrap exclusion.

## Execution

- [x] **Runtime storage inputs**: inspect `container/oci-image/runtime.py` and
  `run-node.py`; map permitted file/device inputs into the common disk directory,
  validate duplicates/types, retain startup inputs, and scope local cleanup.
  Tests: `container/oci-image/tests`, container helper tests. Acceptance A01-A02.
- [x] **Disk observations**: expose bounded monitor inventory, reject duplicate
  resources and mismatched launcher identities. Inventory tests and Clippy pass;
  real raw-device/Docker/containerd acceptance passed on the frozen final image.
- [x] **Effective admission**: add Group-0-gated registration, default file
  creation and geometry validation; merge observations with authority records.
  Files: `container/crowdb-monitor/src/node`, `bootstrap/disk_files.rs`,
  `lib/crowdb-protocol/src/mgmt.rs`, `lib/crowdb-console-shared/src/ops/hardware`,
  `app/crowdb-web/src/physical`. Acceptance A03-A05.
- [x] **Backend initialization**: inspect existing single/bootstrap ownership,
  reuse monitor execution for UI trigger, UUID-min automatic initiation and
  three-/one-voter policy. Persist policy and reuse accepted manifests.
  Files: monitor node runtime, console bootstrap/admission, web managed routes.
  Acceptance A06-A10.
- [x] **Offline/remove semantics**: preserve removed disk metadata and allocation
  identity, prove status application before remove, gate cascades, support explicit
  rejoin without blank initialization. Files: hardware ops, protocol hardware
  records, DiskDB/DiskIO status observation, web routes. Acceptance A11-A13.
- [x] **Accepted node setup**: persist authenticated discovery/admission choices
  on the target monitor; advance binding, default file and fixed service intents
  independently of the browser. Files: monitor `runtime/setup`, protocol
  `mgmt/node/setup`, Web `services/deployment/planned`, node admission and UI.
  Compatible-group reuse/binding, explicit cleanup fencing, supplied-media
  provisioning and visible progress passed focused and real container acceptance.
- [x] **UI baseline**: measure affected current root-runner browser flow before
  visible edits; record exact per-test timings here. Inspect tree/view/history,
  managed candidate components and existing physical/capacity flow registrations.
- [x] **Unified workspace**: consolidate navigation and selection, common center
  scope, candidates and status actions, responsive properties drawer, init policy
  and progress. Files: `app/crowdb-web/ui/src/{shell,views,menus,managed}` and
  associated hooks/styles/tests. Acceptance A14-A18.
- [x] **Scoped verification and documentation**: run focused component/runtime/browser
  tests, then fmt/lint separately; update permanent Console/deployment/OCI guides.
  Focused disk/bootstrap/UI checks passed; broader shared-workload failures
  remain explicitly deferred below and are not reported as passing. Retain this
  evidence and the R235 decisions for the user's consolidated review.

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

- Global numeric DiskGroup placement reservations and bounded allocation now
  cover SDK group creation, owner publication and Console creation. Live legacy
  collision tests exposed a binary/text prefix mix-up; the allocator now uses
  the explicit text-key prefix. Three new cases and the complete SDK suite,
  including rustdoc, pass. Timed-out retained pages now fail explicitly.

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

- Target-monitor setup now accepts one authenticated UUID/admission intent and
  advances in a bounded background pass. It verifies confirmed Group-0 registry
  identity before effects, retains default file operation/group/binding, waits
  for an ordinary metadata group, and reuses compatible bound storage. It uses
  existing typed Web configuration builders and fixed service Start publication;
  resolved configurations are persisted before publication. Explicit cleanup
  fences new passes and waits for an in-flight pass before archiving intent.
  Five new monitor cases, the full monitor suite, ten affected Web cases,
  all-target/all-feature Clippy and Rust fmt pass. Admission form/API wiring,
  supplied provisioning and actual default-plan container proof remain pending.
- Local disk confirmations now have scoped lifecycle reobservation through the
  existing shared four-worker budget. A later authoritative Offline supersedes
  a locally confirmed Online row; failed observations mark last-confirmed state
  visibly stale. Full-width revision and stale in-flight removal guards retain
  the latest accepted state. All 234 UI cases and TypeScript pass. Committed image
  `3ee3305e`, config `sha256:cb2980ba2e9da84391421ead39b3f3499a534f6ebaf960640be02c2be289b3fa`,
  passed full native hardware acceptance in
  `run-15153f6341eb494ead5fef3bce2994f1`: browser 13.5 s versus original 10.1 s,
  native client 40.18 s. Added real second-operator Offline refresh/focus assertion
  passes; all original allocation, missing-file, restart, retained-byte and
  validated Online proofs remain. No retries or timeout inflation were added.
- Fresh bridge baseline on that image passed in
  `run-5bb926a6956f4bb1af02ee183cf40151`: prepare/init 7.6 s, cross-UI recovery
  4.8 s, independent cleanup 2.7 s, authenticated node update 11.3 s. Use this
  exact baseline for the combined admission form changes.
- Combined admission now performs read-only authenticated inventory review,
  then submits fixed Node/Services/Storage choices to the target monitor before
  confirmed binding. Pending reviews are separate from admissions and can be
  discarded without touching accepted work. Existing accepted choices reopen
  read-only; passwords remain only in browser/request memory. Node workspace
  exposes durable progress, diskless selection, stale reads and explicit retry.
  Seven monitor cases, four protocol cases, six Web cases, 241 UI cases/types,
  all-target/all-feature Clippy, Rust fmt and Python compilation pass. Initial
  UI checks caught unsupported assertion syntax and missing Rack fixture fields;
  corrected test setup/types while retaining the assertions. Updated-image
  bridge acceptance, including browser closure and real monitor restart, is next.

- Combined-admission image `910438e1`, config
  `sha256:b1757fe8e5c6fa6a153eb1f55fdd0f46233651c403adb78620fced716323bb7a`,
  preserved all existing bridge checks in `run-67102d4c51194d40a788c93cf5635433`:
  prepare/init 12.9 s (baseline 7.6 s), cross-UI recovery 4.9 s, independent
  cleanup 2.6 s, authenticated node update 11.4 s. The new default submission
  closes the browser in 9.8 s; all disks register and DiskDB/DiskIO/ChunkDB ready.
  Chunk-KV later fails checkpoint because two fixture disks were reduced to 1 GiB;
  actual tree logs report DiskDB No space, not a UI observation delay. Retain all
  three 16-GiB product defaults in this acceptance. No timeout change or retry.
- The same failure exposed managed recovery counting only successful launches,
  allowing failed services to restart indefinitely and setup to remain Accepted.
  Persist a five-attempt per-operation budget and bounded last error, expose it
  in authenticated process observations and mark exhausted setup services Failed.
  Explicit later service commands remain authoritative and start a fresh budget.
  Eight setup cases, two process-observation cases, the full monitor suite,
  all-target/all-feature Clippy, repository Rust fmt and Python compilation pass.
  The new regression covers durable failed attempts, exhausted progress, unchanged
  state for an unconfirmed command and a later confirmed Stop. Initial checks
  caught module visibility/import and fixture log-directory errors; corrected
  without relaxing assertions. Updated OCI acceptance remains pending.

- Image `5641d9d4`, config
  `sha256:75b70fbdc08048840856cab3ee6e62b7184714b1356aaaddead32b8bdec92ed3`,
  failed the existing bridge cleanup before the new default-file scenario;
  artifact `run-cc1ba43a745e4ccd94606b7fc71b8e8b`. Existing browser preparation
  13.8 s, cross-UI recovery 5.0 s, independent cleanup 2.3 s, authenticated
  update 11.4 s pass. The first explicit cleanup HTTP request exceeds 45 s.
  Its retained journal confirms completed nodes 1/2/3, unreachable node 4's
  10-second SSH timeout, and node 5 still pending. Sequential target dispatch
  accumulates shutdown and unreachable-peer delays. Bound independent cleanup
  dispatch to four targets and persist every individual completion; retain all
  pending/unreachable-member assertions and original request timeout. Default
  storage acceptance has not run on this image; no success is claimed.
- Cleanup dispatch change passes six affected Web cases, all-target/all-feature
  Web Clippy and Rust formatting. Initial lint caught an oversized handler and
  needless borrow; split target execution into its owning helper. Original
  bridge and default-plan acceptance remain pending the rebuilt image.

### Supplied-media implementation boundary

- Add compatible version-2 backing metadata: physical capacity and authoritative
  physical-host/resource identity remain separate from exact opened inode/device
  identity and whole-zone logical geometry. Version-1 records stay strict and
  readable. Update monitor retained checks and real DiskIO descriptor checks;
  neither may compare a device's physical tail to logical usable capacity.
- Reserve the canonical host/resource identity globally before membership, with a
  sticky original disk placement and bounded legacy verification. Launcher needs
  verified canonical resources for exposed devices, including loop-device backing
  files; unknown stacked/aliased backing must remain explicitly ineligible.
  Preserve retained launch mappings and host identity across replacement.
- Use one bounded supplied-medium worker and immutable per-medium operations for
  node setup and explicit candidate Join. Validate the authenticated confirmed
  node, captured observation and compatible bound group before effects. Check
  complete contents read-only; reset partial blank-check progress on restart.
  Nonzero/unreadable/replaced/claimed media produces a visible per-row failure,
  never a fallback file or fresh initialization of retained occupied bytes.
- Open raw media exclusively and retain actual DiskIO descriptor ownership through
  retirement. After initial publication as Init, recheck usable zero bytes through
  the real DiskIO descriptor before Up, covering the local-check/owner-open gap.
  An uncertain activation must reconcile receipt and membership rather than
  repeat fresh blank validation of a disk that has already become allocatable.
- Add explicit multi-selection/group assignment and initialization agreement to
  Candidates, shared with accepted node choices. Keep removed Rejoin separate
  from fresh provisioning internally and show each retained/fresh outcome. Add
  owned blank loop-backed Docker/containerd proof; never target host data disks.

- Image `8cf608d0`, config
  `sha256:bd1f2257a8995911a30a2928148912717087078a8c4b090302eaf8a27b415e3e`,
  passes all original bridge tests including concurrent cleanup in
  `run-2cefcee2ac1e4e24b7ffe3e120de21ac`. Preparation 11.2 s, cross-UI recovery
  4.7 s, independent cleanup 2.2 s, authenticated update 11.3 s. Default browser
  submission/closure 9.6 s; all three 16-GiB disks, DiskDB/DiskIO/ChunkDB/Chunk-KV
  become ready across actual monitor restart. Last Access setup fails because
  independent Console journals reserved different catalog Initialize/Activate
  IDs before the shared catalog appeared. Preserve strict identity checks; reserve
  one immutable pair in Group 0 and reuse those commands on every node. Existing
  conflicting pending journals are explicit conflicts, never silently rewritten.
  Updated tests/gates and OCI proof remain pending.
- Shared Console catalog reservation passes three live Group-0 cases: concurrent
  callers converge without revision-changing replay, existing pending identity is
  retained and another local identity conflicts without rewrite, malformed/version
  records fail closed. Three Console bootstrap cases include another node finishing
  shared activation without reinitialization and preservation of later operator
  policy; two existing Iceberg bootstrap cases also pass. Monitor/Web all-target/
  all-feature Clippy, three affected Web cases and repository Rust fmt pass.
  Updated real three-node default setup acceptance remains pending.

- Image `b30c71ed`, config
  `sha256:df1f5379b718aff6a6d1d6050d8d1e23cbd0b38f218f30b4a80d43d9562734e4`,
  passes shared catalog provisioning but fails the following S3 credential step
  in `run-795894c9c9b3481786e50017cc0c54f7`. Read-only inspection of only the
  non-secret Access Key ID fields confirms two successful nodes persisted distinct
  IDs for the same hard-coded Console user. `CredentialAuthority::ensure_user`
  explicitly assumed one writer (lookup then random issuance); concurrent nodes
  create duplicates and a later lookup fails closed. Preserve fail-closed
  duplicate handling; reserve one encrypted candidate per user before conditional
  credential publication. Tests/gates and renewed OCI acceptance remain pending.
- Credential concurrency fix passes five live Group-0 cases: 16 simultaneous
  callers share one encrypted candidate and one credential; projection interruption
  resumes identical bytes; legacy single credentials remain unchanged; existing
  duplicates and malformed prepared records fail closed; 4097 records exceed the
  explicit 4096-record read bound and cannot authorize issuance or install a
  partial cache. All-target/all-feature Access Clippy and repository fmt pass.
  Initial fixture checks caught equality assertion style and the SDK's transparent
  scan pagination: a tiny server page budget still returns a complete client scan,
  so the final case exercises the actual caller limit while retaining its failure
  assertions. No product timeout, retry, or assertion was relaxed. OCI acceptance
  with the updated credential authority is next.

### Follow-on handoff

- Continue R237 after R235. Its detail and `plan-dataset-split-lifecycle.md` are
  present; the latest user instruction authorizes implementation despite the old
  planning-only handoff wording. Preserve R236 as another workstream's UI scope.
- R238 detail/plan are not currently present. Checked the working documentation,
  all local Git refs' matching requirement history, and active worktrees; the
  index still says next number R238. Do not invent its scope or consume that
  number. Recheck after R237; retain this missing-artifact note for the user's
  later review without interrupting authorized work now.

- Image `ebf670db`, config
  `sha256:1585b41dd9e1b80087231a799e1438c59735a378536f7e8a2a820157be8b0f69`,
  preserves every original bridge assertion and the default browser submit/close
  (9.5 s) in `run-7673e60165224396a938f85a5d93d56d`. The later Node 2 setup
  HTTP read times out. Read-only retained journals show all three setup plans
  Ready, all six selected services Ready, and all default disks Ready; this does
  not prove their current health. Actual Chunk-KV logs show `Rust cannot catch
  foreign exceptions, aborting` / SIGABRT after readiness. No core was retained;
  the recorded host core policy pipes to apport. Do not change timeouts or claim
  final acceptance. Capture owned cgroup pids/memory counters and process thread
  counts on the next failure to distinguish resource exhaustion from runtime
  state/recovery failure before changing production code.

- Diagnostic rerun of the unchanged exact ordered bridge profile passes on image
  `ebf670db` / config `sha256:1585b41dd9e1b80087231a799e1438c59735a378536f7e8a2a820157be8b0f69`:
  artifact `run-1f7e4dae606544588868fec9dd50b81d`. Original preparation 13.0 s,
  cross-UI recovery 4.8 s, independent cleanup 2.4 s, authenticated update 10.9 s.
  Default submit/close 9.7 s and middle-workspace progress 1.2 s. The real fixture
  confirms three preallocated 16-GiB files with 1-GiB reserves, unchanged inode/
  byte geometry after an actual monitor/container restart, retained operation IDs,
  all six selected services Ready and all seven supervised processes healthy on
  every node. `admission/default-node-setup.json` retains per-file proof.
- The earlier intermittent foreign-exception exit was not reproduced and is not
  declared fixed. Keep owned resource diagnostics enabled for future acceptance;
  do not raise pids budgets or timeouts without evidence. This does not prevent
  independent supplied-media implementation, but remains a final verification
  concern. Full raw-device/containerd acceptance is still pending.

- Compatible version-2 backing receipts now retain physical host/resource identity
  and physical capacity separately from whole-zone logical geometry. Version-1
  serialization/capacity remain unchanged and strict. Monitor retained checks,
  Web original-medium reconciliation and real DiskIO descriptor checks use exact
  physical capacity; DiskIO also verifies the launch host for new receipts and
  opens raw media with O_EXCL through existing descriptor retirement.
- Seven protocol cases, four retained-medium cases, full KV-client tests/rustdoc,
  three Web authority/lifecycle cases and ten DiskDB Online/retained cases pass.
  Affected all-target/all-feature Clippy, changed C++ format/tree-lint and full
  test-cpp pass; artifact `unit/cpp-e2451c10b98e4c57bb571112a5967bcb`. The new C++
  descriptor test rejects host/physical-capacity mismatch and preserves v1 exact
  capacity semantics. Initial lint caught a test integer conversion and pointer/
  signedness style warnings; fixed without suppressions. Real raw-device
  exclusion/provisioning and global resource reservations remain pending.
- Global physical resource reservations are now wired into version-2 admission,
  initial activation, Rejoin and successful Online completion. Claims are shared
  across Rack fences, retained after interruption/removal and validated against
  confirmed node host authority. Bounded legacy scans reject unknown lineage or
  missing receipts instead of guessing. Eight real Group-0 regression cases,
  full KV-client tests/rustdoc, all-target/all-feature Clippy and Rust fmt pass.
  Initial Clippy caught a projection type naming issue, corrected without an
  allow. Actual launcher lineage, supplied provisioning and raw-device E2E remain
  pending; metadata concurrency tests do not count as raw-device acceptance.
- OCI launcher now reads whole-device lineage before launch: whole-file loop
  resources use LOOP_GET_STATUS64 backing device/inode; partitions, whole devices
  containing partitions, stacked/unknown virtual devices and sliced/raw-backed
  loops fail closed. Exact opened identity remains separate. File/loop aliases
  collide even across backend kinds. Old retained mappings may add verified
  lineage only when all their original fields still match, without rewriting on
  validation. Inventory carries canonical identity and retained v2 checks compare
  it. All 29 OCI-tool tests, full monitor tests, protocol/monitor all-target/
  all-feature Clippy and Rust fmt pass. Actual owned-loop launch/exclusion proof
  and supplied provisioning remain pending.
- Added private PrepareSuppliedDisk/status controls and a four-worker durable
  provisioning queue. Fresh media is fully read under an exact descriptor before
  Init publication; a raw exclusive claim survives that publication. DiskIO
  rechecks all usable bytes before Online. Interrupted scans restart at zero;
  existing Init/Up receipts reconcile before any repeated monitor scan. Explicit
  cleanup retires and drains these workers. No wipe or tail modification occurs.
- Seven new monitor cases cover distant nonzero data, physical tails, interrupted
  scans, replacement during reads, unlisted/changed lineage, real Group-0 Init
  publication without DiskIO, and immutable failed-request replay. Full monitor
  tests and DiskIO-client tests/rustdoc pass; protocol/monitor/DiskIO/Web all-target/
  all-feature Clippy and fmt pass. The first integrated case exposed missing live
  owner classification as a permanent topology error; fixed upstream to typed
  unavailable while retaining duplicate/mismatched/malformed owner failures.
  Initial compile/lint repairs corrected Eq derivation, existing client ownership,
  fixture directory/dead-field usage and split private dispatch; no allow or
  product timeout/retry/assertion relaxation was added.
- Supplied node-setup dispatch, bulk candidate Join UI/API, actual DiskIO/raw-loop
  completion/exclusion, and OCI restart/cleanup acceptance remain pending. Also
  verify loop backing identity through the actual DiskIO descriptor at handoff,
  so changed loop association cannot be accepted solely from launch-time evidence.
- Supplied node setup now seals launcher observations and all fixed per-medium
  requests before dispatch, reuses compatible bound groups per media class, and
  excludes a group already chosen for another class while it is still empty.
  Successful sibling outcomes remain Ready when one fails. Only explicit setup
  retry authorizes redispatch of that failed original operation, consumed before
  sending. Legacy accepted plans lacking sealed lineage fail visibly for review.
  Durable plan validation rejects changed/unselected placement and retry IDs.
- Four new native setup cases pass (explicit mapping/sealed read-only replay,
  stable operations and class grouping, partial failure/one-medium retry, corrupt
  placement rejected before dispatch), together with the full monitor suite and
  all-target/all-feature Clippy/fmt. Native socket fixtures prove control dispatch;
  they do not replace real OCI supplied-media completion. Initial compile repairs
  used exhaustive mode dispatch and explicit group-cache types, without lint
  suppression or test relaxation.
- Still wire the preflight canonical identity into the submitted SetupMedium/UI
  contract before raw support is accepted; sealing only at target acceptance must
  not hide a loop reassociation between review and final submission. Bulk Join,
  new managed-file v2 receipts, actual-descriptor lineage, UI and raw/containerd
  acceptance remain pending.
- DiskIO now compares version-2 canonical identity against the actual descriptor:
  regular files use fstat identity; loop devices read LOOP_GET_STATUS64 and reject
  sliced/raw-backed associations; ordinary devices must reject that ioctl with
  ENOTTY and retain exact device identity. An unbound loop is never treated as an
  independent device. The regular-file regression rejects a changed canonical
  resource even when host, capacity and opened identity otherwise match.
  Changed C++ format/tree-lint and full test-cpp pass, artifact
  `unit/cpp-26766439b819438bb94bf4c9f18b1811` (Tree 543, RPC 56, DiskIO 64,
  Common 31 and both FFI domains). Actual owned-loop exclusion/reassociation
  acceptance remains pending and is not inferred from regular-file unit tests.
- SetupMedium now carries optional canonical resource identity, required for raw
  selections and compared against current launch inventory before acceptance.
  Canonical aliases deduplicate even when opened device numbers differ. Legacy
  file selections derive the same canonical identity from their exact file inode.
  New managed files use version-2 host/resource/capacity receipts and global
  reservations, while existing v1 receipts remain byte/revision-preserving replay.
- A real Group-0 managed-file case proves the new claim, prepared-file identity/
  size/allocation preservation and unchanged legacy receipt; four supplied setup,
  eight existing setup and four protocol cases pass. Protocol/monitor/Web all-
  target/all-feature Clippy and fmt pass. Initial lint repairs added documentation
  markup and split the long fixture/replay checks; no suppressions or product
  behavior relaxation. UI still must echo canonical identity and exclude unlisted
  or unverified candidates. Rebuild the frozen OCI image and capture the current
  browser baseline before those visible UI changes.
- Frozen image for the next baseline: revision `cdecd44d`, config
  `sha256:07be671f197eae4729d1865fa1399a936442acbfe15500ac50f40d64d5175f93`,
  manifest `sha256:e1e6b81308636698ae4968aac5ac499dbdec29fbb09f9d6112c8b1099782562b`,
  runtime `eb4ac5f7e7ef6a6df40874ecd9b786dd4cd124d5b20823207142e3ad3c47f0f4`.
  Rootless build/import succeed without privileged fallback. Keep runtime/UI
  sources frozen until baseline acceptance finishes.
- Hardware baseline attempt `run-2318c2f78efc4d3eb71332a6633ffbba` fails before
  browser execution: native fixture omitted setup choices, so all-service/default
  storage tasks allocate groups alongside its fixed group IDs. Read-only retained
  journals prove Node 2 chose DG 3 and Node 3 DG 2. Use explicit diskless/disabled
  service choices for this manually provisioned fixture, keeping default setup in
  its dedicated real bridge flow. No product guard is bypassed.
- Next attempt `run-dc363355ba36439eb6fe6d1ec36ecb02` reaches new managed-file
  provisioning and correctly rejects legacy physical membership with no backing
  receipt. The native fixture now observes only its labeled/image/mount-verified
  own regular files, checks original capacity, and bootstraps them through atomic
  trusted v1 SDK admission rather than creating unreceipted physical metadata.
  Native files use the common disks directory; existing dedicated public CRUD
  assertions remain (51-chunk-capacity-disk.spec.ts), and dependent fixture path
  literals change without removing assertions. Unbound preparation proves its
  existing provisional inventory is unchanged, rather than assuming it is empty.
  Actual browser/retained lifecycle acceptance with these repairs is running.
- Fixture Python gate has one confirmed pre-existing failure:
  `selection_test.SelectionTest.test_layers_select_only_their_domains_and_count_dependencies`,
  Dataset expected 2 vs actual 3 after `257508ad` added a Dataset SDK case. Original
  test last changed at `5e54be81`; no selection/runtime behavior was changed here.
  Keep its failing evidence and fix the inventory contract in R237. Other 138
  fixture cases passed before adding two owned-file observation regressions;
  focused observation cases (11), e2e-client Clippy and fmt pass.
- Follow-up geometry fix found during code inspection: supplied zones must be
  rounded down to a multiple of 64 allocation units, not only one unit. A 65-MiB
  physical medium should expose 64 MiB and retain its tail; current preparation
  fails validation for such a capacity. Add that regression and correct geometry
  after the frozen baseline. Also finish echoing canonical identity from the UI.
- Preserve supplied-file allocation semantics after the baseline: existing sparse
  files are authorized inputs, not a preallocation fallback. Derive their initial
  sparse indication from the sealed allocated-byte observation, allow that flag
  for File only, and keep raw devices nonsparse. Current supplied preparation
  always writes sparse=false and rejects true, which must not imply external
  hole-backed files were preallocated. Add a focused case with the geometry fix.
- Repaired native hardware baseline passes unchanged production image `cdecd44d`,
  artifact `run-e55e51d349c64b1399a36a993fcf9cee`. Browser 14.5 s (previous
  13.5 s, below 2x); host client 41.35 s. Actual managed v2 file admission/restart,
  missing-file quarantine and restore-Offline, occupied Remove/Rejoin, owner
  restart/unknown usage and Online reading original committed bytes all pass.
  Eleven focused fixture ownership/type/capacity cases and e2e-client Clippy/fmt
  pass. Root fixture suite's one Dataset inventory mismatch remains confirmed
  pre-existing; no assertions/timeouts or production resource guards were relaxed.
  Native fixture trusted v1 backing receipts are only for its freshly truncated,
  owned sparse files and do not authorize adopting arbitrary legacy media.
- Current canvas baseline on image `cdecd44d`: fit 2.0 s, pan 1.2 s, navigation
  2.6 s pass in `run-56493c57aef44c37878e182e11b2231a`. Collapse fails at the
  original assertion after closing R-480 (N-480 remains rendered); exact isolated
  reproduction fails identically in `run-f4f63805c4d342e2868237c4b70a0c60` (7.8 s).
  Source tracing shows ancestor-reveal replays the same focus nonce whenever
  refreshed graph/default arrays change. Consume ancestor reveal once per domain/
  nonce after the target exists; explicit collapse must survive polling. Preserve
  the browser assertion and add deterministic refresh coverage after the frozen
  bridge baseline. No retries, assertion weakening or timeout increases.
- Datacenter registry browser baseline passes in
  `run-c3146550d9674084a16a38cf2d57b130` (1.6 s). Full ordered bridge baseline now
  runs in `run-dcdd65ea164d464a9924193ec6eb70e1` on the same production image.

- Full ordered bridge baseline `run-dcdd65ea164d464a9924193ec6eb70e1`
  passed on `cdecd44d` (171.66 s wall). Default progress 3.4 s versus prior
  1.2 s needs step-level investigation on the next image; added boundaries without
  changing timeouts or assertions. All six selected services and seven actual
  processes are healthy; default backing inode/size/allocated blocks survive restart.
- Current source fixes consume each canvas focus nonce only once, echo reviewed
  canonical resource identity, and retain sparse supplied-file semantics with
  64-unit zone geometry. UI and focused protocol/monitor gates pass; fresh-image
  browser regression remains pending. Cluster/Rack now has cached node cards and
  an optional Diagram, Capacity has filtered/sorted node rows, and scoped Add Node
  reuses candidate access review. Root/Rack does not initiate runtime usage scans;
  incomplete node coverage remains Unknown. Candidate and setup summaries retain
  at most 64 observations. Added component and real hardware/browser assertions;
  acceptance must run against a new committed OCI revision before closing this task.

- Manual supplied Join now uses explicit existing compatible bound groups and
  target-owned per-disk journals. Added HTTP POST/list/status, lossless string disk
  IDs, reviewed-observation comparison, immutable replay, private optional lookup
  and bounded pages (16 records/56 KiB). Missing target groups fail before blank
  scanning or physical claims; full verification still precedes Init publication.
  UI multiselect holds a fixed review snapshot, seals each request, caps dispatch
  at four and preserves successful siblings across retry/close. Raw consent is
  per device. Node progress reopens durable pages and retains at most 64 rows;
  partial read failure does not prevent siblings from refreshing. Physical backing
  properties separate exact host/resource/physical/usable/tail byte values.
- Focused monitor tests: 9 blank/provision/page cases + 4 supplied setup cases +
  8 existing setup cases pass. Web API authority/lifecycle and final UI/Clippy
  gates are running; new supplied Join browser/owned raw/containerd proof remains
  incomplete and must not be counted as accepted support yet.

- Current focused gates pass: Monitor 21 cases, Web authority/file/readonly/lifecycle
  6 cases, protocol backing/setup 11 cases, UI 251 cases plus TypeScript,
  all-target/all-feature protocol/monitor/Web Clippy, workspace fmt, and 14 owned
  fixture observation/candidate cases. New hardware browser flow adds two blank
  supplied files and one nonblank file with known tail markers, closes/reopens
  durable progress, retries only the failed original operation, and verifies
  unchanged inode/size/tails through an owned read-only host proof.
- A standalone native TypeScript invocation lacks three source snapshots consumed
  by untouched 55-chunk-kv-catalog.spec.ts and 60-iceberg-catalog.spec.ts. Further
  inspection confirms `bundle_client` deliberately copies these serializers/types
  into the owned browser client; this is a missing staged-input context, not a
  confirmed product defect. Do not fabricate modules in the repository. Run the
  native TypeScript check in that actual bundle; bridge/shell direct checks pass.
- New runtime/UI freeze at `e4c5695807ca38c0a7b4f011f2cf5fab33138edc`.
  Rootless stage/build/import succeed. OCI manifest
  `sha256:e0bff990e1f53c8d63644e514b24dfcf140d560c4b0c624dbb25695a4dda062a`,
  Docker config `sha256:ddab63877647521dba87ba5fde7cf91896262a732c60f78ca5d8ef35cd294946`,
  runtime `b96a6306e01e672d7396a29ece0b9902e9c99a93e3936156d87893792a66a239`.
  Known CDI/fsverity/shutdown warnings remain non-fatal; artifact receipt verifies
  the completed export. Native stage build and both bridge/shell TypeScript pass.
  Existing npm audit/chunk-size warnings remain visible; no dependency changes.
  Permanent docs now distinguish standard monitor-owned setup from legacy
  session-owned deployment, explicit single preview, storage identities/tails,
  per-disk durable Join, and retained exclusion/rejoin. Browser acceptance runs
  before any further runtime/UI edits; success screenshots are added to its owned
  artifact only, without changing assertions, timeouts or the frozen image.
- Canvas acceptance on `e4c56958` passes in
  `run-b02e63b47433450e9c3f0baf97fe781e`: fit 2.2 s, collapse+explicit Refresh
  6.1 s, pan 1.2 s, navigation 2.7 s. The original collapse assertion is retained.
- New supplied hardware acceptance `run-5e7bc7c3b7694b1fae2250981e31a3e7`
  reaches all original node/file/lifecycle assertions and the new UI review, then
  fails: expected registered/registered/failed, received failed/failed/failed.
  Read-only exact-image helper reads only its three non-secret supplied journals:
  bad medium correctly fails at byte 67108863; blank A fails with DiskNotExist
  immediately before DiskIO's logged descriptor addition; blank B fails class
  validation after A publishes. Screenshots of real cards/table/dialog are retained.
- Root cause reproduced deterministically in real Group 0:
  `compatible_unknown_media_admitted_between_receipt_and_membership_reads_is_not_legacy`
  fails before the fix with false incompatible class. A newly atomic receipt/member
  commit can appear between separate scans, so the old scan misses its receipt and
  misclassifies Unknown as legacy HDD. Validate both scans within a stable Rack
  revision, retrying a changed revision within a bounded loop; no lock or media
  class relaxation. All 16 admission/compatibility cases now pass.
- DiskNotExist is now a distinct typed DiskNotFound, not a physical I/O failure.
  Ordinary client reads do not gain automatic retries. Accepted supplied work may
  remain Waiting for DiskIO only after rechecking the exact retained Init receipt
  and current confirmed binding; Offline/removal/changed intent still fail. Actual
  I/O/alignment failures retain their original classification. Add code-boundary
  regression and rerun full DiskIO client/monitor gates before rebuilding.
- Visual inspection found native grey controls from a nonexistent surface color;
  use the shared dark Input/Select controls in node filters and Join rows, with
  unique IDs. Also distinguish observed allocated external files from proven
  managed preallocation; an external sparse=false receipt cannot prove how its
  host file was allocated. These source changes end the prior image freeze and
  require another committed production image for acceptance.
- Second production freeze is `afa327865e78c7260f97ab62828f059efde5ab7c` after
  `1e8a7c2c` (consistent class reads/typed pending descriptor) and the shared UI
  controls/allocation-label correction. Full DiskIO-client tests/rustdoc, 21
  focused Monitor cases, 16 SDK admission/class cases, five affected crates'
  all-target/all-feature Clippy, UI 251+TypeScript and staged build pass.
  Manifest `sha256:f48fcb8509ea66d50f36a3bc72b0e73e8160e08c1d95f18fb6657efa77f0164d`,
  config `sha256:23414660358feed2ec4c870ed523afe9767c418406ec9bee427214c3405a222a`,
  runtime `f39d7db8083e02b6c1675986d1b4b829a7f7bc56b092183b9ba80d1e365c17bd`.
  Build/import are rootless and receipt-verified, with the known shutdown warning.
- Hardware browser now passes on that image in `run-f9869429e1e849e88e430c6facb3cccb`
  (18.2 s including the new real three-medium Join, versus prior 14.5 s). Two blank
  files register and retain distinct v2 receipts, the nonblank file fails, reload
  reopens durable outcomes, and only its original failed operation is retried.
  Host occupied Remove/Rejoin acceptance and exact file/tail proof still complete
  under the root runner before treating this whole case as passed.
- New isolated containerd-host fixture work is pending actual execution: create
  only owned files and a private loop device node, pin/verify its exact kernel
  mapping and set autoclear for interruption. No host disk or pre-existing device
  permissions change. Test actual fsync/read through the normal Rust DiskIO SDK,
  exclusive raw claim while Online/release after Offline, retained Remove/Rejoin,
  recreated-container reads and physical-tail persistence. Existing single-profile
  initialization/recreate assertions remain. Seven Python boundary/ownership cases
  and e2e-client Clippy pass; do not claim real loop acceptance yet.
- Follow-up after this freeze: registered supplied rows must display the retained
  nullable media class rather than the geometry's compatibility HDD fallback.
  Also hide direct Open candidates in modes with no Node storage workspace.
  These are UI contract corrections, not changes to media admission or group
  class constraints; retain regression assertions and build before final acceptance.
- Isolated containerd supplied File/raw acceptance now passes on the frozen image:
  `run-88ecd8a11c5348b6bf8cbdd47821d57c`, full profile 78.89 s. Real SDK fsync/read,
  raw exclusive ownership/release, retained Remove/Rejoin, same-image recreation,
  exact tail markers and single-mode automatic initialization/recreation pass.
  Earlier `run-09028bb294374640b3e9019011f14844` and
  `run-d75f8b00ba1441cf851db2e33ea4f873` passed all supplied-media probes but failed
  single startup because the fixture precreated `disks/` in a pristine single
  root, violating BootstrapSession's explicit clean-root contract. Create that
  directory only in manual supplied-media fixtures. Memory events confirm no OOM.
  One diagnostic attempt itself failed reading a nonexistent cgroup-root file;
  resolve the host cgroup through `/proc/self/cgroup` and never mask cleanup.
- Docker host profile now exercises the same supplied-media contract through an
  owned privileged loop helper; the actual Node stays unprivileged and receives
  only declared file/device mappings. Helper has no network or runtime socket,
  pins one verified autoclear mapping, and cleanup checks image/label/data mount.
  Host driver reads only the known nonsecret binding through UID 10001. Its
  launcher receipt write temporarily grants the fixture operator group access;
  restore 0755 before SSH because StrictModes rejects a writable parent. Existing
  bootstrap assertions now observe the asynchronous accepted-to-active lifecycle.
  Docker end-to-end result is pending; these fixture corrections are not product
  privilege fallback or a claim that acceptance has passed.
- Docker same-image host acceptance now passes as
  `run-0e82f8ca1fc44609a1982d3ba55c067e`: both SDK media probes, Offline exclusive
  release, Remove/Rejoin and recreated-container original-byte reads pass. Fix
  fixture group permissions explicitly after mkdir (umask had removed write),
  without changing Node privileges or unrelated device permissions. Containerd
  and Docker now have real supplied raw/file evidence on the same frozen image.
  OCI image Python suite: 30 pass under its pinned environment. E2E client Clippy
  and supplied-media/host ownership unit cases pass. A direct root-environment
  invocation lacked PyYAML; the pinned OCI task supplies it and passes.
- Full root fixture suite: 148 pass, one confirmed pre-existing Dataset inventory
  count failure in `selection_test.py` (expects 2 owners, currently 3). This was
  already present before these deployment edits and belongs to Dataset runner
  contract verification; retain the complete failure in `/tmp/crowdb-r235-fixture-gate.log`.
- Source freeze ended after Docker and containerd acceptance. Follow-up UI now
  displays nullable retained supplied classes (including Unknown), with conservative
  Unknown when bounded history is unavailable, and restricts candidate navigation
  to modes with a Node storage workspace. UI gate: 253 cases plus TypeScript pass.
  Extend the real supplied-browser assertion and legacy embedding capability check.
- Managed-node legacy create/batch APIs reject nonempty physical paths before
  metadata publication; the metadata-only batch compatibility tests remain. Three
  real Group-0 Web batch cases pass, including no partial mutation on a mixed batch.
  Migrate large native fixtures to their existing trusted owned-file bootstrap,
  retaining actual inode/size receipts rather than preserving a public bypass.
  Spare capacity observations verify the fresh first-node file's exact identity;
  12 read-only ownership/observation cases pass. Extend the real 128-zone Inspector
  case with a 32-row page bound, inline bitmap and Back-restored page/selection.
- Validate setup status against its retained admission operation/discovery ID,
  numeric node when assigned, and cluster when both sides know it. Retry checks
  intent before submitting it. Five protocol setup tests pass, including rejected
  foreign operation/node/cluster and accepted prebinding status. Rerun Console
  suite and affected Clippy before committing/rebuilding the next frozen image.
- Complete standard-mode entry-point routing: physical group menus open the
  verified file dialog; disk status/deletion menus route to Node storage instead
  of legacy status/delete calls. Keep maintenance actions and legacy mode controls.
  Capacity disk detail exposes Manage disk in place of direct Up/Down. Backend
  guards remain authoritative. UI gate now 254 + TypeScript, including menu mode
  regression; real hardware and Inspector assertions cover these entry points.
- Full Console gate found five old `diskdb_routes_test` cases still configuring
  only local data despite Group-0-owned publication already existing at the prior
  HEAD. Failures were missing management seeds, before their assertions. Give
  the mutation fixtures the existing real in-memory metadata peer and seed their
  actual hierarchy. Keep unavailable-Group-0 cases unavailable and keep the
  missing-group test's API setup/no-group state. All eight exact route cases pass.
- Full Console gate now passes for shared/CLI/Web, artifact
  `unit/console-5f0d691903b34d308b7537b9cb1fd6be` (25.19/31.55/64.73 s).
  Latest UI 254 + TypeScript pass, artifact
  `unit/ui-946208547d2a433abbf67458d63f64f1`. Affected Web/protocol/E2E all-feature
  Clippy and workspace fmt pass. Register the actual visible center/sidebar
  scrollers with existing bounded navigation restoration; the real Inspector
  regression asserts its exact scroll offset alongside zone/page after Back.
- Third runtime/UI freeze is `89a396d9f81a35bb01a6029109c885e18a838423`.
  Release stage, rootless OCI build and Docker import pass. Manifest
  `sha256:90b4dd43e78454b71ca74d740fe76e91a303273cb11223e60dcba064c3b5a184`,
  config `sha256:e196fb21f5b32184506ce12c67b010996704f5f4826915a05924f2fd1c1e9405`,
  runtime `c45c28c44b7680650f3ac0add5ddd6330669b0de082ac042ac8260ca78886a2e`.
  Default E2E host client compilation passes. Start final hardware/Inspector,
  admission/default-plan and affected navigation/capacity acceptance on this image;
  keep runtime/UI frozen while gathering that evidence.
- Final hardware acceptance passes on the third image:
  `run-b4e9347e46fc4671bb14dab659cb9344`, browser 17.9 s / host 42.76 s.
  Its screenshot confirms the shared dark controls and real per-disk review;
  `supplied-media-proof.json` preserves all three original inode/size/tail markers.
  Verified native TypeScript in the actual child `browser-client` with installed
  compiler (`npx --no-install`), not the uninstalled root client source snapshot.
- Final Inspector passes as `run-442cf52d0b1747a6998ffe0aa9721dee`, browser 7.7 s
  against prior 5.0 s, host 19.57 s. Both actual 4-TiB groups, owners and capacities
  remain checked. The UI uses explicit Diagram, Rack node rows, and Node storage
  sections instead of removed center lists. The first diagnostic run retained the
  old default-Diagram assumption; the next contained an incorrectly outer-scoped
  row `has` locator. Correct those test navigation/locator assumptions without
  touching frozen production code. Thirty-two visible zone buttons, actual Zone 32
  bitmap, no zone tree, and Back-restored page/selection/exact scroll all pass.
- Scanner passes as `run-eff2c46251ea451aa56bf1410f664ad7`, browser 3.0 s against
  prior 2.3 s, host 20.03 s. Keep initial unobserved scan/usage distinct from an
  observed never-run scan: explicitly Refresh before checking authoritative scan
  state. Scope summary labels away from new sort controls and navigate current
  Rack rows/Node storage. Raw API scan/recalc and original drift assertions remain.
- Maintenance diagnostic `run-8e0bf97b2cb84f73bae91964f31afc88` correctly committed
  Offline but retained `No scoped DiskIO exclusion confirmation is available`:
  this fixture deliberately starts DiskDB only. Its old direct status menu is now
  replaced by Node lifecycle. Assert that real UI route, committed Offline, the
  explicit unmet condition and disabled Remove even after acknowledgement. Do not
  invent an exclusion confirmation or weaken the separate real DiskIO-positive
  Remove/Rejoin acceptance. Keep metadata-only hardware badge API cases intact.
- Maintenance now passes as `run-88586d7c71b24067b3d8b0804b5c9bdd`, browser
  13.3 s against 6.8 s (under 2x), host 26.13 s. Actual maintenance requests still
  target the chosen owner/group, every hardware badge remains checked, and absent
  DiskIO cannot be mistaken for safe removal. New Node lifecycle replaces the old
  physical-disk direct status menu; metadata-only API badge fixtures are unchanged.
- Third-image full bridge run `run-c0a60bd1f6dc4684a1fc315421954cae` passes every
  browser stage through default submit (12.7/5.0/2.7/11.4/9.5 s), then times out
  reading Node 1 Access setup after the actual monitor restart. All three durable
  setup journals are Ready; failure is observation, not an absent accepted intent.
  Owned `node-resources.json` proves no OOM and Node 1 PID peak 256 / max events 40.
  Its Chunk-KV log again contains foreign-exception aborts. No local core exists;
  the host routes dumps through Apport, so do not claim that crash independently
  explained or fixed without its report. Preserve the diagnostics.
- Confirmed an independent upstream resource defect: every SSH control CLI used
  the service-sized Tokio pool. A real Unix-socket CLI regression with worker
  setting 64 measured 66 threads before the fix. Only Node/Run now build the
  service worker pool; control/probe/validation/credential commands use a current-
  thread runtime. Both worker settings 1 and 64 now produce the same bounded two
  threads, and the real reply remains checked. Profile validation also ignores a
  service-only worker setting. Do not raise PID limits, timeouts or caller retries.
  This ends the third runtime freeze; run the Monitor gates and rebuild before
  rerunning the exact ordered bridge profile. Do not claim the bridge passed yet.
- Full Monitor integration suite: 130 cases pass, including the two CLI cases;
  affected all-target/all-feature Clippy and workspace fmt pass. The observed
  two-thread control bound is constant under worker settings 1/64; it includes
  the executable's existing auxiliary thread, rather than asserting a fictional
  one-thread process. Keep actual request/reply and subprocess success assertions.
- Launcher boundary regression also proves that writing an empty mapping receipt
  after starting single mode dirties its pristine root before bootstrap can accept
  the manifest. The first test failed with `container-storage.json` present and
  with supplied inputs incorrectly accepted. Single mode now rejects external
  media before launch and leaves empty-mapping receipt creation to standard mode.
  This matches the existing fixed preview profile, without weakening clean-root
  validation or deleting retained files. All 31 OCI host tests pass. The adapter
  is host-only and excluded from staged image inputs; the Node stage remains the
  committed `a1eeb063` sources, while host adapter changes are tracked separately.
- Real bare-root supplied-media regression `run-eb3a6a8cef05435db691e02f3ee6d7f0`
  fails before the fix: the runtime-created disk parent is `0:755`, so UID 10001
  cannot create later managed files. Existing fixtures had precreated/chowned it
  and masked this product gap. Manual entrypoint now rejects a symlink/non-directory
  parent and prepares only that directory for crowdb; no recursive source changes.
  Remove fixture-side preparation from Docker/containerd and assert actual UID
  plus write capability. Preserve external source UID/GID/mode in media proof
  alongside inode/tail checks. OCI 31, media ownership 4 and shell syntax pass.
  This changes a packaged runtime asset; commit, restage and build a new image
  before actual acceptance. Do not reuse the prior image for the new entrypoint.
- Clarify parent deletion before final freeze: the existing authoritative guard
  rejects retained receipts even after Remove, and also rejects an owner/binding.
  The previous confirmation incorrectly implied that removing membership alone
  makes a group deletable. Explain the real unassigned/no-retained-media condition;
  do not add an unsafe implicit forget/cascade. Existing CRUD acceptance must
  retain its separate empty/unassigned group deletion checks and verify that
  physical media keep their recovery parent. This is a documented release limit.

### Frozen final image acceptance

- Source `e7754f3ff64c98a33f853ecf23f9014568d81fe4`; OCI manifest
  `sha256:494da00fbc5556b5603232c9a310b31c228ed83d780278a76648f21d8cbc7bca`,
  Docker config `sha256:ac196700658d3862a0daa9a577b1092f359415bb76dd69c63907a4b72868d232`.
- Containerd profile passed, artifact `container-e2e/run-ee59e92050114d6581a708181effb415`: bare
  data root, common directory owner/write check, source permissions unchanged,
  file/raw fsync/read, Offline release, Remove/Rejoin, container recreation and
  single-mode bootstrap/recreation. No packaged sources changed during acceptance.
- CRUD current-image baseline: `run-454f4064f1644cbbbb3110dfc2241931`, 6.6s,
  first divergence obsolete Add Disk menu expectation. Migrate to verified file
  operation after empty-group binding, actual DiskIO and retained-parent 409.
  Successful UI unassignment/deletion is deferred: public routes have no explicit
  owner/binding removal. Decision recorded in R235; no direct fixture metadata
  deletion substitutes for the missing operator flow.
- Docker host profile also passed on the same image:
  `container-e2e/run-de861aaabc9a48829b7e4ecc0ffff747`. File/raw retained-data
  probes, source UID/GID/mode checks and same-image persistent recovery passed.
- Updated CRUD browser passed in 8.6s (old 12.4s), artifact
  `container-e2e/run-8bbddb09896d4538806c4c6c0afd4de2`; generated 64-MiB
  preallocated media registered with actual DiskIO, binding moved before receipt,
  physical legacy menu controls absent, retained parent deletion returned 409.
  Native browser TypeScript check passed against that run's client snapshot.
- Ordered bridge `run-af7bf01453ce4e0ba5228c691f7aae0f` reached all six
  services Ready after monitor restart, then failed an undiagnosed whole-stat
  equality assertion. Saved cgroup evidence: PID max events 0 on every node,
  peaks 183/253/223 on the active three, no OOM. This is not yet a full pass.
  Added before-restart nonsecret receipt/stat artifact and before/after assertion
  diagnostics without changing the assertion or its timeouts.
- Same-filesystem owned 64-MiB preallocation probe: identity and size unchanged
  after 64 separate 4-KiB writes/fsync, `st_blocks` grew 131072 -> 131080. Thus
  block-count equality is not generally a file-identity invariant; confirm the
  exact bridge difference before changing its assertion.
- Freed only three verified disposable 16-GiB managed files from failed older
  `run-c0a60bd1f6dc4684a1fc315421954cae`; kept journals/logs/resource evidence.
  Exact generated paths, regular type, UID/GID 10001 and sizes audited first.
  No unrelated media or Docker state pruned.

- Full native inspection current-image run
  `run-c08a7513edb145678bc44613324173c5`: 10/14 browser cases passed.
  Capacity bounded fetch 1.6s and interrupted DiskDB Unknown 3.7s passed.
  Zone first divergence: obsolete tree traversal did not open Storage before
  finding DG; replaced with existing actual `diskBranch` navigation helper.
  All pixel, block windows and Zone paging assertions remain unchanged.
- Independent Access stop request exceeded its existing 10s request budget;
  test tracked only confirmed stops, so it failed to restore this unknown stop
  and caused two later S3 502 failures. Track attempted stops before dispatch
  and instrument stop/restore steps. Keep stop timeout/assertions unchanged;
  rerun ordered selection to distinguish service latency from cascade.
- Ordered bridge with unchanged stat equality passed:
  `run-95e12d3a36db4ef0956cb0a6eb7006f4`, 160.97s host time. Preparation
  12.8s, other-UI resume 5.2s, independent cleanup 2.2s, authenticated update
  11.3s, default submission 9.7s, completed middle-panel progress 1.2s.
  Default preallocation and all actual processes survived monitor restart, with
  original operation identities and exact file stat unchanged in this run.
  Prior occasional stat difference remains unexplained; preserve the original
  assertion and new diagnostic artifact, do not label that root cause fixed.
- Binding baseline `run-1f549a772cbc46c184a4d6d9b8cdf0ce`, 3.4s: old flow
  read Group 1 immediately when the asynchronous initialization dialog closed.
  First divergence was no known Group-0 leader. Observe completed initialization
  in the actual Cluster status with the existing 10s election bound before
  checking Group 1; no mutation retry or broader request timeout.
- Final-image canvas scope `run-3ffe7788ba5244f38a5c696d386ef48a` passed all
  four cases: fit 2.3s, nested collapse/context 6.2s, pan/reset 1.3s, navigation
  fit 2.6s. Binding scope `run-5c94db77b3db400c9aedad729ad1e731` passed 9.8s
  vs prior 5.8s; observed asynchronous initialization boundary was 4.947s.
  Native TypeScript check passed against that fixture's complete client snapshot.
- Bridge diagnostics now retain before-restart file receipts/stats and print
  both sides on a future stat assertion failure; original equality stays intact.
  Focused admission fixture unit tests: 4 passed.
- Final-image shell scope `run-fedee200552c4c728a97691aea378d59` passed:
  readonly/apiPrefix/module opt-out 3.1s, backend unavailable 0.5s, six domains
  and shared Cluster views 1.5s. No scope or mutation boundary was relaxed.
- Final workspace Rust formatting gate passed (`pixi run rs-fmt-check`).
- Second full inspection `run-b6b5389fb80f459f90d0ec3bcd644d3b`: 11/14
  passed. Fixed Zone navigation's second divergence: row background inspects;
  its explicit disk-name button navigates. Access stop still hit 10s, but attempted
  stop restoration worked and later S3 multipart passed. S3 history encountered
  another stop rejection; added original response text and restore unknown stops.
- Registered focused owned `console.native.browser.storage-windows` with actual
  three-node storage and 21 allocated chunks, without unrelated balancing seed.
  Three original Zone/Access/S3-history cases passed in
  `run-534880431bcf434986529e07b7e21857` (3.8s/3.8s/6.5s). Zone steps are all
  below 2s; current test adds real second bitmap window, three Zone pages and
  navigation restoration beyond its old 1.3s baseline. No wait inflated.
- Adding predecessor ChunkDB interruption also passed all four original cases:
  `run-a88acf7e367a4c93acf910630bc9f8b9`, 66.6s native test. Thus the long-run
  stop failure is not reproduced by that interruption alone. Further ordered
  bisection adds original DiskDB interruption and Iceberg writes.
- CI partition coverage 2 passed; browser selection/reporting tests 3 passed.
  Initial E2E crate all-target/all-feature Clippy passed; new chunk-fixture
  forwarding helper requires a final repeat of that narrow gate.
- Ordered diagnostic variant `run-aa88c2db51984fc486d19d2045faa4a2`: Zone
  3.2s, interrupted DiskDB 3.7s, ChunkDB interruption 36.1s, schema 13.3s,
  Access stop/restore 2.8s and S3 history 6.1s passed. Iceberg 101-file upload
  failed with S3 `SLOW_DOWN` during CompleteMultipartUpload. This reduced
  variant lacks original production balancing preparation and is not a valid
  replacement for the full inspection owner. Restore the focused owner to its
  four storage/recovery assertions; keep the original 14-case owner unchanged.
- User-authorized scope decision: preserve and report broader ordered Access
  stop failures / diagnostic Iceberg load failure for later review in R235.
  No timeout increase, mutation retry, skipped original test or claim that the
  full suite passed. R235's direct disk/bootstrap/UI contract is verified; the
  wider cross-product load/recovery investigation remains deferred.

### Final scoped result

- Final owned storage-window run `run-bfadd9c3c99a4300b52d0d2aea42b903` passed
  all four unchanged assertion sets: actual Zone pixels/pages 3.5s, real
  Mirror/EC placements plus ChunkDB interruption 35.6s, Access stop/restore 2.7s,
  and S3 history/replaced/deleted resources 6.4s. Native test 69.67s.
- Final TypeScript check passed against this complete client snapshot; workspace
  fmt and E2E all-target/all-feature Clippy passed independently.
- Fixture suite 149 passed / 1 known pre-existing failure out of 150:
  `selection_test.SelectionTest.test_layers_select_only_their_domains_and_count_dependencies`,
  line 107: Dataset expected 2 registered cases, actual 3. New Console owner
  updates both relevant counts to 154; CI partition/disjoint coverage passes.
  Preserve the unrelated Dataset expectation for its owner; R237 is not started.
- Packaged sources stayed frozen at `e7754f3f` throughout final image acceptance;
  later commits change docs/test/host evidence only. No push or PR requested.
- Remaining review/deferred work: explicit owner/binding unassignment; full
  shared-workload Access stop timeout/rejection; prior intermittent full-file
  stat assertion difference (new diagnostics retained); diagnostic Iceberg load
  `SLOW_DOWN` without full balancing prelude. No claim of a clean full-regression
  gate or repaired wider recovery. Scope stays R235 only.
