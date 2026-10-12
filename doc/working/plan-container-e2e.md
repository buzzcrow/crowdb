<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; latest accepted native migration `2736c73c` plus the verified retirement batch; browser migration `2e0f9ac2`; CLI migration `62d5dec7`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- The three-node native Console fixture is accepted for logical/Inspector browser
  owners. The sixteen storage/native owners pass the shared parallel gate. Shared-image CI,
  S3 node-fault migration and local-state component proofs are accepted.
  `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Current formal image is built/imported from
  `71f61c4979e73925d6012e433a172f1bd25bde80`, including S3 streaming,
  replica voting/epoch fixes, explicit Web readonly, remote Access endpoints,
  monitor socket recovery, mounted node service logs, remote KV observations,
  configurable DDB heartbeat, restart intent/claim fixes, fenced disk failure
  write-back, canonical KV labels, actual typed startup readiness and validated stream
  geometry in persisted deployment intents, distinct restart execution and live
  per-node process observations. OCI
  `sha256:5e2ba122c8e58047bc1c7c5610d1fba339452f80fa4817b4beee442b97744f15`;
  config `sha256:0603800da1a9179137ff5ffd497eba37217cde33a3a2312657314eb4f3d615f1`;
  runtime `affe80fd3cd6a6ff2a6c542e18d03f81b8959912460d8ba1df6240683d7219d1`.
  Receipt: `target/crowdb.oci.tar.json`; optimized with retained debug information.
  Logs: `/tmp/crowdb-lifecycle-release-{stage,build,import}.log`.
  Last full S3 43/43 (1545.602 seconds) and Console 89/89 (644.632 seconds)
  used `b7b1247b`; backup `target/crowdb.oci-b7b1247b.tar[.json]` and tag
  `crowdb-node:b7b1247b` remain. These results are baselines, not full acceptance
  of the newer image/inventory. Compact domain reports are linked in `test.md`.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 104 pass, `/tmp/crowdb-native-retirement-fixture.log`,
  including managed browser and CLI ownership controls.
  S3 component UT passes 114/114; Access-server passes 108 plus its pinned
  SDK protocol test; Web/Console-shared/E2E all-target clippy
  and workspace fmt pass.

## Accepted assertion references

The completed execution tasks are removed. These compact mappings retain the
original assertion ownership needed to retire launchers; evidence is in commits
and the named run artifacts.

- Metrics' three original cases → `console::web_metrics`: node shape/nonempty,
  Group prefix and Store aggregate, original three-second readiness. Evidence:
  `/tmp/crowdb-root-web-metrics-standalone`; old file removed (`ee25465b`).
- KV happy flow and `kv_get_delegates_to_ops_kv_data` → `console::web_kv`:
  text/binary round trips, client sequence, delete visibility. Evidence:
  `/tmp/crowdb-root-web-kv-sequence`; unavailable-authority UT retained (`1dac8917`, `217965c9`).
- `full_mgmt_cycle_through_web_routes`, `add_store_delegates_to_ops_kv_logical`,
  `delete_server_returns_409_when_replicas_exist` → `console::web_management`:
  Store identity, empty/two-group listing, detail, deletion and server 409.
  Evidence: `/tmp/crowdb-root-web-management-ops` (`217965c9`).
- `cluster_init_delegates_to_ops_cluster_init` → `console::web_bootstrap`:
  Group-0 IDs, replay, stable replica, missing topology 422, invalid bindings
  publish nothing. Evidence: `/tmp/crowdb-root-web-bootstrap-split`.
- `bare_metal_web_bootstrap_consumes_sealed_topology_input` →
  `console::web_sealed_bootstrap`: sealed source, HTTP 201, no residual intent or
  topology. Evidence: `/tmp/crowdb-root-web-sealed-bootstrap`,
  `run-6d91726b6917420580a4988f6701506e` (`aeab91d4`).
- `legacy_in_memory_web_bootstrap_does_not_persist_topology` → shared root Web
  bootstrap plus component `in_memory_configuration_path_never_publishes_topology_or_bootstrap_intent`.
  `mgmt_routes_test.rs` now retains only cached-authority rejection UT.
- `node_diskio_discovers_disk_groups_created_after_deployment` →
  `console::web_diskio_discovery`: readiness before/after Group-0 init, one
  initially empty DiskIO registration, discovery of groups 11/12 within the
  original ten-second bounds, final storage readiness. Uses fixed port 7200 and
  ordinary release deployment, without test-single-node mode. Evidence:
  `/tmp/crowdb-root-web-diskio-late`, `run-36431c401abc4dfe991287a950d14765`;
  old case/helper removed; both remaining provisioning cases are now retired.
- `replica_add_remove_wires_peers_bidirectionally` → `console::web_replicas`:
  two actual KV voters, bidirectional remotes, exact list/detail, missing 404,
  leader delete 204, lone-voter re-election within ten seconds. Added authority
  voting and persisted epoch consistency proof. Evidence:
  `/tmp/crowdb-voting-epoch-web-replicas`, `run-5bd51543ace04ad78fa968985013772b`.
- `remove_leader_from_five_node_group_elects_new_leader` →
  `console::web_leader_removal`: five actual voters, delete 204, four remaining,
  removed peer absent on all nodes, new physical leader within ten seconds.
  Evidence: `/tmp/crowdb-voting-epoch-web-leader`, `run-36708ce7afd8422d8090c81f8591ee7a`.
- `remove_unreachable_leader_retains_group0_membership` → same root module:
  owned kill/death within five seconds, original 500/error text, retained Group-0
  member and unknown role/state. Preserve network namespaces to retain endpoint
  failure semantics; reserve all five helpers. Two independent parallel runs
  pass: `/tmp/crowdb-voting-epoch-web-unreachable-retained-{a,b}`.
  Both original replica files/launchers are removed.
- All three original shell cases → `console.browser.shell` /
  `clients/console-ui/e2e/flows/00-shell.spec.ts`: backend-unavailable alert,
  seven-domain navigation and real-data embedding (apiPrefix, readonly, module
  opt-out). Original spec is removed. Each host browser owns an ordinary release
  Web container and client workspace; KV uses fixed 7000/7001 inside that node.
  Unavailable listener uses closed container port 9091 to avoid loopback access
  from a private-IP page. Proxy page readiness precedes the embedding navigation;
  original three-second request/DOM assertions remain, with no mutation retries.
  Three cases pass together concurrently in
  `/tmp/crowdb-root-shell-proxy-ready/run-11b32d674f504ebfaaa630da85c9077e`,
  then at CI budget in `/tmp/crowdb-root-shell-ci-final`. Browser fixture reserves
  at least 2 CPU plus host client (2.5 CPU / 2560 MiB total); 4 CPU budget gives
  one case slot, 6 CPU allows two. No private browser is installed. Image reuse
  excludes only the audited unbundled source spec; UI source/build guards remain.
- Five independent registry UI cases from `10-cluster-rack-node.spec.ts` and
  `13-todo-ui-behavior.spec.ts` → `console.browser.registry`: empty shell and
  runtime-error check, datacenter/sidebar/canvas rack creation plus API identity,
  Cluster/Chunk selections without ownership probes, Access-only port overrides
  and disabled/waiting states across reload, fixed-slot warning health/detail
  and persisted plan. Only these original cases are removed; multi-service
  cases remain. Each case owns a release Web container; no shared reset or
  native port allocator. Fixed Access plan ports are 9000/9001/9002.
  Exact old cases pass in `/tmp/crowdb-r230-ui-baseline.log`. Root five-case CI
  gate passes in 38.580s at 4 CPU / 8192 MiB / one slot (`run-b02f6a6cf861454e85f49e1d0200f105`).
  Initial pipelined parallel runs fail at pending browser rack/node POSTs while
  peer networks start/stop; extra node CPU does not fix them. Stable-topology
  parallel control passes. Host shell/registry scheduling now starts a bounded
  batch before clients and retires it after every client completes. Five cases
  pass with two slots in 26.301s in `/tmp/crowdb-root-registry-batched`
  (`run-d4552a3c2db5477492d3844aebe9b378`). Existing shell 3/3 also pass
  concurrently after scheduling changes (`run-bb37cf5e8bfd46bcbdc2e63c8d2759ec`); lifecycle component
  proofs cover bounded ordering, client failure and partial-start cleanup.
  Image reuse exempts only the two audited host specs, retaining UI source guards.
- All three original KV data page cases and all four canvas cases →
  `console.browser.kv` / `console.browser.canvas`: exact 60-row pagination,
  selected-key browser history, uppercase binary formatting/CSS, initial and
  reselected auto-scan, system-group readonly UI;
  viewport fit/pan/domain restoration, collapsed children/nested state/context
  menus. Old source files are removed. Owned normal Web/KV uses fixed 7000/7001
  and durable defaults; no host launch, reset, port allocator or mutation retry.
  Baseline: `/tmp/crowdb-r230-local-browser-baseline-valid.log`. New 3/3 and 4/4
  pass with two independent containers in `/tmp/crowdb-root-browser-kv-literal`
  and `/tmp/crowdb-root-browser-canvas`. Browser selectors escape literal test
  names, including parentheses, so exactly one case runs per fixture.
- Both original managed browser cases → `console.managed`: physical write 503,
  KV put/query/delete, Iceberg schema and demo cleanup, S3 preview and actual
  Chunk/disk placement, Group-0 outage under the original ten-second bound,
  retained monitor observation and recovery. Each host browser owns a complete
  release node and workspace; pause/resume targets the owned container KV PID.
  Normal mounted Web configuration explicitly disables readonly internally.
  Both cases pass concurrently in 25.023s (`run-1a8ef3d01e5a45e7a969da8202e25c42`).
  The public readonly browser gate passes (`run-f196be80010c4dbaa5c6fc46b99bb59e`).
  Original managed spec and native server launcher are retired; its old script
  delegates to the root scope. The same immutable image serves both modes.
- Both original Inspector activity cases → `console.browser.inspector`: exact
  ping/restart/stop replies and activity entries, clearing activity, domain
  selection invalidation and confirmed cluster destroy. Each host browser owns
  a release Web/KV container with fixed 7000/7001 and normal durability. Original
  cases pass in `/tmp/crowdb-inspector-original-baseline-v2.log`; root 2/2 pass
  concurrently in 14.777s (`run-eb5e36998eb64f6689ff92bd1f3e2653`). Original
  spec is removed.
- Both original CLI S3 replay cases → `console.cli.container`: host-client
  interruption after confirmed Group-0 or Diskdb deployment, relative roots,
  stable cluster/container/binding identity, retained Diskdb PID and execution,
  marker removal and no local topology. The Group-0 case also owns
  start/status/stop/restart/delete and exact acknowledged object bytes; the
  storage case retains only its distinct recovery checks. Each cluster has
  three formal release nodes with fixed ports and its own persistent bridge.
  Temporary state files carry the owner ID; delete removes only those files,
  with foreign-file/network and symlink rejection retained in component tests.
  Both cases pass concurrently in 70.097s at 8 CPU / 16384 MiB / two slots
  (`run-41dd8b38756e43069ba73ec15cc3b10b`); each node uses 1 CPU / 2048 MiB.
  Original host cases are removed. Twelve CLI component tests, 95 fixture tests,
  workspace fmt/clippy and 28-package CI reachability pass.
- `swagger_routes_are_removed` → component `swagger_routes_test.rs`: all three
  retired paths; no real server startup (`ff711b4c`).
- Both original Store/Group UI cases → `console.browser.logical`: Store and
  Group creation, exact duplicate 409, selectable/copyable error text, Replica
  addition and authoritative API identity, Replica and Group deletion. Both
  original Node Inspector cases → `console.browser.node`: logical-to-physical
  and physical-to-logical jumps, local logical replicas, no remote-proxy rows,
  logical members retained after an actual remote proxy deletion. The original
  duplicate rack/node case → `console.browser.registry::duplicate_ids`: exact
  409 responses and retained inline node error. Source names remain literal in
  the root specs; fixture-owned node IDs replace arbitrary host IDs.
  Original baselines pass in `/tmp/crowdb-{store-group,node-inspect,registry-duplicates}-original-baseline.log`.
  The complete root browser selection passes 22/22 with two slots in 171.539s
  (`run-bf68df629d534cd886532934d403168a`), including the five new owners.
  Every UI action retains the original three-second timeout. Three actual
  manual release nodes provide each logical/Inspector cluster; public admission,
  bootstrap and live KV observation precede the host browser. No reset, host
  launcher, port allocator or mutation retry remains in these owners. Both
  original specs and the duplicate registry source case are removed.

- The binding/owner portion of both native source cases now has one accepted
  root owner: `console.native.binding` / `native_binding.rs`. It preserves
  Store 1 pre-registration binding, lower Store preservation, acquisition within
  three seconds, missing-binding 500 and explicit repair, stable owner identity
  and nondecreasing lease expiry. The host uses public HTTP and the production
  KV client; normal deployment selects a one-second heartbeat without test mode.
  Acceptance: `run-da4f96d27d2c4e22b37a024f52611542`, 1/1 in 15.261s at
  4 CPU / 8192 MiB. All other branches now have verified owners below;
  both original native cases and their private helpers are retired.


- Native source multipart/location and restart assertions → `console.native.s3`
  and `console.native.recovery`: exact 9-MiB bytes, two physical locations,
  real ChunkDB acknowledged cursor, stale replacement cursor 409, all six typed
  Node-1 restarts, actual live Group-0 leader restart, namespace-owned old/new
  PIDs, exact 18-service inventory and retained object bytes. Stale-owner disk
  failure publication is fenced, with a deterministic component proof.
- Original `71-s3-native.spec.ts` → `console.native.browser.s3`: identical UI
  upload, HEAD, 4-KiB preview, complete-byte verification, physical placement,
  browser history and deletion assertions. Exact original per-test baseline is
  7.934s; the root copy passes separately and in the shared native gate. The
  original spec and selector are removed; host browser selectors never enter
  the src-only production UI build.
- `CROWDB_NATIVE_LOAD_ACCEPTANCE` branch → `console.native.load`: actual 512
  incompressible values, normal heartbeat/serving-lease bounds, retained bytes
  within 4–256 MiB, owner restart and exact sampled recovery bytes/scale.
  The shared native gate passes 5/5 with two slots in 105.632s at 8 CPU /
  16384 MiB (`run-624a60c4ad494c519b80938bfa8459bc`). The same formal image
  supports all cases and explicit mounted sparse-disk capacity profiles.
  Both original native source cases are retired after the complete shared gate.


- Remaining native branches → independently selectable `console.native` owners:
  four literal lifecycle/default/plan/prerequisite callbacks, scanner/recalc,
  large Journal pagination and a real second restart/stale 409, mixed units
  rejected before consumers start, production split/transfer and all 512 exact
  values. The existing inspection owner retains fourteen literal browser cases,
  21 real Mirror/EC chunks and full normal balance/data checks. Cold cyclic
  hint budgets remain in the four component `conditional_retry_test` proofs.
  Complete stable-network gate: 16/16, no failures/skips, 1234.764s, two slots
  at 8 CPU / 16384 MiB on `71f61c49`;
  `run-ae039e342c7c4326b353645b38068f55`. Five stable/shared runs have redacted
  evidence under `.crowdb-runtime/artifacts/container-e2e/`.
  Old native Rust cases, four private helpers, seven old native browser callbacks
  and their selectors are removed. Accepted Console inventory is 142.
  `test-console` now runs only shared/CLI/Web component proofs, with no native
  server build or launcher. Important local process/ownership UTs remain.
  Component 263/263, fixture 104/104, image-source guard, native TypeScript,
  workspace fmt/clippy and CI task reachability pass. Logs:
  `/tmp/crowdb-native-retirement-{components,fixture,identity,types,fmt,lint,ci}.log`.

- All three ordinary `53-chunk-capacity-canvas` cases → the existing
  `console.native.browser.scanner` owner: cluster totals and three live
  instances, Refresh, DiskGroup/disk boxes, geometry/Recalc results, datacenter
  root/type/rack count and exact hardware aggregate. The old conditional branch
  that skipped disk assertions is replaced by mandatory actual disk observations.
  Native gate passes at 4 CPU / 8192 MiB in 35.127s
  (browser 2.3s); `run-b79caccc085746698f63afbc3c8a661c`. TypeScript passes. The whole old
  Canvas source/setup is removed; no additional overlapping E2E owner is added.

- Two ordinary Disk source cases (hardware aggregation and dashed disk status)
  → `console.native.browser.hardware`: two exact 4,096,000-byte disks, unit
  size, rack/node/DiskGroup sums, Inspector disk count, and Suspect/Up/Offline
  HTTP 204. Only KV/DiskDB prerequisites start; no unrelated consumers are
  needed for metadata contracts. Gate 1/1 at 4 CPU / 8192 MiB in
  24.193s (browser 1.2s); `run-7e1bbada099b4f85a0dd2401ff86b093`. Both old
  source cases are removed. Accepted Console is 141; the last ordinary Disk case
  is retired after its separate actual Inspector gate. Fixture 104, native
  TypeScript, fmt and full clippy pass; logs:
  `/tmp/crowdb-capacity-hardware-{fixture,types,fmt-check,lint}.log`.

- Ordinary Disk maintenance source → `console.native.browser.maintenance`:
  real Compact/Rebuild dialogs and zone-input validity; scoped Recalc through
  owner 1 and Scan through owner 2; live disk-group/disk Offline menus plus the
  empty DiskGroup status route; all seven hardware status badges after reload.
  Actual disk observations use mounted files and ordinary DiskDB deployment.
  Gate 1/1 at 4 CPU / 8192 MiB in 31.405s;
  `run-54fbee1588bc46579b29aae2fc821784`. Old maintenance case is removed; no
  assertion timeout, retry or fake response is introduced. Fixture 104, native
  TypeScript, workspace fmt/clippy pass; logs:
  `/tmp/crowdb-capacity-maintenance-{fixture,types,fmt-check,lint}.log`.

- Last ordinary Disk Inspector/selection source →
  `console.native.browser.disk`: two mounted 4-TiB disks on node 1 with real
  DiskDB observations, 8.0-TB presentation, two DiskGroups, exact capacity and
  geometry, cluster/rack/node/DiskGroup/disk navigation, known usage, all six
  action buttons, scoped Recalc/zone grid and retained Inspector parent fields.
  Three real voters remain in separate containers; added healthy metadata nodes
  are included in the exact rack/node totals. Gate 1/1 at 4 CPU / 8192 MiB in
  25.066s (browser 2.6s); `run-cf5a3df33aeb4a9d885308989305ec7b`.
  First attempt stopped at an incorrectly exact-matched title; actual DOM and
  source include the zone count. Corrected to the complete observed title,
  retaining the original three-second bound. Fixture 104, native TypeScript,
  workspace fmt/clippy pass. The whole old Disk source/fixture/port allocator
  is removed. Logs: `/tmp/crowdb-capacity-inspector-*.log`.

- DiskGroup binding recommendation source → `console.native.browser.binding`:
  a prepared three-container cluster is initialized through the browser with
  `create_data_group=true`; actual Group 1 is then usable. Preserve missing
  binding 422/no partial group, cross-node counts, least-loaded recommendation,
  manual override, exact persisted binding and subsequent recommendation.
  Gate 1/1 at 4 CPU / 8192 MiB in 24.867s (browser 5.8s);
  `run-25275c49372341ca915ba0b29ff4f8a1`. Fixture 104 and native TypeScript pass. First
  dispatch selected the old shell configuration and found no tests; the native
  scope now selects its own configuration explicitly. The old case is removed.
  Constructor defaults still bootstrap existing native owners as before.

## Remaining browser migration

- [ ] **Cluster and shell**: classify two shell behavior cases, three rack/node
  cases, four ordinary lifecycle cases, two Todo/service-progress cases, and
  the standalone monitor-discovery case. Map cancel/default/tree/health,
  selected-service persistence, process shutdown and failed-plan retry to
  verified owners. Discovery's stale-on-loss assertion still needs explicit
  comparison with the root discovery/browser coverage. Files:
  `app/crowdb-web/ui/e2e/flows/{01,10,11,12,13}-*.spec.ts`.
- [ ] **Capacity**: migrate five DiskGroup cases. Preserve CRUD,
  owner/binding selection, maintenance state, capacity
  totals and unknown/error observations. Files:
  `app/crowdb-web/ui/e2e/flows/50-chunk-capacity-disk-group.spec.ts`.
- [ ] **Topology and cross-domain**: review three topology cases and the two
  full-chain/three-node-data flows. The old topology setup starts 23 nodes for
  disjoint tests; select each flow's actual node subset instead. Preserve the
  eight-node comparative topology, different group memberships, complete
  leader/term/lease observations, exact bytes and cross-group scan isolation.
  Retain the six-node storage-plus-diskless admission/data boundary in its
  own container profile. Files:
  `app/crowdb-web/ui/e2e/flows/{22,90,92}-*.spec.ts`.

## Final integration and closure

- [ ] **Launcher and coverage audit**: enumerate remaining production executable
  and in-process service launch sites, including C++/browser/SDK/manual tasks.
  Map every original distinct flow to an accepted E2E or retained component UT.
  Remove duplicate old owners only after verification. Unused native KV/storage
  harnesses are removed; shared `TestDir`/ownership UTs remain. Explicit production
  local-launch port allocation remains until its deployment callers are audited.
  Current Rust client audit: 318 compiled tests = 305 registered required/manual
  owners + 13 preparation/isolation helpers; shell browser cases are separate
  host owners. Evidence: `/tmp/crowdb-r230-current-client-helper-audit.txt`.
  Refresh after further migration; remaining launch-site evidence is
  `/tmp/crowdb-r230-final-launch-sites.txt` and
  `/tmp/crowdb-r230-current-browser-task-launch-audit.txt`.
  SDK/browser flows have separate declared host owners.
  Audit fixed listener ports/dynamic store pools and retire unused test port
  allocation code; preserve actual deployment callers. Verify data/log/crash
  mount mappings and truthful unsupported host-core diagnostics.
- [ ] **Pixi classification**: finish component versus required container versus
  explicit special/manual task classification, canonical selectors and aggregate
  delegates. Check no duplicate execution or remaining implicit native server
  preparation. `test-storage`/`test-access` component cleanup is committed;
  native Console component cleanup is complete; ordinary UI paths remain. Manual TPC is now an explicit root scope with a separate locked host environment. Java FileIO has SDK ownership and
  foreground GC has the native GC delegate; preserve these distinct selections.
- [ ] **Close CI classification**: finish selectors after native owners and the
  managed browser contract are resolved. Required domain jobs share one verified
  formal artifact; special host/containerd gates use root profiles. Add the
  remaining accepted browser delegates. Bridge admission is already integrated. Check
  each job's CPU/memory reservation and upload only redacted diagnostics.
  Public exports now retain machine-readable measurements and redacted logs,
  excluding mounted data/configuration and host client build trees. YAML and
  28-package task reachability pass; actual remote CI has not run.
- [ ] **Final regression and measurements**: rerun affected complete domains
  once the remaining migration inventory settles. The current membership image
  passes complete Console 96/96 in 691.960 seconds and Deployment 33/33 in
  601.028 seconds at CI quotas (0.25 CPU/server, 4 CPU / 8192 MiB, one effective
  case slot). Reports: `run-ab39e156c2c24da39a5db1bfbcba1a7c` and
  `run-5cfba02195364ce483aac4b5a3eaf1ee`; logs under
  `/tmp/crowdb-voting-epoch-final-{console,deployment}-ci.log`.
  The twenty-five newer browser cases, two CLI replay cases and nineteen native client cases pass separately and are not
  included in the complete 96-case measurement. Current accepted Console inventory is 142;
  final full-domain measurement remains pending. Remote CI and macOS remain
  unmeasured. Files: `doc/working/test.md`, root runner reports.
- [ ] **Close R230**: after inventory, assertion mapping, local gates and required
  environment results are satisfied, remove R230 detail/backlog entry and this
  plan in the final cleanup commit. Requirement commits use coherent verified
  batches and single-line subjects without requirement numbers.
