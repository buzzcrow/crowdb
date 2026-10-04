<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI Follow-up Plan

Persistent issue list requested by the user; keep this filename and remove
completed items after verified fixes. Record newly discovered UI and supporting
API problems here, including workarounds that do not constitute fixes.

Design: [Console UI specification](../design/console/design-crowdb-console-ui.md).
Execution history: [UI implementation plan](plan-console-complete-ui.md).

Goal: make the normal one-rack, three-node flow work without manual repairs.


## Open issues: current UI requirements (2026-10-04)

These entries describe requirements, not implementation plans. Node ownership presentation remains deferred. The user subsequently authorized
fixing the Chunk-KV graph regression and checking Zone allocation correctness.
The user explicitly retained KV display, Cluster collapse and test work.
Previous mocked results do not satisfy real end-to-end acceptance.

### UI behavior

- [x] **S3 object storage locations**: show the object's actual Chunk mapping,
  including Chunk ID, object byte interval, chunk offset and length. Displayed
  locations must agree with authoritative stored metadata, including shared
  chunks and multipart objects. Clicking a location opens the correct Chunk;
  returning restores the object and selected location. Unavailable locations
  must be explicit. Backend native multipart/location verification is now passing;
  real 9-MiB multipart browser navigation, selected-extent restoration and full
  byte verification now pass in the normal three-node fixture.
- [x] **Chunk-KV graph regression**: a populated cluster must display its
  Chunk-KV servers, owned splits and associated trees in the center graph.
  The graph must remain visible and readable after entry, tab switching,
  refresh and panel resizing. Distinguish empty, loading and unavailable states.
- [ ] **Node ownership information missing — UI design deferred**: selecting a
  Node must make its current service ownership and KV Group storage ownership
  independently understandable across all 1024 hash slots. Noncontiguous
  assignments, complete slot identities, owners and generations must remain
  accurate. The left tree must reflect current ownership records rather than
  obsolete range data. Record this requirement only; no new layout, bitmap
  arrangement or UI implementation is approved here.
- [x] **Capacity zone bitmap correctness**: each displayed block reflects its
  actual allocation bit: blue used, green free. A partially occupied zone must
  not appear entirely used because of incorrect decoding or misleading display.
  Make the scope of any displayed block window clear. Usage figures and bitmap
  must be consistent; unavailable bits must not be presented as used or free.
- [x] **KV mixed text/hex display**: preserve readable characters in Key and
  Value. Only undisplayable characters/bytes appear as uppercase hex, without
  `0x`, visually distinguished clearly from ordinary text. Apply this to list previews and full
  details. Presentation must not change the original bytes used for paging,
  selection, copying or mutations.
- [x] **Cluster graph default collapse**: initially show Datacenter → Rack →
  Node, with Node children collapsed. Clicking expands them; users can collapse
  them again. Preserve expansion state on return; Fit All and right-click must
  not unexpectedly expand the graph.

KV and Cluster changes were retained by explicit user clarification. Their
affected ten real-backend browser cases pass in 20.7 seconds; the three byte
presentation unit cases and TypeScript checks pass. Other issues below remain
unaccepted; this is not a full-suite result.

### Real E2E acceptance gaps

- [ ] **No mocked E2E acceptance**: all browser acceptance uses real services,
  APIs, persisted metadata and file data. Existing mocked cases must receive
  real equivalents. Keep unit tests separate from E2E counts. Report collected,
  passed, failed, skipped and uncovered requirements honestly; mocked success
  cannot close a feature task.
- [x] **Native coverage inventory**: map each agreed UI feature to real E2E
  cases and identify missing scenarios. Existing mocked coverage includes
  shell/embedding, auxiliary service plans, Capacity, Chunk, Chunk-KV, Iceberg
  and S3. Cover their current contracts rather than relying on the case count.
  The concrete feature inventory and uncovered acceptance tasks follow below.
- [x] **Fresh three-node provisioning**: one Rack, three Nodes, Groups 0/1 and
  one of each of the six services per Node must become usable without manual
  binding, owner, range or catalog repairs. Resolve the outstanding first
  Chunk-KV readiness failure and verify real data writes afterward.
- [ ] **Interrupted operations and recovery**: verify partial deployment,
  retry, reload, restart, concurrent operators, prerequisite arrival and reset
  cancellation. No duplicate service, orphan process or inconsistent resource
  state may remain.
- [ ] **Creation defaults and mode boundaries**: every create dialog supplies
  valid conflict-free IDs, ports and dependencies on repeated use. Container
  mode rejects topology/disk management in both UI and API while permitting
  supported data operations. Verify readonly embedding and outage recovery.
- [ ] **Navigation and changing resources**: verify every cross-view link and
  Back/Forward route, restoring selection, page, query, expansion and detail.
  Resource deletion, owner changes, stale pages and late responses must not
  display the wrong resource. Include S3 location → Chunk return.
- [ ] **Real data-browser completeness**: verify paged Chunk lists and multi-
  Strip layouts, Chunk-KV splits/journals, Iceberg schemas/snapshots/manifests/
  files/Parquet metadata, and S3 buckets/objects/previews/multipart/locations.
  Large collections stay bounded; independent pages replace rather than
  accumulate data. Metadata inspection does not unnecessarily read payloads.
- [ ] **Real Capacity and failure states**: verify disk/owner management,
  large inventories, zone paging and actual allocation bitmaps. Unavailable,
  partial, recovering and Unknown observations remain accurate and usable.
- [ ] **Final native acceptance and speed**: complete the ordered real E2E
  suite and required native scenarios. Preserve measurements for slow setup,
  mutations, readiness, UI refresh and teardown. Avoid unnecessary repeated
  provisioning/loading; do not hide failures with retries or longer timeouts.
  R203 remains open until the required acceptance succeeds.


## Inspector simplification

- [ ] **Redesign metrics separately**: Cluster and KV no longer render or poll
  raw internal metrics in the right inspector. Their identity, topology,
  election and read-state properties remain. A future metrics experience needs
  a separate design; do not reinstate the old generic counter list.

## Cluster and provisioning

- [x] **Reset versus in-flight deployment**: the full browser suite exposed
  `Directory not empty` during reset while a recovered six-service plan was
  launching an auxiliary service. Quiesce accepted deployments and fence new
  plan/deployment writes before removing workspaces; cancellation must not
  orphan an unregistered child. Preserve the regression, no mutation retries.
  KV/DDB deploy and restart now retain node claims in owned tasks through
  readiness and registration. Four real-process cancellation cases cancel
  after spawn but before registration, then Reset; all children stop and
  workspaces disappear. Existing auxiliary lifecycle and Reset fence coverage
  passes. No mutation retries were added.


- [x] **Automatic DiskGroup data binding**: creating a DiskGroup must resolve
  its ordinary data group and establish its binding and owner automatically.
  On 2026-10-03, Store 0 had Group 0 and Store 1 had Group 1, but the current
  binding path required an ordinary group in Store 0. Bring-up required manual
  creation of Store 0 / Group 1 and `PUT /api/disk-groups/1/1/1/bind`.
  This is a provisioning bug, not a required operator step. Check the existing
  DG 1 with no disks and reconcile partial creations before retrying. Verify
  fresh creation, existing unbound groups, retries, and usable owner routing.
  Files: shared hardware operations, web `owner_assignment.rs`.
  Native normal three-node acceptance now starts with Store 0 / Group 0 and
  Store 1 / Group 1. DG 1 binds automatically to Store 1; adding Store 0 / Group 1
  preserves that destination. Exact-ID retries preserve ownership and renew
  leases. Removing real binding/owner records from existing DG 3 reproduces
  partial persisted state; ordinary creation restores both automatically.
- [x] **Reconcile missing DiskGroup owners**: DG 1 survived an earlier failed
  creation without an owner. Adding its binding and disk did not repair owner
  registration. Chunk-KV bootstrap then failed repeatedly with `no endpoint
  for disk_group 1`. Explicit owner assignment restored normal bootstrap.
  Existing incomplete groups must be reconciled automatically and readiness
  must check usable allocation routing, not only running DiskIO processes.
  Pending DG creation before DDB registration, DDB arrival, and existing-group
  recovery all pass without manual repair. Actual multipart writes/readback,
  Chunk physical mapping, and DG 1 allocation bitmap succeed with mixed Store
  bindings. Four native browser diagnostics pass; the owned fixture takes
  27.91 seconds, with unchanged waits and slow-step measurement.
- [ ] **Native executable staging race**: one default-concurrency lifecycle
  run failed before its first service started with `ETXTBSY` at
  `upgraded_executable_can_restart_without_relaxing_pid_identity`. The exact
  case and subsequent affected suite pass, so the intermittent staging/fork
  failure remains unaccepted. Diagnose concurrent writable executable handles;
  preserve process identity checks and do not mask this with spawn retries.
- [ ] **Six-service plan recovery**: the one-dialog queue is implemented but
  now persists on the server with revision fencing. Reload restoration and
  competing-browser fencing pass. Verify native service restart,
  partial deployment and prerequisite arrival, with no duplicate instances.
  Do not mark the normal three-node bring-up accepted until all six service
  types run on each Node.
- [ ] **Create defaults acceptance**: conflict-free defaults are implemented;
  complete repeated-create and reopen acceptance across IDs, host listener
  ports, DiskDB port ranges and dependency references.
## Capacity and shared behavior

- [ ] **Navigation return history across all views**: every button/link that
  navigates to another resource or view must retain a return route. Back and
  Forward restore the source domain, resource, list cursor, tree expansion,
  selected detail and scroll position. Keep ancestry breadcrumbs separate
  from visit history; refresh and mutations must not create navigation entries.
  Include property links and graphical nodes, not only sidebar navigation.

- [ ] **Cross-view and failure acceptance**: verify scope restoration,
  disk/node navigation, owner movement, stale cursors, unavailable services,
  and container-mode topology/disk mutation restrictions against the current
  Cluster, Capacity and KV specification.

## Data environment and deferred design

- [ ] **Chunk-KV KV Page inspection API missing**: current runtime exposes
  checkpoint/memory/maintenance counters and bounded journal extent fences,
  not root/child/leaf Page links or key/value bytes. Add bounded metadata/key
  inspection before implementing the Page tree. Default key rendering to hex,
  with optional text rendering; do not represent journal extents as KV Pages.


- [ ] **Chunk-KV split/balance backend correctness**: this is a backend issue,
  not a Partition UI redesign. After splits, healthy Nodes should receive
  owned subranges according to the balancing policy. Split eligibility must
  not stall other eligible ranges; estimated weight must describe retained data
  consistently across recovery and reclamation. Large partitions must not
  interrupt heartbeats or serving-lease refresh while measuring load. Verify
  count convergence and byte-load placement separately. Current live catalog
  has 12 splits distributed 4/4/4; that alone does not prove weighted balance.

- [x] **Balance split eligibility**: an ineligible or unsplittable largest
  partition must not prevent other eligible partitions from being sampled and
  split when the cluster needs more owned subranges.
  Regression coverage includes a single-key largest range, an active transition
  and an inherited overlay; the valid smaller range remains selectable. The
  standalone planner also requires a live key below the boundary.
- [ ] **Balance byte-weight correctness**: reported owner/partition load must
  approximately represent retained data and remain meaningful after process restart,
  recovery and reclamation. Verify count balance separately from byte balance.
- [x] **Low-cost approximate split and weights**: prioritize efficiency,
  simplicity and low system overhead. Split boundaries need not divide keys or
  bytes exactly in half; estimated weights need not be exact. Obtain a valid
  boundary with nonempty children using bounded work instead of iterating the
  entire partition for a median. Load observation must likewise avoid full
  scans and unnecessary background work.
  Native index hints, bounded sampling and cached retained-pack estimates are
  implemented and verified by focused tests. Candidate selection is linear.
  Real large-data liveness and weighted placement remain separately unaccepted.
- [ ] **Balance observation liveness**: inspecting a large partition must not
  delay heartbeat publication or serving-grant renewal past their deadlines.
  Bound observation work and verify the behavior with real large data.

- [x] **S3 bucket discovery during range transitions**: a successfully created
  bucket must remain discoverable while Chunk-KV splits or changes owners. A
  native browser run received HTTP 503 from bucket listing immediately after
  a successful bucket creation. Preserve the underlying metadata/routing error
  in service diagnostics and verify transition-time listing without browser
  retries before closing this issue.
  Confirmed `NotMyRange` after discovery budget exhaustion. Bounded serving
  attempts now continue within the unchanged deadline; two regression cases
  verify convergence and persistent-rejection bounds. Native discovery and
  multipart browser acceptance pass without browser retries.

- [ ] **S3 native full-read latency**: the real multipart browser flow once
  exceeded its existing 3-second request deadline reading a 9-MiB object after
  Chunk navigation and return. Other executions completed the same read. Locate
  the first slow backend boundary and retain phase timings; do not increase
  the browser timeout or conceal the failure with retries.

- [x] **Iceberg native metadata latency**: deleting tables in an owned
  eight-table namespace twice exceeded the unchanged 3-second API deadline.
  Creation, nested schema inspection and navigation completed, but cleanup did
  not. A later table GET took 2.99 seconds and missed the Schema control's
  existing deadline. Locate the slow backend boundary and verify reads/deletion without
  browser retries or extended deadlines before accepting this flow.
  Confirmed catalog/grant propagation delay; watch-triggered synchronization
  resolves the observed failure. The same four-case native selection passes;
  Iceberg metadata/schema/navigation/cleanup takes 4.7 seconds in total.

- [ ] **Chunk-stream idle rollover authority**: a native three-node run logged
  repeated liveness renewal against a sealed chunk, followed by a manifest
  mismatch and stalled stream. Verify that superseded split/transfer writers
  stop renewing and cannot publish a new manifest after authority changes.

## Native feature inventory (2026-10-04)

- Collection: the routine configuration collects 85 cases in 22 files; the
  managed deployment configuration collects one separate case, and native
  diagnostics collect four separate cases. The multipart-only configuration
  selects the same multipart case, so it adds no distinct case. Collection is
  not a passing result. The routine set still includes interception-dependent
  cases, which the shared fixture now rejects.
- Shell, modes and shared controls (`00`, `01`): embedding, readonly, module
  opt-out, container capabilities, outage recovery, dialog defaults and shared
  trees. **Unaccepted** where responses are intercepted.
- Cluster (`10`–`13`): physical CRUD, service lifecycle, six-service deployment,
  partial failure and cross-links. Real KV/DDB lifecycle has existing native
  evidence; auxiliary deployment scenarios still have interception gaps. The
  separate normal three-node API fixture proves all 18 service deployments.
- KV management/data (`20`–`22`, `30`, `31`): real Store/Group membership, quorum,
  CRUD and byte-preserving pagination. The retained KV presentation/return
  selection has real passing evidence. All three advanced cases pass; exact
  replacement-page boundaries and Previous restoration are now asserted.
- Shared graphs/activity (`40`, `41`): real physical graph expand/collapse,
  pan/fit and selection restoration; existing focused passing evidence. Verify
  activity failures and resource deletion in the final ordered acceptance.
- Capacity (`50`–`53`): real disk/owner lifecycle plus native bit-by-bit canvas
  verification. Large inventory, unavailable usage and several window/return
  cases still use interception and lack real equivalents.
- Chunk (`54`): paged lists, types, exact IDs, multi-Strip Mirror/EC layout and
  placement return. S3 location navigation verifies actual Strip/block counts
  and selected block Node/Disk/DiskGroup/Zone/offset against native API data;
  bounded multi-page, mixed placement and independent list navigation remain
  uncovered by native browser acceptance.
- Chunk-KV (`55`): native catalog graph survives three returns, three refreshes
  and resize. Paged split counts, journal extent fences/continuations, ownership
  changes and failed/empty catalog cases still need real equivalents. KV Page
  inspection remains a separate missing API contract.
- Iceberg (`60`): a new native case creates eight actual catalog tables and
  verifies nested schema, scoped Actions, tree navigation and return. The flow
  passes after resolving catalog/grant propagation latency. Snapshot/manifest/file and footer scenarios still intercept
  responses and lack native browser/parser acceptance.
- S3 (`70`, `71`): actual multipart upload, HEAD, bounded preview, full read,
  authoritative locations and Chunk-return navigation have passing native
  executions. Transition-time bucket listing now passes; one full-read deadline
  remains unresolved. Large listing/cursors, stale revisions and error feedback
  still need real browser equivalents.
- Cross-domain (`72`, `90`): real management smoke exists. The managed native
  case passes against an isolated actual monitor and seven service processes;
  physical properties are compared to actual Chunk placements. Docker image
  packaging remains a separate unaccepted boundary.

### Remaining acceptance tasks from the inventory

- [ ] Verify shell embedding and capability failures against actual standalone
  and container services, without intercepted responses.
- [ ] Verify all auxiliary service menus, deployment progress, failures and
  resume/restart behavior with real service processes.
- [x] Bring advanced KV acceptance into agreement with replacement pagination
  and current scoped Actions; retain raw-byte identity and mutation coverage.
  Three real cases pass (0.847/4.9/2.8 s), preserving bulk/selected/inline delete,
  all-group restrictions, auto-scan and session-owned demo cleanup.
- [ ] Cover large real Capacity windows and unavailable allocation/owner data,
  including restoration after navigation and late responses.
- [ ] Cover actual Chunk type/list pages, multi-Strip Mirror/EC and physical
  placement return; independent windows must replace previous results.
- [ ] Cover real Chunk-KV journal paging, transition/stale fences, empty catalog
  and service outages while keeping the graph and selection accurate.
- [ ] Cover actual Iceberg nested schemas, multiple snapshots/manifests, file
  pagination and Parquet/Avro metadata inspection without payload scans.
- [ ] Cover real bounded S3 bucket/object windows, continuation revisions and
  unavailable locations; retain exact multipart interval and return assertions.
- [x] Verify the managed container cross-domain case against the current UI
  contracts and actual placement; remove stale control/placement assumptions.
  Native monitor-owned profile passes in 7.7 seconds, retaining hardware
  rejection and KV/Iceberg/S3 operations. No browser interception is used;
  Docker image packaging is not established by this host-native run.

## Verification layers

- Unit: conflict/default selection, plan reconciliation, service labels.
- Integration: automatic binding/owner routing, bounded failures, native
  service startup and fixture write/read round trips.
- E2E: default create/retry/resume flow, agreed three-domain contracts, large
  Capacity views and failure feedback. Run through `pixi run`; use the system
  browser and an isolated test runtime.

## Verified completion pass (2026-10-04)

- The permanent specification now defines all seven domains, the shared tree,
  center Actions, properties and rendering/request bounds. Future metrics and
  authentication redesign remain separate work.
- Current browser suite: 83 tests passed in 3.4 minutes; 101 unit tests passed
  in 2.29 seconds. Timing instrumentation remains on setup, mutation, readiness,
  DOM and teardown. Page-only behavior and native acceptance have separate
  configurations.
- KV uses 20-row replacement pages and original hex bytes for binary cursor
  and row deletion. Six focused tests passed; printable Unicode remains text.
- Chunk ownership uses real validated service/storage maps, lazy owner-specific
  32-slot pages, generation pinning and separate CDB/Group branches. Four UI
  tests and the real KV-backed ownership integration passed. Disjoint slots
  remain explicit. No fabricated range/Replica children are shown.
- Lifecycle failures identify the actual service and include the causal child
  output. Three retained-launch tests passed. Managed Chunk-KV/Access launch
  specifications now direct logs into their own workspace.
- Fresh CDB launch initializes a fixed service/storage plan before starting;
  existing maps are preserved and out-of-plan owners require explicit
  migration. A real CDB deployment test passed. The whole three-node
  six-service bring-up remains unaccepted until the outstanding items above
  are fixed and tested together.

## Prior fixture (2026-10-03)

- Chunk-KV sidebar uses the shared datacenter/Rack/Node/Server/Split tree.
  Registered servers remain visible without catalog entries. Split rows and
  center topology nodes use short IDs; full bounds and IDs remain in hover and
  right-side properties. Owner runtime and selected journal extent properties
  are on the right. Tree counters, journal tracks and bounded extent paging
  remain available; missing KV Page inspection is explicit.
- Chunk-KV center now uses a zoomable, pannable node-link canvas:
  Chunk-KV → CKV server → Split → real Tree ID. Servers expand/collapse;
  canvas windows contain at most eight servers and five splits per server.
  Tree selection opens checkpoint/counters below. Removed the loaded-ID
  filter and All loaded servers button; sidebar navigation does not filter
  the canvas. Backend catalog continuation remains independent of graph
  display pagination.


- Chunk sidebar now uses the same shared Tree component, icons, indentation
  and datacenter/R-/N-/S-/G- labels as Cluster, with no separate placement
  heading. Stores and groups remain lazy and replicas are omitted.
- Placement repair now requires a node/disk protection deficit, not missing
  rack diversity. Allocation, conversion, relocation and repair completion
  share this rule. Three-node EC 8+4 regression and three relocation tests
  passed; live ChunkDB instances restarted with the corrected binary.


- Added `ui_tpch_sf1`: eight TPC-H SF=1 tables, 8,661,245 rows, eight Parquet
  files; loader verified all snapshot inventories. Report:
  `.crowdb-runtime/persistent/console/default/ui-tpch-sf1-report.json`.
  Real Iceberg Chunks include 2, 6, 8 and 26 EC strips. The 26-strip sample
  above has 208 MiB allocated capacity and EC 8+4 layout.
- Chunk UI now uses 10-record windows without a nested vertical list scroll,
  central exact-ID lookup, lazy Rack/Node/CDB/KV/Store/Group hierarchy without
  replicas, explicit pending range ownership, and `Mirror N` block labels.
  Session activity is hidden in this page. Four affected browser tests pass;
  production build and TypeScript checks passed. Disk/Node return navigation
  remains open above.
- One Rack, Nodes 1–3, one of each of the six service types per Node, normal
  multi-node mode. Store 0 / Groups 0 and 1 each have three replicas; Store 1 /
  Group 1 supplies Chunk-KV metadata. Three file-backed disks, one per Node.
- Iceberg `http://127.0.0.1:9092`, catalog `UI Demo`, namespace `ui_demo`:
  TPC-H SF 0.001, eight tables, 8,695 rows, eight Parquet files, one snapshot
  per table. Loader completed and verified committed file inventories.
- S3 `http://127.0.0.1:9091`, bucket `ui-iceberg-demo`: 32 objects under
  `ui_demo/<table>/{metadata,manifest-list,manifest,data}/`, 444,588 bytes.
  Every copied object was read back and compared byte-for-byte.
- Console proxy namespace/table listing and S3 bucket listing returned 200.
  Bounded Chunk listing returns real two-copy Mirror strips on distinct Nodes.
- Reports live in `.crowdb-runtime/persistent/console/default/` as
  `ui-demo-tpch-report.json` and `ui-demo-s3-report.json`. Credentials remain
  private in the existing runtime secrets directory.
- Manual binding/owner/catalog recovery above enabled this fixture; it does
  not prove that initial UI provisioning is fixed or approve deferred designs.

- Navigation completion evidence: shared Back/Forward, 32-visit limit,
  Cluster retained collapse, Chunk placement return, S3 bucket/object return and
  Iceberg file-footer return are implemented and pass focused browser tests.
  Keep the global navigation item open until KV, Capacity, Chunk-KV and
  S3 location selection/cursors plus stale/in-flight restoration are verified.

- S3 storage-location completion: management-only Access metadata inspection,
  private Console proxy, exact decimal offsets, bounded reference/response,
  revision-bound cursors and object/extent return are implemented. Two decoder
  tests, real HTTP authorization/metadata-only test, proxy security/budget test,
  eight hook cases and five S3 browser cases pass. The new browser location
  round trip takes 0.883 seconds; the four existing cases remain 0.348–1.0 seconds.
  Normal three-node native fixture acceptance remains part of provisioning below.

- Capacity completion: shared lazy Node/DiskGroup inventory replaces duplicate
  eager topology walks; four workers and per-branch deduplication bound fanout.
  Cluster does not poll DiskDB usage; selected Disk/DG requests are scoped. A
  200-Node browser fixture verifies no unopened storage branch requests and only
  the selected node/group after expansion (1.4s). Real Capacity domain cases
  passed; the retained owner-capacity wait remains 9.31s, separately measured.
- Context menu race fixed: old asynchronous action completion no longer closes a
  newer resource menu. The no-retry maintenance browser case passes in 4.0s and
  a focused unit regression verifies the close ordering.

- KV/Capacity return completion: KV page and raw Key focus restore after both
  domain changes and another Store selection. Capacity Zone, zone window and
  bitmap block window restore after domain and parent navigation; sidebar
  expansion also survives. The affected 11 browser cases pass, with KV return
  1.2s and Zone return 1.5s. Global navigation remains open for Chunk-KV query,
  journal extent selection and catalog movement acceptance.

- Native provisioning completion: one Rack/three Nodes, Groups 0/1 and all 18
  normal-mode services provision through Console APIs without binding, owner,
  slot-map or catalog repairs. Initial cold chain passed; native timing stays
  outside the routine browser suite. Repeated defaults produced collision-free
  IDs/listeners. CDB slot initialization, first Access catalog provisioning,
  labels, Cluster graph collapse and Chunk placement return items are verified.
- Disposable ownership completion: every local launch/restart records a separate
  PID/start identity before readiness. Concurrent Nodes do not rewrite a shared
  child list. SIGTERM and forced SIGKILL/cleanup tests pass in 0.26s; persistent
  sentinel data is preserved. Protocol regression excludes persistent namespaces.

## Current verification (2026-10-04)

- The complete browser run has 81 passing / 4 failing cases in 3.8 minutes;
  all 117 frontend unit cases pass in 2.48 seconds. Three failures are reset
  request deadlines; the fourth is the Group status read after stopping its
  leader. These are not accepted or suppressed. Final focused checks follow
  the latest reset optimization; preserve the remaining failures if any.
- Eight-second DiskGroup unknown-outcome regression passes. The ID remains
  fenced while accepted provisioning continues. Missing DDB registration now
  identifies the incomplete owner step; exact-ID creation reconciles it.
- Chunk-KV startup failure exits nonzero and retains causal output; startup
  exit regression passes. Storage observation fixtures have current Wal purpose.
- Current live catalog generation 172 has 12 splits, distributed 4/4/4 across
  CKV-1/2/3 (`next=null`); the old 4/2/0 report no longer reproduces. No claim
  of load equality is inferred from these counts.
- Three-node native provisioning now passes: one Rack, Group 0/1 and all 18
  normal services. The readiness delay was descriptor discovery at 30 seconds;
  the normal KV default is now 1 second. Driver tick/lease policy and the
  20-second response deadline are unchanged. Initial chain: 13.09 seconds.
- Native S3 multipart/location acceptance passes in the same fixture (14.24 s
  initial extended run): exact logical coverage, shared Chunk resolution,
  payload readback and stale cursor rejection. The live Access instances were
  outdated and have been rebuilt/restarted; Parquet and Avro inspections return
  200. Browser location navigation remains an unchecked acceptance task.

- Latest affected selection after reset/cadence fixes: 13 browser cases pass
  in 1.5 minutes (specs 13/20/21/50). The owner/usage case is 1.7 seconds versus
  10.6 seconds before cadence serialization. Twenty DDB config tests and seven
  lifecycle tests pass. Full ordered-suite stability and client cancellation
  during reset remain unaccepted; no second full-suite pass is claimed.

## Latest targeted bug verification (2026-10-04)

- Chunk-KV empty graph reproduced on native catalog refresh. Rebuilding the
  ReactFlow graph on catalog/layout keys could clear its node store. Retain
  the graph across catalog updates, initialize it while visible, and refit on
  measured container resize. Existing root/server/split/tree hierarchy is unchanged.
- Zone 0 on the live first disk is 7292/32768 blocks occupied (22.25%). Its
  4096-block windows contain 4096, 3196, 0, 0, 0, 0, 0, 0 occupied blocks.
  Thus the all-blue first window is accurate, not an endian mismatch. Added
  explicit window used/free/unknown counts without changing the allocation bits.
- Two native browser regressions pass (latest Zone 0.948 s, graph 1.7 s). They run
  against the owned ephemeral one-Rack/three-Node fixture, alongside multipart
  data writes; every bitmap pixel is compared to real API allocation bits.
  Provisioning + S3 + browser + teardown: 19.14 s including saved screenshots.
  The preceding passing run took 17.72 s. No mocked response or retry.
- Native diagnostics use their dedicated config; they are excluded from the
  routine page suite, which lacks this six-service fixture. Existing mocked
  cases remain unconverted and do not establish acceptance.
- Five existing Chunk-KV monitor integration cases pass (0.07 s). They cover
  count/size split planning, transfer/grant publication and failover, but do not
  establish the missing restart-weight, sampling-eligibility or large-data
  heartbeat contracts recorded above. Node ownership UI design is deferred.

## Low-cost backend verification (2026-10-04)

- Tree split hints read resident structural boundaries without page fetches.
  Bounded-range and resident-index regressions pass. Split observations use
  live witnesses or one window of at most 64 records; the requested byte budget
  is 64 KiB. A single oversized record may exceed that soft byte budget.
- One background observation job per service samples every Serving partition;
  cached samples are fenced by ownership epoch. A 1,000-record regression
  verifies the 64-record ceiling and that an unsplittable partition does not
  prevent sampling another partition. Observation publication stays synchronous.
- Retained-pack estimates survive reopening and reclamation, and querying the
  cached estimate does not access a hidden catalog. Estimates deliberately lag
  uncheckpointed data and can overcount packs shared by split children. Weighted
  placement and real large-data lease liveness remain acceptance tasks.
- All 59 Chunk-KV Server tests pass; Tree FFI has 51 passing tests, including
  the two new split-hint cases. C++ Tree has 611 passing cases. Rust fmt and
  affected clippy gates pass. Native browser verification continues separately.
- Graph follow-up: the remaining blank canvas retained its six graph nodes but
  discarded their measured dimensions on controlled-node updates. Preserving
  dimensions and accepting ReactFlow dimension changes restores visibility.
  Three tab-return cycles, three refresh actions and resize pass in 2.9 seconds
  on the normal three-node cluster; Zone bit verification passes in 0.872 s.
- Final targeted native run: all three browser cases pass in 9.2 seconds:
  Zone 0.927 s, graph 3.2 s and multipart/location 3.8 s. The owned one-Rack,
  three-Node provisioning/data/browser/teardown chain takes 23.89 s. This is
  targeted acceptance, not a passing full ordered browser suite.
