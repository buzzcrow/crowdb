<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI Follow-up Plan

Persistent issue list requested by the user; keep this filename and remove
completed items after verified fixes. Record newly discovered UI and supporting
API problems here, including workarounds that do not constitute fixes.

Design: [Console UI specification](../design/console/design-crowdb-console-ui.md).
Execution history: [UI implementation plan](plan-console-complete-ui.md).

Current handoff: [Remaining work contract](#remaining-work-contract-2026-10-04).
Read it before selecting historical unchecked tasks below.

Goal: make the normal one-rack, three-node flow work without manual repairs.


## Open issues: current UI requirements (2026-10-04)

These entries describe requirements, not implementation plans. Node ownership bitmap design is approved for implementation. The user subsequently authorized
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
- [x] **Chunk ownership bitmap**: approved owner-colored 1024-slot maps for
  independent Serving and Storage ownership. Group/CDB selections show their
  slots; Node/Rack scopes combine owners, gray outside scope and pattern unknown
  membership. Replicated storage groups are deduplicated. Native browser cells
  match both durable maps; integration checks all 2048 exact owner entries.
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

- [x] **No mocked E2E acceptance**: all browser acceptance uses real services,
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
- [x] **Interrupted operations and recovery**: verify partial deployment,
  retry, reload, restart, concurrent operators, prerequisite arrival and reset
  cancellation. No duplicate service, orphan process or inconsistent resource
  state may remain.
- [x] **Creation defaults and mode boundaries**: every create dialog supplies
  valid conflict-free IDs, ports and dependencies on repeated use. Container
  mode rejects topology/disk management in both UI and API while permitting
  supported data operations. Verify readonly embedding and outage recovery.
  Actual standalone and host-native managed-profile acceptance passes. Docker
  image packaging remains a separate unverified boundary under task 7.
- [x] **Navigation and changing resources — shared flow**: browser and header
  Back/Forward restore bounded selection/query windows, tree expansion and scroll.
  Native acceptance covers KV replacement pages, Capacity zone pages, Iceberg
  schema, owner bitmap selection and S3 → Chunk → Disk return. S3 real overwrite,
  deletion and all-Access outage/restart preserve scope and reject stale location
  links. Unit tests cover reordered responses and changed owner projections;
  slot API integration rejects mismatched generations. A real Group-0 catalog
  owner-change test rejects old catalog/runtime cursors; the runtime hook ignores
  late responses from the previous owner. Fixed Chunk maps have no
  online reassignment operation. Advanced Chunk-KV transfer/journal and Iceberg
  reference parser scenarios remain in their domain acceptance inventory below.
- [x] **Real data-browser completeness**: verify paged Chunk lists and multi-
  Strip layouts, Chunk-KV splits/journals, Iceberg schemas/snapshots/manifests/
  files/Parquet metadata, and S3 buckets/objects/previews/multipart/locations.
  Large collections stay bounded; independent pages replace rather than
  accumulate data. Metadata inspection does not unnecessarily read payloads.
- [x] **Real Capacity and failure states**: verify disk/owner management,
  large inventories, zone paging and actual allocation bitmaps. Unavailable,
  partial, recovering and Unknown observations remain accurate and usable.
- [ ] **Final native acceptance and speed**: complete the ordered real E2E
  suite and required native scenarios. Preserve measurements for slow setup,
  mutations, readiness, UI refresh and teardown. Avoid unnecessary repeated
  provisioning/loading; do not hide failures with retries or longer timeouts.
  R203 remains open until the required acceptance succeeds.


## Inspector simplification

- [x] **Clean up Metrics UI**: remove Metrics entries, panels, polling and unused
  UI metric types. No Metrics page redesign is planned. Future metrics publish
  may feed customer-managed time-series databases; publishing is outside this
  implementation scope. Unused frontend metric types were removed; native
  ownership browsing asserts no metric requests or Metrics entry.

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
- [x] **Native executable staging fork window**: executable copies now run in
  an isolated child, keeping writable executable handles out of the shared
  multithreaded test parent. A controlled Linux fork reproduces `ETXTBSY`
  after the parent closes a CLOEXEC writer, and succeeds after the inheriting
  child exits. Default-concurrency and serial lifecycle suites pass with exact
  PID/argument checks and upgrade restart retained; no spawn retries. The
  original intermittent failure lacked a descriptor snapshot, so attribution
  to this reproduced mechanism remains an inference.
- [x] **Six-service plan recovery**: durable revision-fenced progress survives
  reload. Native interrupted-step reconciliation retains all 18 service PIDs
  and sends no duplicate deployment requests. A partial native plan waits while
  registered DiskIO services are stopped, retains that wait across reload, and
  automatically deploys only missing Chunk-KV/Access after dependency recovery.
  Two live mirrors alone cannot establish the complete DiskIO route generation;
  the queue now waits for stopped registered routes. S3 signing/listing succeeds
  after automatic deployment. Browser cases take 1.3 s and 7.6 s. Ordinary
  three-node six-service restart/data acceptance remains separately verified.
- [x] **Native six-service restart and retained data**: the ordinary three-node
  fixture restarts KV, DDB, CDB, DiskIO, Chunk-KV and Access on Node 1. Each old
  PID exits, each new PID is alive, all 18 exact service identities remain
  unique, and pre-restart S3 content reads back. The latest post-restart native
  browser run passes all four domain cases in a 34.49-second owned fixture.
  Queue partial-plan/prerequisite-arrival acceptance is recorded separately above.
- [x] **Allocation geometry closure**: actual 128-KiB/1-MiB topology returns
  early 409 for ChunkDB and Chunk-KV, with both unit sizes and the uniform-unit
  invariant. No rejected service is spawned. Native fixture passes in 5.64 s;
  mixed-unit support remains outside scope.
- [x] **Create defaults acceptance**: conflict-free defaults are implemented;
  complete repeated-create and reopen acceptance across IDs, host listener
  ports, DiskDB port ranges and dependency references.
- [x] **Group 0 convergence retry volume**: one successful native initialization
  recorded 4,878 leader hints and seven unknown-leader waits in 3.75 seconds.
  Review repeated redirects during election and bound their request rate;
  keep the ordinary cold-start path and existing deadlines. Successful
  browser acceptance does not establish an acceptable background work budget.
  Repeated/cyclic hints now use the existing election wait and retry budget.
  All 68 client cases and real four-case browser acceptance pass; the clean
  cold fixture records nine hints and completes in 33.15 seconds.
## Capacity and shared behavior

- [x] **Navigation return history across all views**: every button/link that
  navigates to another resource or view must retain a return route. Back and
  Forward restore the source domain, resource, list cursor, tree expansion,
  selected detail and scroll position. Keep ancestry breadcrumbs separate
  from visit history; refresh and mutations must not create navigation entries.
  Include property links and graphical nodes, not only sidebar navigation.

- [x] **Cross-view and failure acceptance**: verify scope restoration,
  disk/node navigation, owner movement, stale cursors, unavailable services,
  and container-mode topology/disk mutation restrictions against the current
  Cluster, Capacity and KV specification.

## Data environment and deferred design

- [x] **Chunk-KV KV Page inspection API and explorer**: actual root/child/base
  frames, 20-entry replacement windows and 256-byte key/value previews are
  implemented. Hex is default; UTF-8 is validated. Catalog/owner, tree version
  and page fingerprint fences reject stale continuation. This physical base
  view does not flush pending writes or fetch overflow values. FFI, owner HTTP
  and native browser checks cover the contract; journal extents remain separate.


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
  The native 32-MiB incompressible workload preserves exact aggregate retained
  estimates across owner restart (46,244,412 bytes and 118,165,608 bytes in
  separate observed layouts), with exact sample data readback. Reclamation and
  reopen estimates now pass the native retained-tree-pack regression. Actual
  weighted redistribution remains unaccepted.
- [x] **Low-cost approximate split and weights**: prioritize efficiency,
  simplicity and low system overhead. Split boundaries need not divide keys or
  bytes exactly in half; estimated weights need not be exact. Obtain a valid
  boundary with nonempty children using bounded work instead of iterating the
  entire partition for a median. Load observation must likewise avoid full
  scans and unnecessary background work.
  Native index hints, bounded sampling and cached retained-pack estimates are
  implemented and verified by focused tests. Candidate selection is linear.
  Real large-data liveness is verified below; weighted placement remains open.
- [x] **Balance observation liveness**: inspecting a large partition must not
  delay heartbeat publication or serving-grant renewal past their deadlines.
  Bound observation work and verify the behavior with real large data.
  Native 32-MiB writes and bounded sampling preserve all three heartbeats below
  the normal six-second suspect deadline, including gaps between observed
  heartbeat timestamps. Reads/writes span the normal 12-second serving lease.
  No cadence change, extra client retry or single-node mode. The latest owned
  workload/restart/data fixture passes in 44.02 s.
- [x] **Committed split recovery activation**: startup recovered split overlays
  but looked up only transfer commit evidence, leaving both halves Prepared.
  Load and validate the exact committed split against the current catalog before
  activation. Native large-data owner restart/readback passes; component cases
  reject uncommitted, mismatched transition/owner/artifact and stale epoch proofs,
  cover both halves and retain idempotent grant refresh.

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

Allocation and payload validity are distinct: a confirmed allocated DiskDB
block is used even before payload writes. EC validity uses exact seal bytes;
unused data/code tails can contain old bytes. SSD O_DIRECT alignment must not
pad logical data or increase the actual seal boundary. The 8+4 EC UT matrix
covers 24 size/unit combinations, stale tails, one to four missing shards and
loss beyond recovery capacity.

- [x] **Iceberg native metadata latency**: deleting tables in an owned
  eight-table namespace twice exceeded the unchanged 3-second API deadline.
  Creation, nested schema inspection and navigation completed, but cleanup did
  not. A later table GET took 2.99 seconds and missed the Schema control's
  existing deadline. Locate the slow backend boundary and verify reads/deletion without
  browser retries or extended deadlines before accepting this flow.
  Confirmed catalog/grant propagation delay; watch-triggered synchronization
  resolves the observed failure. The same four-case native selection passes;
  Iceberg metadata/schema/navigation/cleanup takes 4.7 seconds in total.

- [x] **Chunk-stream idle rollover authority**: a native three-node run logged
  repeated liveness renewal against a sealed chunk, followed by a manifest
  mismatch and stalled stream. Verify that superseded split/transfer writers
  stop renewing and cannot publish a new manifest after authority changes.
  Idle maintenance now validates the authoritative epoch/generation/active
  chunk before renewal and rollover, and stops stalled workers. Three idle
  regressions verify higher-epoch takeover, same-epoch reopen and current-owner
  sealed-chunk rollover. Forty-one component cases and four actual-process
  cases pass; native takeover is 1.31 s and the restart/fault suite is 6.47 s.
  Affected all-target clippy and Rust fmt pass.

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
  changes and failed/empty catalog cases still need real equivalents. Actual
  KV Page inspection now has native API/browser and FFI acceptance.
- Iceberg (`60`): a new native case creates eight actual catalog tables and
  verifies nested schema, scoped Actions, tree navigation and return. The flow
  passes after resolving catalog/grant propagation latency. A second native
  case now covers two committed snapshots, Avro manifests, 101 actual files,
  Parquet 20/2 Row Group windows, footer/column return and stale table heads.
  Historical intercepted cases are not counted as native acceptance.
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

- [x] Verify shell embedding and capability failures against actual standalone
  and container services, without intercepted responses.
  Actual managed-profile services cover container-mode shell/capability/data
  behavior on the host; this does not establish Docker image acceptance.
- [x] Verify all auxiliary service menus, deployment progress, failures and
  resume/restart behavior with real service processes.
- [x] Bring advanced KV acceptance into agreement with replacement pagination
  and current scoped Actions; retain raw-byte identity and mutation coverage.
  Three real cases pass (0.847/4.9/2.8 s), preserving bulk/selected/inline delete,
  all-group restrictions, auto-scan and session-owned demo cleanup.
- [x] Verify real Capacity replacement windows: an actual 80-GiB sparse disk
  has 80 zones and 8192 blocks per zone. The browser verifies 32/32/16 zone
  pages, exclusion of earlier rows, final Next disabling and selected Zone 0
  restoration. Both displayed 4096-bit windows match actual allocation pixels;
  the case takes 1.3 seconds without changing timeouts or injecting responses.
- [x] Cover large real Capacity windows and unavailable allocation/owner data,
  including restoration after navigation and late responses.
- [x] Cover actual Chunk type/list pages, multi-Strip Mirror/EC and physical
  placement return; independent windows must replace previous results.
- [x] Cover real Chunk-KV journal paging, transition/stale fences, empty catalog
  and service outages while keeping the graph and selection accurate.
- [x] Cover actual Iceberg nested schemas, multiple snapshots/manifests, file
  pagination and Parquet/Avro metadata inspection without payload scans.
- [x] Cover real bounded S3 bucket/object windows, continuation revisions and
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

## Continued backend verification (2026-10-04)

- Existing Stream observation fixture includes `purpose`; its exact test passes
  in 0.02 seconds. Real background mirror-to-EC conversion and four-shard repair
  pass in 2.88 seconds with actual KV/DDB and six DiskIO processes, exact seals
  and stale-tail checks.
- Baseline post-restart native browsers pass all four cases in a 32.48-second
  fixture. The leader-hint candidate drops 4,105 hints to 9, but subsequent cold
  native runs fail live KV registration, metadata readiness or multipart upload.
  The candidate is unaccepted and uncommitted; five-run diagnosis and exact
  failures are recorded under the execution plan's `Blocked` section.
- Chunk ownership bitmap implementation is approved; Metrics is a UI cleanup task. R203 and the
  real-browser coverage, geometry, recovery and balance tasks remain open.

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
  heartbeat contracts recorded above. Chunk ownership bitmap design is now approved.

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

## Remaining work contract (2026-10-04)

This section is the current handoff scope. Older unchecked inventories are
historical evidence, not permission to repeat completed work or redesign it.
Items 1–2 are complete; the user has authorized the current model to finish
items 3–8. Fix defects necessary for each item, without expanding into
unrelated product features. Record exact verification and remaining gaps.

1. **Chunk-KV Page explorer**: add bounded real root/child/leaf inspection,
   exact page/tree identity and key bytes. Hex is default; text must validate
   UTF-8. Keep Tree and Journal distinct. Bind inspection to the selected
   owner/catalog and tree observation; reject stale continuations. Retain the
   existing graph, scoped tabs and navigation. Do not fabricate pages from
   journal extents or implement a new storage engine.
2. **Iceberg file inspector**: finish the existing reference explorer and
   metadata-only inspection, including snapshot/manifest/file windows and
   Parquet footer, row groups and columns. Follow the existing domain design;
   improve structured presentation and exact reference navigation. Never scan
   data pages to obtain inspection metadata or load all references for counts.
   Test committed files through real Access routes, including stale references.
3. **Large windows and unavailable data**: complete the remaining Capacity,
   Chunk, Chunk-KV and S3 native cases. Replacement windows must exclude prior
   rows; keep prescribed sizes (zones 32, Chunk 10, S3 20). Verify actual
   multi-Strip Mirror/EC placement, journal transitions and stale cursors.
   Unknown allocation/owner data is unknown, never empty or zero. Preserve
   scope on failure, disable actions using stale data, discard late responses,
   and recover by explicit refresh. Preserve verified history/scroll behavior.
   No new dashboard, polling loop, fetch-all counts or automatic mutation.
4. **Creation and lifecycle acceptance**: repeated create/reopen must choose
   unused IDs and listener/DiskDB port ranges and valid dependencies. Verify
   auxiliary service menus, progress, partial failure, resume, stop and restart
   using actual processes. Show authoritative completion or unknown outcome;
   never report success on request acceptance alone. Resume must not duplicate
   live services or discard successful steps. Preserve container capability
   rejection and reset cancellation. No deployment workflow redesign.
5. **Retained-byte balancing**: verify estimates after reclamation and actual
   weighted redistribution with unequal retained data. Separate count balance
   from byte balance; accept approximate shared-pack overcount as documented.
   Preserve bounded sampling, heartbeats, serving leases and recovery data.
   No exact full-tree scans, new locks, changed lease/deadline budgets or new
   placement policy without a specific demonstrated need and user decision.
6. **Allocation geometry closure**: confirm the existing early 409 rejection
   of incompatible allocation units, useful causal error and no partial
   bootstrap. Update the stale task checkbox with evidence. Mixed-unit support
   is not authorized by this cleanup; do not silently implement it or weaken
   the allocator invariant.
7. **Mode and packaging acceptance**: test actual standalone/container shell,
   embedding and capability failures. Backend rejects forbidden physical writes;
   allowed data operations remain usable. Validate the Docker image separately
   from host-native managed tests. An unavailable registry/build prerequisite
   is an explicit unverified boundary, not a reason to substitute a mock or
   redesign packaging/authentication.
8. **Final integration and documentation**: replace remaining mocked acceptance
   with owned real fixtures, keep mutation cleanup in teardown even on failure,
   and run the ordered suite and applicable gates. Preserve persistent user
   deployments. Record collected/passed/failed/skipped counts and timings;
   targeted success does not establish full-suite success. Reconcile obsolete
   checkboxes and close R203 only after all remaining acceptance is established.

Completed constraints remain binding: Chunk ownership uses two independent
1024-slot maps (Storage Group and Serving ChunkDB), not Chunk-KV partitions.
Metrics UI cleanup is complete; no Metrics page or metrics publishing is in
this scope. Do not reopen the parked test-orphan cleanup redesign or add auth,
raw Chunk layout editing, storage reclamation promises, or unrelated features.

## Approved ownership and navigation completion (2026-10-04)

- Chunk Serving/Storage bitmaps and Metrics UI cleanup are complete; Metrics
  redesign is cancelled. Metrics publish is only a future external integration
  direction and was not implemented here.
- Native diagnostics: 7 passed, 0 failed, 1 deliberate fixture-phase skip. Separate
  real KV return and Inspector cleanup also pass. Frontend unit checks: 27 passed;
  slot/catalog API integration: 3 passed. Rust fmt, scoped clippy, production UI
  build and TypeScript checks pass.
- Browser history, query/selection/scroll restoration, stale locations, deletion,
  service outage/recovery and old-owner response isolation are verified at their
  stated layers. This is not completion of the entire native feature inventory.
- User-requested stopping point reached; remaining requirement work stays open.

## Page and Iceberg inspection completion (2026-10-04)

- Tasks 1–2 are complete: actual bounded KV base-page inspection and native
  Iceberg reference/file inspection, with exact identities and stale fences.
  The existing shared layout is retained; no new domain redesign is authorized.
- Final native browser selection: 3 passed, 0 failed/skipped (20.7 s); owned
  deployment/teardown chain: 38.74 s. It covers actual Parquet 20/2 Row Groups,
  101-file manifest pagination, footer/column return and stale table heads.
  Backend checks: 3 Page FFI, 3 owner HTTP, 9 Parquet metadata and 2 Access
  inspection tests passed. Five focused frontend unit tests and applicable
  format/lint/build gates passed. See the execution plan for commands/evidence.
- This is targeted completion, not full-suite or restart acceptance. The plan
  records prior restart/process-loss and transition-latency failures for tasks
  4–5/8. Tasks 3–8 remain for the user's next model; stop after this handoff.


## Resumed tasks 3–8: current acceptance (2026-10-04)

- Browser flow files now use actual routes/services; all response/HAR
  interception has been removed. Component inputs remain unit coverage.
- Real Capacity covers native 32/32/16 zones, exact bitmap pixels, lazy branches,
  scan/recalc completion, seven persisted hardware statuses and all five scope
  levels during DiskDB outage/recovery. Current hardware membership/status and
  physical geometry take precedence over older usage reports; missing usage is
  Unknown. Chunk covers 10/10/1 windows and actual Mirror/EC multiple Strips.
- Real S3 covers 20/1 buckets, bounded object windows, 100/1 multipart parts,
  actual revision conflicts, abort/deletion and Access interruption, and
  S3 → Chunk → Disk history. Page/Iceberg completion above remains accepted.
- Lifecycle/default acceptance covers repeated real auxiliary dialog reopen,
  native menu/PID stop/restart, partial plan progress/resume without duplicates,
  actual prerequisite arrival and reset cancellation. Backend lifecycle,
  startup and capability checks supplement browser evidence. Task 4 is complete.
- Task 6 remains complete: incompatible 128-KiB/1-MiB deployment returns causal
  409 before any incompatible service is spawned. No mixed-unit support added.
- Full uninstrumented native browser chain: 19 collected, 18 passed, 0 failed,
  1 prerequisite-phase skip, browser 1.2 minutes; owned chain 91.60 s and teardown
  7660 ms. Separate prerequisite phase passes in 7.8 s (chain 17.11 s).
  Final post-fix full six-service restart/browser chain: 21 collected, 18 passed,
  zero failed, three phase skips covered by separate prerequisite/overlay/large
  Journal runs; browser 1.2 minutes, owned chain 94.42 s, teardown 8464 ms.
- Latest frontend gate: 138 passed in 26 files. Web server and console-shared
  tasks pass, plus 24 focused lifecycle/cancellation/managed/startup cases.
- Task 3 is complete: actual large Journal 100-fence/remainder replacement,
  selected-extent restoration across Chunk Back/Forward, owner restart, stale
  cursor 409 and Refresh reset pass in 2.8 s (owned chain 35.94 s, teardown
  2545 ms). A real production split browser case separately passes inherited/
  current journals, cutover and dependencies in 32.3 s. Both use real services
  and persisted data.
- Task 5: reclamation/reopen estimates pass native FFI regression. Actual unequal
  load redistribution remains unverified: the corrected fixture failed after
  1971.25 s with eleven ranges (owner counts 9/2) when normal Group-0 reads
  lost quorum. All owned services were cleaned up; normal policy and request/
  heartbeat/lease budgets remain unchanged. The initial slow fixture exhausted the usable portion of
  an 8-GiB disk during split; the corrected fixture provisions 256-GiB sparse
  virtual disks and retains the ordinary cooldown. Do not mark byte balance done
  from count convergence alone or change deadlines to make acceptance pass.
  Final serial run was intentionally stopped at the user's offline request:
  2/0/0 at 577 s, 4/0/0 at 637 s, then 6/1/0 at 1242 s. This demonstrates one
  actual count transfer, not weighted redistribution. The user approved a
  one-minute default cooldown; it is not implemented yet. On resume update both
  protocol/server defaults and documentation, preserving unrelated request,
  heartbeat, lease and transfer safety budgets, then repeat native acceptance.
  All eighteen owned services and the test process are stopped; progress log
  is retained in console-weighted-final/artifacts/weighted-acceptance-stopped.log.
  Latest user criterion: with healthy movable serving partitions and a healthy
  idle target that has capacity, forty seconds without actual balance progress
  is a bug. Diagnose split priority/shared cooldown rather than waiting tens of
  minutes. A one-minute cooldown change alone cannot close this gap; do not
  exclude the disputed cooldown from the measurement or weaken safety fences.
- Task 7: standalone embedding and host-native managed shell/capability/data
  operations pass. Actual Docker image build/acceptance remains unverified:
  the pinned Ubuntu base image cannot be fetched through the configured registry
  proxy. Do not change the host proxy/authentication or substitute host-native
  acceptance for image evidence.
- Task 8: final uninstrumented ordered routine passes all 57 cases in 2.7
  minutes, cleanup 6 ms; frontend 138 tests in 26 files pass. Earlier runs had
  Group-status/reset timeouts. Deterministic regressions reproduce and verify
  two client defects: repeated identical hints discarded in-flight topology
  discovery, and a newly discovered endpoint inherited the failed route's
  exponential backoff. Both are fixed without increasing request, retry or
  election budgets. Temporary production diagnostics are removed. Final native
  restart/browser acceptance also passes as recorded above; slow byte-balance
  acceptance and the Docker external boundary remain separate open items.
  R203 stays open while required boundaries remain unverified.
