<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI Follow-up Plan

Persistent issue list requested by the user; keep this filename and remove
completed items after verified fixes. Record newly discovered UI and supporting
API problems here, including workarounds that do not constitute fixes.

Design: [Console UI specification](../design/console/design-crowdb-console-ui.md).
Execution history: [UI implementation plan](plan-console-complete-ui.md).

Goal: make the normal one-rack, three-node flow work without manual repairs.

## Inspector simplification

- [ ] **Redesign metrics separately**: Cluster and KV no longer render or poll
  raw internal metrics in the right inspector. Their identity, topology,
  election and read-state properties remain. A future metrics experience needs
  a separate design; do not reinstate the old generic counter list.

## Cluster and provisioning

- [ ] **Reset versus in-flight deployment**: the full browser suite exposed
  `Directory not empty` during reset while a recovered six-service plan was
  launching an auxiliary service. Quiesce accepted deployments and fence new
  plan/deployment writes before removing workspaces; cancellation must not
  orphan an unregistered child. Preserve the regression, no mutation retries.


- [ ] **Automatic DiskGroup data binding**: creating a DiskGroup must resolve
  its ordinary data group and establish its binding and owner automatically.
  On 2026-10-03, Store 0 had Group 0 and Store 1 had Group 1, but the current
  binding path required an ordinary group in Store 0. Bring-up required manual
  creation of Store 0 / Group 1 and `PUT /api/disk-groups/1/1/1/bind`.
  This is a provisioning bug, not a required operator step. Check the existing
  DG 1 with no disks and reconcile partial creations before retrying. Verify
  fresh creation, existing unbound groups, retries, and usable owner routing.
  Files: shared hardware operations, web `owner_assignment.rs`.
- [ ] **Reconcile missing DiskGroup owners**: DG 1 survived an earlier failed
  creation without an owner. Adding its binding and disk did not repair owner
  registration. Chunk-KV bootstrap then failed repeatedly with `no endpoint
  for disk_group 1`. Explicit owner assignment restored normal bootstrap.
  Existing incomplete groups must be reconciled automatically and readiness
  must check usable allocation routing, not only running DiskIO processes.
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


- [ ] **Chunk-KV Partition bug reported**: user flagged Partition behavior in
  the Chunk-KV tab on 2026-10-04. Capture the exact selection/display symptom
  and reproduce it before assigning a cause; do not conflate this report with
  the separately recorded balancing issue.

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
- Three-node native provisioning remains blocked after five root-cause-driven
  attempts. DDB cadence override serialization was fixed and tested; the
  first Chunk-KV deployment still exceeds the 20-second response contract.
  See the implementation plan's Blocked section. No timeout was increased.

- Latest affected selection after reset/cadence fixes: 13 browser cases pass
  in 1.5 minutes (specs 13/20/21/50). The owner/usage case is 1.7 seconds versus
  10.6 seconds before cadence serialization. Twenty DDB config tests and seven
  lifecycle tests pass. Full ordered-suite stability and client cancellation
  during reset remain unaccepted; no second full-suite pass is claimed.
