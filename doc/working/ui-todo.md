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

- [ ] **Initialize ChunkDB slot maps during cluster provisioning**: after the
  main rebase on 2026-10-04, a fresh three-node deployment failed because
  `/chunkdb/slot_head/storage` was not initialized. The UI launch configuration
  does not supply the new fixed-slot bootstrap. Initialize explicit service
  instances and ordinary storage groups through `ChunkSlotMapClient` before
  deploying ChunkDB; preserve existing maps and reject conflicting layouts.
  This bring-up used a temporary normal-production bootstrap configuration
  with instances 1/2/3 and Store 0 / Group 1, not a UI-flow fix.

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
- [ ] **Create DiskGroup stalls on unhealthy Group-0 RPC**: requests previously
  waited while Node 3 RPC failed; its logs included a full watch queue and
  `std::bad_alloc`. Restart recovered service but did not establish or fix the
  cause. Diagnose transport failure separately; bound API wait time and show
  actionable failure/unknown outcome instead of an indefinite spinner.
  Verify state reconciliation before retrying a mutation.
- [ ] **Chunk-KV normal deployment exits before readiness**: current Node 1
  deployment on HTTP 15010 returned 502, child PID 668297 exited with status 0,
  and the captured log tail was empty. Three DiskIO services were running;
  metadata Store 1 / Group 1 was present. Service logs traced this occurrence
  to DG 1's missing owner; manual owner assignment and redeployment recovered
  all three instances. Preserve causal startup errors and report nonzero exit
  status on bootstrap failure. Recovery is not a fix of the provisioning flow.


- [ ] **Initialize Iceberg catalog during first Access deployment**: deployment
  failed with `Error: Uninitialized` after Chunk-KV became ready. Explicit
  catalog initialize/activate followed by redeployment brought all three
  Access instances up. Integrate idempotent catalog setup into provisioning,
  preserving existing catalog identity and capabilities on restart. Durable
  request recovery is implemented and unit-tested; real Access restart and the
  native browser chain pass. Fresh normal three-node bring-up remains pending.
- [ ] **Six-service plan recovery**: the one-dialog queue is implemented but
  lives in browser-session state. Verify Node-menu resume after reload, restart,
  partial deployment and prerequisite arrival, with no duplicate instances.
  Do not mark the normal three-node bring-up accepted until all six service
  types run on each Node.
- [ ] **Create defaults acceptance**: conflict-free defaults are implemented;
  complete repeated-create and reopen acceptance across IDs, host listener
  ports, DiskDB port ranges and dependency references.
- [ ] **Uniform service labels**: KV-N, DDB-N, CDB-N, DIO-N, CKV-N and AS-N
  changes exist in the working tree. Finish affected validation and inspect
  tree, canvas, menus and properties before marking complete.

## Capacity and shared behavior

- [ ] **Cluster canvas click-to-expand**: reuse the Chunk-KV center graph's
  click-to-expand/collapse interaction in the Cluster center view. Preserve
  selection, expansion state and bounded rendering while revealing children.
- [ ] **Navigation return history across all views**: every button/link that
  navigates to another resource or view must retain a return route. Back and
  Forward restore the source domain, resource, list cursor, tree expansion,
  selected detail and scroll position. Keep ancestry breadcrumbs separate
  from visit history; refresh and mutations must not create navigation entries.
  Include property links and graphical nodes, not only sidebar navigation.

- [ ] **Return to Chunk after placement navigation**: selecting a disk block
  and following its Disk or Node property link opens Capacity or Cluster, but
  there is no return path to the originating Chunk. Add contextual back
  navigation and restore the originating list window/type/filter, exact chunk,
  strip page, selected strip/block and scroll position. Verify both Disk and
  Node round trips. Reported by the user; deferred from the current layout work.
- [ ] **Bound Capacity population**: remove eager node/disk topology fanout and
  cluster-wide usage reads where scoped observation suffices. Preserve 32-zone
  windows and bounded bitmap rendering; verify large synthetic populations.
- [ ] **Cross-view and failure acceptance**: verify scope restoration,
  disk/node navigation, owner movement, stale cursors, unavailable services,
  and container-mode topology/disk mutation restrictions against the current
  Cluster, Capacity and KV specification.

## Data environment and deferred design

- [ ] **Chunk-KV balance leaves a ready server without splits**: observed
  three registered instances (CKV-1/2/3 on Nodes 1/2/3), all readiness endpoints
  returning 200, but the complete catalog contains six splits distributed
  4/2/0 (`next=null`). User reported this as a balance bug. Check whether
  balancing is enabled, server eligibility, trigger/threshold policy, and
  migration scheduling/failures before assigning a root cause. Verify eventual
  distribution under the intended policy, including a newly joined ready
  server. Do not assume equal split counts alone prove balanced load.
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

- [ ] **Disposable E2E process ownership**: test-mode services used the shared
  persistent console root; a failed earlier run left a KV child alive and its
  outbound connection collided with a later test listener. Add teardown and
  isolated runtime ownership, fail unknown mutation outcomes without blind
  retries, and verify failed-run cleanup without touching the live cluster.
  Ephemeral test-mode runtime, awaited parallel stop, SIGTERM cleanup and
  global teardown now pass real regressions. Forced SIGKILL child recovery
  remains to be verified before removing this item.

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
