<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Console UI Specification

Depends on: [Console architecture](design-crowdb-console.md),
[KV architecture](../kv/design-crowdb-kv.md).
Satisfies: [Console architecture](design-crowdb-console.md).

This is the authoritative UI design and acceptance specification. Each numbered
contract defines observable behavior, not a claim that its implementation has
passed. Implementation status, runtime evidence, defects, and execution steps
belong in the working plan. There is no separate UI spec to reconcile with this
file. Backend architecture and wire formats remain in the linked component docs.

The scope covers all seven domains and their shared interaction rules. Backend
prerequisites and inspection APIs are part of acceptance; a rendering fixture
does not establish that a native service operation succeeds.

## Contents

- [1. Product contract](#1-product-contract)
- [2. Reference environment](#2-reference-environment)
- [3. Navigation and layout](#3-navigation-and-layout)
- [4. Visual language and service identity](#4-visual-language-and-service-identity)
- [5. Cluster](#5-cluster)
- [6. Properties and cross-links](#6-properties-and-cross-links)
- [7. Modes and embedding](#7-modes-and-embedding)
- [8. Bounded observation](#8-bounded-observation)
- [9. Creation and mutation dialogs](#9-creation-and-mutation-dialogs)
- [10. Accessibility](#10-accessibility)
- [11. Verification contract](#11-verification-contract)
- [12. KV](#12-kv)
- [13. Service lifecycle](#13-service-lifecycle)
- [14. DiskGroup and disk management](#14-diskgroup-and-disk-management)
- [15. Capacity](#15-capacity)
- [16. Loading, failures, and recovery](#16-loading-failures-and-recovery)
- [17. Fixed-cluster operator flow](#17-fixed-cluster-operator-flow)
- [18. Acceptance scenarios and scope](#18-acceptance-scenarios-and-scope)
- [19. Chunk](#19-chunk)
- [20. Chunk-KV](#20-chunk-kv)
- [21. Iceberg](#21-iceberg)
- [22. S3](#22-s3)
- [23. Actions and properties](#23-actions-and-properties)
- [24. E2E organization and timing](#24-e2e-organization-and-timing)

## 1. Product contract

- **UI-01:** One Console serves one configured cluster. Opening the UI enters
  that cluster directly; no Connect dialog, endpoint input, or access-key form
  precedes ordinary browsing.
- **UI-02:** The seven top-level tabs are Cluster, KV, Capacity, Chunk, Chunk-KV,
  Iceberg, and S3. Each has an independent selected resource and query window.
- **UI-03:** Assume an administrator/root session until UI authentication is
  introduced. Available backend operations remain authoritative; capability
  restrictions are not inferred from a missing login screen.
- **UI-04:** Display real resources and structured values. Example graphs,
  inferred ownership, and placeholder counts must not look like observed data.
- **UI-05:** Every remote list, expansion, preview, and layout has a work/size
  bound. A large cluster must not require a complete scan before the UI responds.

## 2. Reference environment

The main acceptance environment has one Rack and three Nodes. Each Node has one
KV, DiskDB, ChunkDB, DiskIO, Chunk-KV, and Access Server instance. Group 0 has
three replicas; an ordinary data Group 1 has three replicas. Storage has valid
DiskGroup bindings, live owners, and writable disks. Allocated and free blocks
provide observable capacity and bitmap fixtures.

- This is normal multi-node deployment. Single-node/unsafe colocated settings
  cannot be silently enabled to make acceptance pass.
- Rack diversity is preferred when multiple racks exist. A single rack with
  enough distinct nodes is valid; properties report actual node/rack protection.
- Three local processes on one host verify logical placement and UI behavior;
  they do not demonstrate physical-host fault tolerance.
- API provisioning may prepare this environment. The setup method does not
  replace the dedicated tests for creation and lifecycle dialogs.
- Empty, partial, unavailable, and very large datasets are additional fixtures,
  not reasons to replace the populated reference environment with mocks.

## 3. Navigation and layout

- **NAV-01:** The header keeps the seven domains in a stable order. Domain
  changes preserve that domain's bounded query state and selected identity.
- **NAV-02:** The left panel selects hierarchy/scope; the center presents the
  selected resource or overview; the right panel presents selected properties.
- **NAV-03:** Resource selection updates the center without replacing the whole
  application. Breadcrumbs identify scope; they are navigation, not duplicate
  Back links for inline details.
- **NAV-04:** Cross-links identify the exact destination resource. Returning
  restores the originating selection and page. A stale/deleted destination has
  an explicit state rather than silently selecting a different item. The header
  exposes Back/Forward with at most 32 visits. History stores identities, bounded
  query cursors and scroll coordinates, never object bodies or metadata payloads.
  New navigation clears Forward. Returning to an Iceberg reference verifies the
  saved metadata generation before inspecting it again.
- Shared physical/logical trees retain expansion and local search per domain.
  Return restores both expanded and collapsed branches. KV captures bounded
  byte cursors and focused Key identity, then refetches the window; it does not
  keep Value payloads in history. Capacity retains at most 32 Disk query states
  containing zone window, selected Zone and bitmap block window. Parent
  navigation can unmount a detail without losing its return query.
- **NAV-05:** Trees expand lazily. Each large child collection has continuation
  or Load more, a visible loaded count, and an explicit end/partial state.
  Collapsed branches do not recursively load their descendants.
- **NAV-06:** The center remains usable with the right panel open. Tables and
  diagrams scroll within their work area; action bars and pagination remain
  reachable. Resizing does not reset selection or trigger unbounded refetches.
- **NAV-07:** Every domain uses the shared Tree appearance and two draggable
  panel dividers. Left default width is 280 px; properties default is 320 px.
  Each divider supports pointer and keyboard resizing between 220 and 600 px.
  Switching domains introduces no panel replacement/slide animation.
- **NAV-08:** Parent cards in center topology diagrams collapse or expand their
  children on click while selecting the parent. Children retain their own
  expansion state. Sidebar focus reveals hidden ancestors. Right-click opens
  the entity menu without toggling expansion. Fit All fits visible cards.

## 4. Visual language and service identity

- **VIS-01:** Use muted colors with readable text and visible boundaries.
  Buttons, including Run Recalc, must have adequate text/background contrast.
  Color alone cannot encode state, role, or selection.
- **VIS-02:** Service instances use one compact `TYPE-ID` convention everywhere:
  `KV-1`, `DDB-1`, `CDB-1`, `DIO-1`, `CKV-1`, and `AS-1`. Trees, topology cards,
  selected titles, and instance menus use the same label.
- The suffix identifies the service instance in its own type. It must not be
  fabricated from a Node ID when those identities differ. Exact backend IDs
  remain available in properties and are used for API operations.
- Full names such as ChunkDB and Access Server belong in type properties,
  deployment choices, or help text. A tree label must not mix a verbose name,
  a separator, and another backend identifier.
- **VIS-03:** All integer IDs and offsets preserve their exact values; large
  identifiers do not pass through lossy JavaScript number conversion.
- **VIS-04:** Selected resources use a consistent outline/highlight. Health,
  lifecycle, readiness, and data protection are separate labelled values.
- Capacity bitmap: muted blue means used, muted green means free. Unknown,
  unavailable, and reserved states have distinct labels/patterns; they are never
  rendered as free.

## 5. Cluster

- **CLU-01:** The hierarchy is Datacenter → Rack → Node → service instances. The center
  topology uses the same hierarchy, identities, and selection as the left tree.
  Logical Paxos groups are managed in KV; physical storage is managed in Capacity.
- **CLU-02:** Rack actions include Add Node. A Node context menu independently
  manages each service type and each existing instance, using the same lifecycle
  pattern as KV/DiskDB. It must not route other service types through KV APIs.
- **CLU-03:** Add Node is one dialog and one submit. Defaults select the current
  Rack, next available Node ID, valid host settings, and a complete six-service
  set. Advanced overrides remain available before submission.
- Creating a Node produces an immutable created identity in the same dialog.
  Per-service rows show queued, waiting for dependency, deploying, deployed,
  or failed, with a specific reason. Closing progress is not another deployment
  confirmation and does not cancel already accepted work.
- Waiting deployment steps resume when prerequisites become available while
  the console session is active. The Node menu reopens/resumes the plan after
  navigation or reload; deployed instances are reconciled before retry.
- Six-service progress is saved in each Node workspace, with a revision checked
  on every update. Saving progress must succeed before a deployment starts.
  A competing browser receives a conflict and must reload. Reload turns an
  interrupted Deploying step into an explicit failure; Retry first checks the
  registered instance and never repeats a successful deployment. Stopped
  registered services require their normal Restart action. Removing a Node or
  resetting the cluster clears its deployment intent. This is durable progress,
  not an unattended scheduler while the console is closed.
- A partial failure retains the Node and successful services. Retry operates
  only on missing/failed steps and revalidates defaults. It never creates a
  second Node or a second copy of a successful service.
- **CLU-04:** Cluster readiness is reported by capability: KV quorum, storage,
  Chunk-KV, and Access protocols. A PID or registry entry alone is not Ready.
  Missing prerequisites link to the domain where they can be resolved.

## 6. Properties and cross-links

- **PROP-01:** Right-side properties describe the exact selected Rack, Node, service,
  Store, Group, Replica, DiskGroup, Disk, Zone, or allocation block.
- Show exact ID, type, parent/owner, source, observation time, and relevant
  lifecycle/health fields. Missing information is Unknown or Unavailable.
- Properties provide copyable full identifiers and exact byte/unit values.
  Abbreviating diagram labels must not destroy the original value.
- **PROP-02:** Resource links navigate by exact identity: service → Node,
  Replica → Node, Disk → DiskGroup, Zone → Disk, and DiskGroup → owner/binding. Failed optional
  lookups do not erase a successfully loaded primary resource.
- **PROP-03:** Raw JSON may be a diagnostic affordance. It is not the default
  inspector for supported structured resources.

## 7. Modes and embedding

- **MODE-01:** Standalone mode exposes administrator topology, service, storage,
  logical KV, and data operations supported by the backend.
- **MODE-02:** Container mode uses the same seven-domain UI for observation and
  supported logical/data operations. Cluster topology/deployment mutations and
  Capacity disk-management mutations are unavailable. Backend enforcement is
  required as well as disabled/hidden UI controls.
- **MODE-03:** Readonly embedding disallows mutations. Every fetch uses the
  configured API prefix. Styles remain scoped to the Console; embedding does
  not change resource identities or connect to another cluster.
- Embedding keeps `apiPrefix`, `basePath`, `readonly`, `modules`, `initialDomain`,
  and `onEvent`. Domain selection has no Swagger or per-node connection mode.

## 8. Bounded observation

- **DATA-01:** API pagination is authoritative. Continuations are opaque and
  scoped to the query/source/generation. The browser does not invent offsets
  or fetch all pages to provide a count, badge, filter, or layout.
- Defaults: Capacity zone pages contain 32 zones. Every other collection declares its page size at its boundary and
  exposes bounded continuation instead of an unbounded initial expansion.
- **DATA-02:** Prev/Next replace a window. Type/scope/query changes clear cursor
  history. End-of-list is explicit; no exact total is shown unless supplied by
  an authoritative bounded API. Selection remains stable across page changes.
- **DATA-03:** Label local filtering as applying to the loaded window. It must
  not launch a hidden cluster scan or imply that an empty local result means
  no matching resource exists globally.
- **DATA-04:** Cancel or ignore obsolete responses after scope/selection changes.
  A late response cannot overwrite the current selection. Polling cannot stack
  overlapping requests for the same resource or grow retry work indefinitely.
- Active views refresh at a bounded cadence; hidden views suspend observation
  polling. Accepted deployment work is independent of observation polling.
- **DATA-05:** Preserve partial successes, identify failed sources, and show
  observed/scanned/matched counts separately when they differ. Ownership or
  generation changes invalidate incompatible continuations and layouts.

## 9. Creation and mutation dialogs

- **FORM-01:** Every Create/Deploy dialog opens with usable, nonconflicting
  defaults for IDs, listener ports, and available parent/dependency references.
  With prerequisites present, default OK succeeds without manual editing.
- Defaults are fetched/revalidated at submission, across all service types on
  the relevant host. Include internal listener ranges, such as DiskDB's ports.
  A user edit is not overwritten by a late default response.
- **FORM-02:** Missing prerequisites disable the unsupported operation and name
  the prerequisite. Do not prefill a plausible value that inevitably fails.
  Real device paths are explicit; never infer a physical device to overwrite.
- **FORM-03:** Submission prevents duplicates. Field validation and backend
  errors stay in the dialog with entered values intact. Toasts are supplementary,
  not the only evidence of success or failure.
- **FORM-04:** Show the current operation and elapsed/waiting state for a slow
  request. A timeout means outcome unknown until reconciled, not automatically
  failed. Do not retry a potentially completed creation blindly.
- Successful creation selects or reveals the resource after authoritative
  refresh. Partial creation shows exactly what exists and what remains.
- **FORM-05:** Delete names the exact target and effects. Deleting a service,
  removing configuration, and deleting stored data are distinct operations.
  Reset lists its affected scope and stops queued deployment work before teardown.

## 10. Accessibility

- All controls have semantic roles and unambiguous labels. Keyboard operation,
  focus trapping, Escape/Close behavior, and focus restoration work in dialogs.
- Focus/selection/error indicators remain visible on the dark palette. Text
  contrast targets WCAG AA; color-coded states include text or a legend.
- Large diagrams retain accessible summaries and selected-object properties.
  Errors and asynchronous progress are announced without stealing focus.

## 11. Verification contract

- **TEST-01:** Prepare and verify backend prerequisites through APIs before
  visual acceptance. A mock response tests rendering, not backend success.
- Every acceptance result records: contract/scenario ID, fixture, actions,
  visible result, relevant bounded API result, pass/fail, and concrete defect.
- Use role/label/test-id selectors. Prefer a short scripted flow to repeated
  full-page snapshots. Capture one useful screenshot per completed scenario or
  failure; do not repeatedly inspect unchanged trees.
- Test mutation results from durable API state and resulting UI, not a toast.
  Poll readiness/leadership with a deadline; no arbitrary sleeps or hidden retry.
- Reuse the populated cluster for read-only cases. Isolate destructive cases
  and avoid resetting the whole environment for each tab.
- Scope evidence honestly: rendering passed, API passed, and end-to-end passed
  are distinct. Backend failures remain failures even if the UI handles them.

## 12. KV

- **KV-01:** Left tree is Store → Group → Replica. Center overview lists groups
  with exact Store/Group IDs, observed health, leader, and replica count.
- Before initialization, explain the dependency on deployed KV servers and offer
  Initialize Group 0. The three-node fixture selects all three eligible nodes.
- **KV-02:** Store/group/replica creation, membership operations, and deletion
  use the selected scope and valid defaults. Group 0 is visibly identified as
  the system group; ordinary groups are not confused with it.
- **KV-03:** Selecting a Group directly opens its data window; there is no
  Overview/Data toggle. Membership and node placement remain in properties.
  Unknown leader is not rendered as zero. The right inspector does not poll or
  render generic internal metrics.
- **KV-04:** Data operations target an explicit Store/Group. Get/put/delete/scan
  show encoding and exact key/value interpretation. Scan is bounded and paged;
  empty values, missing keys, and request failures are distinguishable.
- A selected replica or group may expose supported diagnostics; a generic
  physical Cluster diagram does not replace the logical KV workbench.
- **KV-05:** A data window contains at most 20 entries. Previous/Next replace
  rows; each Group has an independent cursor when browsing a Store. Clicking
  a row shows the full Key and Value. Strictly valid printable UTF-8 is text;
  other bytes display their original hexadecimal encoding prefixed by `0x`.
  Display conversion cannot change bytes sent in a mutation or continuation.
- **KV-06:** Get/Put/Delete occupy the shared collapsed Actions strip below
  the heading/path. Its expanded content names Store and Group. System Store 0 /
  Group 0 is read-only for ordinary KV mutations; management APIs own its state.

## 13. Service lifecycle

- **SVC-01:** Each instance has Start when stopped, Stop when running, Restart,
  and the supported remove operation. Menus target exact type and instance ID.
- **SVC-02:** Preserve desired configuration independently of PID. A restart
  retains endpoints, workspace, and data; refreshed properties show the new PID
  and separately observed readiness. Process identity is verified before signal.
- **SVC-03:** Startup observes dependencies: Group 0 before dependent metadata
  services; valid disks/ownership for storage; journal/data prerequisites before
  Chunk-KV bootstrap; Chunk-KV catalog before Access startup.
- Access provisioning durably records catalog initialization and activation
  request identities before issuing either mutation. Interrupted deployment
  reconciles committed state and resumes the same request. An existing catalog
  retains its identity and policy; restart does not reset operator changes.
  Missing or conflicting recorded identity fails explicitly.
- Service failures name the failing step and permit a safe retry. Existing
  instances are not silently duplicated or treated as healthy merely by presence.
- Operational type descriptions remain available even though instance labels
  use the compact convention in §4.

## 14. DiskGroup and disk management

- **DISK-01:** Capacity hierarchy is Rack → Node → DiskGroup → Disk → Zone.
  DiskGroup creation establishes its authoritative registration, data binding,
  and owner, or reports which prerequisite/step prevents completion.
- The dialog does not spin indefinitely when Group 0, a data group, or DiskDB
  is unavailable. It reports the actual failure/unknown outcome and reconciles
  whether a resource exists before offering retry.
- **DISK-02:** Disk creation exposes identity, type, device path, capacity, zone
  size, and allocation unit. Validate consistent geometry and ID uniqueness.
  Batch rows report per-row outcomes and preserve failed entries for correction.
- **DISK-03:** Distinguish physical location, DiskDB owner instance, and metadata
  Store/Group binding. Neither Node ID nor service ID implies ownership.
- Owner/status/binding operations are available only when supported and expose
  the resulting authoritative state. Container restrictions apply to all entry
  points, including context menus and direct API requests.

## 15. Capacity

- **CAP-01:** Cluster/Rack/Node/DiskGroup/Disk selections show relevant capacity,
  used/free allocation, disks, and observation time. Missing statistics are not
  zero and must not produce an all-free disk map.
- **CAP-02:** Disk detail retains its summary and paged zones. Default 32 zones,
  Prev/Next, current visible range, and optional cheap indexed zone lookup.
  Avoid full-zone scans for filtering or drawing a large disk.
- **CAP-03:** Selecting a Zone renders its details below the disk view. Keep the
  parent disk and zone list in place; no redundant Back to parent Disk link.
  If a dedicated zone view is used, provide an actual disk breadcrumb instead.
- **CAP-04:** Zone bitmap uses muted blue for used and muted green for free,
  with a legend, used/free counts, allocation unit, and zone geometry. Bit index
  maps to the correct block offset according to the protocol's decode order.
- Hover/selection provides exact block index and offset. Large bitmaps use
  bounded drawing/tiling or labelled aggregation; an aggregate is not one block.
- **CAP-05:** Refresh bitmap, scan/recalc, compact/rebuild, and status operations
  identify their scope and show progress/results. RPC or owner lookup failure
  remains visible at the affected zone and never becomes an empty/free bitmap.

- **CAP-06:** Opening Cluster does not issue DiskDB runtime usage or scan
  requests. Capacity usage is scoped to the selected DiskGroup/Disk when that
  scope is known; cluster totals explicitly request aggregate observation.
  Disk inventory belongs to one shared source. Expanding a Node loads its
  DiskGroups; expanding/selecting a DiskGroup loads its disks. Selecting a
  linked Disk loads the required ancestor inventory directly. Four workers
  serve opened branches, merging duplicate requests for the same branch.
  Refresh revisits requested branches and never traverses unopened disks.
  Failed branch refresh retains its known inventory and reports failure.
- **CAP-07:** The physical tree reads Node health from the service projection;
  it does not ping every Node or fetch a second per-node KV catalog. Capacity
  Nodes and DiskGroups begin collapsed; Datacenter/Racks remain visible.
  Default-service prerequisite inspection is explicit for its selected Node.

## 16. Loading, failures, and recovery

- **ERR-01:** Distinguish initial loading, refreshing existing data, empty,
  partial, unavailable, and failed. Keep usable previous data visibly stale
  while refreshing; do not flash an empty page on every poll.
- **ERR-02:** Display a concise cause, failed dependency/operation, and retry
  action. Technical endpoint/HTTP details may be expanded for diagnosis.
- **ERR-03:** A long-running or timed-out mutation preserves its target and
  inputs. Reconcile its result before retrying. Closing a modal does not prove
  server cancellation; UI text must not imply otherwise.
- **ERR-04:** Failure of detail/placement/one child branch does not erase the
  parent resource or other successful results. Unauthorized/unsupported actions
  are explicit and do not fall back to another protocol or cluster.
- Polling and automatic dependency checks have bounded cadence and fan-out.
  Identical failures do not flood notifications; explicit retry remains usable.

## 17. Fixed-cluster operator flow

1. Open Cluster directly. Inspect/create Rack and Nodes; add each Node through
   one dialog with six default services and explicit waiting dependencies.
2. In KV, initialize three-replica Group 0 and create ordinary data groups.
3. In Capacity, register DiskGroups/disks with owners and bindings; DiskIO becomes
   ready. Observe capacity and inspect a zone bitmap.
4. Return to Cluster to verify each service's deployment/readiness and resolve
   dependencies. Inspect logical groups in KV and physical storage in Capacity.

This is a dependency guide, not a blocking wizard. A working capability remains
usable while another is unavailable. No domain asks the operator to reconnect.
Data-browser flows for the four deferred domains are outside this specification.

## 18. Acceptance scenarios and scope

Each scenario uses the contracts above; failures are recorded in the working
plan, never edited out of this specification to make a run pass.

- **A01 / entry:** open the fixed cluster; all seven tabs appear; no connection
  or access-key form; restoring a domain does not change cluster identity.
- **A02 / create:** with one Rack, create three Nodes using valid defaults;
  verify one instance of every requested type per Node, conflict-free ports,
  same-dialog progress, and safe retry after one injected service failure.
- **A03 / lifecycle:** independently stop/start/restart an auxiliary instance
  from its Node menu; verify exact target, preserved data/configuration, updated
  PID, and readiness. All instance labels follow §4.
- **A04 / KV:** verify Group 0 and ordinary Group 1 with three replicas; select
  a group, inspect membership, and perform bounded exact/scan data operations.
- **A05 / Capacity:** create/register DiskGroup and disk, inspect owner/binding;
  select a disk with more than 32 zones, page, select Zone, and verify inline
  bitmap, decode offsets, muted colors, and error handling for unavailable owner.
- **A10 / limits:** large topology, group/replica lists, disk/zone lists, and KV
  scans remain bounded. Observe request counts/bytes and rendered item counts;
  no hidden fetch-all, invented totals, or selection replacement on page changes.
- **A11 / failures:** inject unavailable owner/group, malformed API response, stale cursor,
  and slow/unknown mutation outcome; verify §16 and retry without duplication.
- **A12 / modes:** Container rejects topology/disk mutations through UI and API;
  supported admin data operations remain usable. Readonly embedding forbids
  mutations and respects API/style isolation.

- **A06 / Chunk:** default real list, 10-entry replacement windows, exact ID,
  multiple Mirror/EC Strips, selected block properties and Disk/Node round trips.
- **A07 / Chunk-KV:** registered servers including empty owners, bounded graph,
  collapse, exact split identity, selected Tree/Journal and stale continuations.
- **A08 / Iceberg:** Catalog to Parquet footer through a real committed table;
  nested schema, snapshots, paged manifests/files and zero data-page reads.
- **A09 / S3:** bucket/object replacement pages, HEAD, bounded preview,
  upload/download/multipart, locations and return from Chunk to the source object.

Out of scope: UI login/role design and new data-plane protocols.
Unsupported features are explicit rather than represented as working controls.

## 19. Chunk

- **CHK-01:** Left tree is Datacenter → Rack → Node → CDB and KV Server;
  KV branches expand Store → Group without Replicas. CDB service-slot ownership
  and KV storage-slot ownership are distinct. Disjoint slot sets are not drawn
  as one continuous hash range. Unresolved ownership is labelled unavailable.
  Owner branches load only when expanded, with 32-slot Previous/Next windows
  and an exact generation token. A changed generation preserves the previous
  observation with an error and requires Refresh from the first window.
- **CHK-02:** Entering Chunk requests an All-type page automatically. The center
  lists at most 10 chunks without an inner vertical scrollbar. Previous/Next
  replace the page. Type is a protocol-defined selector; exact Chunk ID lookup
  sits above the list in the center. Arbitrary ID-prefix/global filtering is not
  required. Source failures identify partial coverage rather than an empty list.
- **CHK-03:** Selected Chunk summary and layout appear below the list. Each Strip
  has a compact two-line identity: `Strip <id> Mirror` or `Strip <id> EC k+m`,
  followed by logical interval such as `[8M, 16M)`. Stable protocol identities
  remain distinct from presentation indexes. Capacity, written bytes and physical
  usage are separate; missing values are unknown.
- **CHK-04:** Muted selectable block cards form each Strip. Mirror cards use
  `Mirror 1`, `Mirror 2`, etc.; EC cards distinguish data and parity. Small cards
  show identity/role, while properties carry full Node, DiskGroup, Disk, Zone,
  offset and length. No redundant horizontal Strip bar consumes a third row.
- **CHK-05:** Selecting a block updates right properties. Disk and Node links
  retain the originating chunk, page/cursor, Strip window, block and scroll.
  Repair status reflects actual protection deficit, not lack of rack diversity
  alone. Missing placement retains known Disk IDs rather than invented targets.

## 20. Chunk-KV

- **CKV-01:** Left shared Tree is Datacenter → Rack → Node → CKV Server → Split.
  No extra sidebar title duplicates the root. Registered servers stay visible
  even with no assigned splits. Compact split labels retain full ID and bounds
  in hover/properties; five rendered splits are a window, not a global count.
- **CKV-02:** The center is a node-link diagram, not a second indented sidebar:
  Chunk-KV root → CKV Server → Split → actual Tree ID. Its visible window is
  at most eight servers and five splits per server. Server cards collapse their
  children. Graph paging is separate from authoritative catalog pagination.
  Do not add a loaded-ID filter or an ambiguous All loaded servers control.
- **CKV-03:** Selecting a Split opens its scoped Tree/Journal information below
  the graph. Properties retain complete identities, bounds, epoch, owner/source,
  checkpoint and selected journal extent. Root/child KV Pages must come from a
  real bounded page-inspection API; journal fences are not KV Pages. Byte keys
  default to hex with an optional validated text interpretation.
- Returning from Chunk inspection restores the selected Split, Tree/Journal tab,
  catalog generation/window, graph server/split windows, collapsed branches and
  selected journal extent. History stores identities and cursors, not runtime
  payloads. Return revalidates the catalog and stream generations; stale state
  stays explicit until Refresh catalog/runtime starts a new observation.
- **CKV-04:** Tree and journal observations have bounded replacement pages.
  Continuation pins catalog/stream generation. Stale generation requires refresh
  from the first page; parent/child recovery dependencies retain separate stream
  offsets. Placement and split lineage are separate fields.

## 21. Iceberg

- **ICE-01:** The configured cluster Catalog loads automatically. Left shared
  Tree is Catalog → Namespace → Table → Snapshot → Manifest → File. Namespace
  and Table types are labelled explicitly. No Metadata leaf, Snapshots wrapper,
  or Manifest List file layer duplicates content. Current Snapshot is marked
  `Current`; immutable IDs remain exact.
- **ICE-02:** Catalog/Namespace centers list their children. Table center shows
  a compact overview followed directly by a Snapshot table: time, operation,
  records and files. Overview/Schema/Files controls belong only to selected
  Table, disappear for other resources, and have no independent Snapshots tab.
- **ICE-03:** Schema is an expanded tree table with field ID/name/type/required
  and hierarchy for structs, lists and maps. Element/key/value identities stay
  visible. Large schemas use bounded row windows; collapse is optional, not a
  prerequisite to seeing ordinary fields.
- **ICE-04:** Snapshot center has one scoped summary and a Manifest table.
  Expanding one Manifest row requests its File subtable. Manifest and File
  pagination are independent and lazy; no full-snapshot fetch to calculate a
  total. The right properties retain Manifest List path and known size even
  though that file is omitted from navigation.
- **ICE-05:** Manifest/file inspectors render decoded fields. Parquet shows
  footer information and sized Row Group/column blocks. Selecting a column
  shows complete metadata in properties. Inspection reads footer/metadata only,
  not data pages. Unsupported file types show an explicit capability state.
- **ICE-06:** Actions follow selected Catalog, Namespace or Table. Catalog
  actions disappear when a Table is selected. Paths, titles and summaries name
  the focused item once; properties must not merge unrelated parent identities.

## 22. S3

- **S3-01:** Left Tree is S3 → Bucket. A single namespace introduces no wrapper;
  the logical S3 root is not named Datacenter. Root center lists buckets with
  20-row windows; it does not repeat S3 as path, type and heading.
- **S3-02:** Selecting a Bucket displays an API page of at most 20 objects.
  Prefix changes reset cursor history; Previous/Next replace rows. Any bucket
  filter over already loaded metadata explicitly names that local scope.
- **S3-03:** Selecting an Object replaces the list with HEAD fields: key, size,
  ETag, last modified, content type, supplied version and user metadata. Missing
  fields show `—`. Breadcrumbs restore the Bucket list and its cursor/prefix.
  Preview is a separate bounded operation, not an automatic payload GET.
- **S3-04:** Root Actions create buckets/demo; Bucket Actions upload/delete and
  multipart; Object Actions preview/download/delete. Exact bucket/key is visible.
  Multipart state distinguishes uploaded parts, completion, abort, failure and
  unknown outcome. Downloads stream; previews read at most the advertised cap.
- **S3-05:** Object details include Storage locations below HEAD, 20 extents per
  page: stable index, logical `[start, end)`, Chunk ID, chunk `[offset, end)`, and
  physical length. Properties show exact decimal bytes and full identity.
  Chunk links preserve object and extent cursors, selected extent and scroll.
- Locations are an admin metadata query, never inferred from HEAD or obtained
  by reading object payloads. Empty/missing/corrupt/oversized references have
  distinct states. Continuation binds object and metadata generation; overwrite
  reports stale and requires first-page refresh. Exact 64-bit offsets survive
  JSON/JavaScript without rounding. Bounded decoding and response limits are
  mandatory; no recursive placement fetch for every extent.
- **S3-06:** `GET /api/access/s3-inspect/locations` accepts exact bucket/key,
  a 1–100 limit (default 20), and an opaque cursor. The Console targets only
  the configured Access origin and supplies its server-held management token;
  browser credentials and arbitrary endpoint parameters are rejected or ignored.
  Access authorizes management privilege before metadata reads and resolves the
  configured tenant's bucket/object itself. There is no object-payload client in
  the inspection path.
- **S3-07:** Inspection decodes at most 4 MiB of stored references and returns
  at most 1 MiB. Access has a five-second metadata deadline; the Console proxy
  has a six-second request deadline. Reference-limit errors are 413, corrupt
  references 422, missing objects 404, and changed cursor generations 409.
  Invalid, expired or oversized cursors and limits are rejected. Cursor expiry
  is 15 minutes and its authenticated scope includes bucket identity, exact key,
  revision-sensitive generation and next extent index. Identical overwrites also
  invalidate old cursors.
- **S3-08:** Location failures preserve HEAD. Empty objects show `No storage
  extents`; null Chunk identities show `Location unavailable` without a link.
  Previous/Next retain at most 32 cursor positions and replace the page; at most
  four bounded location pages are cached. A stale result keeps the previous
  page labelled stale and disables continuation until first-page refresh.
  Selection is captured before following a Chunk link; return refetches metadata
  against the saved generation and restores the exact selected extent.

## 23. Actions and properties

- **ACT-01:** Resource title/path appear first, then one compact Actions strip,
  collapsed by default, then primary content. Expanded Actions name their exact
  operation target. Changing resource closes obsolete action forms.
- **ACT-02:** KV, Iceberg and S3 use this shared structure and button style.
  Ordinary buttons have a visible border; destructive buttons use muted red.
  Refresh, Previous and Next belong with the query/list and do not hide inside
  mutation forms. Readonly/container capabilities apply to every action.
- **ACT-03:** Right properties show complete information for the clicked item,
  including graph blocks/columns/extents. Optional diagnostics never replace
  primary details. Cluster/KV generic internal metrics are excluded until a
  separate metrics design exists.

## 24. E2E organization and timing

- **TEST-02:** Page-function specs retain each behavior once: shell `0x`,
  physical lifecycle `1x`, logical KV `2x`, KV data `3x`, inspector/canvas `4x`,
  Capacity/Chunk/Chunk-KV `5x`, Iceberg `6x`, S3 `7x`, cross-function `9x`.
  Dedicated creation/reconfiguration specs retain their UI mutations; a smoke
  chain does not repeat every dialog or failure permutation.
- **TEST-03:** Use three layers: fast deterministic rendering/failure fixtures;
  real API-backed management/data tests; isolated native full-stack acceptance
  including deployment and container capability enforcement. A mock layer never
  substitutes for a missing native contract. Share setup per spec and keep
  destructive cases last. Reset only for cases that require empty authority.
- **TEST-04:** Keep the step timer and slow-test reporter. Measure setup, mutation
  response, lifecycle readiness, DOM refresh and teardown separately. Slow steps
  at 2 s and very slow steps at 5 s remain logged even when a step fails. Tests
  at 10/30 s retain slow/very-slow reports. Compare per-test baselines; investigate
  more than 2× rather than hiding time with retries or larger deadlines.
- **TEST-05:** One worker owns the mutable runtime; installed system browser,
  no automatic browser installation. Default assertion deadline is 3 s and
  election 10 s, polling every 100 ms. No fixed sleeps. Native large multipart
  and SF=1 loader acceptance run separately from ordinary UI iteration; normal
  smoke data remain small while preserving multi-Strip and pagination cases.
