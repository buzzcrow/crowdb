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
- [25. Dataset](#25-dataset)

## 1. Product contract

- **UI-01:** One Console serves one configured cluster. Opening the UI enters
  that cluster directly; no Connect dialog, endpoint input, or access-key form
  precedes ordinary browsing.
- **UI-02:** The seven top-level tab labels are Cluster, PaxosKV, Chunk, ChunkKV,
  Iceberg, Dataset and S3. KV and Chunk-KV remain their internal domain identities.
  Each domain has an independent selected resource and query window. Cluster
  shares topology/capacity selection and tree expansion without a view switch;
  topology visibility follows CLU-05.
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
  selected resource or a paged child list; the right panel presents selected
  properties. Resource operations follow the context-menu contract in §23.
- **NAV-03:** Cluster row/card selection updates properties without replacing the center
  scope. Names enter that resource; closing properties retains the work area.
  Cluster breadcrumbs identify the current physical scope; they are navigation, not duplicate
  Back links for inline details.
  This graph inspection distinction does not apply to ordinary resource tree/list
  rows: activating their label, icon or row selects the same resource. Expansion
  controls are separate. At DiskGroup scope, both Disk icon and Disk ID enter
  the same Disk workspace, including from a DiskDB ownership reference.
- **NAV-04:** Cross-links identify the exact destination resource. Returning
  restores the originating selection and page. A stale/deleted destination has
  an explicit state rather than silently selecting a different item. The header
  and browser Back/Forward share one session history with at most 32 retained
  visits. History stores identities, bounded
  query cursors and scroll coordinates, never object bodies or metadata payloads.
  New navigation clears Forward. Returning refetches the saved bounded window;
  scroll restoration waits for that window to render. Resource types and parent
  identities participate in selection equality, so equal numeric IDs in
  different resource kinds cannot share a selected highlight. Returning to an Iceberg reference verifies the
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
  Capacity observations retain the last successful inventory, usage and scan
  independently after a failed refresh. Unknown values and refresh errors appear
  with the affected capacity data; raw observation metadata is not a global strip. Runtime usage is keyed by group/disk
  scope; an unobserved scope remains unknown instead of borrowing other totals.
- **NAV-07:** Every domain uses the shared Tree appearance and draggable
  panel dividers. Cluster/KV left default width is 240 px; data workbenches use
  280 px. Properties default is 320 px.
  Cluster navigation resizes between 200 and 360 px, properties between 280
  and 480 px, preserving 480 px for the center when space permits. Below
  1100 px in Cluster, properties overlays the work area as a dismissible drawer; Escape
  closes it and restores focus. Navigation remains visible without a hide/show control. Other domain dividers retain their 220–600 px bounds.
  Switching domains introduces no panel replacement/slide animation.
- **NAV-08:** Parent cards in center topology diagrams have separate named controls for
  opening the resource and collapsing or expanding children. Card bodies inspect
  without navigating. Children retain their own
  expansion state. Sidebar focus reveals hidden ancestors. Right-click opens
  the entity menu without toggling expansion. Fit All fits visible cards.
  Hiding a topology panel interrupts viewport motion before zero-size geometry
  is observed. Rapid domain switches retain finite coordinates; centering runs
  only in a visible, measured canvas.
  The default diagram initially expands Datacenter/Rack and stops at visible Node cards;
  service descendants appear when a Node is explicitly expanded.

The header shows the CrowDB brand, domain navigation, then Back/Forward and
Refresh in the right action area. Access instance `access-1` displays as `AS-1`
while API operations retain the original identifier.

Shell layout (NAV-01/02/04/07):

```text
+------------------------------------------------------------------------------------------------------+
| CrowDB  Health | Cluster PaxosKV Chunk ChunkKV Iceberg Dataset S3 | [Back] [Forward] [Refresh]           |
+------------------------+------------------------------------------------------+----------------------+
| Resource tree          | Current scope                                        | Properties           |
| v Parent               | Query / filter                            [Refresh]  | Selected identity    |
|   Child                | Primary content or paged child list                  | Structured values    |
|                        |                                  [Previous] [Next]   |                      |
+------------------------+------------------------------------------------------+----------------------+
          draggable divider                               draggable divider
```

All wireframes show layout contracts, not runtime evidence. Names, IDs, counts
and statuses are illustrative. `>`/`v` denote collapsed/expanded branches, `*`
marks selection, and `[Auto]` denotes the accessible Auto icon. Menu entries
remain subject to the resource capabilities and lifecycle rules in the text.

## 4. Visual language and service identity

- **VIS-01:** Use muted colors with readable text and visible boundaries.
  Buttons, including Run Recalc, must have adequate text/background contrast.
  Color alone cannot encode state, role, or selection.
- **VIS-02:** Service instances use one compact `TYPE-ID` convention everywhere:
  `PKV-1`, `DDB-1`, `CDB-1`, `DIO-1`, `CKV-1`, and `AS-1`. Trees, topology cards,
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
  topology uses physical identities and shares selection with the left tree.
  The left tree separates each Node into Services and Storage. Physical groups
  and disks belong under Storage regardless of their DiskDB owner; service
  instances remain under Services. Logical Paxos groups are managed in KV.
  A DiskDB instance also expands to references to the DiskGroups it currently
  owns. These use authoritative instance ownership, not physical Node membership.
  They open the same resource as Storage references and update on ownership changes;
  unknown ownership never assigns a group to an arbitrary instance.
  Candidate nodes forms another Datacenter branch, alongside Racks, consistent
  with disk candidates under Storage. Only unjoined candidates for this cluster
  appear there; confirmed members appear only in their Rack. Raw discovery logs
  are not rendered in the tree. Legacy Capacity links resolve to Cluster.
  Add Rack and Add Store belong to the respective Datacenter context menu;
  uninitialized KV exposes Initialize Cluster there.
- **CLU-02:** Rack actions include Add Node. A Node context menu independently
  manages each service type and each existing instance, using the same lifecycle
  pattern as KV/DiskDB. It must not route other service types through KV APIs.
- **CLU-03:** Standard OCI Add Node opens the existing candidate flow with its
  Rack preselected. Verify and review authenticates SSH and reads actual disk
  inventory without admitting the node or initializing storage. Join and start
  submits the reviewed choices and accepted preflight identity. Existing accepted
  or pending-review Rack choices take precedence over a new default.
- Defaults select Paxos-KV, DiskDB, DiskIO, ChunkDB, Chunk-KV and Access Server.
  Each service and its listeners can be disabled. Access has independent Iceberg,
  S3, Dataset and Health ports; DiskDB's related RPC listeners must not overlap. Node
  admission owns the fixed KV bootstrap listeners. Canonical service type is
  `paxos-kv`; a legacy `kv` wire value is rejected.
- With no supplied input, standard setup chooses a 16-GiB preallocated file and
  1-GiB host reserve. Sparse allocation is explicit. Supplied media retain exact
  observed identities and capacities; each raw device requires initialization
  acknowledgement. Diskless setup creates no disk. Invalid supplied choices
  never fall back to a managed file.
- The target monitor durably accepts storage and service intent before reporting
  acceptance. Progress continues after the dialog, Web connection or browser
  closes; restart resumes the same intent. Node progress polls every two seconds
  and retains last confirmed rows on observation failure. A completed setup
  receipt is separate from current service or I/O health.
- Storage waits for Group 0 and an ordinary metadata binding. Dependent services
  wait for real prerequisites. A failed disk/service retains successful siblings;
  explicit retry uses their original operations and fixed placement. Later
  operator Stop/Restart/Delete commands take precedence over setup replay.
  Consecutive failed startup/readiness attempts have a durable five-attempt
  budget per service operation; successful readiness clears that budget.
- Legacy standalone Add Node retains its one-dialog registration and saved
  six-service plan. Its session reconciler saves revision-checked progress before
  deployment, resumes waiting steps while that Console session is active, and
  requires explicit retry for interrupted deployment. It never duplicates a
  registered Node or successful service. This session-owned plan is distinct
  from the standard target monitor's accepted node setup.
- Internal service health probes the RPC endpoint independently for each
  service. Startup records lacking RPC endpoints use process observation.
  Before its first store exists, PKV reserves but does not bind its RPC port;
  process liveness covers that startup phase. Once a store exists, a failed
  RPC probe remains Failed even when the process is alive.
  Access health probes its dedicated listener; missing retained health
  configuration is Unknown and requires reconciliation before restart.
  Current service observations take precedence over persisted node projections.
  DiskDB instances are associated by complete advertised RPC addresses; equal
  ports in different node namespaces do not imply the same instance.
- Cluster selections never display or request Chunk ownership.
- **CLU-04:** Cluster readiness is reported by capability: KV quorum, storage,
  Chunk-KV, and Access protocols. A PID or registry entry alone is not Ready.
  Missing prerequisites link to the domain where they can be resolved. Remote
  Access readiness uses the committed dedicated health listener on the owning
  node; missing configuration remains Unknown and failed probes are Down.

- **CLU-05:** The cluster overview and Datacenter/Rack/Node scopes show topology.
  Datacenter/Rack/Node capacity appears below it. Storage, DiskGroup, Disk and
  Zone scopes hide topology and release its space for storage content/capacity,
  including when entered through a DiskDB ownership reference. Returning to a
  higher scope restores topology. Service selections and an unselected cluster
  do not show capacity. Node details and
  configuration editing belong in the properties panel. Storage selection exposes
  its disk operations without a second node summary. There are no Topology/Capacity
  tabs, diagram toggle, or node-card toggle.
- **CLU-06:** Auto join starts enabled for a new standard workspace and retains
  an explicit target. Three authenticated eligible nodes are ordered by UUID;
  the smallest UUID initiates Store 0 / Group 0 with those fixed three voters.
  Fewer nodes wait. Later admission is nonvoting and never expands/replaces
  Group-0 voters. Explicit single mode auto-starts the same initialization
  lifecycle with one local voter. Turning Auto join off stops new triggers;
  accepted initialization continues and conflicting intents require recovery.
  Eligible unjoined candidates default to Auto join, including later admissions
  into a bound cluster. Each candidate can retain a durable explicit override.
  Enabled candidates join the default Rack; later node editing can change its
  Rack. Disabling one candidate stops its new admission triggers without changing
  another candidate's policy or cancelling accepted work. Initialization
  progress and failure recovery appear in Datacenter properties; completed
  initialization is not a persistent banner.
- **CLU-07:** Each candidate is one ordinary selectable tree row labelled
  `node-<short-id>`. An Auto icon, accessible label and tooltip express the effective
  policy; unloaded policy is Unknown, never an unchecked/disabled substitute.
  Full identity, addresses, discovery/admission state, errors and policy appear in
  properties. Enable/Disable Auto join is available in its context menu and
  properties, backed by the same state. Manual admission, cancellation and
  discarding a prepared review appear only when applicable, in that node's menu.
  There are no row checkboxes, inline action stacks, raw discovery logs or
  general-purpose Update node / Clean up Group 0 controls in the candidate tree.

Cluster: Datacenter/Rack/Node scope (CLU-01/05):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter *             | datacenter                                                   | Properties                   |
|   v R-1                    | Topology                                                     | Datacenter                   |
|     > N-1                  |                  [datacenter]                                | Name          datacenter     |
|     > N-2                  |                       :                                      | Health        observed       |
|     > N-3                  |                     [R-1]                                    | ...                          |
|   > Candidate nodes        |                  /    :    \                                 |                              |
|                            |               [N-1] [N-2] [N-3]                              | Initialization progress      |
|                            | ----------------------------------------------------         | (only when relevant)         |
|                            | Capacity                                  [Refresh]          |                              |
|                            | Capacity / Allocated / Free / Unknown coverage               |                              |
|                            | Resource             Capacity       Usage                    |                              |
|                            | R-1                  ...            ...                      |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Cluster: candidate selection and effective Auto policy (CLU-06/07):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter               | Cluster overview                                             | Properties                   |
|   > R-1                    | Topology remains the primary overview.                       | node-a1b2                    |
|   v Candidate nodes        |                                                              | Discovery ID  <full UUID>    |
|     node-a1b2 [Auto] *     | Candidate details are in Properties.                         | Addresses     <endpoints>    |
|     node-c3d4              | No inline candidate cards or operation log.                  | State         Eligible       |
|                            |                                                              | Auto join     Enabled [v]    |
|                            |                                                              | Target Rack   Default Rack   |
|                            |                                                              | Admission     Waiting        |
|                            |                                                              | Error         None           |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Candidate menu and DiskDB ownership references:

```text
Right click node-a1b2          Ownership reference       Physical reference
+---------------------------+ N-1                       N-2
| Disable Auto join         |   Services                  Storage
| Join node...              |     DDB-1                     DG-7
| Cancel admission          |       DG-7                      Disk-a
| Discard access review     |         Disk-a
+---------------------------+            \                /
                                   same DG / Disk identity
Joined candidate -> default Rack -> normal Node tree item
Explicitly disabled candidate -> no new automatic admission
```

Candidate menu entries depend on admission state. A DiskDB ownership reference
can point to disks on another physical Node. Selecting its DG hides topology
just like selecting that DG below Storage.

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
- **MODE-02:** Standard manual OCI nodes expose authenticated admission and
  confirmed Group-0 topology/service/storage operations. Explicit single-profile
  preview mode uses the same seven-domain UI for observation and supported logical/
  data operations, but rejects physical topology/deployment/storage mutations.
  Backend enforcement accompanies hidden/disabled controls in either mode.
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
- Automatic Access deployment checks live DiskIO ownership before provisioning
  credentials or catalog state, including when an existing Chunk-KV process is
  available. A transient ownership gap keeps the plan waiting and resumes
  without another Submit when the prerequisite is confirmed.
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
- Cluster reset fences new plan/deployment writes, waits at most 30 seconds
  for accepted work, then stops registered children before deleting workspaces.
  If accepted work remains pending, reset returns a conflict and removes nothing.
  Cleanup operations use the reset's exclusive context; another browser cannot
  start a deployment during cleanup.
- **FORM-05:** Delete names the exact target and effects. Deleting a service,
  removing configuration, and deleting stored data are distinct operations.
  Reset lists its affected scope and stops queued deployment work before teardown.

Shared dialogs and operation feedback (FORM-01–05, ACT-05):

```text
Parent menu: Create table...           Resource menu: Drop table...
+----------------------------------+ +---------------------------------------+
| Create table                 [X] | | Drop table                        [X] |
| Namespace: analytics             | | Target: analytics.events              |
| Name [.........................] | | Remove this Catalog reference.        |
| Schema [field editor...........] | | Physical reclamation follows GC.      |
|                                  | |                                       |
| Validation / request error       | | Exact scope and blockers appear here. |
|                [Cancel] [Create] | |                 [Cancel] [Drop table] |
+----------------------------------+ +---------------------------------------+

Dataset publication: Review -> Evaluate -> Prepare -> Publish -> Receipt
Operation feedback:  exact target / operation ID / waiting / progress / error
Unknown outcome:     reconcile the same operation before retrying
```

Forms open only after a menu action. Progress follows the actual operation;
simple creation need not acquire publication stages. Closing a dialog does not
cancel accepted work.

## 10. Accessibility

- All controls have semantic roles and unambiguous labels. Keyboard operation,
  focus trapping, Escape/Close behavior, and focus restoration work in dialogs.
- Focus/selection/error indicators remain visible on the dark palette. Text
  contrast targets WCAG AA; color-coded states include text or a legend.
- Large diagrams retain accessible summaries and selected-object properties.
  Errors and asynchronous progress are announced without stealing focus.

## 11. Verification contract

- **TEST-01:** Prepare and verify backend prerequisites through APIs before
  visual acceptance. Browser E2E uses real services, persisted metadata and file
  bytes. Response mocks, interception and HAR replay are forbidden. Pure unit
  tests remain separate and cannot satisfy end-to-end acceptance.
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

- **KV-01:** Left tree is Datacenter → Store → Group → Replica. Store creation
  belongs to the Datacenter context menu. The center is the bounded data browser;
  exact Store/Group IDs, health, leader and replica membership remain in properties.
- Before initialization, explain the dependency on deployed KV servers and offer
  Initialize Group 0. The three-node fixture selects all three eligible nodes.
- **KV-02:** Store/group/replica creation, membership operations, and deletion
  use the selected scope and valid defaults. Group 0 is visibly identified as
  the system group; ordinary groups are not confused with it.
- **KV-03:** Selecting a Group directly opens its data window; there is no
  Overview/Data toggle. Membership and node placement remain in properties.
  Unknown leader is not rendered as zero. The right inspector does not poll or
  render generic internal metrics.
- **KV-04:** Read operations target an explicit Store/Group. Query and Scan
  show encoding and exact key/value interpretation. Scan is bounded and paged;
  empty values, missing keys, and request failures are distinguishable.
- A selected replica or group may expose supported diagnostics; a generic
  physical Cluster diagram does not replace the logical KV workbench.
- **KV-05:** A data window contains at most 20 entries. Previous/Next replace
  rows; each Group has an independent cursor when browsing a Store. Clicking
  a row shows the full Key and Value. Printable UTF-8 runs remain text; only
  undisplayable characters and malformed bytes become uppercase hexadecimal
  runs without `0x`, distinguished by warm gold text, a dark background and border.
  Display conversion cannot change bytes sent in a mutation or continuation.
- **KV-06:** The KV data browser is read-only: Query performs an exact Get and
  Scan reads a bounded window. There are no Put/Delete/demo injection controls.
  Topology and membership operations remain in resource context menus and use
  management APIs. A uniform menu style does not add data mutation capabilities.

PaxosKV: Group data inspection (KV-01–06):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter               | PaxosKV                                                      | Properties                   |
|   v S-0                    | Store [S-0 v]  Group [G-1 v]                                 | Group G-1 / selected Key     |
|     > G-0                  | Prefix [........................] [Scan]                     | Store         S-0            |
|     v G-1 *                | Exact key [.....................] [Query]                    | Leader        LR-2           |
|       LR-1  Follower       | ----------------------------------------------------         | Members       LR-1,2,3       |
|       LR-2  Leader         | Key                         Value                            | Key           <exact bytes>  |
|       LR-3  Follower       | key-a                       <bounded byte display>           | Value         <exact bytes>  |
|                            | key-b                       <bounded byte display>           | Encoding      UTF-8 / hex    |
|                            | ...                                                          |                              |
|                            |                          [First] [Previous] [Next]           |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Create Store belongs to Datacenter; Group/Replica management belongs to its
resource menu. Data browsing is read-only; Query/Scan remain center query tools.

## 13. Service lifecycle

- **SVC-01:** Each instance has Start when stopped, Stop when running, Restart,
  and the supported remove operation. Menus target exact type and instance ID.
- **SVC-02:** Preserve desired configuration independently of PID. A restart
  retains endpoints, workspace, and data; refreshed properties show the new PID
  and separately observed readiness. Process identity is verified before signal.
  Lifecycle acceptance first checks the submitted Start/Restart response, then
  observes the new PID and process. An in-flight startup is not a completed
  operation and must not trigger a duplicate cleanup restart.
- In the legacy session-owned deployment flow, page unload stops its runner.
  Standard accepted node setup runs on the target monitor independently of the
  browser, as defined in CLU-03. Cancellation of an old page's
  observation or progress request cannot publish a service failure. The next
  page reloads the persisted plan and reconciles interrupted deployments;
  restoring a cached history page also reloads before resuming its runner.
- **SVC-03:** Startup observes dependencies: Group 0 before dependent metadata
  services; valid disks/ownership for storage; journal/data prerequisites before
  Chunk-KV bootstrap; Chunk-KV catalog before Access startup.
- Plans waiting for a registered DiskIO service to restart observe dependencies
  every two seconds, including when Capacity data has not been loaded. The
  ten-second missing-disk backoff applies only to other hardware waiting states.
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

- **DISK-01:** The physical tree ends at Disk: Rack → Node → Storage → DiskGroup
  → Disk. Zones and allocation bitmaps remain in the center workspace.
  DiskGroup creation establishes its authoritative registration, data binding,
  and owner, or reports which prerequisite/step prevents completion.
- The dialog does not spin indefinitely when Group 0, a data group, or DiskDB
  is unavailable. It reports the actual failure/unknown outcome and reconciles
  whether a resource exists before offering retry. DiskGroup creation returns
  within eight seconds. An unknown outcome keeps the ID fenced until accepted
  work finishes; reopening the same ID/name reconciles registration, binding
  and owner instead of allocating another group. Binding can exist before a
  DiskDB service is registered; this partial state names the missing owner
  prerequisite explicitly.
- **DISK-02:** Node storage separates registered disks, operation progress and
  candidates. Create File Disk seals a generated path, capacity, existing group,
  reserve and allocation mode. Accepted preparation/admission continues on the
  monitor. Pending/failed outcomes do not count as usable capacity.
- Join Candidates selects at most 16 eligible supplied media, each with an
  existing compatible bound DiskGroup and media class. File/device backend and
  HDD/SSD class are separate. Unverified identity, changed observations, missing
  mappings and insufficient capacity disable admission and explain why.
  General initialization consent and per-raw-device acknowledgement are explicit.
- Bulk Join submits at most four independent operations concurrently. Each row
  retains its operation and exact reviewed medium across response loss/retry;
  successful siblings are not resubmitted. Closing stops unsent submissions,
  while accepted work continues. Durable history pages contain at most 16 rows;
  the Node workspace retains at most 64 observations and reports partial coverage.
  Nonzero usable bytes fail without wiping or publishing disk membership.
- Legacy metadata-only disk editors retain geometry and uniqueness checks; they
  are separate from verified physical provisioning.
- Standard-node group menus open Create File Disk, while disk lifecycle links
  open Node storage. Direct legacy Up/Down/Delete controls are unavailable in
  that mode. Maintenance diagnostics remain available. A disk row with no observed
  provisioning receipt reports Unknown media rather than inferring a class from
  compatibility geometry.
- **DISK-03:** Distinguish physical location, DiskDB owner instance, and metadata
  Store/Group binding. Neither Node ID nor service ID implies ownership.
- Owner/status/binding operations are available only when supported and expose
  the resulting authoritative state. Container restrictions apply to all entry
  points, including context menus and direct API requests.

- **DISK-05:** Offline applies exclusion to the current owner and every configured
  DiskIO participant, then waits for I/O quiescence. Remove requires authoritative
  Offline and the exact revision/owner evidence. A lost reply is reconciled with
  Group 0 before showing success. Removed media appear as retained candidates.
  Rejoin restores their original identity and allocation metadata Offline, then
  explicit Online validates the retained format and current descriptor. No
  recovery migration, evacuation or physical file deletion is implied. Missing
  or replaced registered media remain quarantined; discovery creates no substitute.
- A retained receipt preserves its parent even after Remove. DiskGroup deletion
  requires no retained media, owner or metadata binding; empty membership alone
  does not permit a cascade. Confirmation explains the blocking prerequisites.

Storage scope: inventory without topology (DISK-01/02, CLU-05):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter               | Storage - N-1                              [Refresh]         | Properties                   |
|   v R-1                    | Capacity / Allocated / Free / Unknown coverage               | Storage / selected DG        |
|     v N-1                  | ----------------------------------------------------         | Physical Node N-1            |
|       > Services           | DiskGroup       Owner        Binding       Status            | DiskDB owner  DDB-1          |
|       v Storage *          | DG-7            DDB-1        S-0 / G-1      Online           | Binding       S-0 / G-1      |
|         > DG-7             | ...                                                          | Observed      <timestamp>    |
|         > Disk candidates  |                                   [Previous] [Next]          |                              |
|                            |                                                              |                              |
|                            | No topology canvas or reserved canvas space.                 |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

DiskGroup: Disk icon and ID enter the same Disk (NAV-03, DISK-03):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v N-1                      | DG-7                                      [Refresh]          | Properties                   |
|   v Storage                | Capacity / Allocated / Free                                  | DiskGroup DG-7               |
|     v DG-7 *               | ----------------------------------------------------         | Owner         DDB-1          |
|       Disk-a               | Disks                                                        | Binding       S-0 / G-1      |
|       Disk-b               |    +------------+       +------------+                       | Physical Node N-1            |
|                            |    : Disk icon  :       : Disk icon  :                       | Status        Online         |
|                            |    : usage      :       : usage      :                       |                              |
|                            |    +------------+       +------------+                       |                              |
|                            |        Disk-a               Disk-b                           |                              |
|                            |        Online               Offline                          |                              |
|                            |                                                              |                              |
|                            | Click icon or ID -> that Disk workspace                      |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

The same view opens through DDB-1 or Storage. Supported storage lifecycle
operations remain in resource menus with the mode restrictions in §14.

## 15. Capacity

- **CAP-01:** Datacenter/Rack/Node/Storage/DiskGroup/Disk selections show relevant
  capacity, used/free allocation and disks. Observation time belongs with the
  affected detail, not in a global strip. Missing statistics are not
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
  Bitmap counts and usage identify their scope, observation and denominator.
  Partial windows or aggregation cannot imply that the whole Zone is full.
  Comparable allocation counts reconcile with usage; differing scopes or times
  are explicit. Allocation, reserved/preallocated space, logical data bytes and
  backing-file usage are distinct measurements. Initial system allocations are
  attributable to their actual sources, not described as user data by inference.
- Hover/selection provides exact block index and offset. Large bitmaps use
  bounded drawing/tiling or labelled aggregation; an aggregate is not one block.
- **CAP-05:** Refresh bitmap, scan/recalc, compact/rebuild, and status operations
  identify their scope and show progress/results. RPC or owner lookup failure
  remains visible at the affected zone and never becomes an empty/free bitmap.

- **CAP-06:** Opening Cluster does not issue DiskDB runtime usage or scan
  requests. Capacity usage is scoped to the selected DiskGroup/Disk when that
  scope is known. Cluster/Rack overviews use cached reports until an explicit
  Refresh requests a runtime snapshot; opening an overview is not that request.
  Cached candidate/setup summaries are bounded to 64 nodes and never initiate
  node-by-node discovery.
  Disk inventory belongs to one shared source. Expanding a Node's Storage branch loads its
  DiskGroups; expanding/selecting a DiskGroup loads its disks. Selecting a
  linked Disk loads the required ancestor inventory directly. Four workers
  serve opened branches, merging duplicate requests for the same branch.
  Refresh revisits requested branches and never traverses unopened disks.
  Failed branch refresh retains its known inventory and reports failure.
- **CAP-07:** The physical tree reads Node health from the service projection;
  it does not ping every Node or fetch a second per-node KV catalog. Capacity
  Storage branches and DiskGroups begin collapsed; Datacenter/Racks/Nodes and
  Services remain visible.
  Default-service prerequisite inspection is explicit for its selected Node.

Disk and inline Zone bitmap (CAP-02–05):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v N-1                      | Disk-a                                    [Refresh]          | Properties                   |
|   v Storage                | Capacity / Allocated / Free                                  | Selected allocation block    |
|     v DG-7                 | Zones: current bounded window                                | Disk          Disk-a         |
|       Disk-a *             | Zone        Allocated      Free         Usage                | Zone          zone-1         |
|       Disk-b               | zone-0      ...            ...          ...                  | Index         17             |
|                            | zone-1 *    ...            ...          ...                  | State         Free           |
| Tree stops at Disk.        |                             [Previous zones] [Next]          | Unit          <observed>     |
|                            | ----------------------------------------------------         | Zone offset   <exact bytes>  |
|                            | zone-1: block window, not the entire Zone                    | Disk offset   <exact bytes>  |
|                            | BBBBGGGGGG??  B=used/blue G=free/green ?=unknown             | Observed      <timestamp>    |
|                            | BBBBGGGGGG??  Selected block: 17                             |                              |
|                            | Window counts / Zone totals / unit / geometry                |                              |
|                            |           [Previous blocks] [Next] [Go to block]             |                              |
|                            |                                                              |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Letters represent the bitmap colors. Rendering uses actual protocol bit order
and allocation geometry. Unknown/reserved data never becomes free. Free blocks
in another window are not visible in this one.

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

Collection and refresh states (ERR-01–04):

```text
Initial loading              Confirmed empty collection
+--------------------------+ +--------------------------------------------+
| Loading namespaces...    | | No namespaces.                             |
| No invented zero totals. | | Right-click Catalog to create a namespace. |
+--------------------------+ +--------------------------------------------+

Refresh failed               Mutation outcome unknown
+--------------------------+ +--------------------------------------------+
| Last observed rows       | | Target: exact resource                     |
| Stale - refresh failed   | | Operation: accepted ID                     |
| Cause / dependency       | | Outcome unknown - reconcile before retry   |
| [Retry]                  | | Inputs / last confirmed progress retained  |
+--------------------------+ +--------------------------------------------+
```

## 17. Fixed-cluster operator flow

1. Open Cluster directly. Inspect/create Rack and Nodes; add each Node through
   one dialog with six default services and explicit waiting dependencies.
2. In KV, initialize three-replica Group 0 and create ordinary data groups.
3. In Cluster, select Storage to register DiskGroups/disks with owners and bindings;
   DiskIO becomes ready. Enter Disk/Zone to inspect capacity and its bitmap.
4. Select a Datacenter/Rack/Node in Cluster to inspect topology and service readiness.
   Resolve logical group dependencies in PaxosKV and physical storage in Cluster.

This is a dependency guide, not a blocking wizard. A working capability remains
usable while another is unavailable. No domain asks the operator to reconnect.
Data-browser flows are covered by their domain sections below.

## 18. Acceptance scenarios and scope

Each scenario uses the contracts above; failures are recorded in the working
plan, never edited out of this specification to make a run pass.

- **A01 / entry:** open the fixed cluster; all seven tabs appear; no connection
  or access-key form; restoring a domain does not change cluster identity.
- **A02 / create:** with one Rack, create three Nodes using valid defaults;
  verify one instance of every requested type per Node, conflict-free ports,
  accepted operation progress, and safe retry after one injected service failure.
  Closing a standard setup dialog does not stop accepted monitor work.
- **A03 / lifecycle:** independently stop/start/restart an auxiliary instance
  from its Node menu; verify exact target, preserved data/configuration, updated
  PID, and readiness. All instance labels follow §4.
- **A04 / KV:** verify Group 0 and ordinary Group 1 with three replicas; select
  a group, inspect membership, and perform bounded exact/scan data operations.
  Add members to the same group sequentially, refreshing membership after
  each result; independent group preparation may run concurrently.
- **A05 / Capacity:** create/register DiskGroup and disk, inspect owner/binding;
  select a disk with more than 32 zones, page, select Zone, and verify inline
  bitmap, decode offsets, muted colors, and error handling for unavailable owner.
- **A10 / limits:** large topology, group/replica lists, disk/zone lists, and KV
  scans remain bounded. Observe request counts/bytes and rendered item counts;
  no hidden fetch-all, invented totals, or selection replacement on page changes.
- **A11 / failures:** inject unavailable owner/group, malformed API response, stale cursor,
  and slow/unknown mutation outcome; verify §16 and retry without duplication.
- **A12 / modes:** Single preview rejects physical topology/disk mutations through UI and API;
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
- **A13 / resource interaction:** parent context menu creates a supported child;
  selecting it reads details; its own context menu exposes supported lifecycle
  operations. Verify tree/list/property refresh, keyboard access, readonly and
  unknown capability/outcome states across Iceberg, Dataset and S3.
- **A14 / Dataset:** create/import, parent-linked Snapshots, paged Samples/Fields,
  explicit verified preview, Split revisions and supported lifecycle operations.
  No browser Session activity or default raw-JSON properties replace this content.

Out of scope: UI login/role design and new data-plane protocols.
Unsupported features are explicit rather than represented as working controls.

## 19. Chunk

- **CHK-01:** Left tree is Datacenter → Rack → Node → CDB and KV Server;
  KV branches expand Store → Group without Replicas. Chunk ownership has two
  independent maps over slots 0–1023: Serving assigns a ChunkDB instance;
  Storage assigns the metadata Store/Group. Chunk-KV ordered splits and payload
  disk placement are separate concepts. The left tree has no Serving ownership /
  Storage ownership wrapper or repeated footer. Ownership meaning belongs in
  selected-resource properties and the scoped center inspection. Details are
  requested only when a real ChunkDB or replica target
  exists. Unprovisioned selections do not send ownership probes.
- Selecting a CDB or Group displays its full 32×32 bitmap; parent selections
  display one combined bitmap per layer. Each cell is exactly one slot, ordered
  left-to-right then top-to-bottom. Owner colors remain stable across scopes;
  outside-scope slots are gray, unknown membership uses a pattern. Each map
  carries its own generation. Invalid or unavailable maps never become empty
  or gray observations; retained failed-refresh data is marked stale.
- Storage groups are deduplicated across replicas. Node/Rack scopes include a
  group when it has a replica in that scope. Unknown replica/service membership
  remains unknown. Legend hover highlights an owner's slots; clicking an owner
  navigates to its bitmap. Legend windows contain at most 12 owners. Selecting
  a cell shows exact slot, owner, generation, source and node associations;
  arrow keys navigate the grid. Service and storage boundaries need not match.
- Bitmap observations read a validated complete map and return exactly 1024
  lossless owner identities per layer. No owner-page stitching is needed.
- **CHK-02:** Entering Chunk requests an All-type page automatically. The center
  lists at most 10 chunks without an inner vertical scrollbar. Previous/Next
  replace the page. Type is a protocol-defined selector; exact Chunk ID lookup
  sits above the list in the center. Arbitrary ID-prefix/global filtering is not
  required. Source failures identify partial coverage rather than an empty list.
  Datacenter and narrower lists expose Type, Status, Strips, Size and Sealed.
  Status uses the observed lifecycle state; Size reports Chunk capacity and Sealed
  reports sealed length, both with explicit units matching the detailed metadata.
  Sealed length is not a boolean, nor is Chunk capacity physical used space.
  Unknown fields are explicit; valid zero lengths/counts remain visible.
  List and detail refer to the same Chunk identity.
  Aggregation deduplicates authoritative Chunk identities rather than counting
  replicas as separate chunks. Window counts are not global totals. System and
  user-data types remain distinct; a large initial count alone proves no leak.
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

Chunk: scope, ownership inspection and list fields (CHK-01/02):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter *             | Chunk ownership - datacenter                                 | Properties                   |
|   v R-1                    | Serving slots [32x32 map]           [Refresh slots]          | Selected resource / slot     |
|     v N-1                  | Storage slots [32x32 map]           [Refresh slots]          | Full identity                |
|       CDB-1                | ----------------------------------------------------         | Owner / binding              |
|       v PKV-1              | Chunks   Type [All v]               [Refresh chunks]         | Generation                   |
|         v S-0              | Exact Chunk ID [...................] [Lookup]                | Observation / coverage       |
|           G-1              | ID    Type    Status  Strips  Size(KiB)  Sealed(KiB)         | ...                          |
|                            |                                                              |                              |
| No ownership footers.      | ...    WAL      Active  ...     ...       ...                |                              |
|                            | ...    S3       Sealed  ...     ...       ...                |                              |
|                            | Coverage / scanned count          [Previous] [Next]          |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Chunk: selected Strip and block placement (CHK-03–05):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter               | Chunk list stays above its selected detail.                  | Properties                   |
|   v R-1                    | ----------------------------------------------------         | Mirror 1                     |
|     v N-1                  | Chunk <full ID> / type / state / capacity                    | Node          N-1            |
|       CDB-1                | Strip 4 Mirror                                               | DiskGroup     DG-7           |
|                            | [0 MiB, 16 MiB)                                              | Disk          Disk-a [link]  |
|                            |   [Mirror 1 *] [Mirror 2] [Mirror 3]                         | Zone          zone-1         |
|                            | Strip 5 EC 4+2                                               | Offset        <exact bytes>  |
|                            | [16 MiB, 32 MiB)                                             | Length        <exact bytes>  |
|                            |   [Data 1] [Data 2] [Data 3] [Data 4] [P1] [P2]              | Protection    <observed>     |
|                            |                                                              |                              |
|                            | Select block -> Properties                                   |                              |
|                            | Disk link -> Cluster/Disk; Back restores this page           |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

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
- **CKV-05:** Page inspection follows actual inner-child indices from the root,
  with at most 32 child steps, one 1 MiB base frame and 20 entries per window.
  It may load cold structural pages, but never flushes, checkpoints, scans live
  KV records or follows overflow values. Physical base records, inline deltas
  and the count of external delta pages remain explicit; pending writes are not
  presented as part of this base-page view. Keys and inline values have bounded
  256-byte previews with exact lengths and truncation markers. Version fences
  protect paths; page fingerprints additionally protect entry continuations.
  Changed observations require an explicit root refresh. History retains path,
  fences, offset, selected entry and byte format, then revalidates on return.

- **CKV-06:** Split properties, Overview and graph cards do not display Weight.
  Full Range bounds remain visible and wrap independently.
- **CKV-07:** Weighted balance explanations and data/count contribution values
  remain hidden until tree statistics can describe current range data reliably.
  No tree statistics scan is triggered by opening or refreshing the console.
- **CKV-08:** Count-based placement remains a backend policy. Existing bounded
  observations retain catalog/epoch freshness fences, but are not presented as
  data weights. Browser assertions verify that Weight and weighted explanations
  are absent while tree/page inspection and range navigation remain functional.

ChunkKV: distribution graph and selected Split (CKV-01–08):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v datacenter               | ChunkKV range distribution         [Refresh catalog]         | Properties                   |
|   v R-1                    | Catalog generation / page / loaded scope                     | Split <full ID>              |
|     v N-1                  |                  [ChunkKV]                                   | Range         [start,end)    |
|       v CKV-1              |                   /     \                                    | Owner         CKV-1          |
|         split-a *          |              [CKV-1]   [CKV-2]                               | Epoch         <observed>     |
|     v N-2                  |                 :         :                                  | Tree ID       <exact ID>     |
|       CKV-2                |             [split-a] [split-b]                              | Checkpoint    <observed>     |
|                            |                 :         :                                  | Selected extent / page       |
|                            |             [Tree ID] [Tree ID]                              | No Weight field              |
|                            | Graph window controls                      [Fit All]         |                              |
|                            | ----------------------------------------------------         |                              |
|                            | Selected split-a: [Tree] [Journal]                           |                              |
|                            | Bounded page / journal extent content                        |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Graph windows and catalog pages have separate continuation. Selecting a Tree
uses real bounded base-page inspection; the graph does not scan records for weights.

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
  Catalog root uses one Namespaces heading; Namespace uses its actual name.
  No Catalog breadcrumb/type/title stack duplicates the left tree. Namespace
  and Table lists page in the center with visible continuation controls.
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
- **ICE-06:** Catalog context menu creates Namespace; Namespace context menu
  creates Table and, when supported, child Namespace. A Namespace's own menu
  edits its supported properties or drops that empty Namespace. A Table's menu
  exposes supported Rename, Commit metadata and Drop table operations. Drop
  removes the catalog reference; physical reclamation follows service GC.
  Snapshots, Manifests and Files are inspected through table-managed metadata,
  not given arbitrary Create/Delete actions. Demo operations are not primary
  browsing controls. Properties describe only the selected identity.
- **ICE-07:** Reference windows contain at most 100 entries; Parquet windows
  contain 20 Row Groups and 12 columns. Paging replaces the visible window.
  Column widths represent compressed sizes and file spans retain exact byte
  identities. History restores the selected reference, footer/layout view and
  column selection, then revalidates the table head, snapshot and file identity.
  Cached branches do not bypass revalidation. A failed or stale inspection
  preserves selection context but removes data and continuation actions.

Iceberg: Catalog and Namespace collections (ICE-01/02/06):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v Catalog                  | analytics                                                    | Properties                   |
|   v analytics *            | Filter loaded tables [................]   [Refresh]          | Namespace analytics          |
|     > events               | ----------------------------------------------------         | Full name                    |
|     > users                | Name                  Type           Summary                 | Properties                   |
|   > sandbox                | events                Table          ...                     | ...                          |
|                            | users                 Table          ...                     |                              |
|                            | ...                                                          | No duplicate Catalog         |
|                            |                                   [Previous] [Next]          | path/type/title stack.       |
|                            |                                                              |                              |
|                            | Catalog selected -> title: Namespaces                        |                              |
|                            | Namespace selected -> title: actual name                     |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Iceberg context-menu targets:

```text
Catalog                    Namespace                  Table
+----------------------+  +------------------------+ +------------------------+
| Create namespace...  |  | Create table...        | | Rename...              |
+----------------------+  | Create namespace... (*)| | Commit metadata...     |
                          | Edit properties...     | | Drop table...          |
                          | Drop namespace...      | +------------------------+
                          +------------------------+
(*) Nested Namespace creation requires backend support.
```

Iceberg: Table, Snapshot and File inspection (ICE-02–05):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v Catalog                  | events                                    [Refresh]          | Properties                   |
|   v analytics              | [Overview] [Schema] [Files]                                  | Selected Table / reference   |
|     v events *             | Compact table summary                                        | Full identifier              |
|       v snapshot-42        | Snapshot     Time       Operation    Records   Files         | Schema / Snapshot identity   |
|         v manifest-a       | 42           ...        append       ...       ...           | Manifest List path           |
|           file.parquet     | ...                                                          | File format / size           |
|                            |                                   [Previous] [Next]          | Selected column metadata     |
|                            | ----------------------------------------------------         |                              |
|                            | Select Snapshot -> paged Manifests                           |                              |
|                            | Expand Manifest -> paged Files                               |                              |
|                            | Select Parquet -> footer, Row Groups, columns                |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Schema uses a structured field tree table. File inspection decodes metadata
and footer structure; it does not imply reading data pages or SQL querying.

## 22. S3

- **S3-01:** Left Tree is S3 → Bucket. A single namespace introduces no wrapper;
  the logical S3 root is not named Datacenter. Root center lists buckets with
  20-row windows; it does not repeat S3 as path, type and heading.
- **S3-02:** Selecting a Bucket displays an API page of at most 20 objects.
  Prefix changes reset cursor history; Previous/Next replace rows. Any bucket
  filter over already loaded metadata explicitly names that local scope.
- **S3-03:** Selecting an Object reads HEAD fields into properties: key, size,
  ETag, last modified, content type, supplied version and user metadata. Missing
  fields show `—`. The center presents the selected object's content inspection.
  Tree selection and Back/Forward restore the Bucket list and its cursor/prefix.
  Preview is a separate bounded operation, not an automatic payload GET.
- **S3-04:** S3 root context menu creates Bucket; a Bucket's menu uploads a child
  Object, opens multipart uploads or deletes that empty Bucket. An Object's own
  menu exposes Preview, Download, Replace and Delete. Object rows in the center
  use the same menu contract without adding every Object to the left tree.
  Prefix is a key filter, not a physical folder; Bucket deletion does not silently
  cascade. Demo controls are not primary browsing actions. Exact bucket/key is visible.
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

S3: Bucket object list (S3-01/02/04):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v S3                       | photos                                                       | Properties                   |
|   photos *                 | Prefix [...................] [Apply]       [Refresh]         | Bucket photos                |
|   exports                  | ----------------------------------------------------         | Name                         |
|   logs                     | Key                   Size             Modified              | ...                          |
|                            | cat.jpg               ...              ...                   |                              |
|                            | dog.jpg               ...              ...                   | Selecting an Object reads    |
|                            | ...                                                          | its HEAD and properties.     |
|                            |                                   [Previous] [Next]          |                              |
|                            |                                                              |                              |
|                            | Objects stay in this bounded list.                           |                              |
|                            | Right-click Object -> its own resource menu.                 |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

S3 context-menu targets:

```text
S3 root                Bucket                     Object list row / detail
+-------------------+ +-------------------------+ +------------------------+
| Create bucket...  | | Upload object...        | | Preview                |
+-------------------+ | Multipart uploads       | | Download               |
                      | Delete empty bucket...  | | Replace...             |
                      +-------------------------+ | Delete object...       |
                                                  +------------------------+
```

S3: Object inspection and multipart view (S3-03–08):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v S3                       | cat.jpg                                   [Refresh]          | Properties                   |
|   photos                   | Content inspection                                           | Object                       |
|   exports                  | Preview is read only after an explicit request.              | Bucket        photos         |
|                            | ----------------------------------------------------         | Key           cat.jpg        |
|                            | Storage locations                                            | Size          <bytes>        |
|                            | Extent    Object interval    Chunk ID (link)                 | ETag          <exact>        |
|                            | ...       [start,end)        ...                             | Content type  <observed>     |
|                            |                                   [Previous] [Next]          | Modified      <observed>     |
|                            |                                                              | User metadata                |
|                            | Bucket menu -> Multipart uploads view                        | Selected extent identity     |
|                            | Upload key / ID / state; select -> paged parts               |                              |
|                            | Upload menu -> Abort (when supported)                        |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Preview and Download are explicit reads from the resource menu. Selecting an
Object does not download its payload. Multipart inspection is a separate center
view; its state is not piled beneath the ordinary Object list.

## 23. Actions and properties

- **ACT-01:** The left tree owns resource navigation and context menus. Create
  child belongs to the parent menu; Delete belongs to the resource's own menu.
  Selection is Read; child collections are automatic paged lists in the center.
  There is no generic Update requirement. Additional operations use precise
  resource-specific names and exist only where that node's capabilities allow.
  A lifecycle operation such as Reclaim is not relabelled Delete.
- **ACT-02:** All domains share menu, dialog, button and property styles. The
  center has one scope heading, query/filter controls, Refresh and content, with
  pagination below its list. It has no duplicate CRUD toolbar, collapsed Actions
  strip, inline creation forms or repeated path/type/title stack. Query controls
  and bounded Preview/Download are distinct from resource mutations.
  Ordinary buttons have visible borders; destructive menu items use muted red.
  Readonly, deployment mode, backend capability and lifecycle restrictions apply
  to every entry point. Unknown capability does not enable writes.
- **ACT-03:** Right properties show complete information for the clicked item,
  including graph blocks/columns/extents. Optional diagnostics never replace
  primary details. Metrics panels, entries and polling are excluded from the UI. Future metrics
  publishing targets customer-managed time-series systems; it is outside this
  UI contract.
- **ACT-04:** Tree labels, icons and ordinary resource rows activate the same
  identity. Expansion chevrons only expand. List-only resources use the same
  context menu on their center row; a focused detail retains access to that exact
  resource menu. Right-click targets the clicked resource, independent of prior
  selection. Menu key / Shift+F10 opens the same menu; Escape restores focus.
  Matrix cells and diagram blocks retain their documented inspection semantics.
- **ACT-05:** Menus open shared dialogs; long imports use step dialogs with the
  same controls. The target stays explicit and cannot change under an open form.
  Switching resources closes obsolete forms without claiming to cancel accepted
  work. Successful operations refresh affected lists, tree references and properties;
  failures retain input and unknown outcomes require reconciliation.
- **ACT-06:** Empty collections explain how to create a supported child from
  the parent menu. Empty, loading, unavailable and denied states are distinct.
  Browser Session activity is absent from domain workspaces. Operation feedback
  stays with its flow; resource history such as Snapshots and publication receipts
  remains available. Default properties are labelled structured fields, not JSON.

The common layout and menu targets are:

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| Resource tree              | Current scope                                                | Properties                   |
| v Parent                   | Filter [.............]    [Refresh]                          | Selected identity            |
|   Child                    | -------------------------------------                        | Full path / ID               |
|                            | Child name       Type / summary                              | Structured fields            |
|                            | ...                                                          | ...                          |
|                            |                  [Previous] [Next]                           |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
+----------------------------+--------------------------------------------------------------+------------------------------+
+----------------------------+--------------------------------------------------------------+------------------------------+
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Domain-specific scope and menu mappings:

```text
Iceberg                         Center on selection       Context menu
Catalog                         Namespaces (paged)        Create namespace
  Namespace                     Tables/children (paged)   Create table; Drop namespace
    Table                       Overview/Schema/Files     Rename; Commit metadata; Drop
      Snapshot -> Manifest      Paged metadata/files      Inspection only
        File                    Decoded metadata          Supported inspection

Dataset                         Center on selection       Context menu
Datasets                        Datasets (paged)          Create dataset
  Dataset                       Snapshots (paged)         Import / New Snapshot
    Snapshot                    Samples x Fields          Create child; Publish Splits;
                                                          Stable/Retain/Release/Reclaim
      Splits                    Revision/member windows   Supported revision operations
  Sample/Field/Cell in center    Selected scope/value      Supported authoring/preview

S3                              Center on selection       Context menu
S3                              Buckets (paged)           Create bucket
  Bucket                        Objects (paged)           Upload; Multipart; Delete empty
  Object in center              Content inspection        Preview; Download; Replace; Delete
```

These mappings do not imply every listed action is available for every state.
Immutable references and backend-owned internal structures do not acquire generic
CRUD merely to match another domain. Dataset lifecycle support is defined in §25.

## 24. E2E organization and timing

- **TEST-02:** Page-function specs retain each behavior once: shell `0x`,
  physical lifecycle `1x`, logical KV `2x`, KV data `3x`, inspector/canvas `4x`,
  Capacity/Chunk/Chunk-KV `5x`, Iceberg `6x`, S3 `7x`, Dataset `8x`, cross-function `9x`.
  Dedicated creation/reconfiguration specs retain their UI mutations; a smoke
  chain does not repeat every dialog or failure permutation.
- **TEST-03:** Use separate unit, backend integration and native browser E2E
  layers. Browser fixtures provision actual services, metadata and files;
  outages use actual service lifecycle. Deployment and container capability
  enforcement require real API acceptance. Share setup per spec and keep
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


## 25. Dataset

Depends on: [Dataset control lifecycle](../access-server/dataset/design-dataset-control.md).

- **DS-01 — Scope:** Dataset is immediately before S3. The fixed `dataset-ns`
  namespace is a property, not a single-child navigation level. Catalog discovery
  comes from the configured authority, including SDK-created identities. Empty,
  missing configuration, unavailable service and denied access remain distinct.
  Credentials stay on the Console server; no browser endpoint or bearer editor.
  The root center automatically lists Datasets. Root context menu creates a
  Dataset; a Dataset's menu starts Import / New Snapshot. All use §23; there is
  no Session activity region. Properties render named fields rather than JSON.
- **DS-02 — Relationships:** left navigation follows declared parent links as a
  forest. Roots and each parent's children page independently; no ancestry scan
  or Snapshot-ID sorting reconstructs the tree. Latest and Stable are reference
  badges. A Snapshot's Splits group is a peer of its children. Samples and Fields
  remain in the center. Direct Snapshot navigation preserves the exact identity;
  bounded ancestor discovery reports incomplete paths explicitly.
  Dataset and Snapshot inventory pagination is reachable in the center, independent
  of left-tree continuation. Tree appearance and selection use shared conventions.
- **DS-03 — Combined inspection:** one Samples matrix uses Schema definitions as
  columns, defaulting to twenty rows and twelve fields. Required/optional is the
  actual field definition; no inferred type, MIME or nested schema. Header, Sample
  row and Sample/Field cell selections have separate properties and history.
  Missing, tombstoned, inline, payload-backed and inherited values are distinct.
  Known lengths and declared checksums are metadata, not verified payloads.
- **DS-04 — Bounds:** each catalog/row/column query carries its immutable scope and
  explicit continuation. A stale continuation requires first-page refresh. Loaded
  row filtering is labelled local. Opaque binary Sample IDs retain canonical padded
  base64; valid UTF-8 is only a display/helper encoding. Decimal u64 values remain
  strings. Navigation retains bounded identities/cursors and scroll, not payloads.
  Hidden work stops; late responses cannot replace another scope.
- **DS-05 — Preview:** only the explicit Preview action reads a selected field.
  Complete verified values of at most 64 KiB render as UTF-8 or hex. Unknown length,
  missing/tombstoned values, oversized data, integrity failures and cancelled reads
  never appear as a successful truncated value. Browsing starts no Read Plan.
- **DS-06 — Local import:** create Dataset, then Import / New Snapshot or Create
  child. Any complete parent may be selected. Preserve all parent data is the
  default; Replace membership and authority-evaluated Selection are explicit.
  A Split source freezes Snapshot/revision/name. Schema/member edits, local file
  mappings and Split policies are reviewed before publication. External sources
  are SDK-owned and require no connector UI.
- **DS-07 — Client-owned publication:** uploads use immutable parts of at most
  4 MiB, at most 10000 parts, incremental MD5 and two concurrent requests. A new
  upload is created directly; a saved upload is observed for whole-part retries.
  Definitively aborted uploads use a new identity. Completed fields carry their
  existing id/length/md5 descriptor without a lookup to reconstruct it.
  Review precedes one complete Snapshot preparation, followed by an explicit head
  update. A complete result is immutable and readable by ID before publication;
  preparing it writes no editing or evaluation progress. The browser freezes
  parent/source/Split identities and the intended head base before uploading.
  IndexedDB saves one complete input and pending request per Dataset/API origin
  before mutation, with a 10 MiB record bound and no duplicated preparation body.
  Concurrent tabs cannot overwrite another saved resource. Closing and reload
  preserve these client inputs; unfinished local Files must be reselected and
  match their digest. Unknown replies retry the same resource, complete input and
  original head conditions, without draft/status/commit queries. Head conflicts
  retain the returned observation for explicit review and a deliberate conditional
  retry; the client never silently rebases. Discarding local input does not delete
  a complete result or its payload owners. Snapshot preparation accepts at most
  8 MiB through the proxy; upload parts retain their 4 MiB bound.
- **DS-08 — Split revisions:** author the complete set, including an empty set,
  with named Selection or deterministic weighted partitions and independent overlap
  and coverage policies. Inherited membership is frozen and intersected with child
  membership; new Samples remain unassigned. Explicit reevaluation is separate.
  Independent Split publication compares the expected revision and head generation, preserves historical
  members, and does not move Dataset latest or create a data Snapshot. History and
  named membership pages stay pinned to the selected immutable revision.
- **DS-09 — Lifecycle:** stable, retain, release and reclaim name the exact
  Snapshot. Backend protection wins over an observed eligibility hint. Reclaim
  distinguishes unknown, pending logical cleanup and completed retirement; physical
  cleanup may remain deferred. No Dataset delete or Split-history purge action.
  Uniform creation/reading/menu placement does not imply unsupported deletion:
  whole-Dataset deletion requires its own authoritative lifecycle capability.
  Published Sample/Field changes produce a new Snapshot rather than modifying
  the immutable selected one in place.
- **DS-10 — Modes:** `modules.dataset=false` hides the domain and prevents Dataset
  observation. API prefixes apply to every request. Readonly disables editing and
  the Console API rejects direct writes; exact matrix POST remains an observation.
  Managed mode permits supported data operations without enabling topology changes.

Dataset: catalog and Snapshot inventory (DS-01/02/06):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v Datasets                 | images                                    [Refresh]          | Properties                   |
|   v images *               | Published Snapshots                                          | Dataset images               |
|     v snapshot-a           | Snapshot      Parent        Samples       References         | Dataset ID    <exact ID>     |
|       > Splits             | snapshot-a    -             ...           Stable             | Namespace     dataset-ns     |
|       > snapshot-b         | snapshot-b    snapshot-a    ...           Latest             | Latest        snapshot-b     |
|   > audio                  | ...                                                          | Stable        snapshot-a     |
|                            |                                   [Previous] [Next]          | ...                          |
|                            |                                                              |                              |
|                            | Select Datasets root -> paged Dataset list                   |                              |
|                            | Select Snapshot -> Samples x Fields                          |                              |
|                            | No Session activity region.                                  |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Dataset context-menu targets and immutable editing:

```text
Datasets root            Dataset images                Snapshot
+--------------------+  +---------------------------+ +-------------------------+
| Create dataset...  |  | Import / New Snapshot...  | | Create child...         |
+--------------------+  +---------------------------+ | Publish Splits...       |
                                                     | Set stable              |
Whole-Dataset Delete is unavailable without its       | Retain / Release        |
authoritative lifecycle capability (DS-09).           | Reclaim...              |
                                                     +-------------------------+
Sample/Field edits -> reviewed candidate -> new published Snapshot
Published Snapshot data are never edited in place.
```

Dataset: Samples matrix and selected cell (DS-03–05/08):

```text
+----------------------------+--------------------------------------------------------------+------------------------------+
| v Datasets                 | snapshot-b                                [Refresh]          | Properties                   |
|   v images                 | Parent / Latest / Stable / inspection state                  | Selected Sample/Field cell   |
|     v snapshot-a           | ----------------------------------------------------         | Sample ID     <base64>       |
|       > Splits             | Sample           image          label        ...             | Field         image          |
|       v snapshot-b *       | sample-001       <payload>      cat                          | Required      <definition>   |
|         v Splits           | sample-002       <payload> *    dog                          | Storage       Payload        |
|           train            | ...                                                          | Length        <observed>     |
|           validation       |                    [Previous rows] [Next rows]               | Checksum      <declared>     |
|                            |                    [Previous fields] [Next fields]           | [Preview field]              |
|                            |                                                              | Verified complete value      |
|                            | Select Split -> pinned revision member window                | or explicit unavailable      |
|                            | Select column / row / cell -> matching properties            |                              |
+----------------------------+--------------------------------------------------------------+------------------------------+
```

Preview is explicit and bounded to a complete verified value of at most 64 KiB.
Snapshot/revision/member history remains business data, independently of the
removed browser Session activity.

Local batch mapping uses version 1 JSON aligned with Sample/Field edit models,
bounded at 8 MiB and 10000 Sample edits. `schema: null` inherits the parent Schema.
IDs and inline bytes are canonical base64. File names identify exactly one selected
local File; they never grant filesystem access. Ambiguous/missing files, duplicate
Samples/fields and invalid encodings are rejected before upload. A single-Sample
helper can create this mapping from a UTF-8 ID and text or a selected local file.

```json
{
  "version": 1,
  "schema": {"version": 1, "fields": [{"name": "text", "required": true}]},
  "samples": [
    {"kind": "upsert", "id": "c2FtcGxlLTE=", "fields": [
      {"name": "text", "value": {"kind": "inline", "data": "aGVsbG8="}}
    ]}
  ]
}
```

A local payload value is `{"kind":"file","file":"selected.bin"}`. An optional
field tombstone is `{"kind":"tombstone"}`. Removal is
`{"kind":"remove","id":"c2FtcGxlLTE="}`. Authority composition combines these
bounded edits with the frozen source; the browser never constructs a partial
Manifest and labels it complete membership.
