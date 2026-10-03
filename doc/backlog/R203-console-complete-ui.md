<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R203: console — Complete Web UI and Container Operations Interface

#### Problem

- The current top-level Web tabs are Cluster, KV, and Capacity. Cluster shows
  the physical layout; KV provides logical resource management and key-value
  operations; Capacity provides DiskDB capacity operations.
- The Chunk subpage under Capacity still reuses the topology canvas. It lacks a
  complete Chunk list, Strip composition, and actual physical placement views.
  Iceberg and S3 have no corresponding top-level operations pages.
- Container currently opens a separate `ManagedPreview` and cannot use the full
  Console. Container should provide the same data operations interface, while
  deployment configuration and crowdb-monitor manage physical topology and
  process deployment. Users must not change these through the Web UI.
- Initial standalone Web bootstrap must not require Group 0 to exist first.
  Users should be able to add Racks and Nodes and deploy Servers from an empty
  UI, then initialize Group 0 in KV. Earlier removal of local temporary
  configuration and recovery entry points broke this flow. The fixes on the
  current branch have not yet been fully verified; this requirement must not
  treat them as accepted persistence capabilities.
- Concrete scenarios: identify the storage resources used by an Iceberg/S3
  request; inspect the Node, DiskGroup, and Disk holding a Chunk's Mirror/EC
  Strips; continue managing the original cluster after restarting Web; perform
  data CRUD in Container without changing physical deployment.
- Root designs: [Console architecture](../design/console/design-crowdb-console.md)
  and [Console UI](../design/console/design-crowdb-console-ui.md). This requirement
  revises the standalone Web bootstrap boundary while retaining Group 0 as the
  authority for initialized configuration in Container.

#### Solution

##### 1. Shared Shell and Information Architecture

- The authoritative behavior contract is the seven-domain
  [Console UI specification](../design/console/design-crowdb-console-ui.md).
  Its shared tree, resizable panes, collapsed Actions, exact identity, byte
  presentation, replacement pagination and domain-specific content take
  precedence over earlier interaction sketches in this requirement.
- Seven domains: Cluster, KV, Capacity, Chunk, Chunk-KV, Iceberg, S3. One fixed
  cluster and server-held root credentials; container capabilities still reject
  topology/process and disk-management mutations in the backend.
- Mutations report authoritative results; stale, partial and unavailable results
  remain explicit. Activity is session history, not a durable audit log.
- Bounded windows use their specified sizes: KV/S3 20, Chunk 10, Capacity zones
  32. No unbounded append or hidden fetch-all for counts/filters.

##### 2. Startup, Configuration Authority, and Deployment Capabilities

- Standalone Web starts without arguments in a fixed `default` working directory.
  Its initial state is empty: no automatic Rack, Node, Server, or Group 0 creation,
  and no automatic sample cluster deployment.
- Before Group 0 is created, UI configuration changes are atomically persisted
  to a temporary configuration file in the working directory. Local launch
  information and data directories remain stable. Restart reads the same
  directory and restores configured Servers. Identify surviving managed processes
  again; do not start duplicates or control a process solely by an old PID.
- KV Init creates Group 0 on deployed, reachable KV nodes and confirms that
  hardware and logical configuration has been written to Group 0. Preserve a
  recoverable initialization intent; do not delete the only bootstrap information
  before success is confirmed.
- After initialization, Group 0 becomes the cluster configuration authority.
  Local information serves only as connection hints, private credential
  references, and process launch/recovery inputs. Local caches cannot override
  Group 0 or become a writable fallback topology while it is unreachable. On
  restart, restore Servers/Group 0 first, then read its confirmed information.
- The Header shows the appropriate state from `Empty / Configuring / Initializing /
  Ready / Degraded / Unavailable`. Reachable Web without Group 0 is not a backend
  failure. Partial service failures affect only related features. When an
  initialized Group 0 is unreachable, show configuration as unavailable, disable
  dependent mutations, and retain diagnostics and managed recovery operations.
- Container provides the same seven tabs and resource views. Disable Rack, Node,
  and Server deployment/deletion; process start/stop/restart; and DiskGroup/Disk
  addition, deletion, movement, and status changes. Do not allow manual Init/Reset
  of a cluster already managed by Container.
- Container may allow Store/Group/Replica management, user KV, Iceberg, and S3
  data operations when authorized by the authenticated role, along with storage
  runtime maintenance explicitly supported by the deployment profile. The server
  still validates replication/EC limits for single-node profiles; the UI cannot
  bypass them.
- Expose permissions as capabilities: topology mutation, process management,
  logical management, data read/write, and runtime maintenance. Page-wide
  `readonly` still disables all writes, but cannot replace Container's per-domain
  permissions. Hidden/disabled buttons are presentation only; the backend must
  also reject unauthorized requests.

```text
first standalone start
         |
         v
default directory -> empty UI -> Rack/Node -> deploy Servers
         ^                                      |
         |                                      v
         +---------- durable temporary config <-+
                                                |
                                             KV Init
                                                v
                         sealed intent -> create Group 0 -> publish config
                                                |
                                                v
restart -> local launch hints -> restore Servers/Group 0 -> confirmed config
                                                |
                          Group 0 unavailable --+--> diagnostics, no fallback writes
```

##### 3. Cluster: Physical Resources and Server Lifecycle

- Left panel: Datacenter → Rack → Node → Server. Show Servers by type: KV, DiskDB,
  DiskIO, ChunkDB, Chunk-KV, and Access Server, displaying only types actually
  supported/deployed. DiskGroups/Disks assigned to DiskDB may appear under that
  Server; manage unassigned resources in Capacity. Server names include type,
  instance identity, and status to avoid ambiguity.
- Central panel: physical hierarchy layout. Each Node card lists its Servers
  and their status; Racks contain Nodes. The diagram is a navigation and deployment
  status projection, not a representation of KV replication or Chunk Strip
  relationships.
- Operations: Add Rack, Add Node; Deploy Server on a Node with type selection and
  launch parameter validation; Restart/Stop/Delete on a Server; entity Details
  and links to associated resources. Deletion must state its impact explicitly.
  Container hides physical mutations and explains that deployment is managed.
- Group 0 initialization belongs to KV. Cluster may suggest the next step and
  link to KV Init, but must not duplicate initialization logic. Server types
  without complete deployment support show capability information, without
  fabricated operation entry points.

```text
+-------------------+---------------------------------------+----------------+
| CLUSTER           | Rack A                                | Node 1         |
| DC                | +--------------+  +--------------+    | host / rack    |
|  Rack A           | | Node 1       |  | Node 2       |    | service status |
|   Node 1          | | KV       Up  |  | KV       Up  |    | deployment     |
|    KV             | | DiskDB   Up  |  | DiskDB   Up  |    | inputs / links |
|    DiskDB         | | ChunkDB  Up  |  | DiskIO   Up  |    |                |
|    ChunkDB        | +--------------+  +--------------+    | Activity       |
|   Node 2          | [Add Rack] [Add Node] [Deploy Server] |                |
+-------------------+---------------------------------------+----------------+
```

##### 4. KV: Initialization, Logical Resources, and Key-Value CRUD

- Implement Console UI §12: Store → Group → Replica; Group selection opens
  data directly, 20-row replacement pages, exact selected bytes, shared top
  Actions and read-only system Group 0. Preserve membership management and
  per-group scan cursors; binary keys must not become lossy mutation targets.
- Initialization remains in KV, using reachable deployed nodes and durable
  bootstrap intent. Cluster provides deployment and links to Init.

##### 5. Capacity, Chunk, and Chunk-KV Inspection

- Implement Console UI §§14–15 and §§19–20. Capacity owns disk lifecycle and
  inline 32-zone bitmap windows. Chunk lists real records and compact Strip /
  block layouts; service slots and storage slots are independent authorities.
- Chunk-KV presents a bounded collapsible diagram and scoped Tree/Journal
  inspection. Journal fences are not KV Pages. Missing page-inspection support
  must be explicit until the bounded management interface is available.
- Rack diversity is preferred, not mandatory when distinct Nodes satisfy the
  protection policy. No raw layout rewriting/deletion entry points.

##### 6. Iceberg: Reference Tree and File Inspection

- Implement Console UI §21: Catalog → Namespace → Table → Snapshot → Manifest
  → File, lazy independent manifest/file pages, expanded schema tree table and
  selected properties. Manifest List metadata remains inspectable in properties.
- Footer/Row Group/column inspection reads metadata only. Actions follow the
  exact selected resource. Root credentials stay on the server.

##### 7. S3: Bucket and Object CRUD

- Implement Console UI §22: S3 → Bucket; paged buckets/objects, HEAD details,
  explicit bounded preview, streaming transfers and multipart outcome states.
- Add authorized bounded ObjectRecord location inspection and Chunk links.
  Continuations pin object generation; 64-bit offsets remain exact. No payload
  read or recursive Chunk-placement fanout to obtain metadata locations.
- Cross-domain return restores source selection, pagination, expansion and
  scroll. Do not confuse ancestry breadcrumbs with navigation history.

##### 8. Service Boundaries, Credentials, and Capability Gaps

- Browser access consistently uses crowdb-web's API prefix. KV, ChunkDB, and
  DiskDB queries reuse `crowdb-console-shared` and their typed clients. Iceberg/S3
  use the Access Server's real protocols, without directly modifying underlying
  KV/Chunk metadata.
- Access endpoints come from confirmed service discovery or validated deployment
  inputs. Manage Iceberg Catalog and S3 Bucket scopes separately. Switching
  endpoints/scopes cancels old requests and clears data no longer authorized;
  responses from cluster A must not appear on cluster B's page.
- UI login/connection status shows effective capabilities. S3 signing credentials
  and Iceberg read/write/management roles remain separate under existing protocols;
  the Console assumes root until UI login is implemented and supplies deployment
  credentials from the server. No access-key, Catalog-token or management-token
  form is required. Secrets are not returned to the browser or stored in plaintext
  in Group 0. Container mode rejects topology, deployment and Capacity disk
  management writes while allowing root logical and data operations.
- The backend supplies available capabilities and limits; the frontend cannot
  infer them solely from domain or deployment strings. Apply `readonly`, Container
  hardware read-only restrictions, the current authenticated role, and protocol/profile
  restrictions together.
- New Chunk list/detail, placement lookup, Access proxy, and capability APIs needed
  by the UI are deliverables of this requirement. Where existing interfaces are
  insufficient, add typed adapters or bounded query interfaces; do not retain
  “complete” panels that can only show fabricated data.
- This requirement does not change ChunkDB's partition model, Mirror/EC algorithms,
  native data protocols, GC, or permission role meanings. The UI must explicitly
  show existing capability gaps.

Work items:

1. Complete the standalone startup, temporary configuration, initialization, and
   recovery contracts in `app/crowdb-web/src/main.rs`, `state.rs`, `standalone/`,
   and `mgmt/cluster_init.rs`. Keep deployment input boundaries explicit in
   `config/web.rs` and the Container managed router.
2. Implement the five-domain shell, capability controls, logical KV tree, empty
   states, and navigation across domains in `ui/src/App.tsx`, `contexts/`, `shell/`,
   and `views/`. Reduce accumulated domain logic in the root component.
3. Connect actually supported Server lifecycle operations in
   `lib/crowdb-console-shared/src/ops/` and Web domain routes. Share launch/recovery
   inputs and results instead of creating separate business logic for Container.
4. Complete Chunk queries, actual prefix grouping, Strip views, and placement
   views in the ChunkDB client, new Web Chunk domain routes,
   `ui/src/views/ChunkView.tsx`, and domain components.
5. Implement the listed protocol operations, streaming, credentials/scopes, and
   capability error handling in the Web Access domain adapters, `ui/src/api.ts`,
   new Iceberg/S3 views, and domain components.
6. Route the `ManagedPreview` mode branch into the shared UI, reuse Group 0/monitor
   projections, enforce Container permissions in both backend and frontend, and
   update Container Web acceptance tests.
7. Maintain domain types, unit/integration/browser tests using real backends, and
   permanent Console/UI designs. Leave implementation order, detailed file
   decomposition, and interface signatures to the working plan.

#### Dependencies

- Incoming dependencies: existing Console, KV CRUD/membership management, DiskDB
  capacity APIs, ChunkDB ListChunks and chunk/strip records, Group 0 service
  discovery, native Iceberg REST Catalog and FileIO, basic S3/multipart, and
  Container profile/monitor.
- Named artifacts: [ChunkDB model](../design/chunkdb/design-crowdb-chunkdb.md),
  [Iceberg contract](../design/access-server/iceberge/design-crowdb-iceberg.md),
  [S3 contract](../design/access-server/s3/design-crowdb-access-s3.md),
  `container/single-node-container/profile.toml`, and `ui/e2e/`.
- R96 was originally a placeholder for ChunkDB Console/CLI. This requirement owns
  the complete Web interface, including the Chunk subpage. R96 retains the CLI
  scope and shared operation capabilities to avoid duplicate implementation.
- R202's ChunkDB partition design does not block read-only browsing. Current routed
  queries must respect implemented ownership and later adapt to new partition
  interfaces, without assuming a future partition layout now.
- R193 does not block displaying existing Mirror/EC. The UI shows actual limits
  of the current profile and does not expose unimplemented failure-budget settings.
- Until R194 is complete, do not promise Iceberg FileIO listing across all prefixes.
  Files uses supported Table-associated references and supplies an explicit
  capability state when no paginated source exists.
- Incomplete R198/R199 do not block basic S3 CRUD; do not expose copy/batch deletion.
  While physical reclamation in R168/R169/R147 remains incomplete, successful
  deletion means only the logical deletion promised by the protocol, not reclaimed
  space. Incomplete engine integration in R189 cannot serve as the basis for row CRUD.
- Outgoing dependencies: this requirement establishes shared UI/capability/domain
  API contracts for future access protocol features and physical diagnostics.
  Those follow-up requirements need not land first.

#### Acceptance

- **A1 / Startup authority**: Empty default directory and no Group 0 → start Web
  without configuration → show Empty, allow Add Rack/Node, perform no automatic
  deployment, and show no backend-unreachable warning. E2E test
- **A2 / Persistence before initialization**: Add Rack and Node and deploy a KV
  Server without Init → terminate and restart Web → retain configuration, ports,
  and data directories; identify live processes again, restore stopped auto-start
  Servers, and avoid duplicate deployment. Integration test
- **A3 / Atomic configuration**: Readable configuration exists; inject a write
  failure/interruption → write and restart → read only a complete old or new
  version, report errors, and never overwrite a corrupt file with empty
  configuration. Integration test
- **A4 / Recoverable initialization**: Available KV nodes; initialization is
  interrupted during creation or publication → restart/retry → preserve intent
  and member identities, and declare Ready only after all configuration is
  confirmed written to Group 0. Integration test
- **A5 / Recovery of Group 0 authority**: Initialize and write user KV, then tamper
  with the local topology cache → stop and restart Web/Servers → restore Servers
  and Group 0, display Group 0 configuration, and read original KV data; do not
  enable local cache writes when Group 0 is unreachable. Integration test
- **A6 / Three panels and selection across domains**: Various entities exist →
  select in tree/diagram, switch domains, and navigate across domains → show the
  correct Inspector identity, expand targets, explicitly report missing targets,
  and retain no entity from the previous domain. E2E test
- **A7 / Cluster responsibilities**: Multiple Nodes and Server types → select and
  deploy supported Servers, inspect layout, Stop/Restart/Delete → show accurate
  types, states, and impact; link to KV Init without duplicating it, and provide
  no fabricated executable entry points for unsupported types. E2E test
- **A8 / Logical KV navigation**: Multiple Stores/Groups/Replicas including a
  Leader → select Groups and Replicas → show the logical tree, Group CRUD, and
  the Replica's owning Group; locate its Cluster Server and use real protocols
  for membership operations. E2E test
- **A9 / KV CRUD and cursors**: More than one page of keys with the same prefix
  and binary values in different Groups → Scan/Load more/Get/Put/Delete → isolate
  cursors per Group, label results by Group, correctly display UTF-8/Hex, refresh
  after writes, and reject ordinary KV changes to system configuration keys. E2E test
- **A10 / Partial service failure**: One DiskDB or Access endpoint is unreachable →
  view each domain → clearly show failure scope and partial results, keep other
  domains usable, and distinguish empty domains from failed ones. E2E test
- **A11 / Capacity capabilities**: Disks/Zones/Bitmaps exist → select each level
  and perform authorized maintenance → match diagram and request scope, label
  missing instances Partial, and enforce maintenance limits in the backend. E2E test
- **A12 / Chunk prefixes and pagination**: Multiple owners hold many Chunks of
  several types, including unknown types → filter by type and arbitrary valid hex
  prefix, look up IDs, and Load more → return bounded real records, never present
  missing owners as complete results, explicitly flag unknown/mismatched types,
  and show no fabricated totals. Integration test
- **A13 / Strip correctness**: One Chunk contains Mirror/EC and noncontiguous seq
  values → select Chunk/Strip → show actual copy counts/k+m and encoding states
  by real logical range/stable seq, keeping logical capacity, written quantity,
  and physical usage distinct. E2E test
- **A14 / Placement consistency**: Fragments span multiple Disks; layout version
  changes during observation and one Disk lacks topology → refresh and navigate
  across domains → do not combine different layouts, label missing locations
  Unknown, locate Zones only with evidence, and fabricate no reverse lookup
  results without an interface. Integration test
- **A15 / Bounded rendering**: A Chunk contains many Strips/fragments → scroll
  and select details → render only the visible window and on-demand details,
  retain stable selected seq, and avoid loading a full relationship graph. E2E test
- **A16 / Iceberg CRUD**: Valid writer and supported format profile → create,
  update properties, and delete Namespaces; create/load/update Schema/rename/drop
  Tables → use native semantics, show accurate field IDs/head/snapshots, retain
  input on conflicts, and clarify deletion cleanup semantics. Integration test
- **A17 / Iceberg capability boundaries**: Reader, manager, writer, and a Table
  without a file-list source → operate or view Files → do not inherit permissions
  across roles or fabricate successful unsupported listing/row CRUD; metadata
  updates cannot use raw JSON/file overwrites. E2E test
- **A18 / S3 CRUD**: Empty/nonempty Buckets and text/binary objects in an authorized
  scope → create/list/head/upload/get/replace/delete → return real protocol
  results, never implicitly empty nonempty Buckets, do not treat prefixes as
  directories, and preserve special keys exactly. Integration test
- **A19 / Streaming**: Large objects and multipart uploads, with cancellation or
  disconnection during upload → operate and recover → bound memory, accurately
  report confirmed bytes/status, and never present incomplete or unknown outcomes
  as success. Integration test
- **A20 / Credentials and scopes**: Two endpoints/scopes with different permissions →
  switch while a query is pending → keep old responses out of the new scope,
  exclude secrets from responses/Group 0/Activity logs, and reject every
  unauthorized request in the backend. Integration test
- **A20a / Fixed cluster Catalog**: Configured Catalog and server reader → enter
  Iceberg without browser credentials → load the resource tree automatically,
  keep credentials server-side, refuse endpoint changes, and permit root metadata
  mutations through the deployment writer; an unavailable Catalog shows retry
  without connection inputs.
  Integration test and E2E test
- **A21 / Shared Container UI**: Start single-node Container → visit all seven
  domains, perform authorized user data CRUD, and directly request physical
  mutations/deployment/Init/Reset → provide the shared UI, reject all hardware
  and process management writes, and apply logical/data capabilities according
  to role/profile. E2E test
- **A22 / Session and write feedback**: Failed, conflicting, unknown, and successful
  operations exist → view Activity and refresh the page → report each result
  accurately without fabricated cache success, and do not claim session records
  are a durable audit log. E2E test
- **A23 / Independent scopes and demos**: Select a scope in each domain → switch
  domains, refresh, run demos, and clean up → keep write targets visible and
  accurate, clean up only the relevant sample resources, leave system metadata
  unchanged, and never substitute fabricated Chunk/Capacity data for real
  results. E2E test

Additional acceptance for the seven-domain design:

- **A24 / Independent domains**: Existing Capacity and Chunk selections → switch
  among all seven tabs and follow a Chunk-to-Disk link → Capacity and Chunk have
  independent scopes, the link selects Capacity, and no nested domain toggle is
  required (I1, I10). E2E test
- **A25 / Chunk metadata provenance**: Types span Paxos KV and Chunk-KV sources,
  one source fails → filter and page → results identify actual metadata location
  and partial coverage without treating type as backend (I7, I8). Integration test
- **A26 / Partition observation**: Multiple partition owners and a split overlay
  exist → select Server then Partition → ordered ranges, owner epoch, tree,
  distinct journal tracks, and parent dependency match authoritative records
  (I9). E2E test
- **A27 / Catalog movement and scale**: A catalog changes between bounded pages
  → continue or refresh → stale generation is explicit, rendered/requested data
  stay bounded, and stable selection survives when still present (I7, I9, I10).
  Integration test
- **A28 / Service type isolation**: All six service types are registered → invoke
  lifecycle operations → dispatch targets the exact instance/type and an
  unsupported action cannot invoke KV lifecycle (I4, I6). Integration test

#### Delivery status — 2026-10-03

- The first version of the five-domain UI, persistent startup recovery, real
  Chunk/Strip views, Iceberg metadata CRUD, S3 CRUD/multipart, and shared Container
  UI has been implemented.
- Verification and remaining acceptance boundaries are recorded in the
  [implementation plan](../working/plan-console-complete-ui.md). Docker image
  verification is blocked by connection refusal from the local image registry
  proxy; the complete service chain has been verified in an isolated directory.
- This requirement remains open: retain the product questions below that require
  user decisions, along with the fault injection, multiple-owner, and transport
  boundary acceptance work explicitly listed in the plan.

#### Known issues / follow-up acceptance

- Standalone DiskDB registration and Zone bitmap observation have recovered after
  restoring missing data-group binds. New ownership assignment requires a valid
  data-group binding; existing bindings remain unchanged.
- Docker image builds are affected by proxy connection refusal; acceptance inside
  the actual image still needs to be rerun.
- Further acceptance for Partial recovery across multiple owners, layout changes,
  interruption during publication, and multipart disconnection/unknown outcomes
  is recorded in the plan. Keep the corresponding acceptance items open until
  this work is complete.

#### Open Questions

- None. The user approved the seven-domain interaction design and authorized
  bounded object-to-Chunk diagnostics. Remaining work and defects are tracked
  in the implementation plan and persistent UI issue list.

Implementation verification commands (record results when implemented; this
documentation change does not run the implementation suites):

```sh
pixi run test-console-server
pixi run test-console-shared
pixi run test-console-ui
pixi run test-chunkdb-client
pixi run test-chunkdb
pixi run test-access-server
pixi run test-access-iceberg
pixi run test-access-s3
pixi run test-single-node-container
pixi run ts-lint
pixi run rs-fmt-check
pixi run rs-lint
```
