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

- Seven top-level tabs: `Cluster | KV | Capacity | Chunk | Chunk-KV | Iceberg | S3`. The user-facing
  name is Iceberg; existing `iceberge` documentation paths are not renamed here.
- Share the Header, left resource tree/filter, central domain panel, and right
  Inspector. The Inspector provides collapsible Details and Activity views.
  Both sidebars have adjustable widths where present. Iceberg uses only the left
  tree and central content; it does not reserve space for a right Inspector.
- Selection within a domain synchronizes the left tree, central content, and
  Inspector. Switching domains clears inapplicable selected entities. Navigation
  across domains carries the target identity, expands it after loading, and
  reports missing targets explicitly.
- Query/write forms follow the existing KV panel: scope selection, operation
  toolbar, query results, and an editor for the selected item. Each domain shows
  its own operation semantics; resources must not all become generic JSON editors.
- Each tab has its own scope: physical entities for Cluster; Store/Group for KV;
  hardware capacity for Capacity; type/source/ID for Chunk; Server/Partition for
  Chunk-KV; Catalog/Namespace/Table for
  Iceberg; authorized scope/Bucket/prefix for S3. The corresponding panel clearly
  displays its scope. Refresh, filters, and writes apply only to that scope.
  Domain switches may retain filters, but must not reuse another domain's write
  target.
- Provide demo operations per domain and label their actual write targets. KV
  sample keys, S3 sample objects, and Iceberg sample tables use recognizable demo
  names; confirm the exact cleanup scope. Never write demo data to Group 0 system
  configuration keys. Chunk/Capacity show real data only, without fabricated
  layouts; empty states guide users to the relevant deployment or data write
  flow. Demos use normal APIs and permissions and cannot bypass protocols.
- Query lists use server-side filtering and bounded pagination, with 100 items
  per page by default and Load more to append results. Without an exact total,
  show only the loaded count and whether another page exists; do not scan the
  whole domain to calculate totals.
- Mutations wait for backend results before refreshing the relevant resources;
  client caches must not fabricate success. Activity records operations, targets,
  times, results, and associated request information for the current session.
  It does not promise a durable audit log. Domains must not share one generic
  “Backend unreachable” error.

```text
+----------------------------------------------------------------------------+
| CrowDB Console   deployment/status   Cluster KV Capacity Iceberg S3 Refresh |
+-------------------+------------------------------------+-------------------+
| Scope / filter    | Domain toolbar                     | Details | Activity|
|                   +------------------------------------+-------------------+
| Resource tree     |                                    | Selected identity |
| or prefix groups  | Topology / CRUD / capacity / strips | Fields / status   |
|                   |                                    | Cross-domain links|
+-------------------+------------------------------------+-------------------+
```

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

- Replace the left panel with a logical tree: Store → Group → Replica. Physical
  resource management stays in Cluster. Replicas show their Node, health, and
  Leader status; selecting one can navigate to its Server in Cluster.
- Before initialization, the central panel provides Init guidance: select
  deployed, reachable KV nodes and show the Store 0/Group 0 to be created and
  selected members. With no available nodes, guide users to Cluster Deploy.
- After initialization, manage Stores, Groups, and Replicas. Selecting a Store
  shows its groups; selecting a Group opens its Paxos overview with a Data subview; selecting
  a Replica shows details while retaining its Group scope. Replica management
  invokes the existing membership change flow, rather than editing raw
  configuration as a substitute.
- The central panel provides Prefix/Key filters, Scan/Get, a results list,
  UTF-8/Hex views of the selected key, Put/Delete, and Load more. Put edits the
  value; renaming a key must explicitly be two operations: creation and deletion.
  Keep scan cursors independently per Group and label Group identities in
  results spanning a Store.
- Clearly label system Store 0/Group 0 as System. Ordinary user KV CRUD cannot
  modify system configuration keys. System resource changes use the appropriate
  management operations, preventing generic KV operations from damaging the
  cluster authority.

```text
+-------------------+---------------------------------------+----------------+
| KV                | Store 7 / Group 2                     | Group 2        |
| Store 0 [System]  | Prefix [        ] [Scan] [Get]        | leader / state |
| Store 7           | Key       Value preview      Revision | replicas       |
|  Group 1          | key-a     ...                123      | Node links     |
|  Group 2          | key-b     ...                124      |                |
|   Replica 1 N1    | [Load more]                           | Activity       |
|   Replica 2 N2    | Key [key-a]  Value [UTF-8 / Hex]       |                |
| [Add Store/Group] | [Put] [Delete]                        |                |
+-------------------+---------------------------------------+----------------+
```

##### 5. Capacity, Chunk, and Chunk-KV Inspection

- Capacity, Chunk, and Chunk-KV are independent top-level domains with separate
  filters and selections. Implement the agreed navigation, observation, and
  bounded-query contracts in [Console UI design](../design/console/design-crowdb-console-ui.md)
  §§18–21. Chunk type and metadata source are independent filters; unavailable
  sources are partial coverage, not empty results.
- Chunk-KV navigation is Rack → Node → Server → Partition. The center provides
  a range distribution map and selected Partition Overview/Tree/Journal/
  Dependencies. Split lineage and transfer state are distinct from current
  placement. Parent and child stream offsets remain separate; an active child
  can retain a parent recovery dependency. Manual split/repair is outside this
  observation scope.
- Capacity retains hierarchical Cluster/Rack/Node/DiskGroup/Disk capacity,
  DiskDB instance status, Zone grids, and Bitmaps. Scan/Recalc/Compact/Rebuild
  show their exact scope. DiskGroup/Disk management remains in this domain.
  Container can view every level; hardware mutations are prohibited and runtime
  maintenance requires a separate capability. An unreachable instance yields
  partial results identifying the missing source.
- The Chunk subpage provides read-only diagnostics by default, without raw
  Chunk/Strip deletion or manual layout rewriting. Lifecycle remains controlled
  by the owning business and existing service flows, preserving reference and
  reclamation rules.
- Left-side grouping uses the actual type byte of Chunk IDs and a user-entered
  hexadecimal prefix. Type names/encodings come from protocol definitions,
  rather than copied enum numbers from old documents. Show raw values for unknown
  types and flag mismatches between ID type and record. Arbitrary prefix filters
  can be combined with type grouping.
- Grouping is a list filter, not a ChunkDB hash range, KV Group, or storage
  ownership boundary. The backend performs bounded queries across relevant owners,
  handling pagination, routing changes, and partial unavailability. The browser
  must not fetch all Chunks and then filter them. Entering a full ID locates a
  single Chunk.
- Upper central panel: Chunk ID, type, lifecycle state, version/generation (if
  exposed), logical capacity, written ranges/usage (if confirmed by the service),
  Strip count, and query time. Show allocated capacity, written bytes, and physical
  usage separately; missing quantities must not be inferred as zero.
- Lower central panel: Strips in logical order, using their stable sequence
  identities. Do not renumber them by array index after deletion/replacement.
  Mirror shows the actual copy count; EC shows actual k+m and encoding state.
  One Chunk can contain different layouts; do not assume uniform Mirror/EC
  parameters across it.
- Selecting a Strip shows its logical offset/range, layout, write/encoding/health
  state, and Segment/fragment details. Each fragment shows its Mirror copy or EC
  data/parity role, Rack → Node → DiskGroup → Disk, physical offset/length, and
  allocation unit information provided by the protocol. If topology resolution
  fails, retain the Disk ID and label it Unknown.
- Fragment placement diagrams use a bounded visible window and on-demand details;
  do not render large Strip/fragment collections as one complete relationship
  graph. Layout records and physical locations must show the same query generation
  or their separate observation times. During conversion/migration, do not combine
  two versions into a Strip that never existed.
- Fragments can link to a specific Disk in Capacity. Locate Zone/Bitmap only with
  a verifiable Zone mapping. Node/Server links navigate to Cluster. Capacity
  details may link back to related Chunk queries, but must not fabricate “all
  Chunks on this Disk” without a service supporting reverse lookup.

```text
+-------------------+---------------------------------------+----------------+
| CHUNK             | [Capacity] [Chunk]                    | Strip seq 8    |
| Type / ID prefix  | Prefix [0a..] [Query] [ID lookup]     | logical range  |
| All               | Chunk ID  Type   State   Capacity     | layout/state   |
| WAL               | 0a...     ...    Sealed  ...          | physical spans |
| Tree / Index      +---------------------------------------+----------------+
| S3 / Iceberg      | Selected chunk: ID / state / totals   | Placement links|
| Other (raw type)  | seq 7  Mirror x2  [copy 0] [copy 1]   |                |
|                   | seq 8  EC 4+2     [D0][D1][D2][D3]    | Activity       |
|                   |                   [P0][P1]           |                |
+-------------------+---------------------------------------+----------------+

Chunk logical ranges -> stable Strip sequence -> actual fragment locations

Strip seq 7: Mirror x2
  copy 0 -> Rack A / Node 1 / DG 101 / Disk a -> offset, length
  copy 1 -> Rack B / Node 2 / DG 201 / Disk b -> offset, length

Strip seq 8: EC 4+2 (example only; profile determines allowed placement)
  D0 -> N1 / DG101 / Disk a       P0 -> N5 / DG501 / Disk e
  D1 -> N2 / DG201 / Disk b       P1 -> N6 / DG601 / Disk f
  D2 -> N3 / DG301 / Disk c
  D3 -> N4 / DG401 / Disk d
```

##### 6. Iceberg: Reference Tree and File Inspection

- Entering Iceberg automatically loads the current cluster Catalog. Do not show
  endpoint/token inputs or a connection wizard. Deployment supplies the fixed
  Catalog origin and server-held reader credential. Show unavailable/retry when
  that service is absent; do not substitute another cluster. Catalog mutations
  retain native authorization through the existing management session.
- Use a two-column layout. Keep file properties and footer fields in the central
  content, and show selected column details below the Parquet layout. Put refresh
  and table actions in the center; do not duplicate metadata in a right panel.
- The left panel expands Catalog → Namespace → Table → Snapshot → Manifest List
  → Manifest → File. It follows immutable references, including shared files,
  data/delete manifests, branch/tag labels and historical snapshots.
- Selecting each entity shows structured fields and tables, never raw JSON as
  the primary content. Table views show schema, partition/sort specifications,
  properties and snapshot ancestry. Manifest lists show manifest descriptors;
  manifests show entry status, partition values, inherited sequences and metrics.
- File inspection is read-only and bound to the selected table metadata generation
  and snapshot. Reject foreign references, expired files and stale generations;
  changing credentials or scope cancels pending work and clears previous results.
- Parquet inspection reads framing and footer metadata only. Show file size,
  physical rows, schema, writer and footer fields; visualize actual column-chunk
  byte ranges and bounded Row Group pages. Selecting a column shows its path,
  field ID, type, codec, encodings, compressed/uncompressed sizes, offsets, value
  count and optional statistics. Missing statistics remain unknown; bounds retain
  exactness flags. Footer-only inspection does not claim page boundaries or live
  row counts after deletes.
- Expand reference children lazily, use bounded server pages, and retain explicit
  partial/unsupported states. Other file formats expose supported metadata only.
  Native Catalog management remains available alongside the inspector.
- Use crowdb-tpc-loader in an isolated namespace for real Parquet/manifest
  acceptance; independently verify footer-only reads and large integer identities.
  Row queries and row DML are outside this delivery; the user selected metadata
  and file-layout inspection as this iteration's scope.

##### 7. S3: Bucket and Object CRUD

- Left panel: current authorized S3 scope → Bucket → prefix. A prefix virtually
  groups keys; it is not an independently deletable directory. Preserve key case,
  repeated slashes, and other details according to the protocol.
- Central panel: Bucket selector, prefix/key queries, paginated object list,
  selected object preview, and operations. Lists show Key, size, ETag, and last
  modified time when provided by the service.
- Bucket supports create/list/head/delete. Display errors for deleting nonempty
  Buckets as returned, without implicit recursive deletion. Object supports
  upload/replace, head/get/download, and delete. Update replaces object contents;
  ETag is neither content nor an editable property.
- Small text objects allow bounded UTF-8/Hex previews and editing. Objects beyond
  the preview limit and binary files use file upload/download. Large objects retain
  streaming and cancellation semantics without full buffering in Web or the browser.
  Multipart shows upload progress/status. After cancellation, report whether uploads
  remain pending, following existing abort/recovery semantics. Failures and unknown
  outcomes must not be shown as saved.
- The first version does not depend on CopyObject, UploadPartCopy, batch
  DeleteObjects, version history, or IAM management. Add entry points only after
  the corresponding service capabilities are implemented.
- The Object Inspector shows available native references. Navigation to Chunk
  requires an authorized management query; S3 key prefixes and Chunk ID prefixes
  must not be treated as the same classification.

```text
+-------------------+---------------------------------------+----------------+
| S3                | Bucket [datasets] Prefix [raw/]       | Object details |
| authorized scope  | [List] [Upload] [Create Bucket]       | full key       |
|  datasets         | Key       Size     ETag / modified    | size / ETag    |
|   raw/            | raw/a     ...      ...                | content type   |
|   output/         | raw/b     ...      ...                | Chunk links    |
|  logs             | [Load more]                           | Activity       |
|                   | Preview [UTF-8 / Hex]                 |                |
|                   | [Download] [Replace] [Delete Object]  |                |
+-------------------+---------------------------------------+----------------+
```

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

- **Business references from Iceberg/S3 to Chunk**: Should the first version show
  only references already queryable, or add authorized object-to-Chunk diagnostic
  queries? The former has a smaller scope but leaves some objects without links;
  the latter requires explicit business identity, authorization, and pagination,
  without exposing internal references across the whole domain. Independent
  Chunk browsing is not blocked.

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
