<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Console Web UI

Depends on: [`design-crowdb-console.md`](design-crowdb-console.md), [`../kv/design-crowdb-kv.md`](../kv/design-crowdb-kv.md) §15.4.6
Satisfies: [`../kv/design-crowdb-kv.md`](../kv/design-crowdb-kv.md) §15.4.6

This document defines the Console SPA's information architecture, navigation,
and inspection workflows. Backend contracts belong to
[`design-crowdb-console.md`](design-crowdb-console.md).

Sections 1, 3, 6.1, 12.1, and 18–21 describe the agreed target UI, including
capabilities awaiting implementation. Section 21 records those gaps. The
embedding, lifecycle, and transport details in other sections describe the
implementation baseline; they do not imply that the target UI is complete.

## Table of Contents

- [1. Goals (recap)](#1-goals-recap)
- [2. Stack decisions](#2-stack-decisions)
- [3. Information Architecture](#3-information-architecture)
  - [3.1 Selection & cross-jump](#31-selection--cross-jump)
- [4. Visual Language](#4-visual-language)
- [5. Topology Canvas (React Flow, slim)](#5-topology-canvas-react-flow-slim)
  - [5.1 Physical layout](#51-physical-layout)
  - [5.2 Logical layout](#52-logical-layout)
  - [5.3 Interactions](#53-interactions)
- [6. Inspector Panel](#6-inspector-panel)
- [6.1 KV Operator Panel (center panel)](#61-kv-operator-panel-center-panel)
- [7. Embedding Contract](#7-embedding-contract)
- [8. Data & Polling Strategy](#8-data--polling-strategy)
- [9. Module Layout](#9-module-layout)
- [10. Accessibility](#10-accessibility)
- [11. Testing](#11-testing)
- [12. Domain responsibilities](#12-domain-responsibilities)
  - [12.1 Seven domains](#121-seven-domains)
  - [12.2 Scope and navigation invariants](#122-scope-and-navigation-invariants)
  - [12.3 Swagger UI removal](#123-swagger-ui-removal)
  - [12.4 Batch Add Disk](#124-batch-add-disk)
- [13. DiskDB Server Deploy / Restart / Stop](#13-diskdb-server-deploy--restart--stop)
- [14. REST Proxy for DiskDB Runtime](#14-rest-proxy-for-diskdb-runtime)
- [15. Capacity Panel (Canvas Visualization)](#15-capacity-panel-canvas-visualization)
  - [15.1 Rendering](#151-rendering)
  - [15.2 Color encoding](#152-color-encoding)
  - [15.3 Polling](#153-polling)
  - [15.4 Scope dispatch and module structure](#154-scope-dispatch-and-module-structure)
- [16. Console-Shared DiskDB Client + CLI](#16-console-shared-diskdb-client--cli)
  - [16.1 Console-shared client](#161-console-shared-client)
  - [16.2 CLI subcommands](#162-cli-subcommands)
- [17. Native Access and Chunk diagnostics](#17-native-access-and-chunk-diagnostics)
- [18. Fixed-cluster operator flow](#18-fixed-cluster-operator-flow)
- [19. Chunk explorer](#19-chunk-explorer)
- [20. Chunk-KV workbench](#20-chunk-kv-workbench)
- [21. Implementation boundaries](#21-implementation-boundaries)

## 1. Goals (recap)

- Single page, no full-page navigation.
- Seven first-class domains: Cluster, KV, Capacity, Chunk, Chunk-KV, Iceberg,
  and S3. Each owns navigation, operations, and details. Capacity and Chunk
  have distinct identities; embedding compatibility must not conflate them.
- One Console deployment operates one fixed cluster. Access browsing uses
  that cluster's services without a per-page connection wizard.
- Full operator surface: rack/node/server lifecycle, store/group/replica
  CRUD, KV data plane, disk-group/disk lifecycle, capacity
  visualization.
- Offline-capable: no third-party CDN at runtime.
- Lean: minimal dependencies, no feature the requirement does not mandate.

## 2. Stack decisions

- **React + TypeScript + Vite + TailwindCSS** — carried over from the
  existing codebase; no framework migration.
- **React Flow for topology** — slim usage only (custom nodes, pan, click
  select). Deliberately no minimap, zoom toolbar, layout selector, or edge
  labels. The canvas is a navigation aid, not an analytics surface.
- **React Context for state** — domain, selection, toasts, activity.
  No Redux; the state surface is small enough that Context + local hooks
  suffice.
- **No client-side routing** — the SPA mounts at the document root;
  intra-SPA navigation is selection state, not URL navigation. This keeps
  embedding trivial (no history API conflicts).
- **Removed dependencies**: `recharts`, `jspdf`, `jspdf-autotable`,
  `uuid`, `react-router-dom` — none are needed for the lean v1 surface.

## 3. Information Architecture

The shared header selects Cluster, KV, Capacity, Chunk, Chunk-KV, Iceberg,
or S3. Each domain owns its left navigation, central workbench, and optional
selected-resource properties.

```text
+---------------------------------------------------------------------+
| Cluster | KV | Capacity | Chunk | Chunk-KV | Iceberg | S3            |
+----------------------+-------------------------------+--------------+
| Domain scope         | Domain operations and state   | Details      |
| Physical hierarchy   | Rack/Node/Service layout      | Physical     |
| Store/Group/Replica  | Paxos state / Data subview    | Logical      |
| DiskGroup/Disk/Zone  | Capacity and allocation       | Placement    |
| Chunk type/source   | Chunk list / Strip layout     | References   |
| Node/Server/Partition| Range map / Tree / Journal    | Selection    |
| Catalog/Table/Files | Structured file inspector (two columns)      |
| Bucket/Prefix        | Objects and multipart state   | Object       |
+----------------------+-------------------------------+--------------+
```

- Cluster renders physical resources through services. Logical KV resources
  belong to KV. DiskDB-owned disks remain reachable from their physical service.
- Capacity owns physical storage and allocation; Chunk owns logical chunks
  and their layouts; Chunk-KV owns range partitions and their tree/journal
  state. Cross-links preserve these boundaries.
- Iceberg uses a two-column resource tree and central file inspector; file
  properties and selected column details stay in the center. S3 retains its
  object detail panel. Both share input styles and session activity feedback.
- Selection and filters are stored by domain. Hidden workbenches preserve
  bounded navigation state but suspend polling. Deep links identify the domain,
  resource, and subview; secrets never enter URLs or persisted navigation.
- Container renders this same UI. Physical topology/deployment controls are
  read-only; a checked management bearer enables supported logical/data and
  DiskDB maintenance operations. Native Access credentials authorize native
  operations independently of the management bearer.

### 3.1 Selection & cross-jump

Selection identifies a domain, resource kind, full stable identity, and parent
scope. Partition selections retain catalog generation and owner epoch for
observation consistency; selecting a resource does not freeze its owner.

Cross-jumps include:
- KV `Replica` → "Show on node": switch to Cluster, expand the owning
  `Node`, select the matching server entry.
- Cluster KV `Server` → "Show in KV": switch to KV, expand the
  owning `Store → Group`, select the unified row.

- Chunk-KV Partition → Tree/Journal → Chunk → Strip → Capacity Disk →
  Cluster Node. Metadata location links to the exact Paxos Group or Chunk-KV
  Partition; business ownership is a separate link.
- Iceberg and S3 link to lower storage objects only when an authoritative
  mapping is available. A missing mapping is explicit, never inferred from a
  name or prefix.

Breadcrumbs and return navigation restore the originating selection, filters,
and bounded page state. Large hierarchies expand lazily to the selected item.

## 4. Visual Language

Single dark theme via CSS variables under `.crowdb-console` (existing
tokens in `src/index.css`). Status colors: `--healthy`, `--degraded`,
`--failed`, `--unknown`, plus `--remote` for remote-replica accent.

Status is never color-only. Every status row also carries a glyph
(✓ / ! / ✕ / ?). Leader replicas carry a crown badge. Remote replicas use
a dashed border + `--remote` accent so peer-list mis-wirings are visible.

Animations are minimal (selection/hover transitions); honor
`prefers-reduced-motion`.

## 5. Topology Canvas (React Flow, slim)

One layout at a time, chosen by domain. Layout is computed by a small
deterministic tree-layout pass in `topology/layout.ts` (columns by depth,
rows by sibling index). No dagre, no force simulation, no user-selectable
layouts.

### 5.1 Physical layout

Renders `Rack → Node → Server → PxStore → PxGroup → {Local, Remote…}`
read from the physical tree. Node types: `Rack`, `Node`, `Server`,
`PxStore`, `PxGroup`, `LocalReplica`, `RemoteReplica`. Edges follow
parent→child containment. Each `RemoteReplica` draws a solid edge to its
peer `LocalReplica` (a missing edge is the bug this view surfaces). The
leader radiates accent edges to followers.

### 5.2 Logical layout

Renders `Cluster → Store → Group → Replica…`. Node types: `Cluster`,
`Store`, `Group`, `Replica` (with a `node_id` badge). The leader radiates
accent edges to followers; no local/remote distinction.

### 5.3 Interactions

- Drag pans, wheel zooms (React Flow built-ins), click selects.
- Selecting a node drives the inspector and highlights the sidebar row.
- Right-click a node opens the same per-layer context menu as the tree.
- Tooltips on hover surface one useful fact (host, leader id, reachable).
- No minimap, zoom toolbar, search box, focus mode, export, or edge
  labels.

## 6. Inspector Panel

Tabs re-render against the current selection:

1. **Details** — labelled key/value table from the selected entity
   (physical or logical shape). Long values support copy-to-clipboard. A
   footer row shows the cross-jump link (§3.1).
2. **Activity** — chronological client-side list of UI-issued operations
   (timestamp, action, target, outcome). No filter/export in v1.

The KV tab has been removed from the Inspector. All KV operations now
live in the center KV Operator panel (§6.1), which provides a full-width
surface with store/group selectors, scan results, and an action bar.

## 6.1 KV Operator Panel (center panel)

A Group opens its Paxos overview by default: membership, leader, replica
health, and supported operational state. The full-width Data subview contains
the KV operator. A Replica selection opens that replica's state and links to
its physical server. Store and Group lifecycle controls remain in KV.

**Design choices:**

- **Data subview layout** — action bar on top, scan
  results below. The user can scan, see results, and act (put/get/delete)
  without switching tabs.
- **Explicit scope** — reads identify Store/Group. A store-wide scan has bounded
  fan-out and per-group continuations; mutations require an explicit supported
  target and do not randomly choose a writable group.
- **Lazy data reads** — entering the Data subview may read its first bounded
  page. Opening a Paxos overview does not scan the keyspace.
- **Group 0 protection** — general data mutations remain forbidden in both
  the UI and server.

**Scan pagination (`start_after` token):**

The scan API returns at most `limit` items with a `truncated` flag but
had no way to fetch the next page. Rather than adding a total count
(expensive on large keyspaces), we adopted an S3 ListObjectsV2-style
`start_after` cursor: the caller passes the last key from the previous
batch; the engine returns keys strictly greater than `start_after` that
still match the prefix. The UI shows a "Load more" button when
`truncated` is true; clicking it appends the next batch.

**Decision — `CrowdbTreeEngine` over-fetch + filter:** The C++ crowdb-tree
scan API takes only prefix + limit (no `start_after`). Rather than
modifying C++ immediately, `CrowdbTreeEngine` over-fetches with the
original prefix, then filters out keys ≤ `start_after` in Rust before
applying the limit. This is inefficient when `start_after` is deep into
a large prefix range. A follow-up can push `start_after` into the C++
engine. When `start_after` is empty, the fast path is identical to the
old behavior.

**Demo delete at scale:** "Delete all demo" scans for `demo_` prefix
with pagination (up to 1000 keys for the confirmation count), then
deletes with 16-way parallel `kvDelete`. If more than 1000 keys exist,
scan+delete continues in batches after confirmation. The confirmation
dialog shows "1000+" when the count may be higher.

## 7. Embedding Contract

The SPA is mountable as a sub-component with a minimal props interface
(`apiPrefix`, `basePath`, `readonly`, `modules` opt-out, `initialDomain`,
`onEvent` callback). Three isolation rules:

- **Style isolation** — everything wraps in `.crowdb-console`; Tailwind
  uses the `tw-` prefix and `important: '.crowdb-console'`.
- **API isolation** — every fetch resolves against `apiPrefix`.
- **Standalone** — `index.html` mounts at the document root with defaults;
  `embed.ts` exports the component for hosts.

The `initialDomain` prop (values: `Cluster | KV | Capacity | Chunk | Chunk-KV | Iceberg | S3`) replaces the
former `initialViewMode`. The `modules` opt-out keys are
`'racks' | 'nodes' | 'stores' | 'groups' | 'replicas' | 'kv' |
'activity'` — the former `'swagger'` key is removed (Swagger UI is no
longer embedded). The former `initialNodeId` prop is removed.

## 8. Data & Polling Strategy

- **Two-tree contract** — the SPA speaks physical (`/api/racks`,
  `/api/nodes`) and logical (`/api/stores`) trees per `design-crowdb-console.md`.
  No panel constructs raw `host:port` URLs; `api.ts` is the single URL
  builder.
- **Asymmetric polling** — only the active view polls fast (~5s); the
  inactive view polls slow (~30s) so toggling renders immediately.
  Polling pauses while the tab is hidden.
- **Optimistic-free mutations** — mutations call the backend, await
  success, then trigger a refresh of the affected view; they do not
  hand-edit cached data. This trades a round-trip for correctness
  simplicity.

## 9. Module Layout

The source tree follows the pane structure: `shell/` (Header, Sidebar,
Inspector), `topology/` (canvas + layout), `panels/` (KvOperatorPanel,
ActivityLog), `components/` (Dialog, ContextMenu, dialogs,
UI primitives), and `contexts/` (Domain, Selection, Toast, Activity).
`api.ts` and `types/index.ts` are the single URL-builder and data-model
modules respectively.

**Deleted from v1**: CommandPalette, favorites, fuzzy search, export
utils, bulk action dialog, metrics history, theme context. None are
needed for the lean surface.

## 10. Accessibility

- Keyboard reachable: Tab/Enter/Escape on tree rows, dialogs, and menus;
  context menus mirror to keyboard-activatable buttons where practical.
- Color is never the sole status channel (glyph + color).
- Strings go through a single `t(key)` helper (English only) so a future
  locale pack needs no source changes. (Optional for v1; may inline.)

## 11. Testing

- Existing Vitest unit tests for dialog request bodies and `listRacks`
  envelope handling are **retained** (they pin the backend contract).
- The Playwright real-backend E2E suite (`app/crowdb-web/ui/e2e/`)
  targets this lean SPA; selectors track the rewritten DOM. The full
  chain rack→node→deploy→store→group→replica→KV is the acceptance bar.
- The web server's test mode keeps spawned DiskDB heartbeat and group-0
  sync intervals at one second. Normal deployments retain DiskDB's
  production defaults.
- Before group-0-backed tree reads, test mode validates the locally managed
  group-0 process and refreshes its topology. Production uses monitor-cache
  availability because group-0 may be hosted remotely and therefore has no
  locally tracked process.

---

## 12. Domain responsibilities

### 12.1 Seven domains

- **Cluster**: Datacenter → Rack → Node → service instance. Owns deployment,
  start/restart/stop/removal, configuration, and logs for KV, DiskDB, ChunkDB,
  DiskIO, Chunk-KV, and Access Server. Service-specific readiness remains
  distinct from process liveness.
- **KV**: Store → Group → Replica. The center defaults to Paxos management;
  a Data subview supports scan/get/put/delete with explicit scope.
  Store 0 / Group 0 is read-only to general data mutation APIs as well as UI.
- **Capacity**: Node/DiskGroup/Disk/Zone capacity, allocation, and supported
  maintenance. Missing usage is unknown, not zero usage or free capacity.
  Service lifecycle links back to Cluster.
- **Chunk**: Type/source filters, bounded listing, metadata location, business
  ownership, and real Strip/Mirror/EC placement (§19).
- **Chunk-KV**: Rack → Node → Server → owned Partition; range distribution,
  split and transfer state, Tree, Journal, and recovery dependencies (§20).
- **Iceberg**: Catalog → Table → Snapshot → Manifest List → Manifest → File,
  with namespaces grouping tables. The center renders structured fields and
  file-specific content. Parquet inspection shows footer, row-group layout,
  sizes, and column metadata without reading data pages by default. Table
  operations use native REST; row DML has no console execution path.
- **S3**: Bucket/object CRUD, prefixes and cursors, HEAD/ETag, bounded preview,
  downloads, multipart transfer/inspection/abort through native signed requests.

### 12.2 Scope and navigation invariants

- **I1**: each domain retains its own selection and filters. A cross-jump
  changes only the destination selection; unrelated domain scopes survive.
- **I2**: physical layout never expands logical groups or replicas under a KV
  service. KV owns logical relationships and CRUD.
- **I3**: an unavailable authority is an error, not a confirmed empty tree.
  Container monitor recovery remains visible separately from Group 0 topology.
- **I4**: domain operations name their native target. Read-only embedding and
  Container hardware restrictions are enforced by the server as applicable.
- **I5**: demo cleanup uses owned session resources. KV cleanup scans only the
  current session prefix within the selected group/store; Access demos name an
  exact namespace/table or bucket/key and do not recursively remove user data.

### 12.3 Swagger UI removal

The former Swagger API panel (embedded OpenAPI browser + per-node
openapi.json proxy) is removed. The OpenAPI document remains served
by `crowdb-kv-server` at `/openapi.json` for direct access; the
console no longer embeds or proxies it. The `'swagger'` module opt-out
key and `initialNodeId` prop are removed from the embedding contract.

### 12.4 Batch Add Disk

A batch endpoint for atomic all-or-nothing disk creation (unchanged
from the former Capacity view, now accessed from the Cluster domain):

- Validates all `disk_id` formats upfront; rejects the whole batch if
  any is malformed (atomic).
- Writes all disks to config + group-0 sysdata in one transaction;
  if any write fails, rolls back (no partial success).

## 13. DiskDB Server Deploy / Restart / Stop

The Cluster domain owns service lifecycle actions for both KV Server and
DiskDB Server. The Chunk domain displays the DiskDB item but does not
own a second lifecycle workflow. The deploy/restart/stop handlers enable
`AddNodeDialog` to auto-deploy DiskDB alongside KV, and the service
context menu works for both types.

Deployment mechanism: SSH or local fork, same as KV. No Docker. The
`crowdb-diskdb` binary is spawned via `ssh::deploy_via_ssh` or
`lifecycle::deploy_local_in_dir`. A DiskDB deployment receives one
user-facing service endpoint port; the internal HTTP health listener and
any other required listener ports are reserved by the lifecycle layer and
are not exposed as DiskDB management properties.

New handlers mirroring the KV handlers:

```rust
pub struct DeployDiskdbBody {
    endpoint_port: u16,
}

pub async fn http_deploy_node_diskdb(
    State(state), Path(node_id), Json(body),
) -> Result<(StatusCode, Json<DeployResult>), ...>

pub async fn http_restart_node_diskdb(
    State(state), Path(node_id),
) -> Result<Json<DeployResult>, ...>

pub async fn http_stop_node_diskdb(
    State(state), Path(node_id),
) -> Result<Json<StopResult>, ...>
```

- `http_deploy_node_diskdb` — checks no existing DiskDB on the node
  (409 if present), resolves the node, derives the internal listener
  ports from the single `endpoint_port`, spawns via SSH or local fork,
  persists a DiskDB service entry, and records the pid. Route:
  `POST /api/nodes/:id/diskdb/deploy`.
- `http_restart_node_diskdb` — stops the tracked pid and re-deploys on
  the persisted endpoint port. Route:
  `POST /api/nodes/:id/diskdb/restart`.
- `http_stop_node_diskdb` — stops the tracked pid, clears it, and keeps
  the entry. Route: `POST /api/nodes/:id/diskdb/stop`.
- KV and DiskDB use distinct service types and public endpoint models.
  KV includes its HTTP management URL; DiskDB includes its service
  endpoint, health, and process state but not its internal HTTP health
  URL. Runtime PID tracking is keyed by `(node_id, service_type)`.
- `AddNodeDialog` calls `deployServer` (KV) then `deployDiskdb` (new
  API function) after `addNode` succeeds. Both are gated by the
  existing `enableCrowDB`-style checkbox (add `enableDiskDB`, default
  true).

Edge cases:
- Node with KV deployed but DiskDB deploy fails → KV stays deployed;
  the dialog reports the DiskDB failure; the operator can retry via
  the Server context menu's Deploy.
- DiskDB binary not found on the remote host → SSH deploy returns an
  error; surfaced as 502.
- Port conflict (another process on 9941/9942) → spawn fails; surfaced
  as 502. The handler does not pre-check ports (best-effort, matches
  KV behavior).

## 14. REST Proxy for DiskDB Runtime

`crowdb-web` proxies diskdb runtime RPCs (`QueryCapacityStats` drill-down,
scan, recalc, compact, rebuild) via REST endpoints under
`/api/diskdb/`. The CLI and web UI route through `crowdb-web` (no direct
crowdb-rpc from the browser or CLI). `AppState` owns a `DiskdbClient` built
from the same `ServiceRegistryClient` the console already uses:

```rust
diskdb_client: tokio::sync::RwLock<Option<DiskdbClient>>,
```

The `DiskdbClient` is lazily initialized on first diskdb REST request
(the service registry may not be ready at console startup).

Handlers:

- `GET /api/diskdb/instances` — reads live instances from the service
  registry and merges `owned_dg_ids` from the authoritative group-0
  ownership map (no crowdb-rpc fan-out). Returns instance id, endpoint,
  `last_heartbeat_ms`, current ownership, and keepalive `group_usages`.
- `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` —
  `QueryCapacityStats` drill-down (all params optional). When `dg` is
  omitted, iterate all registered instances and merge the responses
  for cluster-wide totals. `DiskdbClient.query_capacity_stats(0)`
  routes to one instance only, so the merge lives in this handler.
- `GET /api/diskdb/scan-status?dg=<id>` — `get_scan_status`.
- `POST /api/diskdb/scan` — `trigger_scan` (optional `dg` in body).
- `POST /api/diskdb/recalc` — `recalc_disk_usage` (optional `dg`).
- `POST /api/diskdb/compact` — `compact_zone` (disk_id + optional
  zone_indices; empty = all zones).
- `POST /api/diskdb/rebuild` — `rebuild_zone_bitmap` (disk_id +
  optional zone_index; absent = all zones — handler loops over the
  disk's zones if zone_index is absent).
- `PUT /api/disks/:disk_id/status` — set a disk's `HwStatus` via
  `HardwareClient.set_disk_status`. Needed by the Set-Status dialog;
  no such endpoint existed before (only add/remove/move).

`GET /api/diskdb/usage` with no `dg` iterates
`read_all_diskdb_instances`, calls `query_capacity_stats` per
instance, merges `DiskGroupInfo` entries by id (summing
capacity/busy/free). A dead instance yields a degraded indicator,
not a failed page. Its contribution is skipped with a warning.

`PUT /api/disks/:disk_id/status` resolves the disk's rack/node/dg
from config, then calls `hw.set_disk_status`. 404 if the disk is
not in config.

Edge cases:
- Cluster overview with a dead instance → merged response excludes
  it; the `/instances` endpoint still lists it (with stale heartbeat)
  so the UI can show the degraded card.
- Zone drill-down → bitmap is omitted at disk level (flatbuffer contract);
  the UI issues the zone-level query separately.
- Scan already running → `trigger_scan` returns `scan_in_progress:
  true`; handler passes it through (no error).

## 15. Capacity Panel (Canvas Visualization)

The Capacity domain renders capacity visualization
that scales to thousands of zones per disk and tens of thousands of
blocks per zone. Canvas with offscreen double-buffering handles 84×84
zone grids and 181×181 bitmap grids without flicker. DOM/SVG
rendering at that scale causes layout thrash and jank.

`CapacityPanel` renders in the independent Capacity domain and suspends its
focused polling while inactive. The panel
content depends on the selected entity (from `SelectionContext`):

- **Cluster (Datacenter or no selection)** — per-rack breakdown. One
  row per rack with DG count, node count, and a capacity/busy/free
  bar. The cluster-wide scan status summary + trigger
  (`ScannerPanel`) renders here only. Data from
  `GET /api/diskdb/usage` (cluster merge).
- **Rack selected** — per-node breakdown within the rack. One row per
  node with DG count and a capacity/busy/free bar. Data from
  `GET /api/diskdb/usage` (cluster merge, client-filtered).
- **Node selected** — per-DG breakdown. One row per DG on the node
  with disk count (array icon + count, not per-disk boxes) and a
  capacity/busy/free bar. Data from `GET /api/diskdb/usage` (cluster
  merge, client-filtered).
- **DiskGroup selected** — per-disk boxes. Each disk is a box with a
  busy% gradient fill (green → amber → red, red = busy) + inline `%`
  label + tooltip (disk id + busy%). Data from
  `GET /api/diskdb/usage?dg=<id>`.
- **Disk selected** — zone grid + per-disk actions. Each zone is a
  box in a square grid (side = ceil(sqrt(zone_count))) with a
  green→amber→red gradient based on busy%. Hover shows a tooltip
  with zone id + usage %. A "jump to zone #" input handles direct
  navigation (7000 zones cannot be a dropdown). All disk-scoped
  actions are inline in the disk header: Scan and Recalc target the
  disk's parent DG (`triggerDiskdbScan` / `recalcDiskdbUsage` with
  the DG id); Compact, Rebuild, Up, and Down target the disk itself
  (`compactDiskdbZones` / `rebuildDiskdbZoneBitmap` /
  `setDiskStatus`). The per-DG recalc result (`RecalcPanel`) renders
  here, scoped to the parent DG. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>` (brief per-zone
  entries, no bitmap).
- **Zone selected (in-panel, within the Disk view)** — zone bitmap.
  Canvas grid of the zone's `usage_bitmap`
  (side = ceil(sqrt(unit_count))). Busy block = red filled cell, free
  block = green filled cell. Zone is not a sidebar entity; it is an
  in-panel click state inside the Disk view. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` (full
  bitmap, on-demand only).

### 15.1 Rendering

Canvas, not SVG/DOM, for all levels:
- Offscreen canvas double-buffering: draw to an offscreen canvas,
  then `drawImage` blit to the visible canvas in one call. The
  visible canvas is never cleared-then-slowly-drawn (that flickers).
- Single `requestAnimationFrame` sync redraw for grids up to 181×181
  (32K cells) — fast enough to not flicker.
- On data refresh (3 s poll), retain the previous frame until the
  new one is fully drawn, then swap. No blank intermediate state.
- No DOM reflow. The canvas is a single element; only its bitmap
  content changes.

### 15.2 Color encoding

Green (free) → amber → red (busy):
- Zone/disk boxes: gradient fill based on `busy_blocks /
  unit_capacity` ratio. 0% = green, ~50% = amber, 100% = red.
- Bitmap cells: binary — busy = red filled, free = green filled.
- Redundant encoding: each zone/disk box shows a `%` text label on
  hover (zone id + usage %) or inline so the information is not
  color-only (color-blind friendly).

### 15.3 Polling

3 s refresh of the currently focused visualization:
- The poll refetches only the data for the selected entity level
  (rack/node → cluster merge; disk-group → dg query; disk → disk
  query; zone → zone query).
- On refetch, the canvas redraws via double-buffer (no flicker).
- If the selection changes, the poll target switches immediately;
  the old canvas is cleared on the next draw.

Zone count math (for layout):
- 200 TB disk / 32 GB zone = 6400 zones → 80×80 grid.
- 32 GB zone / 1 MB unit = 32K units → 181×181 grid.

Edge cases:
- Disk with 0 zones (freshly added, zone load in progress) → empty
  grid placeholder with "loading" text.
- Zone with `used_count == unit_capacity` → all cells red; reported
  as-is.
- `usage_bitmap` shorter than `unit_capacity` (last zone rounded) →
  pad with free (green) cells.
- Poll response slower than 3 s → keep previous frame; next poll
  catches up. No spinner overlay (would flicker).

### 15.4 Scope dispatch and module structure

`CapacityPanel` derives a `CapacityScope` (`Cluster | Rack | Node |
DiskGroup | Disk`) from the selected entity and renders one branch per
scope. The header (title + totals cards) is common to all scopes; only
the body branches. Each scope has a dedicated subview:

- `ClusterView` — per-rack breakdown + `ScannerPanel` (cluster-wide
  scan status + trigger).
- `RackView` — per-node breakdown.
- `NodeView` — per-DG breakdown.
- `DiskGroupView` — per-disk box grid.
- `DiskView` — zone grid (`ZoneGrid`) + zone bitmap (`ZoneBitmap`) +
  jump-to-zone input + per-disk action buttons + `RecalcPanel`
  (scoped to the parent DG).

Shared color/format utilities live in `utils/capacity.ts`:
- `busyColor(pct)` — green → amber → red gradient (4-step thresholds
  30/60/85/100), shared by `DiskGroupView` disk boxes, `ZoneGrid`,
  and the per-rack/per-node bars.
- `busyPct`, `formatBytes` — formatting helpers.

`useZoneBitmap(dg, disk, zone)` fetches the zone bitmap on demand
when a zone is clicked and caches the last result; the 3 s poll
refetches the focused zone via its `refresh` callback.

## 16. Console-Shared DiskDB Client + CLI

### 16.1 Console-shared client

`ConsoleClient` in `crowdb-console-shared` is the typed REST client used
by both the web UI (via `api.ts` wrappers) and `crowdb-cli`. It has
diskdb runtime methods + serde model types so the CLI and UI share one
deserialization path.

```rust
impl ConsoleClient {
    pub async fn list_diskdb_instances(&self) -> Result<Vec<DiskdbInstanceInfo>>
    pub async fn query_diskdb_usage(&self, dg: Option<u64>, disk: Option<String>, zone: Option<u32>) -> Result<UsageResponse>
    pub async fn get_scan_status(&self, dg: Option<u64>) -> Result<ScanSummary>
    pub async fn trigger_scan(&self, dg: Option<u64>) -> Result<ScanSummary>
    pub async fn recalc(&self, dg: Option<u64>) -> Result<RecalcResult>
    pub async fn compact(&self, disk_id: &str, zones: Option<Vec<u32>>) -> Result<CompactionResult>
    pub async fn rebuild(&self, disk_id: &str, zone: Option<u32>) -> Result<RebuildResult>
    pub async fn set_disk_status(&self, disk_id: &str, status: HwStatus) -> Result<()>
}
```

Serde model types (mirrors of the flatbuffer responses):
`DiskdbInstanceInfo`, `DiskGroupUsageSummary`, `DiskGroupUsage`,
`DiskUsage`, `ZoneUsage`, `ScanSummary`, `RecalcResult`,
`CompactionResult`, `RebuildResult`, `UsageResponse`.

### 16.2 CLI subcommands

Runtime queries (usage/zones/scan/recalc/compact/rebuild) are
reachable from the command line via `crowdb diskdb` subcommands.
Lifecycle stays in `crowdb disk` / `crowdb disk-group`; `diskdb` is
runtime queries only.

```
crowdb diskdb status                          — /api/diskdb/instances
crowdb diskdb usage [--dg <id>] [--disk <id>] [--zone <zi>]
crowdb diskdb scan [--dg <id>]                — trigger
crowdb diskdb scan-status [--dg <id>]
crowdb diskdb recalc [--dg <id>]
crowdb diskdb compact <disk_id> [--zones <zi,...>]
crowdb diskdb rebuild <disk_id> [--zone <zi>]
```

All route through `ConsoleClient` → `crowdb-web` → `DiskdbClient` →
crowdb-rpc; no direct talk to `crowdb-diskdb`.

## 17. Native Access and Chunk diagnostics

- Access endpoints are deployment inputs, separate from authoritative hardware
  topology. Container receives finite origins from its profile; standalone
  accepts a persisted HTTP origin without embedded credentials or path.
  Access currently has no Group 0 endpoint registration to discover.
- Entering Iceberg automatically loads the Catalog belonging to the Console
  deployment. The resource sidebar has no endpoint or credential inputs and no
  connection action. Unavailable services show an explicit retry state.
  The fixed reader credential stays in Web process state and is used only for
  GET/HEAD. The endpoint cannot be changed through the browser configuration API;
  a separate cluster requires a separate deployment binding.
- The Web proxy accepts a fixed protocol and operation path, preserves native
  status/authentication/ETag/range headers, forwards native credentials, and
  refuses redirects. Request URLs cannot select arbitrary upstream hosts.
- Iceberg mutations use catalog REST requirements and updates. UUID assertions
  identify the table; concurrent commits retain forms and require metadata
  refresh. They use the existing authorized management session; automatic read
  access does not authorize mutations. Reader/manager/writer authorization remains
  native service policy.
- S3 signs canonical upstream host/path/query/payload in the browser using
  WebCrypto. Secrets remain session inputs, excluded from persisted config and
  activity. Large uploads use serial 8 MiB parts, bounded by the proxy's 16 MiB
  request cap. Cancellation leaves the UploadId available for inspection/abort.
- Downloads stream into a supported browser file writer. The compatibility
  fallback permits only known sizes at most 16 MiB. Preview consumes at most
  4 KiB even when an upstream ignores Range.
- Chunk listing scans bounded windows from live registered owners, merges exact
  IDs, and applies type/hex-prefix filtering. Partial owner failure exposes its
  identity and suppresses continuation until recovery. Routed detail retains
  layout revision and stable Strip sequence; placement has a separate timestamp
  and an explicit unknown state when hardware metadata cannot be read.
- Protocol integers beyond JavaScript's exact range are represented as strings.
  Disk/Chunk IDs use complete 128-bit identities; fragment offsets use BigInt.
  Chunk/Strip capacity and logical offsets are KiB; acknowledged cursor is bytes.
- New workbenches expose bounded browser-session activity. It is operational
  feedback, not a persistent audit record.

- Chunk Strip rendering uses 20-entry pages with stable sequence selection;
  multipart upload and part lists use native continuation markers. Changing
  native credentials clears the corresponding resource scope and loaded data.

## 18. Fixed-cluster operator flow

- A new cluster opens Cluster. An established deployment restores the last
  valid domain and selection. There is no per-domain connect action.
- The operator path is Cluster deployment → KV and Capacity configuration →
  Iceberg/S3 use → lower-layer diagnosis through resource links. This is
  guidance, not a mandatory wizard or a requirement that every service type
  exist for every operation.
- Cluster presents readiness per capability: authoritative registration and
  quorum, relevant data groups, DiskIO and storage bindings, writable capacity,
  ChunkDB/Chunk-KV ownership, and Access protocol availability as applicable.
  A live process alone does not make a capability ready. Unavailable workflows
  identify the missing dependency and link to its owning domain.
- All service lifecycle operations target an exact instance and service type.
  Unsupported types fail explicitly; they never fall through to KV actions.
  Async operations retain progress, outcome, and actionable errors. Removing a
  dependency must not silently cascade to dependent resources.
- Iceberg and S3 use this deployment's Access binding. Protocol setup belongs
  to cluster configuration; browsing does not require endpoint/token forms.
  Automatic read access does not grant mutation privileges. Protocol-specific
  authorization still applies; secret delivery and S3 signing integration are
  implementation work, not permission to expose a server credential.
- The right property panel is contextual: useful for Cluster, KV, Capacity,
  Chunk, and Chunk-KV selections; optional for S3 objects; absent in Iceberg.
  Full Tree/Journal/file views remain central, not squeezed into properties.

Additional invariants:

- **I6 — Cluster binding:** every page and cross-link uses the same cluster;
  a test deployment cannot supply one domain of another cluster's Console.
- **I7 — Bounded observation:** lists, trees, graph layouts, requests, fan-out,
  response bytes, caches, and history have explicit bounds. Filtering never
  requires loading the complete dataset into the browser.
- **I8 — Honest completeness:** unknown, unavailable, partial, empty, and zero
  are distinct. Counts identify their scope and completeness; unavailable
  global totals are not obtained by an unbounded foreground scan.
- **I9 — Observation identity:** responses carry their source and observation
  identity/time. Ownership changes invalidate stale continuations or trigger
  an explicit refresh; mixed catalog generations cannot form one split map.
- **I10 — Stable selection:** changing scope cancels stale work. Paging does
  not silently replace an inspected object. Hidden pages suspend polling;
  navigation retains bounded state and exact integer identities.

## 19. Chunk explorer

Chunk is a top-level domain independent of Capacity. Its purpose is to find
logical chunks across metadata stores and explain their physical placement.

### 19.1 Types, metadata sources, and ownership

- Chunk type is the protocol's extensible `ChunkType`: Repo, Wal, BtreePage,
  PageIndex, Stream, S3, and IcebergTable. Unknown numeric values remain
  inspectable. A type is not a metadata-backend discriminator.
- Metadata source is an independent dimension: Paxos KV Store/Group or
  Chunk-KV Partition. Supporting chunks for Chunk-KV trees/streams and large
  populations such as repository chunks must be discoverable through their
  authoritative source, not assumed to share one persistence route.
- Distinguish business owner (repository/tree/stream/table), metadata location,
  serving instance, and physical placement. The word "owner" alone is
  insufficient as a column or property label.
- Read actual bindings and records to resolve these relationships. Legacy
  unattributed records and unresolved mappings display an explicit unknown
  value. Type alone never supplies a fabricated ownership link.

### 19.2 Navigation and detail

- Left navigation starts with All types and individual types, then optionally
  scopes by metadata source and Store/Group or Partition. Large populations
  remain paged lists rather than one tree node per chunk.
- Central filters include type, exact ID or prefix, state, metadata source,
  Store/Group or Partition, business owner, and physical node/disk when that
  relationship is queryable. Unsupported filters are identified explicitly.
- Results expose full ID, type, state, capacity/length, metadata location, and
  business owner when known. Selecting a result opens structured metadata and
  Strip/Mirror/EC layout. Stable Strip sequence identifies selection.
- The property panel describes the selected strip/fragment and provides
  links to Capacity Disk, Cluster Node, owning Tree/Journal, or metadata
  Group/Partition. Placement observations retain their own timestamps.

### 19.3 Query contract

- A query is bounded at the authoritative source by count, bytes, work, and
  deadline. Type/source filtering should be routed or indexed there. If only
  a bounded scan window is available, report scanned versus matched counts
  and continuation explicitly; an empty window does not mean no matches.
- Multi-source queries use bounded fan-out and source-aware continuation.
  Missing sources are named as partial coverage; retries cannot skip their
  unseen records by advancing a shared cursor past them.
- Do not infer global totals from one page or scan every backend to draw type
  badges. Stable identity deduplication must retain provenance and surface
  conflicting observations rather than arbitrarily selecting an owner.
- Chunk detail and placement can fail independently. A missing placement
  lookup does not erase a successfully read chunk or its disk identities.

## 20. Chunk-KV workbench

Depends on [Chunk-Backed Range KV](../chunkds/design-crowdb-chunk-kv.md),
[Chunk KV Server](../chunkds/design-crowdb-chunk-kv-server.md), and
[Chunk Stream](../chunkds/design-crowdb-chunk-stream.md).

### 20.1 Identity and navigation

The independently owned result of a split is a Partition: stable identity,
half-open binary key range, owner epoch, tree, and distinct journal stream.
Partition count is independent of node count. The left tree is:

```text
Rack
  Node
    Chunk-KV Server
      Partition [start, end)
```

Catalog assignment and observed runtime ownership are shown separately when
they disagree or one is unavailable. An unavailable server's assigned
partitions remain visible; unknown placement is an explicit unresolved scope.
Server lifecycle actions link to Cluster.

### 20.2 Distribution and split maps

- The default center shows ordered key ranges grouped by server/node. Selecting
  a rack or node highlights its ownership while preserving global context.
  Color and labels identify serving, recovery, split, transfer, and failure.
- Arbitrary binary ranges have no meaningful linear width. Default blocks
  communicate order and boundaries; an optional size mode requires measured
  bytes and labels unavailable measurements.
- A separate relationship view shows parent/child lineage, split boundary,
  active dependencies, and source/target transfer. Historical transitions are
  displayed only if retained evidence exists; current catalog state is not a
  complete split history.
- Maps aggregate at large scale and progressively load bounded partition
  pages. Search accepts exact partition identity or a key whose owning range
  is resolved by the catalog. Zoom never fetches the entire tree or journal.
- A catalog generation defines a coherent range map. Refresh exposes stale
  observations and reconciles the selected partition by stable identity.

### 20.3 Partition detail

Selecting a partition opens a central workbench with breadcrumbs to the map:

- **Overview:** exact range, owner, epoch, catalog generation, lifecycle,
  serving readiness, and current split/transfer phase with source and target.
- **Tree:** tree ID, root/checkpoint identity, memory and persisted structure,
  and available page statistics. Actual pages expand lazily with depth/count/
  byte limits and link to their supporting chunks. Missing inspection data is
  explicit; a generic diagram must not masquerade as the real tree.
- **Journal:** stream identity, durable/applied/checkpoint frontiers, trim
  position, active and sealed extents, and recovery lag. An extent links to
  its underlying chunk. Sequence numbers and byte offsets are separately
  labelled; counter differences are meaningful only within the same stream
  and sequence namespace. Record decoding is explicit and bounded.
- **Dependencies:** retained parent overlay, inheritance boundary, pinned
  tree/stream references, and materialization progress. Show phase and measured
  work; do not invent percentage completion from a phase name.

The right panel holds selected page/extent properties. Tree and Journal use
the full central workbench rather than becoming property-panel-only views.

### 20.4 Split and transfer correctness

- Split retains the parent's partition/tree/stream identity and lower range,
  and creates one child for the upper range. Both initially remain on the same
  owner. Distribution is a subsequent balance operation.
- A child has its own journal but can recover through an exact base tree,
  a range-filtered parent-stream suffix through cutover, and then its own WAL.
  Journal visualization uses distinct parent and child tracks with labelled
  cutover boundaries; their byte offsets are never concatenated as one stream.
- Serving does not imply independent recovery. Until materialization and the
  authoritative catalog update clear the overlay, retained references and
  pins remain visible. UI links must preserve the relevant generations.
- During transfer, distinguish catalog assignment, target preparation/catch-up,
  and actual serving authority. A target appearing in the catalog does not by
  itself prove readiness or permit the source to resume writes.
- Initial scope is observation. Manual split, migration, and repair controls
  need separate operation contracts before becoming active UI actions.

## 21. Implementation boundaries

The agreed target is not a claim that all inspection or lifecycle APIs exist.
The remaining integration boundaries are:

- Capacity and Chunk now have separate domain identities, and Chunk-KV has a
  top-level catalog workbench. `domain=Capacity` selects physical capacity;
  `domain=Chunk` selects the Chunk explorer, including for embedding hosts.
  Legacy capacity links must explicitly migrate to `Capacity`.
- Extend typed Cluster lifecycle dispatch beyond KV/DiskDB. Reuse existing
  deployment capabilities where present; unsupported types must not fall
  through to another service's operation.
- KV opens on the Paxos overview, with group membership, replica placement,
  election terms and read frontiers. Data mounts on first use, inherits a
  selected Store/Group/Replica scope, and cancels scans when hidden or when
  scope changes. Ordinary Put requires a specific group. Group 0 is read-only.
  Group/replica tables render 100-row windows; data retains at most 1,000 rows
  and All Groups scans accept at most 10 groups. These limits do not yet bound
  the logical topology loader's aggregate response or detail-fetch fanout.
- Resolve the authoritative Repo and other Chunk-KV metadata records and
  their query paths. The inspected `ChunkStore` currently uses Paxos KV bucket
  bindings; the current Web list queries ChunkDB owners and post-filters scan
  windows. Neither establishes complete multi-backend enumeration.
- The catalog workbench reads one referenced page with a generation-bound
  continuation, verifies checksums and range fences, and returns at most 100
  entries. It reads the head again before responding. Budgets are 1 MiB per
  head, 2 MiB per page, 4096 page references, and a five-second request deadline.
  Validation covers the referenced page, not a full global catalog audit.
- Selecting a Chunk-KV partition reads one owner runtime observation, fenced
  by catalog generation and owner epoch. Web resolves its HTTP origin only from
  the current cluster's configured Chunk-KV service with the matching RPC
  endpoint, verifies partition/owner/tree/stream identities, and rechecks Group 0
  generation. Missing management endpoints remain explicitly unavailable.
  Runtime responses are capped at 64 KiB with a five-second overall deadline;
  no key scan or journal data read occurs. Lifecycle, admission, serving grant,
  durable sequence/offset and applied sequence are independently sampled.
  A Serving lifecycle does not prove a live serving grant. Tree-page, checkpoint,
  stream-extent and transition inspection remain to be implemented.
- Complete S3 fixed-cluster authentication/signing integration and keep native
  privileges explicit. Complete real Iceberg reference-chain and footer
  acceptance; browser fixtures alone do not establish parser coverage.
- Verify large populations, partial owners, stale generations, unavailable
  dependencies, and cross-domain return navigation. Validate against the same
  cluster used by the Console, with isolated fixtures clearly distinguished.
