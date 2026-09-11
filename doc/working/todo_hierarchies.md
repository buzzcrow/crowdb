<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Source Hierarchy Refactoring Backlog

Audit of every crate and app in the workspace against the domain-hierarchy
layout rules in `.agents/skills/coding/SKILL.md` and
`.agents/skills/review/SKILL.md`. Reference shapes:
`crowdb-kv/src/{cluster,paxos,wal}/` and
`crowdb-tree/src/{btree,maptable,memtable,snapshot,backend}/`.

Each target section shows the proposed `src/` (and `include/` for C++) tree.
Files marked with `*` are oversized (>1000 lines) and should be split before
or during the move. `→` marks a rename. Root files are entry points, facades,
ABI boundaries, configuration, or primitives genuinely shared across
domains — each root file has a justification note explaining why it stays.

No `util/`, `core/`, `misc/`, or `helpers/` folders. Helpers move into their
owning domain folder; cross-domain primitives stay at root with a clear
concept name.

## Completed

- [x] `lib/crowdb-tree` — moved flat root headers/sources into `btree/`,
      `maptable/`, `memtable/`, `snapshot/`, `backend/`; renamed `mtable/` to
      `maptable/`; renamed `Options` to `Config` (uncommitted, working tree).
- [x] Coding skill — added domain-hierarchy, small-roots, clear-naming, and
      touched-file-ownership rules.
- [x] Review skill — added whole-root audit, abbreviation, and folder-name
      consistency checks.

## High priority — fully flat, large, clear domain groupings

### lib/crowdb-kv-client (20 files, 8,710 lines)

Five domain groupings: binding (owner problem), service (group-0 registry),
transport (RPC + cluster meta), hardware (hw hierarchy + sysmd), and client
(the core client + its impl splits + topology cache). `topology.rs` moves
into `client/` — it is only used by `client.rs`. `ffi.rs` is a feature-gated
ABI boundary. `config.rs`, `error.rs`, `metrics.rs` are conventional root
files used across all domains.

```
lib/crowdb-kv-client/src/
├── lib.rs                      facade, re-exports
├── config.rs                   ClientConfig, RetryConfig (used by client + hardware + transport)
├── error.rs                    Error, Result (used by all domains)
├── metrics.rs                  ClientMetrics (used by client + transport)
├── ffi.rs                      C ABI (feature = "ffi", ABI boundary)
├── binding/
│   ├── binding.rs              → binding_framework.rs (BindingStrategy trait)
│   ├── chunkdb_strategy.rs     → chunkdb_binding_strategy.rs
│   └── range.rs                → range_binding.rs
├── service/
│   ├── discovery.rs            → service_discovery.rs
│   ├── registry.rs             → service_registry.rs
│   └── watch_notify.rs
├── transport/
│   ├── rpc_transport.rs        → kv_rpc_transport.rs *
│   └── cluster.rs              → kv_cluster.rs (KVClusterMetaClient/Admin)
├── hardware/
│   ├── hardware.rs             *
│   ├── space_usage.rs
│   └── sysmd.rs                CrowdbSysmdClient facade
└── client/
    ├── client.rs               → client.rs * (split: core, routing, scan)
    ├── retry.rs                → client_retry.rs
    ├── admin.rs                → client_admin.rs
    └── topology.rs             ← moved from root (only used by client.rs)
```

- [x] Create `binding/` domain: move `binding_framework.rs`,
      `chunkdb_binding_strategy.rs`, `range_binding.rs`
- [x] Create `service/` domain: move `service_discovery.rs`,
      `service_registry.rs`, `watch_notify.rs`
- [x] Create `transport/` domain: move `kv_rpc_transport.rs`, `kv_cluster.rs`
- [x] Create `hardware/` domain: move `hardware.rs`, `space_usage.rs`,
      `sysmd.rs`
- [x] Create `client/` domain: move `client.rs`, `client_retry.rs`,
      `client_admin.rs`, `topology.rs`
- [x] Keep at root: `lib.rs`, `config.rs`, `error.rs`, `metrics.rs`, `ffi.rs`
- [x] Split `client.rs` (1,599 lines) — candidates: routing/topology refresh,
      scan, journal ops
- [x] Split `kv_rpc_transport.rs` (973 lines) if it grows further
- [x] Update `lib.rs` module declarations and re-exports
- [x] Update all callers across the workspace

### lib/crowdb-chunk-kv (10 files, 2,334 lines)

The crate is one domain — the partition state machine — but `partition.rs`
(1,364 lines) is the root and `frame.rs`, `journal.rs`, `tree.rs` are its
sub-modules. `types.rs` is a catch-all the skill explicitly flags; split it
by domain owner. Use `partition.rs` + `partition/` (non-`mod.rs` layout).

```
lib/crowdb-chunk-kv/src/
├── lib.rs                      facade, re-exports
├── error.rs                    ChunkKvError (used by all modules)
├── metrics.rs                  PartitionMetrics (used by partition + manager)
├── manager.rs                  PartitionManager (facade over partitions)
├── memory.rs                   test-only (feature = "test-util")
├── partition.rs                Partition core * (split: state, mutations, split)
└── partition/
    ├── frame.rs                wire frame codec
    ├── journal.rs              PartitionJournal trait + stream impl
    ├── tree.rs                 PartitionTree trait
    └── types.rs                partition-scoped types (from types.rs split)
```

Shared types (`PartitionId`, `PartitionRange`, `JournalPosition`, etc.) stay
in `partition.rs` or move to `partition/types.rs`; types not owned by the
partition (e.g. `Checkpoint`) get a descriptive home.

- [x] Create `partition/` domain: move `frame.rs`, `journal.rs`, `tree.rs`
      below `partition.rs`
- [x] Split `types.rs` by domain owner; rename or relocate each piece
- [x] Split `partition.rs` (1,364 lines) — candidates: lifecycle state,
      mutation apply, split/merge
- [x] Keep at root: `lib.rs`, `error.rs`, `metrics.rs`, `manager.rs`,
      `memory.rs`
- [x] Update `lib.rs` module declarations and re-exports

### lib/crowdb-chunk-stream (7 files, 1,899 lines)

Only 7 files and each is already one domain concept (stream, storage, memory,
metadata). The hierarchy is acceptable flat; the real issue is `stream.rs`
(1,166 lines) being oversized. No folder moves proposed — just the split.

```
lib/crowdb-chunk-stream/src/
├── lib.rs                      facade, re-exports
├── error.rs                    StreamError (used by all modules)
├── metrics.rs                  StreamMetrics (used by stream)
├── storage.rs                  StreamChunkStore trait (stream + memory)
├── metadata.rs                 manifest resolution (stream + memory)
├── memory.rs                   test-only (feature = "test-util")
└── stream.rs                   ChunkStream * (split: append, read, trim, fence)
```

- [x] Split `stream.rs` (1,166 lines) — candidates: append path, read path,
      trim/GC, fencing/epoch
- [x] No folder moves needed — flat is the correct shape at this size

### lib/crowdb-chunk-kv-client (9 files, 1,584 lines)

Two domain groupings: `compose/` (multi-scan/multi-get composition) and
`catalog/` (catalog routing + request identity). The core client, config,
transport, and error stay at root.

```
lib/crowdb-chunk-kv-client/src/
├── lib.rs                      facade, re-exports
├── client.rs                   ChunkKvClient (used by compose + catalog)
├── config.rs                   ClientConfig (used by client)
├── error.rs                    ClientError (used by all domains)
├── transport.rs                ChunkKvTransport trait (used by client + compose)
├── compose/
│   ├── ordered.rs              multi-scan ordering
│   └── batch.rs                → compose.rs (batch/multi-get composition)
└── catalog/
    ├── catalog.rs              CatalogCache, CatalogMap
    └── identity.rs             RequestIdentityAllocator
```

- [x] Create `compose/` domain: move `ordered.rs`, `compose.rs`
- [x] Create `catalog/` domain: move `catalog.rs`, `identity.rs`
- [x] Keep at root: `lib.rs`, `client.rs`, `config.rs`, `error.rs`,
      `transport.rs`
- [x] Update `lib.rs` module declarations and re-exports

### app/crowdb-chunk-kv-server (11 files, 1,697 lines)

Two domain groupings: `serving/` (domain serving grants, leases, monitoring,
partition transfer, rebalancing) and `catalog/` (catalog store + scan
validation). The server, config, and metrics stay at root.

```
app/crowdb-chunk-kv-server/src/
├── lib.rs                      facade
├── main.rs
├── server.rs                   HTTP + RPC server *
├── config.rs                   server configuration
├── metrics.rs                  ServerMetrics
├── serving/
│   ├── lease.rs                serving grants + leases
│   ├── monitor.rs              domain monitor
│   ├── transfer.rs             partition transfer protocol
│   └── balance.rs              rebalance planning
└── catalog/
    ├── catalog.rs              catalog store
    └── scan.rs                 scan validation
```

- [x] Create `serving/` domain: move `lease.rs`, `monitor.rs`,
      `transfer.rs`, `balance.rs`
- [x] Create `catalog/` domain: move `catalog.rs`, `scan.rs`
- [x] Keep at root: `lib.rs`, `main.rs`, `server.rs`, `config.rs`,
      `metrics.rs`
- [x] Update `lib.rs` module declarations and re-exports

## Medium priority — partially flat, some root files misplaced

### lib/crowdb-rpc (C++)

`co_client.h` is a coroutine client stranded at the include root; it belongs
under `client/`. The remaining root files are the RPC library's foundational
vocabulary — each is used by 3+ domains (client, server, transport) and
represents a major concept, not a helper:

- `buffer.h` — ref-counted byte buffer (used by client, server, transport,
  connection, framing: 5 domains)
- `framing.h` — wire frame codec (used by client, server, transport,
  connection: 4 domains)
- `connection.h` — connection abstraction over transport (used by client,
  server, transport: 3 domains)
- `pool.h` — ConnectionPool for peer connections (used by pool.cpp +
  external `dio_server.h`: cross-cutting)
- `scheduled_executor.h` — thread pool executor (used by diskio app +
  group0: external cross-cutting)
- `rpc_metrics.h` — latency/bandwidth metrics (used by client + server)
- `transport.h` — transport umbrella header
- `c_api.h`, `c_api_internal.h` — C ABI boundary

```
lib/crowdb-rpc/include/crowdb-rpc/
├── buffer.h                    ref-counted byte buffer (5 domains)
├── c_api.h                      C ABI boundary
├── c_api_internal.h             C ABI internals
├── connection.h                 connection over transport (3 domains)
├── framing.h                    wire frame codec (4 domains)
├── pool.h                       ConnectionPool (cross-cutting)
├── rpc_metrics.h                latency/bandwidth metrics (2 domains)
├── scheduled_executor.h         thread pool executor (external)
├── transport.h                  transport umbrella
├── client/
│   ├── client.h
│   ├── co_client.h             ← moved from root (coroutine client)
│   └── rpc_client_metrics.h
├── server/
│   ├── handler.h
│   ├── message.h
│   └── server.h
└── transport/
    ├── epoll/
    ├── kqueue/
    ├── rdma/
    └── socket_transport.h
```

- [x] Move `include/crowdb-rpc/co_client.h` → `client/co_client.h`; update
      all `#include` paths
- [x] Keep root primitives at root — each is a major cross-domain concept
- [x] Root src files — no action needed

### app/crowdb-kv-server (13 root files + `mgmt/`)

Boot/recovery files form a `recovery/` domain. Background tasks
(keepalive, binding monitor) form a `background/` domain.
`operation_registry.rs` moves into `mgmt/` — it is only used by `mgmt.rs`
and `mgmt/group_ops.rs`. `store_registry.rs` stays at root — it is used by
6+ modules across recovery, background, and mgmt. `engine_collector.rs`
stays at root — it is the C++ metrics bridge wired from `main.rs`.

```
app/crowdb-kv-server/src/
├── lib.rs                      facade
├── main.rs                     entry point
├── cli.rs                      CLI parsing
├── store_registry.rs           KvStoreRegistry (used by 6+ modules)
├── engine_collector.rs         C++ metrics bridge (wired from main)
├── mgmt.rs                     → mgmt/ (already a domain)
├── mgmt/
│   └── operation_registry.rs   ← moved from root (only used by mgmt/)
├── recovery/
│   ├── startup.rs              boot sequence
│   ├── restore.rs              disk restore mode
│   ├── reconcile.rs            group-0 reconciliation
│   └── group_rebuild.rs        PxGroup rebuild
└── background/
    ├── keepalive.rs            service registration heartbeat
    └── binding_monitor.rs      → binding_monitor_wiring.rs
```

- [x] Create `recovery/` domain: move `startup.rs`, `restore.rs`,
      `reconcile.rs`, `group_rebuild.rs`
- [x] Create `background/` domain: move `keepalive.rs`,
      `binding_monitor_wiring.rs`
- [x] Move `operation_registry.rs` → `mgmt/operation_registry.rs`
- [x] Keep at root: `lib.rs`, `main.rs`, `cli.rs`, `store_registry.rs`,
      `engine_collector.rs`
- [x] Update `lib.rs` module declarations and re-exports

### app/crowdb-web (16 root files + `mgmt/`)

Two clear domain groupings: `diskdb/` (diskdb REST proxy + lifecycle) and
`physical/` (physical-tree views). `lifecycle.rs` (1,906 lines) is the
cluster lifecycle facade — it stays at root but needs splitting.
`state.rs` (AppState) is used by 12+ files across all domains — root
primitive. `owner_assignment.rs` is a small helper used only by
`lifecycle.rs` — stays at root alongside its primary consumer.

```
app/crowdb-web/src/
├── lib.rs                      facade
├── main.rs                     entry point
├── error.rs                    error types (used by all handlers)
├── state.rs                    AppState (used by 12+ files: root primitive)
├── mgmt.rs                     → mgmt/ (already a domain)
├── lifecycle.rs                cluster lifecycle * (split: store, group, disk)
├── owner_assignment.rs         owner selection (used by lifecycle.rs)
├── kv.rs                       KV data-plane handlers (thin delegation)
├── spa.rs                      static asset serving
├── corr_id.rs                  middleware
├── expand.rs                   extractor
├── health.rs                   liveness probe
├── diskdb/
│   ├── proxy.rs                → diskdb.rs (REST proxy)
│   └── lifecycle.rs            → diskdb_lifecycle.rs
└── physical/
    ├── physical.rs             per-node primitives
    └── view.rs                 → physical_view.rs (expanded views)
```

- [x] Create `diskdb/` domain: move `diskdb.rs`, `diskdb_lifecycle.rs`
- [x] Create `physical/` domain: move `physical.rs`, `physical_view.rs`
- [x] Split `lifecycle.rs` (1,906 lines) — candidates: store lifecycle,
      group lifecycle, disk lifecycle
- [x] Keep at root: `lib.rs`, `main.rs`, `error.rs`, `state.rs`, `mgmt.rs`,
      `lifecycle.rs`, `owner_assignment.rs`, `kv.rs`, `spa.rs`,
      `corr_id.rs`, `expand.rs`, `health.rs`
- [x] Update `lib.rs` module declarations and re-exports

### lib/crowdb-tree root primitives (C++ — already refactored, documenting)

The crowdb-tree refactoring is complete. These root headers stay at root
because each is a major cross-domain concept used by 3+ subsystems:

- `slice.h` — non-owning key/value byte view (used by btree, maptable,
  memtable, snapshot: 4 domains)
- `buffer.h` — move-only byte container with SBO (used by btree, maptable,
  memtable: 3 domains)
- `epoch.h` — epoch-based reclamation for lock-free reads (used by btree,
  maptable, memtable: 3 domains)
- `c_api.h` — C ABI boundary
- `config.h` — engine configuration
- `crowdb-tree.h` — umbrella C++ interface
- `status.h` — status types (cross-cutting)

No action needed — documented for the review skill's root audit.

## Acceptable as-is — no hierarchy change needed

### lib/crowdb-test-harness (8 files, 2,074 lines)

Each root file is one top-level domain module — one per storage system
(`chunkdb`, `cluster`, `diskdb`, `diskio`, `hardware`) plus shared test
infrastructure (`logging`, `test_dirs`). Flat is the correct shape: there
are no sub-domains within any file, and grouping them into folders would
add a layer with no ownership benefit.

```
lib/crowdb-test-harness/src/
├── lib.rs
├── chunkdb.rs          chunkdb test harness
├── cluster.rs          cluster test harness
├── diskdb.rs           diskdb test harness
├── diskio.rs           diskio test harness (714 lines — largest, but single domain)
├── hardware.rs         hardware mock
├── logging.rs          test logging setup
└── test_dirs.rs        temp directory helpers
```

- [x] No action needed — flat is correct.

### lib/crowdb-chunkdb-client (3 files, 1,968 lines)

Too small for subfolders. `rpc_transport.rs` (1,458 lines) is oversized
but that is a split-file issue, not a hierarchy issue — it's one domain
(the RPC transport) and splitting it would create fragments of the same
concept, not separate domains.

```
lib/crowdb-chunkdb-client/src/
├── lib.rs
├── client.rs           (447 lines)
└── rpc_transport.rs    (1,458 lines — split candidate, not hierarchy)
```

- [x] No hierarchy action needed.
- [ ] Optional: split `rpc_transport.rs` by concern (request build,
      response parse, retry) in a future task.

### lib/crowdb-diskdb-client (3 files, 1,538 lines)

Same shape as chunkdb-client — too small for subfolders. `rpc_transport.rs`
(986 lines) is under the 1000-line threshold.

```
lib/crowdb-diskdb-client/src/
├── lib.rs
├── client.rs           (519 lines)
└── rpc_transport.rs    (986 lines)
```

- [x] No action needed.

### lib/crowdb-diskio-client (2 files, 408 lines)

Tiny — two files, no sub-domains.

- [x] No action needed.

### lib/crowdb-common

Has two separate components: `cpp/` (C++ common library) and `rust/`
(Rust common library). Both are already well-structured.

**C++ side** — root headers are cross-cutting primitives (log, crc32c,
gzip, mpsc_queue, request_id, diskio_uring, compressing_sink) plus a
`metrics/` sub-domain. No action needed — each root header is one
independent primitive.

**Rust side** — has `metrics/` sub-domain already. Root files are
`config.rs`, `ec.rs`, `ec_isal.rs`, `logging.rs`, `report.rs`,
`request_id.rs`, `time.rs` — each one independent primitive. No
action needed.

- [x] No action needed.

## Already hierarchical — reviewed, minor findings only

### lib/crowdb-kv — clean

Root files are all domain roots (`cluster.rs`, `common.rs`, `io.rs`,
`kv.rs`, `paxos.rs`, `rpc.rs`, `wal.rs`) plus `lib.rs` and `metrics.rs`.
Each domain has its sub-modules below it. This is the reference shape.

- [x] No action needed.

### lib/crowdb-tree — clean (refactored by the other AI)

Domain folders: `btree/`, `maptable/`, `memtable/`, `snapshot/`, `backend/`.
Root headers are cross-domain primitives (see root primitives section above).

- [x] No action needed.

### lib/crowdb-chunk-client — clean

Domain folders: `chunk/`, `disk_io/`, `worker/`, `writer/`. Root files
are `lib.rs`, `client.rs`, `config.rs`, `error.rs`, `metrics.rs`,
`traits.rs`, `io.rs`, `benchmark.rs`, `negative_list.rs` — all
top-level concepts or cross-cutting primitives.

- [x] No action needed.
- [ ] Optional: `benchmark.rs` (1,017 lines) is a split candidate.

### lib/crowdb-protocol — minor finding

Has `fbs/`, `fb_wrappers/`, `key/`, `types/` sub-domains. Root files
are a mix of cross-cutting primitives and per-service type modules.

`chunk_kv.rs` (886 lines) is at root — it defines chunk-KV catalog and
serving-grant types. It's a per-service type module like the ones in
`types/`, but it's too large and self-contained to fold into `types/`.
Acceptable at root since it's the sole file for the chunk-KV protocol
surface.

`diskdb_type_util.rs` (193 lines) is at root — it's extension methods
on diskdb proto types. Could move into `types/` but it's business logic,
not wire format, so root is defensible.

```
lib/crowdb-protocol/src/
├── lib.rs
├── bitmap.rs            cross-cutting primitive
├── chunk_id.rs          cross-cutting primitive
├── chunk_kv.rs          chunk-KV protocol types (886 lines)
├── chunk_stream.rs      cross-cutting primitive
├── chunk_task_value.rs  cross-cutting primitive
├── common_type.rs       cross-cutting primitive
├── diskdb_type_util.rs  diskdb type extensions (could move to types/)
├── mgmt.rs              management API types
├── port_alloc.rs        port allocation (475 lines)
├── ports.rs             port constants
├── sysdata.rs           system data types
├── fbs/                 flatbuffer schemas
├── fb_wrappers/         flatbuffer wrappers
├── key/                 key encoding
└── types/               per-service type modules
```

- [x] No hierarchy action needed — structure is sound.
- [x] Optional: move `diskdb_type_util.rs` → `types/diskdb_util.rs` for
      consistency with the `types/` pattern.
- [x] Optional: `port_alloc.rs` (475 lines) could be a `port/` domain
      with `ports.rs` if it grows.

### lib/crowdb-console-shared — minor finding

Has `clients/`, `ops/`, `ssh/` sub-domains. 16 root files, several
large: `lifecycle.rs` (1,373 lines), `config.rs` (1,330 lines),
`cluster_deployer.rs` (833 lines), `monitor.rs` (730 lines).

`cluster_deployer.rs` uses `crate::diskdb::DeployDiskdbBody` — it's
the deployment lifecycle, closely related to `lifecycle.rs`. Could
move into a `deploy/` domain or fold into `lifecycle/` if that
domain is created. But `lifecycle.rs` is a single file, not a folder,
so there's no `lifecycle/` to move into without first splitting it.

`monitor.rs` (730 lines) is the monitor cache — a cross-cutting
primitive used by `cluster.rs` and `ops/`. Stays at root.

`diskdb.rs` (396 lines) is diskdb-specific types used by
`cluster_deployer.rs`. Could group with `cluster_deployer.rs` into a
`deploy/` domain, but only 2 files — marginal benefit.

```
lib/crowdb-console-shared/src/
├── lib.rs
├── clients/             HTTP/console clients
├── ops/                 CLI command operations
├── ssh/                 SSH helpers
├── cluster.rs           cluster view (uses monitor)
├── cluster_deployer.rs  deployment lifecycle (833 lines)
├── config.rs            console config (1,330 lines)
├── corr_id.rs           correlation ID middleware
├── diskdb.rs            diskdb deploy types
├── error.rs             error types
├── expand.rs            recursive depth extractor
├── lifecycle.rs         cluster lifecycle (1,373 lines — split candidate)
├── mgmt.rs              management API helpers
├── monitor.rs           monitor cache (730 lines — root primitive)
├── snapshot.rs          snapshot helpers
└── topology.rs          topology helpers
```

- [x] No hierarchy action needed — structure is sound.
- [ ] Optional: split `lifecycle.rs` (1,373 lines) and `config.rs`
      (1,330 lines) — both are over the 1000-line guideline.
- [ ] Optional: if `lifecycle.rs` is split into `lifecycle/`, move
      `cluster_deployer.rs` and `diskdb.rs` into it as a `deploy/`
      sub-domain.

### app/crowdb-chunkdb — minor finding

Has 8 domain folders: `allocator/`, `conversion/`, `lifecycle/`,
`selector/`, `service/`, `storage/`, `task/`, `topology/`. 16 root
files, several large: `conversion.rs` (1,010 lines), `main.rs` (958
lines), `allocator.rs` (720 lines), `repair.rs` (620 lines).

`repair.rs` uses `crate::conversion::io::ConversionDiskIo` — it's a
conversion-domain consumer. Could move into `conversion/` but it's
also a top-level operation, not a conversion sub-module.

`range_guard.rs` (312 lines) is used by `lifecycle/handler.rs` —
could move into `lifecycle/` but it's a standalone primitive.

`allocator.rs` (720 lines) is the allocator root with `allocator/`
below it — correct shape.

```
app/crowdb-chunkdb/src/
├── lib.rs
├── main.rs              (958 lines — entry point, hard to split)
├── allocator.rs         allocator root → allocator/
├── chunkdb_config.rs    config
├── conversion.rs        conversion root → conversion/ (1,010 lines)
├── lifecycle.rs         lifecycle root → lifecycle/
├── metrics.rs           metrics
├── migration.rs         migration
├── range_guard.rs       range guard (used by lifecycle/handler.rs)
├── repair.rs            repair (620 lines — uses conversion)
├── routing.rs           routing
├── selector.rs          selector root → selector/
├── service.rs           service root → service/
├── storage.rs            storage root → storage/
├── task.rs              task root → task/
└── topology.rs          topology root → topology/
```

- [x] No hierarchy action needed — structure is sound.
- [ ] Optional: split `conversion.rs` (1,010 lines) and `main.rs`
      (958 lines) — both near/over the 1000-line guideline.
- [ ] Optional: `repair.rs` could move into `conversion/` or a
      `recovery/` domain if one is created.

### app/crowdb-diskdb — minor finding

Has 6 domain folders: `liveness/`, `metrics/`, `model/`, `recovery/`,
`scanner/`, `service/`. 12 root files, several large: `main.rs` (640
lines), `ddb_kv_client.rs` (627 lines), `ddb_config.rs` (434 lines).

`ddb_kv_client.rs` (627 lines) is used by 10+ files across `recovery/`,
`scanner/`, `metrics/`, `model/` — it's a cross-cutting primitive (the
diskdb KV client). Stays at root.

`bg_task.rs` (226 lines) uses `ddb_kv_client` — it's a background task
runner. Could group with `health.rs` into a `background/` domain, but
only 2 small files — marginal.

`health.rs` (69 lines) is a liveness probe — root primitive.

```
app/crowdb-diskdb/src/
├── lib.rs
├── main.rs              (640 lines)
├── bg_task.rs           background task runner
├── ddb_config.rs        diskdb config (434 lines)
├── ddb_kv_client.rs     KV client (627 lines — cross-cutting, 10+ users)
├── health.rs            liveness probe
├── liveness.rs          → liveness/
├── metrics.rs           → metrics/
├── model.rs             → model/
├── recovery.rs          → recovery/
├── scanner.rs           → scanner/
└── service.rs           → service/
```

- [x] No hierarchy action needed — structure is sound.
- [ ] Optional: `main.rs` (640 lines) and `ddb_config.rs` (434 lines)
      are under the 1000-line guideline but could be reviewed for
      extractable sub-modules.

### app/crowdb-diskio (C++) — minor finding

Has `disk/`, `engine/`, `group0/`, `rpc/` sub-domains. 4 root files:
`dio_config.cpp`, `dio_config_file.cpp`, `dio_config.h`, `dio_main.cpp`.

`dio_config.h` is a private header (only included by `dio_config.cpp`,
`dio_config_file.cpp`, `dio_main.cpp`, and `group0/group0_sync.h`) —
it's beside its implementation, which is correct for a private header
per the coding skill.

`dio_main.cpp` is the entry point — root.

```
app/crowdb-diskio/src/
├── dio_main.cpp         entry point
├── dio_config.h         config (private header)
├── dio_config.cpp       config impl
├── dio_config_file.cpp  config file parsing
├── disk/                disk abstractions
├── engine/              IO engines (blocking, uring, dummy)
├── group0/              group-0 sync
└── rpc/                 RPC server
```

- [x] No action needed — structure is sound.

### app/crowdb-cli — clean

Has `commands/` with sub-domains (`bench/`, `chunk/`, `cluster/`, `kv/`).
Root files are `main.rs` and `utils.rs` — entry point and a small
utility. No action needed.

- [x] No action needed.

## Summary of remaining work

All 7 high/medium priority crates have been refactored. The remaining
projects fall into three categories:

1. **Acceptable as-is** (5 crates) — too small or correctly flat.
2. **Already hierarchical** (9 crates) — structure is sound, with
   minor optional improvements noted.
3. **Optional split-file tasks** — several files exceed the 1000-line
   guideline but are single-domain; splitting them is a separate
   concern from hierarchy.

No further hierarchy refactoring is required. The optional items
above can be addressed incrementally as those files are touched.
