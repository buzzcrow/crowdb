<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Backlog Index

This index contains only requirements that are not complete. Completed
requirements and their temporary execution plans are removed during the final
`implement-requirement` cleanup. Requirement detail and acceptance criteria
live in the linked document; implementation follows
[`implement-requirement`](../../.agents/skills/implement-requirement/SKILL.md).

**Next R number: R245**

## Dataset and access

- **[R233](R233-sdk-local-cache-checkpoint.md)** — optional local disk read cache
  and recoverable checkpoint staging with explicit remote durability.
  **Area:** SDK. **Complexity:** High. **Status:** Deferred until dedicated HTTP workload measurements.
- **[R220](R220-dataset-fat-client-plan.md)** — Dataset SDK fat-client direct
  access and optimization contract. **Area:** Dataset SDK. **Complexity:** High.
  **Status:** Deferred until the core Dataset contract has workload measurements.

## Access server and external data

- **[R168](R168-s3-shared-object-reclamation.md)** — shared small-object
  reclamation. **Area:** access server / S3 / chunkdb. **Complexity:** High.
  **Status:** Deferred until R95 and the basic S3 delete path are stable.
- **[R169](R169-s3-shared-chunk-tree-gc.md)** — shared-chunk and B+tree garbage
  collection. **Area:** access server / S3 / crowdb-tree / chunkdb.
  **Complexity:** High. **Status:** Deferred until R147, R168, and workload
  measurements are complete.
- **[R170](R170-s3-cuobject-rdma.md)** — optional cuObject RDMA data plane.
  **Area:** access server / S3 / DiskIO / RDMA. **Complexity:** High.
  **Status:** Deferred until the basic TCP S3 path is stable and measured.
- **[R240](R240-rdma-datapath-stream.md)** — RDMA S3/HTTP/DiskIO stream using
  existing ChunkIO aggregation. **Area:** access server / S3 / DiskIO / RDMA.
  **Complexity:** High. **Dependencies:** R238 and R239.
- **[R241](R241-rdma-chunk-kv-access.md)** — RDMA Access Server to Chunk-KV
  transport with bounded metadata and aggregate messages. **Area:** access
  server / Chunk-KV / RDMA. **Complexity:** High. **Dependencies:** R238 and
  R239.
- **[R242](R242-rdma-fat-client-sdk.md)** — native fat-client access SDK and
  FFI buffer leases. **Area:** access server / SDK / RDMA. **Complexity:** High.
  **Dependencies:** R238, R239, and R241.
- **[R185](R185-access-iceberg-cache-invalidation.md)** — bounded Iceberg cache
  and invalidation. **Area:** access server / Iceberg / Group 0 / Chunk-KV.
  **Complexity:** Medium. **Status:** Deferred pending focused cache measurements.
- **[R186](R186-access-iceberg-orc-validation.md)** — selected ORC validation.
  **Area:** access server / Iceberg. **Complexity:** Medium. **Status:**
  Deferred as an independent follow-up by user decision.
- **[R196](R196-refine-benchmark-tests.md)** — refine benchmark tests: external
  CLI workloads and shared collection against a three-node container cluster;
  record results in each driver. **Area:** CLI / storage / access / benchmarks.
  **Complexity:** Medium. **Dependencies:** existing OCI and service contracts.

## Chunk, Chunk-KV, and recovery

- **[R83](R83-chunkdb-complete-recovery-flow.md)** — complete data recovery and
  recovery-speed control. **Area:** chunkdb / diskdb / DiskIO.
  **Complexity:** High. **Dependencies:** chunkdb server and DiskIO recovery path.
- **[R84](R84-chunkdb-post-disk-move-placement-scanner.md)** — post-move chunk
  placement scanner. **Area:** chunkdb / diskdb. **Complexity:** Medium.
  **Dependencies:** R81 Part 2 and the chunkdb server.
- **[R92](R92-chunkdb-in-chunk-gc.md)** — in-chunk garbage-collection operations.
  **Area:** chunkdb / chunk IO. **Complexity:** High. **Dependencies:** chunk
  lifecycle and qualified range ownership.
- **[R95](R95-chunkdb-chunk-range-delete.md)** — qualified chunk-range deletion
  and orphan scanning. **Area:** chunkdb / chunk IO / S3.
  **Complexity:** High. **Dependencies:** chunk-range ownership records.
- **[R96](R96-chunkdb-console-cli-integration.md)** — chunkdb CLI management
  integration. **Area:** chunkdb / console / CLI. **Complexity:** Medium.
  **Dependencies:** stable chunkdb management surface.
- **[R147](R147-tree-chunk-gc.md)** — reclaim B+tree chunk strips. **Area:**
  crowdb-tree / chunkdb / diskdb. **Complexity:** High. **Status:** Deferred
  until immutable page packs and durable reclaim candidates are available.
- **[R148](R148-chunk-stream-scale-out.md)** — stream metadata scale-out and
  sealed-chunk EC. **Area:** chunk-stream / chunk-kv / KV / chunkdb.
  **Complexity:** High. **Status:** Deferred until the measured single-group
  mirror path is complete.
- **[R193](R193-chunkdb-node-failure-budget.md)** — configurable node-failure
  budget and EC placement. **Area:** KV / chunkdb / chunk IO / deployment.
  **Complexity:** High. **Status:** Planned.
- **[R207](R207-chunkdb-repo-metadata-chunk-kv.md)** — repo metadata and tasks on
  chunk-kv. **Area:** chunkdb / chunk-kv. **Complexity:** High.
  **Status:** Deferred beyond the direct-KV stage.

## KV, DiskDB, and topology

- **[R80](R80-diskdb-rebalance.md)** — disk space rebalance convergence.
  **Area:** diskdb / DiskIO. **Complexity:** Medium.
- **[R82](R82-kv-watch-notify-coalescing.md)** — watch/notify coalescing.
  **Area:** KV / diskdb. **Complexity:** Medium.
- **[R102](R102-diskdb-dynamic-binding-migration.md)** — dynamic disk-group
  binding migration. **Area:** diskdb / KV. **Complexity:** High.
- **[R103](R103-chunkdb-range-migration.md)** — dynamic slot ownership and
  KV-group expansion/shrink. **Area:** chunkdb / KV. **Complexity:** High.
  **Status:** Deferred by user decision until dynamic ownership is requested.
- **[R139](R139-group0-service-config.md)** — single-container and cluster
  performance profiles, test tuning, and Group-0 distributed service configuration. **Area:** configuration / control plane. **Complexity:** Medium.
  **Status:** Deferred until the file-backed service configuration contract.
- **[R144](R144-chunk-kv-partition-merge.md)** — adjacent partition merge.
  **Area:** chunk-kv / KV / group 0. **Complexity:** High. **Status:** Deferred
  until manifest reuse, transfer fencing, and transition recovery are stable.

## Tree and memory

- **[R4](R4-bounded-mempool.md)** — bounded memory pool for tree allocations.
  **Area:** crowdb-tree engine. **Complexity:** Medium.
- **[R5](R5-rdma-alloc.md)** — RDMA host-buffer RPC and data transport umbrella.
  **Area:** RPC / access server / Chunk-KV / DiskIO / SDK / RDMA.
  **Complexity:** High. **Status:** Decomposed into R238–R242.
- **[R238](R238-rdma-registered-buffer-foundation.md)** — bounded registered
  host-buffer classes, ownership, leases, and quarantine. **Area:** RPC / RDMA.
  **Complexity:** High. **Dependencies:** R4 and R5.
- **[R239](R239-rdma-rpc-transport.md)** — RC SEND/RECV RPC transport with
  epoll/CQ integration and TCP fallback. **Area:** RPC / RDMA. **Complexity:**
  High. **Dependencies:** R238.
- **[R60](R60-tree-scan-sibling-leaf-readahead.md)** — sibling-leaf readahead on
  cold scans. **Area:** crowdb-tree / scan / DiskIO. **Complexity:** Medium.
  **Status:** Deferred pending cold file/block-backed measurements.
