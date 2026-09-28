<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

[![CI](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml/badge.svg)](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

CROWDB is a distributed storage platform for objects, tables, and AI datasets.
It owns the data path from S3, Iceberg, and native Dataset access through
distributed metadata and chunk storage to disk—and eventually GPU memory.

Version `0.1.0-dev` is the first development release being prepared for public
evaluation. Use disposable data; production use and on-disk upgrade compatibility
are not supported. Dataset and direct GPU delivery remain planned work.

- Use **S3** for familiar object access.
- Use **Iceberg** for native catalogs, tables, snapshots, and immutable files.
- **Dataset** is planned for samples, shards, tensors, batches, and direct data access.

## Three Layers, One Data Path

```text
                              Applications
                    S3 tools   Table engines   AI runtimes
                        |            |             |  |
                        | HTTP       | HTTP        |  | native
                        v            v             v  v
+--------------------------------------------------------------------------+
| LAYER 3 — ACCESS                                                         |
|                                                                          |
|   S3                     Iceberg                 Dataset                 |
|   [implemented]          [implemented]           [design]                |
|   HTTP objects           HTTP tables             HTTP + native client    |
|                                                                          |
|   Access Server serves HTTP. Dataset native access can bypass it.        |
+------------------------------------+-------------------------------------+
                                     |
                                     v
+--------------------------------------------------------------------------+
| LAYER 2 — CHUNK                                                          |
|                                                                          |
|   Distributed structures:     Chunk Stream          Chunk-KV             |
|  |  |  |
|   Data path: chunk client -> Chunk I/O -> ChunkDB -> DiskIO -> DiskDB    |
|  |  |
|   Accelerated path: DiskIO buffer -- RDMA / GDS -------> GPU memory      |
+------------------------------------+-------------------------------------+
                                     |
                                     v
+--------------------------------------------------------------------------+
| LAYER 1 — REUSABLE KV                                                    |
|                                                                          |
|   crowdb-kv   multi-group Paxos   WAL   Group 0   crowdb-tree   RPC      |
|                                                                          |
|   A standalone distributed layer—not the product-level data model.       |
+--------------------------------------------------------------------------+
```

## Why CROWDB?

Storage systems are usually assembled by stacking one system on another: table
metadata over object storage, dataset libraries over table or object APIs, and
new accelerators behind paths designed for disks and CPUs. Every boundary adds
another namespace, lifecycle, RPC, copy, and recovery model. When that boundary
becomes the bottleneck, the layers above it can only work around it.

CROWDB exists to own the complete data path. S3 objects, Iceberg tables, and AI
datasets are intended as native access models over the same distributed storage core. They
share durability, placement, protection, and reclamation without pretending
that one model is merely a convention inside another.

That control matters because both hardware and workloads keep changing. NVMe,
RDMA, GPUDirect Storage, and accelerator offload reshape more than one isolated
module. AI training and inference also need data to reach GPU memory without an
HTTP gateway or client CPU becoming the permanent middleman. Supporting those
changes cleanly requires control from protocol semantics down to buffers and
disk layout.

The goal is a storage foundation that can evolve as one system: simple enough
to reason about, fast enough to justify owning the stack, and composed of
layers that remain useful independently.

## Technical Foundation

- **Parallel consensus:** crowdb-kv runs multiple independent Multi-Paxos slots
  concurrently, with WAL durability, lease reads, and pluggable engines.
- **One protected chunk layer:** bounded streaming, mirrored small data,
  strip-level erasure coding, shard repair, placement, and reclamation serve
  every access model.
- **Chunk-based distributed structures:** Chunk Stream provides durable ordered
  append. Chunk-KV uses range partitions that split and rebalance online while
  reads and writes continue.
- **Native access models:** S3, Iceberg, and Dataset share the core without
  being wrappers around one another. The planned Dataset model targets direct
  topology access, RDMA and GPU delivery.

## Where It Stands

| Access model | Status      | What it means                                        |
| ------------ | ----------- | ---------------------------------------------------- |
| S3           | Implemented | Core HTTP object operations and bounded streaming    |
| Iceberg      | Implemented | Native catalog, FileIO and core v1/v2/v3 semantics   |
| Dataset      | Design      | HTTP, topology-aware native client, and GPU delivery |

The KV, tree, DiskDB, ChunkDB, chunk I/O, Chunk Stream, Chunk-KV, RPC,
operations console, and core S3 foundation have working implementations. See
the [backlog](doc/backlog/backlog.md) for current delivery scope.

## Quick Start

The first Linux amd64 image is being prepared for manual publication. Once
`v0.1.0-dev` is published, start the Iceberg catalog and storage with Docker:

```sh
docker run -d --name crowdb-iceberg \
  -p 127.0.0.1:80:80 \
  crowdb/crowdb-iceberg:v0.1.0-dev
```

Follow the [single-node Docker guide](doc/user-manual/docker-single-node-user-guide.md)
for startup checks, client credentials, persistent volumes and recovery.
For source builds and development with Pixi, see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Explore

- [Access architecture](doc/design/access-server/design-crowdb-access-server.md)
  — S3, Iceberg, Dataset, native access, and GPU delivery.
- [Documentation index](doc/doc_index.md) — every permanent architecture and
  subsystem design.
- [User guide](doc/user-manual/user-guide.md) — setup, console, CLI, and
  supported operations.
- [Single-node Docker guide](doc/user-manual/docker-single-node-user-guide.md)
  — preview image, volume, credentials, clients, and recovery.
- [Backlog](doc/backlog/backlog.md) — what is implemented, in progress, and
  planned.

<details>
<summary><b>Current cluster demos</b></summary>

### Cluster lifecycle

Bootstrap a cluster, register physical topology, create stores and Paxos groups,
and watch replicas elect a leader.

<video src="https://github.com/user-attachments/assets/974d4a44-2446-462e-a9d0-9d9a82d07146" autoplay muted loop></video>

### KV operations

Put, get, scan, and delete through a selected distributed group.

<video src="https://github.com/user-attachments/assets/1fbdcf4e-255a-47e6-b4db-6a1fa0fb0df8" autoplay muted loop></video>

### Failover and replica management

Expand a group, remove its leader, and continue operations after re-election.

<video src="https://github.com/user-attachments/assets/63298646-6eaa-4253-b96a-6f5eb420ad91" autoplay muted loop></video>

</details>

## Notes on AI-Assisted Development

The code in this project was written with AI assistance. The architecture,
naming, module boundaries, and trade-offs remain human choices. AI is the
compiler. The intent is mine.

## License

See [LICENSE](LICENSE).
