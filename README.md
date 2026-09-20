<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

[![CI](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml/badge.svg)](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

CROWDB is a distributed storage platform for objects, tables, and AI datasets.
It owns the data path from S3, Iceberg, and native Dataset access through
distributed metadata and chunk storage to disk—and eventually GPU memory.

S3, Iceberg, and Dataset are first-class access models, not wrappers stacked on
top of one another.

- Use **S3** for familiar object access.
- Use **Iceberg** for native catalogs, tables, snapshots, and immutable files.
- Use **Dataset** for samples, shards, tensors, batches, and direct data access.

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
|   [implemented]          [in progress]           [design]                |
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
|                                      |                  |                |
|   Data path: chunk client -> Chunk I/O -> ChunkDB -> DiskIO -> DiskDB    |
|                                                        |                 |
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
datasets are native access models over the same distributed storage core. They
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
  being wrappers around one another. Dataset can also route directly to the
  data topology and is designed toward RDMA and direct GPU delivery.

## Where It Stands

| Access model | Status      | What it means                                         |
| ------------ | ----------- | ----------------------------------------------------- |
| S3           | Implemented | Core HTTP object operations and bounded streaming     |
| Iceberg      | In progress | Native core format v1, v2, and v3 storage semantics   |
| Dataset      | Design      | HTTP, topology-aware native client, and GPU delivery  |

The KV, tree, DiskDB, ChunkDB, chunk I/O, Chunk Stream, Chunk-KV, RPC,
operations console, and core S3 foundation have working implementations. See
the [backlog](doc/backlog/backlog.md) for current delivery scope.

## Quick Start

CROWDB uses [Pixi](https://pixi.sh) to pin its Rust and C++ toolchains and
native dependencies.

### S3 cluster example

Build the binaries, then start a local S3 cluster with the CLI:

```bash
curl -fsSL https://pixi.sh/install.sh | sh
pixi run build

./target/release/crowdb-cli s3 cluster start --root /tmp/s3-cluster
./target/release/crowdb-cli s3 cluster status --root /tmp/s3-cluster
```

The command starts the storage services, S3 endpoint, and Web management
server. Open the printed Web URL, normally
[http://127.0.0.1:14000/](http://127.0.0.1:14000/).

Create a bucket and round-trip an object:

```bash
./target/release/crowdb-cli s3 bucket put --root /tmp/s3-cluster bucket1
./target/release/crowdb-cli s3 object put --root /tmp/s3-cluster bucket1 hello.txt \
  --text "hello from CROWDB"
./target/release/crowdb-cli s3 object get --root /tmp/s3-cluster bucket1 hello.txt
```

## Explore

- [Access architecture](doc/design/access-server/design-crowdb-access-server.md)
  — S3, Iceberg, Dataset, native access, and GPU delivery.
- [Documentation index](doc/doc_index.md) — every permanent architecture and
  subsystem design.
- [User guide](doc/user-manual/user-guide.md) — setup, console, CLI, and
  supported operations.
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
