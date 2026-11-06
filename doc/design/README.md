<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Design

These overviews explain CROWDB's architecture, capabilities, and design trade-offs. Start with the [product design](design-crowdb.md) for workloads and advantages, then read the [architecture](design-crowdb-architecture.md) to see how the components fit together. Component READMEs provide API usage, configuration, and operational instructions.

For a shorter path through the design:

- **Application access:** choose [Iceberg](design-crowdb-access-iceberg.md), [Dataset](design-crowdb-access-dataset.md), or [S3](design-crowdb-access-server.md). Trusted Dataset and S3 applications can also use the [native SDK](design-crowdb-sdk-native.md).
- **Metadata and recovery:** compare [Paxos-KV](design-crowdb-paxos-kv.md) and [Chunk-KV](design-crowdb-chunk-kv.md), then follow their links to Tree and ChunkStream.
- **Payload I/O:** start with [Chunk storage](design-crowdb-chunkdb.md), then follow allocation, device I/O, and transport into DiskDB, DiskIO, and RPC.
- **Operations:** read [cluster management](design-crowdb-monitor.md) and [container deployment](design-crowdb-deployment.md).

## Product and architecture

| Document                                      | Scope                                                                                          |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| [Product design](design-crowdb.md)            | Workloads, shared storage, independent scaling, coordinated recovery, and design advantages.   |
| [Architecture](design-crowdb-architecture.md) | Component roles, logical/physical placement, serving capacity, recovery, and resource control. |

## Components

| Document                                               | Scope                                                                                                |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| [Iceberg storage](design-crowdb-access-iceberg.md)     | Native writes, direct-URI reads, table commits, and integrated file lifecycle.                       |
| [Dataset storage](design-crowdb-access-dataset.md)     | Sample-oriented storage, small payloads, Snapshot/Split selection, and direct training I/O.          |
| [Access Server and S3](design-crowdb-access-server.md) | Host, S3 buckets and objects, streaming, multipart, and authentication.                              |
| [Native SDK](design-crowdb-sdk-native.md)              | Direct Dataset/S3 access, RDMA/TCP, retained host buffers, capacity budgets, and future GPU work.    |
| [Paxos-KV](design-crowdb-paxos-kv.md)                  | Concurrent slot writes, per-key apply ordering, stores/groups/replicas, and durable metadata.        |
| [Chunk-KV](design-crowdb-chunk-kv.md)                  | Ordered metadata, independent payload placement, smooth split/transfer, and serving capacity.        |
| [Tree storage engine](design-crowdb-tree.md)           | Concurrent skiplists, page mapping, fork/join flush, checkpoints, and external-WAL recovery.         |
| [ChunkStream](design-crowdb-chunk-stream.md)           | Reusable durable byte streams, journal composition, rollover/recovery/trim, and future interfaces.   |
| [Chunk storage](design-crowdb-chunkdb.md)              | Chunk placement, large/small write pipelines, EC, read flow, and repair.                             |
| [DiskDB allocation](design-crowdb-diskdb.md)           | Disk/zone allocation, durable records, capacity accounting, and recovery.                            |
| [DiskIO](design-crowdb-diskio.md)                      | Addressed byte I/O, asynchronous backends, completion, and buffer lifetime.                          |
| [Storage RPC](design-crowdb-rpc.md)                    | RDMA data paths, buffer ownership, resource backpressure, asynchronous completion, and TCP fallback. |
| [Cluster management](design-crowdb-monitor.md)         | System metadata in Group 0, node discovery, service supervision, and console operations.             |
| [Container deployment](design-crowdb-deployment.md)    | OCI packaging, runtime independence, host networking/devices, and CPU/memory limits.                 |

## Usage and validation

| Document                                                                  | Scope                                                                          |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [Dataset HTTP SDK](../../sdk/README.md)                                   | Portable Dataset HTTP SDK for Rust/Python, examples, packaging, and limits.    |
| [Native Rust SDK](../../lib/crowdb-sdk-native/README.md)                  | Native Rust API usage, memory budgets, transport options, and shutdown.        |
| [Native Python SDK](../../lib/crowdb-sdk-native-python/README.md)         | Trusted native Python packaging, buffers, and application lifetime.            |
| [Local node guide](../../container/single-node-container/README.md)       | Local node storage, listener setup, and client examples.                       |
| [OCI image guide](../../container/oci-image/README.md)                    | OCI build, launch, host deployment, persistent data, and lifecycle.            |
| [Access acceptance guide](../../app/crowdb-access-server/tests/README.md) | Access acceptance evidence and client/engine validation boundaries.            |
| [E2E guide](../../e2e-test/README.md)                                     | Container suites, deployment/restart checks, service ownership, and artifacts. |
| [Ecosystem matrix](../../e2e-test/clients/ecosystem/README.md)            | Pinned external client/engine matrix and manual acceptance.                    |
| [Tree benchmark guide](../../lib/crowdb-tree/bench/README.md)             | Targeted Tree benchmark conditions, reproduction, and interpretation.          |
| [Tools guide](../../tools/README.md)                                      | Tool directories, Pixi task entry points, and validation commands.             |

## Project files

| Document                                       | Scope                                                      |
| ---------------------------------------------- | ---------------------------------------------------------- |
| [README.md](../../README.md)                   | Product introduction and deployment options.               |
| [CONTRIBUTING.md](../../CONTRIBUTING.md)       | Environment, development workflow, and contribution rules. |
| [VERSION](../../VERSION)                       | Canonical development version.                             |
| [SECURITY.md](../../SECURITY.md)               | Vulnerability reporting.                                   |
| [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md) | Community conduct.                                         |
