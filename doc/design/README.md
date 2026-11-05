<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Design

Start with the product design, then use Scope to choose an architecture or component article. These English overviews explain the design; component READMEs provide usage and operational instructions.

## Product and architecture

| File name                                                      | Scope                                                                                          |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| [design-crowdb.md](design-crowdb.md)                           | Workloads, shared storage, independent scaling, coordinated recovery, and design advantages.   |
| [design-crowdb-architecture.md](design-crowdb-architecture.md) | Component roles, logical/physical placement, serving capacity, recovery, and resource control. |

## Components

| File name                                                          | Scope                                                                                                |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| [design-crowdb-access-iceberg.md](design-crowdb-access-iceberg.md) | Native writes, direct-URI reads, table commits, and integrated file lifecycle.                       |
| [design-crowdb-access-dataset.md](design-crowdb-access-dataset.md) | Sample-oriented storage, small payloads, Snapshot/Split selection, and direct training I/O.          |
| [design-crowdb-access-server.md](design-crowdb-access-server.md)   | Host, S3 buckets and objects, streaming, multipart, and authentication.                              |
| [design-crowdb-sdk-native.md](design-crowdb-sdk-native.md)         | Direct RDMA, gateway bypass, caller-held capacity, retained buffers, and GPU integration direction.  |
| [design-crowdb-paxos-kv.md](design-crowdb-paxos-kv.md)             | Concurrent slot writes, per-key apply ordering, stores/groups/replicas, and durable metadata.        |
| [design-crowdb-chunk-kv.md](design-crowdb-chunk-kv.md)             | Ordered metadata, independent payload placement, smooth split/transfer, and serving capacity.        |
| [design-crowdb-tree.md](design-crowdb-tree.md)                     | Concurrent skiplists, Bw-tree mapping, fork/join flush, external-WAL recovery, and dual backends.    |
| [design-crowdb-chunk-stream.md](design-crowdb-chunk-stream.md)     | Reusable durable byte streams, journal composition, rollover/recovery/trim, and future interfaces.   |
| [design-crowdb-chunkdb.md](design-crowdb-chunkdb.md)               | Chunk placement, large/small write pipelines, EC, read flow, and repair.                             |
| [design-crowdb-diskdb.md](design-crowdb-diskdb.md)                 | Disk/zone allocation, durable records, capacity accounting, and recovery.                            |
| [design-crowdb-diskio.md](design-crowdb-diskio.md)                 | Addressed byte I/O, asynchronous backends, completion, and buffer lifetime.                          |
| [design-crowdb-rpc.md](design-crowdb-rpc.md)                       | RDMA data paths, buffer ownership, resource backpressure, asynchronous completion, and TCP fallback. |
| [design-crowdb-monitor.md](design-crowdb-monitor.md)               | System metadata in Group 0, node discovery, service supervision, and console operations.             |
| [design-crowdb-deployment.md](design-crowdb-deployment.md)         | OCI packaging, runtime independence, host networking/devices, and CPU/memory limits.                 |

## Usage and validation

| File name                                                                                    | Scope                                                                          |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [sdk/README.md](../../sdk/README.md)                                                         | Portable Dataset HTTP SDK for Rust/Python, examples, packaging, and limits.    |
| [lib/crowdb-sdk-native/README.md](../../lib/crowdb-sdk-native/README.md)                     | Native Rust API usage, memory budgets, transport options, and shutdown.        |
| [lib/crowdb-sdk-native-python/README.md](../../lib/crowdb-sdk-native-python/README.md)       | Trusted native Python packaging, buffers, and application lifetime.            |
| [container/single-node-container/README.md](../../container/single-node-container/README.md) | Local node storage, listener setup, and client examples.                       |
| [container/oci-image/README.md](../../container/oci-image/README.md)                         | OCI build, launch, host deployment, persistent data, and lifecycle.            |
| [app/crowdb-access-server/tests/README.md](../../app/crowdb-access-server/tests/README.md)   | Access acceptance evidence and client/engine validation boundaries.            |
| [e2e-test/README.md](../../e2e-test/README.md)                                               | Container suites, deployment/restart checks, service ownership, and artifacts. |
| [e2e-test/clients/ecosystem/README.md](../../e2e-test/clients/ecosystem/README.md)           | Pinned external client/engine matrix and manual acceptance.                    |
| [lib/crowdb-tree/bench/README.md](../../lib/crowdb-tree/bench/README.md)                     | Targeted Tree benchmark conditions, reproduction, and interpretation.          |
| [tools/README.md](../../tools/README.md)                                                     | Tool directories, Pixi task entry points, and validation commands.             |

## Project files

| File name                                      | Scope                                                      |
| ---------------------------------------------- | ---------------------------------------------------------- |
| [README.md](../../README.md)                   | Product introduction and deployment options.               |
| [CONTRIBUTING.md](../../CONTRIBUTING.md)       | Environment, development workflow, and contribution rules. |
| [AGENTS.md](../../AGENTS.md)                   | Repository rules for coding and documentation work.        |
| [VERSION](../../VERSION)                       | Canonical development version.                             |
| [CHANGELOG.md](../../CHANGELOG.md)             | Release history.                                           |
| [SECURITY.md](../../SECURITY.md)               | Vulnerability reporting.                                   |
| [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md) | Community conduct.                                         |
