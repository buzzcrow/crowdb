<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server

The Access Server is CROWDB's stateless external data-access tier. It hosts
independent S3, Catalog/Table, Dataset, and optional accelerated-transfer
protocol modules above the shared chunk and Chunk-KV services.

## Table of contents

1. [Scope](#1-scope)
2. [Sub-design map](#2-sub-design-map)
3. [Architecture](#3-architecture)
4. [Protocol isolation](#4-protocol-isolation)
5. [Shared storage boundary](#5-shared-storage-boundary)
6. [Runtime and scale-out](#6-runtime-and-scale-out)
7. [Security boundary](#7-security-boundary)
8. [Resource and lifecycle contract](#8-resource-and-lifecycle-contract)
9. [Configuration](#9-configuration)
10. [Correctness invariants](#10-correctness-invariants)
11. [Risks and open questions](#11-risks-and-open-questions)

## 1. Scope

The Access Server terminates external protocols and translates each protocol's
semantics directly to CROWDB chunk data and Chunk-KV metadata. It does not own
disk placement, chunk durability, EC layout, consensus, or an authoritative
gateway-local namespace.

Internal FlatBuffer and RPC contracts remain in the Protocol and RPC areas.
External access protocols belong here because their compatibility, lifecycle,
and performance contracts differ from internal wire protocols.

Related designs:

- [Chunk I/O](../chunkio/design-crowdb-chunkio.md)
- [Chunk object reader](../chunkio/design-crowdb-chunkio-reader.md)
- [Chunk-backed range KV](../chunkds/design-crowdb-chunk-kv.md)
- [Chunk-KV routed client](../chunkds/design-crowdb-chunk-kv-client.md)
- [RPC](../rpc/design-crowdb-rpc.md)
- [RPC RDMA transport](../rpc/design-crowdb-rpc-rdma.md)

## 2. Sub-design map

- [S3](design-crowdb-access-server-s3.md): limited S3 compatibility, object
  metadata, publication, streaming, listing, deletion, and SigV4.
- [Catalog/Table](design-crowdb-access-server-catalog.md): Iceberg REST Catalog,
  atomic table metadata commits, and protocol-specific Parquet services.
- [Dataset](design-crowdb-access-server-dataset.md): AI-native dataset,
  sample, shard, tensor, batch, and stream access.
- [Accelerated transfer](design-crowdb-access-server-accelerated-transfer.md):
  optional cuObject and native RDMA data planes.

## 3. Architecture

```text
                         crowdb-access-server
        +--------------------+--------------------+--------------------+
        | S3 module          | Catalog/Table      | Dataset module     |
        | Hyper / HTTP       | independent HTTP   | HTTP or native RPC |
        +----------+---------+----------+---------+----------+---------+
                   |                    |                    |
              protocol-owned metadata, publication, and data flow
                   |                    |                    |
                   +--------------------+--------------------+
                                        |
                       chunk client + Chunk-KV client
                                        |
                         chunk, DiskIO, and KV services
```

The modules do not implement a protocol-neutral object-store trait. S3 object
publication, Iceberg table commits, Parquet decode/filter/repack, and Dataset
streaming retain their native semantics. Small utilities may share safe buffer
owners, authentication primitives, admission accounting, error helpers, and
shutdown coordination, but shared utilities never own storage semantics.

## 4. Protocol isolation

Each protocol is an independent library with its own request types, metadata
schema, authorization policy, listener, metrics, configuration, and background
tasks. Each enabled protocol binds a distinct listen address and enters its own
handler directly; there is no shared host/path dispatcher.

Build features exclude unused protocol libraries. A module absent at build time
has no linked dependencies or branches. A compiled but disabled module binds no
listener, starts no task, and allocates no protocol-specific pool. Some
duplicated orchestration is intentional when it keeps protocol behavior and hot
paths independent.

## 5. Shared storage boundary

The chunk layer is the common data abstraction. Protocol modules use the chunk
client for immutable or append-owned bytes and the Chunk-KV client for metadata
and ordered indexes. Chunk references remain opaque above that boundary;
physical segments, disks, nodes, racks, replicas, and EC shards are not copied
into external protocol metadata.

This boundary allows each protocol to optimize independently while retaining
one durability, placement, repair, and buffer-ownership implementation below
it.

## 6. Runtime and scale-out

One Access Server process owns a Tokio runtime, enabled protocol libraries,
shared low-level clients, service admission counters, and shutdown
cancellation. Protocol listeners and request flows remain independent within
that process.

Access Server instances are stateless with respect to authoritative namespace
and operation state. Durable upload, commit, deletion, retry, and continuation
identities live in Chunk-KV or chunk storage. Any instance can serve a retry,
and ordinary TCP load balancing requires no sticky session for supported
operations. Local caches are generation-qualified hints and can be discarded.

## 7. Security boundary

Each protocol authenticates before metadata or data access and produces a
protocol-owned authorization result. Group 0 is the authority for users and
credential generations. Access Servers install immutable credential-cache
snapshots atomically and fail closed after the configured maximum staleness.

External credentials and memory descriptors are never storage authority. An
internal delegated operation is bound to tenant, operation, immutable
generation, direction, range, byte limit, expiry, and nonce. Secrets, reusable
memory keys, and opaque transfer descriptors are excluded from logs, metrics,
and traces.

## 8. Resource and lifecycle contract

Admission bounds connections, operations, metadata work, native bytes, buffer
views, reader/writer windows, and background tasks. Pressure propagates toward
the producer; runtime workers do not block on pool exhaustion and no unbounded
fallback allocation is permitted.

Shutdown proceeds in this order:

1. Stop new admission independently on each listener.
2. Cancel unadmitted work.
3. Drive admitted metadata mutations to a known durable outcome.
4. Drain or fail native work while retaining every live buffer owner.
5. Close low-level clients, protocol pools, and the runtime.

## 9. Configuration

Configuration independently controls each protocol's build presence, enabled
state, listen address, authentication, concurrency, memory budget, deadlines,
and protocol-specific limits. Service-wide budgets cannot be raised by a
request. Invalid limits fail startup rather than being silently clamped.

Optional accelerated transfer has a separate configuration namespace and is
absent from a basic build. Dynamic cluster configuration may replace validated
local values without changing protocol ownership.

## 10. Correctness invariants

- **AS-I1 — Protocol isolation:** disabling one protocol creates no listener,
  task, pool, or request-path branch in another protocol.
- **AS-I2 — Chunk boundary:** external protocols never become authorities for
  physical chunk placement or disk topology.
- **AS-I3 — Stateless frontend:** every authoritative operation outcome is
  recoverable without the Access Server instance that began it.
- **AS-I4 — Bounded ownership:** retained bytes, views, queues, and work are
  bounded independently of object or dataset size.
- **AS-I5 — One semantic owner:** each protocol owns its metadata and
  compatibility semantics; shared utilities cannot reinterpret them.
- **AS-I6 — Authentication before access:** no metadata or payload operation
  starts without the protocol's explicit authorization result, except an
  explicitly configured development-only trusted-network mode.
- **AS-I7 — Completion lifetime:** a native owner outlives every socket, RPC,
  storage, or NIC operation that references it.

## 11. Risks and open questions

The principal architecture risks are maintenance of the Hyper receive-buffer
extension, protocol compatibility drift, cross-service backpressure, and
native resource lifetime during cancellation and shutdown. Each sub-design
defines its own failure and validation contract.

Group 0 is the credential authority, but protection of stored SigV4 secrets is
unsettled. Direct storage relies on group-0/WAL/snapshot access controls;
encryption under a cluster master key limits disclosure but adds key
provisioning, rotation, backup, recovery, and availability dependencies.
