<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server

CROWDB presents three first-class upper access models: S3, Iceberg, and
Dataset. Each model owns its semantics while sharing the same durable CROWDB
storage substrate. HTTP is the portable server interface. Dataset additionally
provides a topology-aware native client for the shortest path from an
application to CROWDB data and, where supported, GPU memory.

## Table of contents

1. [Intent](#1-intent)
2. [Access portfolio](#2-access-portfolio)
3. [Architecture](#3-architecture)
4. [Semantic ownership](#4-semantic-ownership)
5. [Data paths](#5-data-paths)
6. [Shared storage boundary](#6-shared-storage-boundary)
7. [Scale-out and security](#7-scale-out-and-security)
8. [Correctness invariants](#8-correctness-invariants)
9. [Direction and risks](#9-direction-and-risks)

## 1. Intent

The access architecture gives different users the model that fits their work
without forcing all data through an object-store abstraction:

- existing applications use an S3-compatible object interface;
- table engines use an Iceberg catalog and table interface; and
- AI and data-intensive applications use Dataset concepts and can choose a
  portable HTTP path or a direct native path.

The Access Server terminates the HTTP surfaces. It is a stateless translator,
not a storage authority or a required relay for every client. The HTTP engine
is an implementation choice and is not part of the architecture contract.

## 2. Access portfolio

| Model   | Semantic authority                         | Portable surface | Performance surface          |
| ------- | ------------------------------------------ | ---------------- | ---------------------------- |
| S3      | Buckets, objects, multipart, S3 lifecycle | HTTP             | Optional direct data plane   |
| Iceberg | Catalogs, namespaces, tables, snapshots   | HTTP             | Delegated immutable FileIO   |
| Dataset | Generations, samples, shards, tensors     | HTTP             | Topology-aware native client |

Related designs:

- [S3](s3/design-crowdb-access-s3.md)
- [Iceberg](iceberge/design-crowdb-iceberg.md)
- [Dataset](dataset/design-crowdb-access-dataset.md)
- [Accelerated transfer](s3/design-crowdb-access-s3-rdma.md)
- [Chunk I/O](../chunkio/design-crowdb-chunkio.md)
- [Chunk-KV](../chunkds/design-crowdb-chunk-kv.md)
- [RPC](../rpc/design-crowdb-rpc.md)

## 3. Architecture

```text
                 portable, stateless server access

 S3 client             Iceberg client          Dataset HTTP client
     | HTTP                 | HTTP                    | HTTP
     v                      v                         v
+------------+       +---------------+        +---------------+
| S3 module  |       | Iceberg module|        | Dataset module|
+------+-----+       +-------+-------+        +-------+-------+
       |                     |                        |
       +---------------------+------------------------+
                             |
                    CROWDB storage clients
                             |
                             v
                   Chunk-KV + chunk services
                             ^
                             |
             routed RPC / optional RDMA data plane
                             |
                    Dataset native client
                             ^
                             |
               AI runtime / CPU or GPU memory

                  topology-aware direct access
```

S3 and Iceberg are HTTP protocols. Dataset offers HTTP for interoperability
and a linked native client for maximum performance. The native client is not an
HTTP wrapper: it consumes a validated CROWDB routing view and contacts the
responsible metadata and data services directly, removing the Access Server
routing and payload hop.

## 4. Semantic ownership

S3, Iceberg, and Dataset are peers. None is implemented as a metadata convention
on another model:

- S3 owns object names, overwrite behavior, listing, and S3 compatibility.
- Iceberg owns catalog, table, snapshot, commit, immutable file, and
  reachability semantics. Its S3-shaped file locations do not make S3 the
  authority.
- Dataset owns generation publication, sample and shard selection, batching,
  streaming, prefetch, and tensor-oriented delivery.

The models may share physical bytes only through an explicit reference contract.
Sharing never transfers namespace, publication, authorization, or reclamation
authority.

Each HTTP model has an independent listener, request types, configuration,
authentication policy, admission budget, metrics, and lifecycle. Shared
utilities may manage buffers, credentials, errors, and shutdown, but cannot
reinterpret model semantics.

## 5. Data paths

The ordinary path streams bounded data through the Access Server over HTTP. It
is the universal path and remains available without specialized hardware.

The Dataset native path embeds routing, retry, bounded planning, streaming, and
buffer ownership in the application. It resolves one immutable dataset
generation and distributes work directly to responsible CROWDB services. A
topology view routes work but never grants placement authority.

The accelerated direction is an end-to-end path from storage to the final
consumer buffer. For AI workloads, the intended destination is GPU memory:

```text
storage media -> DiskIO-owned buffer -> RDMA fabric -> client GPU memory
```

GPUDirect Storage, GPUDirect RDMA, cuObject-compatible transfer, and native
CROWDB RDMA are possible mechanisms. The stable architectural goal is no
Access Server payload bounce and no Dataset-client CPU staging for a direct GPU
transfer. CPU streaming is the mandatory fallback, and transport choice never
changes the logical result.

## 6. Shared storage boundary

Chunk-KV is the common metadata and ordered-index substrate. Chunk services are
the common byte, durability, placement, repair, and buffer-ownership substrate.
Each access model stores only its own logical authority and opaque references to
data below this boundary.

Physical segments, disks, replicas, and erasure-coding shards remain storage
concerns. The native Dataset client may observe the minimum versioned topology
needed for routing, but cannot publish or alter physical placement.

## 7. Scale-out and security

Access Server instances keep no authoritative namespace or operation outcome.
Durable state lives in CROWDB, so any healthy instance can serve a later request
or retry. Protocol-local caches are discardable hints.

Direct Dataset access also preserves the storage authority boundary. A client
authenticates before receiving topology, metadata, or payload authority. Direct
operations use short-lived capabilities bound to the principal, logical object,
immutable generation, operation, ranges, limits, expiry, and topology epoch.
Topology and memory descriptors alone grant no access.

All paths bound connections, metadata work, stream windows, retained buffers,
registered memory, retries, and background work independently of total object,
table, or dataset size.

## 8. Correctness invariants

- **AS-I1 — First-class models:** S3, Iceberg, and Dataset each own their
  namespace, publication, authorization, compatibility, and reclamation rules.
- **AS-I2 — HTTP boundary:** S3 and Iceberg are HTTP server protocols; Dataset
  HTTP is the portable Dataset server surface.
- **AS-I3 — Native directness:** Dataset native access reaches responsible
  CROWDB services without an Access Server request or payload hop.
- **AS-I4 — Semantic equivalence:** Dataset HTTP, native CPU, and accelerated
  GPU paths select the same immutable generation and logical data.
- **AS-I5 — Storage authority:** no Access Server module or native client owns
  physical chunk placement, replicas, erasure-coding shards, or disks.
- **AS-I6 — Stateless server:** an authoritative operation survives loss of the
  Access Server instance that began it.
- **AS-I7 — Bounded ownership:** retained state is bounded independently of
  object, table, or dataset size.
- **AS-I8 — Authorized direct access:** topology, endpoints, and memory
  descriptors are not capabilities by themselves.
- **AS-I9 — Completion lifetime:** every buffer and registration outlives all
  socket, RPC, storage, NIC, and GPU operations that reference it.

## 9. Direction and risks

The main architectural direction is to keep HTTP broadly compatible while
making Dataset native access the performance path for AI workloads. RDMA and
GPU-direct delivery should reduce movement, not introduce a second semantic or
storage authority.

The main risks are compatibility drift, stale client topology, capability
containment, cross-service backpressure, native resource lifetime, and uneven
GPU-direct hardware support. Every accelerated path must have a bounded CPU
fallback and must prove the same selection, integrity, and completion semantics.
