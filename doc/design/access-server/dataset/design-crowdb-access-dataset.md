<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Dataset Service

Dataset is CROWDB's AI-oriented access model for immutable generations,
samples, shards, tensors, batches, and streams. It offers both portable HTTP
access and a topology-aware native client with the same dataset semantics.

Depends on: [Access Server](../design-crowdb-access-server.md),
[Chunk I/O](../../chunkio/design-crowdb-chunkio.md),
[Chunk-KV](../../chunkds/design-crowdb-chunk-kv.md), and
[Chunk-KV client](../../chunkds/design-crowdb-chunk-kv-client.md).

Satisfies: dataset-oriented access whose routing, batching, and transfer model
is not constrained by S3 or Iceberg compatibility.

## Table of contents

1. [Intent and boundary](#1-intent-and-boundary)
2. [Dataset model](#2-dataset-model)
3. [Dual access surfaces](#3-dual-access-surfaces)
4. [Native and GPU-direct path](#4-native-and-gpu-direct-path)
5. [Publication and lifecycle](#5-publication-and-lifecycle)
6. [Relationship to other access models](#6-relationship-to-other-access-models)
7. [Correctness invariants](#7-correctness-invariants)

## 1. Intent and boundary

Dataset gives AI, analytics, and data-preparation applications concepts closer
to their work than buckets or tables alone. It owns generation publication,
sample and shard selection, batching, ordering, shuffle, streaming, prefetch,
and tensor-oriented delivery.

Dataset does not own physical placement, replication, erasure coding, repair,
or disks. It is not an S3 extension, an Iceberg table convention, or a query
engine.

## 2. Dataset model

A dataset has stable identity and immutable generations. A generation selects a
manifest that maps logical concepts such as samples, shards, tensors, column
groups, or batches to validated byte ranges and encodings. CROWDB chunks own the
bytes; Dataset owns their logical organization and selection.

One read selects one generation. New data and metadata are built out of view and
become visible through atomic generation publication.

## 3. Dual access surfaces

Dataset HTTP is an independent Access Server protocol. It provides portable
discovery, ingest, selection, and bounded streaming to clients that should not
embed CROWDB routing.

The native Dataset client is a linked client library, not an HTTP wrapper. It
loads the same dataset authority, consumes a bounded versioned CROWDB routing
view, plans work locally, and connects directly to responsible metadata and
data services. This removes one routing and payload relay layer from the
performance path.

The two surfaces may use different request shapes and transport mechanics. They
must select the same generation, enforce the same authorization, and return the
same logical data and integrity result.

## 4. Native and GPU-direct path

The native client owns routing, retry, prefetch, streaming, and application
buffer lifetime. A stale topology produces a bounded redirect, refresh, or
retry. Observing topology lets the client route work but never makes it a
placement authority.

CPU delivery uses owner-backed CROWDB buffers. The accelerated direction is
direct delivery from DiskIO-owned buffers across RDMA into registered client GPU
memory. GPUDirect Storage, GPUDirect RDMA, cuObject-compatible transfer, or
native CROWDB RDMA may provide the mechanism. The goal is to avoid both an
Access Server payload bounce and Dataset-client CPU staging.

Direct operations use short-lived capabilities bound to the principal, dataset
generation, operation, source and destination ranges, limits, expiry, and
topology epoch. Completion covers every delegated span and integrity check.
CPU fallback is chosen before direct submission and never continues a partial
direct transfer.

## 5. Publication and lifecycle

Ingest writes bounded candidates and publishes a complete immutable generation
only after its manifest, indexes, payloads, integrity state, and durability
conditions complete. Failed or losing candidates remain invisible.

Readers retain their selected generation for the whole operation. Retention and
reclamation remove physical data only after no live dataset generation, lease,
or explicit cross-model reference can reach it.

## 6. Relationship to other access models

Dataset may explicitly reference immutable data associated with S3 or Iceberg.
Such a reference is generation-bound and preserves the source model's ownership
and lifetime rules. Dataset cannot overwrite an S3 object, commit an Iceberg
table, or reclaim source-owned bytes.

Likewise, S3 compatibility and Iceberg table rules cannot restrict native
Dataset batching, shuffle, scatter/gather, or GPU delivery.

## 7. Correctness invariants

- **DATA-I1 — Generation consistency:** one operation uses one immutable
  dataset generation.
- **DATA-I2 — Exact selection:** returned samples, shards, tensor ranges, and
  destination offsets correspond exactly to the selected manifest.
- **DATA-I3 — Atomic publication:** readers observe absence or one complete
  dataset generation, never a partially built manifest.
- **DATA-I4 — Native directness:** native access reaches responsible CROWDB
  services without an Access Server request or payload hop.
- **DATA-I5 — Surface equivalence:** HTTP and native access return the same
  logical selection and integrity result.
- **DATA-I6 — Bounded streaming:** dataset size does not determine server or
  client memory, registration, or prefetch depth.
- **DATA-I7 — Non-authoritative topology:** a client routing view cannot publish
  or alter physical placement.
- **DATA-I8 — GPU final destination:** a direct GPU path does not relay payload
  through Access Server or Dataset-client CPU memory.
- **DATA-I9 — Independent authority:** S3 and Iceberg semantics cannot redefine
  Dataset publication, selection, batching, or transfer behavior.
