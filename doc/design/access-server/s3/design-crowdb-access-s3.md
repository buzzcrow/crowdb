<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server S3

S3 is CROWDB's HTTP object-storage access model. It provides the core S3
behavior needed by existing tools while retaining CROWDB's bounded streaming,
atomic publication, and distributed storage properties.

Depends on: [Access Server](../design-crowdb-access-server.md),
[Chunk I/O](../../chunkio/design-crowdb-chunkio.md), and
[Chunk-KV](../../chunkds/design-crowdb-chunk-kv.md).

Satisfies: S3-compatible access without making S3 the universal abstraction for
Iceberg, Dataset, or CROWDB internals.

## Table of contents

1. [Intent and boundary](#1-intent-and-boundary)
2. [Authority model](#2-authority-model)
3. [HTTP data path](#3-http-data-path)
4. [Publication and lifecycle](#4-publication-and-lifecycle)
5. [Relationship to other access models](#5-relationship-to-other-access-models)
6. [Correctness invariants](#6-correctness-invariants)

## 1. Intent and boundary

The S3 module terminates an independent HTTP listener and owns S3 request,
authentication, error, namespace, and compatibility behavior. Its supported
surface is deliberately smaller than the complete AWS product surface. An
unsupported operation returns a stable S3-shaped error and does not mutate
state.

The module does not own physical data placement, replication, erasure coding,
repair, or disk lifecycle. The HTTP implementation library is not part of the
S3 contract.

## 2. Authority model

S3 bucket and object records are first-class CROWDB metadata. Bucket names map
to stable identities, while object keys form an ordered namespace within a
bucket identity. Object records select complete immutable data generations and
carry the logical information needed for S3 reads and integrity checks.

S3 is authoritative for bucket and object visibility, overwrite behavior,
listing position, multipart publication, deletion, and S3 credentials. It
stores opaque data references rather than physical storage topology.

## 3. HTTP data path

PUT and multipart upload stream HTTP bodies into bounded CROWDB writers. GET
streams owner-backed CROWDB data into an HTTP response. Backpressure bounds
memory and storage work independently of object size; slow peers cannot create
unbounded buffering.

The ordinary HTTP path is always available. An optional direct data plane may
move an authenticated object range between DiskIO and registered client memory
without relaying payload through Access Server memory. HTTP remains the control
and compatibility surface, and acceleration cannot change S3 semantics.

## 4. Publication and lifecycle

An object becomes visible only after its bytes and integrity state are complete.
Publication selects the complete object generation atomically. A failed or
losing upload remains invisible and is reclaimed asynchronously.

Reads retain one published generation for the operation. Overwrite selects a
new complete generation; it never mutates bytes observed by an existing read.
Delete removes logical visibility before physical reclamation. Bucket deletion
cannot expose objects from an earlier bucket identity if the name is later
reused.

Listings are ordered and continuation-safe within their documented consistency
model. Continuation state is opaque and bound to the original request scope.

## 5. Relationship to other access models

Iceberg is not implemented as special objects in the S3 namespace. Its catalog,
table, file, commit, and reclamation records belong to the Iceberg authority,
even when an Iceberg client uses S3-shaped file locations.

Dataset is not an S3 extension. A Dataset may explicitly refer to immutable
bytes associated with S3, but Dataset selection, batching, streaming, and
native topology access remain Dataset semantics.

## 6. Correctness invariants

- **S3-I1 — Atomic visibility:** a reader observes absence or one complete
  published object generation, never partial upload bytes.
- **S3-I2 — Stable read:** one HEAD or GET retains one object generation for
  the entire operation.
- **S3-I3 — Bounded streaming:** object size does not determine Access Server
  memory consumption.
- **S3-I4 — Namespace isolation:** credentials, bucket identity, object key,
  and continuation state cannot escape their authorized scope.
- **S3-I5 — Reclamation after invisibility:** physical deletion follows logical
  removal and cannot restore visibility.
- **S3-I6 — Independent authority:** S3 records cannot publish, mutate, or
  reclaim Iceberg or Dataset authority.
- **S3-I7 — Transport equivalence:** ordinary HTTP and accelerated transfer
  produce the same S3 range, integrity, publication, and error outcome.
