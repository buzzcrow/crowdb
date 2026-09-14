<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server Catalog and Table Service

The Catalog/Table module exposes table-oriented metadata and processing without
forcing Iceberg, Parquet, or table commits through S3 object semantics.

Depends on: [Access Server](design-crowdb-access-server.md),
[Chunk I/O](../chunkio/design-crowdb-chunkio.md), and
[Chunk-KV](../chunkds/design-crowdb-chunk-kv.md).

Satisfies: an isolated home for Iceberg REST Catalog and protocol-specific
table and Parquet operations.

## Table of contents

1. [Boundary](#1-boundary)
2. [Metadata and data](#2-metadata-and-data)
3. [Processing](#3-processing)
4. [Isolation and scale-out](#4-isolation-and-scale-out)
5. [Correctness invariants](#5-correctness-invariants)

## 1. Boundary

Catalog/Table is a peer of S3, not an S3 extension and not an implementation of
a common object-store interface. It owns Iceberg namespaces, tables, views,
commits, compatibility, authorization, and errors. Parquet is a data format;
server-side Parquet operations belong to this module when they are part of a
table request.

## 2. Metadata and data

Chunk-KV stores catalog authority, versioned namespace/table/view records, and
atomic commit state. Immutable manifests, metadata files, and Parquet data are
stored through the chunk client. Records may intentionally refer to an S3
object generation for interoperability, but S3 headers, bucket policy, and
object lifecycle do not become table semantics.

Concurrent commits compare the expected table generation and publish one new
metadata generation atomically. A failed commit leaves no partially selected
table state.

## 3. Processing

Table requests may push projection, predicate evaluation, Parquet decode, and
result repacking toward data-serving nodes. Plans carry explicit table and file
generations, selected row groups, columns, filters, result bounds, and
authorization. Returned streams are protocol-specific and need not resemble S3
object reads.

## 4. Isolation and scale-out

The module owns a separate listener, request types, admission budgets, metrics,
and background work. It shares only low-level clients and safe utilities with
other Access Server protocols. Authoritative state is durable, so any Access
Server instance can serve a catalog read or reconcile a commit.

## 5. Correctness invariants

- **CAT-I1 — Atomic table generation:** one successful commit selects one
  complete metadata generation.
- **CAT-I2 — Protocol ownership:** S3 object behavior cannot redefine catalog
  or table semantics.
- **CAT-I3 — Immutable input:** processing runs against explicitly selected
  manifest, file, and table generations.
- **CAT-I4 — Bounded results:** pushed processing and response streams obey
  independent memory, CPU, and output limits.
