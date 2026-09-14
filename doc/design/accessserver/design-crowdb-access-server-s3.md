<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server S3

The S3 module provides a deliberately limited, stateless S3-compatible object
service over CROWDB chunks and Chunk-KV metadata.

Depends on: [Access Server](design-crowdb-access-server.md),
[Chunk I/O](../chunkio/design-crowdb-chunkio.md), and
[Chunk-KV](../chunkds/design-crowdb-chunk-kv.md). Hyper fork maintenance is
defined in the [development guide](../../dev/hyper_fork.md).

Satisfies: basic bucket and object operations with bounded TCP streaming and
atomic object visibility.

## Table of contents

1. [Compatibility surface](#1-compatibility-surface)
2. [Metadata and namespace](#2-metadata-and-namespace)
3. [Publication and recovery](#3-publication-and-recovery)
4. [HTTP buffer ownership](#4-http-buffer-ownership)
5. [Read, list, and delete](#5-read-list-and-delete)
6. [Authentication](#6-authentication)
7. [Correctness invariants](#7-correctness-invariants)

## 1. Compatibility surface

The supported bucket operations are create, head, list, and empty-only delete.
The supported object operations are put, head, get, one contiguous byte range,
ordered list, and delete. Multi-range GET is not supported because the
[S3 GetObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
defines one range per request.

Multipart upload, versioning, lifecycle, replication controls, server-side
encryption, storage classes, object lock, tagging, website hosting,
notifications, and select are outside the basic surface. Unsupported behavior
returns a stable S3-shaped error and performs no mutation.

## 2. Metadata and namespace

Bucket and object metadata is protocol-owned and stored in Chunk-KV. Keys are
versioned, tenant-qualified, binary-safe, and self-sorting so a bucket prefix is
a bounded ordered scan interval.

An immutable object generation records bucket identity, binary key, logical
length, checksum and ETag information, timestamps, supported attributes,
publication generation, upload identity, and opaque chunk data references. A
separate visibility value selects exactly one generation or absence. Physical
storage layout remains below the chunk-client boundary.

Bucket deletion uses a fenced emptiness check so it cannot race final object
publication and leave a visible object without a live bucket.

## 3. Publication and recovery

PUT persists a stable upload intent before allocating data, streams bytes into
bounded chunk writers, finishes parity and sealing, and atomically publishes one
immutable generation by replacing the visibility value. The metadata compare is
the visibility point.

A failed upload never publishes incomplete bytes. Reconciliation scans durable
incomplete intents, proves ambiguous publication by upload identity, completes
an already-published operation, or schedules unpublished owned data for
cleanup. Overwrite publishes the new generation without waiting for old-data
reclamation.

## 4. HTTP buffer ownership

The S3 module uses the maintained Hyper fork directly on Tokio. Its opt-in
HTTP/1 body path reads decoded payload into bounded native glibc-backed buffers
selected after header admission and before the body is polled. Headers and
framing remain in Hyper's normal buffer; a body prefix read with headers is
copied at most once and measured.

An owned buffer view retains its allocation and can be split without copying.
A bounded chain crosses HTTP, block, chunk, EC, and RPC boundaries without
making frame boundaries semantic. Vectored writes are used within platform and
send-queue limits; an operation that cannot fit is coalesced once into a pooled
buffer and the copied bytes are measured.

GET yields native owner-backed views to the response body. The owner is released
only after Hyper has consumed the bytes accepted by the socket. Slow clients
bound storage prefetch through response credits.

## 5. Read, list, and delete

HEAD and GET resolve one immutable generation. HEAD is metadata-only. GET maps
the complete object or one contiguous range to chunk-reader intervals and does
not switch generations during the response.

Object listing is ordered and continuation-safe but not a global snapshot
across Chunk-KV partitions. An opaque token binds bucket, parameters, and last
emitted position. A key that remains unchanged for a complete traversal is not
duplicated or skipped; concurrent creates and deletes have page-relative
visibility.

DELETE first publishes absence. Dedicated large-object chunks are reclaimed
idempotently after the reader-validity grace period. Shared small objects use
the shared writer; deletion records an exact qualified range cleanup. Missing
physical range reclamation leaves safe logical garbage and never restores
visibility or deletes neighboring bytes.

## 6. Authentication

The authentication hook observes the raw method, URI, query, headers, and
payload mode before routing or body allocation. SigV4 performs standard
canonicalization, timestamp and scope checks, constant-time comparison, and
supported payload validation.

Group 0 stores stable users and versioned access-key/secret-key records. User
creation returns a newly generated access key and secret once. Rotation,
disable, and revocation publish new generations. Access Servers load a
linearizable credential snapshot before authenticated readiness, consume
watch notifications, periodically rescan, atomically replace the immutable
cache, and fail closed after maximum staleness.

## 7. Correctness invariants

- **S3-I1 — Atomic visibility:** readers observe absence or one complete sealed
  generation, never partial bytes.
- **S3-I2 — Stable read generation:** one HEAD or GET uses attributes and data
  from one immutable generation.
- **S3-I3 — Idempotent mutation:** stable identities reconcile ambiguous PUT
  and DELETE outcomes without duplicate publication or cleanup.
- **S3-I4 — Bounded streaming:** object length does not determine Access Server
  memory consumption.
- **S3-I5 — Namespace isolation:** tenant, bucket, key, and continuation state
  cannot escape their encoded interval.
- **S3-I6 — Reclamation after invisibility:** physical deletion follows
  metadata removal and reader validity.
- **S3-I7 — Transport independence:** optional acceleration cannot change S3
  range, integrity, publication, or error semantics.
