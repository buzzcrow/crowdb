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

Bucket and object metadata is protocol-owned and stored in Chunk-KV. Object
keys are tenant-qualified, binary-safe, and self-sorting: `tenant / bucket ID /
object key`. A bucket prefix is therefore a bounded ordered scan interval.

One object-key value records bucket identity, binary key, logical length,
checksum and ETag information, timestamps, supported attributes, and opaque
chunk data references. Physical storage layout remains below the chunk-client
boundary.

Bucket-name mappings live under one dedicated tenant-qualified Chunk-KV prefix
so listing buckets is a bounded ordered scan independent of object keys. The
mapping is distinct from an immutable random UUID bucket ID. Operations
resolve a name through its active mapping before accessing that ID's object
interval. Empty-only deletion scans the current interval and atomically
tombstones the name mapping. A PUT that finishes concurrently can only publish
under the old ID; that ID is unreachable after the tombstone and cannot become
visible through a later recreation, which receives a new ID. Old-ID metadata
and chunks are unreachable garbage reclaimed separately. This lets bucket
deletion take a slow path without a bucket-wide lock, permit, or final-publication
step in the PUT hot path.

## 3. Publication and recovery

PUT streams bytes into bounded chunk writers, finishes parity and sealing, then
publishes the complete metadata and chunk location with one unconditional
Chunk-KV `Put` at the object key. A failed upload never publishes incomplete
bytes. Retried and concurrent PUTs are ordinary overwrite writes. Unreferenced
chunks are safe garbage reclaimed asynchronously; cleanup never delays PUT.

## 4. HTTP buffer ownership

The S3 module uses the maintained Hyper fork directly on Tokio. Its opt-in
HTTP/1 body path asks a CROWDB-provided allocator for a buffer sized from the
admitted object length and configured frame bounds, then reads decoded payload
directly into that allocation. The allocator freezes it into an immutable
owner-backed frame before delivery. The initial allocator owns native memory;
registered and RDMA-pinned providers implement the same ownership contract.
Headers and framing remain in Hyper's normal buffer; a body prefix read with
headers is copied at most once and measured.

An owned buffer view retains its allocation and can be split without copying.
A bounded chain crosses HTTP, block, chunk, EC, and RPC boundaries without
making frame boundaries semantic. Vectored writes are used within platform and
send-queue limits; an operation that cannot fit is coalesced once into a pooled
buffer and the copied bytes are measured.

GET yields native owner-backed views to the response body. The owner is released
only after Hyper has consumed the bytes accepted by the socket. Slow clients
bound storage prefetch through response credits.

## 5. Read, list, and delete

HEAD and GET read one object metadata value. HEAD is metadata-only. GET maps
the complete object or one contiguous range to chunk-reader intervals and
retains that value for the response.

Object listing is ordered and continuation-safe but not a global snapshot
across Chunk-KV partitions. An opaque token binds bucket, parameters, and last
emitted position. A key that remains unchanged for a complete traversal is not
duplicated or skipped; concurrent creates and deletes have page-relative
visibility.

DELETE performs one unconditional KV delete at the object key and returns from
that logical result. It does not read or CAS the object first. Physical bytes
become unreachable garbage: dedicated chunks may be reclaimed asynchronously,
while shared small-object ranges wait for qualified range reclamation. Cleanup
is never part of DELETE latency and never restores object visibility.

## 6. Authentication

The authentication hook observes the raw method, URI, query, headers, and
payload mode before routing or body allocation. SigV4 performs standard
canonicalization, timestamp and scope checks, constant-time comparison, and
supported payload validation.

Group 0 stores versioned access-key records bound to user identities. The
continuation-token signing authority is derived from the same configured
cluster master key. Secret material is encrypted under that key before it
reaches group-0 WAL or snapshots. Token issuance returns a newly generated
access key and plaintext secret once; another issuance for the same user is an
independent token. Access Servers load and decrypt a linearizable credential
snapshot before authenticated readiness, periodically rescan, atomically
replace the immutable cache, zeroize retired plaintext, and fail closed after
maximum staleness. Stable-user deduplication, token rotation, disable/revoke
workflows, notification-driven refresh, and hardened master-key provisioning
are later security work.

## 7. Correctness invariants

- **S3-I1 — Atomic visibility:** readers observe absence or one complete sealed
  object metadata value, never partial bytes.
- **S3-I2 — Stable read metadata:** one HEAD or GET retains one metadata value
  and its data reference for the whole response.
- **S3-I3 — Simple mutation:** PUT is one unconditional object-key overwrite
  after chunk completion; retry and conflict control add no metadata round trip.
- **S3-I4 — Bounded streaming:** object length does not determine Access Server
  memory consumption.
- **S3-I5 — Namespace isolation:** tenant, bucket, key, and continuation state
  cannot escape their encoded interval.
- **S3-I6 — Reclamation after invisibility:** physical deletion is independent
  asynchronous work after the object-key delete.
- **S3-I7 — Transport independence:** optional acceleration cannot change S3
  range, integrity, publication, or error semantics.
- **S3-I8 — Bucket name generation:** one active bucket-name mapping resolves
  to one immutable bucket ID; a tombstoned mapping cannot expose old-ID object
  metadata or be reused by a later bucket creation.
