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

The current listener uses one configured tenant for all accepted credentials.
Signature verification authenticates a key but does not pass a user/grant
identity to operations. Accepted keys therefore share that listener namespace;
per-user bucket ACLs and IAM policies are not part of the installed surface.
Namespace isolation currently means the configured tenant/bucket identity
boundaries, not isolation between multiple accepted users on one listener.
Presigned request expiry is enforced independently of the allowed future-clock
skew; clock tolerance cannot extend an issued URL's lifetime.

## 3. HTTP data path

PUT and multipart upload stream HTTP bodies into bounded CROWDB writers. GET
streams owner-backed CROWDB data into an HTTP response. Backpressure bounds
memory and storage work independently of object size; slow peers cannot create
unbounded buffering.

Uploads use the shared bounded `UploadBody` decoder. It verifies supported
CRC32, CRC32C, CRC64NVME, SHA1 and SHA256 headers or declared AWS trailers,
alongside Content-MD5 and signed payload hashes. Streaming SigV4 first verifies
the request seed, then every signed chunk and signed trailer. Unsigned chunks
require a verified declared trailer. Encoded and decoded lengths, frame size,
terminal chunks and trailing bytes are checked before publication; malformed
or corrupt bodies cannot replace a selected object. Checksum calculation on
requests is supported; arbitrary response checksum negotiation is outside the
installed surface.

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
Bucket paths accept a single trailing slash. ListObjectsV2 supports URL encoding
of XML-incompatible keys and selected prefix/delimiter fields. ListBuckets
reports the Unix epoch as a stable CreationDate placeholder because bucket
records do not retain creation timestamps. User metadata headers are rejected
before dispatch rather than accepted and discarded.

Server-side CopyObject and UploadPartCopy resolve source and destination through
the configured S3 tenant. Copy selects one immutable source record before
returning response headers; subsequent source overwrite or logical deletion
does not select new bytes. The source is streamed through bounded readers and
writers, respecting writer capacity, and publication uses the ordinary object
or multipart-part fences. No source reference is shared with the destination.
Copy never deletes the source. Unpublished candidates use ordinary reclamation.

CopyObject supports at most 5 GiB and copies the entire payload. COPY preserves
the supported Content-Type metadata; REPLACE selects the supplied Content-Type
or application/octet-stream. A self-copy requires REPLACE. User metadata,
cache/disposition/encoding/language/expiry metadata, version selectors, tags,
encryption, storage-class changes and destination conditions are unsupported
and rejected. Source ETag/date conditions apply to the captured generation;
their failure returns PreconditionFailed. Copy selectors must be signed.

UploadPartCopy supports complete objects and explicit inclusive byte ranges
from source objects larger than 5 MiB, with the session's normal part bounds.
It replaces a part using the durable generation-selection protocol below.
Neither interrupted copy path publishes partial bytes. After request validation
and source selection, the HTTP 200 response carries whitespace keepalives and
ends with CopyObjectResult, CopyPartResult, or an embedded Error. Clients must
consume and validate the whole response. The body owns the copy future, cancels
it on disconnect, and caps execution at 300 seconds. A retry after response loss
can select a newer source generation; copy is not an exactly-once operation.

DeleteObjects validates its entire request before issuing a mutation: at most
1,000 nonempty UTF-8/XML keys of 1,024 bytes each, a 2 MiB body, and no versions
or conditional object selectors. Content-MD5 is supported; verified CRC32 may
replace it for the current SDK serializer. This is an explicit integrity
compatibility extension to the general-bucket MD5 rule. Every supplied supported
checksum and signed payload hash is verified; unknown integrity extensions are
rejected. Neither missing integrity nor malformed XML can delete any key.

A valid batch uses the same logical deletion as DeleteObject, sequentially in
input order. Each occurrence of a duplicate key gets its own result, and absent
keys are successes. Failures do not undo preceding successes; Quiet omits only
success entries. Dropping the request cancels remaining work. Batch deletion is
not a transaction or an exactly-once operation: retries converge for keys with
no intervening writes, but can delete a new unversioned PUT after response loss.
Physical reclamation remains independent of this request.

Multipart uploads keep a durable session, current part pointers, and immutable
part generations under one upload prefix. Replacing a part number advances its
generation while retaining the previous generation as reference evidence for
the chunk reclamation scan. The upload session records admission bounds, part
accounting, expiration, and completion state. Its pending part mutation is a
durable reservation: a writer stores the immutable generation, reserves the
pointer update with a session compare-and-swap, publishes the pointer, then
clears the reservation. A later request can finish an interrupted reservation.
Completion cannot freeze while one is pending.

Completion validates the ordered selected part numbers, raw MD5 values,
minimum nonfinal size, and current generations. It freezes the selection under
the session compare-and-swap, composes chunk locations with adjusted logical
offsets, and publishes one object record through a predecessor-fenced
object-key mutation. Publication checks the current pointers and immutable
generation digests again. It never reads or concatenates part bytes. The
multipart ETag is the hexadecimal MD5 of the selected raw part MD5 values in
order, followed by the part count suffix. Abort and expiry mark the session
terminal; physical chunk reclamation follows the ordinary reference and age
checks.

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
- **S3-I8 — Multipart selection fence:** a part pointer cannot advance after
  completion freezes its selection. An interrupted pointer reservation is
  settled before completion or abort proceeds.
