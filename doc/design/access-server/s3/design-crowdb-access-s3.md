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

Bucket creation first attempts create-if-absent. A returned active mapping is
the result; a tombstone is replaced with a fresh bucket identity through CAS.
Definite conflicts reuse the returned mapping directly, with bounded attempts
and no confirmation GET. Missing observations permit create-if-absent again.
Transport errors propagate without treating an unknown result as a conflict.

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

User bootstrap reads the bounded final credential inventory to preserve existing
keys and reject already ambiguous or disabled users. If absent, it derives a
stable public access-key identifier from the user and conditionally creates one
final encrypted credential. The secret and encryption nonce remain random;
the public identifier is not an authentication secret. A collision cannot replace
another user: the losing caller reads the exact target and validates its user,
ciphertext and enabled state. Successful creation returns its known token without
another inventory scan. There is no persistent bootstrap preparation or response.
Group-0 CAS currently supplies only a conflicting revision, so that exceptional
path needs one authoritative GET. An absent value after an uncertain write remains
an error for the caller to retry.

Explicit token issuance can still create multiple credentials for a user;
bootstrap does not provide an atomic uniqueness guarantee against that independent
administrative operation. Existing duplicates fail the inventory check. Unused
legacy preparation records are neither authentication data nor a source for
restoring a deleted or disabled final credential. Deleting the final credential
and invoking bootstrap creates a fresh random secret; ordinary lookup never
creates data. Credential snapshots continue to load only final encrypted records.

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

Presigned PUT and UploadPart also accept those checksum values and their SDK
algorithm declaration in the signed query. Authentication verifies the original
URI first; normalized checksum fields then enter the same upload verifier.
Duplicate query fields, header/query overlap and declarations on other
operations are rejected before publication. Signed query values cannot bypass
body integrity checks.

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
records do not retain creation timestamps.

User metadata is accepted through x-amz-meta-* headers on ordinary PUT,
multipart initiation and CopyObject. Nonempty HTTP token names are normalized
to lowercase; values are opaque printable ASCII strings. Combined key/value
bytes, excluding the header prefix, are limited to 2 KiB. Duplicate names,
invalid values and unsupported operation placement are rejected before writes.
The complete attribute map is published with the object's immutable generation
and returned on HEAD, GET and ranged GET. A new upload without metadata clears
the previous generation's attributes. Multipart sessions persist initiation
attributes across part replacement and recovery and publish them on completion.
The session schema has a distinct version; old-version data migration is outside
the supported contract.

STANDARD is the sole storage class reported by listings and accepted explicitly
on PUT, multipart initiation and CopyObject. Other classes are rejected; this
selector does not alter the configured storage policy or introduce tiering.

Server-side CopyObject and UploadPartCopy resolve source and destination through
the configured S3 tenant. Copy selects one immutable source record before
returning response headers; subsequent source overwrite or logical deletion
does not select new bytes. The source is streamed through bounded readers and
writers, respecting writer capacity, and publication uses the ordinary object
or multipart-part fences. No source reference is shared with the destination.
Copy never deletes the source. Unpublished candidates use ordinary reclamation.

CopyObject supports at most 5 GiB and copies the entire payload. COPY preserves
Content-Type and user metadata; REPLACE selects the supplied Content-Type
or application/octet-stream and replaces the entire user metadata map, including
clearing it when omitted. A self-copy requires REPLACE.
Cache/disposition/encoding/language/expiry metadata, version selectors, tags,
encryption, storage-class changes and destination conditions are unsupported
and rejected. Source ETag/date conditions apply to the captured generation;
their failure returns PreconditionFailed. Copy selectors must be signed.
The optional SDK x-id query marker must match CopyObject or UploadPartCopy and
occur at most once; it does not change operation selection.

UploadPartCopy supports complete objects and explicit inclusive byte ranges
from source objects larger than 5 MiB, with the session's normal part bounds.
It replaces a part using the durable generation-selection protocol below.
Neither interrupted copy path publishes partial bytes. After request validation
and source selection, the HTTP 200 response carries whitespace keepalives and
ends with CopyObjectResult, CopyPartResult, or an embedded Error. Clients must
consume and validate the whole response. The body owns the copy future, cancels
it on disconnect, and caps execution at 300 seconds. A retry after response loss
can select a newer source generation; copy is not an exactly-once operation.

Destination mutation conditions (`If-Match`, `If-None-Match` and date conditions)
are rejected before dispatch. Object GET/HEAD and copy-source conditions remain
supported. Ordinary object publication and deletion do not acquire a preliminary
object read or conditional mutation.

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
success entries. After validation, the response streams the XML opening, each
ordered result, and the closing tag. Quiet successes send XML whitespace to
maintain read progress. The response body owns the deletion stream; dropping
it cancels remaining work. Batch deletion is
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

Different part numbers can publish concurrently. A writer helps a pending
reservation and re-evaluates its own publication when the durable session
revision advances, retaining its already-written bytes. An immutable candidate
left by a failed reservation does not block that part number: matching length
and raw MD5 reuse the retained candidate under the same idempotency rule as a
published part; different contents select the next unused generation. Generation
numbers therefore increase monotonically and can have gaps. Retained candidates
remain immutable, and accounting changes only through the session fence.
Session and part-pointer replay checks consume the CAS conflict observation;
a confirmed conflict does not require another GET to test whether the exact
proposed bytes were installed. Once the routed client exhausts its retry budget,
an uncertain mutation returns to the caller without a confirmation GET. A later
request retries the same input and validates the retained resource facts.
Part publication carries its latest loaded session into the next
iteration instead of discarding it and reading again. This coordination is scoped
to one upload and preserves exact staged-byte accounting and completion selection;
independent uploads share no reservation record.

After a successful reservation, the writer reuses its known part pointer and
immutable generation to publish and settle it. Recovery loads that evidence
when a later request helps the reservation. A bound upload is loaded directly
through its authoritative key and validated against its bucket, object, and
upload identity; direct identity lookup does not read the listing index.
ListUploads and expiry scanning validate that index when enumerating resources.
Definite
conflicts retain the typed upload record for the next operation. Part progress
has a five-second work deadline so continuous contention cannot run forever;
expiration of this local work budget returns a retryable busy result.

Completion validates the ordered selected part numbers, raw MD5 values,
minimum nonfinal size, and current generations. It freezes the selection under
the upload-resource compare-and-swap, composes chunk locations with adjusted logical
offsets, and publishes one object record through an unconditional object-key PUT.
Every Complete retry repeats this PUT, including after an earlier success; it may
overwrite a later ordinary PUT to the same name. The selected part numbers,
revisions and digests remain immutable. No predecessor, request digest, cached
response or publication-success transition is persisted. Legacy records are
decoded into that selection and retain their exact observed bytes only within
the request for conditional updates. Publication checks the current pointers and immutable
generation digests again. It never reads or concatenates part bytes. The
multipart ETag is the hexadecimal MD5 of the selected raw part MD5 values in
order, followed by the part count suffix. Abort and expiry mark the session
terminal while retaining its listing entry, part pointers and immutable part
generations. Expiry reuses the authority loaded through the validated listing
entry and consumes the abort CAS observation without another confirmation read.
An uncertain mutation remains an error; later scanning rechecks the durable
resource state.

The background expiry worker owns discardable bucket and upload key positions
for one tenant. Each two-second step scans one raw bucket mapping or at most
20 upload-index entries, with a four-MiB aggregate result budget. The legacy
scan transport materializes values before applying that aggregation limit; it
does not enforce the budget on wire responses or native value materialization.
Bucket discovery continues after filtered tombstones and legacy object entries even when none
of those rows is an active bucket. It has no bucket-count cutoff. Raw positions
advance before decoding a row or dispatching its point operations, so a corrupt
returned row or uncertain abort cannot pin later uploads. A failed or cancelled
upload scan drops that bucket hint, allowing later buckets to advance; a later full
round discovers the skipped bucket again. A process restart starts a fresh
scan without loading a persistent progress record.

Bucket discovery requests one raw row at a time. If a materialized first row
exceeds the aggregate budget, the client returns only its validated key. The
worker advances that position and reports an oversized-row error, retains the
original metadata, and revisits it in later rounds. A transport failure that
provides no validated key cannot be skipped safely and remains retryable.
Obtaining discovery keys without materializing their values requires a separate
key-only or native bounded-scan capability.

Workers continue after 50 milliseconds while a round has more work, wait
15 seconds between complete rounds, and defer failures for 500 milliseconds.
These local cursors authorize no physical deletion. Physical range discovery,
reference checks and retirement fences remain separate reclamation duties;
marking an upload terminal does not report its physical storage reclaimed.

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
