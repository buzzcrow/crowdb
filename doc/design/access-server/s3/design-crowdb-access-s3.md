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

Multipart uploads keep a durable `MultipartSessionRecord`, current part pointers,
and immutable `MultipartPartRecord` generations under one upload prefix. The
upload resource binds admission bounds, expiration and the completion selection.
Replacing a part number retains its previous generation as reference evidence;
expiry does not remove that evidence.

Independent-part uploads use the `S3MS\x05` resource format. They require the
checked product of `max_parts` and `max_part_bytes` to fit within
`max_staged_bytes`, so every valid set of current parts fits the staged-byte
bound without a shared accounting update. The ordinary bounds are 10,000 parts,
5 GiB per part, a 5 TiB completed object and a staged bound of 10,000 times
5 GiB. Completion separately enforces the object-size bound. These bounds do not
count retained superseded generations as current parts. Format 5 rejects pending
reservations and tighter or overflowing bounds. Codecs that do not support this
format reject it; resource CAS cannot change between independent and strict modes.

`MultipartRepository::put_stream_part` admits an open upload, loads its current
part pointer, persists an immutable generation, then conditionally replaces that
part's pointer. It does not reserve or clear a shared upload mutation, and distinct
part numbers share no accounting CAS. An initially matching length and raw MD5
return the retained part directly after admission. Once a writer enters the
mutation path, successful acknowledgement also observes the upload authority:
an observed abort or invalid open admission rejects it. This check reuses the
request's supplied admission timestamp and does not fence a later abort. Failed
acknowledgement does not remove persisted part evidence.

An admitted part writer may acknowledge B after Complete freezes A for the same
part number. The frozen selection still references A's exact
generation and digest; publication and retries use A even when the current pointer
selects B or is absent. A request first admitted after the freeze is rejected.
Completing B requires a new upload identity.

Strict uploads retain the reservation protocol and exact staged-byte accounting.
They include decoded `S3MS\x03` and `S3MS\x04` resources and uploads whose bounds
require a tighter quota or cannot establish the checked product bound. Their writer stores the immutable generation,
reserves the pointer update with an upload CAS, publishes the pointer, then clears
the reservation. A later request can finish an interrupted reservation; Complete
settles it before freezing. Publication also rechecks the selected current pointers.
Ordinary bounds do not upgrade a decoded strict resource to independent mode.

In both modes, an unused immutable candidate does not block a part number:
matching length and raw MD5 reuse its retained generation and locations;
different contents select the next unused generation. Generation numbers increase
monotonically and can have gaps. Definite pointer and upload CAS conflicts consume
the returned observation rather than issuing a confirmation GET. Part progress
carries that observation into the next iteration, retaining already-written bytes.
Its five-second local work deadline returns a retryable busy result under sustained
contention. An uncertain mutation returns without a confirmation GET after the
routed client's retry budget; a later request retries the full input against the
retained resource facts.

A bound upload is loaded through its authoritative key and validated against
bucket, object and upload identity; direct lookup does not read the listing index.
ListUploads and expiry scanning validate that index when enumerating resources.
Exact observed strict-format bytes are retained only in memory for conditional
updates; they do not add a response or recovery record.

Completion validates ordered part numbers, raw MD5 values, minimum nonfinal size,
current generations and aggregate object length. `freeze_completion` binds the
selected numbers, revisions and digests under the upload-resource CAS.
`publish_completion` validates those immutable generation identities and digests,
composes chunk locations with adjusted logical offsets, and publishes one object
record through an unconditional object-key PUT. Independent publication does not
reload current part pointers. It never reads or concatenates part payload bytes.
The multipart ETag is the hexadecimal MD5 of the selected raw part MD5 values in
order, followed by the part count suffix.

Every accepted Complete execution performs the object PUT, including retries after
an earlier success; it may overwrite a later ordinary PUT to the same name. The frozen selection is
the reference authority, not a recorded response. No predecessor, request digest,
cached response or publication-success transition is persisted. Abort and expiry
mark open uploads terminal while retaining their listing entries, pointers and
immutable generations. Publishing uploads remain bound, retaining the selected
generation references. Expiry reuses the authority loaded through the validated
listing entry and consumes the abort CAS observation without another confirmation
read. An uncertain mutation remains an error; later scanning rechecks the durable
resource state.

The background expiry worker owns discardable tenant and upload key positions
for one configured tenant. Each two-second step scans one raw tenant metadata
key or at most 20 upload-index entries, with a four-MiB aggregate result budget. The legacy
scan transport materializes values before applying that aggregation limit; it
does not enforce the budget on wire responses or native value materialization.
The first sixteen bytes after the tenant prefix supply a BucketId hint; the
name mapping and its payload are not discovery authority. An old bucket remains
discoverable after deletion or same-name recreation. The worker then scans that
identity's existing listing index, validating each canonical index and upload
authority before expiry. Upload creation persists the index before its authority.
Long name keys may supply harmless extra hints; short keys advance by their
observed raw position. There is no bucket-count cutoff.

Selecting a hint also saves the inclusive successor of the entire bucket
prefix. Finishing, failing or cancelling its index scan can therefore move to
later identities without scanning retained object, part and generation rows
one by one. A later complete round retries discarded hints. Successful partial
index progress advances before its point operation, so a corrupt row or
uncertain abort cannot pin subsequent uploads. A process restart scans the
configured tenant again without loading a persistent progress record.

Bucket discovery requests one raw row at a time. If a materialized first row
exceeds the aggregate budget, the client returns only its validated key. The
worker retains its identity hint for the next window and reports an oversized-row
error, preserving the original metadata for later rounds. A short key instead
advances its raw position. A transport failure that
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
