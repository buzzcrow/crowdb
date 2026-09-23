<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Native Iceberg Storage

Iceberg is CROWDB's HTTP catalog and table-storage access model. Catalog,
namespace, table, snapshot, commit, and file concepts are first-class CROWDB
storage authorities, not management metadata layered over S3.

Depends on: [Access Server](../design-crowdb-access-server.md),
[Chunk I/O](../../chunkio/design-crowdb-chunkio.md), and
[Chunk-KV](../../chunkds/design-crowdb-chunk-kv.md).

The backed-up [REST Catalog OpenAPI](iceberg-rest-catalog-open-api-1.11.0.yaml)
and [Table Specification](iceberg-table-spec-1.11.0.md) are normative.

## Table of contents

1. [Intent and boundary](#1-intent-and-boundary)
2. [Authority model](#2-authority-model)
3. [HTTP and FileIO surfaces](#3-http-and-fileio-surfaces)
4. [Commit and lifecycle](#4-commit-and-lifecycle)
5. [Compatibility](#5-compatibility)
6. [Relationship to other access models](#6-relationship-to-other-access-models)
7. [Correctness invariants](#7-correctness-invariants)

## 1. Intent and boundary

The Iceberg module lets standard Iceberg engines use CROWDB without first
modeling tables as ordinary S3 objects. It terminates an independent HTTP
listener and owns Iceberg request, authentication, error, compatibility, and
lifecycle behavior.

The module does not become a query engine and does not own physical placement,
replication, erasure coding, repair, or disks. Server-side scan planning and
table processing are separate capabilities rather than requirements of the
core storage authority.

## 2. Authority model

Catalogs contain multipart namespaces; namespaces contain named tables; tables
select immutable Iceberg metadata generations; metadata and snapshots reach
immutable table files. Stable identities separate durable authority from
renameable names.

Standard Iceberg metadata is the recoverable table state. CROWDB may maintain
derived indexes or projections for scale, but they are disposable and cannot
become a second table authority.

Iceberg metadata stores bounded logical records and opaque data references.
Physical chunk placement and storage topology remain below the access boundary.

### Catalog foundation

One active root selects a random stable CatalogId and activation epoch. Display
rename updates its authority without moving descendant keys. System-scoped
management receipts, audit and retry bindings survive catalog replacement;
resource records and retained REST response bodies are catalog-scoped.

Initialize, rename and clear use bounded single-key CAS state machines, not a
global lock or a multi-key transaction. A root retains the operation identity
until its durable outcome and audit can be recovered by any instance. Clear
fences admission, records a maintenance observation after the durable fence,
publishes an empty replacement under maintenance, and persists the grace proof
before reopening admission. Completion uses persisted lease, request, delegated
access and clock-skew limits, never shorter restart configuration. Retired
authorities remain unreachable; physical deletion is not implemented.

The baseline has no root lease or delegated credentials. Each HTTP connection
has an absolute lifetime starting before its authoritative root read and covering
response transmission. Listeners stop admission before bounded draining;
startup and periodic reconciliation resume interrupted management operations.

Management and shared REST retry ledgers each use 4096 deterministic hash slots.
A slot occupied by an unfinished or unexpired operation rejects new admission;
it is never evicted for capacity. Management audit uses the same slot mapping.
Client identities use UUIDv7 issuance time with a 24-hour admission window and
30-second future-clock allowance. Retention starts at first admission and includes
grace. Principal, digest and catalog context must match before REST replay;
catalog replacement prevents old-body replay or rebinding. Terminal results are
immutable, while transient failures retain recoverable state. Requests without
client keys receive distinct internal identities, not cross-request deduplication.

Immutable operation payloads use 32-KiB pages with a 2-MiB aggregate limit. Each
reference binds catalog, operation, content digest and total size; readers validate
every page and the complete digest. Small retry responses remain inline. Larger
responses publish one immutable manifest only after all pages are durable, then
complete the system retry binding. A lost reply resumes page writes or replays the
published manifest without changing the original response.

Namespace operation journals occupy a separate key scope from HTTP responses.
They preserve request identity, principal, stable target and parent IDs, immutable
mutation snapshots, phase revisions and bounded child-probe cursors. Phase CAS
arbitrates publication versus abort; publishing cannot transition back to abort.
Snapshots cannot change after their write phase starts. Probe cursors advance
within one parent-scoped child range and reset when switching ranges.

Authoritative namespace reads resolve each parent/name mapping against the
selected stable authority and full canonical identifier. Reservations, stale
epochs, missing targets and tombstones are not visible; corruption is an error.
Active-context checks bracket resolution so retirement cannot turn an old-domain
lookup into a response from the replacement catalog.

Property updates persist their input and immutable before/after snapshots, then
CAS the whole namespace authority. Publication advances the property and mutation
revisions but preserves the name epoch and admission fence. An operation marker
protects uncertain publication evidence until the terminal result is durable;
another writer can finish that operation before replacing its marker. Cleanup
uses a conditional write and advances the mutation revision again. After a
definitive CAS conflict proves the input revision is obsolete, a property update
may return to preparation with fresh snapshots; unknown outcomes never take that
path. Work is bounded and exhaustion remains retryable, not a terminal conflict.
Property preparation uses the same holder-bound marker dispatcher as creation
and drop. It can finish interrupted child admission or a nonempty drop before
publishing properties; recursive helpers consume the caller's phase budget.
Namespace mutations use the shared HTTP retry ledger before executing their
durable operation driver; terminal client errors are retained alongside success.

Namespace creation installs a recoverable parent/name reservation before a parent
authority CAS. Nested admission leaves a pending-operation marker and advances only
the mutation revision; a top-level admission conditionally validates the active
root without replacing its management operation. Helpers resolve uncertain parent
writes before allowing subsequent parent mutation. Definitive admission conflicts
may retry with fresh parent snapshots while retaining the name reservation.
Publication selects an initial authority and replaces the reservation with its
published mapping; a creation marker remains until the result is durable. Abort
records retain their exact failure outcome before conditional reservation cleanup.
Recursive creation helping shares one bounded phase budget. Root-admission
backend identities include the individual creation operation, so a different
creator cannot reuse a cached no-op CAS result from before its reservation.

Namespace drop persists a Ready-to-Dropping fence before scanning its two child
index ranges. Durable cursors advance across bounded pages; reservations are
helped, stale namespace mappings are conditionally removed, and corruption blocks
the proof. A live child restores Ready without changing the name epoch or property
revision. Only completion of both ranges permits the fenced tombstone CAS.
Terminal replay and conditional cleanup cannot delete a recreated NamespaceId.
Table-child records currently fail closed until table authority is implemented.
Each listener runs a namespace-journal sweep with bounded pages, per-operation
phase budgets and a wall-clock deadline. The sweep resumes abandoned operations
and their conditional mapping cleanup without requiring a client retry. Catalog
changes invalidate its cursor; cancellation preserves durable recovery evidence.
Alternating mapping sweeps help durable reservations and conditionally remove
published bindings disproved by authoritative state. Corruption and unresolved
reservations never authorize deletion; no sweep physically removes file bytes.
The listener exposes authenticated namespace listing, load and exists routes.

Namespace list pages scan bounded direct-child ranges and validate each published
mapping against its authority and canonical parent spelling. Reserved and stale
entries are omitted; corruption fails the page. HMAC-authenticated continuations
bind the catalog activation, stable parent identity, spelling, page size and last
scanned key. A stale-only page can therefore be empty while retaining a token.
Unpaginated lists build a complete in-memory spool before success headers, capped
independently at 2 MiB, 1024 results, 4096 scanned mappings and four concurrent
spools. Atomic admission rejects excess work without waiting. The request deadline
bounds construction and sending; cancellation drops the spool permit. Completed
responses stream in 16-KiB frames. Absent page tokens request complete results;
empty page tokens begin paginated mode. Tokens use a domain-separated signing key
derived from the configured credentials so equally configured listeners interoperate.

## 3. HTTP and FileIO surfaces

The REST Catalog is the portable control surface. It exposes only capabilities
CROWDB implements with compliant Iceberg semantics.

The catalog listener exposes authenticated config and namespace REST. An absent or
empty warehouse selects the sole active catalog; other selectors fail with
`NoSuchWarehouseException`. Its endpoint list advertises namespace CRUD and all table
format capabilities are disabled. Namespace mutations advertise a 24-hour UUIDv7
idempotency window, bind canonical route, exact request input, principal and catalog
activation, and retain large results in immutable payload pages. Server errors
remain retryable, never terminal ledger outcomes. Exhausting the configured request
deadline leaves durable recovery evidence; subsecond completion is not guaranteed.
Static bearer credentials
separate reader, writer, management and clear roles; this is not an OAuth token
issuer. All four credentials are required and distinct. Writer has a separate
namespace-write capability and no catalog management or clear privilege; reader,
manager and clearer do not inherit namespace-write rights. All four can read the
configuration endpoint and namespaces. Only writer may invoke namespace mutations.
Management commands are separate from the Iceberg REST listener. Operational
configuration is in the [user guide](../../../user-manual/user-guide.md#9-iceberg-catalog-foundation).

Iceberg FileIO uses reserved S3-shaped locations so existing Iceberg clients can
address immutable metadata and data files. The shape is a compatibility
contract, not delegation to the general S3 authority. File publication,
immutability, authorization, and deletion remain under Iceberg control.
Typed locations use lower-case unpadded base32 catalog IDs and lower-case hex
table IDs. Relative UTF-8 object keys preserve case, literal percent signs, plus
signs and repeated internal slashes; the whole object key is bounded to 1,024
bytes. Dot traversal, leading slash, backslash, controls, query and fragment
delimiters are rejected rather than normalized. HTTP percent decoding belongs
only at the transport boundary, not in stored S3-shaped locations.

Native file records bind FileId to exact location, kind, format, canonical length
and SHA-256 digest. Eligible metadata stores at most 16 KiB inline; bounded LZ4
compression considers at most 64 KiB original input, and decoding verifies the
canonical length and digest. Other file kinds retain a fixed-size chunk root,
never a growing location vector. Hints are non-authoritative and out-of-bounds
hints are ignored. The publication primitive stages an immutable authority before
the exact-location CAS; equal-content retries return the selected FileId, while
conflicts retain losing candidates without overwriting or physical deletion.
Streaming format sealing and the native FileIO HTTP surface remain unexposed.

Chunk-backed files use bounded leaf blocks and immutable chunk-resident directory
pages, with at most 256 children per page and eight directory levels. Each page
binds its catalog, table and file identity, child heights and covered byte count.
The writer retains only one partial leaf and bounded per-level frontiers. Native
block completion waits for the readable chunk cursor before publishing a root.
Pull readers retain one leaf, verify directory/leaf digests and read no future
block until requested; full-file reads also verify the canonical digest. Range
parsing accepts one contiguous interval and rejects multiple ranges explicitly.

Metadata JSON structural validation uses a bounded pull-reader bridge and an
ignored-value parser rather than retaining the metadata graph. A separate scanner
bounds nesting and verifies raw UTF-8 before parser scratch can grow. Admission
caps blocking workers; cancellation keeps its permit until the worker exits.
This structural check does not replace Iceberg schema or commit validation.

Parquet and Puffin container probes derive footer ranges from canonical framing,
ignoring stored hints even when those hints happen to be in bounds. Their reads
retain one bounded leaf and only fixed-size framing bytes, independent of the
advertised footer size. Puffin probing also checks footer-start magic and reserved
flags. Container framing does not validate footer contents or data semantics.
ORC probing retains at most 255 postscript bytes, checks protobuf wire framing
and resolves footer/metadata spans without decoding stripe directories. It accepts
legacy header-only magic and skips bounded unknown protobuf fields.

Delegation tokens carry catalog activation epoch, table, principal fingerprint,
nonce, exact operation set, issue/expiry times and independent request/file byte
limits. Domain-separated HMAC authenticates bounded claims and derives per-grant
S3 credential material without a mutable credential registry. Verification requires
a freshly checked Ready context; file DELETE is not representable. These token
primitives are not yet credential vending or FileIO request-signature verification.

Writes and reads stream through bounded CROWDB storage clients. Delegated FileIO
access may move immutable ranges without an Access Server payload bounce, but
cannot overwrite published files or bypass table reachability.

## 4. Commit and lifecycle

A table commit validates requirements against one selected table generation,
constructs a complete new metadata state, writes immutable candidate files, and
atomically publishes one new table head. Concurrent commits either publish from
the generation they validated or fail for the client to reconcile; they never
merge implicitly.

Retries are idempotent across response loss. Any healthy Access Server can
recover the durable operation outcome, so no server instance is a table leader
or lock owner.

Drop, replacement, and snapshot expiration remove logical reachability first.
Physical reclamation follows a proof that no live metadata, snapshot, reference,
lease, or retained operation can reach the file. General S3 deletion and
lifecycle rules cannot reclaim Iceberg-owned data.

## 5. Compatibility

CROWDB covers the core Iceberg format semantics for v1, v2, and v3, including
reading, creating, writing, and the defined version upgrades. Mandatory behavior
for the selected format version is not weakened. Optional features are exposed
only when their complete semantics are enabled.

REST wire types, Iceberg domain state, and CROWDB storage records remain
separate. Unknown or disabled requirements and updates fail before mutation.
The backed-up specifications decide behavior when implementations differ.

## 6. Relationship to other access models

General S3 and Iceberg share chunk, Chunk-KV, transport, credential, and safe
utility mechanisms, but not semantic authority. S3 bucket records, overwrite,
delete, listing, and lifecycle behavior cannot publish or alter an Iceberg
table.

Dataset may consume data described by an Iceberg table through an explicit,
generation-bound reference. That does not transfer snapshot, commit, file, or
reclamation authority to Dataset.

## 7. Correctness invariants

- **ICE-I1 — Native authority:** catalog, namespace, table, snapshot, commit,
  file, and reclamation semantics belong to Iceberg rather than general S3.
- **ICE-I2 — Standard recoverability:** published standard Iceberg metadata is
  sufficient to recover table state; derived projections are not authoritative.
- **ICE-I3 — Atomic table publication:** a table head selects exactly one
  complete immutable metadata generation.
- **ICE-I4 — Validated concurrency:** a commit publishes only from the table
  generation against which its requirements were validated.
- **ICE-I5 — Immutable files:** a published Iceberg file is never overwritten.
- **ICE-I6 — Reachability before reclamation:** physical deletion cannot precede
  proof that no protected Iceberg state reaches the file.
- **ICE-I7 — Spec compliance:** supported v1, v2, and v3 behavior preserves all
  mandatory semantics of the selected format version.
- **ICE-I8 — Bounded operation:** table size and history do not determine one
  Access Server request's retained memory or unbounded work.
