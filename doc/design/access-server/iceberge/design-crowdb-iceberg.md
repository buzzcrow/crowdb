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

Table ALL/REFS loads read and verify canonical storage, run the complete bounded
metadata parser, and recheck the namespace and selected head before responding.
ALL preserves exact canonical bytes; REFS filters snapshots from the same validated
content. Table loads do not read or write projection pages, validation receipts,
or other persistent cache state, including on repeated and conditional requests.
Optional generation-local JSON projections used by file-level selection remain
disposable derived data with canonical fallback; they cannot replace table
validation, participate in commit proofs, or become file/publication authority.

Iceberg metadata stores bounded logical records and opaque data references.
Physical chunk placement and storage topology remain below the access boundary.

### Identity directory and file authority

The Iceberg key codec carries its storage family and layout version separately
from the directory payload. A table-owned payload begins with
`<catalog-id>/<storage-namespace-id>/<table-id>/`; it has no redundant
`iceberg`, `namespace` or `table` labels before those IDs. The storage namespace
is frozen at table creation, including staged creation. The default namespace
has a durable ID under the same rules as named namespaces.

```text
<catalog-id>/
├── head
├── namespace/name/<encoded-namespace> -> namespace-id
└── <namespace-id>/
    ├── head
    ├── name/<encoded-table-name> -> (storage-namespace-id, table-id)
    └── <table-id>/
        ├── head
        ├── file/<public-relative-filename>
        └── projection/<metadata-generation>/<page-or-receipt>
```

Catalog and namespace control records have their own typed scopes. Namespace
names use the existing unambiguous multipart-name encoding. Table-name lookup
returns the complete storage identity; internal tasks carry it as well, so no
table-ID reverse index is needed. A move updates live name mappings and the
head's live identity/epoch while retaining the storage path. Deleting the old
namespace cannot cascade into moved tables, and IDs are never reassigned.

Each file has one `file/<public-relative-filename>` record. The suffix is also
its public object-key suffix. Its value contains FileId, canonical length,
format, integrity metadata, native descriptor/root and lifecycle state. FileId
is a value/locator binding, not another file index. One create-only CAS publishes
the complete record after durable bytes; exact-key conflicts cannot overwrite
selected content. Retries preserve the selected file identity and URL. Workflow
records exist only where recovery or multipart/GC progress requires them, not
as three mandatory records for every file.

Service recovery journals are separate from that directory. Their existing
catalog control scopes remain explicitly versioned binary keys, even in a
native catalog. They are not file lookup indexes and cannot produce an object
listing. The service must discover abandoned work before a table head exists
and after a table has disappeared, without a table-ID reverse index:

- Legacy creation, commit and drop journals and payload pages are keyed by
  operation identity. Their selected heads carry the frozen storage identity.
  New creation and staging keep complete input only within the request.
  Name moves use bounded namespace occupancy instead
  of request journals; existing rename journals remain recoverable.
- Multipart sessions and parts are keyed by upload identity/part number. Catalog
  admission records apply only to legacy credited sessions; new streamed sessions
  use independent per-session bounds.
  Every session and part carries and validates the full storage identity.
- Write intents/fences track allocated but potentially unpublished native
  extents, including failed creation. Catalog maintenance scans their bounded
  control scopes; the value binds the full table/file owner and exact chunk root.
- GC tasks, candidates, claims, proof nodes and pages belong to the recovery
  worker. A retirement task can cover several tables and adopts interrupted
  purge work. Claims coordinate physical deletion across tasks/generations;
  candidate values retain the canonical filename and full storage identity.

These records are retained only for retry, fencing and reclamation. They do not
add another published file authority or participate in direct GET/HEAD. GC data
discovery chooses v1 or v2 from the persisted catalog layout; independent
service-control scans never merge the two file namespaces. Head, file and
projection records use the native table directory. Historical native injection
receipts remain decodable for reclamation; uploads create no request receipt.

### Standard metadata and bounded scale

TableHead selects an immutable standard metadata JSON file. Its `snapshots`
array contains snapshot records and its `refs` map selects branches/tags;
snapshot-log and metadata-log retain their standard meanings. Snapshot records
reference manifest-list files, which reference manifest files, which describe
data/delete files. Reading v1 also supports its direct `manifests` alternative.
The backed-up v1/v2/v3 specification determines fields, sequence inheritance,
schema/partition evolution, delete matching and row lineage. No authoritative
snapshot or manifest KV index substitutes for those standard file contents.

All of these file kinds use the same single file-record model. Large metadata
JSON, manifests and manifest lists are native chunk-backed objects. KV values
and TableHead never embed an unbounded metadata document or snapshot array.
Small eligible metadata may use the bounded inline representation; larger
objects and large extent sets use chunk payloads and bounded directory roots.
Changing storage representation preserves the canonical bytes returned by
FileIO. Table publication selects a completely durable metadata generation by
CAS; a losing commit cannot expose a partially written generation.

Metadata files use useful public relative prefixes such as
`metadata/<name>.metadata.json`, `metadata/<name>.avro` and
`statistics/<name>.puffin`. These are defaults, not restrictions on safe
client-chosen paths. The optional generation-bound projections described above
support file-level selection; they are not used by table ALL/REFS loads or duplicate
file authorities. Parser, collection and response budgets remain explicit even for
chunk-backed JSON.
An unsupported oversize request fails clearly rather than silently truncating
snapshots. ALL and REFS keep their standard response semantics; internal
projection paging does not introduce private REST pagination fields.

New catalogs use the identity-directory layout version. Existing legacy
catalogs retain their own decoder, keys, URLs and read/write behavior. Layout
selection is catalog-bound; relabeling or mixing key versions in one scan is
rejected. In-place migration is not implicit in decoding.

### Catalog foundation

One active root selects a random stable CatalogId and activation epoch. Display
rename updates its authority without moving descendant keys. System-scoped
management receipts, audit and retry bindings survive catalog replacement;
resource records and retained REST response bodies are catalog-scoped.

Initialize, rename, capability activation and clear use bounded single-key CAS state machines, not a
global lock or a multi-key transaction. A root retains the operation identity
until its durable outcome and audit can be recovered by any instance. Clear
fences admission, records a maintenance observation after the durable fence,
publishes an empty replacement under maintenance, and persists the grace proof
before reopening admission. Completion uses persisted lease, request, delegated
access and clock-skew limits, never shorter restart configuration. Retired
authorities remain unreachable; bounded retired-catalog cleanup follows the
persisted retention and owner proofs.

Format capability bits are durable catalog authority. Zero means no advertised
table format service; startup never rewrites or widens a legacy zero profile.
An authenticated management operation explicitly activates a validated profile
under the root fence. It preserves the catalog ID, activation epoch, table keys,
name generation and admission bounds, advances config generation, and may only
add support. A resumed operation replays its original profile and audit result.
Clear creates a new zero-profile catalog that requires separate activation.

The baseline has no root lease. Each HTTP connection closes after five minutes
without network progress; active streamed file bodies and multipart completion
heartbeats extend the idle deadline. Request dispatch has a separate deadline
starting at connection acceptance; incomplete request headers close at that
deadline, while an active response remains governed by network idleness. REST
and FileIO admission reject configured request timeouts exceeding the persisted
catalog request bound. Newly initialized runtime catalogs use a five-minute
request bound;
new catalogs also persist a fifteen-minute delegated-access bound. Restart never
increases persisted bounds. Explicit catalog clear may expand them componentwise
under the maintenance fence and waits the resulting full grace before admission;
neither clear nor smaller restart settings can shorten existing bounds.
Listeners stop admission before bounded draining;
startup and periodic reconciliation resume interrupted management operations.

Management and audit ledgers use 4096 deterministic fast-hash slots with
exact-identity overflow keys. An occupied slot routes a different identity to
its own durable key. Their retention and authority checks belong to the
management operation contract. Client write identities use UUIDv7 issuance
time with a 30-second future-clock allowance; requests without a client key
receive independent internal identities.

REST namespace and table writes do not allocate a shared HTTP retry binding
or cache terminal HTTP results. Each request validates its current authority
and input. Existing business drivers bind operation identities to their stable
resources and validate their own request preconditions; direct expected-base
table updates retain their head CAS contract. A failed request without a
business operation can be submitted again after the resource state changes.
Legacy HTTP bindings, overflow keys and response payloads remain decodable
for retention cleanup, with no producer on new request paths.

Immutable operation payloads use 32-KiB pages with a 2-MiB aggregate limit. Each
reference binds catalog, operation, content digest and total size; readers validate
every page and the complete digest. Business operation drivers use these
references for their existing inputs and results. Legacy large HTTP response
manifests remain readable by cleanup; independent requests do not create them.

New namespace creation writes only its final name reservation and namespace
authority. Before publishing a nested child, it places that child's ID in the
parent's `creating_child` occupancy; the child authority is already complete.
Parent deletion must settle or cancel this occupancy before proving emptiness.
Publication clears it through an exact parent update, preserving concurrent
properties. Top-level creation needs no parent occupancy. Helpers resolve an
interrupted create from these resource facts, without an input or response
journal. Cancellation and retired-catalog cleanup permanently retire the same
child authority slot before releasing its exact name reservation, including an
absent slot. A delayed first authority write therefore cannot resurrect it.
Occupied parents use a format that older writers reject.

Legacy namespace creation, property and drop journals occupy a separate key
scope from HTTP responses. Retained records bind the request and stable target,
immutable mutation snapshots, phase revisions and bounded child-probe cursors.
Recovery validates their phase transitions, snapshot identities and cursor ranges.
Fresh namespace writes do not create these journals or payloads.

Authoritative namespace reads resolve each parent/name mapping against the
selected stable authority and full canonical identifier. Reservations, stale
epochs, missing targets and tombstones are not visible; corruption is an error.
Active-context checks bracket resolution so retirement cannot turn an old-domain
lookup into a response from the replacement catalog.

Property updates apply the request's assignments and removals directly to the
current namespace authority through one conditional write. This advances the
property and mutation revisions once, preserving the name epoch and admission
fence. A definite conflict supplies the next authority; the request reapplies its
changes while preserving other properties. Deletion, changed identity or a
pending lifecycle operation blocks that update. Active-context validation
brackets resolution and checks completion. Retry work is bounded.

Property requests persist no input, operation journal, marker, response payload
or HTTP retry binding. A request that leaves the properties unchanged performs
no write. A lost reply is returned as an error; the client can submit its changes
again. Each response describes that request's actual removed and missing keys,
so a repeated removal can move from removed to missing. Repeating assignments
can overwrite a concurrent assignment to the same property. Other properties
are retained. Old property journals remain readable by recovery, including the
holder-bound marker dispatcher. Namespace creation and drop likewise use
resource facts without an input or response journal.
The catalog configuration does not advertise a universal idempotency-key
lifetime, since independent property updates do not retain operation responses.

Legacy namespace creation recovery installs a parent/name reservation before a
parent authority CAS. Its nested admission leaves a pending-operation marker
and advances only the mutation revision; a top-level admission conditionally
validates the active root without replacing its management operation. Helpers resolve uncertain parent
writes before allowing subsequent parent mutation. Definitive admission conflicts
may retry with fresh parent snapshots while retaining the name reservation.
Publication selects an initial authority and replaces the reservation with its
published mapping; a creation marker remains until the result is durable. Abort
records retain their exact failure outcome before conditional reservation cleanup.
Recursive creation helping shares one bounded phase budget. Root-admission
backend identities include the individual creation operation, so a different
creator cannot reuse a cached no-op CAS result from before its reservation.

Namespace drop must reject a nonempty namespace. Before fencing the selected
namespace, deletion settles its child-creation occupancy, rename occupancy and
legacy pending markers. These resource facts protect child publication and name
moves while the parent is checked for emptiness.

Each drop selects the current namespace binding once. An exact CAS changes a
Ready authority to ownerless Dropping, advancing the admission fence and
mutation revision; an existing Dropping authority can be helped without another
fence. Lifecycle tag 4 identifies this state and requires no pending operation,
child occupancy or rename; older decoders reject it. Dropping remains readable
and counts as a live child when an ancestor is being deleted.

Deletion scans both direct-child index ranges through bounded pages, validating
range, ordering, continuations and child bindings. Reservations are helped and
stale mappings are conditionally removed; corruption blocks the empty proof. A
live namespace or table child restores the exact Dropping authority to Ready,
advancing its admission fence and mutation revision while preserving properties,
name epoch and property revision, and returns a nonempty error. Only completion
of both ranges for the same exact Dropping authority permits its CAS to
permanent RetiredNamespace at the same authority key. The retired slot prevents
delayed authority creation from resurrecting that namespace identity.

Scan cursors and empty proofs exist only within the current invocation. A CAS
conflict supplies the next validated authority without a confirmation GET. A
changed Dropping generation discards both range cursors and the old empty proof
before rescanning; a Ready authority restored by another helper ends that drop
attempt. Unknown write results return an error immediately without a follow-up
read or write. An installed Dropping fence survives cancellation, timeout and
exhausted work budgets, so a later request or mapping-repair pass can restart a
complete proof. No input, phase, scan cursor or response record is persisted.
Active-catalog validation brackets selection, precedes each authority transition
and checks completion.

Terminal cleanup conditionally deletes only the raw name mapping selected by
that invocation. It cannot delete a replacement NamespaceId at the same name. A
fresh client retry selects the current name again and may delete a recreated
empty namespace; a nonempty replacement still fails. A missing current name
returns absence rather than a cached terminal response. Legacy owner-bound
lifecycle states and journals retain their recovery and exact-target cleanup.

Current property and rename-admission CAS conflicts with the same-key retired
slot report a conflict without another authority read. Retained legacy work
treats that slot as final parent absence and aborts work not admitted before
retirement. Cleanup still fences an absent table-head slot before removing its
exact reservation. Captured authority payloads keep their identity and owner
validation; a retired current parent does not make an invalid capture acceptable.

Table-child probes resolve published mappings against the selected table head.
Ordinary table creation leaves only a reserved name and its final head slot.
Probes retire an absent slot before clearing that exact reservation, or settle
the matching Ready head and reject deletion as nonempty. A creator reserves its
name before its last Ready-parent check, so a concurrent drop cannot both miss
the reservation and permit a later first head. Retained staged operations and
legacy lifecycle journals remain recoverable through their exact markers.
Stable namespace identity prevents old state from becoming visible under a
recreated namespace. Name moves occupy both participating parent
authorities before reserving the destination; deletion helps settle or fence the
move before taking a fresh emptiness proof. Corrupt
table authority blocks the emptiness proof.
Each listener alternates bounded recovery of retained namespace journals with
mapping repair, using local page cursors, work budgets and a wall-clock deadline.
Mapping repair helps child-creation reservations and ownerless Dropping resources,
including a full new child proof after an interrupted deletion, then conditionally
removes bindings disproved by authoritative state. Catalog changes invalidate
local cursors; cancellation preserves resource facts. Corruption and unresolved
reservations never authorize deletion; no sweep physically removes file bytes.
The listener exposes authenticated namespace listing, load and exists routes.

Namespace list pages scan bounded direct-child ranges and validate each published
mapping against its authority and canonical parent spelling. Reserved and stale
entries are omitted; corruption fails the page. HMAC-authenticated continuations
bind the catalog activation, stable parent identity, spelling, page size and last
scanned key. A stale-only page can therefore be empty while retaining a token.
Unpaginated lists build a complete in-memory spool before success headers, capped
independently at 2 MiB, 1024 results and 4096 scanned mappings. A shared
128 MiB byte budget admits response spools, reserving each operation's maximum
while constructing the response (2 MiB for namespace lists and 4 MiB for table
responses). Serialization shrinks the reservation to the retained buffer capacity;
the response releases it on cancellation or completion. Atomic admission rejects
excess work without waiting. The request
deadline bounds construction before success headers. Dispatch stops before that
deadline, reserving the smaller of 100 ms or 10% of the request timeout for
emitting a bounded error response. Header receipt does not restart this budget.
A stalled transport closes after the independent idle timeout. Cancellation
drops the spool permit. Completed
responses stream in 16-KiB frames. Absent page tokens request complete results;
empty page tokens begin paginated mode. Tokens use a domain-separated signing key
derived from the configured credentials so equally configured listeners interoperate.

## 3. HTTP and FileIO surfaces

The REST Catalog is the portable control surface. It exposes only capabilities
CROWDB implements with compliant Iceberg semantics.

The listener classifies complete method/path pairs before domain mutation and
uses the same fixed route set for discovery and protocol metric labels. Request
measurements use bounded atomic counters and follow response bodies through
completion or cancellation; streamed file bytes are measured when emitted.
Protocol counters never use principal, table name, token or raw path as a label.
An authenticated management-credential-only `GET /_crowdb/metrics` exposes a
bounded snapshot, including while catalog storage is unavailable. This local
diagnostic is not an Iceberg REST endpoint and is absent from `/v1/config`.

The catalog listener exposes authenticated config and namespace REST. An absent or
empty warehouse selects the sole active catalog; other selectors fail with
`NoSuchWarehouseException`. Its endpoint list advertises installed namespace and
table read/create/commit/lifecycle/credential routes only when the persisted
format profile permits them. A zero-profile catalog returns unavailable config
rather than publishing a misleading set of false overrides; table routes reject
until management activation. Selected table versions gate load, HEAD, credential
refresh, create and commit. Version upgrades require each intermediate edge,
including direct v1-to-v3 requests. FileIO bytes alone do not identify a table
file's semantic kind or grant format-version authority. File grants intersect
the principal role with the selected version: published tables require write
support for upload permission, while staged drafts require create support.
Runtime table routes require a persisted delegation bound of at least fifteen
minutes. Legacy catalogs below that bound retain foundation-only service; activation
requires an explicit clear with expanded bounds and a listener restart after the
maintenance grace. Namespace mutations, table creation and lifecycle operations
use retained UUIDv7 retry identities bound to route, input, principal and catalog
activation. Ordinary existing-table commits publish through one head CAS without
a separate durable retry-result ledger; a retry whose result is hidden by a later
generation may report an uncertain outcome. Server errors
remain retryable, never terminal ledger outcomes. Exhausting the configured request
deadline leaves durable recovery evidence; subsecond completion is not guaranteed.
Static bearer credentials
separate reader, writer, management and clear roles; this is not an OAuth token
issuer. All four credentials are required and distinct. Writer has a separate
namespace-write capability and no catalog management or clear privilege; reader,
manager and clearer do not inherit namespace-write rights. All four can read the
configuration endpoint and namespaces. Only writer may invoke namespace or table mutations.
Management commands are separate from the Iceberg REST listener. Operational
configuration follows the selected deployment profile and its startup inputs.

Iceberg FileIO uses reserved S3-shaped locations so existing Iceberg clients can
address immutable metadata and data files. The shape is a compatibility
contract, not delegation to the general S3 authority. File publication,
immutability, authorization, and deletion remain under Iceberg control.
Public URIs use the fixed virtual bucket `iceberg`:
`s3://iceberg/<catalog-id>/<storage-namespace-id>/<table-id>/<filename>`.
Native typed locations use lower-case fixed-width hexadecimal for all three
128-bit IDs. Legacy compatibility buckets retain their base32 catalog component.
Relative UTF-8 object keys preserve case, literal percent signs, plus
signs and repeated internal slashes; the whole object key is bounded to 1,024
bytes. Dot traversal, leading slash, backslash, controls, query and fragment
delimiters are rejected rather than normalized. HTTP percent decoding belongs
only at the transport boundary, not in stored S3-shaped locations.

Native `ListObjectsV2` retains this address model. A list request must specify
the complete `<catalog-id>/<storage-namespace-id>/<table-id>/` prefix or a
descendant prefix and a table-bound delegated list capability;
catalog-wide discovery and general S3 credentials are rejected. Reader and
writer FileIO credentials include listing; cleanup-only credentials do not.
Exact-object HEAD/GET/PUT remain independently usable. PyArrow missing-file
probes may issue listing to distinguish a missing object from a directory.

Empty, catalog-only, namespace-only and foreign-table prefixes fail before a
scan. HeadBucket/GetBucketLocation expose only the configured virtual bucket
and region to a valid grant; ListBuckets returns AccessDenied, and bucket
creation/deletion is unsupported. The URI host determines the bucket. Arrow
FileSelector adds a trailing slash to its key prefix and uses `/` as delimiter
for nonrecursive listing; an exact HEAD miss may probe `<object-key>/` without
widening scope. These behaviors follow the
[Arrow S3 implementation](https://raw.githubusercontent.com/apache/arrow/apache-arrow-25.0.0/cpp/src/arrow/filesystem/s3fs.cc).

Listing enumerates selected, fully published immutable file-location records,
including files awaiting a table commit or later reclamation. It excludes
incomplete uploads, unselected draft/losing candidates and logical deletion
markers. It does not enumerate the files reachable from a current snapshot;
clients use Iceberg metadata and manifests for that selection. Names, lengths
and ETags come from the selected record. Since file records do not store a
wall-clock publication time, listing returns the fixed Unix epoch timestamp;
clients must not use it as a file-age or reclamation signal.

Each page scans at most 256 records and 4 MiB, possibly returning fewer than
the requested maximum of 1,000 keys. The supported delimiter is `/`; URL
encoding, zero-key pages and table-scoped `start-after` are supported. XML is
bounded to 2 MiB. Unsupported or ambiguous selectors fail before scanning.
Pagination is a forward live traversal rather than a snapshot: unchanged
eligible sets have no omissions or duplicates, publication behind the cursor
is seen by a new traversal, deletion ahead removes that entry, and publication
ahead may appear. Common-prefix rollups advance past the entire subtree.
Authenticated continuation tokens bind the principal, credential nonce,
catalog epoch, table, prefix, delimiter and encoding. Their fixed expiry is
no later than the originating grant, and invalid or foreign tokens fail before
storage access. A credential refresh requires starting a new traversal.

List operates directly on `file/` records in public-key order. For example,
the public prefix `<table-path>/metadata/` maps to the KV prefix
`<table-path>/file/metadata/`; filename prefixes narrow that same range.
Pagination uses encoded prefix bounds and the last examined key, including
filtered tombstones and sparse pages. It never materializes or sorts the whole
table, opens metadata objects, scans unrelated file prefixes or enumerates
snapshots inside JSON. Returned keys omit the internal `file/` component.
The same limits apply whether a table has ten or millions of metadata files.

Native file records bind FileId to exact location, kind, format and canonical length.
Legacy tree and inline records also bind a whole-file SHA-256 digest; streamed
records bind a bounded array of complete Chunk locations and an HTTP ETag, and
rely on verified 64-KiB storage frames rather than a whole-file digest. Eligible
metadata stores at most 16 KiB inline; bounded LZ4
compression considers at most 64 KiB original input, and decoding verifies the
canonical length and digest. Location vectors are validated for exact logical
coverage and bounded by the record limit. Hints are non-authoritative and out-of-bounds
hints are ignored. One exact-location key stores the complete file descriptor.
Publication uses one create-only CAS without a preliminary existence GET; equal
content is resolved from the CAS conflict value. Losing candidates cannot
overwrite the selected bytes. Legacy file-ID/mapping pairs remain readable for
recovery. Prefix scans of current records require no file-ID join.
An SDK upload supplies a path and bytes, not the eventual Iceberg data/delete
use. FileIO treats client-supplied bytes as opaque. It verifies the declared
transport checksum and durable frame writes before publishing the descriptor;
format interpretation belongs to the client or to a CROWDB component that
constructs those bytes. Commit validates the selected table metadata and request
requirements in memory; it does not traverse client manifests or perform
per-file reference checks. Format validators remain available to components
that explicitly construct or examine those files.
The isolated native HTTP surface exposes signed immutable object reads/writes,
multipart operations and separately authorized single/batch cleanup, without
a general S3 bucket authority.

### Native injection and direct reads

Native injection finishes chunk writes and its bounded descriptor, publishes
one file record, then returns one canonical URL before constructing manifests.
For parquet, the public filename is
`f/v1/<readable-stem>.<locator>.parquet`. The URL-safe, dot-free locator carries
a unique FileId, length, integrity information and complete stable native
location or chunk-resident directory root. Its versioned signature binds the
full table identity, readable stem and unsigned locator fields; the signature
bytes are excluded from that signing input. Repeated readable stems are harmless;
a full upload retry can return a new filename for equivalent contents. The
client retains the returned URL for its eventual metadata commit. Unreferenced
copies are discovered through file and physical range ownership facts. No
operation record selects or replays an upload response. Locator and stem
together must fit the object-key limit.

Ordinary SDK writers choose logical filenames before S3 PUT and keep those
URLs. A PUT-time allocation does not change a client's chosen path. These
files resolve their one file record; no response field or manifest rewrite
converts them into direct locators. Metadata/Avro files may retain useful
logical prefixes regardless of their native object size. Position-delete and
deletion-vector path references always use the original canonical URL.

Direct HEAD/GET/Range GET validate request permissions and locator authenticity
and read the native descriptor without Iceberg file-location or file-tombstone
KV queries. Chunk-layer layout resolution remains available for replica/EC
routing, cold reads, repair and migration. A KV-resolved opaque file token is not a direct
locator. Large files use bounded chunk-resident directory traversal, not an
unbounded URL. Native chunk routing preserves repair/failover without embedding
transient replica addresses in URLs. Reclaimed identities and reused extents
must never make an old locator return a different file's bytes.

Native trees retain an owner-bound directory root even when they contain one
leaf. Its digest and encoded full file identity distinguish an identical-content
replacement at a reused extent. Direct request preflight checks that root before
HEAD or conditional success, and GET reuses its verified bytes in the pull reader.

Authorization uses bounded validated grant/context caches and signature,
scope and expiry checks. An authorization cache miss may load authority; it
must not become a per-file location/state lookup. Expired entries are not
accepted during refresh failure. Cache entries bind principal, catalog epoch,
full storage identity and operation rights, with validity bounded by the grant
and applicable authority lease. Fresh credential issuance still checks live
authority. Persistent locators have no credential expiry; retain signing-key
verification material for their files' lifetime. A locator is not a bearer
permission. Cold/warm authorization costs and storage resolution counts are
measured separately.

Native catalog authority persists the default locator verification/signing key,
independent of access tokens. Key version 1 is reserved for that catalog key;
renaming and capability activation preserve it. Additional signing versions are
configured through `iceberg.locator_signing`, with an active version and retained
verification keys. Configured version 1 must match the persisted catalog key.
Every serving node uses the same configured versions and retains them until the
corresponding files are physically reclaimed. A lock-free cache selects the
current catalog's key ring; clearing to a new catalog selects its new default key.
Native authority uses record envelope 2 so a legacy server cannot silently ignore
the storage layout. Legacy authorities retain envelope 1 and omitted layout
fields decode as the legacy layout.

Credential refresh URLs carry the table ID and frozen storage namespace ID.
The listener resolves that immutable head directly, so refreshing an old endpoint
after a move or namespace recreation does not require a table-ID reverse index.

Legacy chunk-backed files use bounded leaf blocks and immutable chunk-resident directory
pages, with at most 256 children per page and eight directory levels. Each page
binds its catalog, table and file identity, child heights and covered byte count.
The writer retains only one partial leaf and bounded per-level frontiers. Native
block completion waits for the readable chunk cursor before publishing a root.
Pull readers retain one leaf and its current verified leaf-directory page,
bounded to 32 KiB independently of file length. Directory reuse is reader-local,
bound to the exact immutable root, and never shared across files or generations.
Readers verify directory/leaf digests and read no future block until requested;
full-file reads also verify the canonical digest. Range
parsing accepts one contiguous interval and rejects multiple ranges explicitly.
The HTTP pull-body adapter adds shared response admission and 16-KiB frames.
Only body polling starts a storage read; cancellation drops the in-flight read
before releasing admission. Exact remaining-byte hints track delivery, and storage
errors terminate the body rather than returning a successful truncated stream.
Legacy writer checkpoint decoding remains only for reclaiming existing physical
frontiers. GC verifies owner identity, storage checksum, frontier heights and
complete byte coverage before deleting any child. Foreground writers do not
produce or restore byte checkpoints. Versioned digest state in legacy records is
trusted-storage metadata, never a client authentication assertion.
Legacy native block writes persist an exact physical-range ownership intent in the
catalog before DiskIO. A shared-writer callback receives the assigned location;
an uncertain catalog reply stops the physical batch without a confirmation GET.
Explicit retries reconcile the existing range fact. This ledger also covers
process loss before file publication and legacy checkpoint ranges. Reclamation waits for the chunk readable
cursor or terminal state to settle any unconfirmed physical write.
Staged-tree readers validate physical roots, byte lengths and digests without
assigning a semantic file kind or declaring an incomplete multipart fragment to
be a valid complete-format file. Published-file reads retain record validation.
The legacy tree assembly byte engine consumes complete selected parts in ordinal
order within one request. It preflights the entire selection against the
intersection of request, grant, service and upload file limits before physical
copying. The copier retains bounded read frames and writer frontiers, verifies
each full part digest, and emits no intermediate checkpoints or progress CAS.
One next frame read can overlap the current writer push; either failure cancels
the other future and poisons that request-local copier. A complete-input retry
starts a new copier, while unowned physical ranges remain discoverable by GC.
Multipart session/part models retain independent resource limits and validate
phase coherence: publishing requires complete candidate bytes, published outcomes
require a selected FileId, and abort retains completion evidence without claiming
publication. Their FlatBuffers envelopes bind session and part identities to
separate catalog key scopes, retaining only bounded checkpoint references and
current-part digest state. Unknown phases and invalid revisions fail closed.
New multipart sessions publish their own authority without a catalog-wide
credit reserve or release. Persisted per-session limits bound staged bytes and
part count. Global credit reserve/release/journal drivers have been removed,
including compatibility write hooks. New sessions reject legacy credit input;
existing session and policy formats remain decodable. Recovery never reads or
settles a global policy. A legacy pending reservation without a session is ignored:
session admission preceded any permitted part writes. A pending credit release
does not change the published FileId or part references and is not replayed.
Existing sessions still expire or finish from their own resource facts; obsolete
policy/payload metadata remains available to legacy metadata cleanup without
keeping accounting alive or creating another upload.
The legacy tree multipart repository reserves one part mutation in the session before
changing its part authority. A bounded before/after snapshot and monotonically
increasing revisions make the write and fence release recoverable across servers.
Counts and current staged bytes are reserved once at the session CAS. Abort cannot
bypass an unresolved mutation; stale helpers cannot restore an older part. Abort
retains parts and completion evidence rather than deleting physical storage.
Legacy completion freezes an ordered part-number/revision/digest selection in immutable
payload pages, then changes the session phase by CAS to fence part replacement.
Selections are independently bounded to 10,000 entries and 420,007 encoded bytes.
A client Complete request copies the entire bounded selection in one attempt and
holds the existing upload concurrency permit through assembly and sealing. Its
300-second response-body deadline cancels all pending work. A selection exceeding
the request copy limit fails explicitly before any physical copy. Existing byte
checkpoint sessions reject continuation with an instruction to abort and create
a new upload; their original frontiers remain available to expiry and GC.
Assembled bytes remain unexposed until semantic sealing and immutable location
publication.
Streamed UploadPart writes one part key by CAS without changing the session for
each part. Create caps part count by the reserved staged-byte ceiling divided
by the per-part byte ceiling. Complete freezes an ordered selection with each
part's exact location bytes, length and MD5 ETag in bounded immutable payload
pages. A later replacement cannot change that selection. Completion composes
logical offsets across the selected locations and derives the multipart ETag
from the ordered raw part MD5 values; it does not read part data or assemble a
new chunk. Session phase CAS freezes publication against later selections. The
final file descriptor becomes visible through the immutable location publication
protocol.
A successful completion transition passes its exact installed session to the next
step; a definite CAS conflict passes the returned session observation. The HTTP
driver does not reload the session to confirm successful freeze, publication
preparation or completion. Stream publication reuses this request's selection
after checking its count and digest against the frozen binding, without reading
the immutable selection pages again. An uncertain part or completion mutation
returns its error without a confirmation GET. A client can retry its complete
part transfer or Complete request; the existing frozen selection and publication
facts continue to protect physical ownership and GC.
A recovery page scans at most four session authorities and performs one pending
legacy part settlement, logical expiry or frozen publication per session. It
validates the complete scan page before mutations, rejects foreign continuations,
and never starts or resumes byte assembly. Completing uploads wait for a client
request or expiry. Expiry retains physical parts and legacy frontiers and cannot
bypass an unresolved part mutation or a publication fence.
Publication freezes a caller-validated sealed file record in immutable payload
pages before the publication phase CAS. Recovery replays that exact record through
the immutable file repository and persists the selected FileId. Equal preexisting
bytes retain their original identity. Only a proven incompatible immutable location
permits the terminal Conflicted phase; uncertain writes and context failures do not
become false aborts. Canonical format validation remains the seal caller's contract.
Each native listener schedules this resource sweep independently of namespace
recovery, with one bounded scan and no copy-observation pass. Context changes
reset the cursor. Each session uses the persisted catalog request deadline;
timeout defers only that session so later entries can progress. A separate outer
budget bounds page and context work. Empty and terminal-only sweeps perform two
context checks and the scan, with no policy GET or mutation. There is no global
credit recovery or background byte-copy continuation.


Multipart part listing uses one upload-scoped scan with at most 256 records per
page. Numeric markers preserve gaps and resume strictly after the returned part
number. Current-session checks bracket each scan; concurrent mutations invalidate
the page rather than mixing pending counters with old part records. Expired or
terminal sessions and malformed storage pages are not reported as successful lists.

Native HTTP upload staging holds a bounded concurrency credit. Known small
bodies receive an owner sized to payload plus 34 bytes per 64-KiB storage frame.
A per-object small writer computes declared MD5/SHA-256 synchronously as socket
views arrive, then hands the complete framed owner to the shared pipeline once.
The worker assigns chunk IDs and slices immutable views across strip boundaries;
ordinary mirror writes do not copy payload. The HTTP parser's prefetched-body
copy is measured by one bandwidth counter with count and bytes.
Large and unknown-length bodies receive through bounded 1-MiB owners. Routing
uses the configured fraction of a strip's data capacity, not socket frame size.
Unknown-length bodies choose a dedicated chunk. Per-pipeline disk completion and
readable-cursor publication finish before the next batch; requests accumulating
while publication waits form the next batch. Hash selects a preferred pipeline,
with atomic alternate-route admission before backpressure. Shared chunks default
to 256 MiB with 32-strip groups, asynchronously refilled at half remaining.
Allocation appends after reserved capacity, including hidden strips, and runs
outside the existing chunk publication guard. Exact length, declared checksum and
durable completion precede file publication. Failed uploads leave unpublished
allocation ownership to lower-layer recovery and applicable cleanup tasks.
See the [upload flow](design-crowdb-iceberg-upload-flow.md) for transfer details.

Metadata JSON structural validation uses a bounded pull-reader bridge and an
ignored-value parser rather than retaining the metadata graph. A separate scanner
bounds nesting and verifies raw UTF-8 before parser scratch can grow. Admission
caps blocking workers; cancellation keeps its permit until the worker exits.
The validating full-file reader checks the canonical digest in the same storage
pass for chunked JSON; no independent preliminary full-file read is required.
This structural check does not replace Iceberg schema or commit validation.

Avro writer-schema binary layouts compile to bounded named-reference graphs.
Decoded block validation checks datum widths, UTF-8, collection byte counts,
union/enum indexes and exact record consumption without retaining datum graphs.
Independent graph, recursion and visited-value limits also bound zero-byte values.
The record reader compiles its container's schema once, decodes one bounded block
per pull and permanently stops after failure or cancelled reads. Reader-schema
resolution and Iceberg logical/manifest semantics remain separate checks.

Parquet and Puffin container probes derive footer ranges from canonical framing,
ignoring stored hints even when those hints happen to be in bounds. Their reads
retain one bounded leaf and only fixed-size framing bytes, independent of the
advertised footer size. Puffin probing also checks footer-start magic and reserved
flags. Container framing does not validate footer contents or data semantics.
Puffin metadata parsing separately caps encoded and decoded footer payloads at
1 MiB and bounds blob, field and property collections. It accepts plain JSON or
one sized, checksum-verified LZ4 frame and rejects overlapping blob ranges. Footer
deletion-vector descriptors validate their reserved snapshot/sequence markers,
uncompressed storage, referenced file and cardinality; manifest checks require
exact offset/length and referenced-file/cardinality agreement. The deletion-vector
reader then streams Roaring array, bitset and run containers, validates their
directories and cardinalities, and checks the blob's framing and CRC-32. It retains
one bounded container directory, not the deleted-position set; byte and bitmap
limits independently bound work. Snapshot-wide uniqueness and referenced data-file row-count checks belong to
explicit selected-file validation; normal metadata commit does not run them.
ORC probing retains at most 255 postscript bytes, checks protobuf wire framing
and resolves footer/metadata spans without decoding stripe directories. It accepts
legacy header-only magic and skips bounded unknown protobuf fields.

Avro OCF framing uses a bounded header map and pull-based encoded blocks. Header
bytes, metadata count, block bytes and records per block have independent caps;
negative map blocks must match their declared byte lengths. Sync markers and
canonical block integrity are verified before a block returns. Errors or cancelled
reads poison the cursor rather than resuming at an ambiguous record boundary.
Null and raw-deflate block decoding enforce an independent decoded-byte cap;
truncated compressed data or unused suffixes fail closed. This layer does not
resolve Avro schemas or validate Iceberg manifest fields.

Manifest inheritance is a separate constant-state semantic layer. It distinguishes
the manifest version from the containing table version: v1 sequences default to
zero, while new snapshots can assign row IDs to older manifests. Only added files
inherit missing sequence numbers; explicit file ages are preserved. Unassigned
data files advance the row-ID cursor in manifest order, including existing files
after an upgrade; delete files cannot carry row IDs. Invalid entries and arithmetic
overflow leave the cursor unchanged. Avro decoding and commit admission are not
yet connected to this resolver.

Delegation tokens carry catalog activation epoch, table, principal fingerprint,
nonce, exact operation set, issue/expiry times and independent request/file byte
limits. Domain-separated HMAC authenticates bounded claims and derives per-grant
S3 credential material without a mutable credential registry. Verification requires
the operation's authorized context: writes and credential issuance check Ready
authority, while direct reads may reuse the bounded validated permission cache
described above. Ordinary FileIO grants exclude DELETE;
explicit cleanup grants include the separate delete operation. These token
primitives feed native request-signature verification through a request-local
credential provider. Only the shared SigV4 algorithm is reused; general S3
credentials and metadata are never consulted. Header and presigned requests have
bounded authentication input and reject duplicate authentication fields. Grant
expiry remains exact even when signature timestamps allow clock skew. Table
credential issuance requires a matching Ready catalog authority and rejects
lifetimes above its persisted delegated-access bound, independently of the
signer's configured maximum. A zero persisted delegation bound disables issuance.
Credential-issuance callers freshly authorize the root and exact live table or
client-selected unpublished storage identity; serialization performs no storage reads.
An unpublished identity requires a Ready namespace, current writer authorization,
an exact table-ID and format-version selector, and the matching catalog layout.
Its final head slot must still be absent. No original-owner or draft lookup is
required. Published heads and retired slots cannot be relabeled as unpublished.
Grants last at most fifteen minutes. Published readers receive
read-only grants; only writers receive upload and multipart rights. Responses
configure the native S3 origin and SDK credential-refresh endpoint without embedding
long-lived secrets or changing canonical metadata bytes. Table-load ETags include
the SDK configuration as well as the selected-generation metadata representation;
changed endpoints cannot be hidden by a metadata-only conditional response.
Routed operation checks and streamed
request/response limits already enforce signed scopes and server budgets.
A session token alone never authenticates a request.
The native path-style request parser preserves decoded object-key bytes and limits
operations to immutable object reads/writes, multipart subresources and
explicitly authorized cleanup. Unknown
query operations, duplicate parameters and general buckets fail closed. HTTP
DELETE identifies either an upload abort or separately authorized object cleanup;
batch cleanup uses the S3 DeleteObjects request and per-key response contract.
These request primitives are attached to the native listener.

Writes and reads stream through bounded CROWDB storage clients. Delegated FileIO
access may move immutable ranges without an Access Server payload bounce, but
cannot overwrite published files or bypass its operation-specific authorization.

## 4. Commit and lifecycle

A composite table read checks CatalogContext at entry and before returning.
Inside that request it reuses context-bound namespace, table and file resolution
without repeating root reads. Admission resolves complete namespace ancestry:
namespace identities cannot move, and strict nonempty deletion protects every
ancestor while its child remains live. The final checks revalidate the selected
head generation, leaf mapping, authority identity/name epoch and lifecycle.
Property updates do not invalidate this binding; Dropping remains visible until
Tombstone. Public standalone repository methods retain their own context guards.
There is no cross-request permission cache or reader registration. The same
context permits capability additions only; catalog replacement advances its epoch.
Capability revocation or authority-only retirement would require revisiting this
request-local admission contract.

A table commit validates requirements against one selected table generation,
constructs a complete new metadata state, writes immutable candidate files, and
atomically publishes one new table head. Concurrent commits either publish from
the generation they validated or fail for the client to reconcile; they never
merge implicitly.

Ordinary commits prepare immutable metadata and publish with one TableHead CAS.
The Catalog config advertises `crowdb.guarded-maintenance=true` when table writes
are installed. Console maintenance uses the optional CrowDB extension
`x-crowdb-base-metadata-location` on an existing-table commit to bind the complete
reviewed metadata generation. A changed base conflicts; guarded `remove-snapshots`
also rejects every current or named-reference target on that exact base. The head
CAS prevents a concurrent Branch or Tag from being silently removed. This guard
does not change ordinary Iceberg update semantics and cannot accompany `assert-create`.
An exact successful request replay returns its published metadata. Prepared metadata
may still publish while its base remains current; when a newer generation conceals
that operation's metadata file, the result stays uncertain rather than being rebased.

Existing-table commits do not write a phase journal, per-file committed state or post-publication
head settlement. No server instance is a table leader or lock owner. A current
head carries the operation-named metadata pointer and an optional 32-byte digest
binding principal, route and request bytes. A matching visible retry returns that
metadata; changed input conflicts and old heads without this binding remain
uncertain. After a later generation hides that result,
an old request may return uncertain rather than silently reapplying its update.

Table creation retains its complete input, evaluated metadata and response only
inside the request. It writes the final metadata `FileRecord`, reserves the exact
namespace/name, checks that the same parent is Ready, creates the final table head
with an expected-absent CAS, and conditionally publishes the reserved name. This
order is required: a reservation precedes the last parent check, so namespace
deletion either sees it or prevents that check from succeeding. The head contains
no pending request identity. Cancellation discards request-local work; complete
client retries may create new unowned resources and never replay a stored response.
Response headroom is checked before any resource publication.

A `RetiredTable` record occupies the same storage head key as a published table.
It binds the immutable catalog, TableId and optional storage namespace: 32 raw ID
bytes in the legacy layout, 48 in the native layout, plus a bounded envelope. It
contains no request, owner, schema, input, response, phase or continuation. It is a
permanent terminal resource fact and never returns to an absent or Ready slot.
Name resolution treats it as missing; credential issuance, initial publication
and physical write admission reject it. GC never interprets it as a live head.

When namespace deletion or reservation cleanup encounters a reserved table whose
head is absent, it creates this retirement record by CAS before removing the
exact reservation. A conflict supplies the current head or retirement value;
cleanup uses that value without a confirmation GET. If a Ready head won, cleanup
can publish its matching reserved name and strict nonempty namespace deletion
fails. If retirement won, the delayed initial-head CAS fails permanently. Removing
only the reservation is unsafe: a delayed expected-absent head creation could
otherwise expose an object after the namespace's empty proof. Exact name bytes and
an input-bound reservation identity prevent delayed cleanup from removing a later table.
An uncertain CAS reply stops that request; a later full retry reconciles final
resource facts.

For inline metadata, a successful immediate create performs four conditional
resource writes: metadata authority, name reservation, initial head and published
name. It creates no operation, input-page or response-page keys. Chunked metadata
adds only the existing per-range physical write intents. Ordinary creation does
not modify the parent namespace authority. Retiring an absent head adds one CAS
only on cancellation or GC; its retained metadata has fixed size independent of
table schema or request size.

New-table requests may carry unique provisional field IDs starting at zero,
as produced by Spark. Creation assigns positive durable IDs before validating
the canonical schema and remaps identifier, partition, sort and nested-default
references consistently. Negative or duplicate provisional IDs remain invalid.
Existing-table commits and stored metadata still require their durable IDs.

Staging evaluates metadata and returns its complete initialization material,
including a fresh UUID and storage identity, without writing any server record.
A repeated stage request produces independent material. The client retains the
chosen response and sends complete standard assert-create updates when ready.
The final request initializes an empty metadata builder with the supplied UUID,
location, schema, partitioning, sorting and properties, preserving field IDs
already used by client-prepared files. It evaluates initial metadata and
requirements, then follows the same resource publication order as immediate
creation without scanning staged data files or client manifest chains.

An assert-create name reservation uses a domain-separated, length-delimited
digest of catalog activation, namespace identity, TableId, name, principal and
complete request bytes. It excludes request IDs and evaluation clocks, allowing
identical complete retries to use the exact existing reservation. Ordinary create
uses a fresh reservation identity. Competing input cannot adopt another reserved
resource. If the initial head already won, a retry only settles its matching name
and returns a conflict; clients load the published table to learn the result.
No response replay or server-owned draft is required. Unknown storage replies
stop the current request without a confirmation read.

Bounded background scans rotate creation, legacy update and lifecycle journals,
four records per page. Independent continuations reset on catalog activation
changes. Legacy unbound drafts retain their existing expiry. Creation cleanup
never writes missing metadata or publishes a first head: it occupies an absent
head slot with permanent retirement, settles exact name/head markers from final
resource facts, and clears only the captured parent marker revision. After those
facts are durable, it conditionally deletes the old creation record. Unknown
writes stop cleanup. Physical allocation discovery remains in write intents.
Retired-catalog GC permanently retires the exact legacy head slot before releasing
that record's protection, whether the first head won or remained absent. It waits
for the catalog's existing retry and request grace; exact-head CAS conflicts stop
that step unless the returned value is already retired. Final file authorities
and per-range write intents remain independent discovery sources for physical
reclamation. An old nonterminal phase cannot retain the catalog indefinitely.
Legacy update and lifecycle recovery retain their own bounded compatibility paths.
New creators leave only resource facts for reservation cleanup and GC.

Logical drop resolves the currently named table and conditionally tombstones its
exact head, then conditionally removes that table's name mapping. It writes no
request, input, phase or response journal. A conflicting in-flight head update
returns conflict without rebasing. A new DELETE retry resolves the name again
and may delete a table recreated at that name. Background cleanup of an older
table remains bound to its captured table ID and cannot remove the replacement.

Same/cross-namespace rename stores no new input, phase, response or lifecycle
journal. Request admission validates the route and principal without reading an
old request journal or encoding a replay payload. Each affected parent holds one
bounded `NamespaceRename` occupancy containing the source mapping, target name, expected head generation/fence and a
fresh per-attempt reservation identity. Parents are occupied in stable ID order;
failure to acquire the second cancels and releases the first, without waiting on
another rename. Definite parent CAS conflicts use the observed authority directly,
preserving concurrent property changes. Ordinary readers do not acquire occupancy.

The target name is reserved before one exact-head CAS switches the canonical
identifier and name epoch. Table identity, UUID, metadata generation, digest and
file location stay fixed. The destination is published before the captured source
mapping and parent occupancies are conditionally cleared. A namespace cannot enter
Dropping while occupied, including a same-parent move where a scan could otherwise
miss both names across the switch. Mapping visibility remains qualified by the
head, and the old name never becomes an alias.

Recovery reads the head and name facts: a switched head completes the destination;
an unchanged head is first conditionally fenced to invalidate delayed publication,
then the reservation and occupancy can be removed. Conflicts use their returned
head. Namespace mapping sweeps and name-reservation/deletion helpers recover
abandoned occupancy. Every new attempt has a distinct reservation identity, even
when an HTTP request key is reused, preventing delayed cleanup of a canceled
attempt from deleting a later reservation. Cleanup never removes a recreated name
with a different identity. Existing lifecycle journals still finish through their
legacy recovery driver.

Rename replies are generated directly from current resource state. After a
completed move the source is missing; repeating that request can return 404.
A recreated source with an occupied target conflicts. There is no historical
response replay or implicit rebasing of an in-flight head CAS.

Drop tombstones the selected head without traversing snapshots or deleting files.
A purge request persists a `TablePurgeTask` containing the candidate tombstoned
head and activation epoch before publishing the tombstone, indexed by table,
generation and metadata file. Its initial marker time is zero. GC admits it only
after observing the exact tombstone and fixes a twenty-minute delay from that
observation; an existing admitted task never renews its deadline. Older markers
keep their stored times. If the deletion did not win, GC first fences the exact
Ready predecessor before removing the marker, blocking a delayed tombstone CAS.
Unknown writes stop immediately. Deferred or canceled markers do not keep an
otherwise idle scanner spinning. A drop without an explicit
purge request leaves the underlying files in place. Purge reclaims table-scoped
files after the delay and owner checks, without snapshot reachability scans
inside the dropped table. The REST
drop/rename routes require independent writer credentials and return empty success
responses; stale table names fail normal load, exists, commit and credential refresh.

Drop and replacement remove logical authority first. Purge and retired-catalog
cleanup reclaim only their captured inactive identities after retention and
owner checks. Snapshot expiration alone does not delete published files.
Explicit single/batch cleanup scans retained references under the caller's
no-future-reference contract described below.

The reclamation proof binds current and retained historical metadata to their
captured heads. Its immutable traversal stack and compressed binary file-ID index
use content-addressed payload pages. A task CAS publishes the pending stack and
mark root together; a missing page is an error, including during a nonmembership
query. Physical deletion runs only for a tombstoned table or retired catalog;
the worker rechecks inactive authority before sweeping. A Ready table may retain
unreachable files until drop or clear rather than interrupt reads or commits.
Unfinished table operations and active multipart work defer table purge.
Completed operation retry records and terminal multipart sessions do not extend
the 20-minute purge delay. Background task advancement is enabled by default;
it uses a separate storage client pool, one-step concurrency admission, bounded
KV and chunk request/byte budgets, and durable retry state. The
chunk byte budget covers payload reads and writes. Range retirement sends a
bounded control descriptor and consumes one chunk-operation request without
charging the referenced range as transferred payload. Large ranges therefore
remain eligible for notification under the same limits. These are logical
operation and payload budgets, not exact routed-RPC or wire-byte accounting.
enabled scheduler admits persisted table purge markers and completed catalog clears;
management may also start inactive tasks. Operators can disable the scheduler
or adjust validated resource limits. Discovery starts at startup. Each catalog
with unfinished discovery or runnable work continues after the retry-base delay
(one second by default, at least 100 ms and at most the configured scan interval).
One window still advances at most one worker step under its original I/O and time
budgets. A completed discovery round returns to the scan interval, 30 minutes by
default. A purge becomes eligible 20 minutes after the drop; discovery may add up
to one idle scan interval before its first step. A known task retry deadline can
wake its catalog sooner, without shortening retention or other resource deadlines.

Scheduler discovery positions are discardable, per-catalog memory. Purge,
multipart admission, retired-catalog admission and task lanes rotate their first
turn before awaiting storage. Due catalogs rotate independently; one catalog's
failure does not delay another catalog's due work. A failed lane reports its
original error and leaves other lanes eligible. A validated raw row advances its local position before key
or value decoding and before any admission or worker action. An error, deadline
or cancellation therefore retains that observed position. An empty page closes
that lane's pass; actual task progress reopens its source lane when more work is
runnable. Terminal, paused and future-retry tasks do not keep a round busy.
Failed or cancelled lanes are rescanned with capped exponential backoff until a
complete error-free pass, so a long-lived corrupt row cannot cause a hot loop.
Multipart admission runs once per discovery round. Local scheduling retains at
most the 64 configured catalogs and the current catalog; retired identities remain
discoverable through durable system markers. Restart loses these hints and rescans the unchanged
durable markers, ledgers and tasks. These hints grant no deletion authority and
do not replace task proof, permanent retirement fences or retry state.

Active multipart and deleted-file cleanup skips candidates whose fixed retention
deadline has not arrived, so later eligible files can advance in the same pass.
Those candidates and their deadlines remain unchanged. Complete denotes the end
of that eligibility pass; periodic admission reopens it to discover newly eligible
resources. Eligibility may therefore wait for the next scan interval and bounded
drain time. Reopened passes retain a monotonically increasing sweep round, so a
completed candidate contributes to deletion totals once. Inactive table purge
and retired-catalog cleanup retain their task-wide retention and fence rules.

The dedicated read-only local scan retains validated catalog/scope/prefix bounds,
strict ordering, positive revisions and item/byte limits while allowing an
undecodable raw key as its continuation. It uses the native exclusive continuation
and an actual routed catalog generation, including for keys longer than the
Iceberg record format permits. Generic scans and persisted GC task continuations
keep their existing format validation. This path still uses the legacy scan
transport: its byte limit is checked after materialization. A transport failure
or an oversized response without an observed key cannot advance this position;
it remains an explicit error, with no claim of a wire memory bound or universal
poison-row recovery.

Provisioned disk capacity is the allocation boundary for both foreground files
and GC durable workspace. A failed GC workspace write retains the last durable
continuation and defers retry; it never substitutes an incomplete proof or
authorizes deletion. Committed files remain readable when new chunk allocation
fails. Progress resumes after capacity is restored through the normal storage
flow. Shared-chunk ranges remain pending while range deletion is unsupported.

Publication and logical-URL resolution check catalog and table or draft
authority without request-level pins. Direct reads instead use the validated
permission cache and authenticated locator. Physical reclamation observes the
configured retention interval. Logical deletion hides the file from listing;
its tombstone rejects logical-URL resolution but is not consulted by direct
HEAD/GET/Range GET. With a valid grant, an already known direct URL can still
read retained bytes until physical reclamation. Logical deletion is not an
instantaneous direct-URL revocation guarantee. Legacy live tasks are retired without
further deletion, releasing an owned table fence. Retained and deferred
candidates remain durable work for later inactive passes.

An ordinary inline file in a table purge has no physical deletion frontier.
After checking retention, head generation, purge fence and immutable claim, one
bounded worker step conditionally deletes its exact file authority and persists
the completed candidate and task count. It does not persist an intermediate
Deleting checkpoint. Part, assembly and non-inline candidates retain their
destructive traversal checkpoints. An unknown mutation response stops the step;
a subsequent worker resumes from the durable file, candidate and task facts.

An already authorized FileIO GET or HEAD can continue within the configured
retention interval after logical deletion. Native storage arbitrates direct
read/reclamation races, returning the original bytes or a storage error, never
bytes from a reused allocation. Logical-URL admission rejects a reclaiming
record. Uploads and new credentials require a Ready table. After logical drop,
direct URLs remain readable under still-valid grants while bytes are retained;
physical purge and grant expiry bound that access.

Retired catalog recovery scans system retry and management ledgers before file
deletion and after the final file rescan. Pending or retained bindings stop the
pass; exact-identity overflow entries remain independent of occupied primary
slots. After files are reclaimed, the worker conditionally removes expired
bindings, audits, projections and non-GC catalog records while preserving the
active root's management operation. Multipart parts have their own durable tree
candidates. Assembly checkpoints have separate claims and a durable frontier-root
index. Abandoned frontiers are authenticated before traversal; a conflicted final
tree is traversed once instead of revisiting its shared frontier. Published
sessions reclaim only the checkpoint block, preserving the assembled data tree.
Unpublished server-generated initial metadata is identified by Metadata/JSON
resource type and the exact generation-one metadata filename bound to its FileId.
Its storage table must match the absent or retired final head slot; metadata for
another table cannot select that resource as its canonical head metadata.
The active resource sweep may discover this candidate when its final head is
absent or retired. Its first observation and existing safety retention remain
persisted in the GC candidate; restart never restarts or shortens that interval.
Immediately before reclaiming it, GC creates the same final-head retirement fact
if still absent. A Ready head returned by the CAS conflict preserves the file;
retirement winning prevents all delayed initial publishers. Only then may the
existing exact-range deletion cursor notify ChunkDB. Acceptance of DeleteRange is
the access server's completion boundary; it does not poll physical release.

This proof does not authorize deleting data, manifests or inherited references
merely because their owner head is absent. Their existing final-resource reference
proof remains required. Physical ranges written before a final metadata authority
still retain their BlockIntent discovery facts. Both catalog cleanup layouts keep
retirement records permanently and allow those terminal facts in their final empty
proof; removing them would reopen a delayed expected-absent creation slot.

Retired-catalog cleanup leaves a permanent retirement value at every discovered
table head key. A scanned head uses an exact-value CAS with the scan bytes.
Before removing a name mapping, cleanup reads its complete storage head key once
and retires the absent slot or observed head. This is independent of creation
journals, including a reserved name whose first head never won. One concurrent
slot change can be consumed directly from the CAS observation; further changes
leave the durable cleanup cursor unchanged. Unknown replies stop the step. A
restarted worker recognizes the retirement value before removing the exact
mapping. File authorities, range intents and GC candidates remain the physical
discovery chain. Table purge keeps its tombstone head; this cleanup cannot retire
a table in the current active catalog.

An active catalog has a separate resource cleanup task for expired terminal parts,
assembly checkpoints, explicit deleted-file records and unpublished initial
metadata. It never infers missing snapshot references for other published files
or fences a Ready table. Candidate progress is
durable across restarts. After every part and checkpoint is reclaimed, it removes
the terminal session's payload pages and session record; pending part settlement
or unreleased multipart credit keeps that session available for recovery.
Streamed file and part candidates instead reclaim their exact Chunk location
ranges after retention. A published selected part remains owned by the immutable
file descriptor; part cleanup does not reclaim it a second time. Legacy tree allocations
abandoned before publication retain FileWriteIntent ownership. Native streamed
writes rely on Chunk-KV WAL and chunk allocation lifecycle rather than a
per-frame Iceberg intent record. Active-catalog cleanup does not reclaim an intent merely because a
file descriptor is absent: a multipart session in `Publishing` may still own
those blocks. Retired-catalog and table-purge passes can reclaim intents after
their authority and owner checks. The disk leak scanner alone does not establish
Iceberg ownership.
Each physical step rechecks the terminal session and retention. The checkpoint
block is deleted after its children, and session cleanup requires its completed
claim. Block intents are swept after tree candidates, preserving reachable owners
and any unfinished file or assembly cursor. An owner fence prevents publication
or new block writes once orphan deletion begins. Superseded intents of a reachable
owner are conservatively retained until that owner becomes unreachable.

Final catalog cleanup verifies that all candidates are complete and that no owner
is paused or quarantined. A durable retirement marker selects the cleanup owner
and rejects stale GC mutations before bounded deletion of claims, candidates,
proof pages, write fences and old tasks. The retired authority, permanent table
retirement values, winning task result and retirement marker remain. Late
unfinished records stop cleanup.
Uncertain progress responses are resolved by reading durable state; an unfenced
live proof whose authority changed terminates without deleting files.

## 5. Compatibility

CROWDB covers the core Iceberg format semantics for v1, v2, and v3, including
reading, creating, writing, and the defined version upgrades. Mandatory behavior
for the selected format version is not weakened. Optional features are exposed
only when their complete semantics are enabled.

REST wire types, Iceberg domain state, and CROWDB storage records remain
separate. Unknown or disabled requirements and updates fail before mutation.
The backed-up specifications decide behavior when implementations differ.

### Executable conformance profile

The official Java oracle is Apache Iceberg 1.11.0; the official Rust REST
client is 0.10.0. The declared selected-file profile uses Parquet data/deletes,
Avro manifests and Puffin deletion vectors/statistics. ORC bytes can be stored,
but selected ORC validation and compute-engine certification are separate work.

- **v1/v2/v3 metadata and upgrades:** `official_java_metadata_roundtrips_without_rewriting`,
  `official_catalog_creates_commits_upgrades_stages_and_refreshes_native_credentials`,
  and the official create/update/snapshot fixtures compare canonical metadata.
  `TestIcebergVersionRows` reads actual rows before and after adjacent upgrades,
  reads historical snapshots, expires them logically, and reloads after restart.
- **Selected data and deletes:** client-generated fixtures exercise original
  rows, equality-delete visibility and historical snapshots. Commit treats client
  manifests as opaque references. `TestIcebergSelectedFiles` publishes mismatched
  data/delete declarations in isolated tables and checks the selected head and
  snapshot inventory; its valid table still verifies actual rows, equality deletes
  and historical reads. Selected-use rejection probes are explicit validator
  tests rather than a normal publication gate. `TestIcebergFileOperations` checks
  that upload credentials lack cleanup permission (403 `AccessDenied`) and that
  rejection preserves the immutable file. Canonical file
  validators separately cover position deletes, v3 lineage, deletion vectors,
  defaults, nested/variant types, integer encodings and nullable values.
- **Statistics:** `TestIcebergCatalogWrites` and the official partition-statistics
  fixtures cover publication, replay, evolution and staged creation.
  `TestIcebergCommitErrors` verifies supported partition-statistics metadata
  publication, exact inventory reload and removal; a foreign file location
  rejects the whole update batch and preserves the selected head. Metadata
  acceptance is separate from explicit Parquet content validation. Historical
  omissions follow the explicit compatibility rules described above.
- **Discovery and authorization:**
  `discovery_uses_installed_routes_and_unsupported_paths_leave_no_record` and
  `explicit_partial_activation_limits_discovery_and_table_admission` exercise
  installed/disabled route and version combinations. HTTP namespace, table,
  lifecycle, credentials and body tests cover roles, malformed requests, limits,
  cancellation, unchanged authority on rejection and bounded metrics.
- **Faults and retirement:** `official_rust_client_observes_lost_create_reply_on_another_listener`,
  `official_rust_client_lost_reply_survives_native_storage_restart`, and the
  Java response-loss/retired-catalog fixtures exercise listener changes and
  restart. The SDKs do not automatically replay a lost mutation POST with the
  same key; direct HTTP fault tests prove the server's same-key replay contract.
- **Apache REST Compatibility Kit:** the unmodified 1.11.0 runner verifies six
  supported cases: namespace create, basic table create, rename, drop,
  missing-table drop and table list. This is not a full-kit pass. Other cases
  assume register/views or local filesystem locations that the native authority
  deliberately rejects. Custom SDK fixtures are not described as kit results.
- **Large logical files:** `tib_address_space_range_reads_keep_fixed_windows_and_small_authority`
  and `tib_reclamation_progress_serializes_a_bounded_resumable_cursor` use a
  1 TiB logical tree with repeated immutable blocks. They prove bounded windows
  and resumable state, not physical TiB capacity. Oversized declared metadata
  is rejected before I/O at the configured metadata budget.

## 6. Relationship to other access models

General S3 and Iceberg share chunk, Chunk-KV, transport, credential, and safe
utility mechanisms, but not semantic authority. S3 bucket records, overwrite,
delete, listing, and lifecycle behavior cannot publish or alter an Iceberg
table.

Dataset may consume data described by an Iceberg table through an explicit,
generation-bound reference. That does not transfer snapshot, commit, file, or
reclamation authority to Dataset.

The native file endpoint exposes separately authorized S3 `DeleteObject` and
`DeleteObjects` cleanup. A cleanup credential is requested through the table
credentials endpoint with `cleanup=true`; ordinary FileIO credentials cannot
delete objects. The server scans retained table metadata and refuses a path
that is referenced or whose reference status is uncertain. Successful deletion
records a logical tombstone and removes list visibility. The background worker
discovers that final tombstone and durably claims its exact old file before
physical reclamation after 20 minutes. DELETE writes no GC task or candidate;
restart discovers the same DeletedFile. Direct URLs remain readable until their
bytes are reclaimed, subject to grant validity. Their FileIds/locators are never
reused. A new logical-path PUT can immediately reuse its path: if the worker has
not queued the old file, PUT first retains its exact resource in the existing
GC claim and candidate records, then conditionally replaces the observed
DeletedFile. An uncertain write stops without a confirmation read. Either the
deletion marker or the independent GC candidate retains the old reclamation
obligation across failure. Batch deletion returns individual errors inside
HTTP 200, so callers must inspect every item.

> **Warning:** Before deleting, the client must stop and resolve every writer
> and retry that might still commit the path, including unpublished metadata.
> The server cannot see a future commit during its reference scan. If a client
> later commits a deleted path, the snapshot can reference missing data. Do not
> use cleanup credentials in ordinary FileIO. A reader retaining old metadata
> must finish before the 20-minute physical reclamation deadline.

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
- **ICE-I9 — Live file retention:** a Ready table's published file is not
  reclaimed solely because no retained snapshot refers to it; active cleanup
  handles expired multipart remnants and explicitly deleted files with durable
  owner checks.
- **ICE-I10 — Stable identity directory:** rename and cross-namespace move
  preserve storage namespace, table path and every published file URL.
- **ICE-I11 — Single file authority:** one filename-keyed record selects each
  published file; list and lifecycle use that record without secondary file indexes.
- **ICE-I12 — Direct native read:** a direct URL supplies its immutable native
  descriptor without Iceberg file-location or file-state KV queries; native
  chunk layout resolution and repair retain their normal behavior.
  Logical deletion removes list visibility; physical GC ends byte availability.
- **ICE-I13 — Bounded metadata scale:** large metadata bytes live in native
  objects, descriptors/head remain bounded, prefix listing reads bounded KV
  pages, and optional projections cannot replace standard metadata authority.
