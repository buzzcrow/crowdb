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

Generation-local metadata projections preserve raw top-level JSON children in
bounded pages. REFS loads construct them only after canonical metadata passes
the complete bounded parser. A versioned validation receipt binds the selected
head, exact parser limits and projection root; a generic JSON projection alone
cannot stand in for metadata validation. REFS loads may reuse this validated
representation without decoding the complete object graph. They still read and
verify canonical storage and recheck the namespace and head before responding.
Missing, partial, corrupt, unknown-version or differently bounded projections
fall back to the canonical parser. ALL responses preserve exact canonical bytes.
Projection construction is optional and never participates in commit proofs or
head publication. No cross-generation cache or deduplication is implied.

Iceberg metadata stores bounded logical records and opaque data references.
Physical chunk placement and storage topology remain below the access boundary.

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

Management, audit and shared REST retry ledgers each use 4096 deterministic
fast-hash slots with exact-identity overflow keys. An occupied slot does not
reject a different identity: it routes that identity to its own durable key.
Neither slot nor overflow records are evicted inside their retention window;
overflow storage is subject to normal disk capacity and physical reclamation.
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
Table-child probes resolve published mappings against the selected table head.
Unpublished table reservations are helped through their creation or lifecycle
journal. Ordinary table creation does not modify the namespace authority. A
concurrent drop scan may miss a creator; later mapping reconciliation resolves
the leftover state. Stable namespace identity prevents that state from becoming
visible under a recreated namespace. Rename retains its lifecycle admission
protocol. Corrupt table authority blocks the emptiness proof.
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
Typed locations use lower-case unpadded base32 catalog IDs and lower-case hex
table IDs. Relative UTF-8 object keys preserve case, literal percent signs, plus
signs and repeated internal slashes; the whole object key is bounded to 1,024
bytes. Dot traversal, leading slash, backslash, controls, query and fragment
delimiters are rejected rather than normalized. HTTP percent decoding belongs
only at the transport boundary, not in stored S3-shaped locations.

Native `ListObjectsV2` retains this address model. A list request must specify
an explicit `t/<table-id>/` prefix and a table-bound delegated list capability;
catalog-wide discovery and general S3 credentials are rejected. Reader and
writer FileIO credentials include listing; cleanup-only credentials do not.
Exact-object HEAD/GET/PUT remain independently usable. PyArrow missing-file
probes may issue listing to distinguish a missing object from a directory.

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
Writer checkpoints flush partial leaves and store the bounded directory frontier
plus resumable digest state in a chunk; durable journals need retain only one root.
Restoration checks owner identity, checksum, frontier heights and total byte
coverage. SHA-256 compression uses RustCrypto; versioned digest checkpoints retain
only chaining state, byte length and a partial block. They are trusted-storage
recovery records, not client authentication assertions. Failed checkpoint writes
poison the current writer without invalidating earlier durable checkpoints.
Legacy native block writes persist an exact physical-range ownership intent in the
catalog before DiskIO. A shared-writer callback receives the assigned location;
uncertain catalog writes are read back before the physical batch proceeds.
This ledger also covers process loss before file publication and checkpoints
superseded by later assembly progress. Reclamation waits for the chunk readable
cursor or terminal state to settle any unconfirmed physical write.
Staged-tree readers validate physical roots, byte lengths and digests without
assigning a semantic file kind or declaring an incomplete multipart fragment to
be a valid complete-format file. Published-file reads retain record validation.
The assembly byte engine consumes a previously frozen part selection in ordinal
order. Each step copies at most one bounded window, persists target-writer progress
and checkpoints the current part digest. This verifies complete part digests even
when recovery spans many windows. Lost replies can repeat old progress without
duplicating bytes in the selected output; losing physical writes remain retained.
The engine requires a durable selection/progress journal and does not itself
authorize multipart operations or publish file locations.
Multipart session/part models retain independent resource limits and validate
phase coherence: publishing requires complete candidate bytes, published outcomes
require a selected FileId, and abort retains completion evidence without claiming
publication. Their FlatBuffers envelopes bind session and part identities to
separate catalog key scopes, retaining only bounded checkpoint references and
current-part digest state. Unknown phases and invalid revisions fail closed.
New multipart sessions publish their own authority without a catalog-wide
credit reserve or release. Persisted per-session limits bound staged bytes and
part count. Legacy admission journals and credit receipts remain recoverable in
the background; ordinary new sessions never contend on those shared records.
The legacy tree multipart repository reserves one part mutation in the session before
changing its part authority. A bounded before/after snapshot and monotonically
increasing revisions make the write and fence release recoverable across servers.
Counts and current staged bytes are reserved once at the session CAS. Abort cannot
bypass an unresolved mutation; stale helpers cannot restore an older part. Abort
retains parts and completion evidence rather than deleting physical storage.
Legacy completion freezes an ordered part-number/revision/digest selection in immutable
payload pages, then changes the session phase by CAS to fence part replacement.
Selections are independently bounded to 10,000 entries and 420,007 encoded bytes.
Each completion step verifies that bounded selection and one selected part before
copying a bounded byte window and publishing its checkpoint by session CAS. Lost
replies reload progress without appending selected bytes twice. Assembled bytes
remain unexposed until semantic sealing and immutable location publication.
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
Foreground and recovery drivers use the same native-block-aligned byte window
below the one-MiB assembly ceiling. Equal windows prevent systematic CAS losses
to a smaller competing recovery step; alignment avoids checkpoint-only tiny leaves.
Within a step, one next 16-KiB frame read may overlap the current writer push.
There are no detached copy tasks or unbounded queues. Either IO failure cancels
the other future and returns no new checkpoint; prior durable progress stays valid.
A recovery page scans at most four session authorities and performs one pending
part settlement, logical expiry or assembly byte window per session. It validates
the complete scan page before session mutations, rejects foreign continuations and reports
finished assembly as awaiting semantic sealing. Expiry never deletes physical
parts and cannot bypass an unresolved part mutation or a publication fence.
Publication freezes a caller-validated sealed file record in immutable payload
pages before the publication phase CAS. Recovery replays that exact record through
the immutable file repository and persists the selected FileId. Equal preexisting
bytes retain their original identity. Only a proven incompatible immutable location
permits the terminal Conflicted phase; uncertain writes and context failures do not
become false aborts. Canonical format validation remains the seal caller's contract.
Each native listener schedules the multipart sweep independently of namespace
recovery. It observes at most four session revisions, then rechecks the same page
on the next tick. Byte-copy recovery defers revisions that advanced meanwhile;
unchanged revisions remain eligible. This bounded observation is only scheduling
advice, not a lock or lease: expiry, journal settlement, publication and all
context/session CAS checks remain authoritative. An observation does not retain
file bytes or survive a restart. It resets its cursor when the active context changes and bounds each
session by the persisted catalog request deadline. Timeout defers only that session,
allowing later entries in the page to progress. A separate outer budget bounds the
whole page and context/scan work. One separately bounded admission-journal recovery
step runs before scanning, including a reservation whose session is not yet present.
Legacy terminal sessions settle retained credits on a later recovery visit;
new sessions have no global credit release.
The HTTP driver composes this durable state machine with physical sealing;
recovery remains the authority for abandoned or uncertain work.

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
a freshly checked Ready context. Ordinary FileIO grants exclude DELETE;
explicit cleanup grants include the separate delete operation. These token
primitives feed native request-signature verification through a request-local
credential provider. Only the shared SigV4 algorithm is reused; general S3
credentials and metadata are never consulted. Header and presigned requests have
bounded authentication input and reject duplicate authentication fields. Grant
expiry remains exact even when signature timestamps allow clock skew. Table
credential issuance requires a matching Ready catalog authority and rejects
lifetimes above its persisted delegated-access bound, independently of the
signer's configured maximum. A zero persisted delegation bound disables issuance.
Callers still must freshly authorize the root and exact live table or draft;
the serialization primitive does not perform those reads. The credential endpoint
checks the current namespace and published table or exact unbound draft. Draft
vending requires its original writer principal and a table-ID query selector;
same-name drafts cannot authorize each other. Expired drafts cannot refresh. Grants
last at most fifteen minutes and never outlive a draft. Published readers receive
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

A table commit validates requirements against one selected table generation,
constructs a complete new metadata state, writes immutable candidate files, and
atomically publishes one new table head. Concurrent commits either publish from
the generation they validated or fail for the client to reconcile; they never
merge implicitly.

Ordinary commits prepare immutable metadata and publish with one TableHead CAS.
They do not write a phase journal, per-file committed state or post-publication
head settlement. No server instance is a table leader or lock owner. A current
head carries the operation-named metadata pointer and an optional 32-byte digest
binding principal, route and request bytes. A matching visible retry returns that
metadata; changed input conflicts and old heads without this binding remain
uncertain. After a later generation hides that result,
an old request may return uncertain rather than silently reapplying its update.

The library's immediate table creator records its immutable input, candidate
identity, canonical metadata and response before reserving the namespace/name.
It writes initial metadata before selecting the initial head and replacing the
name reservation with a published mapping. Creation does not acquire a parent
admission marker or modify the namespace authority. The durable terminal result
precedes conditional cleanup of table markers. REST write admission
binds the principal, route and exact body in the shared retry ledger before invoking
these operations. Recovery reloads an existing operation before resolving the name
or current head and never rebases an uncertain request. Response headroom is checked
before publication so credential configuration fits the durable replay budget.

New-table requests may carry unique provisional field IDs starting at zero,
as produced by Spark. Creation assigns positive durable IDs before validating
the canonical schema and remaps identifier, partition, sort and nested-default
references consistently. Negative or duplicate provisional IDs remain invalid.
Existing-table commits and stored metadata still require their durable IDs.

Staged creation retains an invisible durable draft and metadata-only response.
Its native table location resolves the draft without a client-specific token.
The final assert-create request initializes an empty metadata builder using the
retained UUID, preserving the field IDs already used by staged files. One journal
CAS binds its request identity, input bytes, evaluation clock and candidate before
the ordinary name-reservation sequence begins. Publication evaluates initial
table metadata and requirements, then writes canonical metadata; it does not
scan staged data files or client manifest chains.

Draft expiry and final binding compete on the same phase CAS. Only an unbound
draft may expire; bound operations recover their original publication outcome
regardless of elapsed time. Known semantic file failures retain a terminal client
error and release their reservation. Uncertain storage outcomes remain recoverable.
The draft response and final commit response are retained separately for exact
replay.

Bounded background scans rotate creation, legacy update and lifecycle journals,
four records per page, with independent continuations reset on catalog activation changes. Recovery
expires only unbound drafts, reconstructs fixed candidate proofs, settles published
markers and retains uncertain storage errors. Known semantic validation failures
become durable client outcomes before any candidate is published. Recovery deadlines
preserve journal evidence rather than canceling the logical operation.

Logical drop and same/cross-namespace rename use a bounded `TableLifecycleOperation`
journal. It fixes the original head and exact source mapping, request identity,
principal, input and candidate before publication. A single head CAS arbitrates
against metadata commits and other lifecycle operations. A losing operation keeps
its terminal conflict instead of rebasing onto a new generation or recreated name.
Rename changes the canonical identifier and name epoch, not table identity, UUID,
metadata generation, digest or file location. Destination reservation precedes a
namespace admission CAS. The admission marker remains until the head outcome and
destination mapping are durable; namespace-drop helpers finish or abort that exact
operation with a shared bounded work budget. The source stays head-qualified until
the move publishes, and the old name never becomes an alias. Cleanup conditionally
removes only the captured mapping, preserving names recreated with another identity.

Drop tombstones the selected head without traversing snapshots or deleting files.
A purge request persists a `TablePurgeTask` containing the tombstoned head,
activation epoch and durable marker time, indexed by table, generation and metadata file.
The purge task becomes eligible 20 minutes after that marker time; a delayed
scheduler scan or restart does not restart the delay. A drop without an explicit
purge request leaves the underlying files in place. Purge reclaims table-scoped
files after the delay and owner checks, without snapshot reachability scans
inside the dropped table. Success is
retained before releasing rename head/namespace markers. Retrying after response
loss returns the original result without mutating a replacement table. The REST
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
KV and chunk request/byte budgets, and durable retry state. The enabled
scheduler admits persisted table purge markers and completed catalog clears;
management may also start inactive tasks. Operators can disable the scheduler
or adjust validated resource limits. It scans once at startup, then every
30 minutes by default. A purge becomes eligible 20 minutes after the drop;
the next scan may start its work up to one scan interval later.

Provisioned disk capacity is the allocation boundary for both foreground files
and GC durable workspace. A failed GC workspace write retains the last durable
continuation and defers retry; it never substitutes an incomplete proof or
authorizes deletion. Committed files remain readable when new chunk allocation
fails. Progress resumes after capacity is restored through the normal storage
flow. Shared-chunk ranges remain pending while range deletion is unsupported.

Request entry checks catalog and table or draft authority without request-level
pins. Selected context and records flow through normal publication and reads;
PUT and GET do not perform deletion-fence or GC-claim GETs. Physical reclamation observes a
minimum retention interval that covers admitted request lifetimes. Once a file's canonical
logical deletion has started, its location-key tombstone rejects resolution
until cleanup state permits reuse, even if physical range reclamation is deferred. Legacy live tasks are retired without
further deletion, releasing an owned table fence. Retained and deferred
candidates remain durable work for later inactive passes.

An already authorized FileIO GET or HEAD can continue within the configured
retention interval after a logical deletion. A concurrent transition to
`Reclaiming` rejects new admission. Uploads and new table credentials require
a Ready table. Logical drop preserves admitted file reads until their bounded
response lifetime ends.

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
An active catalog has a separate multipart cleanup task that scans only expired,
terminal parts and assembly checkpoints. It does not scan published file records
for missing snapshot references or fence a Ready table. Candidate progress is
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
proof pages, write fences and old tasks. Only the retired authority, winning task
result and retirement marker remain. Late unfinished records stop cleanup.
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
records a logical tombstone, then queues bounded physical reclamation after
20 minutes. A new PUT may reuse the path only after the old file's GC candidate
is durable. Batch deletion returns individual errors inside HTTP 200, so callers
must inspect every item.

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
