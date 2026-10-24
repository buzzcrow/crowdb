<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Dataset

Dataset is CROWDB's sample-oriented data domain for AI, analytics, and data
preparation. It owns dataset metadata, immutable snapshots, sample selection,
field projection, bounded reads, and payload lifetime while using CROWDB KV and
chunks for durable state and bytes.

The normative object and behavior contract is [CROWDB Dataset Specification](spec-crowdb-dataset.md). This design document describes the implementation architecture that realizes that contract; it does not redefine Dataset semantics.

This design is independent of S3 and Iceberg. A Dataset may record an external
source reference, but that reference does not replace Dataset publication,
selection, or reclamation authority.

## Table of contents

1. [Scope and boundary](#1-scope-and-boundary)
2. [Authority and naming](#2-authority-and-naming)
3. [Dataset and snapshot model](#3-dataset-and-snapshot-model)
4. [Manifest, schema, samples, and fields](#4-manifest-schema-samples-and-fields)
5. [Selection and read plans](#5-selection-and-read-plans)
6. [Access surfaces and bounded delivery](#6-access-surfaces-and-bounded-delivery)
7. [Progress and lifecycle](#7-progress-and-lifecycle)
8. [Interactions](#8-interactions)
9. [Correctness invariants](#9-correctness-invariants)
10. [Implementation and I/O flows](#10-implementation-and-io-flows)
11. [Risks and open questions](#11-risks-and-open-questions)

## 1. Scope and boundary

Dataset provides a stable logical model for samples and heterogeneous fields. It
supports HTTP access through an independently packaged Rust SDK and Python
native wrapper. Direct storage access is a future client mode with the same
selection and integrity semantics. It does not own disk placement, replication,
erasure coding, repair, or physical chunk layout. It does not become a query
engine or a training-framework-specific storage protocol.

The core objects are Dataset, Snapshot, Manifest, Schema, Sample, Field,
Split revision, Selection, Read Plan, and Cursor. Worker scheduling, tensor decoding, collation,
and model checkpoints remain in adapters or execution systems above Dataset.

## 2. Authority and naming

Dataset authority always supplies the fixed `dataset-ns` default/root namespace. Every Dataset
identity carries that namespace, even though the first implementation exposes no
namespace create, delete, or mutation operation. Dataset names are unique within
the supplied namespace. The representation keeps namespace in the key so a
future multi-namespace authority can be added without changing Dataset identity,
snapshot inheritance, chunk placement, or read ordering.

## 3. Dataset and snapshot model

A Dataset owns metadata, snapshots, manifests, samples, fields, and the payload
references needed to read them. A Snapshot is an immutable published view
identified by an opaque `snapshot_id`. `latest` and `stable` are mutable names
that resolve to real snapshot IDs; cursors and read plans store the real ID.

A Snapshot has at most one `parent_snapshot_id`. A child applies additions,
deletions, and membership rules to its parent; the numeric order of IDs never
implies ancestry. Any committed same-Dataset Snapshot can be the parent; multiple
parentless roots form a forest. Multi-parent merging is not supported. Old
Snapshots remain readable until reclamation has passed their protection checks.

Publication builds and validates the manifest, schema binding, indexes, payload
references, checksums, and durability state before atomically advancing a
mutable name. Readers observe either the old complete snapshot or the new
complete snapshot.

## 4. Manifest, schema, samples, and fields

The Manifest is the authority for a Snapshot's schema binding, sample membership,
field locators, checksums and parent delta. Split sets are separate immutable
revisions owned by that Snapshot. The Manifest
may be partitioned and indexed; a single hot key cannot be required for all
reads. The current Schema records a version and field names/requiredness; it
does not declare types, MIME, nested structures or semantic media metadata.

`sample_id` is a stable opaque byte sequence supplied by the importer, represented
as canonical padded base64 in JSON. Within one
snapshot, `(sample_id, field)` has one logical value. A field is either an inline
KV value for bounded metadata or an opaque chunk location string for larger
payloads. Dataset passes the location to the chunk system and never parses or
constructs its physical meaning. Copy-on-write shares unchanged values between
snapshots; field and sample deletion uses tombstones so historical snapshots
remain valid.

A sample may contain image, video, audio, text, label, embedding, or metadata
fields. Fields are aligned by snapshot and sample identity, while decoding and
tensor composition belong to the consumer adapter.

## 5. Selection and read plans

A Split is a named logical subset of a Snapshot. One immutable Split revision
contains the complete set of names, definitions, membership and overlap/coverage
policy. A Snapshot's currentSplit points to one revision or is absent. Publishing
a revision changes only this pointer, never Dataset latest or the data Snapshot.
A Selection in a Read Plan chooses
samples using prefix, range, explicit IDs, keyword, field match, and boolean
composition. Prefix, range, and explicit IDs can be checked for overlap directly;
metadata predicates require bounded work against one fixed Snapshot. Defaults
are disjoint membership and allowed unassigned Samples. A committed empty set
differs from no Split revision. All committed revisions live until owner reclaim.

The [control lifecycle](design-dataset-control.md) specifies durable composition,
frozen Split inheritance, publication attempts, bounded inspection and cleanup.

A Read Plan binds one snapshot ID to selection, ordering, optional shuffle
parameters, and batch policy. The implementation supports bounded sample and
group shuffle.
Sample shuffle loads only sample IDs and lightweight metadata into bounded groups;
a selection that fits one group completes in one pass, while a larger selection
uses multiple groups with the same deterministic contract. Group shuffle remains
an explicit plan type for prefix, metadata, session, device, time-window, or
association-key grouping. Every shuffle has sample-count, retained-byte, batch,
prefetch, and in-flight limits and is rejected before payload reads when a limit
would be exceeded. Shuffle never moves or rewrites published payload.

## 6. Access surfaces and bounded delivery

HTTP is the portable Dataset surface used by the public Rust SDK and its
Python native wrapper. In-process typed authority calls preserve the same
Snapshot semantics. A future direct client uses a bounded versioned routing
view to reach responsible metadata/data services; it cannot replace authority.

`GetSample`, `GetBatch`, and bounded Scan resolve metadata first, apply field
projection, and then read inline values or chunk ranges/streams. Batch size,
read window, prefetch, cache, and in-flight requests are bounded and propagate
backpressure. Direct GPU delivery may use short-lived capabilities and a CPU
fallback, but it must preserve the same selection and integrity result.

## 7. Progress and lifecycle

A cursor is valid only for its bound Read Plan and snapshot. Ordered reads store a
sample position; grouped shuffle stores group identity or index and an offset
within the group. The client advances a cursor after batch confirmation. A crash
may replay the last unconfirmed batch, but may not skip or change the snapshot.
Epoch and sampler state remain client state.

Read admission checks the selected snapshot's authoritative deletion state without
registering a reader. Deletion durably fences new reads and records a five-minute
`reclaim_after` deadline; retries and restarts preserve that deadline. Existing
streams can finish using resolved metadata and payloads during the grace period.
Unusually long reads may fail after reclamation; partial delivery is never complete
success. Access Servers and native clients do not acknowledge deletion. Client
checkpoints and local request/buffer admission have independent lifetimes. EOF,
cancel and drop release local request admission without remote cleanup writes;
caller-held buffers remain valid and charged until their final release.

Dataset owns the lifetime of its metadata and payload references. Reclamation
preserves explicit retention, live snapshot ancestry and shared payload owners.
External S3 or Iceberg references are provenance unless an explicit cross-model
retention contract says otherwise. Physical I/O identity checks and buffer ownership
remain necessary when storage is reclaimed or reused.

## 8. Interactions

Dataset metadata uses KV authority and can use chunk-backed range state for
large manifests or indexes. Chunk I/O owns location validation, range/stream
reads, checksums, and physical recovery. Access Server exposes HTTP and health
boundaries; a direct client observes topology but never publishes placement.
Framework adapters map Read Plans to PyTorch `IterableDataset`, TensorFlow/JAX
iterators, Hugging Face streaming examples, or Ray blocks without adding storage
objects.

S3 and Iceberg remain peer metadata domains. Dataset can consume immutable data
from either through an explicit source reference, but cannot commit their tables,
overwrite their objects, or reclaim their owned bytes.

The Dataset library owns these semantics and storage clients. The existing Access
Server may expose Dataset HTTP on its own configured listener, alongside S3 and
Iceberg. The Dataset SDK has an independent Rust workspace and release version
in the CrowDB repository. Its HTTP client uses reqwest, Tokio and rustls; the
Python package calls that implementation through PyO3. Clients share pooled
connections, atomic request/byte admission and process-owned runtime resources.
Blocking native waits release the GIL; live handles inherited across fork are
rejected. File transfers and batched iteration bound memory independently of
file size. Tensor decoding and collation stay above the SDK. Direct Chunk-KV,
chunk access, local staging and RDMA remain extensions.

## 9. Correctness invariants

- **DATA-I1 — Namespace identity:** every Dataset carries the configured
  default/root namespace, and a Dataset name is unique within it.
- **DATA-I2 — Snapshot consistency:** one operation resolves exactly one immutable
  snapshot and never mixes fields from another snapshot.
- **DATA-I3 — Atomic publication:** readers observe no manifest or one complete
  published snapshot.
- **DATA-I4 — Explicit ancestry:** only the selected snapshot's parent chain
  explains inherited state; ID ordering is irrelevant.
- **DATA-I5 — Opaque locations:** Dataset does not interpret physical chunk
  locations.
- **DATA-I6 — Logical shuffle:** ordering and shuffle do not move published
  payload.
- **DATA-I7 — Bounded delivery:** batch, window, prefetch, cache, and in-flight
  limits bound memory and registration.
- **DATA-I8 — Cursor binding:** a cursor and Read Plan cannot resume against a
  different snapshot or selection identity.
- **DATA-I9 — Safe reclamation:** retention and live ownership protect reachable
  metadata and payload. Deletion rejects new reads and delays reclamation for the
  grace period; overdue reads may fail without returning truncated success.
- **DATA-I10 — Surface equivalence:** HTTP and native access return the same
  logical selection and integrity result.

## 10. Implementation and I/O flows

The implementation is split into a `crowdb-access-dataset` library and a thin
`crowdb-access-server` Dataset listener, following the S3 and Iceberg boundary.
`DatasetAuthority` owns keys, records, publication state, ancestry, retention,
through the `DatasetStore` interface; read checkpoints stay with the caller. `DatasetReadService`
owns admission, bounded delivery, plan/cursor execution, and request lifetime.
`DatasetHttpService` owns HTTP framing, authentication, route/status mapping,
shutdown, and health wiring. `ChunkKvDatasetStore` adapts Dataset metadata and
payload access to the chunk-backed KV client without exposing physical layout
to the authority.

### 10.1 Upload and publication flow

`DatasetUploads` persists bounded upload sessions, immutable numbered parts,
accepted receipt pages and Dataset-scoped ownership in `DatasetStore`.
`ChunkPayloadStorage` writes parts through the shared small-object pipeline with
Dataset chunk type 7. Each part is at most 4 MiB; 10000 parts bound an upload to
39.0625 GiB. A write intent records the exact physical location before payload I/O.
Successful durable completion accepts the receipt. Recovery accepts an intent
only after the chunk's acknowledged cursor covers the location and the bytes
pass integrity checks. Readable but unconfirmed bytes remain pending. A fenced
abandoned allocation is reclaimed before replacing it under the same part ID.
Completion verifies every part and the whole declared MD5 with bounded buffers.
For one part whose declared length and MD5 match the whole upload, verification
hashes the stored bytes once and reuses that exact-range result. Multiple part
digests cannot be concatenated into the whole-upload MD5.

Expiry scans bounded pages of upload authorities directly. Each caller retains
its scan cursor locally; restart repeats the scan and loses no cleanup obligation.
Discovery reads at most twenty keys / 1 MiB and seeks past each upload's child
subtree. A bounded local queue schedules uploads in turn; each cleanup window
scans at most 32 actual part records / 1 MiB and attempts at most 64 range
notifications. Discovery and cleanup each have a two-second deadline. Sparse
uploads do not perform a point GET for every declared but absent part.
Accepted range retirement permits deletion of that aborted upload's part record;
the upload cannot accept a new part or owner. Deferred or uncertain notification
retains the exact range. A full queue can discard a hint only after serving it,
leaving a discovery slot; removed part records ensure useful progress survives
hint eviction and restart. There is no persistent scan or queue record.
Independent upload creation writes only its own authority. No shared registry tail,
registration generation, registry fence or durable scan cursor participates in
creation, part upload or owner release. Corrupt/failed entries are reported and
retried next cycle without starving subsequent records. Legacy registry pages are
ignored. Claimed payloads and terminal owner revocation remain authoritative;
removing an owner cannot release another owner's bytes. Accepted-part index pages
remain bounded to 128 entries to avoid a point read for every part during listing
and completion.

Prepare validates the complete current sample membership, schema, parent and
operation token, then persists an invisible candidate and claims completed
uploads. Omitted samples disappear; surviving samples inherit omitted optional
fields through explicit ancestry. Tombstones remove inherited values. Required
field names cannot be removed from a child schema.

Publish reuses the prepared Snapshot ID, marks its complete record Published,
and CAS-advances the head. A Published operation record can precede head
advancement; a successful publish response establishes the completed operation.
Lost responses are resolved by inspecting status and retrying identical content
with the same operation token. Historical retries cannot rewind a newer head.
Divergent retries conflict. Prepared candidates remain until publication or
explicit reclaim; upload expiry does not reclaim claimed candidate payloads.

### 10.2 Read flow

`DatasetReads` resolves latest/stable and optional currentSplit into immutable
versions and returns a self-contained continuation. The server stores no read
record, selected-ID pages, progress, TTL or cancellation state. Every next request
supplies its continuation and passes ordinary authentication and deletion admission.
The token binds Dataset, Snapshot, Split, selection, projection, worker partition,
shuffle seed/group size and position. It is a cursor, not an authorization grant.

Selection uses bounded inspection windows without fetching a full Manifest.
Within one next request, selection windows share admission, immutable summaries,
schema and compiled predicates. Explicit IDs are validated once. This prepared
query is local to the request and binds its authority, Dataset and Snapshot;
independent page requests still perform their own admission. Per-window storage
budgets and the overall group deadline remain enforced.
Split filtering precedes deterministic shuffle and worker partitioning. The cursor
carries a group start and offset, not the Dataset's selected IDs. A partly consumed
shuffle group is reconstructed from immutable metadata; this bounds memory but
adds metadata reads when groups are much larger than delivered batches. Groups
are capped at 100000 selected IDs and 8 MiB of IDs; selection work has a two-second
deadline and at most 5000 scan windows of up to 20 candidates. Serialized cursor
content is capped at 256 KiB before base64 encoding. Invalid or excessive work
returns an explicit error. Stream metadata and headers remain capped at 8 MiB.

Repeating a continuation replays its batch, including from another Access Server
or native client. After successful application processing, the caller saves the
returned next continuation in its own checkpoint. EOF and buffer release never
save progress. There is no remote ack, resume or close; cancelling a request stops
its local producer. The SDK iterable's acknowledgement is a local operation.

The public binary response streams a bounded JSON header, integrity-checked
payload frames, field terminators and a batch terminator. Success also requires
HTTP EOF with no trailing bytes. Each active transfer owns local admission
credits through delivery/drop. Snapshot reads perform no persistent activity
updates or heartbeat writes. Cancellation stops the producer and returns credits;
retained SDK-owned buffers keep their byte credits until released.
For an uploaded field, HTTP and native readers validate one immutable upload
description per request, then fetch each part record once. Part indices, lengths
and payload digests remain checked; EOF does not reload metadata. The description
is request-owned and registers no remote reader or retention. New requests still
perform admission, and reclaimed parts fail instead of producing successful EOF.
Native delivery reuses a single part's successful-EOF length and MD5 proof only
when the part is the complete field range and matches the field's expected digest.
Partial data and failed streams provide no proof. Multiple parts still require
the independent whole-field digest, and inline fields remain checked directly.
The internal legacy scan adapter also returns a caller-owned cursor without
loading or persisting progress. Requests have no shared inactivity TTL.

Binding reuses its committed-Snapshot admission. Inspection-backed selection,
preview and nonempty batch delivery perform admission inside their bounded
resolver; wrappers do not repeat the same deletion lookup. Empty and legacy
batch entry points retain independent admission. Selection and delivery remain
separate bounded operations, each with its own admission.

Legacy Snapshots without a stored visibility proof are checked against head and
anchor ancestry using bounded, cancellable reads. The traversal validates the
Snapshot identity and publication state, rejects cycles and never fills a
persistent proof cache. Existing proofs remain readable; modern unpublished
candidates cannot use legacy ancestry as evidence of publication.

### 10.3 Retention and reclaim flow

Reclaim atomically fences new reads, invalidates stale head publishers with a
head nonce, and rechecks latest, stable, explicit retention and all live committed
child branches. Temporary child admission fences do not prove retirement. A
reclaimer owns a CAS token; only that owner can reopen admission or enter the
irreversible retiring phase. Concurrent stale reclaimers cannot undo retirement.
After the terminal deletion mark, cleanup waits until the persisted grace
deadline. The Access Server scans bounded pages of deletion records and resumes
cleanup independently of reader processes. An Aborted Snapshot tombstone preserves
the original Manifest while payload cleanup is pending and prevents publication retry resurrection. Upload ownership
is released for that operation only; other Snapshot owners preserve shared
bytes. Deferred physical cleanup remains pending. Shared range retirement does
not count as a reclaimed whole chunk.

`reclaim_snapshot_metadata_step` persists phase and continuation while deleting
at most twenty metadata keys per step. It removes Manifest storage and inspection
projections. Snapshot-owned Split revision/member records become small permanent
CAS fences so delayed immutable writes cannot restore removed contents. Commit
receipts, head fences and terminal retirement evidence remain. A retry helps the
final status marker if cleanup completed before the response or status write failed.

Composition evaluation work pages record their possible row writes before applying
those writes. Obsolete-window cleanup seals rows before sealing their work page,
then seals the first absent work page to exclude delayed next-window creation.
Split evaluation similarly registers immutable page keys/digests before writing
members. Split cleanup processes at most twenty page references per step, retaining
committed live revision data. Initial Split cleanup requires terminal retirement of
its data Snapshot. Both cleanup cursors survive restart and preserve active input
revisions. Split drafts without the work-intent format remain readable but are
rejected by draft scratch cleanup; new roots carry an explicit work format marker
that older strict decoders reject. Terminal Snapshot cleanup walks Dataset draft
roots in resumable single-root windows, including drafts without a published
revision summary. Under that terminal fence it can upgrade an older root's work
marker before retiring its stored pages. Split revision summaries remain valid
small records and are filtered by owner/commit visibility.

The composed reclaim operation advances upload owners, Split drafts, index fences
and ordinary metadata cleanup with a ten-second deadline. Each component retains
its own restart cursor; completion does not claim physical upload deletion.

Obsolete composition candidates are discovered by deterministic operation IDs,
even when preparation failed before storing a pointer in the draft. Missing
operation/Snapshot reservations become permanent fences. Existing uncommitted
candidates enter terminal retirement before cleanup enumerates every possible
Manifest/index key from the retained input. This seals writes not yet sent by a
failed preparer. Small valid retired inventory/child summaries and write fences
survive general cleanup. Candidate recovery caps its original record at 9 MiB
and metadata window at 9 MiB with a two-second operation deadline.

Managed composition uploads register staging and candidate owners durably before
claiming payloads. Publication requires complete candidate ownership. Terminal
cleanup retires the staging owner and obsolete candidate owners, preserving
published candidates and independent owners of shared uploads. Snapshot upload
reclaim walks at most twenty retained Manifest field positions per step after
terminal admission fencing. It revokes only that Snapshot publication's owner;
physical cleanup stays discoverable from durable upload authorities. The retained Manifest
allows owner cleanup even after inspection indexes have been retired.

Composition version 3 registers each edit ID in the root before writing its
payload page. Up to 4096 registered edits, including failed attempts, are allowed.
Terminal cleanup seals these pages in bounded windows and keeps immutable edit
receipts. Older composition writers reject version 3 rather than ignoring the
registration protocol. Existing records remain readable; editing upgrades the
root while preserving its journal and payload ownership.

`retire_owner` permanently revokes one publication's upload claim with the same
session CAS that removes its ownership. A delayed claim for that operation then
conflicts; other owners remain valid. Sessions retain at most 4096 retired owner
IDs and report capacity explicitly. Ordinary `release` remains reversible.

Upload expiry uses local continuation/queue hints and existing upload/part resource
facts. Shared registry registration and removal are absent. Snapshot deletion
sweeps also yield after a two-second page deadline, keeping durable deletion marks
and advancing past a stalled item. The listener schedules snapshot pages every
fifteen seconds and upload catch-up windows fifty milliseconds apart; failed
upload windows back off five hundred milliseconds while other work remains.
Idle discovery runs every fifteen seconds. Limits bound work per window rather
than promise a maximum reclamation latency or measured foreground p99.

### 10.4 Transport and compatibility

The public versioned HTTP surface uses JSON control records, binary payload
streams and bearer authentication. Legacy bincode routes remain an internal
compatibility interface. HTTP/1.1 keep-alive has separate bounded idle connection
and executing request admission, header deadlines and graceful shutdown.
Default executing admission is eight requests. Early authentication/capacity
rejection drains a bounded body without materializing it so valid part writers
receive the structured error. Oversized or stalled rejected bodies close their
connection after a bounded drain; they cannot consume unbounded resources.
Uploads and downloads stream with bounded concurrency and backpressure. The SDK
retries only documented idempotent requests, preserving replayable bytes and
operation IDs under one overall deadline. Authentication, integrity and semantic
conflicts are not blanket retried. File downloads use a private temporary file
and rename only after successful integrity verification.

Head envelopes decode legacy versions and use version 3 for commit-attempt
markers. Access envelopes decode versions 1 through 4 and write version 4 with
reclaim ownership, terminal deletion, grace deadline, cleanup target and completion.
Legacy reader activity is ignored; all authority writers must use this contract.
Upload sessions decode versions 2 through 4 and write version 4 without registry
fields. Permanent retired-owner identities still reject delayed claims. This
metadata upgrade is one-way: do not downgrade an upgraded authority or mix old
and new authority writers against the same Dataset store. Immutable Dataset/Snapshot/Manifest encodings
remain compatible. Persisted read-session records and legacy cursor keys are
unused. Session read routes are absent; clients use `POST reads/bind` and
`POST reads/next`. Upgraded clients and servers must deploy together. Control v2
advertises `stateless-reads` so an SDK cannot treat the retired session capability
as support for this contract. Physical purging of unused keys is an explicit
maintenance operation, not a read-path side effect.

Hot-path synchronization uses atomics, immutable owned views, CAS and bounded
notification channels. No application-wide request lock serializes independent
operations. Producer admission includes queued native buffers and metadata
parsing, while caller-retained completed results belong to the application.
Functional resource checks verify these ownership rules. Benchmark drivers,
measurements and performance thresholds are outside this implementation.

## 11. Risks and open questions

Future namespaces add metadata fan-out and authorization scope. The first
implementation intentionally has no namespace operations; it only carries the
configured default/root namespace in every identity.

Manifest partitioning, predicate indexes, cache invalidation, and direct GPU
transport require workload measurements. They are independent extensions and do
not alter the Dataset identity or snapshot contract.
