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
Selection, Read Plan, and Cursor. Worker scheduling, tensor decoding, collation,
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
implies ancestry. Snapshot merge is explicit: a merge produces a new snapshot
with a declared parent and delta. Old snapshots remain readable until retention
releases them.

Publication builds and validates the manifest, schema binding, indexes, payload
references, checksums, and durability state before atomically advancing a
mutable name. Readers observe either the old complete snapshot or the new
complete snapshot.

## 4. Manifest, schema, samples, and fields

The Manifest is the authority for a snapshot's schema binding, sample membership,
splits, selections, field locators, checksums, statistics, and parent delta. It
may be partitioned and indexed; a single hot key cannot be required for all
reads. Schema defines field type, structure, requiredness, missing-field
semantics, and evolution rules.

`sample_id` is a stable opaque string supplied by the importer. Within one
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

A Split is a logical subset of a snapshot. A Selection in a Read Plan chooses
samples using prefix, range, explicit IDs, keyword, field match, and boolean
composition. Prefix, range, and explicit IDs can be checked for overlap directly;
metadata predicates require a scan against one fixed snapshot. The manifest
records any permitted overlap policy.

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

An active-read lease protects payloads reachable from the selected snapshot while
the read is in progress. Normal close or cancel releases it immediately. If the
connection has no activity for 60 seconds, the lease is invalid and GC may treat
the read as disconnected; this TTL is the crash and network-failure fallback.

Dataset owns the lifetime of its metadata and payload references. Reclamation
requires that no retained snapshot, tombstone needed by a retained history, or
active read lease can reach the value or chunk. External S3 or Iceberg references
are provenance unless an explicit cross-model retention contract says otherwise.

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
- **DATA-I9 — Safe reclamation:** retained snapshots and active reads protect all
  reachable metadata and payload.
- **DATA-I10 — Surface equivalence:** HTTP and native access return the same
  logical selection and integrity result.

## 10. Implementation and I/O flows

The implementation is split into a `crowdb-access-dataset` library and a thin
`crowdb-access-server` Dataset listener, following the S3 and Iceberg boundary.
`DatasetAuthority` owns keys, records, publication state, ancestry, retention,
and durable progress through the `DatasetStore` interface. `DatasetReadService`
owns admission, bounded delivery, plan/cursor execution, and lease lifetime.
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

`DatasetSessions` resolves latest/stable once, binds the request to that
immutable Snapshot, and persists immutable pages of selected IDs separately
from a small progress record. Selection, deterministic shuffle and rank/worker
partition execute before fetching payloads. Projection walks explicit ancestry
only until all requested fields are resolved. Plans admit at most 100000 IDs;
metadata is capped at 8 MiB and selection pages contain at most 1024 IDs.

Fetching issues a plan-bound opaque next cursor without advancing acknowledged
progress. Repeated fetches replay that batch. Acknowledgement CAS-advances only
its issued cursor and is idempotent; progress inspection performs no payload
read. Resume restores acknowledged progress. Explicit close affects only that
handle; a disconnected handle expires after 60 seconds and can resume while its
Snapshot remains available.

The public binary response streams a bounded JSON header, integrity-checked
payload frames, field terminators and a batch terminator. Success also requires
HTTP EOF with no trailing bytes. Each active transfer owns a unique scoped read
lease and admission credits through transport delivery/drop. Heartbeats renew
leases during active streaming. Cancellation stops the producer and returns
credits; retained SDK-owned buffers keep their byte credits until released.
The internal legacy scan adapter retains its compatibility behavior; public
fetch never implies application acknowledgement.

### 10.3 Retention and reclaim flow

Reclaim atomically fences new reads, invalidates stale head publishers with a
head nonce, and rechecks latest, stable, explicit retention and parent ancestry.
An Aborted Snapshot tombstone preserves the original Manifest while payload
cleanup is pending and prevents publication retry resurrection. Upload ownership
is released for that operation only; other Snapshot owners preserve shared
bytes. Deferred physical cleanup remains pending. Shared range retirement does
not count as a reclaimed whole chunk.

Upload expiry scans one bounded registry page per background sweep. Registration
and removal use generation nonces and session CAS fences so stale cleanup cannot
remove a newer registration. Restart restores sessions, accepted parts, progress
and pending cleanup from the durable store; in-memory state is not authoritative.

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

Head and lease envelopes decode legacy version 1 but write version 2 with
publication/reclaim fencing and scoped leases. This metadata upgrade is one-way:
do not downgrade an upgraded authority or run old and new authority writers
against the same Dataset store. Immutable Dataset/Snapshot/Manifest encodings
remain compatible. Upload registry pages also decode the initial array form and
write generation-tagged entries. Private read-session progress is version 2;
public API versions do not expose these private persistence formats.

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
