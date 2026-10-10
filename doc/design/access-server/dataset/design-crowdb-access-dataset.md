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
supports HTTP access and a separately maintained SDK with thin and fat client
modes, with equivalent selection and integrity semantics. It does not own disk placement, replication,
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

HTTP is the portable Dataset surface. The native client loads the same authority
and a bounded versioned routing view, then connects directly to responsible
metadata and data services. Neither surface may change snapshot semantics.

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
boundaries; the native client observes topology but never publishes placement.
Framework adapters map Read Plans to PyTorch `IterableDataset`, TensorFlow/JAX
iterators, Hugging Face streaming examples, or Ray blocks without adding storage
objects.

S3 and Iceberg remain peer metadata domains. Dataset can consume immutable data
from either through an explicit source reference, but cannot commit their tables,
overwrite their objects, or reclaim their owned bytes.

The Dataset library owns these semantics and storage clients. The existing Access
Server may expose Dataset HTTP on its own configured listener, alongside S3 and
Iceberg. A separate Dataset SDK project, maintained by this team, provides both a
thin HTTP client and a fat client that connects directly to internal Chunk-KV and
DiskIO. The same SDK owns the PyTorch `IterableDataset` adapter. These are two
client modes, not two Dataset authorities or a separate Dataset server product.

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

### 10.1 Publication flow

The HTTP or native client serializes a publication request and sends it to the
listener or authority. The authority validates the Dataset, parent, operation
token, and Manifest before any head mutation. It CAS-writes the operation
record, writes a `Prepared` Snapshot record, partitions and persists the
Manifest, then CAS-transitions the Snapshot to `Published` and advances the
head. A retry reads the operation record and resumes the same Snapshot. A
failure before the published transition leaves `latest` unchanged. Manifest
partitions and the head are never read through the HTTP listener directly;
all reads go through the authority's complete-snapshot checks.

### 10.2 Read flow

`DatasetReadService` validates the request and acquires a delivery-window
permit before admitting work. It then records the Dataset/Snapshot read lease,
loads the Manifest and inherited ancestry through `DatasetAuthority`, computes
bounded sample groups and projection, and resolves each field. Inline values
are verified locally; chunk locators are passed unchanged to the chunk store,
which performs range/stream I/O and integrity checks. The service releases the
lease and delivery permit after the batch result is formed, then persists the
next cursor when the operation is a Scan. Errors release lifecycle state before
being mapped to the native or HTTP error class.

### 10.3 Retention and reclaim flow

Retention mutations update Snapshot-scoped records through the authority.
Reclaim first walks the published head's explicit parent chain and checks
latest, stable, retained, and active-read protections. It then computes
Dataset-owned metadata and payload candidates, leaves shared or externally
owned locators untouched, deletes only eligible state, and persists completed
reclaim progress. Repeated reclaim and authority restart read the same durable
progress record and remain idempotent.

### 10.4 Restart and parity flow

The authority reconstructs its view from DatasetStore records on startup; no
in-memory head or lease registry is authoritative. HTTP and native entry
points call the same authority and read service, so parity tests compare
Snapshot IDs, manifests, sample ordering, checksums, cursors, leases, and
logical error classes. Restart tests reopen the same store and verify published
heads, Manifest partitions, cursors, retention state, and reclaim progress.

Metadata mutations use the smallest possible authority operation. A single-key
CAS is reserved for a publication head, an operation phase, or an admission
fence whose conflict is the correctness decision. Do not CAS every sample,
field, payload block, or read cursor. Immutable payloads are written once and
referenced by the manifest; retry identity and persisted journals handle
recovery without a global CAS loop.

Hot reads and planning use immutable snapshots, atomics, or `ArcSwap`-style
published views. Do not add a mutex or async lock to point reads, manifest
lookups, chunk reads, or stream delivery. Lifecycle-only coordination may use a
bounded lock when an existing owner requires it, with contention measured before
acceptance.

Small values follow the existing small-object path and remain inline only when
the configured threshold admits them. Large fields use the large-write pipeline
with bounded blocks, backpressure, and chunk-native range/stream reads. Readers
fetch bounded frames and verify chunk/block integrity. The record contract may
require MD5 for compatibility or SHA-256 for canonical object identity; the
algorithm is selected by correctness requirements, never omitted to optimize a
benchmark. Digest cost, placement, and whether a full-object pass is needed are
measured in the relevant backlog item. Dataset does not add whole-object
buffering to the hot path.

## 11. Risks and open questions

Future namespaces add metadata fan-out and authorization scope. The first
implementation intentionally has no namespace operations; it only carries the
configured default/root namespace in every identity.

Manifest partitioning, predicate indexes, cache invalidation, and direct GPU
transport require workload measurements. They are independent extensions and do
not alter the Dataset identity or snapshot contract.
