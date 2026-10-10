<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Dataset Specification

This document is the normative Dataset contract. It defines the objects,
identities, state transitions, observable behavior, and compatibility rules
shared by Dataset authorities, HTTP clients, native clients, and future SDKs.
The implementation design is described in
[Design: Dataset](design-crowdb-access-dataset.md); when the two documents
appear to disagree, this specification is authoritative.

## Table of contents

1. [Scope and terms](#1-scope-and-terms)
2. [Identity and names](#2-identity-and-names)
3. [Dataset records](#3-dataset-records)
4. [Operations](#4-operations)
5. [Manifest and field values](#5-manifest-and-field-values)
6. [Snapshot publication](#6-snapshot-publication)
7. [Selection, plans, and cursors](#7-selection-plans-and-cursors)
8. [Read behavior](#8-read-behavior)
9. [Retention and reclamation](#9-retention-and-reclamation)
10. [HTTP and native surfaces](#10-http-and-native-surfaces)
11. [Errors and compatibility](#11-errors-and-compatibility)
12. [Normative invariants](#12-normative-invariants)

## 1. Scope and terms

Dataset is a logical, sample-oriented collection. It stores metadata and
references to payload bytes; it does not define physical chunk layout,
replication, erasure coding, or framework tensor types.

The following terms have one meaning throughout this specification:

- **Dataset** is the named authority for immutable snapshots and their
  metadata.
- **Snapshot** is an immutable view of a Dataset. It is identified by an
  opaque `SnapshotId` and may name one parent Snapshot.
- **Manifest** is the complete, validated description of one Snapshot's
  schema, sample membership, field values, partitions, and integrity metadata.
- **Schema** defines the fields, requiredness, and compatibility rules for a
  Manifest.
- **Sample** is an opaque `sample_id` and its field map in one Snapshot.
- **Field** is a named value of one Sample. A missing field and a tombstone are
  distinct: missing means no value is supplied by the visible ancestry;
  tombstone explicitly hides an inherited value.
- **Locator** identifies payload bytes. Inline values are stored in metadata;
  chunk locators are opaque strings interpreted only by the chunk subsystem.
- **Selection** identifies samples from one Snapshot.
- **Read Plan** binds a Selection, ordering, projection, batching, and optional
  shuffle to one immutable Snapshot.
- **Cursor** records progress for one Read Plan and is acknowledged only after
  the corresponding batch is accepted by the caller.
- **Active-read lease** protects data reachable from one Dataset/Snapshot while
  a read is admitted or executing.

Dataset does not infer semantics from a locator, a Snapshot ID's numeric
ordering, a sample ID's encoding, or an external source URI.

## 2. Identity and names

A Dataset identity is `(namespace, dataset_name)`. The initial authority uses
the configured `dataset-ns` root namespace for every Dataset. Names are unique
within that namespace and are compared as opaque UTF-8 names after validation.

`SnapshotId` is immutable and opaque. `latest` and `stable` are mutable names
whose values are Snapshot IDs. A Read Plan, Cursor, Manifest, or lease stores
the resolved Snapshot ID and never stores a mutable name in its place.

A Snapshot has zero or one declared parent. Parentage is explicit; ID ordering
never implies ancestry. A child may add, replace, or tombstone samples and
fields while inheriting all values not overridden by its visible parent chain.

## 3. Dataset records

A Dataset record establishes that the identity exists and owns its metadata.
Creating an existing identity is a conflict. Opening a missing identity is a
not-found error. Dataset deletion is outside this specification.

A Snapshot record contains the Snapshot ID, optional parent, publication
operation identity, Manifest binding, and publication state. Publication states
are:

- `Prepared`: the operation and candidate Snapshot are durable but the
  Snapshot is not visible through `latest`.
- `Published`: the complete Manifest and binding are durable and the Snapshot
  may become visible through `latest`.
- `Aborted`: the candidate is permanently unavailable for publication.

A Manifest binding records the Snapshot it belongs to, its partition count, and
its integrity metadata. A binding for another Snapshot is invalid. Partitions
are an internal representation; readers observe one logical Manifest.

## 4. Operations

The Dataset authority provides these operations. Each operation is scoped by a
Dataset identity; operations that take a Snapshot ID MUST use the immutable ID,
not `latest` or `stable` as an implicit mutable reference.

| Operation | Input | Successful result |
| --- | --- | --- |
| `CreateDataset` | Dataset identity | Creates the identity; duplicate creation conflicts. |
| `OpenDataset` | Dataset identity | Returns the Dataset record. |
| `PrepareSnapshot` | Parent, Manifest, operation token | Returns one durable candidate Snapshot ID that is not visible through `latest`. |
| `PublishSnapshot` | Parent, complete Manifest, operation token | Returns the published Snapshot ID and advances `latest` atomically. |
| `GetLatest` | Dataset identity | Returns the current Snapshot ID or no value for an empty Dataset. |
| `ListSnapshots` | Dataset identity | Returns published Snapshot records in declared ancestry order. |
| `GetSnapshot` | Snapshot ID | Returns the immutable Snapshot record. |
| `GetManifest` | Snapshot ID | Returns the complete validated logical Manifest. |
| `SetStable` | Published Snapshot ID | Sets the mutable `stable` name. |
| `RetainSnapshot` / `ReleaseSnapshot` | Snapshot ID | Adds or removes an explicit retention reference idempotently. |
| `ReclaimSnapshot` / `GetReclaimStatus` | Snapshot ID | Reclaims eligible state or returns durable reclaim progress. |
| `GetSample` / `GetBatch` | Snapshot ID, sample IDs, projection | Returns ordered samples and verified field values. |
| `Scan` | Read Plan, optional Cursor | Returns one bounded batch and the next Cursor. |
| `SaveProgress` / `Resume` | Read Plan, Cursor | Persists or loads plan-bound progress. |
| `Cancel` | Bound read or service | Stops admission and releases the active-read lease. |

`PrepareSnapshot` and `PublishSnapshot` share the same operation token and
validation rules. A complete publication may be implemented as one call that
performs preparation and completion internally; it MUST have the same durable
state transitions. Read operations are side-effect free except for lease and
cursor state required by their documented lifecycle.

## 5. Manifest and field values

A Manifest MUST validate before publication. Validation rejects an empty
sample set, duplicate sample IDs, duplicate field names, missing required
fields, invalid checksums, invalid parent references, and incomplete or
inconsistent partition metadata.

Within one Snapshot, `(sample_id, field_name)` has at most one value. A Schema
field can be required or optional. A required field removed by a child must be
represented by a tombstone or an explicitly compatible schema transition;
readers never silently combine incompatible schemas.

A Field value is exactly one of:

- **Inline**: bounded bytes plus the declared digest.
- **Chunk**: an opaque location, range or stream metadata as defined by the
  chunk subsystem, and the declared digest.
- **Tombstone**: an explicit deletion marker with no payload.

Readers verify the declared digest before returning bytes. External S3 or
Iceberg references are provenance and input metadata; they do not transfer
ownership of those systems' objects to Dataset.

## 6. Snapshot publication

Publication is an idempotent operation identified by an operation token. A
retry with the same token and equivalent parent and Manifest returns the same
Snapshot ID. A retry with divergent content is a conflict.

The authority MUST perform publication in this order:

1. Validate the Dataset, declared parent, operation token, and decoded Manifest.
2. Allocate or recover one candidate Snapshot ID for the operation.
3. Persist the prepared Snapshot record.
4. Persist every Manifest partition and the Snapshot binding.
5. Persist the published Snapshot record.
6. Advance `latest` atomically to the complete Snapshot.

A failure before step 5 leaves the candidate invisible and recoverable by the
same operation token. A reader of `latest` MUST see either the previous
complete Snapshot or the new complete Snapshot, never a prepared or partial
Manifest.

`stable` may be set only to an existing published Snapshot. Publishing a new
Snapshot never changes `stable` implicitly. A published Snapshot remains
addressable by ID until reclamation removes its metadata under Section 8.

## 7. Selection, plans, and cursors

A Selection is evaluated against exactly one Snapshot. Supported selection
forms are explicit sample IDs, prefix or range, and bounded metadata predicates;
compound forms preserve their declared order and duplicate policy. Invalid or
ambiguous selections are rejected before payload reads.

A Read Plan contains the resolved Snapshot ID, Selection, ordering, projection,
batch size, and optional deterministic shuffle parameters. The plan identity
includes all of those values. A plan cannot be resumed with a different
Snapshot, Selection, projection, ordering, or shuffle configuration.

A Cursor is either at the start or contains a plan-bound group and offset. The
caller acknowledges a batch by persisting the next Cursor. A failed or
unacknowledged batch may be replayed; a successful acknowledgement may not be
skipped or applied to another plan.

Batch size, metadata bytes, prefetch, in-flight requests, and shuffle memory
are bounded. The authority rejects a plan that exceeds configured limits before
reading payload bytes.

## 8. Read behavior

`GetSample`, `GetBatch`, and bounded Scan first resolve Manifest metadata,
selection, projection, and ancestry, then fetch only the requested values.
Requested sample and projection order is preserved unless the plan explicitly
requests another ordering. Unknown samples, invalid projections, checksum
mismatches, and malformed cursors are errors.

Each admitted read increments the active-read lease for its Dataset and
Snapshot. Normal completion, cancellation, and explicit close release the
lease. A bounded inactivity TTL expires a disconnected lease; the expiry is a
GC safety policy, not a publication or cursor transition.

HTTP and native reads use the same authority semantics. Transport framing,
serialization, and topology discovery may differ, but the resolved Snapshot,
logical samples, field values, checksums, cursor result, and error class are
equivalent.

## 9. Retention and reclamation

`retain` and `release` apply to an immutable Snapshot ID. The latest Snapshot,
stable Snapshot, retained Snapshots, parent-reachable history, and Snapshots
with active-read leases are protected from reclamation.

Reclamation is idempotent. It removes only unreachable Dataset-owned metadata
and payload references. A payload shared by two retained or readable
Snapshots remains available until its final Dataset owner is unreachable.
External S3, Iceberg, or other source references are never deleted by Dataset
reclamation.

A reclaim status record reports the Snapshot, pending or completed state, and
reclaimed-count progress. Repeated requests and authority restarts preserve
that status and do not double-delete a payload.

## 10. HTTP and native surfaces

The HTTP surface is the portable wire contract. It exposes Dataset creation and
opening, latest and Snapshot inspection, publication and preparation, Manifest
retrieval, retention, stable selection, reclamation status, bounded reads, read
plans, progress, resume, and cancellation.

The native surface exposes the same operations through typed calls. Every
operation identifies the Dataset explicitly or through a bound service and
carries a Snapshot ID where the operation is Snapshot-scoped.

A configured HTTP bearer credential protects all Dataset routes. Deployments
that need separate management and data policies may enforce that distinction at
the service boundary without changing Dataset semantics.

## 11. Errors and compatibility

Errors preserve their logical class across surfaces:

- invalid input, Manifest, selection, or cursor → client-invalid;
- missing Dataset, Snapshot, or Manifest → not-found;
- duplicate operation, parent race, protected Snapshot, or stale cursor →
  conflict;
- checksum or corrupt durable state → integrity/unavailable;
- cancelled read or expired admission → cancelled;
- delivery-window or configured bound exceeded → backpressure/resource
  exhausted;
- transient storage or service failure → unavailable and retryable only when
  the operation is documented idempotent.

Wire records are versioned. Unknown optional fields may be ignored, but a
reader MUST reject an unsupported version, missing required field, invalid
checksum, or incompatible state transition. New surfaces must preserve the
logical behavior in this specification before adding implementation-specific
extensions.

## 12. Normative invariants

- **DATA-S1 — Snapshot identity:** every operation resolves one immutable
  Snapshot ID.
- **DATA-S2 — Complete visibility:** `latest` exposes only a published,
  complete Manifest.
- **DATA-S3 — Explicit ancestry:** only declared parent links provide
  inheritance.
- **DATA-S4 — Manifest uniqueness:** one Snapshot has one valid value per
  `(sample_id, field_name)`.
- **DATA-S5 — Opaque location:** Dataset never parses or manufactures chunk
  locations.
- **DATA-S6 — Plan binding:** a Cursor is valid only for its exact Read Plan.
- **DATA-S7 — Bounded delivery:** configured memory, batch, and in-flight
  limits are enforced before payload admission.
- **DATA-S8 — Lease protection:** active reads protect all reachable metadata
  and payloads until release or expiry.
- **DATA-S9 — Ownership-safe GC:** reclamation deletes only unowned Dataset
  data and never external source objects.
- **DATA-S10 — Surface parity:** HTTP and native surfaces preserve the same
  logical results and error classes.
