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

## 3. HTTP and FileIO surfaces

The REST Catalog is the portable control surface. It exposes only capabilities
CROWDB implements with compliant Iceberg semantics.

Iceberg FileIO uses reserved S3-shaped locations so existing Iceberg clients can
address immutable metadata and data files. The shape is a compatibility
contract, not delegation to the general S3 authority. File publication,
immutability, authorization, and deletion remain under Iceberg control.

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
