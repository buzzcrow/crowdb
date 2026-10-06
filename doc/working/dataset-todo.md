<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowDB Dataset TODO

Upstream design and contract:

- `/cjdata/cpp/workshop/crowdb/proposal-storage/dataset-crowdb-scope.md`
- `/cjdata/cpp/workshop/crowdb/proposal-storage/dataset-crowdb-design.md`
- `/cjdata/cpp/workshop/crowdb/proposal-storage/dataset-use-cases.md`
- `/cjdata/cpp/workshop/crowdb/proposal-storage/dataset-crowdb-plan.md`

Goal: track the remaining design and implementation gaps required for a working
Dataset core. R213–R226 now cover the completed authority, Manifest,
publication, read-plan, retention, and API parity foundations. The Dataset SDK
is intentionally deferred until the core Dataset path is usable.

Current implementation status: R213–R226 are complete. Durable per-snapshot
leases, ownership-aware reclamation, complete HTTP publication, and
parity/recovery coverage are in place; only the deferred R220 SDK remains.

The backlog items are ordered by dependency: publication recovery (R222),
retention and GC (R225), then complete API parity (R226). Small wording or
scope corrections belong in this TODO; implementation work belongs in the
linked backlog item.

## P0: publication and read correctness

- [x] **Atomic snapshot publication** (R222): Manifest partitions, binding, publication state, and head advancement are now ordered so `latest` exposes only a complete Snapshot. Files: `lib/crowdb-access-dataset/src/authority.rs`, `record.rs`, `publication.rs`.
- [x] **Publication retry and recovery** (R222): Prepared operations recover through their durable operation token; retries preserve one Snapshot ID and reject divergent requests.
- [x] **Safe retention and GC** (R225): Make snapshot reclamation require retention/release authority and protect latest/stable snapshots, retained parent chains, and active reads. `reclaim_snapshot_metadata` must not unconditionally delete a live snapshot. Files: `authority.rs`, `retention.rs`, `lease.rs`.
- [x] **Per-read GC protection** (R225): Track active Read Plans by Dataset and Snapshot, rather than using one global lease or one boolean for all reads. Protect the referenced payload until the read completes, is released, or expires under an explicit policy.

## P1: Snapshot and Manifest model

- [x] **Opaque locator contract** (R214): Store and pass the chunk location as an opaque location string according to the Dataset contract. Dataset code must not parse or infer chunk identity from the locator.
- [x] **Manifest completeness validation** (R222): Persisted Manifest bindings now carry partition count and digest, reject zero/incomplete partitions, validate parent availability, and reject divergent re-publication.

## P1: Read Plan, streaming, and resume


## P1: Public control and data surfaces

- [x] **Complete control API** (R226): Add the Dataset operations required by the contract: ListSnapshots, PrepareSnapshot, PublishSnapshot, OpenSnapshot, GetManifest, RetainSnapshot, and ReleaseSnapshot.
- [x] **Complete read API** (R226): Add stable operations for GetSample, GetBatch, bounded Scan, StartReadPlan, NextBatch, SaveProgress, and Resume. HTTP and native surfaces must use the same authority and return equivalent logical results.
- [x] **End-to-end HTTP publication** (R226): Make the HTTP client able to publish a complete validated Dataset snapshot, including Manifest persistence and publication completion, rather than publishing only an opaque reference.
- [x] **Status and error mapping** (R226): Map not-found, conflict, invalid Manifest, checksum, transient, cancellation, backpressure, and service-unavailable states to stable native and HTTP errors.

## P2: lifecycle and operational behavior

- [x] **Mutable names** (R225): Implement and validate `latest` and `stable` as mutable names that never replace the immutable Snapshot ID stored in Read Plans or cursors.
- [x] **Snapshot listing and inspection** (R226): Expose snapshot ancestry, publication state, Manifest completeness, retention state, and provenance for operators.
- [x] **Dataset-owned payload lifecycle** (R225): Track Dataset-owned chunk references separately from external source references. External source refs must not control Dataset payload GC.
- [x] **Shared payload reclamation** (R225): Reclaim a chunk only after no retained Snapshot and no active Read Plan references it, including inherited fields and shared locators.
- [x] **Authentication and authorization scope** (R226): The HTTP service applies its configured bearer token to every Dataset route, covering management, snapshot access, publication, retention, reclamation, and cancellation; deployments can place finer-grained policy at the service boundary.

## Verification gaps

- [x] **Atomic publication tests** (R222): Cover prepared visibility, snapshot-write crash recovery, duplicate operation tokens, head advancement, and declared parent ancestry.
- [x] **GC tests** (R225): Cover retained parent chains, latest/stable protection, active reads by Snapshot, lease expiry, shared chunks, external source refs, and idempotent deletion.
- [x] **HTTP/native parity tests** (R226): Complete publication and bounded reads run through HTTP and native surfaces, with stable status mapping, durable lease cleanup, and restart recovery coverage.

## Deferred

- [ ] **Dataset SDK** ([R220](../backlog/R220-dataset-fat-client-plan.md)): Start only after the core Dataset publication, payload read, Snapshot membership, bounded streaming, retention, and resume semantics are working and covered by the tests above. The SDK must adapt the stable core contract rather than define new storage semantics.

## Files to revisit

- `lib/crowdb-access-dataset/src/authority.rs`
- `lib/crowdb-access-dataset/src/manifest.rs`
- `lib/crowdb-access-dataset/src/planner.rs`
- `lib/crowdb-access-dataset/src/shuffle.rs`
- `lib/crowdb-access-dataset/src/transport.rs`
- `lib/crowdb-access-dataset/src/chunk_store.rs`
- `lib/crowdb-access-dataset/src/retention.rs`
- `lib/crowdb-access-dataset/src/cursor.rs`
- `lib/crowdb-access-dataset/src/wire.rs`
- `app/crowdb-access-server/src/dataset.rs`
- `lib/crowdb-access-dataset/tests/`
- `app/crowdb-access-server/tests/`
