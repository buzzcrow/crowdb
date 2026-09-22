<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R181: access server / Iceberg — Table metadata and lifecycle

## Problem

An Iceberg table is not a mutable object value. Its stable identity, current name,
selected metadata generation, immutable metadata JSON, format version, and
lifecycle must stay coherent across list, load, exists, rename, and drop. Name
mappings and multi-key rename steps can become stale after crashes, while large
metadata cannot be copied into `TableHead` or decoded without bounds.

R177 separates CROWDB TableId from Iceberg `table-uuid`, resolves recoverable
rename/drop, and requires v1, v2, and v3. R179 supplies namespace fences and R180
supplies immutable metadata files and projections.

## Solution

- **TABLE-I1 — Stable table:** TableId and Iceberg `table-uuid` do not change on
  rename and are never interchangeable.
- **TABLE-I2 — Selected metadata:** `TableHead` selects one immutable metadata
  generation and digest; the selected standard JSON contains complete table state.
- **TABLE-I3 — Name consistency:** load and list accept a mapping only when its
  TableId, NamespaceId, canonical name, name epoch, and lifecycle match `TableHead`.
- **TABLE-I4 — Version fidelity:** v1, v2, and v3 metadata are validated and served
  without dropping unknown optional fields or violating version-specific rules.
- **TABLE-I5 — Logical lifecycle:** rename and drop change visibility through
  bounded durable state machines and never synchronously move or delete files.

1. Add `table/id.rs`, `key.rs`, `record.rs`, `metadata.rs`, `repository.rs`,
   `lifecycle.rs`, and `wire.rs`. Store ordered namespace/name mappings separately
   from bounded `TableHead` values.
2. `TableHead` stores TableId, NamespaceId, canonical table name, name epoch,
   lifecycle, metadata generation, current metadata FileId/location/digest, format
   version, and operation fence. It stores no metadata JSON, snapshot graph,
   manifests, or data-file children.
3. Parse and validate all mandatory table metadata, schema/type, partition,
   sorting, snapshot, reference, statistics, encryption-key metadata, and
   serialization rules for format v1, v2, and v3. Preserve the original immutable
   JSON for full REST and FileIO responses; projections cannot re-encode authority.
4. Implement list, load, exists, rename, and drop. Support `snapshot-loading-mode`
   `ALL` and `REFS` from one selected generation. Bind ETag and conditional loads to
   TableId, generation, and metadata digest. Table listing uses R179's distinction
   between absent and empty page tokens, complete bounded-spool responses, and
   pre-response resource-exhaustion errors.
5. Rename, including a move across namespaces, reserves the destination mapping,
   advances `TableHead` name epoch and canonical identifier by CAS, and tombstones
   the source through a durable operation record. Source and destination namespace
   lifecycle fences follow R179's reserve-before-admit protocol. Destination
   admission CAS occurs after its reservation is durable and before publication;
   an unresolved reservation blocks destination drop. Reconciliation resolves the
   head publication outcome before removing a reservation. Repeated lifecycle
   reads alone do not fence a cross-key move.
6. The old name is never an alias. A known old-name cache may later produce an
   authorization-filtered hint under R185, but the repository returns not-found
   once the head selects the new name. List filters every stale reservation or
   mapping using bounded validation.
7. Drop CASes the head into a tombstoned lifecycle and removes name visibility.
   `purgeRequested=false` leaves files retained; `purgeRequested=true` schedules an
   R183 proof task. Neither path traverses snapshots in request latency.
8. Reject register-table and every unadvertised endpoint. A location can enter
   table authority only through the native create/commit flow in R182.

## Dependencies

- Depends on R177, R178, R179, and R180.
- Produces TableId, TableHead generation, name epoch, lifecycle, metadata validator,
  and selected-generation load contract for R182 through R185.
- R182 owns create, staged create, and metadata updates. Tests here may install
  valid fixture heads through a test utility but may not define a second publisher.
- R183 owns purge and orphan reclamation. Drop remains a correct logical operation
  while physical cleanup is unavailable.

## Acceptance

- Given valid and invalid v1, v2, and v3 metadata with version-specific schemas,
  types, snapshots, row lineage, delete representations, and serialization, when
  parsed and loaded, assert valid bytes are preserved and every mandatory violation
  fails closed. Invariant: TABLE-I4. Unit test.
- Given `ALL` and `REFS` loads plus conditional ETags, when one selected metadata
  generation is served, assert each response is derived from that generation and a
  later commit cannot mix fields into it. Invariant: TABLE-I2. E2E test.
- Given same-namespace and cross-namespace rename crashes at every transition, when
  reconciliation and concurrent list/load run, assert one canonical name resolves,
  the old name is not an alias, and TableId, table UUID, and file locations do not
  change. Invariants: TABLE-I1, TABLE-I3, and TABLE-I5. Integration test.
- Given concurrent destination drop and rename-in with delayed head-CAS responses,
  when recovery runs, assert the destination cannot tombstone while publication is
  possible and no source or destination cleanup deletes a recreated mapping.
  Invariants: TABLE-I3 and TABLE-I5. Integration test.
- Given stale mappings, reservations, tombstones, and valid entries over multiple
  pages, when list and exists run, assert only head-qualified tables are exposed and
  work per page remains bounded. Invariant: TABLE-I3. Integration test.
- Given logical drop with and without purge requested plus response loss, when the
  request retries, assert name visibility disappears exactly once, no foreground
  snapshot traversal occurs, and purge only creates a durable R183 task. Invariant:
  TABLE-I5. E2E test.
- Given register-table and other unadvertised operations, when clients call them,
  assert the standard unsupported response is returned and no mapping, head, or
  file authority changes. Invariant: TABLE-I2. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
