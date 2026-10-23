<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access Server Simplification Plan

Upstream: [Dataset SDK](../backlog/R220-dataset-fat-client-plan.md),
[Access design](../design/access-server/design-crowdb-access-server.md).

Goal: remove coordination from independent requests where the business contract
allows interruption or retry, and retain only justified durable authority.
This is a persistent cleanup inventory requested by the user; remove completed
items after recording their verified outcome in the owning design.

## Agreed Dataset deletion contract

- Mark deletion durably, reject new requests, then reclaim after a persisted
  grace deadline. No Access Server/client acknowledgements or reader registry.
- Requests admitted before the mark may continue during the grace period.
  Reclamation may interrupt unusually long reads; return an error, never a
  successful truncated response or a different snapshot.
- Default grace: five minutes. It is a delay, not a promise that a running job
  can issue new requests forever. Retries/restarts must not reset the deadline.
- Retention, live snapshot ownership, publication fences, physical allocation
  identity, in-flight I/O and caller-owned buffers remain distinct protections.
- A read checks authoritative availability at admission. A stale cache is not
  sufficient to admit a new read after deletion. Read paths perform no snapshot
  activity writes, heartbeats or asynchronous release writes.

## Execution

- [ ] **Dataset durable read sessions**: `DatasetSessions::{start,next,resume,
  acknowledge,close}` persists bindings, selected IDs, issued tokens, activity
  deadlines and cursor changes. `next` writes even when replaying an already
  issued batch; streaming checks session state repeatedly. Determine which
  callers need server-persisted restart/ack semantics and whether the ordinary
  path can use a snapshot-bound client cursor. Keep explicit acknowledged
  progress separate from fetching. Files: `lib/crowdb-access-dataset/src/read_session/`,
  `app/crowdb-access-server/src/dataset/api/{session.rs,batch.rs}`.
- [ ] **Dataset preparation and scratch journals**: Composition/Split evaluation
  persists work pages, input revisions, candidate links, cleanup cursors and
  source activity. Separate unpublished scratch that may expire from committed
  visibility and shared payload ownership. Measure KV mutations per page and
  evaluate coarser checkpoints/expiry before removing recovery facts. Files:
  `lib/crowdb-access-dataset/src/{composition,split,inspection}/`.
- [ ] **Dataset upload shared session updates**: inspect part progress and owner
  vectors, registry page updates, registration generations and permanent retired
  owner lists for contention between independent parts/owners. Preserve digest,
  durable payload publication and shared-owner safety; evaluate independent part
  records and delayed orphan cleanup. Files:
  `lib/crowdb-access-dataset/src/upload/{ownership.rs,retirement.rs,registry.rs}`
  and upload part/completion implementations.
- [ ] **Iceberg catalog-wide multipart credits**: every reservation/release
  changes catalog-wide `sessions`, `reserved_bytes`, and `pending` state through
  `MultipartAdmission`. Independent uploads share a durable coordination point.
  Evaluate local bounded admission plus coarse distributed quotas, with explicit
  allowance for quota overshoot; do not weaken memory limits. Files:
  `lib/crowdb-access-iceberg/src/file/multipart_credits.rs`,
  `src/file/multipart_credits/settle.rs`, `src/file/multipart_admission.rs`.
- [ ] **Iceberg parent namespace admission**: namespace mutations and table
  lifecycle operations CAS the parent's `pending_operation` and
  `mutation_revision`, persist before/after payloads and journal phases, then
  release the marker. Independent child creations can help/wait on each other.
  Evaluate immutable namespace identities plus deletion marks/grace periods;
  retain same-name uniqueness and reject publication into retired identities.
  Files: `lib/crowdb-access-iceberg/src/namespace/admission.rs`,
  `src/table/lifecycle/admission.rs`, their journals/recovery modules.
- [ ] **Iceberg publication and GC fencing**: distinguish expected table-head
  CAS conflicts from extra persisted operation phases, payload copies, admission
  markers and GC `operation_fence` writes. Audit ordinary commit versus create,
  rename, delete and recovery separately. Simplify only after counting RPCs and
  identifying the exact guarantee being removed. Files:
  `lib/crowdb-access-iceberg/src/commit/`, `src/gc/{admission.rs,fence.rs,worker/}`.
- [ ] **S3 and Iceberg multipart per-upload contention**: part publication and
  completion use session generations and CAS. Check whether independent parts
  rewrite shared session state and whether completion/recovery duplicates
  persistent work. Keep immutable selected-part identity, integrity and
  lost-reply recovery. Files:
  `lib/crowdb-access-s3/src/metadata/multipart_repository/`,
  `lib/crowdb-access-iceberg/src/file/multipart_repository/`.
- [ ] **Retry hygiene across authority loops**: classify CAS failure as stale
  base, business precondition failure or uncertain transport outcome. Where
  retry is valid, use returned observed state and recompute; remove redundant
  GETs and no-op writes. A transport retry must retain mutation identity while
  a new logical mutation must get a new identity. Files: Dataset read-session,
  publication/upload loops, Iceberg journals and S3 multipart repositories.

## Inspection coverage and exclusions

- Inspected route ownership and coordination sites in
  `app/crowdb-access-server/src/` and production sources of
  `lib/crowdb-access-{dataset,s3,iceberg,multipart}/src/`, including shared credentials,
  admission, streaming, Dataset lifecycle, S3 metadata and Iceberg catalogs.
  This inventory identifies candidates; it is not a measured proof that every
  listed write is unnecessary.
- No direct `Mutex`/`RwLock` usages found in those five production source trees.
  Dependencies and FFI may contain locks; audit them only when profiling points
  there. Atomic resource counters are not automatically removal candidates.
- S3 ordinary object PUT/DELETE already use unconditional point mutations;
  ordinary GET has no persistent reader registration. Bucket-name deletion CAS,
  explicitly conditional writes and multipart completion have different scopes.
- Iceberg `FileReader` verifies physical directory ownership and block digests
  without registering each reader. Preserve stale/reused-location rejection.
- Credentials, immutable content publication, explicit retention, shared payload
  owners, authorization and physical allocation identity are not expendable
  merely because they persist state or use CAS.

## Verification for each cleanup

- Record the old guarantee, accepted replacement semantics, conflict scope,
  persistent GET/write counts and reader/request lifetime before implementation.
- Unit/integration: independent concurrent operations, deletion races, retries,
  restart, partial results, retained buffers and shared references.
- E2E: existing HTTP/native Dataset parity and mixed-size workload; use matched
  source/builds. For other domains run the affected existing protocol suites.
- Run affected tests and Rust fmt/clippy through `pixi run`. Do not increase
  retry bounds, weaken integrity assertions or call workload reduction a fix.
