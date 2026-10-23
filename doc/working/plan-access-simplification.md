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

- [~] **Verify and deliver the cleanup**: run affected Dataset/S3/native/HTTP
  regressions, format/lint, rebuild the same server source, and run the unchanged
  mixed-size workload with restart acceptance. Retain measured limits; do not
  infer throughput from mutation counts. Commit the coherent implementation and
  record acceptance here before removing this task.

## Decisions applied to the inventory

- Dataset sessions retain explicit durable acknowledgement and remote close.
  Normal fetch/replay is read-only; start/ack prepares the next token. Only
  start/ack/resume extends admission. Legacy unprepared sessions upgrade once.
  See [Dataset read flow](../design/access-server/dataset/design-crowdb-access-dataset.md#102-read-flow).
- Composition/Split retain draft-scoped recovery checkpoints and cleanup fences
  for exact lost-reply recovery and late-writer exclusion. Source activity leases
  are absent; receipt helping avoids redundant CAS. No reader-shared journal was
  found. See [control lifecycle](../design/access-server/dataset/design-dataset-control.md).
- Dataset uploads no longer register in shared tail/pages or persist a scan
  cursor. Expiry directly scans upload authorities with local continuation.
  Permanent owner revocation protects other live snapshots; bounded part indexes
  remain to avoid one metadata point read per part during listing/completion.
  See [upload flow](../design/access-server/dataset/design-crowdb-access-dataset.md#101-upload-and-publication-flow).
- Iceberg catalog-wide credits are legacy recovery only. New streamed sessions
  already use independent session limits; no foreground reserve/release was
  found. Keep legacy recovery so old outstanding records can settle.
- **User decision, 2026-10-24**: nonempty namespace deletion must fail. Keep the
  parent admission protocol where required by creation/rename/drop recovery;
  do not replace it with cascading deletion. Ordinary table creation already
  avoids a parent authority mutation. See [Iceberg authority](../design/access-server/iceberge/design-crowdb-iceberg.md#2-authority-model).
- Ordinary Iceberg commits already use one head CAS; the legacy multi-phase
  publisher is not the normal HTTP path. Preserve GC/publication fencing because
  it prevents deleting referenced files, rather than only interrupting a reader.
- Iceberg streamed parts already publish independently by part number. S3 keeps
  upload-local accounting and the completion/part-pointer fence; independent
  uploads do not contend on one global record. Confirmed S3 CAS conflicts now
  use returned observed bytes, and part retry retains its latest loaded state.
  See [S3 data flow](../design/access-server/s3/design-crowdb-access-s3.md).
- Dataset session and upload-owner retry loops use returned observed state.
  Confirmed stale-base retries and uncertain transport reconciliation remain
  distinct. Explicit revision preconditions still return conflict; retrying an
  application mutation against a new base is not universally valid.

## Files and focused checks

- Dataset: `read_session/`, `upload/`, `composition/`, `split/state.rs`; Access
  Server Dataset cleanup wiring and publication; Dataset design/spec/OpenAPI.
- S3: `metadata/store.rs`, `metadata/multipart_repository{.rs,/parts.rs}`;
  owning design. Iceberg design corrections document existing foreground paths.
- Cost regressions: `session_cost_test`, `draft_cost_test`,
  `upload_registry_test`; lifecycle regressions retain restart, shared-owner,
  lost-response, invalid-content and cancellation assertions.
- Integration: affected Access Server/native/S3 suites; Iceberg namespace drop
  and admission tests retain the explicitly selected nonempty-delete contract.
- Gates: Rust fmt and workspace clippy through Pixi. Full workload and restart
  use `tools/sdk/dataset_workload.py` with its unchanged defaults and same-source
  image; physical multi-host acceptance remains in its independent plan.

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
