<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R231: Dataset — Public REST API and complete payload lifecycle

Problem:

- The root contract is [Dataset design](../design/access-server/dataset/design-crowdb-access-dataset.md)
  and its [normative specification](../design/access-server/dataset/spec-crowdb-dataset.md).
  A remote application must upload its own bytes, publish a Snapshot, and read
  it using HTTP alone, without constructing internal chunk locations.
- `app/crowdb-access-server/src/dataset.rs` exposes action routes using bincode
  and textual errors. `prepare` calls publication; progress operations share
  the scan handler; cancellation stops the shared read service. These are not
  sufficient public preparation, acknowledgement, and per-reader contracts.
- `lib/crowdb-access-dataset/src/client.rs` depends on the authority crate and
  its storage dependencies. `chunk_store.rs` provides a reader interface but
  defaults to an unavailable reader; the public listener has no payload upload
  resource. Existing inline-data tests do not prove a complete remote SDK path.
- The listener disables HTTP keep-alive and collects request bodies before
  checking size on several routes. A pooled client alone cannot establish a
  representative sustained-throughput baseline over this path.

Solution:

1. Define and check in a language-neutral OpenAPI contract alongside the Dataset
   specification. The new resource prefix is `/api/dataset/v1`; preserve the
   existing `/v1/dataset/*` binary routes during migration. Metadata uses JSON,
   identifiers are opaque strings, byte metadata uses explicit base64, and
   payload bodies use binary HTTP streams. Do not expose persisted bincode
   records as the new wire contract. Reject unsupported versions and missing
   required fields; tolerate unknown optional fields.
2. Route by namespace and Dataset name, with correct escaping and identity
   validation. Retain the current `dataset-ns` root namespace restriction;
   introducing namespace administration is out of scope. Expose create/open,
   latest/stable resolution, bounded snapshot listing and Manifest retrieval,
   prepare/publish/operation status, retention/release, reclaim/status, sample
   and batch reads, scan/read plans, progress/resume, and cancellation.
   Publish exact methods, paths, schemas, status codes, pagination tokens, and
   limits in OpenAPI before client implementation. Existing bound listeners
   must reject identities they do not serve, never silently substitute one.
3. Extend `DatasetAuthority` and the server adapter so Prepare leaves a durable
   candidate invisible to latest. Publish validates complete durable payloads
   and Manifest before visibility. Require a caller-supplied operation token;
   retries with identical content return the same candidate, divergent content
   conflicts, and operation status resolves a lost publication response.
4. Add Dataset-scoped upload sessions with immutable numbered parts, declared
   length/digest, status, completion, and abort. Identical part retries succeed;
   changed bytes for an accepted part conflict. Completion verifies all parts
   and yields a server-issued opaque payload reference. Bind the reference to
   its Dataset; reject foreign, incomplete, or aborted references at publication.
   Wire real chunk writing and reading through existing `crowdb-chunk-client`
   and Dataset storage adapters. Add a dedicated Dataset chunk type across
   protocol encoding and allocation; preserve existing numeric tags and reject
   unsupported types explicitly. Never reuse S3/IcebergTable or add a generic
   Repo type. Bound memory independently of total payload
   size; no whole-file buffering or JSON byte arrays for large fields.
5. Protect completed uploads until publication or configured upload expiry.
   Serialize publication versus abort/expiry through durable ownership state;
   reclaim only unreferenced Dataset-owned bytes. Restart restores sessions
   and accepted parts. Reuse existing shared-payload and active-read protection.
6. Bind each read plan and opaque progress token to one immutable Snapshot,
   selection, projection, ordering, and shuffle. Fetching a batch does not
   acknowledge application consumption. SaveProgress acknowledges the supplied
   next cursor without reading another batch; Resume starts at acknowledged
   progress. Cancel/close affects only its read handle, releases its lease,
   and preserves other readers. Disconnection uses bounded lease expiry.
7. Apply existing bearer authentication to every new resource. Return structured
   errors with stable logical code, request ID, retryability, and bounded
   details; distinguish invalid, unauthorized, not-found, conflict, integrity,
   cancelled, exhausted, and unavailable. Enforce body, Manifest, result,
   concurrency, and time limits while receiving, before unbounded allocation.
   Map these resources onto the same authority as native calls.
8. Support HTTP/1.1 connection reuse with bounded idle time, connection count,
   admission and graceful shutdown. Separate idle connections from executing
   request capacity so idle pools cannot consume every work slot. Stream large
   bodies under byte budgets through completion/disconnect; retain delivery
   credits and payload protection until the transport no longer needs them.
   Keep control JSON bounded and specify binary batch framing, lengths and
   integrity boundaries in the public contract. HTTP/2 is a later measured
   option, not a prerequisite for a correct pooled HTTP/1.1 baseline.

Dependencies:

- Existing Dataset authority, publication, retention, Chunk-KV and chunk I/O.
  Historical completion labels do not substitute for the new acceptance cases.
- R232 consumes this API; its packaging work can start independently, but a
  mock-only client cannot satisfy Dataset SDK release acceptance.
- R233 uses upload recovery; R220 may optimize delivery only after HTTP parity.
  Legacy routes remain usable while the new API is incomplete.

Acceptance:

1. Given JSON golden requests and legacy binary fixtures -> call both surfaces
   and send unknown optional fields, missing fields, and unsupported versions ->
   assert documented compatibility and explicit rejection. Wire invariant. Integration test.
2. Given two Dataset identities and escaped names -> create, inspect, paginate,
   retain/release, and reclaim through every documented resource -> assert
   isolation, bounded pages, and authority-equivalent outcomes. Scope invariant. Integration test.
3. Given a prepared operation -> inspect latest, publish, lose the response,
   restart, and retry identical/divergent input -> assert preparation is invisible,
   one published Snapshot and divergent conflict. Publication invariant. Integration test.
4. Given a payload larger than the configured memory window -> upload parts,
   retry parts, complete, publish, and read projected bytes over real HTTP/chunks ->
   assert exact bytes/digests, bounded memory, and rejection of incomplete,
   foreign, or divergent parts/references. Payload invariant. E2E test.
   Given Dataset allocation requests -> round-trip protocol records and restart
   storage -> assert the dedicated type survives and existing type tags remain
   unchanged. Chunk identity invariant. Integration test.
5. Given interrupted uploads and a shared published payload -> restart and race
   completion/publication with abort/expiry/reclaim -> assert accepted parts
   recover and reachable payloads survive. Ownership invariant. Integration test.
6. Given two readers and advancing latest -> fetch without acknowledgement,
   save progress, resume, cancel one reader, then expire a disconnected lease ->
   assert replay before acknowledgement, no skipped batch, fixed Snapshot,
   plan mismatch rejection, isolated cancellation and lease cleanup. Progress invariant. Integration test.
7. Given absent credentials, malformed or oversized streamed input, exhausted
   capacity, corrupted bytes and transient failures -> call the resources ->
   assert stable error classes, bounded allocation and unchanged authority state
   on rejected mutation. Admission invariant. Integration test.
8. Given persistent connections, idle pools and a slow/disconnected payload
   consumer -> issue sequential and concurrent requests, then shut down ->
   assert connection reuse, progress within configured capacity, bounded bytes,
   protected live payloads and eventual credit/lease release. Transport lifetime
   invariant. Integration test.

Verification commands:

- `pixi run -- cargo test -p crowdb-access-dataset`
- `pixi run clean-env && pixi run test-access-server`
- `pixi run clean-env && pixi run test-e2e-dataset`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
