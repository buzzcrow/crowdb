<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R179: access server / Iceberg — Namespace authority and REST operations

## Problem

Iceberg namespaces are multipart identifiers with parent-scoped listing,
properties, idempotent mutation, and empty-only deletion. Storing a nested child
array in one catalog value is unbounded, while using names as authority makes stale
mappings, deletion races, and future table moves ambiguous.

R177 resolves multipart support, property limits, stale-mapping filtering, and the
drop fence. R178 supplies the active catalog and storage envelope. This requirement
turns those decisions into a stable NamespaceId authority and the standard REST
surface.
The architecture boundary is [Native Iceberg Storage](../design/access-server/iceberge/design-crowdb-iceberg.md).

## Solution

- **NS-I1 — Stable identity:** NamespaceId survives property changes and is never
  derived from its identifier.
- **NS-I2 — Ordered index:** name mappings support bounded parent-scoped scans but
  are not authority.
- **NS-I3 — Empty drop:** a namespace cannot be tombstoned while a valid child
  namespace or table can still be created or resolved beneath it.
- **NS-I4 — Bounded namespace:** identifier, properties, pages, retries, and stale
  filtering all obey explicit limits.

1. Add `namespace/id.rs`, `key.rs`, `record.rs`, `repository.rs`, and
   `wire.rs`. Encode multipart identifiers as a sequence of length-delimited UTF-8
   components with at most 32 levels and 4096 total encoded bytes, including
   two-byte component lengths; each component must also fit the name-index key.
   Advertise the standard URL-encoded unit separator `%1F` and accept it at the
   REST boundary after exactly one URL decode. Empty components and embedded NUL
   or unit separators in JSON components are invalid.
2. Store an ordered parent/name mapping to NamespaceId and a separate authority
   containing the canonical identifier, name epoch, property revision, admission
   fence, lifecycle, and bounded properties. Validate mapping CatalogId,
   parent NamespaceId, NamespaceId, and name epoch against the authority on load
   and list. Properties advance only their revision; lifecycle transitions advance
   the admission fence. Neither invalidates current mappings or existing children.
   Use the storage revision for CAS of the complete authority. Recreating a dropped
   name allocates a new NamespaceId; old operations cannot attach to it. Creating
   a multipart namespace requires its immediate parent to exist; a missing parent
   is a 400 invalid request and does not implicitly create ancestors. The catalog
   root is the parent context for top-level names.
3. Implement list, create, load, exists, property update, and drop endpoints from
   the backed-up OpenAPI. Namespace rename is unsupported and unadvertised.
4. Enforce R177's property contract: 256 entries, 1 KiB key, 8 KiB value, 64 KiB
   encoded authority, UTF-8 without NUL. Apply removals and updates atomically;
   duplicate keys across both sets return 422.
5. List direct children only. Scan mappings with bounded over-fetch, validate
   targets in bounded batches, omit stale mappings, and encode catalog, parent,
   parameters, and last scanned key into an authenticated opaque continuation
   token. Concurrent mutations have page-relative rather than global-snapshot
   visibility. Distinguish absent `pageToken` from an empty token: absent requires
   all results and a null next token; empty starts paginated mode. Build a complete
   unpaginated response in a bounded temporary spool, with independent byte, item,
   scan-work, time, and concurrency caps, before sending success headers. Exhaustion
   returns the OpenAPI's 503 error and releases the spool, never a truncated 200 or
   an invented continuation. Stream the completed spool with a bounded window.
   Paged mode may return an empty page with a non-null token when stale filtering
   exhausts its scan budget. Corruption is an error, not proof of staleness or
   emptiness. Tokens bind the stable parent identity as well as its spelling.
6. Implement child admission and drop using durable name reservations and single-key
   CAS, without a process lock or a multi-key transaction. A creator first installs
   an exclusive reservation in the parent/name index with put-if-absent and a
   recoverable operation identity. Only after that write is durable may it CAS the
   parent in `Ready` at the expected admission fence; a preliminary read alone is
   insufficient. This CAS validates admission without changing the name epoch or
   property revision. Reserve-before-admit ordering ensures that drop either sees
   the reservation or wins the parent CAS and prevents publication. Cross-namespace
   table moves use this protocol for their destination too.
   Drop CASes `Ready` to `Dropping` before scanning both child index ranges.
   Published children cause a return to `Ready` and a not-empty result. Unresolved
   reservations block an empty proof; bounded recovery settles them, with a
   retryable 503 if the work budget expires. A publisher admitted before the fence
   may finish because its reservation prevents tombstoning. Operation-phase CAS
   arbitrates recovery versus publication: abort may win only before publishing;
   an unknown publication result must be resolved before reservation cleanup.
   The reservation remains until a valid child mapping replaces it or an abort is
   durable and no delayed publisher can succeed. Backend request identity and
   publication evidence must survive outcome resolution. Helpers never remove a
   reservation merely because a process or lease expired.
   Empty proof scans through stale entries in bounded durable steps and requires
   reaching both range ends; the first stale entry or an exhausted budget is not
   emptiness. Tombstoning CAS checks the same drop operation and admission fence.
   A reservation installed after the scan cannot publish because its parent CAS
   sees `Dropping` or the tombstone. Mapping cleanup uses conditional deletion so
   it cannot remove a later recreation. All probes use authoritative storage, not
   cached or lagging views. No child collection is stored in the parent authority.
7. Persist idempotency identity, request digest, phase, and result for create,
   property update, and drop so another Access Server can resume after response
   loss. Use R178's shared HTTP identity, retention, terminal-error replay, and
   retired-domain rules from the first exposed endpoint. Repair stale mappings
   asynchronously with bounded work and conditional deletion.

## Dependencies

- Depends on R177 and R178 for active context, key/value envelope, request identity,
  and error mapping.
- Produces NamespaceId, name mapping, name epoch, property revision, admission
  reservation/fence, and listing contracts consumed by R181 and R182.
- Table-child probes become effective when R181 lands. Until then that range is
  empty by construction; the key range is reserved here.
- R185 may cache mappings and authorities but cannot alter list or drop semantics.

## Acceptance

- Given identifiers at every level and byte boundary plus malformed separators,
  when they are encoded and decoded through REST and storage codecs, assert valid
  identifiers round-trip and invalid ones fail before mutation. Invariant: NS-I4.
  Unit test.
- Given concurrent creates with the same identifier and response-loss retries, when
  operations finish on different instances, assert one NamespaceId is visible and
  identical request identities return one result. Invariants: NS-I1 and NS-I2.
  Integration test.
- Given properties at entry and byte limits plus overlapping removal/update keys,
  when property update runs, assert the complete valid change is atomic and the
  overlap returns 422 without changing the authority. Invariant: NS-I4. E2E test.
- Given stale, corrupt, and current mappings across multiple scan pages, when a
  client lists a parent with continuation tokens, assert only direct current
  children are emitted, work per page is bounded, and tokens cannot cross catalog
  or parameter contexts. Invariants: NS-I2 and NS-I4. Integration test.
- Given concurrent child creation and namespace drop, when the drop fence is
  installed at any crash point, assert either the child is valid and drop returns
  not-empty or the namespace tombstones and no child becomes visible beneath it.
  Invariant: NS-I3. Integration test.
- Given reservations before and after the drop scan, delayed parent CAS responses,
  and crashes at every publication/abort phase, when recovery runs on another
  server, assert no parent tombstones with a publishable child, no uncertain
  reservation is removed, and recreated names reject old operations. Include
  namespace creation, table creation, and rename-in. Invariant: NS-I3. Integration test.
- Given stale entries before a live child, corruption, or unresolved reservations,
  when drop exhausts one bounded probe, assert it does not claim emptiness and
  resumes safely or reports the appropriate error. Invariant: NS-I3. Integration test.
- Given property updates and a not-empty drop, when authority revisions and fences
  advance, assert mappings and existing children remain resolvable and concurrent
  changes are not lost. Invariants: NS-I1 and NS-I2. Integration test.
- Given absent, empty, and continuing page tokens plus spool/scan limits, when
  listing through an official client, assert an unpaginated 200 is complete with a
  null token, exhaustion returns 503 before success headers, paged empty results
  can continue, and all temporary resources are released. Invariant: NS-I4. E2E test.
- Given a missing multipart parent or a dropped and recreated parent, when create
  or token resume runs, assert no implicit ancestor creation or cross-identity
  attachment occurs. Invariants: NS-I1 and NS-I2. Integration test.
- Given official REST clients invoking every declared namespace endpoint, when
  success, not-found, conflict, not-empty, and pagination cases execute, assert
  status and error payloads match the OpenAPI. Invariant: NS-I2. E2E test.

## Open Questions

- Which principal may create, update and drop namespaces? The existing REST
  foundation has read, management and clear credentials but no writer role.
  Reusing management/clear credentials avoids new configuration but grants daily
  Iceberg clients administrative authority. A separate writer credential isolates
  namespace writes from catalog management and clear, at the cost of another
  credential. Reader credentials remain read-only under either choice.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
