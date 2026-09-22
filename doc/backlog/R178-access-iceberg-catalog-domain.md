<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R178: access server / Iceberg — Catalog domain and service foundation

## Problem

Iceberg REST treats the catalog as configured service context and does not define
catalog create, rename, or clear endpoints. CROWDB still needs a stable root for
all namespace, table, file, retry, and reclamation state. Reading one unqualified
root, deriving identity from a display name, or synchronously deleting descendants
would create a hot key, make rename move data, and make clear unbounded.

R177 defines one active catalog, a stable CatalogId, no tenant or warehouse in the
first milestone, and a lease-plus-grace clear boundary. This requirement builds the
library and service foundation on which all other Iceberg requirements depend.
The architecture boundary is [Native Iceberg Storage](../design/access-server/iceberge/design-crowdb-iceberg.md).

## Solution

- **CAT-I1 — Single active root:** one `ActiveCatalogRecord` is the only authority
  selecting the visible CatalogId and activation epoch.
- **CAT-I2 — Stable domain:** display-name and configuration changes never change
  CatalogId or descendant key prefixes.
- **CAT-I3 — Root publication:** initialize and clear publish visibility with one
  compare-exchange; candidates not selected by it remain unreachable.
- **CAT-I4 — Admission fence:** every admitted request carries immutable CatalogId
  and activation epoch context; clear completion follows R177's lease-plus-grace
  rule.
- **CAT-I5 — Safe management:** initialize, rename, and clear are authenticated
  CROWDB management operations, not Iceberg REST endpoints.

1. Create feature-gated `lib/crowdb-access-iceberg` modules for `catalog`, `key`,
   `record`, `operation`, `wire`, and `error`. Keep REST wire models, Iceberg domain
   models, and versioned FlatBuffer storage records separate. Unknown key or value
   versions, malformed IDs, and oversized values fail closed.
2. Reserve the binary `ICE\0`, key-version, and scope prefix. Separate the system
   range for root, management operations/audit, and REST idempotency bindings from
   half-open CatalogId resource ranges. Use big-endian fixed fields and binary-safe
   length-delimited variable fields. System bindings carry bounded identifiers and
   digests, not old resource response bodies; catalog-scoped records own those.
3. Store bounded `ActiveCatalogRecord` and `CatalogAuthority` values. The authority
   holds display name, name/config generations, lifecycle, and the v1/v2/v3
   parse/read/create/write and upgrade capability matrix, but no child collection.
4. Implement idempotent initialize, epoch-checked display rename, and clear as
   recoverable state machines in `catalog/repository.rs`. The root carries the
   management operation identity and phase. Clear first CASes the old root into
   maintenance, creates a new empty authority, then publishes its pointer by CAS
   while retaining maintenance. Old lifecycle marking is reconciliation, not the
   publication point. Before releasing maintenance, durably record publication and
   the completed grace proof in the system operation. A later root replacement
   cannot discard an unresolved operation reference; another server helps recover
   it. Initialize uses the same recoverable publication receipt rule.
5. Add durable management request and audit records. Clear requires a distinct
   privilege, exact active epoch, explicit confirmation material bound into the
   request digest, and an operator-visible result. A retry with the same identity
   and digest returns the original result even after subsequent clears. Authenticate
   and verify the digest before replay; only a new operation checks the current
   epoch. Bound retention, record size, and admission capacity independently; never
   evict unfinished operations or unexpired results to admit new work. Management
   identities contain a validated issuance time and have an explicit retry window;
   expired identities are rejected rather than reused after result cleanup.
6. Add Iceberg server configuration and lifecycle wiring in
   `app/crowdb-access-server/src/iceberg/`. Startup connects routed Chunk-KV and
   chunk clients, validates the active root, and only then opens the separate
   external Iceberg HTTP listener. Shutdown stops admission before draining
   mutations and background work.
7. Implement the baseline `GET /v1/config`. Absent or empty `warehouse` selects the
   active catalog; non-empty warehouse returns `NoSuchWarehouse`. The response
   advertises only endpoints landed by later requirements and the exact v1/v2/v3
   capability matrix. It does not derive a REST prefix from the display name.
   Publish `idempotency-key-lifetime` only when the retry contract below is active.
8. Do not introduce a global lock. Before R185, request admission reads the active
   root authoritatively. R185 may add a bounded lease-qualified cache without
   changing this contract.
9. Persist clear timing limits before entering maintenance. Let L be the maximum
   root lease, Q the maximum request lifetime after admission, D the maximum
   delegated credential lifetime, and S the clock-skew allowance. A conservative
   completion deadline is maintenance observation time + L + Q + D + S; before
   R185, L is zero. Persist that observation time only after confirming the durable
   maintenance CAS. If a crash precedes timestamp persistence, recovery starts a
   fresh conservative grace after observing maintenance; a pre-CAS preparation
   timestamp cannot shorten the window. Lease age begins before reading the root,
   so delayed replies cannot extend it. Without a root cache, the request's Q
   deadline starts before its authoritative root read; a delayed reply past that
   deadline cannot admit old-context work. Requests and credentials inherit absolute
   deadlines; no renewal or chained
   delegation extends old-context access. Enforce expiry at response/stream and
   FileIO boundaries, not just HTTP admission. Resume uses persisted bounds, never
   shorter current configuration. Clock uncertainty fails closed. Ordinary
   authoritative admission returns 503 during maintenance; leased admission may
   continue only until expiry. Open the new domain after the grace proof, then
   finalize the management result. Durable GC pins remain a separate R183 fence.
10. Implement the shared REST retry boundary in `operation` and `wire` now, for R179
    to consume. Accept the OpenAPI's optional UUIDv7 `Idempotency-Key`; validate
    issuance time, clock skew, and the advertised reuse window. Atomically bind a
    key in the system scope to authenticated principal, route/action, canonical
    request digest, CatalogId, and activation epoch before domain mutation. Keep it
    at least for the advertised lifetime from first submission plus grace. Reuse
    with different input fails without mutation; another principal cannot inspect
    the result. Check active context before replay: a retired binding returns a
    non-disclosing conflict and cannot be rebound to a new catalog. Persist and
    replay 200/201/204 and deterministic terminal 4xx, including 409; never finalize
    5xx. Unknown mutation outcomes retain recoverable state and resume before any
    new attempt. Without a client key, allocate an internal recovery identity but
    do not promise deduplication across separate HTTP requests. Retain backend
    request identities and exact mutation input through outcome resolution; the
    bounded Chunk-KV retry cache alone is not the application retry ledger.
11. Supply management authorization, audit persistence, and the common Iceberg
    authentication/admission boundary with this foundation. Existing S3 SigV4
    wiring is not an Iceberg bearer/OAuth implementation. R184 extends and verifies
    this boundary rather than supplying security for already-exposed endpoints.

## Dependencies

- Depends on R177, routed Chunk-KV compare-exchange and scans, stable request
  identity, and Access Server configuration. Produces Iceberg authentication,
  management authorization, and audit integration before exposing its endpoints.
- Produces CatalogId, activation epoch, key/value envelope, service lifecycle, and
  capability types consumed by R179 through R185.
- Old-catalog physical cleanup is R183. Before R183 lands, retired domains remain
  unreachable but are not erased.
- Cache fanout is R185. Before it lands, authoritative root reads preserve correct
  behavior at higher latency.

## Acceptance

- Given an empty root and concurrent initialize requests, when all instances publish
  candidates, assert exactly one active pointer wins and same-identity retries
  return that result. Invariants: CAT-I1 and CAT-I3. Integration test.
- Given a ready catalog, when rename succeeds or loses a concurrent CAS, assert the
  winning display name and name epoch are deterministic while CatalogId and every
  descendant prefix remain unchanged. Invariant: CAT-I2. Integration test.
- Given crashes immediately before and after clear's root CAS, when reconciliation
  resumes on another instance, assert the old or new CatalogId is uniquely active,
  respectively, and no candidate is partially visible. Invariant: CAT-I3. E2E test.
- Given clear, a disconnected lease holder, and delayed root replies, when
  maintenance and publication occur, assert authoritative admission stops, only
  unexpired leases admit old work, and the new domain opens only after all persisted
   deadlines. Delay maintenance CAS beyond its preparation timestamp and crash
   before persisting its observation time; neither may shorten the grace.
   Restart with changed limits must not shorten the grace; expired
  streams and credentials cannot expose old resources. Invariant: CAT-I4. E2E test.
- Given crashes between root CAS, operation persistence, and maintenance release,
  when recovery and a second clear run, assert the unresolved receipt is preserved
  and an authenticated retry of either clear returns its original result without
  another replacement. Invariants: CAT-I3 and CAT-I5. Integration test.
- Given UUIDv7 keys, absent keys, mismatched principals/digests, expired keys, and
  exhausted ledger capacity, when namespace mutations and retries execute, assert
  bounded admission, advertised retention, terminal 4xx replay, recoverable 5xx,
  and no retired-resource replay or key rebinding after clear. Requests without a
  key must not claim HTTP deduplication. Invariants: CAT-I4 and CAT-I5. E2E test.
- Given a fresh installation and unauthorized REST or management callers, when
  listeners start and requests arrive, assert authentication and audit are ready
  before exposure and no unauthorized mutation occurs. Invariant: CAT-I5. E2E test.
- Given missing confirmation, stale epoch, insufficient privilege, response loss,
  or a reused identity with a different digest, when clear is requested, assert no
  unauthorized second mutation occurs and the durable audit result is exact.
  Invariant: CAT-I5. Integration test.
- Given absent, empty, and non-empty warehouse parameters, when an official client
  calls `/v1/config`, assert the sole catalog is selected for the first two and the
  last receives `NoSuchWarehouse`; the advertised endpoints equal landed support.
  Invariant: CAT-I1. E2E test.
- Given unknown key/value versions, zero IDs, mismatched IDs, oversized records, and
  epoch overflow, when codecs and repositories process them, assert they fail closed
  without mutation. Invariant: CAT-I3. Unit test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
