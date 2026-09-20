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
2. Reserve the binary `ICE\0`, key-version, and scope prefix. Store the system root
   outside CatalogId ranges and every descendant record inside a half-open
   CatalogId range. Use big-endian fixed fields and binary-safe length-delimited
   variable fields.
3. Store bounded `ActiveCatalogRecord` and `CatalogAuthority` values. The authority
   holds display name, name/config generations, lifecycle, and the v1/v2/v3
   parse/read/create/write and upgrade capability matrix, but no child collection.
4. Implement idempotent initialize, epoch-checked display rename, and clear as
   recoverable state machines in `catalog/repository.rs`. Clear creates a new empty
   authority and publishes it by root CAS; old lifecycle marking is reconciliation,
   not the commit point.
5. Add durable management request and audit records. Clear requires a distinct
   privilege, exact active epoch, explicit confirmation material bound into the
   request digest, and an operator-visible result. A retry with the same identity
   and digest returns the original result.
6. Add Iceberg server configuration and lifecycle wiring in
   `app/crowdb-access-server/src/iceberg/`. Startup connects routed Chunk-KV and
   chunk clients, validates the active root, and only then opens the separate
   external Iceberg HTTP listener. Shutdown stops admission before draining
   mutations and background work.
7. Implement the baseline `GET /v1/config`. Absent or empty `warehouse` selects the
   active catalog; non-empty warehouse returns `NoSuchWarehouse`. The response
   advertises only endpoints landed by later requirements and the exact v1/v2/v3
   capability matrix. It does not derive a REST prefix from the display name.
8. Do not introduce a global lock. Before R185, request admission reads the active
   root authoritatively. R185 may add a bounded lease-qualified cache without
   changing this contract.

## Dependencies

- Depends on R177, routed Chunk-KV compare-exchange and scans, stable request
  identity, Access Server configuration, authentication, and audit facilities.
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
- Given a clear operation and old admitted work, when the new pointer is published,
  assert new admission cannot acquire old context and clear does not report complete
  until the bounded lease and request/delegated-access grace expires. Invariant:
  CAT-I4. E2E test.
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
