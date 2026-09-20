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
   components with maximum levels and total encoded bytes; accept the advertised
   separator and legacy unit separator at the REST boundary.
2. Store an ordered parent/name mapping to NamespaceId and a separate authority
   containing the canonical identifier, authority epoch, lifecycle, and bounded
   properties. Validate mapping CatalogId, NamespaceId, and epoch against the
   authority on load and list.
3. Implement list, create, load, exists, property update, and drop endpoints from
   the backed-up OpenAPI. Namespace rename is unsupported and unadvertised.
4. Enforce R177's property contract: 256 entries, 1 KiB key, 8 KiB value, 64 KiB
   encoded authority, UTF-8 without NUL. Apply removals and updates atomically;
   duplicate keys across both sets return 422.
5. List direct children only. Scan mappings with bounded over-fetch, validate
   targets in bounded batches, omit stale mappings, and encode catalog, parent,
   parameters, and last scanned key into an authenticated opaque continuation
   token. Concurrent mutations have page-relative rather than global-snapshot
   visibility.
6. Drop CASes the authority from `Ready` to `Dropping`, which fences namespace and
   table creation. It then performs bounded first-entry probes in both child index
   ranges. A non-empty result restores `Ready`; an empty result tombstones the
   authority and removes the mapping through a recoverable operation record.
7. Persist idempotency identity, request digest, phase, and result for create,
   property update, and drop so another Access Server can resume after response
   loss. Repair stale mappings asynchronously with bounded work.

## Dependencies

- Depends on R177 and R178 for active context, key/value envelope, request identity,
  and error mapping.
- Produces NamespaceId, name mapping, authority epoch, lifecycle fence, and listing
  contracts consumed by R181 and R182.
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
- Given official REST clients invoking every declared namespace endpoint, when
  success, not-found, conflict, not-empty, and pagination cases execute, assert
  status and error payloads match the OpenAPI. Invariant: NS-I2. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
