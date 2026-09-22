<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R185: access server / Iceberg — Bounded cache and invalidation

## Status

Deferred until R178 through R184 stabilize identities, generations, digests,
lifecycles, clear fences, and the measured uncached correctness baseline. Cache is
not a dependency of the first correct Iceberg milestone.

## Problem

Authoritative reads on every request create an active-catalog hot key and repeat
name, head, metadata, manifest, and format parsing work. Independent caches would
multiply memory budgets and stale-data rules. Cross-server notification can reduce
staleness but cannot be authority because instances disconnect, register late, and
receive duplicated or reordered messages.

R177 resolves lease-plus-grace clear semantics and requires per-class limits chosen
by focused benchmarks. This requirement adds one Iceberg-owned cache manager while
preserving correct behavior when notifications or the complete cache are disabled.

## Solution

- **CACHE-I1 — Authority isolation:** a hit can avoid I/O but cannot create,
  publish, rename, drop, clear, recover, authorize, or reclaim a resource.
- **CACHE-I2 — Bounded ownership:** total and class bytes, entries, one-entry size,
  fills, queues, batches, fanout, retries, and retained owners have separate caps.
- **CACHE-I3 — Qualified identity:** mutable entries carry CatalogId plus epoch or
  generation; immutable entries carry stable identity plus generation/digest.
- **CACHE-I4 — Optional notification:** loss, duplication, reorder, late join, and
  unreachable receivers preserve behavior through expiry and authoritative refresh.
- **CACHE-I5 — Lock-free hit:** a hot hit takes no global or cross-class lock.
- **CACHE-I6 — Rename is not aliasing:** an old table name can retain a bounded
  tombstone hint but never resolves as a normal alias.

1. Add `cache/manager.rs`, `budget.rs`, `policy.rs`, `entry.rs`, `fill.rs`, and
   `metrics.rs`. Reserve a validated global byte budget and class budgets for active
   catalog, names, authorities, heads, negative/tombstone entries, parsed metadata,
   serialized responses, manifests, format structures, and file records. Oversized
   entries bypass cache.
2. Configure maximum entries, bytes, one-entry bytes, idle TTL, absolute lease,
   negative TTL, fill concurrency, and retained-owner accounting per class. Derive
   checked-in initial defaults from focused cold/hot and adversarial benchmarks; a
   missing, zero-invalid, or unbounded setting fails startup.
3. Treat active catalog, names, namespace authorities, and TableHeads as mutable.
   An expired entry must refresh authoritatively or fail closed. A mutation always
   validates current durable revision and cannot succeed from cache alone.
4. Cache metadata projections under CatalogId, TableId, generation, JSON digest,
   and projection version. Cache manifests and parsed Parquet/ORC/Avro/Puffin
   structures under FileId, digest, and parser version. Whole large files never
   enter this cache.
5. Add one bounded typed invalidation coalescer. After a durable mutation completes,
   coalesce transitions by resource to the newest generation or strongest state.
   Immutable files emit no event. Queue saturation drops optimization events rather
   than delaying the mutation.
6. Start one internal `crowdb-rpc` listener per Access Server and publish its
   endpoint through the Group-0 service registry. Use service-specific FlatBuffer
   messages, bounded control size, deadlines, connection/fanout concurrency, and
   retries. External Iceberg HTTP never accepts invalidation RPC.
7. Receivers validate the complete batch before applying any entry, then apply it
   idempotently. Older generations are ignored. Table rename converts an existing
   old-name entry to an authorization-neutral tombstone; only request-time current
   authorization may disclose the destination.
8. Integrate clear with R177: notification prompts eviction, but completion waits
   for R178's persisted maintenance deadline and admitted/delegated grace. Start
   lease age before the authoritative root read, never when a delayed reply arrives;
   maintenance prevents fresh leases, while an existing lease may admit old-context
   requests only until its original expiry. New-domain admission opens after the
   grace proof. Retired-catalog GC uses
   durable fences and never waits for physical cache eviction acknowledgements.
9. Expose per-class hit, miss, stale, fill, bypass, bytes, entries, eviction, expiry,
   rebuild, notification, fanout, drop, and refresh-failure metrics. Benchmark hit
   latency and contention for unrelated cache classes before enabling by default.

## Dependencies

- Depends on R177 through R184 and their final identities and measured baseline.
- Reuses Group-0 service discovery and `crowdb-rpc`; if registration is unavailable,
  notification is disabled and TTL-only correctness remains. Static instance lists
  are forbidden.
- Does not depend on R82 watch coalescing. This coalescer batches typed post-commit
  cache transitions, not KV watch keys.
- Canonical JSON and file bytes remain the fallback for every missing, corrupt,
  unknown-version, or evicted projection/cache entry.

## Acceptance

- Given mixed entries and fill pressure beyond every total and class limit, when
  admission, eviction, and bypass run concurrently, assert retained owners, bytes,
  entries, queues, and work stay within each configured cap. Invariant: CACHE-I2.
  Integration test.
- Given an expired active-catalog or TableHead entry while Chunk-KV is unreachable,
  when a request arrives, assert it fails closed and never reuses stale authority.
  Invariants: CACHE-I1 and CACHE-I4. Integration test.
- Given duplicate, reordered, dropped, oversized, and malformed heterogeneous
  invalidation batches, when receivers apply them, assert validation is atomic,
  current entries only advance, and TTL refresh converges missed entries. Invariants:
  CACHE-I3 and CACHE-I4. Integration test.
- Given a successful table rename and later authorized and unauthorized old-name
  requests, when a cached tombstone exists and then expires, assert it never loads
  the table, disclosure is authorization-filtered, and an uncached old name is
  ordinary not-found. Invariant: CACHE-I6. E2E test.
- Given valid and corrupt metadata/format projections, when cached loads execute,
  assert qualifiers prevent cross-generation reuse and every invalid entry falls
  back to canonical bytes without authority mutation. Invariants: CACHE-I1 and
  CACHE-I3. Integration test.
- Given a hot commit stream, unrelated mutations, queue saturation, disconnected
  instances, and a late server, when fanout runs, assert work remains bounded,
  newest generations converge, committed mutation latency is unaffected, and the
  late server loads authority before readiness. Invariant: CACHE-I4. E2E test.
- Given a disconnected old-root lease holder and delayed cache fills, when clear
  enters maintenance and publishes a new pointer, assert no lease extension and
  no old-context response after the persisted completion boundary, even across a
  recovering clear coordinator. Invariants: CACHE-I3 and CACHE-I4. E2E test.
- Given concurrent hits, fills, invalidations, expiry, and eviction across classes,
  when contention benchmarks run, assert hot lookup takes no global lock and its
  latency is independent of unrelated class activity. Invariant: CACHE-I5.
  Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo test -p crowdb-kv-client --all-targets`
- `pixi run -- cargo test -p crowdb-rpc-ffi --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
