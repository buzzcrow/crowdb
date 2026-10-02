<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R202: chunkdb — Key partition model and storage ownership

Status: Deferred by user for architecture review. The temporary default is
12 fixed hash ranges; this change is not the final partition design.

#### Problem

ChunkDB assigns chunk key hashes in the 16-bit space to service instances.
The previous 1024-range default creates 1024 group-0 binding records even
with one instance, without a measured scale or balancing requirement.
These service ownership ranges are distinct from the KV store/group mapping
used by `ChunkStore` and from chunk-kv's storage range catalog. Treating them
as interchangeable obscures where metadata lives, whether a partition owns
a tree, and what must move when a partition changes.

The [range binding design](../design/chunkdb/design-crowdb-chunkdb-range-binding.md)
needs a revised contract covering both direct Paxos KV and chunk-kv storage.
Current production `ChunkStore` writes through `CrowdbKvClient` to a routed
store/group, with the startup fallback mapping all keys to store 0/group 0.
This document does not claim that a chunk-kv-backed ChunkDB metadata path is
already wired. Chunk payload placement on DiskIO is a separate authority.

#### Solution

Keep 12 ranges as a temporary empty-table initialization baseline. Determine
the final model through user review rather than selecting an unproven topology.

- **I1 — Explicit ownership.** Document separately chunk key service routing,
  metadata storage routing, tree ownership and payload disk placement.
- **I2 — Complete coverage.** Every supported key maps to exactly one current
  partition; boundary definitions cannot leave holes or overlapping authority.
- **I3 — Safe change.** Service reassignment, storage transfer, split/merge and
  partition-count conversion have distinct fenced cutover protocols. Old
  bindings are not overwritten in place to invent a new layout.
- **I4 — Durable routing.** Restart, stale caches and uncertain cutover outcomes
  cannot lose acknowledged metadata or admit two unfenced writers.
- **I5 — Justified scale.** Partition count and representation follow measured
  metadata, routing, maintenance and migration costs, not instance count alone.

Work items:

1. Audit `ChunkStore`, `TaskStore`, ChunkDB `routing`, the KV server domain
   monitor, client `ChunkdbRangeStrategy` and chunk-kv range catalogs. Publish
   the supported metadata backend paths and tree/group ownership for each.
2. Compare fixed hash ranges with ordered key ranges, including a small
   single-instance bootstrap, explicit configured counts and dynamic split/merge.
   Decide whether service and storage ranges align or have an explicit mapping.
3. Define generation-fenced routing and migration ownership for direct Paxos
   KV and chunk-kv. Keep chunk payload movement separate from metadata routing.
   Compose existing storage transfer primitives where appropriate; do not
   assume changing a service binding moves a tree.
4. Define an explicit upgrade from existing 1024-range tables to the selected
   model. Preserve existing tables until that protocol is available; the
   temporary 12-range default only initializes new tables. Both binding writers
   reject incompatible populated boundaries instead of creating mixed layouts.
5. Measure binding record sizes, initialization/WAL replication, cache loading,
   lookup cost, owner balancing and migration state overhead for realistic
   single-instance and multi-instance deployments. Record supported limits.

#### Dependencies

- R103 owns ChunkDB instance range migration; revise its contract alongside
  this requirement once the ownership model is selected.
- Existing Paxos KV and chunk-kv range split/transfer are candidate storage
  primitives. R144's merge remains deferred; do not promise merge until its
  implementation or another reviewed mechanism exists.
- R201 addresses MemTable write/flush correctness independently. Smaller
  binding batches and skipped tests do not resolve that race.

#### Acceptance

- Given each supported metadata backend, allocate and update a chunk;
  assert the documented service owner, metadata destination, tree owner and
  disk placement match observed routing (I1). Integration test.
- Given minimum, maximum and boundary chunk hashes, route under the selected
  bootstrap layout; assert complete non-overlapping coverage and deterministic
  ownership (I2). Unit test.
- Given instance join/leave and a storage range transfer, exercise their
  distinct cutovers; assert acknowledged metadata survives and writes follow
  the selected authority without requiring unjustified payload moves (I1–I4).
  Integration test.
- Given an existing 1024-range table, attempt temporary 12-range publication;
  assert rejection preserves all records. Then execute the reviewed upgrade;
  assert no gaps, mixed generations or lost records (I2–I4). Integration test.
- Given crashes and stale routing during cutover, restart and retry;
  assert one fenced write authority and visibility of acknowledged records
  across every supported backend (I3, I4). Integration test.
- Given representative scales, measure startup, routing, memory and migration
  costs; assert the chosen count and supported limits meet the user-approved
  budget and record the measurements (I5). Integration test.

#### Open Questions

- Fixed hash partitions or ordered key ranges? Fixed ranges simplify routing;
  ordered ranges may align with storage split/transfer but require different
  skew and key-boundary handling.
- One storage tree per ChunkDB partition, or explicit many-to-one routing?
  Alignment can simplify migration; separate routing can share resources but
  needs independent authority and transfer contracts.
- Which deployments use direct Paxos KV versus chunk-kv for chunk metadata?
  Define supported configuration and bootstrap before promising equal behavior.
- Is 12 retained as a default, configurable, or replaced by dynamic sizing?
  Decide against measured scale and migration granularity.

Run `pixi run cargo test -p crowdb-kv-client --test chunkdb_partition_test`,
`pixi run cargo test -p crowdb-kv-server --test domain_monitor_test`,
`pixi run cargo test -p crowdb-chunkdb --test routing_test`,
`pixi run rs-fmt-check`, and
`pixi run cargo clippy -p crowdb-kv-client -p crowdb-kv-server -p crowdb-chunkdb --all-targets -- -D warnings`
for the implemented scope, plus reviewed migration suites through `pixi run`.
