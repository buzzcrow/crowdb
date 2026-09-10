<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Server (R143)

This implementation design refines
[`../backlog/R143-chunk-kv-server.md`](../backlog/R143-chunk-kv-server.md).
The service owns discovery, catalog, lease, routing, and process lifecycle;
`crowdb-chunk-kv` remains the sole owner of partition trees and WAL streams.

## 1. Catalog

The authoritative range map is an immutable generation head referencing
checksummed ordered pages. Entries contain exact half-open bounds, partition
identity, owner identity and endpoint, owner epoch, lifecycle, artifact
references, and optional transition identity. The first bound is the empty
binary key, adjacent endpoints are equal, and the final end is unbounded.
Readers validate the complete generation before replacing their last valid
view. Publishers write pages first and the head last, then reread an ambiguous
head outcome.

## 2. Domain Monitor

An `EnsureDomainMonitor` request persists a descriptor naming a compiled
domain driver and versioned policy. Every group-0 replica supervises tasks from
the descriptor set; only the current leader publishes decisions. Identical
requests are no-ops and conflicting driver versions fail closed. Failed
control-plane reads abort a tick and preserve its previous observation.

Chunk-KV, chunkdb, and diskdb request their domains after creating a group-0
client and before becoming ready. The generic KV server depends only on the
driver boundary and protocol types, not service libraries.

## 3. Registration and Serving Grants

Chunk-KV instances heartbeat endpoint, capacity, load, hosted partitions, and
recovery state. A raw observation scan retains expired entries for failure
detection; normal discovery remains TTL-filtered.

Authority comes from a monitor-issued aggregate serving grant, not a
heartbeat. The grant binds owner, lease sequence, catalog generation, expiry,
and a canonical digest of sorted `(partition_id, owner_epoch)` assignments.
The owner computes a conservative local monotonic deadline from the received
remaining wall-clock duration and self-fences before expiry. Replacement waits
through the old grant plus skew unless explicit fencing is proven.

## 4. Transfer, Split, and Balance

Transfer and split are persisted, idempotent catalog transitions. Targets open
artifacts as `Prepared`, validate and recover them, and report readiness before
a new generation and grant activate them. Graceful transfer first obtains an
old-owner fence; dead-owner recovery waits for lease exclusion. Neither path
copies tree or stream chunks.

Balancing targets at least `live_owners * target_partitions_per_owner`
non-empty ranges. It splits the largest eligible range near its live-byte
median. Placement minimizes partition-count difference first and durable bytes
second, preserves an owner unless improvement crosses the configured
threshold, and obeys cooldown and per-owner transition concurrency bounds.

## 5. RPC and Routing

Each data request carries map revision, partition ID, owner epoch, and, for
mutations, the original client request ID. The contacted server validates its
catalog view and current serving grant before calling R142. It never proxies.
A stale request returns structured `NotMyRange` with the latest known revision
and optional owner hint without WAL I/O.

The network surface preserves point operations, conditional results, journal
positions, minimum-position reads, ordered seek, and bounded directional scan.
Continuation tokens bind direction, last key, partition ID, owner epoch, and
map revision; topology changes return refresh-required. Cancellation before
R142 admission creates no record. Cancellation after admission drops only the
response while the partition completes the durable outcome.

## 6. Process Lifecycle

Startup loads config, initializes logging and metrics, ensures the monitor,
registers the instance, validates the catalog, opens assignments as
`Prepared`, recovers them, reports readiness, and serves only with a matching
grant. Shutdown stops admission, drains bounded work, checkpoints where
possible, reports draining, and relinquishes or lets grants expire.

Health, metrics, management output, and heartbeats derive from the same hosted
partition snapshots. One process may host zero or many independent ranges.

## Open Issues

- R142 does not yet expose production tree/stream construction, prepared-child
  activation, online split building, or native ordered cursor operations.
- Existing group-0 storage has blind puts; the injected immutable page/head
  publisher implements page-first ordering and ambiguous reread, but its
  production KV adapter and retained-generation reclamation remain.
- The generic monitor registration and leader-gated staged tick boundaries are
  implemented. Durable group-0 descriptor storage, task restart/backoff, leader
  change wiring, and chunkdb/diskdb startup migration remain.
- Point-operation protocol models and an in-process R142 handler are complete.
  Production FlatBuffers schemas, `crowdb-rpc` transport, and native ordered
  seek/scan execution remain.
- The persisted transfer record/reducer and pure balance policy are complete.
  Group-0 transition CAS, target recovery workers, split orchestration, and
  catalog cutover wiring remain.
- Lease timing and balance policy have deterministic boundary tests; deployment
  sizing, metrics export, and real-process failure evidence remain.
