<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Routed Client (R145)

This implementation design refines
[`../backlog/R145-chunk-kv-routed-client.md`](../backlog/R145-chunk-kv-routed-client.md).
The client is the only application-facing router for R143 and makes no
cross-partition atomicity or snapshot claim.

## 1. Catalog Cache and Direct Routing

A catalog source reads the immutable R143 head and every referenced page. The
client validates the complete binary keyspace before atomically replacing an
immutable cache. Point routing binary-searches that cache and sends directly to
the entry's owner endpoint. A cold cache requires group 0; a valid warm cache
may continue trying its recorded owner while refresh is unavailable.

`NotMyRange`, stale ownership, and connection loss invalidate the affected
route and trigger bounded refresh within the same total deadline. A server is
never used as a proxy. Connection management is bounded by owner and endpoint.

## 2. Request Identity and Retry

Each handle obtains a random 128-bit client instance ID from the operating
system and owns an atomic nonzero sequence starting at one. A mutation reserves
one sequence before routing and retains the resulting identity, operation, and
digest across every transport and topology retry. Reconnection cannot change
identity. Exhaustion closes new mutation allocation so outstanding operations
can drain before handle replacement.

Applications may explicitly resubmit a persisted request identity after their
process restarts. Reusing it for a different operation reaches R142's digest
check and returns `RequestConflict` unchanged.

## 3. Point and Ordered Operations

The public surface preserves get, put, delete, put-if-absent,
compare-exchange, conditional delete, ceiling, higher, floor, lower, and
bounded directional scan. Every response retains applied revision, journal
position, condition result, retry hint, and typed R143 failure. A minimum
journal position provides explicit read-after-write ordering.

One deadline covers discovery, refresh, connection, RPC, and backoff. Only
transport loss and typed transient results retry. The logical request ID and
the caller's owner epoch never change silently inside one send attempt.

## 4. Composed Operations

Multi-get retains input indices, including duplicate keys, groups by partition
and owner, dispatches bounded parallel group RPCs, and restores one result per
input position. A failed partition does not erase successful results. Each
partition group observes its own applied view, not a global snapshot.

Batch mutation reserves one durable request identity per input operation,
groups by partition, preserves relative input order inside each group, and
dispatches bounded parallel group RPCs. Per-operation results return in input
order. Successful groups remain committed when another group fails; retry is
limited to unresolved operations with their original identities. No atomicity
or ordering exists across partition groups.

Multi-partition scan pins one catalog generation and intersects the requested
interval with its partitions in direction order. It enforces global item and
byte limits and a bound on prefetched pages. Its continuation carries original
bounds, direction, last emitted key, and observed generation. After topology
change, refresh replans only the strict remaining interval after the last key
for forward order or before it for reverse order.

## 5. Bounds and Consistency

Empty multi-get, batch, and scan succeed without discovery. Malformed bounds,
oversized groups, response-byte excess, and identity exhaustion fail before
unbounded allocation. Prefix listing converts a prefix to its minimal binary
successor and uses bounded scan.

Point operations have R142 partition semantics. Multi-get and batch are
partition-local compositions. Multi-partition scan is globally ordered but not
a snapshot across partitions; mutations to already consumed keys need not be
observed. TTL, watch, range delete, multi-key transactions, and R144 merge
handling remain outside this requirement.

## Open Issues

- R143's production FlatBuffers/crowdb-rpc transport and group-0 catalog adapter
  are prerequisites for real-process routing coverage.
- Native ordered R142 cursors are required for seek and partition scan
  execution; the client must not emulate them from point reads.
- R143's internal multi-get and batch group handlers must validate every key
  before read or WAL admission.
- Real-process restart, response-loss, owner-death, split, balance, and group-0
  interruption fixtures remain dependent on the R143 production process.
