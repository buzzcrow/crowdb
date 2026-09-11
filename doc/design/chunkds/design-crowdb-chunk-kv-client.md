<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Routed Client

`crowdb-chunk-kv-client` is the application-facing router for the chunk-backed
KV service. It validates and caches the immutable range catalog, sends each
single-partition operation directly to the current owner, and composes bounded
multi-partition operations without claiming distributed atomicity or a global
snapshot.

## Catalog and Routing

`CatalogSource` supplies a catalog head and all referenced pages. The client
decodes them as one ordered `CatalogMap`, reusing the protocol validation that
requires complete, non-overlapping coverage of the binary keyspace. A valid
generation is published through `ArcSwap`; invalid or regressing refreshes do
not replace a warm map.

Point lookup binary-searches partition starts. No data server acts as a
forwarding proxy. A cold client must load group 0 before sending a nonempty
operation. A warm client may continue trying the cached owner while refresh is
unavailable, but the server's owner epoch and serving lease remain the final
authority.

Each operation has one total deadline covering discovery, refresh, transport,
and backoff. `NotMyRange`, `RefreshRequired`, lease loss, and connection loss
cause bounded rerouting. Overload, recovery, and write-stall responses retry
without inventing a new owner. The route-refresh and total-attempt budgets are
independent and explicit in `ClientConfig`.

## Request Identity

Every client handle generates a random nonzero 128-bit instance ID from the
operating system. An atomic counter allocates nonzero sequences beginning at
one and closes permanently before wraparound. A logical mutation allocates its
`ClientRequestId` once, before routing, and retains it across every retry.

`execute_with_identity` accepts an application-persisted identity and operation
for explicit recovery after process restart. The server compares the operation
digest against its retained result, so changing an operation under a reused
identity remains a typed `RequestConflict`. Connections, endpoints, and RPC
ports never contribute to identity.

## Direct Operations

The client wraps get, put, delete, put-if-absent, compare-exchange,
conditional delete, ceiling, higher, floor, and lower. It returns the complete
`ChunkKvResponse`, including journal position, applied or observed revision,
condition outcome, map revision, retry hint, and typed failure. Gets can carry
a minimum journal position for explicit read-after-write ordering.

Seek is an owner-local ordered operation. It is transported directly and is
not emulated with point reads. Directional scan likewise consumes native
partition scan pages and validates that every returned key is strictly ordered,
inside both requested and partition bounds, and consistent with the returned
continuation topology.

## Multi-Get and Batch Mutation

`multi_get` retains every input index, including duplicates, groups keys by
partition, dispatches groups with bounded parallelism, and restores one result
per input position. A failed group produces per-item typed errors without
discarding successful partitions. Each partition observes its own applied view;
the result is not a global snapshot.

`batch_mutate` allocates or accepts one stable request identity per operation.
It rejects reads and oversized input before dispatch, preserves relative input
order inside each partition group, and returns per-operation results in original
input order. Successful operations are removed from the retry set. Only
transient or transport-unknown operations retry, using their original identity.
There is no atomic commit or ordering promise across partition groups.

The protocol request types expose `validate_for_range` preflight methods.
Multi-get validation rejects the whole group if any key is outside the declared
partition. Batch validation additionally rejects empty groups, reads, and bad
request identities. Owners run this preflight before reads or WAL admission.

## Multi-Partition Scan

A scan validates its binary half-open bounds and optional continuation, pins a
catalog generation, intersects the interval with that generation, and visits
partitions in ascending or descending byte order. Execution is sequential, so
the number of buffered partition pages is one and therefore stays within the
configured page bound. Global item, caller byte, and client response-byte caps
apply across every partition.

The public continuation contains direction, original bounds, last emitted key,
and observed catalog generation. Pagination resumes strictly after that key for
forward scans and strictly before it for reverse scans. When a split or transfer
invalidates the plan, the client refreshes and rebuilds only the unconsumed
interval. This prevents repeated emitted keys and stable-key gaps across
topology replacement. It does not make the scan a cross-partition snapshot;
concurrent changes to already consumed keys need not appear.

An empty scan limit, empty multi-get, or empty batch succeeds without catalog
discovery. Malformed bounds, mismatched continuations, oversized requests or
responses, out-of-order server pages, and request-sequence exhaustion fail
explicitly.

## Resource Bounds and Transport Boundary

`ClientConfig` caps attempts, catalog refreshes, owner connections, in-flight
partition groups, batch items, response bytes, buffered scan pages, and the
total operation duration. The composition layer enforces group, retry, item,
page, and byte bounds. `ChunkKvTransport` is injected so protocol behavior is
testable independently of sockets; its production implementation owns the
bounded connection pool keyed by owner instance and endpoint.

Cancellation before server admission leaves no retained request. Cancellation
after admission may hide a committed result, so mutation callers persist and
resubmit the complete identity and operation when the outcome matters.

## Consistency and Non-Goals

Single-partition operations inherit the sequencer, durability, request-result,
and serving-fence semantics of `crowdb-chunk-kv`. Multi-get and batch are
partition-local compositions with partial success. Multi-partition scan is
globally ordered but not globally snapshot-consistent.

TTL, watches, range delete, atomic multi-key transactions, forwarding proxies,
and merge-specific continuation behavior are outside this client contract.

## Open Issues

- The production group-0 subscription and periodic-refresh adapter is not yet
  wired because the chunk KV server has no production catalog RPC surface.
- The production `crowdb-rpc` transport and bounded owner connection pool await
  the server's FlatBuffers handlers.
- Native ordered cursors, internal group handlers, and the real chunk KV server
  process are still required for real-process seek, scan, batch, and lifecycle
  coverage.
- Restart, response-loss, owner-death, split, balance, control-plane outage,
  graceful-drain, and merge-specific scenarios remain real-process follow-up.
