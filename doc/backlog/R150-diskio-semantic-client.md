<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R150: diskio client — Routed Semantic Disk I/O

## Problem

`crowdb-diskio-client` currently wraps only FlatBuffer construction, RPC
submission, and response parsing. Every caller must still create and run an
`RpcServer`, discover DiskIO service instances, join those instances with the
hardware hierarchy, parse endpoints, create and attach `Connection` objects,
select a connection, refresh routes, and interpret low-level return codes.
Its public read, write, and fsync methods therefore require transport objects
and physical wire fields instead of expressing one disk operation.

This thin wrapper has caused the same routing and connection-lifetime logic to
be implemented independently in `crowdb-chunk-client` and
`crowdb-chunkdb::conversion`. In particular, a topology refresh can replace
still-valid connections with newly created ones. The C++ transport retains its
side of each connection until closure, so periodic refresh can accumulate
connections, exhaust transport resources, and make unrelated ChunkDB recovery
or chunk-KV restart fail. Different callers also classify the same DiskIO
result differently and must separately implement write-plus-fsync behavior.

The intended boundary is already described in
`doc/design/diskio/design-crowdb-diskio.md`: a Rust caller supplies a segment
address and operation, while the client resolves the owning DiskIO node and
executes the RPC. `doc/design/chunkio/design-crowdb-chunkio.md` additionally
requires immutable route publication and fixed connection groups, and
`doc/design/rpc/design-crowdb-rpc.md` defines the underlying healthy-connection
selection and reconnect behavior. The current implementation stops below that
boundary.

Concrete failures include a ChunkDB topology timer creating a new connection
to every DiskIO node each second, a chunk caller having to pass
`RpcServer`/`Connection` into a disk write, and two callers disagreeing about
whether a timeout, partial write, or missing disk is retryable.

## Solution

Make `crowdb-diskio-client` the complete routed semantic client for DiskIO.
Production callers identify the disk location, operation, data, durability,
priority, and deadline. The client owns discovery, endpoint resolution,
connection groups, RPC correlation, retry classification, and typed results.

1. Replace the public transport-shaped API with semantic read, write, and
   durability operations. A target contains the globally unique `DiskId`,
   zone, segment base and capacity, and a segment-relative byte range; the
   client validates all arithmetic and bounds before network admission. A
   write accepts caller-owned `Bytes` and an explicit durability policy:
   acknowledge after the write or acknowledge only after the owning disk has
   completed fsync. Keep an explicit disk fsync barrier for callers that batch
   several writes. Callers never provide `RpcServer`, `Connection`, endpoint,
   FlatBuffer fields, or response parsers.
2. Move the existing wire encoder and response decoder behind the semantic
   client as an internal transport. Preserve zero-copy ownership of write
   payloads through completion, exact read-length validation, related-write
   ordering by segment base, and typed distinctions among invalid input,
   unavailable topology, transport ambiguity, queue backpressure, permanent
   disk failure, partial write, and durability failure.
3. Let the client connect from group-0 management seeds or injected
   `ServiceRegistryClient` and `HardwareClient` handles. It joins live DiskIO
   registrations with hardware disk records to produce one complete immutable
   route generation mapping each `DiskId` to its rack, node, disk group,
   DiskIO instance, and endpoint. A missing owner, duplicate owner, mismatched
   node/disk-group relationship, malformed endpoint, or failed control-plane
   read rejects the new generation; the client never invents or partially
   publishes a route.
4. Own a fixed connection group per endpoint. Use the connection-pool and
   reconnect facilities in `crowdb-rpc` through a safe Rust FFI wrapper rather
   than implementing caller-local connection vectors. Healthy connections are
   selected without a caller lock; connection establishment and reconnect run
   outside the I/O hot path. Configuration bounds endpoints, normal and
   priority connections per endpoint, RPC workers, pending calls, send queue,
   reconnect backoff, and total operation deadline.
5. Make topology and connection generations coherent. Refresh builds a full
   replacement route generation off-path and reuses an existing connection
   group when endpoint identity is unchanged. An endpoint or instance change
   installs a new group atomically; in-flight operations retain the old group
   until completion. A late failure from an old generation cannot invalidate a
   replacement. Removed groups close their client-owned connections after the
   final in-flight reference retires, so repeated unchanged refresh has a
   constant connection count.
6. Provide one bounded retry policy over discovery, connection selection,
   reconnect, send, response, and optional fsync. Reads may retry transient
   transport outcomes. A write may retry only the exact same immutable bytes
   to the exact same allocated location, relying on the lifecycle layer's
   existing no-reuse window; it never retries a partial write internally.
   Fsync can be repeated after an ambiguous response. Permanent disk, bounds,
   alignment, and topology-consistency failures return immediately. Every
   retry preserves one total caller deadline and surfaces an ambiguous outcome
   when completion cannot be proven.
7. Preserve traffic isolation with named normal and priority lanes backed by
   separately bounded connection groups. Foreground chunk traffic uses the
   normal lane; recovery, repair, and conversion select their configured lane
   explicitly. Lane selection changes scheduling resources, not DiskIO address
   or durability semantics, and neither lane may create connections beyond its
   configured bound.
8. Publish client status and metrics for route generation and age, discovered
   disks/nodes/endpoints, connection-group size and health, connect/reconnect
   attempts, in-flight and queued operations, queue rejection, retries,
   ambiguous writes, latency by operation, and fsync latency. Status snapshots
   must not expose raw transport handles or block the request path.
9. Migrate `crowdb-chunk-client`, ChunkDB conversion and EC repair, test
   harnesses, and CLI/benchmark paths to the semantic API. Remove their
   endpoint parsing, DiskIO topology joins, connection refresh loops, and
   duplicated result-code translation. Native crowdb-tree integration receives
   an opaque retained route set produced by the DiskIO client; application
   code does not assemble `OwnedClientRoute` values.
10. Keep a narrow injected transport and topology boundary for unit tests.
    Direct wire-level methods may remain crate-private or under `test-util` for
    protocol tests, but they are not a second production API. MemDisk,
    connection failure injection, fake topology generations, and a real
    multi-process DiskIO stack must exercise the same public semantic client.
11. Update the DiskIO, ChunkIO, and ChunkDB permanent designs so the ownership
    boundary is explicit: DiskIO client owns node lookup and connection
    lifecycle; ChunkDB owns conversion/reconstruction policy; chunk callers own
    foreground object semantics; `crowdb-rpc` owns transport reconnect and
    healthy-connection selection.

## Dependencies

- Uses the group-0 service registry and hardware hierarchy supplied by
  `crowdb-kv-client`, plus DiskIO wire types in `crowdb-protocol` and the
  existing `crowdb-rpc` connection-pool/reconnect implementation.
- Overlaps R149 only at the generic RPC connection-index boundary. If R149
  lands first, R150 consumes its generation-safe shared pool. If R150 lands
  first, it adds the safe `crowdb-rpc-ffi` pool surface needed by DiskIO and
  R149 later adopts that surface for other transports without changing R150's
  public semantics.
- R143 and R145 may retain their current bounded DiskIO connection-reuse fix
  while this requirement is pending; R150 replaces that caller-local code and
  must preserve their benchmark and restart behavior.
- R83 recovery, R146 orphan sealing, and R148 sealed-chunk EC consume this
  client when implemented. None should introduce another direct DiskIO
  connection manager.

## Acceptance

- Given only management seeds and a valid group-0 topology, when a caller reads
  or writes a segment by `DiskId`, assert the client resolves the correct
  DiskIO node and endpoint and the caller never constructs an RPC transport or
  connection. Semantic-boundary invariant. Integration test.
- Given disks distributed across several nodes and disk groups, when topology
  discovery runs, assert every disk maps to its unique live owner with matching
  hardware identity; inject a missing owner, duplicate owner, node mismatch,
  malformed endpoint, and failed group-0 read and assert no partial generation
  is published. Authoritative-routing invariant. Unit test.
- Given repeated unchanged topology refreshes, when the refresh interval runs
  hundreds of times, assert each endpoint retains exactly its configured
  normal and priority connection counts and the DiskIO server's accepted/live
  connection counts remain bounded. Connection-reuse invariant. E2E test.
- Given an endpoint changes while operations use its old connection group,
  when a delayed old-generation failure arrives after the new group is
  published, assert new requests keep using the replacement and the old group
  closes only after its final operation retires. Generation-lifetime
  invariant. Integration test.
- Given one connection closes, when more operations target that endpoint,
  assert the underlying RPC pool selects another healthy connection and
  reconnects with bounded backoff without exceeding the group size. Managed-
  reconnect invariant. E2E test.
- Given a segment-relative read or write with zero unit size, overflow,
  out-of-range offset, excessive length, or invalid alignment, when submitted,
  assert it fails before RPC admission with a typed input error. Address-safety
  invariant. Unit test.
- Given caller-owned bytes, when a write completes, assert the payload remains
  valid without a client-side `Bytes`-to-`Vec` copy and the server receives the
  exact bytes at the exact segment-relative location. Payload-integrity
  invariant. Integration test.
- Given buffered and durable write policies, when each write is acknowledged,
  assert buffered mode requires only successful write completion while durable
  mode requires successful write followed by fsync on the owning disk; inject
  fsync failure and assert durable acknowledgement is withheld. Durability-
  policy invariant. Integration test.
- Given transient read failures, an ambiguous write response, a partial write,
  and a permanent disk error, when retry policy executes, assert reads retry
  within one deadline, a write resubmits only identical bytes to the same
  location, partial writes are not retried, permanent failures return
  immediately, and unresolved writes remain typed ambiguous outcomes. Retry-
  safety invariant. Integration test.
- Given normal traffic and saturated repair traffic, when both lanes target
  one node, assert each remains within its own connection and queue bounds and
  repair cannot consume the normal lane's reserved connection group. Traffic-
  isolation invariant. Integration test.
- Given concurrent operations while topology and status refresh, when callers
  read route and metric snapshots, assert request routing observes one complete
  immutable generation and status collection does not add a request-path lock.
  Lock-free-publication invariant. Unit test.
- Given migrated chunk writes, reads, ChunkDB mirror-to-EC conversion, and EC
  reconstruction, when they run against a real DiskIO process, assert all use
  the semantic client, preserve prior data/durability behavior, and no
  production caller imports `Connection`, `RpcServer`, or parses a DiskIO
  endpoint. Single-client-ownership invariant. E2E test.
- Given the chunk-KV production regression with periodic topology refresh and
  a ChunkDB restart, when load and automatic splitting run, assert DiskIO
  connection counts remain bounded, no route-refresh timeout prevents restart,
  and retained throughput/latency results satisfy the existing thresholds.
  Production-path regression invariant. E2E test.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo clippy -p crowdb-diskio-client --all-targets -- -D warnings`
- `pixi run -- cargo test -p crowdb-diskio-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run test-rpc-ffi`
- `pixi run test-rpc-ct`
- `pixi run clean-env && pixi run test-server`
- `pixi run -- bash tools/bench-chunk-kv-regression.sh`
