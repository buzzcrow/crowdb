<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R192: chunk IO — Explicit deployment protection and strip I/O

#### Problem

The single-node container currently uses one KV replica but permits colocated mirror and EC fragments. It therefore spends resources on copies that cannot survive a node failure. The large-write path hardcodes EC and its mirror strip writer is incomplete; the small-write path implements mirror I/O separately. Without a deployment-level guard, a protected cluster could accept an unsafe layout when topology shrinks. See [chunk IO](../design/chunkio/design-crowdb-chunkio.md), [chunk placement](../design/chunkdb/design-crowdb-chunkdb.md), and [KV quorum](../design/kv/design-crowdb-kv.md).

#### Solution

Production deployment requires at least three voting nodes, with KV and chunk placement capable of continuing after any one node fails. There is no standalone two-node deployment mode. The two surviving nodes of a three-node cluster retain the original three-voter membership and its two-vote Paxos quorum.

Single-node is an explicit test-only mode. It has one KV server and one voting copy per KV group. Every new chunk strip has 1 MiB logical data capacity and one mirror copy; EC, multiple mirror copies, and mirror-to-EC conversion are disabled. A data error is returned to the caller. This mode provides no data protection and cannot be entered automatically because of missing nodes, failed placement, or quorum loss.

1. Make the deployment protection mode explicit in startup configuration. Validate the KV replica topology and chunk placement policy against it before serving writes. Reject a production configuration with fewer than three voting nodes, and reject test-only single-node configuration that requests multiple copies or EC.
2. Treat a chunk as a sequence of strips, each with its own logical data capacity and protection layout. The chunk write path advances through strips and delegates block alignment, cross-block writes, mirror duplication or EC encoding, durability, and repair to the selected strip writer. A mirror strip writer must handle the single-copy test layout and protected mirrored layouts. The read path dispatches to the matching strip reader, whose error recovery is layout-specific.
3. Keep foreground write policy in each access library and physical strip I/O in chunk-client. S3 and Iceberg may choose separate policies; neither decides placement or performs EC encoding itself. Existing small-write admission remains independent of the large-write path while sharing strip-level semantics where appropriate.
4. In a healthy production cluster, place each configured layout across failure domains so loss of any one node leaves enough information to read committed data. Reject an EC layout whose per-node shard distribution cannot satisfy that invariant. After one node fails, retain the original protected-cluster identity and quorum. Permit new writes only through a defined degraded layout that fits the two surviving nodes and can later be repaired; never silently allocate a one-copy strip. Recover full placement when capacity returns.

#### Dependencies

- R191 supplies protocol-owned chunk types and write policies; this requirement consumes them without merging their small-write pools.
- KV already computes majority quorum from voting members: three voters need two votes, while two voters also need two. This requirement does not change Paxos quorum semantics or introduce a two-voter production profile.
- If protected degraded writes and their repair cannot yet be completed, reject those writes explicitly while preserving readable committed data; do not claim full one-node-failure availability until the write acceptance case passes.

#### Acceptance

- Given production configuration with fewer than three voting nodes, start the services; startup rejects it before accepting a write. Given three voters, startup succeeds. Integration test.
- Given a single-node test profile with one KV replica, write and read both small and large objects; every new strip has one 1 MiB mirror copy, and no EC or conversion task is created. E2E test.
- Given single-node test mode and a storage read or write failure, perform an object operation; the caller receives an error and no second copy or EC reconstruction is attempted. Integration test.
- Given production mode and a request to enable single-node placement, colocated fragments that break one-node recovery, or an EC layout that loses too many shards with one node, start or allocate; validation rejects the unsafe request. Integration test.
- Given a chunk with consecutive mirror and EC strips of differing capacities, write data across strip and block boundaries, seal, restart, and read it; each strip applies its own write and read behavior and all bytes match. Integration test.
- Given a healthy three-node cluster with committed objects, stop any one node and perform linearizable metadata reads, object reads, and new writes through the surviving two; operations succeed with the unchanged three-voter membership and no single-copy allocation. E2E test.
- Given the failed node returns, run placement repair and read objects written during the outage; each object remains readable and its placement returns to the configured protected policy. E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-kv`, `pixi run cargo test -p crowdb-chunk-client`, `pixi run cargo test -p crowdb-chunkdb`, and `pixi run test-single-node-container` for the implemented scope.
