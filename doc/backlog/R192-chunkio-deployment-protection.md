<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R192: chunk IO — Explicit deployment protection and strip I/O

#### Problem

The single-node container currently uses one KV replica but permits colocated mirror and EC fragments. It therefore spends resources on copies that cannot survive a node failure. The large-write path hardcodes EC and its mirror strip writer is incomplete; the small-write path implements mirror I/O separately. Without a deployment-level guard, a protected cluster could accept an unsafe layout when topology shrinks. See [chunk IO](../design/chunkio/design-crowdb-chunkio.md), [chunk placement](../design/chunkdb/design-crowdb-chunkdb.md), and [KV quorum](../design/kv/design-crowdb-kv.md).

#### Solution

Production deployment requires at least three voting nodes, with KV and chunk placement capable of continuing after any one node fails. There is no standalone two-node deployment mode. The two surviving nodes of a three-node cluster retain the original three-voter membership and its two-vote Paxos quorum.

The deployment property `max_node_failures` is fixed at `1` for this three-node production profile. Healthy mirror strips, including small writes, chunk-KV journal and tree pages, use two copies on distinct nodes; a third copy does not increase this profile's node-failure tolerance. Healthy `2+1`, `4+2`, and `8+4` EC layouts place at most their parity count of fragments on any one node. After one node fails, the remaining node-failure budget is zero: new mirror strips still allocate two copies across the survivors, while new EC strips may place their fragments across those two nodes, mark the placement degraded, and create a durable repair task. A second node failure is outside this profile's guarantee; neither placement nor deployment mode silently falls back to a one-copy mirror.

Single-node is an explicit test-only mode. It has one KV server and one voting copy per KV group. Every new chunk strip has 1 MiB logical data capacity and one mirror copy; EC, multiple mirror copies, and mirror-to-EC conversion are disabled. A data error is returned to the caller. This mode provides no data protection and cannot be entered automatically because of missing nodes, failed placement, or quorum loss.
Its `max_node_failures` property is `0`.

Existing colocated EC integration fixtures use a separate explicit
`test_unsafe_placement` mode, accepted only by debug builds. It is not the
single-node deployment profile and cannot be used by release binaries.

Chunk capacity is independent of strip capacity. A single-node chunk may
contain multiple 1 MiB strips. Each chunk type's writer exposes its chunk
capacity in its component configuration; the single-node profile also selects
its RPC worker and connection counts explicitly.

1. Make the deployment protection mode and its fixed `max_node_failures` value explicit in startup configuration. Validate the KV replica topology and chunk placement policy against them before serving writes. Reject a production configuration with fewer than three voting nodes, a mismatched failure budget, or a single-copy mirror, and reject test-only single-node configuration that requests multiple copies or EC.
2. Treat a chunk as a sequence of strips, each with its own logical data capacity and protection layout. The chunk write path advances through strips and delegates block alignment, cross-block writes, mirror duplication or EC encoding, durability, and repair to the selected strip writer. A mirror strip writer must handle the single-copy test layout and protected mirrored layouts. The read path dispatches to the matching strip reader, whose error recovery is layout-specific.
3. Keep foreground write policy in each access library and physical strip I/O in chunk-client. S3 and Iceberg may choose separate policies; neither decides placement or performs EC encoding itself. Existing small-write admission remains independent of the large-write path while sharing strip-level semantics where appropriate.
4. In a healthy three-node cluster, place two-copy mirror strips and EC layouts so loss of any one node leaves enough information to read committed data. After one node fails, retain the original protected-cluster identity and quorum. First replace a failed mirror segment on the unused surviving node; if replacement fails, rotate the chunk and retry the uncommitted write once, returning an error if rotation or retry fails. Keep two-copy mirror placement for new strips. Permit new EC writes across the two survivors with a persisted degraded-placement marker and durable repair task; never silently allocate a one-copy strip. Recover the full EC placement when the third node returns.
5. Let mirror strip I/O use its configured copy count from one through five. The three-node production policy selects two; the implementation must not assume that all mirror strips have two copies.

#### Dependencies

- R191 supplies protocol-owned chunk types and write policies; this requirement consumes them without merging their small-write pools.
- R193 generalizes this fixed zero/one-node failure budget to larger clusters; its six-node policy does not block R192.
- KV already computes majority quorum from voting members: three voters need two votes, while two voters also need two. This requirement does not change Paxos quorum semantics or introduce a two-voter production profile.
- If protected degraded writes and their repair cannot yet be completed, reject those writes explicitly while preserving readable committed data; do not claim full one-node-failure availability until the write acceptance case passes.

#### Acceptance

- Given production configuration with fewer than three voting nodes, start the services; startup rejects it before accepting a write. Given three voters, startup succeeds. Integration test.
- Given single-node test and three-node production configurations, start each service with `max_node_failures` set to zero and one respectively; matching values succeed, while a mismatched value or incompatible mirror policy is rejected. Integration test.
- Given a single-node test profile with one KV replica, write and read both small and large objects; every new strip has one 1 MiB mirror copy, and no EC or conversion task is created. E2E test.
- Given single-node test mode and a storage read or write failure, perform an object operation; the caller receives an error and no second copy or EC reconstruction is attempted. Integration test.
- Given production mode and a request to enable single-node placement, a one-copy mirror, colocated fragments that break one-node recovery, or an EC layout that loses too many shards with one node, start or allocate; validation rejects the unsafe request. Integration test.
- Given three healthy storage nodes, allocate small-write, chunk-KV journal, and tree-page mirror strips; each persisted strip has two copies on distinct nodes. Stop a node holding one copy and write again; replacement uses the unused survivor or the writer rotates once, and a failed rotation or retry returns an error. Integration test.
- Given a chunk with consecutive mirror and EC strips of differing capacities, write data across strip and block boundaries, seal, restart, and read it; each strip applies its own write and read behavior and all bytes match. Integration test.
- Given a healthy three-node cluster with committed objects, stop any one node and perform linearizable metadata reads, object reads, and new writes through the surviving two; operations succeed with the unchanged three-voter membership and no single-copy allocation. E2E test.
- Given one node stopped, request a new EC strip with a supported `2+1`, `4+2`, or `8+4` policy; fragments occupy the two surviving nodes, the stored layout is marked degraded, and a durable placement task exists. Integration test.
- Given the failed node returns, run placement repair and read objects written during the outage; each object remains readable and its placement returns to the configured protected policy. E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-kv`, `pixi run cargo test -p crowdb-chunk-client`, `pixi run cargo test -p crowdb-chunkdb`, and `pixi run test-single-node-container` for the implemented scope.
