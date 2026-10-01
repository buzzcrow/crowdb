<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R193: chunkdb — Configurable node failure budget and EC placement

#### Status

Planned. The explicit single-node and three-node profiles, persisted strip layouts, and degraded-placement repair baseline are complete.

#### Problem

The current system defines two deployment contracts: one test node with no node-failure tolerance and three production nodes tolerating one failed node. The current EC selector checks the maximum fragments on one node against the parity count. That check cannot express a larger failure budget: with six nodes and two allowed failures, `4+2` and `8+4` can survive any two nodes, while `2+1` cannot. Mirror copy counts and journal/tree write policies are also configured independently rather than derived from one system protection contract. See [chunkdb placement](../design/chunkdb/design-crowdb-chunkdb.md) and [chunk IO](../design/chunkio/design-crowdb-chunkio.md).

Operators need to choose a node failure budget for a deployment without accidentally admitting an EC shape or mirror layout that loses committed data within that budget. When nodes fail, new placement must use the remaining budget and record any loss of the full-cluster protection target for repair after recovery.

#### Solution

The system property `max_node_failures` is the number of unavailable nodes the configured deployment promises to tolerate from its complete topology. It is a protection target, not an automatic instruction to reduce copies each time a node fails. The remaining budget is the target minus the nodes already unavailable. A production profile must have enough voting KV replicas to retain quorum at that target and enough distinct storage nodes to place its selected layouts. The explicit test-single-node profile has a zero budget. Startup and allocation reject mismatched service policy, impossible budgets, and unsafe layouts; failure never silently switches deployment mode.

For a mirror strip, the full protection target requires at least `max_node_failures + 1` copies on distinct nodes. Continue to use the full copy count after a failure when placement permits it. For an EC strip with `k` data and `m` parity fragments, sort per-node fragment counts descending; the sum of the largest `max_node_failures` counts must be at most `m` in a healthy topology. Apply the corresponding remaining-budget check to new allocations after failures. Keep fragments spread across available nodes even when no further node-failure budget remains. Persist actual strip geometry and its full protection target separately so readers use the real layout and repair can restore the target.

1. Validate the deployment property and KV/storage topology consistently in deployment configuration, ChunkDB startup, and access/chunk writer policy. Preserve the existing one-node/zero-failure and three-node/one-failure profiles.
2. Replace the one-node EC bound in ChunkDB placement and physical validation with the worst-case sum across the configured number of failed nodes. Select mirror copy counts from the deployment contract, while retaining explicit per-strip policy only when it meets or exceeds the target.
3. During an outage within the configured budget, try full protection first. If it cannot fit, permit a layout that meets the remaining budget, persist a degraded-placement marker, and create a durable placement task. Reject allocation if even the remaining-budget layout or KV quorum is unavailable. Never claim full protection for a degraded strip.
4. After capacity returns, use fenced placement tasks to move or rebuild fragments until the original target holds. Reads and writes follow each strip's persisted geometry throughout migration; a restart resumes unfinished tasks without accepting stale placement.

#### Dependencies

- The existing two deployment profiles, strip dispatch, and placement-repair baseline are the starting point for generalized validation.
- R103 is responsible for ChunkDB range-owner migration after a ChunkDB instance failure. This requirement's storage placement budget does not replace metadata service failover; end-to-end availability depends on both.
- R139 may later distribute the system property through Group 0. Until then, startup must reject inconsistent local configuration rather than assume a remote configuration service exists.

#### Acceptance

- Given six voting KV/storage nodes and `max_node_failures = 2`, start services; startup accepts the budget and reports the configured target. Given an impossible budget or too few voters, startup rejects it before writes. Integration test.
- Given six healthy nodes with budget two, allocate `4+2` and `8+4` EC strips; every pair of nodes owns at most two and four fragments respectively. Request `2+1`; allocation rejects it because a pair can own more than one fragment. Integration test.
- Given six healthy nodes with budget two, allocate mirror strips used by object data, journal, and tree pages; each new strip has at least three copies on distinct nodes and reads from its persisted layout. Integration test.
- Given a six-node budget-two cluster, stop one node and allocate new mirror and EC strips; placement retains the full target where possible, otherwise uses the remaining one-failure budget and persists a repair task without changing deployment mode. E2E test.
- Given the same cluster with two nodes unavailable, allocate while KV quorum and a valid remaining-budget placement exist; operations succeed without claiming that a third failure is tolerated. Remove enough further capacity or quorum; allocation returns an error. E2E test.
- Given a degraded EC strip and recovered nodes, restart the repair worker and complete its task; data remains readable during movement, the task survives restart, and the final placement again passes the full two-node-failure check. E2E test.
- Given the existing single-node and three-node configurations, run their write/read and one-node-out suites after introducing the generalized policy; their configured budgets and persisted layouts remain valid. E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-kv`, `pixi run cargo test -p crowdb-chunkdb`, `pixi run cargo test -p crowdb-chunk-client`, and `pixi run cargo test -p crowdb-chunk-stream` for the implemented scope.

#### Open Questions

None.
