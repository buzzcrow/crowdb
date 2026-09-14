<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R97: chunkdb — Advanced Placement Strategies

**Problem**: Basic rack-aware placement may lead to imbalanced resource
utilization over time. `MirrorPlacement` randomly rotates racks and chooses the
first eligible disk-group on a node. `EcPlacement` round-robins racks and
balances only the number of fragments selected during one call. Neither uses
the disk-group usage summaries already published in group 0, accounts for
allocations concurrently in flight, nor reports whether the finished strip is
actually protected from a rack, node, or disk failure.

This is unsafe to interpret as a protection contract on uneven topology. A
large rack can contain most of the cluster's nodes and disks, while a small
rack can be forced to accept the same number of fragments until it fills. A
node can expose several disk-groups of different sizes, but the selector does
not compare their projected utilization. Physical disk selection happens later
inside DiskDB, so a topology-level plan cannot claim disk protection until the
returned `Segment`s have been validated.

The root design is the [chunkdb design](../design/chunkdb/design-crowdb-chunkdb.md)
sections 6–8. The current rack-first fallback in section 7 does not distinguish
a requested preference from a verified failure guarantee. Operators need to
choose whether rack or node protection is primary, understand the reduced
guarantee when the topology cannot satisfy it, and change the policy without
silently rewriting existing chunk layouts.

**Solution**: Add capacity-aware, failure-domain placement with an explicit
policy and a verified post-allocation assessment.

1. Add `placement.failure_domain_priority = "rack_first" | "node_first"` to
   `app/crowdb-chunkdb/src/chunkdb_config.rs` and pass the selected policy
   through `app/crowdb-chunkdb/src/allocator.rs` into the selectors. Default to
   `rack_first`, matching the existing architecture. A configuration change
   applies to new strips and replacements that have no recorded policy; repair
   of an existing strip preserves that strip's policy. It never moves an
   existing segment by itself. Until R139 provides distributed dynamic config,
   changing the server setting requires the normal ChunkDB restart/reload.

2. Define the protection budget in `app/crowdb-chunkdb/src/selector.rs`. For an
   EC `data_num + code_num` strip, a protected rack, node, or disk may contain
   at most `code_num` fragments. For a mirror strip, loss of a protected domain
   must leave at least one complete copy. Prefer one mirror copy per rack, node,
   and disk whenever the chosen priority and available topology permit it.
   Health filters and rack/node/disk-group negative hints remain hard
   exclusions.

3. Make protection priority lexicographic rather than a weighted score:

   - `rack_first` first rejects or minimizes violations of the rack loss
     budget, then spreads fragments across nodes inside each selected rack,
     then across disk-groups and physical disks. A rack-safe plan is therefore
     also bounded at its contained node and disk domains.
   - `node_first` first rejects or minimizes violations of the node loss
     budget and maximizes distinct nodes across the cluster. Rack diversity is
     the next tie-breaker, followed by disk-group and disk spread. This mode
     can produce a node-safe but rack-unsafe candidate when most eligible nodes
     are in one rack; publishing it requires explicit degraded placement and
     the result must say so.
   - Disk protection is never inferred from disk-group selection. DiskDB must
     spread a strip's allocations across distinct healthy disks before reuse.
     ChunkDB validates the returned physical `disk_id`s against the strip's
     loss budget and rolls back and retries another candidate when they violate
     it.

4. Extend `app/crowdb-chunkdb/src/topology.rs` to join each healthy disk-group
   with its healthy disk records, `DiskGroupUsageSummary`, allocatable disk
   count, freshness, and locally reserved in-flight bytes. Aggregate those
   values by node and rack, and reject a disk-group that cannot satisfy the
   requested disk anti-affinity. Extend the summary in
   `lib/crowdb-protocol/src/types/common.rs` with allocatable capacity and free
   bytes because its current totals include non-allocatable disks and cannot
   safely rank usable headroom. Safety is evaluated from topology membership
   and segment counts; stale or absent usage data may reduce balancing quality
   but must never relax a failure-domain constraint. DiskDB remains the
   authority on whether a block can actually be allocated.

5. Rank candidates within the safe set by projected utilization:
   `(used_bytes + in_flight_bytes + planned_bytes) / usable_capacity_bytes`.
   Compare the primary domain, secondary domain, disk-group free-space
   headroom in policy order. Raw node, disk, or byte counts are not balance
   scores because the hardware is heterogeneous. A fresh normalized I/O-load
   signal may be used after these safety and capacity terms; when no
   authoritative per-disk-group signal exists, omit that term rather than
   treating unknown load as zero. Use a stable hash of chunk id, strip
   sequence, and fragment index only to break equal scores, making retry
   decisions deterministic. Track in-flight bytes with lock-free counters and
   publish topology generations immutably; do not add a placement-path lock.

6. Treat insufficient topology explicitly. Priority controls candidate order,
   not the definition of safety: a normal placement must satisfy every
   requested rack, node, and disk loss budget. Return a typed error naming the
   first unsatisfied domain and perform zero DiskDB allocation. The explicit
   `placement.allow_degraded_failure_domains`, defaulting to `false`, may
   authorize the best remaining plan. An EC layout that also exceeds its
   node/disk recovery budget remains gated by the existing
   `placement.allow_unsafe_ec`. The plan and persisted strip metadata must
   record which of rack, node, and disk protection are false. Capacity
   exhaustion remains distinct from a failure-domain error.

7. Extend `PlacementPlan` with the effective priority, planned rack/node
   counts, required disk constraints, topology generation, and usage
   freshness. After DiskDB returns physical segments, produce a separate
   `PlacementAssessment` with the actual maximum fragments per rack, node, and
   disk plus the three protection flags. Persist the requested priority and
   creation assessment with `ChunkStrip` in
   `lib/crowdb-protocol/src/types/chunkdb.rs` and
   `lib/crowdb-protocol/src/fbs/chunkdb.fbs`. Repair preserves or improves the
   strip's recorded protection; a later global policy change does not silently
   weaken an existing strip. The R84 placement scanner recomputes current
   protection after disk movement because the creation assessment can become
   stale.

8. Handle uneven hardware without sacrificing the selected guarantee. For a
   three-copy mirror on one large and two small racks, `rack_first` places one
   copy in each while all three have headroom; when a small rack can no longer
   accept a copy, allocation fails or returns an explicitly degraded plan
   instead of claiming rack protection. `node_first` may choose three
   low-utilization nodes in the large rack and records node/disk protection but
   not rack protection. For 8+4 EC, a rack-protected plan never places more
   than four of the twelve fragments in one rack, regardless of rack size.

9. Add passive and active balancing separately. New allocations immediately
   use the projected-utilization ranking. A cross-disk-group rebalance planner
   acts only after configurable skew and minimum-free-space thresholds persist
   for a hysteresis interval. It emits bounded moves, relocates at most one
   segment of a strip at a time through the existing fenced replacement flow,
   and never publishes an intermediate or final layout with weaker protection.
   R80 remains responsible for balancing disks within one disk-group; this
   requirement owns selection and movement across disk-groups, nodes, and
   racks.

10. Add metrics in `app/crowdb-chunkdb/src/metrics.rs` for allocations by
    priority, rack/node/disk protection failures, explicitly degraded plans,
    stale-usage decisions, projected-utilization skew, retry/rollback after
    physical-disk validation, placement tasks waiting/completed/failed, and
    rebalance moves. Logs and operator output report the effective guarantees
    without placing rack/node/disk identifiers in metric names.

11. Treat every explicitly degraded EC layout as temporary. Add a
    `TASK_KIND_REPAIR_PLACEMENT` handler in the existing persistent task
    framework, implement its admission/reconciliation logic in
    `app/crowdb-chunkdb/src/placement_repair.rs`, and add its versioned payload
    to `lib/crowdb-protocol/src/types/chunk_task.rs`. Publishing a degraded EC
    strip persists `placement_repair_required`, the desired priority, and the
    failed protection flags in its `ChunkStrip` metadata. It then admits one
    deterministic task keyed by chunk id and strip sequence. A bounded periodic
    placement-repair reconciliation scan walks marked chunk metadata and
    recreates a missing task when admission was interrupted after the chunk
    commit, so no unsafe strip can be forgotten.

    Bound background work with placement-repair concurrency, bandwidth, and
    retry-backoff settings in `ChunkdbConfig`; foreground allocation and repair
    share neither unbounded tasks nor unbounded data buffers.

    The handler re-evaluates the latest topology and waits with bounded backoff
    when no safer placement exists; insufficient topology is not a terminal
    task failure. A topology-generation change triggers reconciliation and
    makes waiting placement tasks immediately eligible. When capacity becomes
    available, the handler relocates only the minimum fragments required, one
    fragment per strip at a time, through the existing fenced replacement
    flow. Each published step must keep at least
    `data_num` readable EC fragments, must not turn any currently protected
    domain into an unprotected one, and must validate the destination physical
    disk. Clear `placement_repair_required` and complete the task only after
    rack, node, and disk protection satisfy the strip's recorded target. Crash,
    lease expiry, duplicate admission, and stale topology retries remain
    idempotent through the existing `TaskManager` claim generation and source
    revision fences.

12. Add an end-to-end EC placement matrix using 10+2, 20+2, and 40+4. The
    constrained fixture has two racks with four nodes in one rack and two nodes
    in the other, with enough physical disks to distinguish disk protection
    from node and rack protection. The matrix first verifies the exact maximum
    fragments and protection flags for each scheme under `rack_first` and
    `node_first`; it must not label an impossible rack guarantee as safe. It
    then adds the racks, nodes, disks, and capacity mathematically required by
    each EC loss budget and verifies the background placement task converges to
    a protected layout without making the chunk unreadable. These larger cases
    run serially in the ChunkDB E2E gate to avoid multiplying cluster load.

**Dependencies**:
- Uses group-0 hardware membership and `DiskGroupUsageSummary` maintained by
  the existing topology refresh. When a summary is missing or stale, placement
  keeps the hard safety filters and uses deterministic topology-only ranking.
- Uses DiskDB's distinct-disk batch allocation and exclusion hints. If DiskDB
  cannot return a disk-safe batch, ChunkDB rolls it back; it does not accept a
  topology plan as proof of physical disk diversity.
- R80 owns balancing within a disk-group. R97 must not duplicate its per-disk
  planner.
- R84 recomputes protection after a disk is moved. Before R84 lands, disk moves
  must surface the affected strips for operator review rather than trusting
  their creation assessment.
- R139 is optional for changing the placement policy without restarting
  ChunkDB. The policy semantics and persisted per-strip intent do not depend on
  R139.
- Reuses the persistent `TaskManager`, `TaskScanner`, and fenced strip
  replacement flow. Placement repair is a new task kind, not a second task
  execution framework; its bounded chunk-marker reconciliation pass is the
  fallback when task admission or a task record is lost.

**Acceptance**:
- Given three mirror copies and three healthy racks, select with `rack_first`;
  allocate and resolve the returned segments; assert three distinct racks,
  nodes, and disks and an assessment marking all three domains protected,
  proving the rack-first protection invariant — Integration test.
- Given three mirror copies, one large rack, and two smaller racks with usable
  headroom, select repeatedly with `rack_first`; assert every strip uses all
  three racks while projected utilization, not raw allocation count, chooses
  nodes and disk-groups inside them, proving safety-before-balance — Unit test.
- Given the same uneven cluster, enable explicit degraded placement and select
  repeatedly with `node_first`; assert distinct nodes are primary, rack
  diversity is a tie-breaker, and any layout concentrated in one rack is
  marked rack-unprotected and repair-required, proving truthful node-first
  assessment — Unit test.
- Given 8+4 EC and heterogeneous racks, select with `rack_first`; assert no rack,
  node, or returned disk contains more than four fragments, proving one-domain
  loss remains within the EC recovery budget — Integration test.
- Given 8+4 EC with enough nodes but only two racks, request `rack_first`
  without degraded placement; assert a typed rack-protection error and zero
  DiskDB allocations, proving safe requests never silently degrade — Unit
  test.
- Given that same topology with explicit degraded placement, allocate a strip;
  assert the persisted assessment marks rack protection false while accurately
  reporting node and disk protection, proving degraded layouts are explicit —
  Integration test.
- Given two disk-groups on one node with different capacities and utilization,
  select multiple placements; assert the lower projected-utilization group is
  preferred without exceeding failure budgets, proving normalized capacity
  balancing — Unit test.
- Given a stale usage summary and healthy current topology, select a strip;
  assert safety constraints still hold, the decision is marked stale, and
  deterministic fallback ranking is used, proving metrics never control
  correctness — Unit test.
- Given concurrent allocations against the same low-headroom disk-group,
  reserve projected bytes and select plans; assert in-flight accounting diverts
  later plans without a placement lock and releases on success and rollback,
  proving bounded oversubscription — Integration test.
- Given DiskDB returns duplicate physical disks that exceed the strip's loss
  budget, validate the response; assert all tentative segments are rolled back
  and a different eligible disk-group is tried, proving post-allocation disk
  protection — Integration test.
- Given an existing rack-first strip and a runtime default changed to
  `node_first`, repair one segment; assert the replacement preserves or
  improves the strip's recorded rack/node/disk guarantees and unrelated strips
  do not move, proving policy changes are non-retroactive — Integration test.
- Given sustained cross-domain utilization skew above the configured threshold,
  run the rebalance planner; assert moves are bounded, only one segment per
  strip is in transition, and every published layout preserves protection;
  then lower skew below the hysteresis threshold and assert no move is emitted,
  proving safe stable rebalancing — Integration test.
- Given the two-rack fixture with four nodes in one rack and two in the other,
  allocate 10+2, 20+2, and 40+4 EC strips under both priorities; resolve every
  physical segment and assert the reported maximum fragments per rack, node,
  and disk match the layout, rack protection is false whenever a rack contains
  more than `code_num` fragments, and node/disk flags independently reflect
  their own budgets, proving the heterogeneous EC assessment matrix — E2E
  test.
- Given explicitly degraded 10+2, 20+2, and 40+4 EC strips in that two-rack
  fixture, commit each chunk and interrupt task admission once; restart
  ChunkDB and assert the durable strip markers recreate exactly one placement
  task per strip, proving unsafe EC placement cannot escape background repair —
  E2E test.
- Given those waiting placement tasks, add enough healthy failure domains for
  each scheme (`ceil((data_num + code_num) / code_num)` racks: six for 10+2
  and eleven for both 20+2 and 40+4); assert the tasks wake, relocate the
  minimum required fragments one at a time, keep at least `data_num` readable
  fragments throughout, clear the durable markers, and finish with rack,
  node, and disk protection true, proving eventual safe-placement convergence
  — E2E test.
- Given a degraded EC placement task with unchanged insufficient topology,
  advance repeated scan and lease cycles; assert it remains retryable with
  bounded backoff rather than becoming terminal or spinning, proving topology
  shortage is a waiting condition — Integration test.

Verification commands:
- `pixi run test-chunkdb`
- `pixi run test-diskdb`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`
