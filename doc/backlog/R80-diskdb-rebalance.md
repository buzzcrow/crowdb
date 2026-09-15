<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R80: diskdb — Disk Space Rebalance Convergence

**Problem**: DiskDB can now place new allocations on the least-used eligible
disk and can move one committed block through its durable relocation journal.
The current intra-disk-group planner, however, reacts to one utilization
sample, chooses the hottest and coldest disks without projecting the selected
block's effect, and has no end-to-end proof that repeated cycles converge and
then stop. A large block can overshoot the balance point, transient skew can
start unnecessary movement, and passing handoff or restart tests does not show
that a newly added or recovered empty disk actually absorbs old allocations.

The implemented allocation, relocation, and owner-fencing baseline is defined
by the [DiskDB design](../design/diskdb/design-crowdb-diskdb.md), especially
sections 3.2, 9, and 10. ChunkDB's protection-preserving cross-disk-group
planner is defined by the
[ChunkDB design](../design/chunkdb/design-crowdb-chunkdb.md#74-active-cross-domain-rebalance).
This requirement is limited to completing and proving DiskDB's space-balance
behavior within one disk-group.

**Solution**:

1. **Define an improving move**. `RebalancePlannerTask` computes normalized
   utilization as `busy_bytes / capacity_bytes` for every allocatable disk. It
   selects the hottest source and coldest eligible target deterministically,
   but admits a source block only when projecting that exact block onto the
   target strictly reduces the disk-group's utilization spread. Require the
   target to retain `rebalance.min_target_free_bytes` after reservation. If no
   committed block improves the spread, emit no relocation and report the
   group as stalled; never oscillate a block across the balance point merely
   because the current spread exceeds the threshold.

2. **Require sustained skew**. `RebalancePlannerTask` tracks when each owned
   disk-group first exceeds `rebalance.imbalance_threshold_pct` and starts new
   work only after it remains above the threshold for
   `rebalance.hysteresis_secs`. A balanced observation clears the timer. This
   observation state is an optimization and may reset on restart;
   `RelocationJournalValue` remains the durable recovery authority and resumes
   before any new decision.

3. **Converge at a bounded rate**. `RebalancePlannerTask` re-evaluates live
   usage after each completed relocation, starts no more than
   `rebalance.max_jobs_per_cycle`, and keeps at most one active relocation per
   disk-group. `RebalanceZonePacer` keeps disk-groups and zones serially paced
   by `rebalance.zone_delay_secs`. The planner stops creating journals once the
   spread is below the configured threshold. A source or target that is no
   longer allocatable invalidates the candidate without weakening
   `RelocationWorker` safety rules.

4. **Make convergence observable**. `DiskdbMetrics` keeps the existing
   imbalance and relocation metrics and distinguishes groups that are observing
   hysteresis, actively moving, balanced, or stalled because no safe improving
   block fits. Status is derived from `DdbDiskGroup::aggregate_usage`, the
   hysteresis observation, and durable journals, not from a process-local job
   set. Per-disk-group keepalive summaries remain the cluster-wide source for
   placement and operator inspection.

5. **Prove passive and active balance together**. Preserve the lock-free
   `DdbDiskGroup::allocate_block` load-aware policy and verify its behavior over
   sequences rather than a single allocation. Add a full-stack fixture with
   real DiskIO and the ChunkDB owner path that repeatedly runs the production
   planner until a hot disk-group converges or truthfully reports that block
   granularity prevents further improvement.

The relocation invariants remain unchanged: ChunkDB alone publishes layout
changes; the exact target is copied, fsynced, published, and confirmed before
the source is freed; and timeout, age, or utilization never authorizes a free.

**Dependencies**:

- Uses DiskDB's existing load-aware allocator, per-disk usage snapshots,
  relocation journal, DiskIO copy/fsync path, and tentative-owner scanner.
- Uses ChunkDB's existing exact-source revision fence and durable relocation
  task. No new metadata publication path is introduced.
- Cross-disk-group selection is already owned by ChunkDB. This requirement
  does not change rack, node, or physical-disk placement policy.
- Dynamic disk membership and status come from the existing group-0 sync. A
  disk leaving `Up` is excluded on the next planning decision.

**Acceptance**:

- Given equal-capacity disks at 90% and 0%, when 100 single-block allocations
  run with load-aware allocation enabled, assert the colder disk receives the
  allocations until its utilization catches up; repeat with the policy
  disabled and assert cursor-order round-robin, and with equal utilization and
  exclusions to assert deterministic ties and strict anti-affinity. Invariant:
  passive balancing changes selection only and preserves allocation safety.
  Integration test.
- Given unequal-capacity disks, when allocation uses `free_bytes` and then
  `inverse_used_pct`, assert each policy follows its documented absolute or
  normalized ordering without division-by-zero or overflow. Invariant: mixed
  capacity produces deterministic, configured behavior. Unit test.
- Given a disk-group whose spread crosses the threshold for less than the
  hysteresis interval, when planner cycles run, assert no relocation journal is
  created; after sustained skew, assert exactly one improving journal is
  admitted. Invariant: transient skew cannot trigger data movement.
  Integration test.
- Given a candidate block whose projected move would preserve or increase the
  spread, or leave less than the configured target headroom, when the planner
  evaluates it, assert no target is reserved and the group reports stalled.
  Invariant: every admitted move strictly improves usable balance. Unit test.
- Given a hot disk and a newly added or recovered empty peer, when production
  planner cycles execute through real DiskIO and ChunkDB, assert each
  `SourceFreed` move lowers projected spread, restart once with a non-terminal
  journal, and eventually observe spread below threshold with no subsequent
  journal. Invariant: active rebalance converges, survives restart, and stops.
  E2E test.
- Given balanced disks, a single-disk group, all disks non-allocatable, or a
  source/target status change between cycles, when the planner runs, assert it
  creates no unsafe new journal and retains or safely resumes any existing
  journal. Invariant: topology edge cases are no-op or retry, never unsafe
  relocation. Integration test.
- Given several owned disk-groups, when some are observing, moving, balanced,
  and stalled, assert metrics/status report each state consistently with
  current usage and non-terminal journals. Invariant: operators can distinguish
  progress from inability to improve. Integration test.

Verification commands:

- `pixi run test-diskdb`
- `pixi run test-chunkdb`
- `pixi run test-diskdb-client`
- `pixi run test-chunkdb-client`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`
