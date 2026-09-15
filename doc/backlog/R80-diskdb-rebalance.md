<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R80: diskdb — Space Rebalance Across Disks + Disk-Groups

**Problem**: DiskDB originally selected allocatable disks with a pure
round-robin cursor and never moved existing allocations. A new or recovered
disk therefore received only an equal share of new writes while older disks
remained hot. Cross-disk-group imbalance also persisted because ChunkDB chose
the disk-group without a safe physical relocation contract. Tentative blocks
left by interrupted allocation, repair, or relocation had no owner-fenced
cleanup path.

**Solution**: provide passive and active convergence while keeping allocation
lock-free and making ChunkDB the only authority that publishes chunk layout
changes.

1. **Load-aware passive allocation**:
   - `allocator.load_aware`, default `true`, selects the eligible disk with the
     best configured weight before falling back to the existing cursor.
   - `allocator.load_aware_weight = "free_bytes" | "inverse_used_pct"`.
   - Equal weights retain deterministic cursor order. Disabling the policy
     restores pure round-robin behavior.
   - Exclusion hints and multi-block rollback semantics remain unchanged.

2. **Usage and rebalance visibility**:
   - The reporting loop publishes disk-group usage summaries through DiskDB
     keepalive extras. ChunkDB joins those summaries to the live service
     registry and hardware topology.
   - DiskDB exposes `disk_group.imbalance.used_pct_{spread,max,min}` plus
     `rebalance.plan_count`, `rebalance.planned_blocks`,
     `rebalance.moves.total`, and `rebalance.errors.total`.

3. **Tentative BusyBlock owner scanner**:
   - Scan only durable `BusyBlockValue` records with
     `commit_state = Tentative`.
   - Query the current ChunkDB owner with the chunk ID and exact segment
     incarnation. `Referenced` confirms that exact target, `TaskPending`
     retains it, and `Absent` starts or continues a durable grace record.
   - Free only after `Absent` remains authoritative through
     `scanner.tentative_owner_grace_secs`, default 86,400 seconds. Routing and
     RPC failures retain the target.
   - Do not mutate before the owning disk-group reaches lifecycle `Up`.
   - Traverse one disk-group, one disk, and one zone at a time. Await
     `scanner.tentative_owner_zone_delay_secs`, default 3 seconds, after every
     visited zone. The delay uses the async runtime timer and does not block an
     executor thread.

4. **Durable relocation handoff**:
   - Key one relocation journal by the exact source incarnation. Its value
     records the deterministic operation ID, owner chunk, exact source and
     target, target disk-group, unit size, timestamps, error, and phase.
   - Phases are `Reserved`, `Copied`, `Accepted`, `Published`,
     `TargetConfirmed`, `SourceFreed`, and `Discarded`. Persist each phase
     before relying on it after restart.
   - Reserve an exact tentative target, copy through DiskIO, and fsync the
     target before contacting ChunkDB.
   - ChunkDB durably claims the request before returning `Accepted`. Its task
     conditionally replaces the exact source under the chunk revision fence.
     Duplicate delivery observes the same durable task and publication.
   - ChunkDB first confirms the target after its successful CAS. Only
     `Published` authorizes DiskDB to verify and idempotently confirm that exact
     target and then write the source free record. `Stale` discards the target;
     `Rejected` or transient failures retain both sides for reconciliation.
   - On restart, DiskDB lists non-terminal journals for the disk-group and
     resumes from the persisted phase. A journal is counted only by its
     recorded target disk-group, even when several groups share a KV binding.

5. **Paced intra-disk-group planner**:
   - On `rebalance.plan_interval_secs`, compare allocatable disk utilization.
     If the configured spread persists, choose a committed source on a hot
     disk and an eligible target disk in the same disk-group.
   - Process disk-groups and zones serially and await
     `rebalance.zone_delay_secs` between zones. Default batch and concurrency
     remain bounded; no cluster-wide burst is permitted.
   - Existing non-terminal journals resume before a new source is selected.

6. **Cross-disk-group consumer**:
   - ChunkDB ranks live disk-group usage summaries, waits for sustained skew,
     and moves at most one fragment per cycle through `ExecuteRelocation`.
   - The target is allocated tentatively on the chosen cold disk-group. Before
     handoff, ChunkDB recomputes the proposed strip and rejects any move that
     weakens rack, node, or physical-disk protection or increases a recorded
     domain maximum.
   - DiskDB adopts the exact preallocated target into its durable journal.
     Re-delivery must match the journal's exact source and target.
   - A target DiskDB can finalize a source owned by another local disk-group or
     route the exact free request to the remote DiskDB owner.
   - The default policy uses a 300-second scan interval, 20-point utilization
     threshold, 900-second hysteresis, 1-GiB minimum target headroom, and
     exactly one move per cycle.

**Safety invariants**:

- ChunkDB is the sole publisher of chunk layout metadata.
- Source data is never freed before the exact target is fsynced, published,
  and confirmed.
- A timeout or age is never sufficient authority to free a tentative block.
- Operation and scanner identities include `allocation_ts`; reused offsets do
  not alias an older incarnation.
- Scanner and planners remain serial and paced. No new placement-path lock is
  introduced.
- Missing or stale utilization affects ranking only; it never weakens physical
  placement safety.

**Acceptance**:

- Load-aware allocation prefers the less-used disk, equal weights preserve
  cursor order, disabled mode is round-robin, and exclusions remain strict.
- Metrics report disk utilization spread and relocation activity.
- Scanner covers `Referenced`, `TaskPending`, `Absent` before/after grace,
  deleted owner, stale incarnation, transient owner failure, lifecycle gate,
  durable grace restart, and disk-group/disk/zone pacing order.
- Relocation proves copy and fsync precede owner handoff; duplicate requests
  are idempotent; `Stale`, `Rejected`, and transient outcomes retain the safe
  side of the move.
- Restart from every durable phase (`Reserved`, `Copied`, `Accepted`,
  `Published`, `TargetConfirmed`, and `SourceFreed`) converges without a second
  publication or premature source free.
- A true cross-disk-group RPC path reaches `SourceFreed`, leaves a durable
  source free record, installs the target in ChunkDB, and keeps rack/node/disk
  protection true.
- Sustained skew honors hysteresis and emits one move; balanced summaries emit
  no further journal.
- Cross-domain 10+2, 20+2, and 40+4 EC moves preserve all recorded protection
  bounds.

Verification commands:

- `pixi run test-diskdb`
- `pixi run test-chunkdb`
- `pixi run test-diskdb-client`
- `pixi run test-chunkdb-client`
- `pixi run test-protocol`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`

**Current cleanup blocker**: the affected crates and tests are clean, but the
workspace `rs-lint` gate is blocked by pre-existing unclassified DashMap fields
`sessions` and `expired` in
`lib/crowdb-kv/src/rpc/snapshot_registry.rs`. Keep this requirement and its
working plans until the complete workspace gate is clean.
