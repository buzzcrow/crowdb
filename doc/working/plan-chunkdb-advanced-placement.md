<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Advanced Placement Plan

Upstream: [R97](../backlog/R97-chunkdb-advanced-placement-strategies.md),
[chunkdb design](../design/chunkdb/design-crowdb-chunkdb.md), and
[diskdb design](../design/diskdb/design-crowdb-diskdb.md).

Goal: implement configurable rack-first/node-first placement with truthful
rack, node, and disk protection, capacity-aware balancing, and durable repair
of temporarily degraded EC strips.

## Phase 1 — Contracts and topology inputs

- [x] **Placement policy configuration**: add rack-first/node-first and
  explicit degraded-domain configuration, pass it through lifecycle
  allocation constraints, and add validation tests. Files:
  `app/crowdb-chunkdb/src/chunkdb_config.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`,
  `app/crowdb-chunkdb/src/selector.rs`, `app/crowdb-chunkdb/src/main.rs`,
  config tests.
- [x] **Placement assessment contract**: add serialized placement priority,
  protection assessment, degraded marker, task kind, and typed placement
  failures while preserving legacy decoding. Files:
  `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
  `lib/crowdb-protocol/src/types/chunk_task.rs`, selector/protocol tests.
- [x] **Usable capacity snapshot**: extend disk-group summaries with
  allocatable capacity/free bytes and freshness, join healthy disk records and
  usage into immutable topology snapshots, and add lock-free in-flight byte
  reservations. Files: `lib/crowdb-protocol/src/types/common.rs`, matching
  FlatBuffers/keepalive codecs, `app/crowdb-diskdb/src/`,
  `app/crowdb-chunkdb/src/topology.rs`, topology tests.

## Phase 2 — Placement selectors

- [x] **Failure-domain evaluator**: calculate mirror/EC loss budgets and
  planned/actual rack, node, and disk protection without relying on capacity
  metrics. Files: `app/crowdb-chunkdb/src/selector.rs`, selector tests.
- [x] **Capacity-aware rack-first selector**: replace random rack rotation with
  deterministic safe-set selection and projected-utilization ranking. Files:
  `app/crowdb-chunkdb/src/selector/mirror.rs`,
  `app/crowdb-chunkdb/src/selector/ec.rs`, selector tests.
- [x] **Capacity-aware node-first selector**: implement distinct-node-first
  ranking, rack tie-breaking, explicit degraded output, and deterministic
  retry ordering. Files: selector modules and tests.

## Phase 3 — Allocation and physical-disk validation

- [x] **Allocator policy wiring**: pass policy and planned-byte reservations
  through ordinary, batch, conversion, and replacement allocation; release
  reservations on every completion and rollback path. Files:
  `app/crowdb-chunkdb/src/allocator.rs`, lifecycle/repair/conversion callers,
  allocator tests.
- [x] **Physical assessment**: validate returned `Segment.disk_id` placement,
  retry alternatives after rollback, assemble and persist the final assessment,
  and prohibit silent degradation. Files: allocator, storage/wire codecs,
  integration tests.

## Phase 4 — Durable degraded-placement repair

- [x] **Placement repair task contract**: add a versioned deterministic task
  identity and payload plus marker reconciliation for interrupted admission.
  Files: `lib/crowdb-protocol/src/types/chunk_task.rs`,
  `app/crowdb-chunkdb/src/placement_repair.rs`, task/store tests.
- [x] **Placement repair execution**: re-evaluate topology, wait without
  terminal failure when domains are unavailable, wake on topology generation,
  and move the minimum fragments one at a time through fenced replacement.
  Files: placement repair, task runtime wiring, lifecycle/storage, integration
  tests.
- [x] **Placement repair controls and metrics**: add bounded concurrency,
  bandwidth, backoff, protection/degradation counters, and status reporting.
  Files: config, metrics, service wiring, tests.

## Phase 5 — Rebalancing and E2E

- [x] **Cross-domain rebalance planner boundary**: passive capacity-aware balancing is
  complete. Active movement is deliberately pending R80's disk-level
  relocation contract and durable owner handoff; R97 records the required task
  ownership/fencing decision in its open question rather than introducing a
  competing mover.
- [~] **EC topology matrix**: cover 10+2, 20+2, and 40+4 on two racks with a
  four-node/two-node split under both policies, interrupted admission/restart,
  insufficient-topology waiting, and convergence after adding six or eleven
  racks as required. Files: `app/crowdb-chunkdb/tests/` and test helpers.
  The physical-assessment slice is verified for all six shape/priority cases:
  it recomputes rack, node, and disk maxima from returned segments and compares
  them with the persisted assessment. DiskDB repeated distinct-disk passes are
  used only when a selected disk-group has more fragments than physical disks;
  post-allocation assessment still gates the disk guarantee. A real admission
  gap now persists degraded 10+2, 20+2, and 40+4 strips before the task store
  is attached; restart reconciliation creates exactly one task for each marker
  and a second restart creates none. A real 40+4 repair task with every
  physical disk already excluded now persists as `RetryWait` with the waiting
  metric (not a task failure) when no safe destination exists. Expanded-
  topology convergence remains in this active task. The E2E fixture now grows
  a running DiskDB instance without restart and starts one real DiskIO owner
  per disk-group, so convergence will copy and fsync physical segments.

## Phase 6 — Documentation and cleanup

- [x] **Permanent architecture**: merge implemented placement, assessment,
  and repair behavior into `doc/design/chunkdb/design-crowdb-chunkdb.md`; the
  active-rebalance dependency remains explicitly deferred in R97.
- [ ] **Final cleanup**: run every gate, remove R97 from the backlog and delete
  this plan after all acceptance cases pass.

## Files

- `app/crowdb-chunkdb/src/{allocator,chunkdb_config,metrics,selector,topology}.rs`
- `app/crowdb-chunkdb/src/selector/{mirror,ec}.rs`
- `app/crowdb-chunkdb/src/placement_repair.rs` and runtime callers
- `app/crowdb-chunkdb/tests/`
- `app/crowdb-diskdb/src/` keepalive usage production
- `lib/crowdb-protocol/src/types/{chunkdb,chunk_task,common}.rs`
- `lib/crowdb-protocol/src/fbs/chunkdb.fbs` and affected generated codecs
- `doc/design/chunkdb/design-crowdb-chunkdb.md`
- `doc/backlog/backlog.md`

## Tests

### Unit

- Policy/config serialization and validation.
- Mirror and EC failure-budget assessment for even and uneven topology.
- Rack-first/node-first deterministic ranking and typed failure modes.
- Missing/stale capacity fallback and in-flight reservation accounting.

### Integration

- DiskDB distinct-disk response validation, rollback, and alternative retry.
- Persisted strip assessment and non-retroactive repair policy.
- Placement task admission, marker reconciliation, claim expiry, bounded
  waiting, and one-fragment fenced moves.
- Protection-preserving passive and active balancing.

### E2E

- 10+2, 20+2, and 40+4 two-rack 4+2-node assessment under both policies.
- Restart after interrupted task admission.
- Expansion to the required rack count and eventual safe convergence.

### Gates

- `pixi run test-chunkdb`
- `pixi run test-diskdb`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`

Validation note: the first policy-config slice passed `pixi run test-chunkdb`
and package-scoped Clippy. Workspace `pixi run rs-lint` is currently stopped
before Clippy by pre-existing unclassified DashMap fields in
`lib/crowdb-kv/src/rpc/snapshot_registry.rs`.

Latest validation: `pixi run rs-fmt -- --check` passed. `pixi run test-chunkdb`
and `pixi run test-diskdb` passed after the physical EC matrix and DiskDB
large-batch reuse path were added. `pixi run rs-lint` remains stopped before
Clippy by the same two pre-existing `snapshot_registry.rs` DashMap findings.
