<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Relocation Handoff Plan

Upstream: [R80](../backlog/R80-diskdb-rebalance.md) and
[R97](../backlog/R97-chunkdb-advanced-placement-strategies.md).

Goal: move one exact live block through reserve, copy, fsync, owner-fenced
publication, target confirmation, and source release with restart-safe
ownership at every phase.

## Phase 1 — Durable contracts

- [x] **Relocation identities and outcomes**: add a versioned handoff request
  carrying deterministic operation ID, owner chunk, exact source incarnation,
  and exact tentative target incarnation; add `Accepted`, `Published`,
  `Stale`, and `Rejected` owner outcomes. Reserve new message IDs without
  changing existing values. Files: `lib/crowdb-protocol/src/fbs/`, typed RPC
  models and protocol tests.
- [x] **DiskDB relocation journal**: persist one value keyed by exact source
  incarnation. Record target, `Reserved`/`Copied`/`Accepted`/`Published`/
  `SourceFreed`/`Discarded` phase, timestamps, and last error. A phase write
  precedes every externally visible next action. Files:
  `lib/crowdb-protocol/src/key/`, DiskDB value codecs and KV tests.
- [x] **ChunkDB durable claim**: add one deterministic task kind keyed by the
  handoff operation. Its checkpoint owns the tentative target before an
  `Accepted` response and remains visible to the owner-disposition scanner.
  Files: chunk-task protocol, `app/crowdb-chunkdb/src/task/`, tests.

## Phase 2 — Owner publication

- [x] **Idempotent handoff RPC**: route the request to the current ChunkDB
  range owner. Duplicate delivery returns the durable task result; malformed,
  owner-mismatched, or geometry-mismatched requests are rejected without
  mutation. Files: `lib/crowdb-chunkdb-client/`, ChunkDB RPC service, tests.
- [x] **Fenced task execution**: locate the exact source in current chunk
  metadata, checkpoint its strip index and current `modify_ts`, replace only
  that source with the exact target through tentative publication, confirm the
  target after CAS success, and checkpoint `Published`. If the source is gone,
  return `Published` only when the same target is already present; otherwise
  return `Stale`. Never free the source from ChunkDB. Files: ChunkDB relocation
  task handler, lifecycle integration and crash/retry tests.
- [x] **Runtime wiring**: register the task handler in the existing bounded
  executor and expose relocation checkpoints through
  `SegmentOwnerResolver::TaskPending`. Files: ChunkDB main/task wiring and
  owner tests.

## Phase 3 — Physical mover

- [x] **Reserve and copy**: select an eligible target, create an exact
  tentative BusyBlock for the same owner, persist `Reserved`, copy the source
  bytes through DiskIO, fsync the target, then persist `Copied`. No metadata
  handoff occurs before fsync. Files: `app/crowdb-diskdb/src/rebalance/`,
  DiskIO client wiring, tests.
- [x] **Deliver and finalize**: retry owner delivery after restart. Persist
  `Accepted`; on `Published`, verify/confirm the exact target before writing
  the normal source free record. On `Stale`, discard the target and retain the
  source. On `Rejected`, quarantine both until exact ownership reconciliation,
  because the target may already be referenced. Transient routing/RPC failures
  retain both incarnations.
  Files: DiskDB relocation worker and KV/RPC fault tests.
- [x] **Paced planner integration**: feed relocation jobs serially from the
  rebalance planner with bounded batch/concurrency and dynamic inter-zone
  pacing; never scan or copy all disk-groups concurrently. Files: DiskDB
  rebalance background task/config/metrics/tests.

## Phase 4 — Cross-domain acceptance

- [x] **Crash matrix**: restart after target reservation, copy, fsync,
  accepted handoff, metadata publication, target confirmation, and source-free
  persistence. Each restart converges without double publication or freeing a
  referenced source. Files: DiskDB/ChunkDB full-stack tests.
- [x] **R97 planner consumer**: allow the ChunkDB cross-domain planner to
  request an R80 relocation while preserving rack/node/disk guarantees and
  limiting each action to one fragment. Files: R97 planner, client wiring,
  10+2/20+2/40+4 E2E tests.

## Cleanup status

- Affected protocol, DiskDB, ChunkDB, and client gates pass, including the
  every-phase restart matrix and the cross-domain EC matrix.
- Keep this plan until the workspace `rs-lint` gate is clean. It is currently
  blocked outside R80/R97 by unclassified `sessions` and `expired` DashMap
  fields in `lib/crowdb-kv/src/rpc/snapshot_registry.rs`.

## Files

- `lib/crowdb-protocol/src/{fbs,types,key}/`
- `lib/crowdb-chunkdb-client/`
- `app/crowdb-chunkdb/src/{task,service,lifecycle}/`
- `app/crowdb-diskdb/src/{rebalance,ddb_kv_client,service}/`
- `app/crowdb-{chunkdb,diskdb}/tests/`

## Tests

### Unit

- Exact-incarnation keys and typed protocol round trips.
- Deterministic operation/task identity and phase transition validation.
- Duplicate delivery and terminal outcome mapping.

### Integration

- Task checkpoint owns the target before acknowledgement.
- Source CAS races resolve as Published or Stale without ambiguous cleanup.
- DiskIO copy/fsync ordering precedes owner delivery.
- Restart resumes each persisted phase and transient failures retain data.

### E2E

- One same-group relocation completes copy → publish → confirm → source free.
- Owner deletion and competing replacement discard only the tentative target.
- R97 cross-rack/node/disk moves converge for 10+2, 20+2, and 40+4.

## Gates

- `pixi run test-diskdb`
- `pixi run test-chunkdb`
- `pixi run test-chunkdb-client`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`
