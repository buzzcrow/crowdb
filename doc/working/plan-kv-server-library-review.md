<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Server and Core Library Review Plan

Upstream requirement: `doc/backlog/R32-kv-custom-rust-rpc.md`

Goal: harden `crowdb-kv` and `crowdb-kv-server` after the completed
gRPC-to-`crowdb-rpc` migration without weakening their performance role.

Disposition rule: implement small, locally provable fixes during review;
record complex findings here or in follow-up requirements; stop for agreement
before protocol, durability, retry-ownership, RPC-isolation, or synchronization
design changes. For every performance-sensitive finding, record the suspected
resource, relevant counters, and same-CPU comparison before changing code.

## Phase 0 — Contract and Baseline

- [x] **Confirm transport replacement**: verify production consensus and KV
  handlers use `crowdb-rpc`/FlatBuffers and the scoped crates have no
  tonic/prost dependency. Files: `lib/crowdb-kv/Cargo.toml`,
  `app/crowdb-kv-server/Cargo.toml`, `lib/crowdb-kv/src/rpc.rs`,
  `lib/crowdb-kv/src/cluster/kv_server.rs`.
- [x] **Refine R32**: retire mixed-gRPC rollout, unavailable legacy benchmark,
  misplaced `NotLeaderHint`, separate-port, and tonic-era stream assumptions.
  Files: `doc/backlog/R32-kv-custom-rust-rpc.md`, `doc/backlog/backlog.md`.
- [ ] **Capture reproducible performance baseline**: record host, CPU affinity,
  build profile, worker/connection counts, dataset, payload mix, fsync mode,
  throughput, and RPC/consensus/WAL/apply latency before hot-path changes.
  Tools: `tools/bench-kv-read-regression.sh`,
  `tools/bench-kv-write-regression.sh`.

## Phase 1 — Consensus and Dedup

- [x] **Confirm request-replay scope and naming**: replace ambiguous `dedup`
  terminology with `RequestIdentity`, `RequestResultCache`, and request-replay
  suppression names. Confirm the proposed minimal contract: same-leader,
  same-process bounded replay suppression for ordinary writes; leader change
  and restart may re-propose idempotent Put/Delete/Batch; ambiguous CAS remains
  `OutcomeUnknown` plus read reconciliation. Decision confirmed: do not
  replicate or persist request-result metadata. Symbols:
  `DedupTag`, `FBAcceptRequest`, `FBChosenNotification`,
  `PxLearner::record_dedup_tags`. Files: `lib/crowdb-kv/src/cluster/`,
  `lib/crowdb-kv/src/rpc/px_rpc_{transport,service}.rs`,
  `lib/crowdb-protocol/src/fbs/kv_consensus.fbs`.
- [~] **Apply request-replay decision**: remove unused follower tag
  transport/state and rename the leader-local cache coherently without adding
  WAL/RPC work. Keep request identities attached to coalescer waiters only
  until the leader publishes the chosen result. Files:
  Phase 1 files plus `cluster/local_replica_apply.rs` and focused tests.
- [ ] **Test the selected retry boundary**: verify same-leader ordinary write
  retry returns the cached slot, leader-change ordinary retry remains data
  idempotent, and ambiguous CAS returns `OutcomeUnknown` for read
  reconciliation. Files: `lib/crowdb-kv/tests/store_test/`,
  `lib/crowdb-kv-client/tests/`, and `tests/common/`.
- [ ] **Make Chosen coverage causal**: assert chosen/applied frontiers and
  engine visibility before/after the notice; cover stale ballot and missing
  value. This is a direct fix once expected frontier behavior is confirmed.
  Files: `lib/crowdb-kv/tests/rpc_migration_test.rs`,
  `lib/crowdb-kv/src/rpc/px_rpc_service.rs`.
- [ ] **Audit consensus fences**: trace ballot, term, membership epoch,
  proposing term, adopted values, quorum short-circuit, and deferred local
  persistence. Directly fix local invariant defects; bring protocol or
  durability changes back for agreement. Files: `cluster/group_prepare.rs`,
  `group_accept.rs`, `group_propose.rs`, `group_election*.rs`,
  `local_replica*.rs`; tests under `lib/crowdb-kv/tests/`.

## Phase 2 — RPC Pressure and Data Movement

- [ ] **Complete send-side pressure handling**: retain the existing main MPSC
  queue plus 256-entry overflow queue and writable-event drain. When both are
  full, keep KV-server-to-KV-server sends in bounded send-side deferred retry
  until success, deadline, or connection failure. Record retained bytes and
  queue time so the retry path cannot become hidden unbounded memory. Files:
  `lib/crowdb-rpc/src/transport/`, `rpc/px_rpc_transport.rs`.
- [ ] **Preserve healthy queue-full connections**: stop invalidating a pool
  generation for `SendQueueFull`, while retaining generation-safe eviction for
  actual connection failure. Add focused coverage for `map_rpc_err` and pool
  generation. Files: `lib/crowdb-kv/src/rpc/px_rpc_transport.rs` and tests.
- [ ] **Keep transport outcomes typed**: distinguish connection failure,
  timeout, backpressure, protocol rejection, and internal invariant errors in
  `PxReplicaError`, proposal/election decisions, and metrics. Files:
  `cluster/replica.rs`, `cluster/remote_replica.rs`,
  `rpc/px_rpc_transport.rs`, `metrics/`.
- [ ] **Add deterministic transport faults**: cover queue full without
  eviction, late old-generation failure, reset/reconnect, deadline expiry,
  pending-call cleanup, and recovery. Files:
  `lib/crowdb-kv/tests/rpc_migration_test.rs`; `lib/crowdb-rpc/ffi/tests/`
  only if the boundary requires it.
- [ ] **Remove avoidable copies**: directly remove the Accept payload
  `to_vec()` temporary and round-trip-test multi-tag payloads; inspect Prepare,
  FetchGap, snapshot, and client handlers for other full-buffer temporaries.
  Required async-lifetime copies remain explicit. Files: `lib/crowdb-kv/src/rpc/`,
  `lib/crowdb-protocol/src/fb_wrappers/kv_consensus.rs`.
- [ ] **Verify shared-worker fairness**: consensus and client RPC continue
  sharing one `RpcServer` and worker pool. Measure per-handler queue wait and
  service latency under mixed client, Accept, Heartbeat, FetchGap, and snapshot
  traffic; fix fairness/admission inside the shared architecture if evidence
  shows starvation. Do not split listeners or workers. Files:
  `cluster/kv_server.rs`, `rpc/`, and `lib/crowdb-rpc/` if needed.
- [ ] **Hand snapshot streaming to R151**: record current single-frame peak
  memory, copies, queue occupation, and consensus tail impact as the R151
  baseline; do not duplicate its implementation in R32. Files:
  `rpc/px_rpc_transport.rs`, `rpc/px_rpc_service.rs`,
  `cluster/group_fetchgap.rs`, `doc/backlog/R151-kv-snapshot-streaming.md`.

## Phase 3 — WAL, Recovery, and Apply

- [ ] **Trace acknowledgement modes**: identify the exact success point for
  normal, `wal_early_ack`, async apply, coalesced, and CAS proposals; verify
  persistence ordering and quorum short-circuit against the durability
  contract. Files: `cluster/group_accept.rs`, `group_propose.rs`,
  `local_replica_accept.rs`, `lib/crowdb-kv/src/wal/`.
- [ ] **Audit replay/frontiers**: verify accepted, chosen, durable, applied,
  highest-seen, safe-slot, and next-slot reconstruction after replay,
  snapshot, gap recovery, and partial WAL tail. Files: `wal/replay.rs`,
  `paxos/learner.rs`, `cluster/local_replica_replay.rs`,
  `cluster/group_fetchgap.rs`, `app/crowdb-kv-server/src/recovery/`.
- [ ] **Test crash boundaries**: add deterministic restart tests only for
  uncovered acknowledgement/frontier combinations; assert both data and
  metadata. Files: `lib/crowdb-kv/tests/group_test/`, `store_test/`,
  `app/crowdb-kv-server/tests/{startup,restore}_test.rs`.

## Phase 4 — Concurrency and Performance

- [ ] **Inventory synchronization**: classify production locks, channels,
  semaphores, atomics, and spawned tasks by hot path/lifecycle/background.
  Record protected state, critical-section work, awaits, ordering, cache-line
  sharing, and observed contention. Files: `lib/crowdb-kv/src/{cluster,paxos,rpc,wal,metrics}/`.
- [ ] **Audit cancellation and wakeups**: check coalescer ownership,
  admission, quorum drain, read-index gates, apply wakeups, FetchGap, election,
  and maintenance for stranded work, lost wakeups, starvation, or unbounded
  fan-out. Files: `group_coalesce.rs`, `group_inflight.rs`, `group_accept.rs`,
  `local_replica_apply.rs`, `group_maintenance.rs`.
- [ ] **Triage performance findings**: directly fix redundant local
  allocation/copy/lookup/logging work with tests. For locks, extra RPC/WAL,
  worker topology, queues, or scheduling, present counter evidence and options
  before implementation. Update this plan with each disposition.
- [ ] **Run same-CPU comparisons**: after a hot-path change, rerun the focused
  benchmark with identical affinity/configuration and compare throughput plus
  p50/p95/p99 stage latency and pressure counters.

## Phase 5 — Server Lifecycle and Operations

- [ ] **Review RPC lifecycle ownership**: determine whether `server_state`,
  `rpc_server_state`, and `client_rpc_server_state` remain distinct after the
  unified-server migration. Present ownership redesign before changing it;
  directly remove only proven residue. Files: `cluster/px_kv_store.rs`,
  `cluster/kv_server.rs`.
- [ ] **Verify endpoint/config propagation**: ensure port `0` advertises the
  actual bound endpoint and startup, restore, system-init, and management
  paths apply identical configuration before serving. Files:
  `cluster/kv_server.rs`, `app/crowdb-kv-server/src/main.rs`,
  `mgmt/system_init.rs`, `mgmt/store_ops.rs`, `recovery/`.
- [ ] **Review topology mutation/persistence**: trace create/delete,
  membership, handoff, node config, group-0 reconciliation, and partial
  failure rollback. Files: `cluster/group_membership.rs`, `node_config.rs`,
  server `mgmt/` and `recovery/reconcile.rs`.
- [ ] **Review shutdown ordering**: verify RPC, monitors, metrics,
  maintenance, election, FetchGap, operations, and signals stop idempotently
  in dependency order and expose failed joins. Files: server `background/`,
  `main.rs`, `cluster/group_maintenance.rs`, `cluster/kv_server.rs`.

## Phase 6 — Observability and Cleanup

- [ ] **Map signals to stages**: ensure status/counters distinguish admission,
  coalescing, Prepare, Accept quorum, RPC queue/transport, WAL enqueue/persist,
  apply, recovery, and maintenance. Remove ambiguous/permanently-zero fields;
  add only confirmed missing signals off critical sections. Files:
  `lib/crowdb-kv/src/metrics/`, `cluster/status.rs`, server metrics/status.
- [ ] **Remove migration residue**: remove blanket `dead_code` allowances and
  stale phase, tonic, LearnerStream, separate-port, and port-offset comments
  after behavior is covered. Files: KV `rpc/`, `cluster/`, tests, server.
- [ ] **Reconcile architecture docs**: update only confirmed design and split
  substantial unrelated findings into new requirements. Files:
  `doc/design/kv/`, `doc/backlog/backlog.md`.

## Phase 7 — Verification and Closure

- [ ] **Run focused checks per fix**: use the smallest affected test target
  and preserve complete output.
- [ ] **Run scoped gates**: `pixi run cargo test -p crowdb-kv`;
  `pixi run cargo test -p crowdb-kv-server`;
  `pixi run cargo fmt --all -- --check`;
  `pixi run cargo clippy -p crowdb-kv -p crowdb-kv-server --all-targets -- -D warnings`.
- [ ] **Run performance sentinels**:
  `pixi run bash tools/bench-kv-read-regression.sh` and
  `pixi run bash tools/bench-kv-write-regression.sh` on the recorded CPU setup;
  explain material deltas with stage counters.
- [ ] **Close R32**: leave no critical/high finding; fix or explicitly split
  medium findings; update the index; delete this plan and requirement under
  `/implement-requirement` after verification.

## Consolidated File List

- `lib/crowdb-kv/src/{cluster,paxos,rpc,wal,metrics}/`
- `lib/crowdb-kv/tests/`
- `app/crowdb-kv-server/src/`
- `app/crowdb-kv-server/tests/`
- `lib/crowdb-protocol/src/fbs/kv_consensus.fbs`
- `lib/crowdb-protocol/src/fb_wrappers/kv_consensus.rs`
- `lib/crowdb-rpc/ffi/` only for a confirmed KV transport boundary issue
- `doc/design/kv/`, `doc/backlog/R32-kv-custom-rust-rpc.md`,
  `doc/backlog/backlog.md`

## Tests

Unit tests:

- FlatBuffer multi-tag/payload round trip.
- Queue-full classification without connection eviction.
- Timeout/error mapping and pending-call cleanup.
- Newly uncovered fence, frontier, coalescer, and admission edge cases.

Integration tests:

- Three-replica coalesced dedup across failover.
- Accepted-but-unchosen identity remains unpublished.
- Chosen notification causally advances chosen/applied state.
- Connection reset, generation-safe replacement, and retry.
- WAL acknowledgement crash/replay cases.
- Store create/restore/reconfigure/ephemeral-port/shutdown lifecycle.

E2E tests:

- Existing KV read/write regression scripts on controlled CPU resources, with
  latency/counter attribution for material changes.
