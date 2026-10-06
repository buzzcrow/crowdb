<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R221: chunkdb — Dynamic service-slot ownership with submission epoch checks

#### Problem

[ChunkDB slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md)
has independent service and storage maps over 1024 ChunkId hash slots. The
Group 0 monitor currently audits a fixed service map; joining, stopping or
losing a server does not redistribute work. A newly ready server can own no
slots, while a failed owner can leave its slots unavailable indefinitely.
Refreshing routing alone cannot revoke delayed writes or background tasks.
The existing per-chunk mutation mutex is retained: it orders local chunkinfo
updates and does not provide distributed ownership fencing.

#### Solution

Implement service-only ownership changes. Chunk IDs, storage bindings and
DiskIO payload remain unchanged. Keep the existing fixed policy available and
require explicit selection of a new dynamic policy; never silently reinterpret
an initialized fixed deployment.

1. Identify each assignment by owner instance ID and a durable, monotonically
   increasing per-slot ownership epoch. Same-ID restart, reassignment and regrant
   advance the affected epochs before serving; returning to an earlier owner
   never reuses an epoch. Incarnation is not an independent admission condition:
   the epoch already distinguishes executions. Incarnation may be diagnostic
   information, but cannot replace durable epoch advancement. Global map
   generation identifies snapshot publication, not each slot's ownership epoch.
2. Capture slot owner/epoch at ChunkDB request entry and recheck immediately
   before every client KV submission. Stop the execution if either changed.
   Updating slot 10 must not reject work on unchanged slot 5. Preserve business
   revision CAS; add no hot-path lock, shared ownership CAS on ordinary writes,
   or handoff wait for accepted writes to drain. Preserve DiskDB's KV fencing.
3. Persist ownership changes and publish complete routing/epoch snapshots in
   Group 0 using revision CAS. Prepare and recover targets before admitting new
   work. Entering client KV is acceptance: submitted work may finish after
   ownership changes. Accept the race between the final local check and
   submission, and old submitted work completing after new-owner work. Strict
   exclusion after cutover is not promised; record conflicts use business CAS.
4. The dynamic Group 0 monitor selects compatible live instances, replaces
   failed owners after a configured grace period, and balances slot counts with
   stable tie-breaking, hysteresis and a per-tick movement budget. Use revision
   CAS for plans and complete service-map publication. Audit failures stop
   changes and expose diagnostics; they never authorize guessed ownership.
5. ChunkDB RPC admission, ChunkStore, TaskStore and reservation writes retain
   the owner/epoch captured for an execution. Check task claim, renewal and
   completion before each submission and stop stale executions. New owners
   recover durable tasks and chunkinfo from the unchanged data group. Existing
   domain separation and local per-chunk mutation ordering remain intact.
6. RangeGuard and client routing publish validated immutable snapshots.
   Reject stale or partial snapshots. Handle pre-submission ownership rejection
   internally: refresh routing and reexecute on the current owner with the same
   request identity, including this process under a newer epoch. Never change an
   old execution's captured epoch and continue with stale state. Preserve or
   deduplicate completed steps in multi-write requests. Retry target preparation
   with bounded backoff under the original deadline; ownership rejection alone
   is not a caller-visible failure. Unknown submitted outcomes require
   reconciliation or deduplication before replay, never blind resubmission.
7. Resume every phase after controller/server failure, lost replies and leader
   replacement. Recovery moves forward without rolling back slot epochs.
   Expose assignment, transition and readiness state through
   existing management status and Console surfaces.

Invariants: one published owner/epoch per slot; complete routing publication;
checks reject observed ownership changes before submission; submitted work may
finish; incarnation adds no independent rejection; acknowledged writes survive;
storage map and payload placement stay unchanged; ordinary owner writes do not
serialize on the ownership record. Slot-count balance does not promise byte or
request-rate balance. Dynamic storage migration is outside this requirement.

#### Dependencies

- Existing fixed-slot maps, Group 0 domain monitor, direct-KV metadata/task
  stores and KV concurrent owner admission are the implementation baseline.
- [R103](R103-chunkdb-range-migration.md) retains storage-slot migration and
  KV-group expansion/shrink. Its service handoff foundation depends on this
  requirement; its storage-copy decisions do not block service-only handoff.
- [R207](R207-chunkdb-repo-metadata-chunk-kv.md) may reuse this fencing contract
  for its future metadata backend; direct-KV remains the supported backend.
- Preserve preceding uncommitted console fixes and loader tooling. They are
  independent work, not permission to reset or discard the checkout.

#### Acceptance

- Three owners and a complete map -> join a fourth compatible ready instance
  under dynamic policy -> bounded eventual slot balance, exactly-once coverage,
  unchanged storage map and no payload copy. Integration test.
- The same deployment under fixed policy -> join or expire a heartbeat ->
  assignments and generations remain unchanged. Integration test.
- Capture an operation on slot 5 -> update only slot 10 -> slot 5 still submits;
  global publication generation is not a per-slot rejection condition. Unit test.
- Restart or regrant slot 5 to the same instance -> durably advance its epoch ->
  old executions fail their next submission check without a separate incarnation
  check; epoch rollback/reuse is rejected. Integration test.
- Reject an execution before submission -> refresh and reexecute with the same
  request ID on the current owner, including the same process -> caller succeeds
  within its original deadline and completed effects are not duplicated.
  Integration test.
- Hold an already submitted old write -> change ownership -> handoff does not
  wait for it; the submitted write may finish and business CAS retains its usual
  conflict semantics. Integration test.
- Pause between final local check and client KV entry -> change ownership ->
  submission may proceed under the explicitly accepted race. Integration test.
- Lose a submitted write's reply or change data-group leader -> reconcile its
  request identity before replay -> no blind duplicate mutation. Integration test.
- Crash or lose replies at each transition phase -> replace monitor/owner and
  retry -> durable progress resumes forward without ownership epoch rollback.
  Integration test.
- Publish overlapping, incomplete or stale maps -> refresh server/client ->
  reject partial authority; bounded routing refresh and unknown-outcome handling
  preserve acknowledged operations. Integration test.
- Run both maintenance domains with a stale worker and persisted claims ->
  handoff -> successor recovers tasks, obsolete executions stop at their next
  submission check, and work cannot cross domains. Integration test.
- Flap heartbeats and repeat concurrent monitor ticks -> reconcile -> grace,
  hysteresis and movement budget prevent unbounded oscillation; CAS publication
  remains idempotent. Integration test.
- Start a cluster through Console, add a diskless node and inspect dynamic
  assignment/readiness -> node joins service ownership and KV/S3/Iceberg round
  trips remain valid without adding local disks. E2E test.

```sh
pixi run cargo test -p crowdb-protocol --test chunk_slot_test --test chunk_slot_authority_test --test chunk_slot_handoff_test
pixi run clean-env && pixi run cargo test -p crowdb-kv-client --test chunk_slot_owner_fence_test --test conditional_retry_test
pixi run clean-env && pixi run cargo test -p crowdb-kv-client --test chunk_slot_handoff_test --test chunk_slot_map_test
pixi run clean-env && pixi run cargo test -p crowdb-kv --test group_test
pixi run clean-env && pixi run cargo test -p crowdb-kv-client --test chunkdb_partition_test
pixi run clean-env && pixi run cargo test -p crowdb-kv-server --test domain_monitor_test
pixi run clean-env && pixi run cargo test -p crowdb-chunkdb --test routing_test --test runtime_scope_test --test task_runtime_scope_test --test lifecycle_scope_test
pixi run clean-env && pixi run cargo test -p crowdb-chunkdb-client --test client_test
pixi run rs-fmt-check
pixi run rs-lint
```
