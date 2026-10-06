<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R221: chunkdb — Dynamic service-slot ownership with durable fencing

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

1. Define a durable service incarnation and per-slot authority generation.
   A process restart under the same instance ID has a new incarnation.
   Every active assignment identifies the instance, incarnation and generation;
   routing publication still consists of complete validated bitmap maps.
2. Extend the existing KV owner-write admission to a distinct ChunkDB slot
   fence namespace in the selected data group. Ordinary writes only verify
   applied owner identity; they do not mutate or CAS a shared fence record.
   Ownership CAS closes admission, drains old accepted proposals through apply,
   and durably revokes the old identity. Preserve cancellation, leader-tenure
   and unknown-outcome recovery barriers. Business-record CAS remains required.
3. Persist service handoff progress in Group 0 before side effects. The phases
   are Prepare -> Fence -> Publish -> Activate. Fence every affected storage
   destination before publishing target ownership. A prepared target cannot
   serve writes. If the old owner is unreachable, durable KV fencing proves
   revocation; no response from the old process is required for safety.
4. The dynamic Group 0 monitor selects compatible live incarnations, replaces
   failed owners after a configured grace period, and balances slot counts with
   stable tie-breaking, hysteresis and a per-tick movement budget. Use revision
   CAS for plans and complete service-map publication. Audit failures stop
   changes and expose diagnostics; they never authorize guessed ownership.
5. ChunkDB RPC admission, ChunkStore, TaskStore and reservation writes retain
   the assignment identity captured for an operation. Revoke task claim,
   renewal and completion under an old generation. Newly activated owners
   recover durable tasks and chunkinfo from the unchanged data group. Existing
   domain separation and local per-chunk mutation ordering remain intact.
6. RangeGuard and client routing publish validated immutable snapshots.
   Reject stale or partial generations, return explicit unavailable/routing
   rejection during handoff, and bound refresh for NotMyRange. Unknown mutation
   outcomes require request-identity reconciliation, never blind resubmission.
7. Resume every phase after controller/server failure, lost replies and leader
   replacement. Post-fence recovery only moves forward; it cannot restore an
   old incarnation. Expose assignment, transition and readiness state through
   existing management status and Console surfaces.

Invariants: exactly one active identity per slot; complete routing publication;
new activation follows durable revocation; acknowledged writes survive handoff;
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

- Three owners and a complete map -> join a fourth compatible ready incarnation
  under dynamic policy -> bounded eventual slot balance, exactly-once coverage,
  unchanged storage map and no payload copy. Integration test.
- The same deployment under fixed policy -> join or expire a heartbeat ->
  assignments and generations remain unchanged. Integration test.
- Fail or partition an owner -> pass grace and activate a successor -> old RPCs,
  metadata writes, reservations and task renewals/completions are rejected by
  durable authority, including a restart using the same instance ID. Integration test.
- Hold an accepted old write before apply -> start handoff -> fence waits and
  target recovery includes the acknowledged write; unrelated writes proceed
  without changing fence revision. Integration test.
- Cancel an ownership caller or change data-group leader with unresolved old
  proposals -> resume -> no activation before the recovery barrier resolves
  old identity; request identity prevents duplicate mutation. Integration test.
- Crash or lose replies at each transition phase -> replace monitor/owner and
  retry -> durable progress resumes forward, never two active writers or a
  rollback to an obsolete generation. Integration test.
- Publish overlapping, incomplete or stale maps -> refresh server/client ->
  reject partial authority; bounded routing refresh and unknown-outcome handling
  preserve acknowledged operations. Integration test.
- Run both maintenance domains with a stale worker and persisted claims ->
  handoff -> successor recovers its tasks and obsolete work cannot cross domain
  or complete under the successor identity. Integration test.
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
