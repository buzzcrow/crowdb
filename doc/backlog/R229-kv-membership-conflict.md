<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R229: KV membership — Complete members record with epoch CAS

## Problem

[Reconfiguration design](../design/kv/design-crowdb-kv-reconfiguration.md)
recommends adding members one at a time. Console currently reads membership,
wires peers, then CAS-creates individual replica records. Different replica
keys do not conflict, so two UI servers can concurrently change the same group.

KV server remote mutation rebuilds a captured group, persists its configuration,
then replaces the running group. Concurrent replacements can start from the
same snapshot. Checking for conflict after writing disk is too late.

The original concurrent-add topology case fails with two of three ready groups.
Sequential additions pass the complete five-case spec. Controlled regressions
must establish the server interleaving; no data-loss claim is established.

## Solution

The approved model is one authoritative Group-0 record per store/group,
containing the complete members list and a monotonically increasing epoch.
The epoch identifies the configuration change; no separate operation ID,
admission record, or administrative version is introduced. Installation state
is kept with this record so an unfinished change rejects the next submission.

- **MEMBERSHIP-CAS:** Membership mutation APIs accept an expected epoch and the
  complete requested members list. KV client submits through Group-0 CAS before
  changing any peer. CAS checks the observed record revision, expected epoch
  and Ready state together. The server assigns the successor epoch. A stale
  epoch or Installing state returns explicit 409 Conflict. Different groups
  remain independent; losing callers do not automatically retry on a new epoch.
- **MEMBERSHIP-INSTALL:** Successful CAS publishes the successor members and
  epoch in Installing state. KV client propagates that exact complete
  configuration through the existing reconfiguration procedure. Keep enough
  installation scope to reconcile removed as well as retained/new participants
  after restart. Installation follows existing consensus fencing and catch-up
  rules; publishing desired members alone is not proof of completed cutover.
- **MEMBERSHIP-SERVER:** Each KV server checks and atomically guards configuration
  submission before persistence, covering persistence through memory publication.
  Lower epoch is rejected. Same epoch and same complete configuration is an
  idempotent replay; same epoch with different configuration is rejected.
  A higher epoch must be the authorized successor, not an arbitrary caller
  value. Guard add/remove, batch wiring, replacement and reconciliation.
- **MEMBERSHIP-ORDER:** Validate and acquire the local submission guard, persist
  the authorized successor, then publish that same successor. A losing/stale
  request must not modify either durable or running configuration.
  Checking only at in-memory group replacement is insufficient.
- **MEMBERSHIP-COMPLETE:** After all required installation/fencing confirmations,
  CAS the exact Installing record to Ready without changing its epoch/members.
  A stale completion cannot clear a newer record. Only Ready permits the next
  membership mutation. Same-group UI additions await completion and refresh
  membership before another submission.
- **MEMBERSHIP-RECOVERY:** Timeout or lost reply is an unknown outcome, not
  permission to clear Installing. Linearizably reread the authoritative record
  and confirm the exact epoch/configuration. Partial installation resumes that
  epoch; no rollback to older members and no time-based release. Restart after
  local persistence restores the durable successor and reconciles with Group 0.
  An interrupted completion is confirmed or retried by exact-record CAS.
- **MEMBERSHIP-FORMAT:** Use the new complete-record and local epoch format
  directly. No legacy migration, compatibility reads, or old unconditional
  mutation path. Endpoint and voting changes participate in the same complete
  configuration/epoch checks; consensus integration must be reviewed explicitly.
- **MEMBERSHIP-COST:** Extra CAS, reads and coordination belong to management
  operations, which may be slower. Add no data-path locks or wait queues.
  Any proposed new lock requires separate contention/ordering review.
- **MEMBERSHIP-UI:** Keep submission sequential for each group, show 409 as a
  conflict, refresh current configuration, and never silently resubmit changes.

1. Define the complete record and epoch/Installing/Ready API contract in
   protocol types, and review integration with existing reconfiguration.
2. Implement shared CAS submission and exact-epoch recovery in the corresponding
   KV-client operation; route Console membership changes through it.
3. Protect KV server durable configuration submission and publication.
4. Verify races, lost replies, partial installation and restart boundaries.
5. Update permanent reconfiguration/UI documents after implementation; maintain
   separate sequential topology and concurrent rejection coverage.

## Dependencies

- Existing Group-0 CAS and linearizable reads.
- Existing local configuration durability, catch-up and consensus fencing.
- Independent of Chunk-KV cutover and deferred tree metrics.
- Sequential operator changes are the interim behavior, not proof of
  cross-UI protection. Implementation is not yet complete.

## Acceptance

- Two UI servers read one Ready epoch -> submit different configurations ->
  one CAS succeeds and the other returns 409 before peer writes
  (**MEMBERSHIP-CAS**). Integration test
- A successor is Installing -> submit another change even with its current
  epoch -> explicit 409 until exact completion (**MEMBERSHIP-COMPLETE**).
  Integration test
- Force overlapping local submissions -> losing request changes neither disk
  nor running configuration; independent groups progress
  (**MEMBERSHIP-SERVER**, **MEMBERSHIP-ORDER**, **MEMBERSHIP-COST**). Integration test
- Replay same epoch/configuration, then same epoch/different configuration and
  lower epoch -> respectively idempotent success and explicit conflicts;
  authorized higher epoch installs through reconfiguration
  (**MEMBERSHIP-SERVER**, **MEMBERSHIP-FORMAT**). Integration test
- Lose CAS/installation/completion replies -> reread and resume exact epoch ->
  no duplicate configuration change or premature Ready
  (**MEMBERSHIP-RECOVERY**). Integration test
- Crash during partial fan-out or after local persistence before publication ->
  recover the durable successor and finish the current epoch; reject delayed
  older configuration and stale completion (**MEMBERSHIP-RECOVERY**). Integration test
- Remove a participant -> restart during propagation -> installation scope
  preserves the required removal/fencing proof before Ready
  (**MEMBERSHIP-INSTALL**, **MEMBERSHIP-COMPLETE**). Integration test
- Two UI sessions change one group -> loser displays conflict; winner waits for
  completion and refreshes before its next change (**MEMBERSHIP-UI**). E2E test

Verification:

- `pixi run cargo test -p crowdb-kv-server --tests`
- `pixi run cargo test -p crowdb-kv-client --tests`
- `pixi run cargo test -p crowdb-console-shared --tests -- --test-threads=1`
- `pixi run test-console-ui`
- `pixi run rs-fmt-check`
- `pixi run cargo clippy -p crowdb-kv-server -p crowdb-kv-client -p crowdb-console-shared --all-targets -- -D warnings`
