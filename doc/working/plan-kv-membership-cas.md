<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Membership CAS Plan

Upstream: [R229](../backlog/R229-kv-membership-conflict.md).
Goal: publish complete members through epoch CAS and reject concurrent/stale
configuration submissions before they can change durable or running groups.

Implementation resumed by the user on 2026-10-10. The protocol/client authority,
server guard, cross-node epoch fencing, recovery scan, topology state display,
idempotent replay checks, and exact Installing-resume path are implemented.

## Protocol and shared authority

- [x] **Implement record and KV client CAS**: add complete member/epoch/state
  types, canonical validation, a typed group-members key and linearizable
  snapshots. Fresh creation starts Installing at epoch 1; changes require the
  expected Ready epoch. Preserve previous members during installation for
  removal/restart scope. Complete only the exact Installing snapshot by CAS.
  Files: protocol `kv_membership.rs`, `key/kv_cluster.rs`;
  KV client `membership.rs`; protocol/client integration tests.
- [x] **Wire new group authority**: group creation now publishes an
  `Installing` complete record before the legacy topology projection and
  completes it only after projection success; add/remove replica operations
  CAS the complete successor before peer wiring and complete it afterward.
  Existing readers retain a fallback for pre-authority records. Files:
  Console `ops/context.rs`, `ops/kv_logical.rs`,
  `ops/kv_logical/publication.rs`.

## Server submission and propagation

- [x] **Guard before durable write**: management add/remove/batch handlers now
  serialize persistence and publication per local group through a
  management-only guard. Expected-epoch request validation and cross-node
  authoritative submission are wired through the management header. Files: KV server
  `mgmt/replica_ops.rs`, KV `cluster/px_kv_store.rs`.
  atomically protect the persistence-to-publication interval. Include remote
  add/remove/batch, replacement, restore and reconcile. Inspect lifecycle
  cancellation and persisted config identity before selecting the guard.
  Files: KV server `mgmt/replica_ops.rs`, `recovery/group_rebuild.rs`;
  KV `cluster/px_kv_store.rs`, `group_config.rs`, `node_config.rs`.
- [x] **Propagate one authorized epoch**: KV client management submission sends
  complete configuration/epoch; Console maps conflicts to 409. Confirm exact
  installation/fencing before Ready. Capture no-effects vs unknown failures;
  never clear Installing on timeout. Files: client `transport/cluster.rs`;
  Console `ops/kv_logical.rs`; Web `mgmt/replica_ops.rs`.
- [x] **Resume after interruption**: restore the durable successor, retain the
  Installing record's previous participants during reconciliation, and reuse
  an exact Installing successor after a lost coordinator reply. Delayed prior
  configuration remains fenced by the successor epoch.

## Acceptance and completion

- [x] **Control-plane races**: real Group-0 CAS concurrent clients, Installing
  rejection, stale expected epoch, exact completion, stale completion and
  independent groups. Server replay coverage also proves same-epoch matching
  payloads are idempotent while conflicting payloads are rejected before write.
- [x] **Crash and unknown outcome**: lost CAS/reply confirmation and partial
  installation scope are covered by the client/publication/reconciliation
  suites; exact Installing successors resume without allocating a new epoch.
- [x] **UI and docs**: sequential submissions remain serialized per group,
  HTTP 409 is surfaced as a console conflict, and the permanent reconfiguration
  document now defines the complete Group-0 authority and replay contract.
- [~] **Gates and cleanup**: affected protocol/client/server/Console tests,
  Rust fmt/clippy and affected UI spec; requirement commits include only this
  requirement's coherent work. Preserve other uncommitted changes. Never push.
  Delete R229, its index entry and this plan only after all acceptance passes.

## Verification

- Protocol 2/2 and real Group-0 CAS 1/1 passed, zero ignored. Coverage includes
  independent clients, Installing rejection, stale completion, exact Ready
  completion confirmation and independent groups. Server management API 38/38,
  reconciliation 9/9, and all console-shared tests passed. Workspace fmt and
  affected-crate clippy passed with `-D warnings`.
- Protocol: `pixi run cargo test -p crowdb-protocol --test kv_membership_test`.
- Shared CAS: `pixi run cargo test -p crowdb-kv-client --test membership_cas_test`.
- Server/Console: commands from R229; runtime suites sequential and owned.
- UI: the latest full `test-console-ui` run reached 46/51 tests. It still
  recorded one higher-epoch bootstrap conflict before the final bounded
  bootstrap allowance, plus replica-delete and Iceberg timing/data-flow
  failures; the focused server gate passes after the final allowance.
- Gates: `pixi run rs-fmt-check` and affected-crate clippy with `-D warnings`.

## Adjacent unfinished work

Native lifecycle response still exceeds its unchanged 3s budget; investigate
actual recovery cost rather than claiming fsync or weakening tests. Complete
Console/native matrix remains separate from this requirement. Chunk-KV cutover
core proposals retain their own review boundary.
