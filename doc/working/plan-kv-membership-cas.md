<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Membership CAS Plan

Upstream: [R229](../backlog/R229-kv-membership-conflict.md).
Goal: publish complete members through epoch CAS and reject concurrent/stale
configuration submissions before they can change durable or running groups.

Execution is deferred by the user on 2026-10-09. The first protocol/client task
is committed as `339ef482`; do not resume the remaining tasks until requested.

## Protocol and shared authority

- [x] **Implement record and KV client CAS**: add complete member/epoch/state
  types, canonical validation, a typed group-members key and linearizable
  snapshots. Fresh creation starts Installing at epoch 1; changes require the
  expected Ready epoch. Preserve previous members during installation for
  removal/restart scope. Complete only the exact Installing snapshot by CAS.
  Files: protocol `kv_membership.rs`, `key/kv_cluster.rs`;
  KV client `membership.rs`; protocol/client integration tests.
- [ ] **Wire new group authority**: creation, bootstrap and membership readers
  use complete records; remove unconditional/member-per-key mutation authority
  without legacy fallback. Files: client `transport/cluster.rs`,
  `hardware/sysmd.rs`; Console `ops/kv_logical/publication.rs`.

## Server submission and propagation

- [ ] **Guard before durable write**: validate epoch/full configuration and
  atomically protect the persistence-to-publication interval. Include remote
  add/remove/batch, replacement, restore and reconcile. Inspect lifecycle
  cancellation and persisted config identity before selecting the guard.
  Files: KV server `mgmt/replica_ops.rs`, `recovery/group_rebuild.rs`;
  KV `cluster/px_kv_store.rs`, `group_config.rs`, `node_config.rs`.
- [ ] **Propagate one authorized epoch**: KV client management submission sends
  complete configuration/epoch; Console maps conflicts to 409. Confirm exact
  installation/fencing before Ready. Capture no-effects vs unknown failures;
  never clear Installing on timeout. Files: client `transport/cluster.rs`;
  Console `ops/kv_logical.rs`; Web `mgmt/replica_ops.rs`.
- [ ] **Resume after interruption**: restore durable successor, reconcile the
  exact authoritative epoch and reject delayed prior configuration/completion.
  Required removal/fencing scope must survive coordinator restart.

## Acceptance and completion

- [ ] **Control-plane races**: real Group-0 CAS concurrent clients, Installing
  rejection, stale expected epoch, exact completion, stale completion and
  independent groups. Then controlled local persistence/publication overlaps.
- [ ] **Crash and unknown outcome**: lost CAS/reply, partial installation,
  restart after durable write and before memory publication; delayed old request.
- [ ] **UI and docs**: sequential submissions and explicit conflict across two
  UI servers. Update permanent reconfiguration/console specifications once
  implemented. Keep topology setup separate from concurrent conflict coverage.
- [ ] **Gates and cleanup**: affected protocol/client/server/Console tests,
  Rust fmt/clippy and affected UI spec; requirement commits include only this
  requirement's coherent work. Preserve other uncommitted changes. Never push.
  Delete R229, its index entry and this plan only after all acceptance passes.

## Verification

- Protocol 2/2 and real Group-0 CAS 1/1 passed, zero ignored. Coverage includes
  independent clients, Installing rejection, stale completion, exact Ready
  completion confirmation and independent groups. Workspace fmt and affected
  protocol/client all-target clippy passed. Production submission is not wired
  yet; this does not establish server persistence/publication safety.
- Protocol: `pixi run cargo test -p crowdb-protocol --test kv_membership_test`.
- Shared CAS: `pixi run cargo test -p crowdb-kv-client --test membership_cas_test`.
- Server/Console: commands from R229; runtime suites sequential and owned.
- UI: affected replica dialog tests and topology/conflict browser scenarios.
- Gates: `pixi run rs-fmt-check` and affected-crate clippy with `-D warnings`.

## Adjacent unfinished work

Native lifecycle response still exceeds its unchanged 3s budget; investigate
actual recovery cost rather than claiming fsync or weakening tests. Complete
Console/native matrix remains separate from this requirement. Chunk-KV cutover
core proposals retain their own review boundary.
