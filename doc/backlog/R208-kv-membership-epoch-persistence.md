<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R208: KV — Crash-safe membership epoch persistence

## Problem

`PxGroup` increments `membership_epoch` in memory when a voting member is
added, removed, promoted, or demoted. The management handlers later rebuild
the group and call `persist_config()`, which writes members and the epoch to
`node-config.json`. A process crash between the in-memory mutation and that
write restores the previous membership and epoch. A failed persistence is
also logged while the changed group continues serving.

This violates the reconfiguration design's crash contract: after a voting-set
change, a restarted replica must not advertise an older fencing epoch. A stale
replica can reject current traffic indefinitely or, if membership and epoch
are recovered through different paths, combine an old configuration with a
new fence token.

The root design is [`design-crowdb-kv-reconfiguration.md`](../design/kv/design-crowdb-kv-reconfiguration.md), especially §2 and §9. The current
implementation is split between `lib/crowdb-kv/src/cluster/group_membership.rs`,
`app/crowdb-kv-server/src/mgmt/replica_ops.rs`, and
`lib/crowdb-kv/src/cluster/node_config.rs`.

## Solution

Make membership mutation and its fencing token one durable operation.

1. Add a membership-update API in `group_membership.rs` that computes the
   complete next member list and next epoch before publishing the new in-memory
   group view.
2. Persist the complete `(members, membership_epoch, term, replica_id)` entry
   through `NodeConfigStore` before the rebuilt group becomes serving. A write
   failure aborts the management operation, returns the error to the caller,
   and leaves the old group active. The new in-memory membership is never
   published ahead of its durable record.
3. Keep `node-config.json` updates atomic for both the group entry and the
   shared file. Ensure the durability boundary includes the required file and
   directory syncs, and prevent concurrent read-modify-write calls from
   overwriting another group's update.
4. On restore, reject a malformed or internally inconsistent group entry rather
   than silently replacing it with an empty default. Recovery must either use
   the last complete entry or fail closed and require operator repair.
5. Preserve the exact-match epoch fence and upward adoption behavior; this
   requirement changes the persistence boundary, not the Paxos message rule.

The intended sequence is:

```text
management request -> durable node-config entry -> publish rebuilt group
                  -> peers converge at the persisted membership_epoch
```

## Dependencies

- Incoming: `design-crowdb-kv-reconfiguration.md` §2, §6, and §9.
- Outgoing: group restore in `app/crowdb-kv-server/src/recovery/` must consume
  the same complete entry.
- Existing `NodeConfigStore` remains the on-disk format unless an atomic
  versioned envelope is required; unlanded format changes must include a
  compatibility reader.

## Acceptance

1. Start with epoch 10, apply a voting-set change, kill the process before the
   management call returns, restart, and assert that the recovered epoch and
   member list are the new complete pair. Invariant: fencing tokens do not
   regress after restart. **Integration test**
2. Inject a node-config write failure during a membership change and assert that
   the operation fails and the old group remains active. Invariant: an
   unpersisted membership view is never published. **Unit test**
3. Run concurrent updates for two groups in one `node-config.json` and assert
   that neither group's complete entry is lost. Invariant: atomic persistence
   covers the whole shared config file. **Integration test**
4. Corrupt or truncate the config file and restart; assert that recovery fails
   closed with an actionable error and does not construct a quorum-1 group from
   an empty default. Invariant: membership and epoch are restored together or
   not restored. **Integration test**
5. Run the existing membership epoch mismatch fan-out test and assert that
   exact-match fencing and upward adoption remain unchanged. Invariant: durable
   persistence does not weaken stale-message rejection. **Unit test**

Run the relevant gates with:

```text
pixi run cargo fmt --all -- --check
pixi run cargo clippy -p crowdb-kv -p crowdb-kv-server --all-targets -- -D warnings
pixi run cargo test -p crowdb-kv --test node_config_test
pixi run cargo test -p crowdb-kv --test group_test membership_epoch_fence
```
