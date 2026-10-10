<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container layer E2E

This component owns progressively migrated real-server acceptance. The first
profile is KV: three packaged `crowdb-kv-server` processes with block tree storage,
block-device WAL and the existing `e2e` election preset. It consumes the shared
`crowdb-node` OCI runtime image; it never starts a host server substitute.

## Run

```sh
pixi run test-container-e2e-fixture
pixi run clean-env
pixi run test-container-e2e --layer kv --concurrency 2 --cpus 4 --memory-mib 4096
```

Build/import the shared OCI image first. An absent image, missing Docker daemon,
unsupported daemon, stale image/source revision or inadequate budget fails the
required test. There is no successful missing-prerequisite skip.
`--layer all` currently selects the only implemented layer, KV.

`--concurrency` is a cap. Each slot reserves 2 CPUs and 2048 MiB for three
0.5-CPU/512-MiB servers and one equally bounded client. The effective cap also
accounts for Docker daemon capacity. Two slots run two isolated fixtures at once;
one slot runs the CRUD/recovery scenario without claiming concurrent isolation.
The initial batch uses at most two slots. CI sets `--require-isolation` and
fails if two slots are unavailable, so the required isolation check cannot skip. Host/hardware and containerd test
profiles remain pending.

## Lifecycle and endpoints

- Each fixture owns a UUID-labeled bridge network, three named data volumes,
  three server containers and its client containers. No global prune or shared
  implicit cluster is used. Cleanup verifies ownership labels and exact names.
- Network-local management is fixed at `7000`; store RPC uses `7001..7011`.
  Every fixture reuses the same names, store/replica IDs and port pool inside its
  own network. New fixtures do not call the legacy offset/probe/flock allocator.
- Only management is temporarily published, with Docker-assigned ports bound
  to `127.0.0.1`. The host receives those endpoints from container inspection.
- RPC clients run in a sidecar on the owned network. Peer topology rewrites
  wildcard listen addresses to inspected bridge IPs (the C++ RPC transport
  expects numeric addresses) while retaining actual
  returned store ports. RPC ports are never host-published.
- The KV profile directly tests a user-data group, as the original scenario did.
  It does not create Group 0, run discovery, SSH, monitor or console, and makes
  no claim about management bootstrap authority. Deployment profiles continue
  to own that coverage until their migration.
- SIGKILL/restart retains the same volumes, network identities and internal
  endpoints. Runtime-assigned host mappings are refreshed after restart because
  Docker can change them; container service ports and peer IPs remain fixed.
  Monitor automatic recovery is absent in this layer profile.
- Client compilation uses locked Cargo dependencies; its executable and Pixi
  shared-library dependencies are mounted read-only in the shared image.
  Normal component Rust gates leave the container-only test ignored; this
  runner explicitly executes it and missing fixture setup fails.
- Interruption or partial startup gathers diagnostics and tears down owned
  allocations. Operations have bounded deadlines; SIGINT/SIGTERM abort runner
  coordination and cleanup follows in-flight operations. SIGKILL cannot execute
  cleanup; retained resource labels identify orphaned fixture ownership.

Listener implementations and later migrations follow the
[TCP restart/ownership contract](../../doc/design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart):
`SO_REUSEADDR` before bind, stable four-digit endpoints and one live owner;
no default `SO_REUSEPORT` or conflict-driven port hopping.

## Assertions and artifacts

The migrated `e2e_three_node_cluster_kv_put_batch_delete` retains every original
assertion: two remotes per voter, Put success, exact Get bytes, Batch success,
Delete success and empty/not-found after deletion. It additionally checks the
surviving batch value, a per-fixture value under the same key, and a tombstone
across crash/restart. In the concurrent case both clusters first write different
values, then one crashes/restarts and is removed; the other must still read its
own exact bytes and report its three-voter topology.

Artifacts under `.crowdb-runtime/artifacts/container-e2e/run-*` retain image
config ID, labels/source revision, registry digests when available, client
results, topologies and Docker logs/resource state. Diagnostics redact credential
fields. CI consumes the once-built OCI artifact in independent KV jobs in both
regular CI, preview and release workflows. Successful KV digest validation is required by
publication; workflow dispatch/publication remain separate operator actions.

## First-batch ownership inventory

- Moved: the three-voter CRUD/batch/delete scenario from kv-server's
  `cluster_e2e_test`. Its former host-launch owner was removed.
- KV follow-up: remaining `cluster_e2e_test` cases cover follower hints, topology,
  dynamic groups, multiple-group isolation and replica reconfiguration.
  `snapshot_join_e2e_test` covers snapshot import before wiring and WAL tail.
  Both still use the component-local process helper until migration.
- KV follow-up: `server_api_test`, `async_ops_test`, `multi_store_test`,
  `restore_test`, `reconcile_test`, `system_init_test`, `group0_discovery_test`
  and `deployment_reconfig_test` launch processes for management APIs, startup,
  persisted restore and Group-0 operations. Their timing, endpoint and fault
  assertions must be retained per batch.
- Retain component logic tests: CLI parsing; placement/balance planning;
  monitor state/domain reconciliation tests using embedded stores or mocked
  dependencies. Mixed tests require per-case classification before moving.
- Shared harness follow-up: `cluster`, `diskdb`, `diskio`, `chunkdb`, `chunk_kv`
  launch service chains. Migrate the real-server portions to their layer profiles;
  retain deterministic hardware/simulation and internal logic checks locally.
- Other layers and existing deployment/browser acceptance keep their current
  owner. This inventory covers the initial KV batch, not full migration closure.
