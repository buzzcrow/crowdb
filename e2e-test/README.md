<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# E2E tests

The root-level `e2e-test/` is the unified E2E project. Its Rust workspace package
is `e2e-test`; server domains use separate test modules within this crate.
The Python runner owns container lifecycle and scheduling. Future browser and
SDK clients with independent language dependencies live under `clients/<client>/`
and consume the same fixture endpoints and ownership contract. They do not
introduce separate server launchers. Domain profiles and migration scope are
tracked in the [working plan](../doc/working/plan-container-e2e.md).

Test clients run on the host against inspected node-container IPs and fixed
ports, with three node containers for a three-node cluster. Fresh one-, two- and three-node profiles exercise management, membership,
async operations and multi-store routing. Host bridge IP reachability is
required; remote Docker and Linux-VM routing are not implicitly supported.
Containers supply server subsets or clusters from the formal release image;
they are not test execution environments. Domain profiles configure that same
server artifact rather than introducing separate test-server builds.

This component owns progressively migrated real-server acceptance. The first
profile is KV: three packaged `crowdb-kv-server` processes with block tree storage,
block-device WAL and the existing `e2e` election preset. It consumes the shared
`crowdb-node` OCI runtime image; it never starts a host server substitute.

## Run

```sh
pixi run test-container-e2e-fixture
pixi run clean-env
pixi run test-container-e2e --layer kv --concurrency 2 --cpus 4 --memory-mib 4096
# Run one domain scope (other migrated scopes: kv.membership, kv.routing,
# kv.recovery, kv.isolation):
pixi run test-container-e2e --scope kv.management --concurrency 2
# Optional parent outside the default artifact directory:
pixi run test-container-e2e --layer kv --data-root /path/to/e2e-data
```

Build/import the shared OCI image first. An absent image, missing Docker daemon,
unsupported daemon, changed release server sources or inadequate budget fails the
required test. There is no successful missing-prerequisite skip.
`--layer all` currently selects the only implemented layer, KV. Image and client
revisions are recorded independently. Client/documentation/test-registration
commits reuse an image after verifying the release server inputs still match;
production source, dependency, runtime configuration and build changes require
a rebuild. A missing/unverifiable image source label fails this verification.

`--concurrency` is a cap. Three-node isolation slots reserve 2 CPUs and 2048 MiB for three
0.5-CPU/512-MiB servers plus a host-client resource allowance. Named-flow slots
reserve the largest selected node count plus its host-client allowance. The effective cap also
accounts for Docker daemon capacity. Two slots run two isolated fixtures at once;
one slot runs the CRUD/recovery scenario without claiming concurrent isolation.
Recovery/isolation acceptance uses at most two slots; independent named flows
use the effective cap. CI sets `--require-isolation` and
fails if two slots are unavailable, so the required isolation check cannot skip. Host/hardware and containerd test
profiles remain pending.

## Lifecycle and endpoints

- Each fixture owns a UUID-labeled bridge network, independent host data bind
  mounts and its server containers. No global prune or shared
  implicit cluster is used. Cleanup verifies ownership labels and exact names.
- Network-local management is fixed at `7000`; store RPC uses `7001..7011`.
  Every fixture reuses the same names, store/replica IDs and port pool inside its
  own network. New fixtures do not call the legacy offset/probe/flock allocator.
- No service port is host-published. Management and RPC clients use inspected
  node IPs directly, so concurrent fixtures reuse all fixed internal ports.
- Host RPC clients consume the owned endpoint list. Peer topology rewrites
  wildcard listen addresses to inspected bridge IPs (the C++ RPC transport
  expects numeric addresses) while retaining actual
  returned store ports. RPC ports are never host-published.
- The KV profile directly tests a user-data group, as the original scenario did.
  It does not create Group 0, run discovery, SSH, monitor or console, and makes
  no claim about management bootstrap authority. Deployment profiles continue
  to own that coverage until their migration.
- SIGKILL/restart retains the same mounted data, network identities and internal
  endpoints. Inspection rejects a changed peer IP rather than masking stale
  topology by choosing another endpoint.
  Monitor automatic recovery is absent in this layer profile.
- Client compilation uses locked Cargo dependencies; the staged executable and
  Pixi shared-library dependencies execute on the host with per-run environment.
  Normal component Rust gates leave the container-only test ignored; this
  runner explicitly executes selected cases and missing fixture setup fails.
- Interruption or partial startup gathers diagnostics and tears down owned
  allocations. Operations have bounded deadlines; SIGINT/SIGTERM abort runner
  coordination and cleanup follows in-flight operations. SIGKILL cannot execute
  cleanup; retained resource labels identify orphaned fixture ownership.
- Node mounts and their logs/data/crash files remain on the host after teardown.
  `mounts.json` records paths. Startup, client phases, collection and teardown
  produce timing reports; a required client invocation must execute one case.
  `endpoints.json` records node/store/group/data layout. `results.json` collects
  per-case client, start/restart/collect/stop and complete wall durations, sorts
  the slowest flows, and preserves failure status after cleanup errors.

Listener implementations and later migrations follow the
[TCP restart/ownership contract](../doc/design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart):
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
- Migrated: remaining `cluster_e2e_test` cases cover follower hints, topology,
  dynamic groups, multiple-group isolation and five-node replica reconfiguration
  in three named flows. `snapshot_join_e2e_test` maps to the snapshot import
  before wiring and WAL-tail flow. Both old files and registrations are removed.
- Migrated: all `server_api_test`, `async_ops_test` and `multi_store_test`
  cases consolidate into eight named KV flow cases. The old files and explicit
  Cargo test registrations were removed after container acceptance. Multi-store
  coverage additionally checks different values for identical keys and restart.
- KV follow-up: `restore_test`, `reconcile_test`, `system_init_test`, `group0_discovery_test`
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

The cluster flows also cover exact follower hints, nontrivial topology identities,
dynamic group creation/removal with isolated same-key values, and a five-node
3→5→3 membership transition with acknowledged data and subsequent writes. The
former KV server `cluster_e2e_test.rs` is fully migrated. Selected-case resource
budgets reserve 0.5 CPU/512 MiB per node plus the host client; default 4 CPU/4096
MiB permits two three-node isolation fixtures or one five-node fixture.

Snapshot join uses two voting nodes and a third bare store from the same release
image. Direct local reads prove imported old values before remote wiring, then
verify WAL-tail catch-up on the non-voting member. Node profiles can omit initial
groups; the management API owns join/bootstrap.
