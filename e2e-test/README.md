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
ports, with three node containers for a three-node cluster. Fresh one- through five-node profiles exercise management, membership,
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
# Diskdb public allocation/ownership/exact reuse flows:
pixi run test-container-e2e --layer diskdb --concurrency 2
# Diskio file/direct durability, Mem/Null I/O, discovery and client faults:
pixi run test-container-e2e --layer diskio --concurrency 2 --cpus 6 --memory-mib 6144
# Chunkdb lifecycle/recovery, multislot failover and real Diskio conversion/repair:
pixi run test-container-e2e --layer chunkdb --concurrency 2 --cpus 12 --memory-mib 12288
# Host object/stream clients against the same protected storage profile:
pixi run test-container-e2e --layer chunk-client --concurrency 2 --cpus 12 --memory-mib 12288
pixi run test-container-e2e --layer chunk-stream --scope chunk-stream.recovery --cpus 6 --memory-mib 6144
# Normal release monitor full set, service subsets and persisted restart:
pixi run test-container-e2e --layer deployment --concurrency 2 --cpus 6 --memory-mib 6144
# Run one domain scope (other migrated scopes: kv.membership, kv.routing,
# kv.recovery, kv.isolation, kv.group0, kv.client, kv.metadata):
pixi run test-container-e2e --scope kv.management --concurrency 2
# Select one exact client case or list available profiles without Docker:
pixi run test-container-e2e --case kv::bootstrap::custom_replica
pixi run test-container-e2e --list --scope kv.group0
# Optional parent outside the default artifact directory:
pixi run test-container-e2e --layer kv --data-root /path/to/e2e-data
```

Build/import the shared OCI image first. An absent image, missing Docker daemon,
unsupported daemon, changed release server sources or inadequate budget fails the
required test. There is no successful missing-prerequisite skip.
`--layer all` selects the registered KV, Diskdb, Diskio, Chunkdb, chunk-client,
chunk-stream and deployment cases. Chunk-client migration is being verified;
the whole-storage-outage stream case currently exposes an unresolved fault budget.

Chunkdb lifecycle, placement and recovery use five owned containers: three KV
voters, one Diskdb and one production-mode Chunkdb. Actual inspected KV replicas
are registered through the public metadata API. Allocation transport acceptance
needs only KV and Diskdb. The placement profile seeds four distinct failure
domains and checks metadata geometry. The storage profile owns eleven containers:
three KV, Diskdb, Chunkdb and six Diskio failure domains, with eighteen disks.
The multislot profile owns three Chunkdb service instances, three KV voters and
Diskdb. Storage cases verify public conversion, partial bytes, exact parity,
expired foreground claim takeover, four missing-shard repair, degraded node-return
repair and exact relocation journal copy/publication/source-free ordering. All eleven
Chunkdb cases and their source retirement pass. Important internal task, journal,
fence and placement proofs remain in the Chunkdb component with pinned memory
metadata and their original assertions.
Other domain migrations remain tracked in the working plan. Image and client
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
- KV scopes separately test user-data groups and Group-0 initialization,
  discovery, membership CAS and metadata authority through public APIs. Bare
  profiles start no initial store; Group-0 ownership tests explicitly initialize
  through management. SSH, monitor and console coverage belongs to deployment
  profiles until their migration.
- SIGKILL/restart retains the same mounted data, network identities and internal
  endpoints. Inspection rejects a changed peer IP rather than masking stale
  topology by choosing another endpoint.
  Monitor automatic recovery is absent in this layer profile.
- Host fault requests use a bounded fixture-owned control directory. Only
  stop/start/kill/pause/unpause of declared nodes are accepted; ownership is
  rechecked before each action. Responses and individual action timings survive
  teardown. Client phase time already includes these actions.
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
- Group-0 initialization, persisted ownership and discovery now use named
  container cases. `system_init_test` is removed; `restore_test`, `reconcile_test`
  and `group0_discovery_test` retain their disk/identity/planning UT portions.
  All deployment failover flows pass together; `deployment_reconfig_test` and
  its final process launcher are removed.
- KV client public CRUD/CAS/scan/deadline, domain monitor ensure, authority
  and membership CAS flows have container owners. Deterministic watcher
  admission, forced pagination, internal barrier metrics, watermark and
  cancelled owner-handover tests remain component-local. Slot-map, handoff and
  epoch-publication flows pass; their old embedded-server sources are removed.
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

Latest complete KV acceptance: 48 independent fixtures, 47 distinct Rust cases,
54 client phases and three invalid release-startup checks pass in 85.987 seconds
with two slots and a 6 CPU/6144 MiB budget. See `doc/working/test.md` for measured
flow timings. Other service domains and final task/job classification remain
pending.

## Storage and normal monitor batches

- Diskdb client allocation, ownership-routing and exhaustion/reuse now have one
  owner under `tests/diskdb/`; the old client process fixtures are removed after
  acceptance. Six deterministic client UT remain in their component. Diskdb
  application keepalive/fence/journal/scanner proofs retain component ownership
  using pinned in-memory metadata, with no host KV launcher. Public recovery
  independently SIGKILLs Diskdb or Diskdb plus KV and checks exact durable
  records/revisions, persist-only free, compaction and capacity reuse.
- `deployment.selection` consumes mounted normal profiles in the same immutable
  release image: complete seven services, KV-only and KV+Diskdb. Check exact
  selected readiness, acknowledged host-client writes, abrupt node loss and
  recreation with preserved data. Unknown/missing-dependency selections fail
  before partial startup. This does not replace the pending complete deployment
  UI/SSH and runtime failure matrix.
- Latest Diskdb client batch: 5 flows/12 host phases, 12.726 s scenario wall at two
  slots. Normal monitor selection batch: 3 flows/6 host phases and five startup
  rejection checks, 20.153 s. Detailed measurements and image/source identities
  are in `doc/working/test.md`; other domains remain pending.

## Diskio profiles

- `diskio.durability` uses an owned 16 MiB regular data file with buffered file I/O;
  `diskio.direct` uses the release default O_DIRECT setting on the same geometry.
  Both fsync acknowledged aligned and unaligned bytes, SIGKILL/restart at the
  same IP/7200, and verify exact persisted contents plus an unaligned range read.
  Regular files do not claim real block-device or io_uring capability coverage.
- `diskio.io.mem` and `diskio.io.null` own KV plus Diskio, with the original four
  disks including a high-half disk ID. They retain every boundary size, overwrite,
  zone/offset and 100 concurrent cycles, with exact bytes or NullDisk patterns.
- `diskio.discovery` owns heartbeat, stable connection pools across refreshes,
  malformed-owner rejection and new-disk reconciliation through Group 0.
- `diskio.errors` owns missing disk/zone and missing write creation time;
  `diskio.faults` owns deterministic I/O errors using the normal release option.
- `diskio.semantic` owns payload/view framing, ordering, independent strip and
  priority admission, endpoint replacement and error classification. Its KV and
  three sequential Diskio generations use four owned containers. Replacement
  containers are created dormant and started by the bounded fixture control
  only after the prior generation exits. Startup waits for its RPC listener;
  tests then verify disk and registry readiness without hiding transport errors.
- Address validation, authoritative-owner topology and synthetic native-route
  lease recovery remain component tests. The shared host Diskio launcher remains
  until chunk client/stream callers migrate, and is not used by this domain.
