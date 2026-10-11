<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: review and migrate all real-server tests into domain modules under container E2E, simplify repeated setup and preserve important deterministic component flows.

## First batch

- [x] **Inventory**: classify kv-server and shared harness launch sites; preserve component logic tests. Files: `e2e-test/README.md`.
- [x] **KV fixture**: allocate labeled private bridge networks and per-node volumes, start packaged KV servers with the existing block/block-device/e2e profile, expose loopback management ports (container management `7000`, RPC `7001..7011`), wire real RPC topology, collect sanitized diagnostics and teardown owned resources. Files: `e2e-test/fixture.py`.
- [x] **Protocol migration**: move `e2e_three_node_cluster_kv_put_batch_delete` without losing assertions into a Rust test client executed as a sidecar on the fixture network. Record image ID/source labels and client source. Files: `e2e-test/Cargo.toml`, `tests/kv_test.rs`, `tests/common.rs`, application source test.
- [x] **Recovery and isolation**: crash/restart persisted servers, verify exact data/tombstones, run two clusters concurrently and terminate one before verifying the survivor. Enforce concurrency/CPU/memory budgets and interruption cleanup. Files: component runner and fixture tests.
- [x] **CI**: consume the existing once-built OCI artifact in a KV job; run the identical local entry point and upload diagnostics. Keep release publication dependent on its accepted digest. Files: Pixi tasks and OCI workflows.
- [x] **Verification**: fixture unit/failure tests, KV sidecar E2E, missing image rejection, Rust fmt/clippy, Python and workflow lint. Commit the coherent first batch; retain this plan/backlog for subsequent migration.

## Files

- `e2e-test/`: lifecycle, runner, Rust client tests, unit tests, documentation.
- `app/crowdb-kv-server/tests/cluster_e2e_test.rs`: remove only the migrated scenario.
- `Cargo.toml`, `Cargo.lock`, `pixi.toml`, CI component inventory/workflows.

## Tests

- Unit/integration: budget rejection, partial startup failure, owned cleanup, original-error preservation, diagnostics redaction.
- E2E: original three-voter CRUD/batch/delete assertions, retained WAL-backed values after SIGKILL/restart, two isolated networks with identical ports/store/replica IDs and different values; survivor remains usable after other fixture teardown.
- Gates: `pixi run test-container-e2e-fixture`, `pixi run test-container-e2e --layer kv --concurrency 2`, `pixi run rs-fmt-check`, `pixi run rs-lint`.

## First-batch verification

- Shared runtime rebuilt/imported; tested config ID `sha256:47411b55b98365a5de3e0b85684d68eeb9901a61ac94d9a93b7674842282219c`, OCI manifest `sha256:c4ef05c71f45771314b09263412bb538bb6009ab02d177fab6d5dbffc9e6a33c`.
- Required two-slot container acceptance passed: both original CRUD cases, one three-server SIGKILL/restart and persisted-data check, full fixture retirement, survivor exact-value/tombstone check.
- Fixture tests passed, including partial allocations, interruption, original-error preservation, resource budgets, redaction, endpoint refresh and fault targeting. The 12 OCI policy/artifact tests passed.
- Remaining five kv-server cluster cases passed; missing image fails before allocation.
- Workspace Rust fmt/clippy, changed Python/workflow lint and CI task reachability passed. Remote workflow execution remains unverified.
- User-requested `pixi run clean` completed after gates, releasing roughly 659 GB. A root-owned sandbox cache initially blocked Cargo cleanup; its generated cache ownership was repaired and the full clean rerun succeeded. Runtime/build/test artifacts were removed; the local Docker image remains available.

## Listener rule for all later migrations

- Follow [TCP listener ownership](../design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart): set/check `SO_REUSEADDR` before bind on every RPC/HTTP listener; fixed endpoints survive restarts and `TIME_WAIT`.
- Wait for the old listener/process to exit. A live owner causes explicit `EADDRINUSE`; no automatic port increment and no default `SO_REUSEPORT`.
- Audit each server/framework as its tests migrate; include same-port restart and concurrent-live-bind rejection coverage. The C++ RPC server already requests `SO_REUSEADDR`; check option-error handling and HTTP listener equivalence during the service migration.

## Migration rules

- User-deferred scope: keep `tools/benchmark/bench-chunk-kv-regression.sh` and
  its various test/benchmark selections unchanged during this migration. Decide
  their migration, consolidation, removal and final gate ownership only after
  the rest of this work is complete. Do not silently classify them as migrated
  or delete them through the launcher/duplicate-test closure audit.
- Preserve distinct UT flow/edge/fault assertions rather than every old case.
  Review cross-module overlap, merge/delete redundant cases after mapping their
  assertions to a verified owner, and keep finer internal state/race/crash-cut
  coverage local. After target E2E verification
  and assertion mapping pass, delete the corresponding old server-dependent
  cases and unused launcher; mixed tests first split and keep their UT portion.
- Inventory flows across all modules before deciding where a check belongs:
  ordinary lifecycle, invalid requests, authority conflicts, concurrency,
  persisted state and fault/recovery. Record covered/missing/not-applicable for
  each relevant flow and add cases for gaps. Similar operations are not enough
  to prove duplication when fault target, protocol, durability or state assertion
  differs. Judge progress by covered contracts, not increased test counts.
- Scope includes production server executable launches and externally observable
  flows built from real in-process services. It includes startup/exit checks,
  indirect launches through shared harnesses, shell/Python SDK fixtures, browser
  setup and monitor-managed services. Search `app/`, `lib/`, `container/`,
  `tools/`, SDK fixtures and workflow/task delegates; Rust imports alone are
  insufficient inventory evidence.
- Keep component-local deterministic important flows: Paxos/election/WAL
  ordering, ownership/epoch fencing, allocators, placement/EC mathematics,
  split/handoff transitions, commit validation, reconciliation and cancellation.
  Replace real-server dependencies in such cases with existing memory stores,
  fake transports or test-util hooks. Keep small protocol stubs and fake child
  processes when these are the subject of the component test.
- Each original case receives a disposition: migrate, split, retain, or merge
  into a named target with equivalent assertions. Record `source file::case`,
  actual launch chain, target module/case, positive/negative assertions, fault
  and persistence requirements, required local flow, and old/new verification.
  Do this before deleting a source case. Module scope below is reviewed at
  fixture/file level; per-case mappings are still pending, not completed work.
- Review assertions, not only names. Preserve exact bytes, not-found/tombstones,
  status/error codes, stale epoch rejection before persistence, topology and
  callback reachability, acknowledged-write durability and bounded failures.
  An upper-layer round trip does not replace a lower-layer fault test.
- Simplify repeated bootstrap, client creation and polling; parameterize
  equivalent inputs. A short lifecycle may cover create/use/restart/delete
  with labeled checkpoints. Separate quorum loss, crash cut points, failed
  startup, authority conflict and timeout cases. No giant all-services test
  whose first failure masks every later contract.
- Each independently mutating case owns a fixture. Sharing several checkpoints
  within one case is allowed; sharing mutable clusters between test functions
  is not. Batch performance measurements have separate opt-in profiles and do
  not impose machine-dependent throughput assertions on correctness gates.
- Readiness checks dependency usability and identity; waits have deadlines and
  report last state. Retry only documented transient client errors. Never hide
  stale/conflict responses, extend an assertion's timeout without diagnosis,
  or rerun a whole failed scenario to count it as passing.
- Fault commands name service/node, volume retention and monitor recovery
  policy. Verify data before the fault and after recovery. Pause/partition/kill
  are different cases. Test-only deterministic hooks stay component-local;
  release-image E2E does not require special test server builds or fault endpoints.
- Namespace-isolated fixed internal ports replace host allocation. E2E clients
  run on the host, including Rust, HTTP, SDK workers and Playwright with the
  installed system browser. Inspect each node's network IP and fixed ports;
  supply those addresses to clients and record locked build/dependency identity.
  No ordinary E2E client sidecar or host port mapping is required. Verify direct
  host reachability during startup; supported local/CI Linux daemon networking
  must make the bridge IPs reachable. Remote/VM daemon support is not implied.
- Every migrated batch removes its duplicate launcher only after verification,
  updates Pixi/CI delegates, and demonstrates component logic without Docker.
  Do not globally run `clean-env` between concurrently owned container suites;
  run the required clean before the coordinated invocation, then owned teardown.

## Target modules and fixture contract

- Containers run the formal release server artifact only. They are deployment
  units for a configured service subset or cluster, not a test runtime. Test
  executables, browsers and SDK dependencies remain on the host. Use the same
  immutable release image across all domain profiles and record its receipt;
  do not build a server image per test/module or add test-util instrumentation.
- Debug packaging exception: use release optimization/build/entrypoint and
  optionally skip stripping debug information. Apply this through the normal
  release staging/package option once, record binary build IDs, stripping mode
  and immutable image digest, then reuse that image for all domains. A symbol-
  retaining package is allowed; service selection never requires a rebuild.
- Profiles vary supported startup configuration, selected services, node count,
  geometry and recovery policy. They do not swap production implementations or
  rely on a test-only entry point. Exercise bootstrap through the owning public
  management/monitor interfaces; server subset profiles need not start unrelated
  services. Release artifact validation and publication are distinct actions:
  testing the release artifact does not require publishing it to a registry.
- The normal release startup interface must support the complete configured
  service set and explicit subsets. Reuse `DeploymentProfile` service/dependency
  validation and monitor supervision; extend the release entrypoint's currently
  fixed `/opt/crowdb/etc/profile.toml` path with a documented launch/profile
  selection input if needed. Resolve required local dependencies or explicit
  external endpoints before startup, reject unknown/unsatisfied selections,
  and ensure reconciliation does not restart deliberately omitted services.
- Node data bind mounts use independently owned host directories such as
  `.crowdb-runtime/artifacts/container-e2e/run-<id>/nodes/node-<id>/data`
  mounted at `/opt/crowdb/data`. Keep logs under `data/log` and supported dumps
  under `data/crash`; record node/container identity, mount paths and UID/GID
  ownership. Never share a writable data root across concurrent fixtures.
- Startup accepts a caller-selected host data directory for each node, including
  directories outside the default artifact tree. The common container target
  stays `/opt/crowdb/data`; host paths may differ across nodes and runs. Validate
  writable ownership and reject overlapping writable roots between active
  fixtures. Record the resolved host/container mount mapping in the run manifest.
- Persist every service's data, monitor/service log and collected crash report
  below that mount, so local tools can inspect them directly without `docker cp`
  or container exec. Audit each child process's paths rather than assuming the
  profile data root redirects all output. Keep console stdout/stderr captures in
  the same node directory. For host-collected cores, record the collector location
  and copy/export the matching report into the mounted crash directory when
  supported; do not silently imply kernel dumps already land there.
- Keep failed/interrupted run directories after runtime teardown for search and
  crash inspection. Normal successful cleanup may remove disposable data only
  under an explicit retention policy after diagnostics are collected. A later
  explicit artifact cleanup is distinct from network/container teardown.
- Audit core limits, process working directory and actual host `core_pattern`/
  collector using the existing crash debugging guide. Mounting `data/crash`
  does not redirect a host collector. Preserve matching binaries/symbols/build
  IDs and verify a symbolized controlled-crash report when supported; do not
  globally change host core policy for concurrent tests or treat logs as a dump.
- Three-node cluster = three node containers joined to that fixture's private
  network. Each node uses the same fixed service-port map; profiles activate the
  required services in those nodes. Five-replica cases use five node containers.
  Rust/browser/SDK clients run outside those containers; include their host
  resource usage in budgets. Multiple clusters reuse all internal ports safely.
- Establish a single service-port map before adding profiles. Preserve KV
  management `7000` and store pool `7001..7011`; enumerate Diskdb, Diskio,
  Chunkdb, Chunk-KV, Access protocols/health, console and monitor listeners,
  selecting fixed non-overlapping four-digit ports within each node namespace.
  Do not invent a separate base/offset for each test, rack, process or worker.
- Remove container-path free-port probing, increment-until-bind, test-global
  flock/semaphore port allocation and namespace offset arithmetic. Dynamic KV
  stores need explicit pool capacity, stable store-to-port assignments and a
  clear exhaustion error; runtime topology carries those actual endpoints.
  Direct IP endpoints are the default; remove unnecessary management host
  publishing from the existing KV fixture when host-client migration lands.
- Restart retains node container/network identity and data volumes; stop and
  wait for the old service before rebind. Same-port recovery and conflicting
  live-owner rejection remain independent assertions. Do not mask ownership
  conflicts with `SO_REUSEPORT` or selection of another port.
- Unified root: `e2e-test/`; Rust workspace package: `e2e-test`. Start with one
  crate and domain modules. Per-server crates would duplicate helpers and
  multiply feature/task wiring without providing additional fixture isolation.
- Keep `Cargo.toml`, Rust `tests/`, Python runner and fixture tests under this
  root. As clients migrate, place independent pinned language projects under
  `clients/<client>/` (for example Playwright, PyIceberg or Java FileIO). Each
  child owns client code and its dependency lock, receives fixture endpoints
  and is invoked by the unified runner. It never launches its own servers.
- No nested Rust workspace or one crate per domain initially. A future Rust
  child crate requires evidence of incompatible features/dependencies or a
  distinct toolchain; preserve one lifecycle, ownership manifest and selection
  entry point when splitting. Browser presentation unit tests remain in UI.
- Rust entry points remain `tests/<domain>_test.rs` with declarations in those
  files and cases under `tests/<domain>/`. Use `kv.rs` + `kv/` style for helpers,
  no `mod.rs`; tests and helpers stay outside `src/`. Root `common.rs` and
  `common/` contain only reusable endpoint/client/fixture access.
- Target domains: `kv`, `diskdb`, `diskio`, `chunkdb`, `chunk_kv`,
  `chunk_stream`, `access_s3`, `access_iceberg`, `access_dataset`, `console`,
  `deployment`. A domain module owns behavior; its selected profile specifies
  infrastructure. Console clients checking KV behavior can belong to KV;
  console authority/UI behavior belongs to console.
- Python fixture separates runtime operations, profile descriptions, readiness,
  diagnostics and scheduling as it grows. Keep existing KV behavior as a
  regression baseline; do not copy `Cluster` once per service.
- Fixture descriptions declare services, node/rack/disk geometry, Group-0 and
  user-data bootstrap, startup configuration, CPU/memory/storage budgets, client
  location and supported faults. Dependency services are explicit; simple KV
  management cases do not pay for the entire storage/access stack.
- Host test clients receive a serialized endpoint/topology manifest.
  Lifecycle control remains owned by the host runner: cases request targeted
  actions through a fixture-owned bounded control mechanism or explicit runner
  phases. Container faults use owned container/service identity rather than
  host PID assumptions or production convenience fault APIs. Choose the mechanism in the fixture task
  after examining dynamic fault call sites.
- Extend `client.py` beyond hardcoded `kv_test`/`kv-test`: select test target,
  test-name filter, feature/dependency environment and record result count.
  Required execution fails on zero matching cases or missing fixture manifest.
- `--layer all` must eventually select every implemented required domain;
  requested unimplemented profiles fail explicitly. Host/hardware profiles are
  separately selected and serial; do not silently replace them with simulation.

## Module analysis and scope

### KV and Group 0

- Sources: `app/crowdb-kv-server/tests/` process helper and
  `cluster_e2e_test`, `snapshot_join_e2e_test`, `server_api_test`,
  `async_ops_test`, `multi_store_test`, `restore_test`, `reconcile_test`,
  `system_init_test`, `group0_discovery_test`, `deployment_reconfig_test`,
  `startup_test`; real-client portions of console-shared `kv_e2e_test` and
  `mgmt_e2e_test`. Check every consumer of `tests/common/process.rs`.
- Move management CRUD/errors, async completion, independent stores/groups,
  topology/follower hints, membership expansion/shrink, snapshot-before-wiring
  and WAL-tail catch-up, startup/exit, persisted Group-0 restore and endpoint
  retention. Profiles need empty roots, configurable node counts (including
  five-node membership), stores/groups and retained restart volumes.
- Simplify `server_api_test` repeated single-server startup: management lifecycle
  plus parameterized missing/conflict cases. Reuse existing RPC request helpers;
  preserve progressive setup, membership epoch replay/conflicting payload,
  rejected local replica removal, wipe preserving Group 0 and async ordering.
- Keep CLI/config parsing, placement/balance planning and internal Paxos/group/
  store/WAL tests. `lib/crowdb-kv-client/tests/e2e_retry_test.rs` pins real
  in-process roles: retain deterministic hint-following flow locally and add
  packaged follower-routing/leader-change coverage; do not label pinned roles
  a production election test. Review follower-read and handoff helpers similarly.

### Diskdb

- Sources: `app/crowdb-diskdb/tests/common/cluster.rs` and its consumers;
  `lib/crowdb-diskdb-client/tests/allocation_lifecycle_test.rs`,
  `ownership_routing_test.rs`, `space_reuse_test.rs`; shared harness `diskdb.rs`.
  Audit `diskdb_e2e`, recovery/recovery_failure, startup/keepalive sync,
  ownership_fence, relocation_journal, lifecycle and space_flow per case.
- Existing application fixture independently launches three KV processes and
  serializes startup using a semaphore. Replace its server-dependent portions
  with a KV + packaged Diskdb profile, explicit hardware/group bindings and
  simulated geometry; remove semaphore/host ports after the last caller moves.
- E2E scope: allocate/query/free/reuse, capacity accounting, ownership routing,
  restart reconstruction, persisted journal recovery, stale authority rejection
  and live service discovery. Combine compatible allocation/reuse checkpoints;
  keep interruption/journal cut points independent.
- Keep zone/disk allocators, free-batch, compaction/state-machine, query handler,
  metrics and recovery transition logic local with memory metadata. A test that
  directly calls Diskdb internals while launching KV is split, not copied whole.

### Diskio

- Sources: shared harness `diskio.rs`; client `disk_io_full_test`,
  `disk_io_errors_test`, `disk_io_durability_test`, `disk_io_group0_sync_test`,
  `semantic_io_test`, `native_routes_test`; native C++/FFI test/task launch sites.
- Profiles distinguish isolated simulated disks, null backend and explicit
  real-device/hardware execution. Null pattern checks are protocol checks and
  never serve as durable-storage evidence. Enumerate mmap/io_uring/native modes
  from existing cases before choosing required versus special profiles.
- E2E scope: registered disk discovery, exact offset/range bytes, write/read/
  sync/restart durability, invalid disk/range errors, ownership fencing, Group-0
  updates and bounded live I/O errors. Parameterize equivalent sizes/backends;
  retain distinct persistence and fault contracts.
- Keep address/topology parsing and internal I/O/FFI buffer semantics local;
  migration does not move all C++ unit tests into containers. Hardware resources
  need explicit ownership and prerequisites, never arbitrary host device access.

### Chunkdb and chunk client/stream

- Sources: application `full_stack_test`, `e2e_test`,
  `partial_ec_background_test`, `allocator_pool_test`, `journal_handoff_test`
  and `tests/common/cluster.rs`; shared `chunkdb.rs`; chunk-client
  `tests/common/e2e_stack.rs` and all its callers (reader, small/large writer,
  write_stream and benchmark); chunk-stream `production_restart_e2e.rs`.
- `full_stack_test` mixes real KV, in-process Diskdb/Chunkdb and direct task/
  lifecycle calls. Split its real externally visible scenarios into packaged
  KV + Diskdb + Diskio + Chunkdb profiles, while keeping TaskStore/TaskExecutor,
  admission/fence/claim and deterministic handler flows in component tests.
- Chunkdb E2E scope: allocation/reservation/append/seal/query/delete, capacity
  and geometry, mirror/EC conversion, degraded-node repair, durable task/journal
  recovery and ownership handoff. Racks/disks/loss budget are explicit; do not
  use unsafe EC to obtain a passing protected-placement case.
- Chunk stream/client scope: exact small/large/range data and durable publication,
  takeover stopping the old writer, acknowledged bytes after service restarts,
  bounded failure when Diskio dies and when a live Diskio returns errors.
  Share the storage profile and lifecycle helpers, not assertion ownership.
- Keep placement/selector/routing/codec/EC validity and injected I/O transitions
  local. `production_chunk_test.rs` uses fake storage despite its name and needs
  case review rather than automatic migration. Performance tests stay opt-in.

### Chunk-KV

- Sources: shared `chunk_kv.rs`; `app/crowdb-chunk-kv-server/tests/`, especially
  startup_exit and transfer_storage; native split/handoff fixtures in server
  common and `lib/crowdb-chunk-kv/tests/`; client production_transport.
- Profile adds packaged Chunk-KV to real storage, with Group-0 catalog authority.
  Scope includes ordered reads/writes/scans, partition discovery, assignment
  epoch fencing, split publication/transfer/recovery and server exit behavior.
- Distinguish `native_split_handoff_test` native tree/storage execution from
  actual server startup; preserve native storage integration locally when it
  does not launch services. Keep split state machines, proof/cut-point tests,
  lease sampling/balance planning and fake transport races in their components.
- Add a packaged split/restart flow that checks every original record and writes
  accepted during handoff. It complements deterministic proof tests. Review
  test-only storage hooks and retain their proof coverage component-local;
  container acceptance uses the release image without those hooks.

### Access S3

- Sources: access-server `s3_full_stack_test`, `s3_copy_body_test`, protocol
  HTTP/production policy; console-shared S3 mini-cluster/bench; CLI S3 tests;
  `tools/pixi-tasks/test-s3-*.sh` and their Python/Java/JS/Go clients.
- Use packaged Access plus the full required storage chain. Separate HTTP
  protocol behavior, production durable storage, SDK compatibility and fault
  profiles; a memory backend HTTP check is not durable full-stack coverage.
- Scope: bucket/object/list/range/copy/delete, multipart lifecycle and exact
  content, auth/policy/protocol errors, service restart and recovery, lost
  responses/idempotency and concurrent clients. Reuse SDK-independent setup;
  keep actual SDK request/response compatibility cases in their languages.
- Keep multipart/S3 repository state machines, parsers/signatures and injected
  body/copy failures local. Simplify duplicate happy paths only after checking
  differences in SDK signing, streaming and error decoding. Preserve every
  restart target, including combined restart coverage where currently present.

### Access Iceberg and Dataset

- Iceberg sources: access-server `iceberg_*_test.rs`, common `iceberg_stack`,
  `iceberg_process`, commit child/file worker; `test-iceberg-*.sh`, Java FileIO,
  PyIceberg, Rust SDK, RCK and container ecosystem fixtures.
- Scope: REST catalog namespace/table/commit, FileIO exact data, auth/retired
  routes, listing, GC/capacity, response loss, file/table crash matrices and SDK
  interoperability. JVM/SDK workers run on the host using container IPs; packaged
  Access/storage remains fixture owned. Preserve pinned environments and worker
  process semantics; do not rewrite compatibility clients into Rust.
- Share catalog/storage bootstrap, parameterize equivalent formats, retain
  distinct commit/ack crash windows and native versus HTTP FileIO contracts.
  Keep schema/projection/codec, commit evaluator, manifest validation, journal/
  multipart/GC proofs and deterministic publication races local.
- Initial scan of `lib/crowdb-access-dataset/tests/{core,adapter,transport,retention}_test.rs`
  found no shared harness, process or listener launch references. Retain these
  local tests; this scan does not prove the whole Dataset call graph is covered.
  Dataset sources require an explicit launch-site/case audit of access-dataset,
  access-server routing and SDK tasks. Do not infer coverage from Iceberg/S3.
  Add a separate module for existing server-dependent Dataset flows; retain
  codec, schema, query/layout and memory repository tests locally. No new
  Dataset functionality is introduced by this migration.

### Console, CLI and deployment/monitor

- Sources: console-shared tests using real KV or S3 clusters; CLI `direct_cli`,
  `cluster_cli`, S3 and storage bench fixtures; web Rust tests and
  `native_cluster_provisioning_test`/`native_access_deployment_test`; UI
  `e2e/globalSetup.ts`, realBackend/native configs and fixtures; monitor
  `kv_process_bootstrap`, `storage_bootstrap`, `hardware_bootstrap` and other
  real-service consumers; single-node Docker/containerd scripts and ecosystem.
- Console scope: actual HTTP client/CLI operations, Group-0 publication and
  stale/concurrent authority, deployment cancellation/resume, service lifecycle,
  readiness and browser provisioning through exact persisted S3/Iceberg data.
  Browser clients stay Playwright with the system browser; replace backend
  launching and receive fixture endpoints. UI-mocked presentation tests stay
  local. Apply console-ui-e2e when changing browser fixtures/tests.
- Deployment scope: packaged monitor bootstrap, persisted node identity,
  manual versus automatic/read-only single mode, supervised restart, admission,
  logs/crash retention, resource limits, Docker/containerd adapters and explicit
  host-network profile. Monitor must remain active for supervisor tests; disable
  automatic recovery only in a profile testing client recovery without it.
- Keep monitor manifest/profile/render/credentials/probe tests, fake-child
  Supervisor transitions, console reconciliation/fanout/cancellation logic and
  stub HTTP route tests local. Some `Command::new` sites launch a CLI or fake
  child, not a server; classification follows the child and its setup chain.
- Browser full-chain and SDK round trips may share fixture helpers but remain
  independently selected flows. Host-network/SSH/real devices have explicit
  special profiles; bridge coverage cannot close their contracts.

## Dependency-ordered execution

- [x] **Root project rename**: move the existing component to `e2e-test/`, rename
  the Rust package, update workspace/lockfile, relative dependency/root paths,
  Pixi and CI inventory plus documentation links. Verify fixture tests, locked
  Cargo metadata, test-target discovery and formatting before further migration.
  Verified: all nine fixture tests, locked metadata/local dependency paths and
  `kv_test` discovery, runner help, workspace Rust fmt and CI task reachability.
  No full server E2E rerun or Rust compilation is claimed for this path rename.
- [x] **Rules and module scope**: inspect existing first batch, shared launchers,
  mixed fixtures and task entry points; extend the requirement boundary and this
  plan. Files: R230 detail and this plan. This is planning evidence, not proof
  that all test cases or launch sites have been inventoried.
- [ ] **Complete case inventory**: enumerate all test cases consuming process,
  in-process production service, browser and SDK launchers, including Dataset,
  C++ and hidden task delegates. Add per-case dispositions and assertion maps
  under the relevant module above. Confirm important local flow coverage before
  moving tests. Files: source tests, launch helpers, Pixi/tasks, workflows, plan.
- [ ] **Cross-module coverage and deduplication**: map important flows and their
  assertions to one appropriate owner, identify untested flows and consolidate
  equivalent cases across components. Record removed/merged cases and retained
  boundary/internal coverage; verify owners before removing redundancy. Files:
  component tests, E2E domain tests, this plan and `doc/working/test.md`.
- [ ] **Generalize fixture/client dispatch**: introduce profile/service
  descriptions, endpoint manifest, named-target test selection and bounded
  targeted lifecycle control; split growing Python modules. Preserve KV pair
  acceptance and add zero-test/missing-profile/failed-action tests. Files:
  `e2e-test/{fixture,client,run}.py`, `tests/common*`, fixture tests.
- [x] **Host clients and direct IPs**: replace existing KV client sidecars with
  host test execution, inspect node IPs and expose fixed listeners, verify
  management/store RPC reachability and normalize advertised wildcard addresses
  to inspected IPs. Preserve exact topology, restart and isolation assertions;
  keep run-specific client environment/artifacts and remove unnecessary Docker
  publish options. Files: `e2e-test/{fixture,client,run}.py`, common clients.
- [x] **Release startup service selection**: document and implement runtime
  full-set/subset selection through the normal entrypoint/profile contract,
  preserving existing bootstrap modes. Reuse monitor dependency validation;
  test unknown/missing dependencies and selected-service restart/reconciliation
  against one unchanged image. Files: release entrypoint/profile, monitor
  profile validation/runtime, container README, E2E profiles/deployment cases.
  Component implementation verified: the selected normal profile controls both
  initial bootstrap and restart authority checks. KV-only skips disk creation;
  storage prefixes skip Access/catalog bootstrap. CLI validation and direct
  single-node startup reject unknown services, missing selected/declared
  dependencies, mismatched management seeds and invalid durable KV roots before
  identity/discovery startup. Default full-set bootstrap order is unchanged.
  Focused selection/preview/profile tests: 10 passed; profile/render/supervisor/
  manifest/crash-retention tests: 23 passed; monitor all-target clippy and Rust
  fmt passed. Same-image full seven-service, KV-only and KV+Diskdb startup,
  SIGKILL/container recreation and exact acknowledged-value recovery passed in
  run-baa850f79d5e442e8d9153309e9bf1f2 (20.153 s scenario wall). Each ready
  monitor exposes exactly the selected services. Five invalid release startup
  checks passed; unknown selections and missing dependencies created no node
  identity, SSH, bootstrap or KV state. Data mounts are initially empty and
  explicitly owned by the release service UID; fixture log precreation must not
  violate the normal fresh-root bootstrap contract.
- [ ] **Debug information and inspectable data**: add a release staging option
  retaining debug info, mount each node's owned data root at a discoverable host
  path, retain logs/crash evidence on failure and record matching binary IDs.
  Verify supported core collection/symbolization and explicit unsupported-host
  diagnostics. Files: release staging/OCI receipt, fixture mounts/diagnostics,
  crash guide and deployment acceptance.
- [ ] **Caller-selected node data mounts**: expose normal launch and fixture
  inputs for per-node host paths, validate mount ownership/isolation, audit all
  data/log/crash paths and record resolved mappings. Start two nodes/runs with
  different paths and verify locally visible exact data/log/crash evidence,
  retained restart state and teardown preservation. Files: release launcher,
  profile paths, fixture manifest/mounts, deployment tests and README.
- [ ] **Fixed node ports and allocator retirement**: inventory listener defaults,
  dynamic pool needs and all test/deployment port allocator callers; define the
  common fixed per-node layout, start three-node profiles as three containers,
  and remove allocator use in each migrated path. Delete unused test allocation
  code after the final caller; preserve and review remaining host deployment
  requirements explicitly. Files: profile/fixture, service configs, protocol
  port module, monitor templates and former test harnesses.
- [x] **KV management/async/multi-store batch**: consolidate all 38 server API,
  six async operation and one multi-store source cases into eight independently
  owned container flows; verify epoch fences, maintenance, operation completion
  and persistent store isolation. Files: kv-server tests and `tests/kv/`.
- [x] **KV startup/exit batch**: migrate executable startup/exit checks and
  empty-root initialization; preserve component CLI/configuration tests.
- [x] **KV recovery/membership batch**: add profile node-count/config support;
  migrate restore, snapshot/WAL tail, dynamic group/membership and Group-0
  discovery; retain deterministic KV-client flows and test actual production
  leader retry. Retire component process helper only after all callers migrate.
- [~] **Diskdb batch**: add dependency-ready Diskdb profile, map client and
  application assertions, split internal handler flows from real metadata
  setup, migrate allocation/recovery/fence/journal boundaries. Files: Diskdb
  tests, harness `diskdb.rs`, container `tests/diskdb*`, profile/runner.
- [ ] **Diskio batch**: add simulated/null profiles and explicit special-device
  scope; migrate RPC errors, registry sync and durability. Audit listener restart
  and live-bind rejection. Files: client/C++ launch sites, `tests/diskio*`.
- [ ] **Chunkdb batch**: add configurable rack/disk geometry and fault policies;
  split mixed full-stack cases, migrate lifecycle, degraded repair/conversion
  and journal handoff with deterministic component flows retained. Files:
  Chunkdb tests/harness, `tests/chunkdb*`, storage profiles.
- [ ] **Chunk client/stream batch**: migrate exact object/stream publication and
  all four production restart/takeover/error flows; separate opt-in benchmark
  profiles. Files: chunk client/stream tests, `tests/chunk_stream*`.
- [ ] **Chunk-KV batch**: add service/catalog readiness; migrate server startup,
  transport and storage-backed split/transfer boundaries; preserve local split
  proof tests and add release-image durable split coverage. Files: Chunk-KV server/
  library/client tests, `tests/chunk_kv*`, profile/runner.
- [ ] **S3 batch**: migrate HTTP/production and SDK chains; parameterize shared
  bootstrap, retain policy/errors/multipart/restart matrix. Files: access-server,
  console/CLI S3 fixtures, SDK scripts, `tests/access_s3*` and client adapters.
- [ ] **Iceberg/Dataset batches**: migrate catalog/FileIO/SDK/GC/crash boundaries,
  preserving worker environments; migrate audited Dataset server flows. Files:
  access tests/helpers, SDK tasks, `tests/access_iceberg*`, `tests/access_dataset*`.
- [ ] **Console/CLI batch**: migrate real-service operations and web route flows;
  replace browser backend launcher with owned profiles, preserve UI assertions
  and important local reconciliation/cancellation flows. Files: console-shared,
  CLI/web tests and UI E2E fixtures, `tests/console*`.
- [ ] **Deployment batch**: move monitor real-service and current container
  acceptance under deployment modules; implement Docker/containerd and explicit
  host profiles without changing their ownership/identity contracts. Files:
  monitor tests, single-node container tests, `tests/deployment*`, adapters.
- [ ] **CI/delegates per batch**: extend immutable-image layer matrix and budgets,
  required result-count checks, artifacts and old Pixi entry-point delegates
  whenever a layer lands. Remove production server build dependencies from
  component test jobs only as their last server consumer retires. Files:
  `pixi.toml`, `tools/pixi-tasks/test-*.sh`, `.github/workflows/`, CI inventory.
- [ ] **Mixed-domain concurrency acceptance**: run different migrated domain
  fixtures concurrently on one host with independent host test clients. Fault
  and retire one while checking exact bytes/topology on another; verify no
  global cleanup, shared workspace artifacts or port allocator can affect it.
  Extend beyond the first KV/KV pair. Files: scheduler, isolation scenarios, CI.
- [ ] **Final Pixi test classification**: separate component logic gates from
  required container E2E domain gates; use one canonical selection path, update
  suite aggregators and old aliases without duplicate execution. Files:
  `pixi.toml`, `tools/pixi-tasks/`, `tools/ci-checks/check-ci-test-tasks.py`.
- [ ] **Final CI jobs**: replace legacy host-server E2E jobs with shared-image
  domain matrix jobs, resource limits and mixed-domain isolation acceptance;
  retain component and special-profile gates. Update preview/release dependency
  and accepted-digest wiring. Files: `.github/workflows/`, CI task graph checks.
- [ ] **Final test.md tables**: reconcile `doc/working/test.md` task rows with the
  final categories, record scope/platform support and measured counts/timings,
  distinguish unavailable/unexecuted profiles from passes, and document
  concurrency/budget/artifact identity. Preserve this persistent file and do not
  relabel historical measurements as results for the migrated suites.
  Initial target scope/CI-group and per-phase timing tables are now recorded in
  that file (46 scopes; pending rows are not implemented task claims). Fixture
  check passed 9/9 on 2026-10-11, full invocation measured at 0.436 seconds.
  Extend to per-binary, per-SDK, browser spec and fault cases during execution;
  report slowest cases/setup phases while CI combines multiple measured scopes.
- [ ] **Closure audit**: rescan all launchers and task delegates, verify every
  original assertion mapping, run component gates without Docker/server binaries
  and complete required layers on the same immutable image. Retire unused
  harness process/port-allocation code; retain non-test/deployment callers.
  Delete requirement/index entry/plan only when all required scope passes.
- [ ] **Deferred benchmark decision**: after the migration and coverage/timing
  results are available, review `tools/benchmark/bench-chunk-kv-regression.sh`
  selections with the user before changing their ownership or content. Until
  that decision, explicitly retain the script and its existing behavior.

## Batch verification and coverage record

- Current execution discovery: a Rust source scan found 609 test files and
  3111 annotated cases, including 172 files with direct launch/shared-harness
  evidence. Raw inventory: `.crowdb-runtime/artifacts/container-e2e/inventory/rust-cases.json`.
  This is discovery, not completed case review; indirect launch chains, C++,
  browser and SDK fixtures remain to audit. Benchmark deferral is unchanged.
- Current foundation changes (KV acceptance passed): host KV client execution,
  direct IP endpoints without published ports, configurable host data parent,
  persistent node data/log mounts, release `server <service>` and mounted
  `CROWDB_PROFILE_PATH` interfaces, debug-info-retaining release staging.
  Fixture/client checks pass 12/12. The retained-debug release image passed
  host-client concurrent three-node CRUD, SIGKILL/restart, survivor isolation
  and singleton management acceptance on 2026-10-11. Mounted data/log files
  remain available after owned containers/network teardown. Broader service
  selection and crash symbolization acceptance remains pending.
- KV management consolidation verified:
  `app/crowdb-kv-server/tests/server_api_test.rs` cases `health_check`,
  `openapi_json_is_served`, `list_stores_with_initial_cli_store`,
  `get_store_not_found`, `add_store_via_api`, `add_store_conflict`,
  `remove_store_via_api`, `remove_store_not_found`, `list_groups`,
  `add_group_via_api`, `add_group_conflict`, `add_group_store_not_found`,
  `remove_group_via_api`, `remove_group_not_found`, `topology_export`,
  `topology_alias_top`, `metrics_endpoint_returns_structured_snapshot`,
  `metrics_prefix_filter_excludes_non_matching` map to independently owned
  `kv::management::management_lifecycle`, with discovery/metrics/store/group
  checkpoints preserving status/body/count/error assertions. All 18 source cases were removed after real release-container verification.
  This was the first verified subset; the subsequent batch below completes the file.
- Acceptance artifacts: `.crowdb-runtime/artifacts/container-e2e/run-c2d57c0480f946c2828ebab5b0f35d8f/`;
  exact image config `sha256:96e297780624131a1db323f7fdac8aaef0e819076f1c50a86fae78ab94bc0613`.
  Warm client build 0.619s; complete scenario wall 9.069s. Three-node startup
  1.456/1.550s, write 0.623/0.626s, post-restart/survivor verify 0.224/0.236s;
  singleton startup 0.733s, management case 0.350s, collect 0.075s, stop 0.574s.
- Release startup rejection checks passed against that same immutable image:
  unknown service, standalone-on-managed-data policy, and malformed mounted
  profile. The profile assertion was corrected from lowercase `error` to the
  actual `Error: Decode` diagnostic; rejection behavior was already correct.
  Artifacts: `.crowdb-runtime/artifacts/container-e2e/release-bd3c24b1e1f1498d942bbdf9e8247365/`.
- After removing the verified 18 duplicate cases, the remaining server API
  suite passed 20/20 (1.94s test execution). Workspace fmt/clippy, OCI image
  unit checks, fixture/client checks and the CI task/package inventory passed.
- For each batch, record original cases and merged/split destinations before
  editing; record commands, image/source identity, case count and outcomes after
  verification. First-batch history above remains historical evidence only.
- Unit: important component algorithms/state transitions and new runner/profile
  scheduling/manifest/selection validation; no production server required.
- Integration: partial allocation failure, diagnostics redaction, teardown and
  original-error preservation, dispatch zero matches, stale/missing images,
  unavailable profiles and control requests. Keep fixture tests fast.
- E2E: migrated domain boundary assertions, retained fault/persistence cases,
  fixed-port restart and live bind conflict; two concurrent distinct domain
  fixtures when dependencies land, with one teardown leaving the other usable.
- Required commands per applicable batch: `pixi run test-container-e2e-fixture`,
  `pixi run clean-env && pixi run test-container-e2e --layer <implemented-layer>`,
  affected component task/tests through Pixi, `pixi run rs-fmt-check`,
  `pixi run rs-lint` and changed Python/workflow checks. C++ changes additionally
  run their format/tree-lint/tests; browser changes use console-ui-e2e gates.
- Full closure: `pixi run clean-env && pixi run test-container-e2e --layer all
  --concurrency 2`; special host/containerd/hardware commands are added only
  when implemented and required prerequisites are explicit. Global cleanup
  cannot execute while coordinated container jobs are running.
- Commit verified coherent requirement batches. The separately user-requested
  deletion of `plan-ci-regression.md` is pre-existing work and must not enter a
  migration commit by accident.

## Final Pixi / CI / test.md contract

- Proposed canonical runner tasks: `test-e2e --layer <domain|all>` and
  `test-e2e-fixture`. Existing `test-container-e2e*` and legacy SDK/container
  entry points remain delegates while callers migrate. The final task graph
  lists each scenario once; an alias must not introduce a second suite owner.
- Keep component gates grouped as core, storage, access, console and C++;
  distinguish UI presentation/unit tests from real-backend browser acceptance.
  They exercise local important flows without requiring production binaries or
  Docker. Container E2E reports domain, profile and client type, independently
  of the package from which the case was migrated.
- CI retains lint and component matrices. `ContainerImage` builds the immutable
  server image once; `ContainerE2E` selects domain/profile matrix entries that
  consume that same receipt. Host client builds/SDK environments are separately
  identified and locked. Verify the runner can reach inspected bridge IPs.
- `ContainerIsolation` exercises mixed domains on the same runner, because
  separate GitHub jobs cannot prove same-host isolation. Bound job matrix
  parallelism and in-run fixture concurrency separately; reserve full service
  and client CPU/memory/storage budgets, not merely container counts.
- Host-network, real-device and hardware scenarios remain explicit dedicated
  execution profiles with containers as the server execution boundary and host clients.
  Shared fixed host ports require serial execution or separate machines; these
  special cases do not weaken parallel bridge acceptance. containerd deployment
  behavior remains a required adapter/profile where previously required.
- Final `doc/working/test.md` tables cover canonical component tasks, fixture
  checks, each required E2E domain and special profiles. Include task/scope and
  runtime requirements, plus per-platform latest measured date/case count/time/
  status. Align raw Markdown columns; use `—` for no measurement and explicit
  pending/unsupported reasons. Record aggregate wall time separately from summed
  domain durations when suites overlap. Containerized Linux execution on macOS
  is not native macOS server evidence; macOS VM execution remains deferred.
- Closure checks the matrix/task/document mapping, zero selected-case failures,
  missing prerequisites, exact image identity and preserved fault assertions.
  New task names and matrix rows above are planned until implemented; do not
  present them as already available or verified.

## Management, membership and multi-store acceptance

- The remaining 20 `server_api_test` cases map to `kv::membership` replica
  lifecycle and epoch replay/authority bootstrap, `kv::progressive` topology
  wiring, and `kv::maintenance` synchronous step-down plus flush/wipe. Their
  assertions retain exact HTTP statuses, local-removal rejection, missing
  resources, replica counts, stale/conflicting epoch rejection without mutation,
  idempotent replay, progressive per-store topology and post-wipe exact writes.
- All six `async_ops_test` cases map to management discovery (readiness schema
  and missing operation) and `kv::operations::async_and_sync_leader_lifecycle`
  (three-voter readiness, asynchronous acceptance/completion and synchronous
  acceptance followed by restored cluster readiness).
- `multi_store_test::multi_store_per_node_isolation` maps to
  `kv::routing::multi_store_isolation_and_recovery`: original separate keys and
  exact bytes, strengthened with same-key isolation, cross-store misses and
  SIGKILL/restart durability across three containers with two stores each.
- These three source files and their explicit Cargo test registrations were
  deleted after acceptance. Component UT files remain. Cluster topology,
  follower hints, dynamic groups, five-voter membership, snapshot join and
  Group-0 bootstrap/restore remain separate unfinished work.
- Acceptance: `pixi run clean-env && pixi run test-container-e2e --scope all
  --concurrency 2 --require-isolation` passed. Artifact directory:
  `.crowdb-runtime/artifacts/container-e2e/run-f8abf634d02e4d63a055d7941ed642f1/`.
  Ten fixtures, nine unique Rust test functions, thirteen exact client phases
  and three release startup rejection checks passed; client build 1.872s and
  scenario wall 21.786s. Per-flow timings are recorded in `test.md`.
- Formal release image config:
  `sha256:e5f84a21b8bcbd4e1ed176aa2d31dc9ba87884610682e067c2f00cff6f8c6ab2`.
  The runner verifies packaged server inputs against the image source commit;
  client/docs/test-registration-only changes can reuse it, while dirty or
  untracked production inputs reject reuse. Client revision is recorded
  independently. Reports mark failed cleanup as failed even when clients pass,
  and rank complete fixture durations and client phases for slow-flow review.

## Cluster flow acceptance

- `cluster_e2e_test::{e2e_follower_returns_not_leader_hint,
  e2e_topology_reflects_cluster_state}` map to
  `kv::cluster::topology_and_follower_hint`: nontrivial store IDs 10/20/30, exact
  local replica/group identities, fixed RPC endpoint and direct follower write
  rejection with exact current leader endpoint.
- `cluster_e2e_test::{e2e_dynamic_group_management,
  e2e_multi_group_isolated_kv}` map to
  `kv::cluster::dynamic_groups_isolate_data`: create/elect/use/delete group 2,
  exact topology counts, same-key distinct values, group-1 tombstone leaving
  group-2 value intact and successful group-1 write after group-2 removal.
- `cluster_e2e_test::e2e_kv_after_dynamic_replica_change` maps to
  `kv::reconfiguration::quorum_expands_and_shrinks_without_losing_writes`: five
  containers, 3→5→3 membership with changed survivor set, stable automatic
  election, acknowledged values across expansion/shrink, delete and new write.
  The already migrated CRUD case remains in its recovery/isolation owner.
  All cases in the original file now have verified owners; delete the file and
  explicit Cargo registration. Component internal Paxos/election tests remain.
- Scoped routing and membership acceptance passed with the same formal image.
  Artifacts: `run-2b865a05b3504498ac6c62b1cc110fa5` and
  `run-c55577fa86c74768b29d0b582fa3a480` below the common artifact root.
  Budget reservation now includes the largest selected node count plus its
  host client: five-node flows reserve 3 CPUs/3072 MiB per slot, independently
  from three-node concurrent isolation reservations.

- Explicit remaining server API mapping (source prefix
  `app/crowdb-kv-server/tests/server_api_test.rs::`):
  - `list_remote_replicas_empty`, `add_and_list_remote_replicas`,
    `add_remote_rejects_local_replica`, `remove_remote_replica`,
    `remove_remote_rejects_local_replica`, `remove_remote_not_found` →
    `kv::membership::replica_lifecycle`.
  - `stale_membership_epoch_rejected_before_persistence`,
    `membership_epoch_replay_is_idempotent_but_conflicting_payload_is_rejected`,
    `new_replica_bootstrap_accepts_authority_epoch_two_from_local_epoch_zero` →
    `kv::membership::epoch_replay_and_authority_bootstrap`.
  - `batch_add_remote_replicas_from_topology`,
    `progressive_setup_multiple_stores_groups_replicas` →
    `kv::progressive::topology_wiring_and_progressive_setup`.
  - `step_down_via_api_flips_leader_to_follower`,
    `step_down_rejects_when_not_leader` →
    `kv::maintenance::synchronous_step_down_fence`.
  - `step_down_group_not_found`, `flush_group_drains_local_engine`,
    `flush_group_not_found`, `wipe_user_data_clears_keys_and_preserves_group0`,
    `wipe_user_data_store_not_found`, `wipe_user_data_group_not_found`,
    `wipe_user_data_in_openapi` →
    `kv::maintenance::flush_and_wipe_preserve_group_and_resume_writes`.
- Explicit async source mapping (`async_ops_test.rs::`):
  `readiness_api_returns_200_when_leader_elected`,
  `readiness_api_returns_503_before_leader_election`,
  `get_operation_returns_404_for_unknown_id` → management discovery plus
  cluster readiness; `async_step_down_returns_operation_id`,
  `sync_step_down_preserves_old_behavior`,
  `ready_endpoint_after_full_cluster_lifecycle` →
  `kv::operations::async_and_sync_leader_lifecycle`.

## Snapshot join acceptance

- `snapshot_join_e2e_test::e2e_new_member_joins_via_snapshot_then_catches_up_wal_tail`
  maps to `kv::snapshot::snapshot_import_precedes_wiring_and_catches_up_tail`.
  Two voting nodes acknowledge exact pre-join values. A bare third store joins
  through the release management API, then direct MIN_SLOT reads prove imported
  values before any remote wiring. One bootstrap batch wires its existing peers;
  existing nodes add it as non-voting. An acknowledged post-join write is read
  locally on that replica after WAL-tail catch-up. The source file and explicit
  Cargo registration are removed after acceptance.
- Fixture profiles now declare per-node groups and omit `--groups` for bare
  stores through the normal release startup path. Mixed group layouts require
  explicit case-controlled wiring. Readiness accepts omitted empty `groups`
  fields while requiring exact configured groups; that response boundary has a
  focused fixture test. First run found the omitted-field parsing issue, fixed
  without changing server behavior or deadlines.
- Scoped recovery acceptance passed in
  `.crowdb-runtime/artifacts/container-e2e/run-124c62ba7fa8467da45d741af9e371aa/`.
  Group-0 restore/discovery and empty-root system initialization remain pending.

- Final combined acceptance for this batch passed: fourteen independent
  fixtures, thirteen unique Rust test functions and seventeen exact client
  phases, plus three release startup rejection checks. Artifact directory:
  `.crowdb-runtime/artifacts/container-e2e/run-42152dd490414633bb80a5c7426bf32f/`.
  Warm client build 0.413s; scenario wall 32.816s, two slots,
  6 CPUs/6144 MiB. Fixture checks 19/19 (0.474s); workspace fmt/clippy and
  changed Python compilation passed. No production component UT was removed.
  Group-0 restore/discovery/initialization is the next KV migration scope;
  other service domains and final Pixi/CI classification remain unfinished.

## Group-0 bootstrap and restore

- `system_init_test::{system_init_creates_store0_group0,
  system_init_idempotent_store0, system_init_creates_store_visible_in_list,
  system_init_group_visible_in_list}` and `restore_test::{first_boot_root_only_no_toml,
  restart_restores_group0_from_disk, restored_store_port_is_not_reallocated}`
  map to `kv::bootstrap::initialize_and_restore`: empty root/no config, 201 exact
  identity, 409 repeat init, public lists, mounted WAL directory, SIGKILL restart,
  fixed port 7001 retained and next store assigned 7002.
- `system_init_test::system_init_with_custom_replica_id` →
  `kv::bootstrap::custom_replica` (replica 5).
- `system_init_test::prepared_store_ownership_survives_restart_and_rejects_takeover`
  → `kv::ownership::prepared_owner_survives_restart`: prepare without visible
  membership, persisted authority, conflicting/generic init/store/join fences,
  authorized init and idempotent retry.
- `system_init_test::competing_prepares_have_one_durable_winner` →
  `kv::ownership::competing_prepares`: concurrent 200/409 and winner replay.
- `system_init_test::explicit_cleanup_fences_delayed_bootstrap_and_allows_a_new_operation`
  → `kv::ownership::cleanup_fences_delayed_requests`: idempotent cleanup, fenced
  old requests, new init, rejected old cleanup and fences retained on restart.
- `reconcile_test::{reconcile_skips_when_group0_missing,
  reconcile_skips_when_group0_empty, reconcile_healthy_after_init}` merge into
  bootstrap health checkpoints before/after init and restart. These old cases
  never invoked a second reconcile after init, so no extra internal reconciliation
  contract is inferred. Preserve all pure reconcile/membership planning tests
  and all four disk-scan/group0-existence UTs.
- Five named flows/eight exact client phases passed against the formal image:
  `run-404ac5c49e27478d94b98671bde259ed`. WAL-directory assertion added explicitly
  before fault injection; repeat focused acceptance includes it. Recovery now
  compares persisted store/group/replica/listener identities captured immediately
  before the fault, including stores created through management APIs.

## Container exit regression diagnosis

- Full parallel KV acceptance `run-1ba84d6ea14448e996a49e52865ef3ce`
  fails `kv::failover::graceful_leader_shutdown` after acknowledged five writes
  and confirmed graceful exit code 0. Two survivors stay pre-candidates;
  `cluster::leader` rejects absence of a stable unique leader. Preserve the
  old deployment source until all three mapped cases pass.
- First divergence: `SocketTransport::connect` performs blocking `connect(2)`
  before setting `O_NONBLOCK`. A disappearing container IP leaves TCP handshake
  retries blocking Tokio workers; pre-vote fanout takes 39s and later ~3s,
  although the configured peer deadline is shorter. This is a production
  transport defect, not a readiness timeout to increase.
- Fix attempt 1 sets nonblocking mode before connect, admits `EINPROGRESS`, and
  lets queued write/error events complete through the existing worker paths.
  A local saturated listener backlog gives a deterministic pending handshake
  component regression without Docker or a production executable. RPC tests,
  C++ lint, rebuilt formal release image and original container failover are
  required before accepting this fix.

## KV client authority boundary review

- `membership_cas_test::independent_clients_cannot_submit_two_changes_or_skip_installation`
  has no deterministic hook or internal assertion: it tests independent public
  clients against an embedded real KV server. Target container owner
  `kv::membership_cas` retains concurrent one-winner CAS, installation barriers,
  previous members, ready/idempotent completion, stale completion rejection,
  unrelated group progress and refreshed client reads.
- `chunk_slot_owner_fence_test::{process_restart_revokes_old_writes_and_preserves_record_cas,
  ordinary_and_malformed_writes_cannot_change_slot_authority,
  raw_owner_rpc_rejects_malformed_authority_and_cross_namespace_mutation}`
  target `kv::authority` with all old public byte/revision/error-code assertions.
  The first source case changes incarnation without actually restarting a
  process; the target name describes that real contract. True persisted restart
  remains a distinct KV recovery owner.
- Retain `cancelled_handover_drains_one_slot_without_blocking_another` locally:
  its held-owner-write and pending-owner-change hooks pin a deterministic race,
  including cancellation and progress in the neighboring slot. Keep its embedded
  fixture and test-util seam; public containers cannot replace those assertions.
- New targets are awaiting formal-container verification. Do not remove old
  owners until each mapped target passes.

## KV client public flow review

Source `lib/crowdb-kv-client/tests/e2e_single_node_test.rs`:

- `put_get_delete_round_trip_via_topology_discovery` -> `kv.client_data` same name: official discovery, positive revision, exact value, higher delete revision and MinSlot not-found. Passed release-container client 0.38s.
- `revision_cas_and_conditional_batch_round_trip` -> `kv.client_data` same name: create-if-absent rejection reports exact current revision, matching conditional delete/put commits atomically, exact related value. Passed 0.46s.
- `batch_write_and_scan` -> `kv.client_data` same name: two acknowledged records, complete untruncated scan. Passed 0.33s; add exact sorted values before final acceptance.
- `concurrent_domain_monitor_ensure_reconciles_cas_busy` -> `kv.domain_monitor` same name: 32 concurrent official client ensure operations, exactly one Created, all other AlreadyExists. Passed 0.38s against real Group-0.
- `scan_keys_only_and_scan_count` -> `kv.client_scan` same name: 12 exact sorted keys with empty values, counts 12/0/5. Passed 0.44s.
- `scan_deadline_returns_partial_result` -> `kv.client_scan` same name: 3,000 acknowledged records, complete no-deadline scan, expired deadline returns timed_out/truncated proper ordered prefix. Population simplified from 3,000 sequential writes to one acknowledged batch. Passed 0.42s; strengthen exact full values before final acceptance.
- Retain `concurrent_watch_subscriptions_keep_independent_push_handlers`: fixed internal watcher admission checkpoint isolates push-handler multiplexing.
- Retain `reverse_scan_paginates_without_repeating_boundaries`: internal per-page byte-budget knob forces pagination boundaries.
- Retain `read_your_writes_uses_auto_tracked_watermark`: client session-state transition checks.
- Retain `linearizable_multi_page_scan_pays_barrier_once`: deterministic budget and internal exact read-barrier metric, 1 rather than N.

Source `lib/crowdb-kv-client/tests/chunk_slot_map_test.rs`: all eight cases exercise public Group-0 records/client APIs; targets `kv.slot_map` (initialization atomicity/idempotence/layer independence, conflicting concurrent initializers, pagination preserving empty owners, route refresh/unregistration) and `kv.slot_map_validation` (legacy range state, legacy chunks, corrupt owner/partial generation, orphan binding preservation). Source retained until container verification.

## Failover verification follow-up

- Image OCI `sha256:c46270ee88ab849b5e5781d61352ffdfaf3d9f9669f76c3b28b83d3216978d60`, config `sha256:32245c52a081944e3de56c80ae1421261cb61bdf4d4ab456e49cc628abc5bcab`, source `5b87f3bd99b2037d6c208977bb19956f23411194`, retained debug info.
- Coordinated run `run-2510162cd1d74c0da8b538838ebcc636`: original CRUD/restart/isolation, public client six flows, authority three, membership CAS, Group-0 seven and graceful shutdown/remove-leader passed. Graceful shutdown client 4.81s, original missing-IP election failure fixed without weakening the scenario.
- Required aggregate FAILED: unavailable-voter/follower-removal wrote after removal, but exact linearizable reads failed. Reproduced alone `run-fef77fcdccb640389ea2b3fd202fe873`. Server logs reveal `newly connected pool must have an open connection` task panics against the refused fourth RPC port; pending Phase-1 recovery can die before opening read readiness. Preserve original scenario and source launcher until fixed verification.
- Root-cause attempt: `ConnectionPoolIndex` must treat asynchronous close between connect/publication/selection as a recoverable transport error. Both install and degraded replacement remove only their failed candidate generation; no lock, no caller retry/timeout change. Add deterministic closed-candidate UT; map error through all transport consumers. Gates and rebuilt-image acceptance pending.

- Native follow-up: isolated run exited 139 without OOM. Apport `/var/log/apport.log` explicitly rejected `/opt/crowdb/bin/crowdb-kv-server`; no core exists under `/var/crash` or the mounted crash root. Attached GDB to owned fixture PIDs using exact retained-debug binary/source without changing host collector policy. Artifact `refused-native-debug/crowdb-e2e-f3fc5f34f14041799325fc1583bdb82e/gdb-node0.log` captures SIGSEGV in `RpcClient::fail_all`, called from `Connection::close` on the transport worker while client attachment replaces the `std::function` close callback. The initial symbol-less capture is diagnostic only, superseded by the symbolized stack.
- Root cause: callbacks were mutated in place concurrently with worker invocation; FFI attachment first installs the raw client callback and then replaces it with an owning closure. Publish fully constructed immutable callback versions through lock-free atomic pointers, retaining old versions until connection destruction. No new mutex, Rust unsafe, caller retry or timeout changes. Deterministic C++ UT holds close callback invocation while replacing it and verifies client-owner lifetime through connection destruction. This complements the closed-candidate Rust pool regression.

## KV metadata controller migration

- `chunk_slot_map_test` maps each of its eight public cases to the identically named `kv::slot_map` or `kv::slot_map_validation` case. Preserve atomic initialization, exact legacy records, full slot assignments, empty owners, no-head rejection, corrupt/partial generations, route refresh and unregistration. Source remains until rebuilt-image acceptance passes.
- `chunk_slot_handoff_test::a_replacement_controller_resumes_partial_fences_without_replacing_the_cohort` -> `kv::slot_handoff` same name: controller replacement resumes durable receipt progress, forged receipts and competing cohorts fail, fencing retries are idempotent and publication cannot be persisted separately.
- `chunk_slot_handoff_test::competing_cohorts_use_the_service_head_revision_and_reject_stale_maps` -> `kv::slot_handoff` same name: head revision reserves one cohort, stale generation fails and stale CAS cannot discard persisted receipts.
- `chunk_slot_publication_test` four cases -> `kv::slot_publication` identical names: cached missing endpoint refresh; stale publication conflicts and concurrent full-map regrants; atomic/idempotent complete authority publication with unchanged storage; one concurrent winner and unchanged data fences. Shared setup initializes real Group-0 and data group through the official host client, with no embedded server or internal hook. Source remains pending acceptance.
- `kv::cluster::topology_and_follower_hint` now also forces the official client's cached leader to a real follower, then verifies hint-followed writes, increasing revisions and exact reads. The old single pinned-role hint-follow case has only the same public write/revision assertions; remove it after this target passes, while retaining bounded/cyclic/no-hint retry UTs.

- RPC fix committed as `6c5bc476`: complete `test-cpp` passes tree 624, common 31, RPC 81, Diskio 139 and both FFI suites; focused KV election 76 and RPC migration 4 pass. `tree-lint` exits 0 with existing warnings; affected Rust transport clippy and workspace fmt pass. Rebuild formal retained-debug release image before accepting the unavailable-voter scenario.

## Remaining KV-client local contracts

- Retain all fourteen `e2e_follower_read_test` cases locally. The fixture pins leader/follower roles and deliberately keeps one learner outside replication while its synthetic topology advertises it. Each read policy checks internal distribution/fallback metrics; two tests inject exactly 50 ms read delay to verify load and latency selection. These controlled lag/selector transitions complement real container election and follower-hint flows.
- Retain `conditional_retry_test::{conditional_writes_discover_leader_after_explicit_rejection_without_a_hint, conditional_unknown_leader_retries_are_bounded, cyclic_leader_hints_exhaust_a_bounded_budget_for_all_write_forms, discovering_a_replacement_endpoint_does_not_wait_on_the_failed_endpoint_backoff}`. Synthetic no-hint/unknown/cyclic role states verify exact three-attempt budgets, five write forms and internal counters; 250 ms replacement bound isolates a one-second failed-endpoint backoff.
- Retain `topology_publication_test::repeated_identical_hint_does_not_discard_an_inflight_topology_refresh`: Notify pins publication during repeated hint injection.
- `topology_endpoint_test::wildcard_listener_uses_reporting_management_host_for_remote_clients` is a public normalization flow. Map to the official container client's wildcard-listener topology discovery and exact write/read checks; verify the actual advertised wildcard before deleting the stub/server source.

## Runtime subset implementation boundary

- Existing `CROWDB_PROFILE_PATH` mounts a normal deployment profile, but `run_preview` unconditionally starts all seven services. Fix this normal monitor path before storage fixtures depend on multi-service subsets. Do not add a test supervisor or build per selection.
- Validate known release service IDs and their real dependency requirements before creating files or starting children; generic profile validation remains usable by supervisor fake-child UTs. Derive management seed from the selected KV probe rather than requiring Access env.
- Keep KV and logical-topology bootstrap; include disk files/hardware only for storage selections, S3/Iceberg bootstrap only with Access. Start only selected service IDs and run matching readiness/registry probes. Recovery validation repeats the same selection. Preserve the default complete profile behavior and durable profile digest fence.
- Split preview service startup/recovery helpers under the owning preview module rather than growing the existing 550-line root. Tests must prove valid KV/storage subsets, unknown IDs, missing dependencies and repeat/recovery step order; container acceptance uses the same retained-debug artifact with host-visible mount roots.

- Deduplication review: `e2e_retry_test::client_follows_not_leader_hint_to_real_leader` uses pinned roles only to establish an ordinary follower redirect, with no internal race/counter assertion. Its raw rejection/hint and two successful writes are now owned by `kv::cluster::topology_and_follower_hint`, which also verifies exact values against an actual elected cluster. Delete the source after new acceptance. Retain the four conditional retry tests and fourteen controlled follower-read tests.

- Rebuilt formal release artifact for RPC fix: source `6c5bc47606ed2e500bc8533956dcdfe25e473499`, OCI manifest `sha256:a44ebecd72d7588f8e78f0e713711f2afa30c39629997d83fe2f72d26b579146`, config `sha256:f1338467ef62b70542339def054629a745ce7ff58dcc1c4002940edd0c66e5ff`, debug info retained, runtime SHA `d4a16c4358b213ca193f442a3c2db87d8d630b2ff097adeade26d2827afded9d`. Build/import is not acceptance; focused failed-case and complete KV run follow.
- Affected app/client/E2E all-target clippy passes. Retained KV-client tests pass: cancellation 1, conditional retry 4, controlled follower read 14, single-node internal flows 4, topology publication 1, old public redirect 1 (pending deduplication). Fixture gate 25/25, 0.642s, including new scope inventory and fail-before-allocation rejection.

## Verified source retirement

- `deployment_reconfig_test::{graceful_shutdown_leader_under_load, reconfig_via_api_add_then_remove, reconfig_via_api_remove_leader}` -> `kv::failover::{graceful_leader_shutdown, unavailable_voter_and_follower_removal, remove_leader_and_continue}`. All three pass the exact rebuilt formal image during full concurrent acceptance. Delete that source/registration and its final unused `common::{process,test_client}` launcher; preserve direct-path component helpers.
- `group0_discovery_test::prebootstrap_registration_waits_for_election_before_admitting_writes` -> `kv::discovery::paused_bootstrap_admits_no_registration_writes`; `prebootstrap_nonmember_discovers_group0_and_retains_hints_after_restart` -> `kv::discovery::nonmember_recovers_discovery_and_registration_identity`. Both pass with explicit inspected-IP advertisement and independent node mounts; identity-conflict/corrupt-binding UTs remain.
- All fourteen slot-map/handoff/publication targets pass. Delete the three legacy public test files and their now-unused embedded handoff server helper. The wildcard discovery target confirms the actual `0.0.0.0:7001` listener and exact official-client values, and the elected follower target confirms raw hints plus corrected-cache writes/reads: delete both duplicate public client source files.

## KV migration acceptance closure

- Full required concurrent acceptance PASSES in `run-5d914d0b899045439e600b7510c5f044`: 46 named flows plus the two-fixture original CRUD/restart/isolation pair, 48 owned fixtures, 47 distinct Rust cases, 54 exact client phases and three release startup rejection checks. Warm client build 0.440s; scenario wall 85.987s; two slots, 6 CPUs/6144 MiB. Every fixture reports pass including owned collection and teardown.
- Previously failing unavailable-voter/follower-removal also passes alone in `run-8c9a9bf1907e4fb5b4f7071dd274f4ed`, 3.17s client; complete parallel run 3.18s. Graceful actual leader shutdown passes 4.70s. Preserve original refused listener and missing-IP faults and all acknowledged exact values.
- `test.md` now records all 48 measured flow rows with image/source identity and separate start/client/restart/collect/stop/wall phases. Remaining domains and final Pixi/CI classification remain pending. Next active task is normal release monitor service selection, followed by storage domains.

## Diskdb client migration acceptance

- Source `allocation_lifecycle_test::allocate_commit_and_free_through_client`
  -> `diskdb::allocation::allocate_commit_and_free_through_client`: preserve
  32 simultaneous first frees, allocate/commit/query drill-down, all 400
  concurrent allocate/free cycles, persist-only free, compact all twelve zones,
  capacity conservation and allocation after reclaim. Simplify verbose reporting
  and make each concurrent operation assert exact counts and no failures.
- Source `ownership_routing_test::wrong_owner_free_is_ignored_by_compaction`
  -> `diskdb::ownership::wrong_owner_free_is_ignored_by_compaction`: retain
  service endpoint add/remove, accepted wrong-owner immutable free fact and
  compaction rejecting it without reclaiming the current 1 MiB allocation.
- Source `space_reuse_test::exhaust_free_compact_and_reuse_exact_space`
  -> `diskdb::space_reuse::exhaust_free_compact_and_reuse_exact_space`: retain
  1536 units/four zones/three disks, exact non-overlap and exhaustion, duplicate
  response-loss retries, free remaining busy until compact, exact freed-address
  reuse and delayed old-incarnation rejection after replacement allocations.
- Three targets passed with two independently owned fixtures concurrently in
  run-12cc7c61426c42498ff12e85536cbcf6, scenario wall 7.424 s, client build
  2.841 s. Each fixture uses two release containers, KV 7000/7001 and Diskdb
  7100/7101/7102. The advertised RPC IP equals the inspected owned address.
  Preserve Rack 1/Node 10/group 100/instance 999 and original disk geometry.
- Delete the three source files after equivalent target acceptance; preserve
  all six config/normalization/routing snapshot UT assertions (passed). The
  shared Diskdb harness still has other callers; retire it after their migration.
  Internal application flows depending on real KV still need splitting into
  public E2E and deterministic memory-backed component logic.
- First startup attempt exposed missing required `[storage]` fields in the new
  fixture; complete the original normal configuration rather than changing
  timeout/assertions. Collect Docker stderr along with stdout, with a regression
  UT preserving machine-readable inspect stdout. Fixture gate: 27 passed.
- Exact shared release source d8bf780c351bf3afec87624db74956410e8ced5f,
  OCI manifest sha256:743539d20444ad687f00b771d37d257a5e30803f2b84ade5e40934dc99bfbdd4,
  config sha256:10254caf607d21cb102e0b1fd636d55a65625eb06d54895bad0d01cf6bc3be9a,
  debug info retained. All domain selections consume this one artifact.
