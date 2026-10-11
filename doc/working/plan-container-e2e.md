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
- [x] **Diskdb batch**: add dependency-ready Diskdb profile, map client and
  application assertions, split internal handler flows from real metadata
  setup, migrate allocation/recovery/fence/journal boundaries. Files: Diskdb
  tests, harness `diskdb.rs`, container `tests/diskdb*`, profile/runner.
- [x] **Diskio batch**: add simulated/null profiles and explicit special-device
  scope; migrate RPC errors, registry sync and durability. Same-port file/direct
  restart passes; shared listener error/collision audit remains in closure. Files: client/C++ launch sites, `tests/diskio*`.
- [x] **Chunkdb batch**: add configurable rack/disk geometry and fault policies;
  split mixed full-stack cases, migrate lifecycle, degraded repair/conversion
  and journal handoff with deterministic component flows retained. Files:
  Chunkdb tests/harness, `tests/chunkdb*`, storage profiles.
- [ ] **Chunk client/stream batch**: migrate exact object/stream publication and
  all four production restart/takeover/error flows; separate opt-in benchmark
  profiles. Files: chunk client/stream tests, `tests/chunk_stream*`.
- [x] **Chunk-KV batch**: add service/catalog readiness; migrate server startup,
  transport and storage-backed split/transfer boundaries; preserve local split
  proof tests and add release-image durable split coverage. Files: Chunk-KV server/
  library/client tests, `tests/chunk_kv*`, profile/runner.
- [ ] **S3 batch**: migrate HTTP/production and SDK chains; parameterize shared
  bootstrap, retain policy/errors/multipart/restart matrix. Files: access-server,
  console/CLI S3 fixtures, SDK scripts, `tests/access_s3*` and client adapters.
- [ ] **Iceberg/Dataset batches**: migrate catalog/FileIO/SDK/GC/crash boundaries,
  preserving worker environments; migrate audited Dataset server flows. Files:
  access tests/helpers, SDK tasks, `tests/access_iceberg*`, `tests/access_dataset*`.
- [~] **Console/CLI batch**: migrate real-service operations and web route flows;
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
- [x] **Mixed-domain concurrency acceptance**: preserve KV/KV acceptance and
  add three-voter KV versus standalone release Web, with independent networks,
  bind mounts and host client workspaces. Both write exact witnesses before the
  barrier; crash/restart/verify/retire KV, then verify Web's exact rack/node data
  and health. `--scope kv.isolation --require-isolation` runs both pairs.
  Accepted `run-000c4652c4444d648b20f92008062c7a`: scenario wall 14.249 seconds,
  host build 9.018 seconds. Failure wake-up/owned teardown component proof passes.
  Files: `e2e-test/mixed_isolation.py`, `tests/console/web_isolation.rs`.
- [ ] **Final Pixi test classification**: separate component logic gates from
  required container E2E domain gates; use one canonical selection path, update
  suite aggregators and old aliases without duplicate execution. Files:
  `pixi.toml`, `tools/pixi-tasks/`, `tools/ci-checks/check-ci-test-tasks.py`.
- [ ] **Final CI jobs**: replace legacy host-server E2E jobs with shared-image
  domain matrix jobs, resource limits and mixed-domain isolation acceptance;
  retain component and special-profile gates. Update preview/release dependency
  and accepted-digest wiring. Files: `.github/workflows/`, CI task graph checks.
- [ ] **Final domain measurements**: populate `doc/working/test.md` with final
  complete-domain runs by environment, counts, wall time and report links.
  The user revised tracking to one row per domain: per-case/SDK/fault details
  stay in artifacts, not this persistent document. Its simplified rules,
  environment baselines and remaining issues are recorded; full measurements
  still await migration closure. Never sum separate batch timings or label
  partial migration acceptance as a full domain pass.
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

## Diskio verified source retirement

- `disk_io_durability_test::disk_io_e2e_durability` ->
  `diskio::durability::disk_io_e2e_durability`: exact 4096-byte payload,
  fsync success, SIGKILL/same-IP/7200 restart and exact recovery; retain the
  25,220-offset/182-byte unaligned witness. Add O_DIRECT profile and exact
  2048-offset/238-byte range read, including incomplete frame-magic prefix.
- `disk_io_full_test::disk_io_e2e_full_flow` -> separate
  `diskio::io::{mem_disk_full_flow,null_disk_full_flow}`: preserve read-before-
  write, 0/100/4096/1 MiB/2 MiB sizes, overwrite, zone 1, offset 8192,
  high-half disk ID, fsync and all 100 concurrent cycles; exact bytes/patterns.
- `disk_io_errors_test::disk_io_e2e_error_paths` ->
  `diskio::errors::disk_io_e2e_error_paths` and
  `diskio::faults::disk_io_fault_errors`: preserve write/read/fsync missing-disk,
  write/read missing-zone and deterministic injected I/O failures. No whole-case
  retry or absent-binary skip; fault readiness accepts only explicit IoError.
- `disk_io_group0_sync_test::disk_io_e2e_group0_sync` ->
  `diskio::group0::disk_io_e2e_group0_sync`: exact instance/ownership heartbeat,
  connection pool stability across 100 refreshes, malformed owner preserving
  prior routes, new disk reconciliation, exact raw and semantic payloads/fsync.
- `semantic_io_test::semantic_client_owns_route_transport_payload_and_durability`
  -> same named `diskio::semantic` case: retain buffered/durable/scatter-gather,
  range rejection before I/O counters, bad zone without retries, status/pools,
  multi-frame views, independent strip overlap, ordered frames, priority
  admission, in-flight failure and replacement generation usability, and
  write/read/fsync DiskFailure without retries. Replace port allocation and
  host child processes with owned dormant replacement containers and fixed7200.
- C++ `DiskioStartupTest.WriteReadRoundTrip` and
  `DiskioServerTest.{WriteAndReadRoundTrip,FsyncRoundTrip}` -> file/direct
  durability owners including exact payload, fragment prefix, unaligned direct
  range and sync. `DiskioServerTest.DiskNotExist` -> errors owner;
  `RejectsWriteWithoutCreationTime` -> explicit malformed wire request ID77,
  zero creation time returning exact OldRequest/response ID77. Delete both
  C++ full-server files only after target acceptance.
- Preserve local client `address_test`, `topology_test`, `native_routes_test`:
  arithmetic rejection, one authoritative owner and synthetic lease/generation
  recovery. Preserve C++ aligned writer, engine, disk set, config and identity
  tests, including ordering/SQ pressure/injected engine faults. These own internal
  deterministic invariants and do not launch the packaged server executable.
- Shared Diskio host harness still has chunk client/stream callers; leave until
  their migration. Remove obsolete harness/HTTP dev dependencies. Image reuse checks unchanged
  production manifests and permits only proven dev-only edges in existing lock
  packages; dependency identities/new packages/production edge changes still fail.
- First semantic attempt exposed Docker start completing before RPC listen;
  fixture control now verifies the owned replacement listener before returning.
  Semantic then passes alone and concurrently without relaxing transport errors.
- Image reuse now excludes only unpackaged single-node README; an added UT still
  rejects entrypoint changes. Inspection confirms staging never copies README.
- Shared RPC enables SO_REUSEADDR but currently ignores its option-error result.
  Finish the common listener error/collision audit in the cross-domain closure
  task; regular file acceptance does not claim special-device support.

- Final Diskio acceptance: run-4760a5f986824b02b404acfa4828c361 passes all eight
  fixtures/fourteen phases at concurrency2, client build6.165s, wall20.641s.
  Same exact d8bf780c release/config image; tests and mounts recorded in test.md.
- Full test-cpp passes after removing the five migrated C++ full-server cases;
  retained client UTs3/3 and all-target clippy pass. Fixture gate and tree-lint
  completion recorded below before committing this batch.
- Diskio commit gates: workspace fmt, affected all-target clippy, retained3client UT,
  fixture30/30 (1.009s), full test-cpp and tree-lint exit0 pass. Existing tidy
  warnings remain; no production C++ source changed by this migration batch.

## Diskdb application boundary verification

- Public owner: five `tests/diskdb/` flows, including independent Diskdb and
  metadata crash recovery; run `run-62923a58b6674ed58b9943b84b81dc21` passes
  concurrency two, twelve host phases, wall 12.726 s. Existing client process
  flows were already retired after equivalent container acceptance.
- Retained component owners: `metadata_flow_test` checks internal record/bitmap
  transitions, keepalive checks reconciliation, ownership_fence checks lease
  fencing, recovery checks conservative bitmap reconstruction, relocation_journal
  checks journal transitions, scanner checks evacuation choices. These call
  internal component APIs and retain every assertion. Their shared metadata
  helper now uses independent pinned memory stores rather than three host KV
  executables; fourteen binary-availability skips and duplicate discovery removed.
- Public recovery witnesses all three busy records and revisions, including
  persist-only free. Restart conservatively retains busy bits; compaction releases
  only the freed record and leaves committed neighbors unchanged. Initial test
  expected automatic reclamation incorrectly; `recovery/journal_replay.rs` and
  documented free contract confirmed compaction ownership. Corrected expectation
  with additional pre/post-compaction checks, then all five flows passed.
- Gates: all 173 application tests pass (13.433 s including build), thirty fixture
  UT pass (1.121 s), affected application/E2E all-target clippy and Rust fmt pass.
  Production unchanged; same proven release image reused. Shared harness stays
  until remaining Chunkdb/stream callers migrate.

## Chunkdb allocation transport diagnosis

- First container transfer of `allocation_connect_failure_reroutes_but_lost_reply_does_not`
  failed at the first unsent allocation: TCP ECONNREFUSED was surfaced as generic
  ConnectionClosed, so the pool correctly refused to replay an ambiguous request.
  Artifact: `run-0726ac967e2a40779db1fb65a4c19a1e`. No assertion or timeout relaxed.
- Native transport now retains the narrow ECONNREFUSED fact from socket SO_ERROR
  or failed read/writev before notifying close. Existing FFI Connection exposes it;
  Diskdb transport maps only proven refusal to ConnectFailed, retaining unknown
  outcome for established peer loss/reset/timeout. No new lock or unsafe scope.
- Native regression exercises 100 refusals across worker and caller-write paths
  plus accepted peer loss; it passes. Original allocation test passes and still
  proves exactly two allocations after one reroute and one lost reply.
- Production gates and exact release-image rebuild must finish before claiming
  migrated container acceptance. Remaining Chunkdb fixture work is unverified.

- Stress reproduction exposed a second defect: ASan reported a freed Connection
  at `Worker::run_loop` while a second worker processed an already returned epoll
  event. Events now carry monotonically assigned connection IDs; dispatch resolves
  and retains the corresponding generation through the batch. Descriptor cleanup
  has one atomic owner, and deferred map removal checks ID before erasing a reused
  fd. Connection registration no longer reads a shared map outside its mutex.
- Registry lookup uses existing required Folly ConcurrentHashMap: readers take
  no bucket lock; only connection lifecycle writers serialize. No hot-path mutex
  introduced. Stale IDs resolve to nothing; callbacks remain pinned during close.
- ASan regression passes 100 repeats of 100 connections (10,000 refusals), with
  accepted-peer loss still distinguished. Native full-suite/lint and Rust gates
  are running against the final fix; release image will be rebuilt afterward.
- Chunkdb retained component split is in progress: 33 direct handler cases now
  use pinned memory metadata, retaining all state/fence/task/journal/placement
  assertions. Reservation-prefetch plus mixed-geometry and single-rack helpers
  follow the same boundary. Three Diskio-dependent flows, partial EC byte flow
  and multi-group failover still retain their legacy setup until public targets
  pass. Public allocation, lifecycle and exact-record crash fixtures are added
  but not yet accepted against the rebuilt image.
- Final-source Rust verification: six Diskdb client component tests, both
  allocator-pool tests, 34 retained full-stack internal flows, mixed geometry and
  single-rack placement pass. The latter 36 cases use pinned metadata, including
  reservation-prefetch. Affected RPC/Diskdb-client/Chunkdb/E2E all-target clippy
  and workspace fmt pass; thirty fixture tests pass in 0.933 s. Final native
  full-suite (tree, common, RPC, Diskio and Rust FFI) and tree-lint exit zero.
  Linux verified; kqueue remains subject to the macOS platform gate. No new image
  or migrated Chunkdb container pass claimed before release packaging.

## Chunkdb first public boundary targets

- `allocator_pool_test::allocation_connect_failure_reroutes_but_lost_reply_does_not`
  -> `chunkdb.allocation` preserves unsent reroute, ambiguous lost reply and exact
  two-MiB busy witness. Keep `endpoint_refresh_is_one_complete_publication` local.
- `full_stack_test::chunkdb_full_stack_allocate_seal_delete` ->
  `chunkdb.lifecycle` preserves active/query identity, three prefetched mirror
  copies, all persisted physical assessments, exact byte cursor/KiB rounding,
  unused-tail release, empty deleted layout and exact allocation free facts.
  Target additionally checks repeated delete and public tombstone query.
- New `chunkdb.recovery` witnesses exact public chunk and raw metadata bytes plus
  revision before crashing all three service containers; post-restart checks the
  same records, then seals and deletes through the production client.
- New `chunkdb.placement` checks public mirror 2/3 and EC 2+1/4+2/8+4 matrices:
  exact shard counts and persisted rack/node/disk protection with loss bounds.
- Host test target/bundle becomes `domain_test`/`domain-test`; no server binaries
  or server launchers are bundled. Diskdb diagnostic manifests list the actual
  seeded disk groups for both ordinary and protected Chunkdb fixtures.
- All four new targets remain pending release-container acceptance. Do not
  remove the two old public source cases before the corresponding target passes.
- First release run `run-74d56f13d2334071bd69db5a0cc06261` refused readiness:
  production Chunkdb requires at least three actual metadata voters; the initial
  one-KV fixture exposed an empty metadata voting registry. Correct fixture uses
  three wired packaged KV containers (same store 0, groups 0/1), registers only
  their inspected actual replicas through public metadata APIs, plus Diskdb and
  Chunkdb containers. Five nodes per fixture still fit concurrency two at six
  CPUs / 6144 MiB. Production mode and its startup validation are unchanged.
- Second run `run-187d50f8e55a4be8b89f210c55310cce` successfully started
  production Chunkdb but the probe incorrectly parsed its plain-text `/ready`
  response as JSON. Corrected to require HTTP 200 plus exact `ok`, and fail
  immediately with logs if the service exits. No timeout or assertion relaxed.
- Final-source acceptance `run-6308a1cf82014b9cb72cb3840e65f8c6`: four cases,
  nine host phases including four seeds, concurrency two, 12.181 s scenario wall.
  Removed the old allocation wire case and old public lifecycle case only after
  target acceptance; allocator snapshot UT and all 36 retained handler/geometry
  flows plus it pass (37 tests). Shared host fixtures remain for the still-pending
  three Diskio cases, partial-EC byte proof and multislot failover.
- Rebuilt formal release image from `6f17c8f690d1bfeb594aa010d209be72b8ce1e0b`:
  OCI manifest `sha256:16911413347a2c73c237aa1513151f125609704bd9c1cf8645cb2452d0a62125`,
  config `sha256:6cd7f00266bee11cc6c2820a3e302c1607f1fa31567151ab1ebb3b2fcc31960c`,
  runtime `dd4ac552c34396f4b511bc00a26e9a2c7860fd899640caaf5931fc645e95c66b`.
  Normal release optimizations with retained debug info; all profiles use this
  single artifact. Host target rename and fixture split pass affected clippy/fmt.
- Shared Diskdb fixture regression `run-f188655e12184ecc944f6525a84b57de`
  passes all five flows against this image. Thirty fixture tests pass in 1.041 s.
  The Chunkdb batch stays active for remaining byte/repair/failover boundaries.

## Chunkdb storage migration and restart diagnosis

- Added formal-image six-rack Storage fixture: three KV voters, Diskdb, Chunkdb and six independently owned Diskio containers; host client uses fixed 7200 ports and actual Group-0 owner discovery. Normal runtime conversion configuration enables manual admission without periodic conversion. Actual service startup requires every ConversionConfig field; no image-specific test configuration build.
- Public IO routes all 18 disks and proves owner discovery fails before Diskio startup. Public partial mirror-to-EC conversion compares all twelve data/parity shards, acknowledged cursor, dirty source tail and untouched fresh destination tail. Public repair marks four data/parity shards unavailable and verifies exact replacement segments, unchanged healthy segments and byte recovery.
- Individual preliminary passes: IO `run-915fac852be14c52943835f06ace6efb`, conversion `run-655f610154c14814a44e02192c2f7c94`, repair `run-3e89313f0f304197a8d880cd7b49ebed`. These do not authorize source retirement until the final batch passes.
- Aggregate `run-f48b7d4bddd24dc186caa35b0ddc8aa3` failed recovery: two restarted KV containers exited 139 without OOM. Empty inspected IP was a consequence of process death; preserve the static-IP assertion.
- Diagnostic reproduction `run-9f48ea6f6ad549debd37edfd8f35ecf9` attaches retained-debug GDB through a temporary host-PID diagnostic container, with host libraries mounted read-only. `node1-gdb.log` captures SIGSEGV in tokio oneshot sender drop, called by RpcClient::call's submit-error Box reclamation. Native send published callback state before submit; disconnect could complete/free user_data while submit rejection returned false and reclaimed it again.
- Root-cause fix committed as `94aa2834`: generation-tagged slab ownership; completion claims PROCESSING before reading callback fields; submit cancellation returns false only when its own exact generation wins. Immutable shared map completion has one atomic winner across response, disconnect, reaper and submit cancellation. No added hot-path lock or caller-side replay. Regression checks rejection ownership for slab/map, callback-triggered slot reuse, and concurrent response/disconnect. Native gates pass: full `pixi run test-cpp` (tree 629 plus common/RPC/Diskio and Rust FFI), `pixi run tree-lint` exit 0, affected all-target clippy and Rust fmt. Five new ownership tests pass 20 ASan repetitions, including 40,000 response/disconnect races with leak detection enabled. Rebuilt formal-image verification passes: KV full batch `run-caa924a0a8b54742afac3375e4205231` and Chunkdb full batch `run-9801d52258a74d099b0e77138b3c9e91`.

## Chunkdb storage and multislot accepted batch

- Formal OCI `sha256:a00b2374d2556617437c835addb43f8580c2efa865467bc16a439f9c9ac609a6`, Docker config `sha256:c9d68aac2556f0ed855e9c20a9852c1aab87a80b8605df05a123e9efe79d674e`, source `94aa283499d77d641b2fcd7d9a7846cb12455b8f`, release optimization with retained debug info.
- Chunkdb `run-9801d52258a74d099b0e77138b3c9e91`: nine cases pass, concurrency two, budget 12 CPUs / 12288 MiB, client build 0.574 s and scenario wall 54.618 s. Full KV regression also passes on the same immutable image.
- `partial_conversion_and_four_shard_repair_preserve_exact_bytes` maps to manual conversion, expired public client claim takeover and four missing data/parity shard repair. Preserve all twelve exact byte witnesses, source zero padding, dirty destination tail, completed durable tasks and healthy shard identities. Component claim fencing and deterministic transitions remain local. Delete `partial_ec_background_test.rs` after this acceptance.
- `three_nodes_preserve_all_purposes_across_independent_maps_and_paxos_failover` maps to three actual released Chunkdb owners, all six purposes, independent service/storage maps, atomic chunk/task/finalizer revisions and actual KV leader loss. Copy count two adds physical protection. Delete `slot_cluster_test.rs` after acceptance.
- `diskio_routes_cover_every_group_in_the_two_rack_fixture` maps to unavailable-before-start discovery, eighteen disk routes and exact durable segment bytes in the six-rack profile. Two-rack assessment truth remains pinned locally. Remove its former executable test function.
- Multislot initial fixture assertion expected three addresses instead of all seven owned servers; corrected the declared profile size. Subsequent failover showed the metadata reader supplied only the killed first management seed; instantiate it with all three real voters, matching original cluster client discovery. No operation replay or assertion weakening. Focused `run-a09a06e677684697bc75cb73e879c171` and final concurrent batch pass.
- Remaining Chunkdb work: split unsafe large-matrix internal placement/rebalance proofs from public release boundaries; production degraded node-return repair and public handoff acceptance. This batch does not close the full migration.

## Chunkdb degraded placement and journal boundary

- `production_degraded_ec_task_repairs_after_node_returns` splits into public `chunkdb::repair::topology::degraded_ec_recovers_protection_and_exact_bytes_after_node_returns` and pinned `placement_task_requeues_and_survives_scanner_replacement`. Public release test starts with two healthy physical domains, records the actual attempted durable placement task, restores a third domain and verifies 2+1 EC, all protection bounds and exact acknowledged fragment bytes. Focused `run-f7ef1d871fa342049d08df72ee0445d2` passes. Component scanner replacement verifies RetryWait, exactly two attempts and claim generations without an executable dependency. Use the actual task due time and a live writer lease so unrelated finalizer admission does not consume the single executor slot.
- Unsafe 10+2 / 20+2 / 40+4 topology expansion remains an important internal component matrix; normal release forbids that initial unsafe layout. Replace KV executables with pinned memory and Diskio executables with volatile protocol stubs. Preserve exact initial degraded assessment, expansion, task/checkpoint convergence, safe cross-domain rebalance, Accepted-to-SourceFreed journal and final protection bounds. Public real copy/fsync and bytes have container owners. All 36 full-stack component cases pass (20.30 s), plus all other `crowdb-chunkdb --tests` targets. No server binary discovery, skips, shared executable semaphore or host port scan remains in Chunkdb application fixtures; component listeners bind port zero directly.
- Added public `chunkdb::handoff::relocation_journal_copies_publishes_then_frees_exact_source`: three original mirror byte witnesses, tentative replacement, paused Chunkdb authority, acknowledged Diskdb journal with source still retained, normal owner publication, exact preserved healthy segments/bytes, and durable SourceFreed/free fact. Important local handoff identity, rejection, claim and grace assertions remain component-local.
- First public journal run `run-7b8b788583b04ddd867db76edff2fd3d` reached Accepted but could not retire within 30 s: normal Diskdb journal resumption uses the 300 s rebalance planner timer. Configure the Storage profile through supported `[rebalance] enabled=false, plan_interval_secs=1`; resumption runs before new-planning enablement, so existing journals progress without introducing unrelated balancing. Configure all normal lifecycle fields with layout validity 1000 ms (old internal test used 1 ms). No client replay or timeout increase. Focused `run-82fecd2e286c40d79e6a5806ed1f0284` passes.
- Ten-case concurrent acceptance before the journal profile change: `run-d5225c26abfe4d5dad05ada979ee07ec` passes. Final eleven-case batch including the shared Storage configuration passes at `run-6ae19b8e9e724a3fb3dc2b6edf863481`: 67.887 s scenario wall, 0.647 s client build, concurrency two. All source retirement and component splits above are verified; continue with client/stream boundaries.

## Chunk client and stream migration in progress

- Added stream host-client scopes for idle authority takeover, complete service restart, exited Diskio and live write errors. Preserve exact old bytes, stale-writer rejection, unchanged failed tail and rollover assertions. Client processes stay outside eleven independently owned released containers.
- Persistent storage mounts eighteen regular files. CLI disk IDs are hexadecimal: passing decimal metadata IDs silently creates different IDs and auto-discovery adds null disks, so initial writes appeared acknowledged while framed reads failed. Correct the CLI IDs and seed exactly one 128-MiB physical zone per file. Three KV voters, Diskdb, Chunkdb and six Diskio services recover exact stream bytes. Focused `run-fe456f3fdc4a4b46b4da67ee550bd71e` passes before the one-zone metadata alignment; rerun final profile after that alignment.
- Live write-error and idle-authority cases pass in `run-3e7858d5baf94b3f9fce72e49e3dfbe9`. Exited Diskio remains unresolved: `run-c2b48b1ad45d446a926090b3b6bb016e` exceeds the original 15-second append budget. Initial writes to vanished bridge endpoints reach their 5-second operation deadlines; mirror repair then tries three other destinations sequentially. A removed container's address behaves as a network blackhole, unlike the old localhost refusal. Keep the original deadline and tail/rollover assertions; no source retirement or passing status is claimed. No production fix attempted yet. Temporary host tracing records each phase.
- Five large-object host-client cases are running: full data/parity and metadata, one-copy mirror ranges, concurrent complete strips, chunk rotation/range streaming, and unknown-size partial tails. Keep old sources until exact acceptance passes. Important deterministic writer/reader fault, ordering and pressure UT remain.

- Object final acceptance `run-d3773353e00943cc8704485116f3a8b8`: all eight cases pass. Normal release rejects one-copy mirror allocations; public mirror cases use two physical copies with the original byte/range/concurrency assertions. Retain deterministic single-copy boundary/error and in-flight ordering proofs in `chunk_writer_test`; no unsafe placement is enabled in the image. Retire five original large-object cases and three small-object public cases after acceptance; important component UT are untouched.
- Stream restart final one-zone profile `run-b30240af4fa64db0aed6c0ab4c0a913d` passes. Retire original idle takeover, full service restart and live-Diskio-error cases; the unresolved exited-Diskio source remains. Affected client/stream/E2E all-target clippy passes. Retained component test targets for client reader/writer/small objects/local-file streaming and all seven stream component files pass. Fixture ownership/control gate passes 31 tests.
- Pending user design choice: retain the old 15-second whole-storage-outage append bound by adding an overall production fault budget, or preserve current per-I/O/replacement retries and distinguish a vanished container endpoint's network-blackhole behavior. No timeout/assertion change or production workaround has been applied.

## Additional Chunk client and Chunk-KV acceptance

- Chunk client reader seven cases and large repair eight cases pass in `run-7871c12a3acb4616b299af7be4401862`. Their original `chunk_reader_e2e.rs` and remaining `large_object_writer_e2e.rs` sources are retired. Exact data/parity, partial healthy ranges, corruption markers, transient failure non-marking, restart repair, coalescing, ordered buffers, repair exhaustion and abort/drain assertions remain. The whole mixed run failed in a separate small conversion case and is not recorded as a suite pass.
- Normal protected mirrors require two copies. Transient read failure now exhausts both replicas before asserting the original caller error and empty durable corruption markers. Small fault injection counts now account for configured replica count, preserving the acknowledged predecessor/prefix before the injected failure. Original single-copy deterministic writer UT remains.
- The small-flow follow-up `run-6f99bf60aec84742b17caa0b5e3468a2` passes manual conversion, automatic scan, in-flight claim recovery, repair and queue pressure; the exited-storage read case misses the original 15-second bound for the same pending network-blackhole budget choice as stream append. Its original source remains. A previous conversion-active observation also failed during a concurrent run; it passed the unchanged follow-up and needs final-suite verification.
- Register the two missing manual/automatic conversion cases. Name reader, mirror/EC repair, background conversion, batching and queue pressure modules by responsibility. New fixture inventory verification prevents unregistered migrated cases and duplicate execution. The scheduler completes all independent fixtures after ordinary case failures, retains the original failure and cancels pending work on interruption. All 34 fixture UT pass (1.047 s).
- Chunk-KV production transport rejection passes against a formal twelve-container persistent profile in `run-e60b51663b364143a10d1b6e2e98c3b5` (12.057 s scenario wall; build 89.447 s includes the Cargo lock wait). Preserve the source empty/unpublished catalog by omitting normal `bootstrap_partition`; verify zero hosted partitions, catalog generation zero and false readiness, then all original typed `NotMyRange` responses. A bootstrapped full-range catalog legitimately reroutes point hints; it is a distinct profile and must not substitute for the original empty-catalog case.
- Formal-image missing-config startup passes in `startup-51860db6b5d949a68705bc50e9fb0d9b` (0.394 s, nonzero exit and the original diagnostic). Retire the original host executable startup and in-process client transport source after those passes. Collect diagnostics from both stdout/stderr, matching the original startup assertion.
- Internal root authority/pin proofs use pinned memory groups 0 and 1 instead of a host KV executable. Both pass (0.21 s); all retained Chunk-KV client/server component tests pass: 97 cases across 27 test binaries. Component all-target clippy passes. Keep deterministic memory-tree/stream service transition, fencing, split/transfer and raw wire correlation proofs local.
- Added a separate production routed native CRUD/CAS/tombstone/seek/forward-reverse scan witness with cold-client verification after all twelve services restart. Acceptance is pending; no recovery pass is claimed. The storage restart loop explicitly targets the six Diskio nodes, leaving subclass-owned services to their own lifecycle.

- Final named Chunk client run `run-21df8676b48245858979b6c373e04da8` executes all 40 cases (263.815 s wall, 5.140 s build). 38 pass; whole-storage-outage read and the one-copy assumption in functional benchmark counters fail. Benchmark counters now assert exact physical replica requests/objects/buffers/logical bytes for the configured two copies; both functional benchmarks pass in `run-3f5767fedbc54bb6834424cd4510d0d2` (12.061 s wall). Thus 39/40 Chunk client cases have passing final assertion owners; the outage read remains pending the choice below. Retire all verified small-object cases and both old benchmark cases; retain only the original outage read and the original stream outage append. Their shared legacy fixture is reduced to the remaining setup/lifetime/crash functions, without altering the remaining assertion or one-copy baseline.
- Chunk-KV final `run-a563226b470342ab91474403ad22de27`: two cases / three host phases pass (34.589 s wall; 9.479 s build), including exact native records, conditional results, tombstones, inclusive seek and both scan directions after all twelve services restart. It runs concurrently with the last Chunk client fixtures on the same formal image and independently owned networks/data. Remove unused host-launcher/server dev dependencies from the migrated component gates. Shared Chunkdb profile regression: eleven cases pass after the restart/profile extension; see `/tmp/crowdb-chunkdb-shared-profile-regression.log` and its recorded run artifacts.

## Blocked

### Managed Console mode contract choice

- Formal Docker web mode has `require_editable` middleware rejecting every non-read request with 403. The old managed native browser flow expects physical-mutation 503, successful KV writes and writable Iceberg/S3 demos. It is incompatible with the current whole-Console readonly mode. This is a product-mode choice, not a reason to relax the old assertions silently.
- Original host baseline fails before browser startup because its monitor launcher omits required discovery configuration. Unchanged first browser flow against the formal image confirms the disconnected `MonitorSummary`; reconnecting the existing component and source label passes 171 UI component tests, production build and TypeScript checks.
- Fresh-image run `run-ac7bbae4baf9418cac72b3c66a54f37a` preserves both assertions: the combined data flow fails on `/api/racks` 403 vs 503; the pause/outage flow times out at its original 10-second response bound with disconnected preview requests during supervised recovery. Ownership/PID-validated container signaling and trace/log evidence are retained. No client timeout, request retry or assertion changes.
- Alternatives sent to the user: keep formal Docker readonly and move writable behavior to an ordinary writable Console profile; or restore Docker logical/data writes while retaining physical restrictions. No response yet. Original managed browser source and host runner remain. Other modules continue independently.


Only the exited-storage timing migration awaits a human design choice; S3 and subsequent independent migration work continues.

- Original stream append and single-copy reader tests require failure within 15 seconds after Diskio process exit. Container disappearance leaves bridge endpoints as network blackholes. The stream append exceeds 15 seconds after per-I/O 5-second deadlines and sequential replacement attempts; the protected reader also exceeds 15 seconds after replica/route refresh attempts. Exact failures: `chunk_stream::production_stream_write_returns_after_diskio_failure` in `run-c2b48b1ad45d446a926090b3b6bb016e`; `chunk_client::small_flow::replica_repair::protected_read_reports_diskio_process_failure` in `run-21df8676b48245858979b6c373e04da8` (`Elapsed(())`, test 17.39 s including fault control).
- Alternative A: retain the 15-second overall outage bound and introduce a shared production operation/fault budget across reads, writes and replacement attempts. This changes cancellation/ambiguous-write handling and requires a deliberate production contract.
- Alternative B: retain current per-I/O/replacement retry policy, distinguish vanished endpoint/network blackholes from process-refusal outages, and define the corresponding container fault contract explicitly. Preserve the original process-refusal timing proof locally until equivalent release-container coverage exists.
- An asynchronous user question requested this choice. No choice is inferred from the preselected answer or elapsed time. No timeout was increased, assertion weakened, caller retry added, or production workaround implemented. Original host cases remain; these two migrations and the full all-domain gate are not declared complete. This is a contract choice, not exhaustion of five root-cause fix attempts.

### S3 migration in progress

- Host compatibility clients now live under `e2e-test/clients/s3/`, split by
  wire, upload, multipart, copy, concurrency, object, metadata and CLI contracts.
  All 23 existing unittest methods have one named `s3.protocol` runner owner.
- `AccessS3Cluster` adds two formal release Access containers to the protected
  persistent 12-node Chunk-KV profile, with fixed 7500/7501 endpoints and mounted
  logs/data. Host Group-0 credential provisioning checks issue/ensure/lookup
  identity; temporary credential handoff is owner-only and deleted immediately.
- First acceptance remains in progress. Preserve original S3 launchers until
  native metrics, admin CLI, response loss, each service restart and language SDK
  assertions have equivalent passing owners. No whole S3 pass is claimed.

### Additional Access production launchers

- `protocol_production_policy_test` now runs against the persistent multislot
  container profile: three Chunkdb services and three metadata groups, exact
  native S3/Iceberg bytes, protocol protection policies and WAL/tree/index
  purposes. The original host launcher was deleted after its owner passed.
- Combined S3/Iceberg HTTP policies now run against one normal combined Access
  container. Exact small mirrors and 2+1 versus 4+2 EC policies passed; the
  original combined HTTP launcher was retired.
- Terminal small-write-manager handling and two/three-protocol shutdown were
  extracted unchanged into `listeners.rs` and covered by three component UTs.
  The old file-triggered test-util subprocess hook was removed. The existing
  Chunk client manager-failure proof remains local. A freshly staged formal
  image must verify these equivalent production lifecycle paths next.
- The original protected exited-Diskio repair/rotation test remains under the
  existing `s3-e2e` feature as an explicit legacy E2E comparison. Its 15-second
  bound and all repair/failure counters are unchanged. It is a third instance
  of the existing exited-IP versus connection-refusal budget decision, not a
  unit test and not a migrated passing owner.
- S3 host task delegates are explicitly excluded from image source inputs:
  staging and the OCI builder do not read these three scripts. Build tooling,
  runtime sources, dependency/features and new packages still require a new
  image; fixture provenance tests cover this boundary.
- Per-node CPU quota is explicit. The host client retains a separate 0.5-CPU
  reservation; CI uses 0.25 CPU per server, fitting a single 14-node fixture
  in 4 CPU / 8192 MiB. Protocol and language clients passed at this quota.

## Iceberg first verified owners

- Formal `AccessIcebergCluster` adds two normal Iceberg frontends to the
  protected 12-server persistent storage fixture. Fixed 7502 endpoints,
  supported runtime configuration and mounted data/logs replace host launchers.
  Native-file tests initialize their own catalog and start the selected GC
  frontend only when needed; component test clients never start host servers.
- `iceberg_full_stack_test::metadata_create_and_commit_stay_within_operation_budget`
  maps to `access_iceberg::metadata` with identical time/get/CAS assertions.
- `iceberg_full_stack_test::namespace_functional_crud_survives_native_storage_and_listener_restart`
  maps to `access_iceberg::namespace`, retaining the official PyIceberg client
  on both frontends before/after restart and exact persisted properties.
- `iceberg_file_storage_test::native_file_tree_publication_and_ranges_survive_catalog_storage_restart`
  maps to `access_iceberg::native_files`, preserving checkpoint/publication/range
  and multipart recovery assertions, strengthening native mirror protection
  from the old unsafe colocated single copy to two independent rack copies.
  Its GC helper now starts the normally configured release worker container.
- All three pass in `run-4db6ca739f2f472f9687867bc6c8dd6a`; original three cases,
  file-storage target and its now-unused multipart/worker host helpers retire.
  Catalog lifecycle and other consumers keep their original ownership until
  separately migrated and verified. Fine timing rows are in `test.md`.
- First PyIceberg attempt expected six routes on the full-delegation profile.
  The migrated client checks all 14 routes there; the short-delegation catalog
  lifecycle profile retains the exact six-route expectation. No namespace
  assertion is dropped.

## Iceberg catalog and GC accepted owners

- `iceberg_full_stack_test::catalog_recovery_survives_real_chunk_kv_restart`
  maps to `access_iceberg::catalog`; all original root/authority/retry/journal
  and conditional namespace assertions pass in
  `run-4e9a903dd7b74cfd9d1e3a00b04384c5`. Two fixed-address release frontends
  stop/restart through owned container control, and the CLI executable/libraries
  come from that exact image for the host management client. Retire the old
  full-stack target and its seven unused native client helpers.
- The four `iceberg_gc_control_test` cases map to `access_iceberg::gc` and
  `gc::foreground`, preserving task authority, admission/idempotency,
  pause/resume, stalled-storage retry, durable purge/retired catalog scheduling
  and simultaneous public SDK foreground progress. Three pass in
  `run-e6d70f2565cf473a8003fb6914e660be`; foreground passes in
  `run-6c94205d10954458a2cc548f495aa897`. Retire the old target after all four pass.
- Normal GC configuration selects the worker and page/interval bounds; ordinary
  client lifecycle control starts its deferred release container. No custom
  worker executable, injected production endpoint or fault image is added.
- Host PyIceberg/native task aliases delegate the migrated scopes. Existing
  in-memory namespace SDK limits proof remains in its component and still runs.
- Rust SDK clients now have a root child project with the original pinned lock.
  The response-loss/native restart owner passes in the ongoing SDK run; the
  native retirement owner keeps its 1500-second external budget and full
  production grace wait. Do not retire these original SDK sources before the
  complete SDK gate succeeds. Host client deadlines are declared per owner;
  ordinary cases retain 120 seconds and child SDK deadlines remain unchanged.

## S3 batch-delete phase budget diagnosis

- Fresh-image 0.25 CPU run passes 37/38 owners; the thousand-key sequence exceeds
  the runner's 120-second cumulative subprocess limit. The 0.5 CPU focused run
  passes in 112.320 s; the two-slot 1 CPU run also reaches the cumulative limit
  after initial/repeated calls (17.319 s and 34.442 s), during its third call.
- Exact source audit: original `run_boto3_case` used `Command::output()` without
  a cumulative deadline; the 120-second process cap was introduced by the
  migration runner. Boto3 request deadlines/retries have not changed.
- Split the same owner into initial/retry/new-publication host phases on its
  one owned fixture. Preserve three full thousand-key requests, exact returned
  keys, no errors, fresh-publication deletion and zero final objects. Record each
  phase separately with its bounded 120-second client process limit. The third
  response now also checks exact keys/no-errors. No request retry or timing
  assertion is added or weakened. Await focused/full acceptance before closure.

## Iceberg SDK accepted owners and client isolation

- The full native retirement and lost-reply restart gate passes in `run-b5aa23e83362458ea5272ca331bb292a`; full retirement takes 1206.136 s with its original grace intact. The complete namespace/table lifecycle owner passes in `run-41cfc3ee4b15416c90f70e958a12b9fe`. Retire the two old SDK test sources and delegate their Pixi alias to `iceberg.sdk`. Move the remaining version-check reference and old child-project directory during final task taxonomy cleanup.
- Three signed FileIO cleanup/session owners pass in `run-8fb3396aafc445b5a9d3bcf62c2bbe76`. Further range/streaming/threshold owners are being verified before deleting their old bodies.
- Bundle each run’s host Python/foreign SDK sources and locks, compile the pinned Rust SDK from that snapshot, and record every client language in its source digest. SDK prepare/build products stay inside the run. The protected native fixture explicitly selects two large mirror copies as well as two small copies.
- The phased thousand-key S3 owner passes in `run-90c1aaa3f8f44a95b788a8a790fdd50b`; fresh full 38-owner acceptance remains running.

## Fresh S3 and FileIO gate closure

- All 38 S3 owners pass on the fresh image in `run-095737a638d44969b4514582ee277b44`, including the bounded three-phase thousand-key owner. Earlier failed cumulative-cap runs remain documented, superseded by this successful exact gate.
- Seven FileIO owners pass together in `run-0280cfaaed50430780f313188cff296b`; remove the six newly equivalent old HTTP bodies and their unused cleanup helper/imports. Protected large data uses two normal mirror copies.
- Default access-server and retained Iceberg component gates pass; fixture tests, all-target clippy and workspace fmt pass. Remaining FileIO publication/crash/SDK/capacity, Dataset, Console, deployment, final taxonomy/CI and the exited-storage deadline choice remain unfinished.

## Managed Console first divergence

- Original host runner baseline fails before browser startup: its monitor is missing the current required discovery configuration. Container baseline runs the unchanged first browser flow against the formal image (`run-9effe6c52f8349379ceef8d698f31e68`) and confirms a separate existing UI regression: `/api/preview` returns confirmed Group 0 and real monitor observations, but `MonitorSummary` is never mounted by `App`. The normally configured Docker-mode container profile (`run-813a60fc0fce4bbcbf1274f7ba4a03eb`) confirms the same missing component.
- Reconnect the existing monitor summary in managed mode and expose the confirmed source label. Keep the original browser assertions and 3-second UI bounds; 171 component UI tests pass and production TypeScript/Vite build is verified before refreshing the formal release image. The original failed browser gate is retained; acceptance follows with the rebuilt exact image.
- Browser outage signals will target the observed KV PID inside its owned release container through guarded fixture control, preserving the original STOP/CONT behavior and live monitor observation. Other Console migration remains pending.

## Large FileIO and Java accepted owners

- Twelve FileIO owners pass together in `run-06f8475d48c7489db24b39ff91714689`; the added 32-request direct-upload owner passes in `run-677b374720884908bf44c3b1c14b2088`. Keep complete bytes/ETags/ranges, metrics, checksum rejection, actual credential expiry, lifecycle fencing and restart replay. Retire only their equivalent old bodies and the now-unused lifecycle/metrics helpers.
- Three Java native owners pass in the exact separate runs recorded in `test.md`. Their original deadlines remain 600/1200/600 seconds. The locked Maven child project lives under root clients and compiles once per run before parallel dispatch. Verify the combined new `iceberg.sdk.java` scope and delegate its former host FileIO task; Rust keeps a separate `iceberg.sdk.rust` selector.
- Resource accounting distinguishes the full managed profile's single physical container from its 2-CPU/2048-MiB service footprint, adds host-client reservation, and honors a higher requested quota. Per-fixture ownership/PID signal guards and CLI selections are unit tested. Managed Console's two browser flows remain explicitly unaccepted pending the mode contract choice above.

### Protected Iceberg capacity recovery acceptance

- The formal release-container owner `access_iceberg::file_http::capacity::full_simulated_disk_preserves_file_authority_then_recovers_after_compaction` passes with six full simulated disk groups and two protected copies. Original file-authority, exact-byte, GC resource-stall/proof and recovery assertions remain. All held blocks are freed and every affected disk compacted before recovery.
- Initial migration failure came from carrying over disk-group IDs 100..105; the protected fixture declares 1000..1005. Corrected the fixture references, preserving all assertions. The client passes in 23.56 s; `/tmp/crowdb-iceberg-capacity-gate-2.log` records acceptance. All-target E2E clippy passes. The obsolete host-stack capacity target and its private workspace helper are retired after acceptance.

### Dataset listener ownership and shared CI artifact

- Audited all four Dataset component test files and the access-server implementation. Component tests use in-memory stores/algorithm fixtures and remain intact. Two production HTTP listener tests were embedded under `src/dataset.rs`; equivalent container clients pass in `run-5569f38c94964c71b2d50abf3789e4c6`, then their inline listener implementations retire. Initial new full-manifest comparison used the caller draft snapshot ID; the existing publication API assigns an authoritative ID. Expected metadata now uses the publish response, preserving original returned-ID assertions and adding complete metadata/byte comparison.
- CI S3/Iceberg jobs now depend on and import the existing `ContainerImage` artifact. A separate storage matrix selects eight migrated layers with bounded parallelism and diagnostics. Final complete classification and legacy launcher closure remain unfinished.

### Final regression work in progress

- Coherent accepted FileIO/Dataset batch committed as `be5757bb`; Console clients and CI matrix remain separate worktree drafts. Root staged selections were checked independently before committing; unaccepted Console owners were excluded from that commit.
- Eight-layer concurrent regression at the proposed CI quota: DiskDB, DiskIO and ChunkDB pass; Chunk stream reproduces the unchanged 15-second budget failure. Deferred Chunk-KV/Dataset/deployment invocations are correctly rejected once Dataset source-test retirement changes the image source identity. Rebuild the formal release image before rerunning those layers; no bypass added.
- All 42 focused publication/recovery component cases pass across seven Iceberg test targets. These retain creation/staging/commit/file/MPU replay and failure boundaries; the legacy native custom-listener crash matrices still require exact assertion mapping before retirement.
- CI component-task inventory passes for all 28 workspace packages. Shared artifact YAML parses and validates with system PyYAML through Pixi; default and Iceberg environments do not include PyYAML. CI runtime acceptance and complete taxonomy remain pending.

## Console operation and host CLI migration

- Standalone KV recovery before/after Group 0 initialization now uses the formal release Web and its normally deployed KV child. Preserve empty pre-init stores, live recovered server, sealed bootstrap-intent consumption, confirmed Store-0 authority, ordinary Store-7/Group-1 creation and exact acknowledged value/rack recovery. Whole-container abrupt restart also kills the child, strengthening the original Web-only pre-init restart. Three standalone scopes pass concurrently at 0.25 CPU in `run-4d4f063a409e4ddfbb7d23aa719e3146`; recovery client 9.868 seconds, fixture 11.728 seconds (old focused baseline 3.86 seconds). Preserve original 20-second startup and 30-second HTTP budgets. First gate failed because the host cannot read the release-owned private config; guarded Docker file transfer exports the config and imports the host-captured intent with its original owner/mode. No release permission changes. Foreign ownership, invalid indexes and oversized/missing intent guards pass. Retire the equivalent native owner only after this gate.

- Strengthened registry acceptance also matches both initial/replacement API PIDs to the persisted runtime identity and requires the identity file to disappear after stop. Fresh `run-66a319b2ba7c409f86d0ad4c4dde9fa4` passes: 2.486-second host client, 3.996-second fixture; no deadline changes.

- Launch registry acceptance now runs the shipped Bare Metal Web entrypoint and auto-starts the shipped KV binary using the normal launch registry. Host client verifies initial PID, absent topology (KV stores empty and Web authority 503), different restart PID, stopped process, live auto-start policy reload and no PID persisted in policy. Original 5-second startup/3-second request budgets remain. `run-3eb59840d48c462784faac4049d8d294` passes (2.434-second client, 4.057-second fixture), against the unchanged release image; old native Web/fake-child baseline passed in 2.64 seconds. Retire only the old server-launching owner and helper; keep Docker's registry rejection component test.

- Consolidate the four old frontend listener owners into the accepted standalone release case: shipped SPA mount, deep links, exact health body, API JSON content type and no repository-file leakage all pass in `run-ed609d7c88714ab58ac193cfe46214e8` (1.206-second client). Missing-built-UI behavior remains one deterministic oneshot handler test, including the original instructional marker/build hint, API/health coexistence and traversal boundary (0.02 seconds); it no longer starts a local HTTP server. Original four-owner baseline passed in 0.11 seconds. A missing direct `http_body_util` dependency was corrected using the existing Axum body API, without manifest changes.

- Standalone Web empty UI restart and malformed-config rejection move to `console.web.standalone`: supported release `server web`, one independently addressed container per case, fixed port 9090 and host-mounted persistent data. All original mode/empty topology/UI/rack/node/config-path/nonzero/config-preservation assertions pass concurrently at 0.25 CPU in `run-606e79c944934ad28f5a2d5231f1f835`; retain the original 20-second listener bound and abrupt Web termination. Retire only those two old native owners. Baselines: 0.20/0.05 seconds. Other standalone recovery and lifecycle owners remain pending.

- Web handler component fixtures no longer launch host KV binaries: `storage_readiness_test`, `chunk_kv_catalog_test`, and `managed_mode_test` use pinned in-memory groups 0/1 plus a topology peer. All seven original assertions pass before and after conversion; preserve ownership completeness, configured/discovered owner validation, journal/window bounds, managed snapshots and forbidden mutations. Public authority, catalog owner changes, slot windows and shipped managed deployment remain owned by release-container acceptance. Baseline target times were 0.91/2.24/6.01 seconds; component target times are 0.12/0.76/5.20 seconds. The managed snapshot proof now runs unconditionally instead of skipping when a host server binary is absent.

- Move discovery (10), hardware authority (5), group/store publication (7), bootstrap/resume (10), cleanup/fanout/delete (15) and deployment identity (3) to `e2e-test/tests/console/`. Each mutating flow owns three release KV voter containers and fixed 7000/7001 ports. Named assertions are unchanged; host HTTP/proxy doubles test client failure boundaries and never replace production KV.
- Split mixed `bootstrap_intent_test`, `deployment_test` and `ops_cluster_test`: their six deterministic local owners remain and pass. Retire eight fully migrated operation sources and their old KV launcher helpers only after the corresponding complete scopes pass.
- The original `concurrent_conflicting_store_publication_is_preserved` passed on its old harness. The first container batch unexpectedly accepted a conflicting store; a diagnostic read confirmed the committed conflict but could mask ordering. Remove that diagnostic before acceptance: the unchanged complete seven-owner scope passes. Record the first divergence; the combined operation gate is an independent check, not a caller retry.
- Move CLI direct/observation/hardware/management/data and three registry-backed public flows to host clients. Missing host CLI is an error, not a skip. Compile and pin it once per run; record executable hashes alongside the domain client and locked source. Local fake-child launch controls remain in the CLI component. No port allocator or actual server launcher remains in the migrated CLI helpers.
- `console.operations` combines the 65 accepted host operation clients for broad CI dispatch; fine discovery/hardware/logical/bootstrap/cleanup/deployment/clients/cli scopes remain selectable. Managed browser proof remains separate and unaccepted pending the mode choice.
- Two FileIO benchmark owners pass on persistent protected formal storage (`run-087b7c04f0bc47b68b603d3b27fc1f03`). Delete the old equivalent native profiles and upload helper; retain the distinct per-write publication/crash matrix until its assertion map and ownership are closed.

## Async apply conflict discovered by migration

- Combined 65-owner Console operation gate has 64 passes and one failure: create-if-absent store publication can overwrite an already acknowledged competing store. Focused/old-harness passes did not resolve the ordering failure.
- Deterministic regression `cas::create_if_absent_waits_for_acknowledged_async_apply` parks the preceding ordinary write's engine apply after acknowledgement. Before the fix, CAS chooses slot 2 while slot 1 is unapplied; the regression fails with chosen frontier 2 rather than 1.
- Production defaults enable async engine apply while the pinned leader component defaults disable it. `propose_cas_owned` read the engine revision without the apply fence used by linearizable reads. Fence the captured last-chosen frontier before the revision check; no new locks or caller retries. Commit `712bafe6` passes four CAS, two read/apply fence and twelve owner fence tests, all-target KV clippy and workspace fmt.
- Fresh formal release source `712bafe63bcc0bd1f63a574f5350e900da2321c0`, OCI `sha256:5024981383d993cedfdbbbfe31d35ec0b4dabae4ab303331f139a8618968e354`, config `sha256:c6c054e56522efcdde98366cabd114571e13e3253cf4cc1c9e650819dd75faf6`; optimized debug info retained. The complete seven-owner logical scope passes at the CI quota (0.25 CPU per voter); all 68 Console/CLI/Bare Metal Web owners pass together at the CI quota in `run-f1e60806f97f447283bc24ce904c1cf4`. Both FileIO performance owners also pass on this image in `run-b4c543cc563746e389617b8a680f20b6`; fine per-owner client and fixture times are recorded in `test.md`.
- Bare Metal Web authority's three old oneshot owners now use three release KV voters and two independent release Web listeners. Original hierarchy mutations, exact returned authority, duplicate/expired registration rejection, later replica host validation, no stale/local fallback and all-voter-exit 503 assertions pass (`run-074958bb66394e4cab941daddcd4d226`). Remove the old source after this gate. These writable Bare Metal checks do not decide Docker's readonly policy.
- Java and Rust SDK sources/locks were exact duplicate projects. Keep their single owners at `e2e-test/clients/iceberg-{java,rust}` and repoint retained injected-store SDK wire component tests. Their fake store/block/paging/response-loss assertions remain local; they never launch the packaged storage services. Maven compilation, six SDK wire owners and the namespace empty-page owner pass with the explicit pinned JDK/Maven environment; version verification follows the root Rust client project. Remove only byte-identical old project files.
- `benchmark_replaces_its_previous_log_directory` is a confirmed pre-existing CLI component failure: unchanged body SHA256 `bc7f76db5289eb6d992b7fd633f1d519180162fa2b0bd5c80b3f71744ef65e4b` equals the original source, and CLI production sources are unchanged. `bench rpc --server-port 1 --metrics-interval 0` returns success after asynchronous connection failure, violating its existing exit-status assertion. Keep this test and assertion; other retained CLI help/fake-child controls pass. This is separate from migrated container flow acceptance.

- Accepted Console/CLI/Web and native FileIO cohort committed as `08c995ee`; staged fixture tests passed independently (37) with pending managed-browser selectors excluded. The actual worktree fixture gate also passed (39).
- Web slot windows and both complete 1024-slot bitmaps now pass through the formal Web HTTP listener in `run-d3d0d1e6d3cf4b638c7022066237e3df`; original maximum IDs, generations, continuation, invalid bounds and missing-owner assertions are unchanged. Retire the equivalent old oneshot source.
- Official Iceberg RCK first container gate executes all six original pinned 1.11.0 selectors successfully (`run-af275a0603714c7997652f02662e9d0b`). Upstream source is isolated per run; strengthened acceptance disables Test task caching and validates JUnit counts before retiring the old launcher.

- Strengthened RCK acceptance passes in `run-d48cca26df0c4eaca6c4d2bb71538ceb`: Gradle Test caching disabled, six JUnit cases executed with no skips/failures. Old native launcher retires; Pixi alias delegates to `iceberg.sdk.rck`. The SDK job reserves 12288 MiB for protected release storage plus Gradle/JVM clients.

- Split the remaining local-deploy owner into formal aggregate observation (`console.clients`, `run-06d28b770ff44e5391f7ec4b0d21e477`) and a deterministic fake-child reaping component test. The component fixture prepares the required log directory and an absolute executable path; both original non-zombie/2-second reaping assertions pass without starting a host CROWDB server. All three Console client owners pass together.

- All fourteen normal release-monitor service crash/hang owners pass together in `run-63921b227f644bd2a162643e3891d318`; preserve original 90-second recovery budget, exact fault-event kind, new PID/increasing generation and readiness. New host clients add confirmed data before/after every fault. First fixture iteration omitted its initial witness write; second incorrectly used ptrace-gated `/proc/PID/exe`. Correct the fixture's setup and inspect the owned child's command line before signalling. No release behavior or caller retries changed. Guard unit tests reject foreign containers, invalid services/signals and monitor/reused PIDs.
- Both hardware bootstrap owners pass in `run-f16aa9b5eff7493787b86398330f1b1b`: root clients preserve restart/idempotence, exact four disks/capacity, owner/bind facts, event count and conflict leaving no hardware. Keep client-side manifest and monitor logs in mounted artifacts instead of deleting them. Retire the old real-server component source.
- Interrupted bootstrap passes in `run-d649f54bbf36450fabd09b7fa5d90d5c`: kill the shipped monitor after a durable completed step, recreate on the same volume/IP, retain deployment ID and every completed step, reach normal readiness and perform real storage operations. Remove the equivalent old shell function and fourteen migrated service fault loops. The remaining legacy deployment proofs are still pending; its restart-exhaustion helper remains necessary.
- Catalog owner publication invalidates saved catalog/runtime cursors through the normal Web listener; remove only that passed old owner, retaining distinct configured/discovered runtime validation.

- Retained Monitor component gate initially found the unchanged storage bootstrap owner's error-message regression: production returned generic `chunk registration conflicts with configuration`, while both old assertions require specific ChunkDB/Chunk-KV conflicts. Restore those original diagnostics in commit `61cb7a78`; focused old storage owner, all Monitor tests, all-target Monitor clippy and workspace fmt pass. A fresh formal release image is being built from this source before new container gates.
- Remaining Monitor real-process KV/storage owners are being migrated to `deployment.kv` and `deployment.storage`: preserve normal bootstrap reconciliation/restart, exact logical records, configured/live chunk identity conflicts and catalog listener readiness; add namespace/data recovery. New `deployment.validation` prepares invalid profile/manifest input before starting the shipped monitor and retains nonzero exit plus original failure messages. These owners remain unaccepted until fresh-image gates pass; old sources remain.

- First fresh `61cb7a78` deployment gates expose three setup/diagnostic issues without changing assertions: client bootstrap sessions opened after creating their log directories fail the clean-data-root contract; initialize the session first. The release UID owns the mount, so prepare corrupt input using an owned filesystem helper before starting the server. Finally Monitor's `Result` termination prints Debug variants, omitting the original operator diagnostic strings. Commit `7a470121` prints Display errors while retaining nonzero exit and destructor behavior; its new CLI regression, all-target clippy and workspace fmt pass. A second fresh formal image is required for the rejection owners.

- Fresh release source `7a47012129be3e3015c463db5f17726593c6e8df`, OCI `sha256:f2e9efbc72ba47411c1a0237b5bcbc6795ab6d0b5eae124453dc6e5c4dd881a9`, config `sha256:bccfcd41f739d0370c4687ffc96bcbc18c45ed0edc0dc64e496e0886cee65649`, retained debug info. Both storage owners pass in `run-c208b8a204e54cec953c8a67413a8b1b`; both invalid-input owners pass in `run-18c32de4733a4cad9a1ec1929cb46a39`. Correct setup preserves the original corrupt manifest owner/mode (crowdb/0600); topology reconciliation validates the original release profile while exported configuration uses a separate local path mapping. Retire old Monitor real-process storage/KV sources and old invalid-input shell functions after equivalence acceptance.

- The formal single-deployment SDK owner passes in `run-128904fc37114011af4eab51a9f5d531`: Boto3, AWS CLI, rclone and PyIceberg write complete small/9-MiB data, namespace/table properties, native listing and grouped revenue, then all original reads pass after whole-node volume recreation. The host ChunkDB client verifies both type-prefixed S3/Iceberg chunks, one mirror segment, capacity 1024 and no placement repair. Mount the normal ChunkDB template with an explicit reachable fixed RPC bind; no image change. Retire three equivalent scripts, their old shell calls, container-executed layout helper and unshipped example. First attempt exposed the loopback-only RPC bind; second template mount violated template_root. Correct only the normal mount location and bind.
- Remaining-launch audit confirms ChunkDB `tests/common/cluster.rs::start_pinned` uses `PxKvStore` with pinned in-memory leaders, not server executables; internal handler/allocation/placement proofs remain component-local. Native Web launchers, custom native fault matrices, S3 mini-cluster operations, ecosystem/host deployment and the two pending contract choices still prevent complete closure.

- Strengthened deployment SDK/runtime owner passes in `run-704a072312a34ea5816676437a12acef`: retain server/client credential mode 0600, crash mode 0700, Monitor/KV crash cwd, unchanged durable credentials across recreation, Group-0 authority/preview, Dataset missing-route 404 and original single-mode readonly mutation 403. All container ports remain unpublished while host clients use the private IP. Normal Monitor readiness executes its configured access health probe. Retire the equivalent old shell public-service/runtime/readonly assertions; managed browser contract remains a separate pending choice.
- Fresh complete 25-owner deployment run `run-f1261241061e4e0faae2fea7ff06c2f2` has 23 first passes and the two diagnosed setup failures above; both affected whole scopes pass after correction. All 26 current deployment owners therefore have acceptance on the same fresh release image; the failed full-run result is preserved instead of reported green. Retained Monitor components pass with the old process launchers removed; staged fixture tests pass independently (40).

- Terminal deployment failure/lifecycle scope passes in `run-c560ffe7c4c54c4fb47ac83461767904`: recover a hung Web child, gracefully stop/recreate the persisted complete node, confirm acknowledged data, then recover five crashed children and require the sixth to exhaust the original restart policy. Preserve nonzero exit and exact event kind. The same persistent logs retain all twelve original lifecycle event kinds; plain/gz service logs and Docker logs contain none of the server/client secret values. Changed durable credentials exit nonzero without another ready event and preserve the original diagnostic. Retire the two old fault functions, shared recovery helper and `logs.py` after this strengthened gate.
- Default anonymous-volume policy and Monitor death pass in `run-d919293a24e64f03a09b125f8087676e`: normal shipped profile, actual anonymous volume, ready manifest and operator persistence warning, nonzero exit after Monitor SIGKILL. Copy log/crash directories to host artifacts before removing the owned container with its anonymous volume. Ownership guard prevents deleting a foreign volume. First fixture treated plaintext healthz as JSON; correct only its decoder. Retire the equivalent old final shell block.

- Original shipped readonly Web browser baseline passes unchanged in 6.089 s against an owned full release node (`crowdb-e2e-3f178dd694ff4014b3c03368a6105a3d`). Container-root `deployment.browser` passes in `run-604b717d7494423f9a4ec4232c6fb7ee`: installed host Chromium, seven domains, hidden Add Rack, exact outage 503/group0_unavailable/observed PID, stale N-1 removal and resumed sidebar. Preserve 3-second UI and 10-second response/signal bounds; replace direct unguarded Docker signals with fixture-local owned PID/executable control. Pinned Console client dependencies have one canonical package/lock; prepare an independent per-case client workspace. Retire old `web-ui.cjs` and both calls; remove now-unused published-port helper/exposure from the remaining shell script. Writable managed Console owners remain unaccepted.

- Six release-startup boundary checks pass against the fresh immutable config in `.crowdb-runtime/artifacts/crowdb-release-startup-accepted-6hkayr4r`. The baseline exposed the obsolete `Error: Decode` expectation after Monitor correctly switched to operator Display diagnostics; require the full `failed to decode deployment profile` prefix instead. Unknown service, missing Chunk-KV configuration, managed-root standalone rejection, invalid profile, unknown selected service and missing dependency all exit nonzero; invalid selections create no node/SSH/bootstrap/KV state.

- Both occupied-listener owners pass in `run-6360a52e1976425abf4c1dcb7900c180`: normal release entrypoint starts a second access container in the owned node network namespace, with separate data/log/crash mount and copied private server environment. Only the protocol under test remains occupied; the other protocol and health listener use explicit distinct fixed addresses. Preserve nonzero exit, Address already in use, 30-second failure bound and parent normal readiness. Reserve the complete node, extra process and host client (3 CPU/3072 MiB at the CI quota). Retire the old docker-exec child launcher.
- Single-node split policy passes on an externally selected data root in `run-17106e704fb44b28971fecac49f07723`: persisted domain descriptor has no range balance policy, no split transition appears throughout the original 12-second interval, actual KV logs contain no local split planned event and acknowledged data remains. Read data mount locations from fixture metadata. The original deployment shell now delegates entirely to the canonical 33-owner deployment layer; no host port publishing, global temporary data deletion, server/client launch or duplicate assertions remain in it. Image packaging/policy task taxonomy and other deployment profiles still await final closure.

## Typed Web lifecycle migration evidence

- Original `service_lifecycle_test::deploys_real_chunkdb_with_fixed_cluster_seeds_and_retained_launch`
  baseline passes (1.37 seconds). Its host owner remains intact.
- Draft release owner has not passed and is excluded from accepted commits.
  Setup corrections uncovered release constraints: init returns 201; unsafe
  placement is unavailable in release; normal Chunkdb needs live DiskIO and
  disk groups with discovered disks; disk IDs use CrowDB's 128-bit format.
- Latest `run-d7d025161b064eebbd7da749bfaf9e3c` reaches normal Chunkdb startup
  but receives 502 after the child exits 0. Mounted structured log identifies
  the first production divergence: group 0/0 has one voting node, incompatible
  with Production. Next fixture must use three actual KV node containers plus
  the writable Web deployment node; keep every original PID/delete/workdir and
  seed assertion. No release-only test switch or deadline extension is accepted.
