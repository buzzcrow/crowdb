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
- [ ] **Release startup service selection**: document and implement runtime
  full-set/subset selection through the normal entrypoint/profile contract,
  preserving existing bootstrap modes. Reuse monitor dependency validation;
  test unknown/missing dependencies and selected-service restart/reconciliation
  against one unchanged image. Files: release entrypoint/profile, monitor
  profile validation/runtime, container README, E2E profiles/deployment cases.
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
- [ ] **KV management batch**: move startup/exit, management store/group/replica
  lifecycle and error cases; parameterize compatible checks, keep membership
  epoch and Group-0 wipe assertions. Files: kv-server tests and `tests/kv/`.
- [ ] **KV recovery/membership batch**: add profile node-count/config support;
  migrate restore, snapshot/WAL tail, dynamic group/membership and Group-0
  discovery; retain deterministic KV-client flows and test actual production
  leader retry. Retire component process helper only after all callers migrate.
- [ ] **Diskdb batch**: add dependency-ready Diskdb profile, map client and
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
  Other server API cases stay until their independent assertions migrate.
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
