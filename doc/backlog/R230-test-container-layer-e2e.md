<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R230: test — Container-backed layered E2E

## Problem

Real-server tests currently live in individual application/library components
and in `container/single-node-container`. Several fixtures independently start
KV, Diskdb, Diskio, Chunkdb, access and console servers on the host. They repeat
configuration, port allocation, readiness, data-directory and process cleanup
logic. This makes local setup harder, allows parallel suites to interfere, and
does not consistently test the packaged deployment environment.

The [deployment architecture](../design/deploy/design-crowdb-deploy.md) defines
node containers, startup policies and persistent identity. Tests should consume
that environment through one test component, with layer-specific scenarios.
An operator should be able to run KV and access E2E concurrently without shared
ports, candidates, data or cleanup affecting the other run. CI should verify
the same scenarios using the same built image as local execution.

## Solution

Use the root-level `e2e-test/` as the unified E2E project, with common cluster lifecycle support and suites grouped
by KV, Diskdb, Diskio, Chunk/storage, access, console and deployment. Production
components retain tests of their own logic; tests requiring real server
processes progressively move into this component. Existing clients and test
languages remain usable; centralization does not require rewriting all tests.

Start with one Rust workspace crate named `e2e-test`, partitioned by domain
modules rather than one crate per server. Python lifecycle/scheduling, browser
tests and SDK clients belong under this same root. A client requiring a distinct
language toolchain or dependency lock may be a child project under
`e2e-test/clients/<client>/`; it consumes the shared fixture endpoints and never
owns a second server launcher. Split Rust child crates only when a demonstrated
dependency/feature/toolchain conflict requires it, with shared lifecycle and
one top-level local/CI entry point retained.

Invariants:

- **I1 — Owned isolated cluster**: each concurrently mutating scenario owns its
  network, containers, persistent volumes, credentials and cluster identity.
  Unrelated tests cannot discover or modify its nodes or data.
- **I2 — Real packaged servers**: E2E executes the intended server binaries from
  the formal release server image identified by immutable digest/source revision,
  not a separate test-server image, host substitutes or in-process mocks.
  The release build/package path may retain debug information for E2E crash
  diagnosis; record this packaging option and its exact image digest. Release
  optimization and server behavior stay the same. Build/import once and reuse
  the artifact across service selections; do not rebuild an image per profile.
- **I3 — Explicit endpoint access**: every client receives usable endpoints for
  its execution location. Temporary published internal ports are limited to the
  test run and bound to loopback; normal deployment exposure is unchanged.
- **I4 — Preserved assertions**: migration retains protocol, persisted-data,
  fault/recovery and timing assertions. Missing prerequisites fail required gates
  rather than silently skipping E2E.
- **I5 — Bounded ownership and resources**: concurrency fits declared CPU/memory
  budgets; teardown only removes owned resources, including after interruptions.
- **I6 — Identical local/CI contract**: local and CI tasks select the same layer
  profiles and identify the exact tested image/source revision.
- **I7 — Coverage with one owner**: each real-server scenario has one container
  E2E owner. Consolidation preserves distinct error, authority, concurrency and
  durability assertions; component tests retain deterministic important flows.
- **I8 — Host clients, isolated server nodes**: E2E servers execute in owned
  node containers; test clients, SDK workers and browsers execute on the host
  using inspected container IPs and fixed service ports. Startup verifies host
  reachability, including advertised RPC endpoints. Independent bridge scenarios
  run concurrently within declared budgets without global cleanup or host-port
  locks; host client workspaces/artifacts are scoped to the run too.

Test boundaries and review rules:

- Migrate tests starting production server executables, including startup/exit
  tests, and externally observable flows currently assembling real production
  services in-process. Group scenarios into domain modules inside the container
  E2E component: KV, Diskdb, Diskio, Chunkdb, Chunk-KV, chunk stream/client,
  access S3, access Iceberg, access Dataset, console and deployment.
- Keep deterministic component algorithms, state transitions, codec/validation,
  pinned-role concurrency and injected-failure tests local. A small protocol
  stub or fake child used to test a client/supervisor is not a packaged-server
  scenario. Mixed tests split at this boundary; do not merely move internal
  implementation dependencies into the E2E crate.
- Preserve distinct UT flow and edge-case coverage; existing case count is not
  a preservation target. Review overlap across modules and E2E, then merge or
  delete redundant cases after mapping their assertions to a verified owner.
  Finer internal state, error, race and crash-cut checks remain component-local
  even when the happy path overlaps E2E.
  After a migrated server scenario passes with its original assertions mapped,
  delete its former test/launcher ownership; do not retain duplicate host-server
  E2E. Mixed tests retain their local UT portion after splitting.
- Review every source case before removal. Record its original name, retained
  assertions, target case/module, local flow coverage and verification result
  in the working plan. File names such as `e2e` or `production` do not determine
  ownership. Unclassified launch sites prevent migration closure.
- Build a cross-module flow coverage map: normal operations, validation and
  authority errors, concurrency, persistence and recovery where relevant.
  Identify gaps and add focused cases. Broad flow coverage is the objective;
  equivalent assertions need one owner, while layer-specific contracts remain
  distinct even when they exercise similar operations.
- Prefer short, independently selectable flows with explicit checkpoints.
  Reuse setup helpers and parameterize equivalent input cases; combine checks
  sharing one lifecycle only when failure location and isolation remain clear.
  Dedicated crash, partial startup, stale authority and timing tests remain
  independently runnable. Fewer tests or faster runs alone do not prove coverage.
- Expand E2E at public boundaries: actual production client routing, exact
  returned data, durable delete, rejected stale/conflicting requests, recovery
  and unaffected neighboring workloads. Keep exhaustive internal interleavings
  in component tests. Never replace a boundary assertion with eventual success
  obtained by retrying the operation under test.
- Use bounded condition waits with diagnostic state, not blind sleeps or whole
  test reruns. Distinguish expected client retries from fixture readiness.
  Assert an acknowledged-write recovery witness before injecting a fault.
- Component test gates must not require an image, Docker or installed server
  binaries after migration. Explicit container/hardware gates fail when their
  prerequisites are unavailable; existing entry points delegate to their owners.

Numbered work items:

1. **Inventory and ownership**: classify existing server-spawning tests in
   `crowdb-test-harness`, application/library suites, console Playwright fixtures
   and container acceptance. Record each scenario's layer, dependencies, faults,
   assertions, runtime needs and target owner. Tests of internal algorithms,
   state machines and deterministic concurrency remain in their own component,
   even when historically named integration or E2E. Real-server portions of
   mixed tests move without losing the internal checks. Rust tests stay under
   the owning crate's `tests/`; no requirement to convert them to inline tests.
2. **Common cluster fixture**: expose create/start/readiness/endpoints/fault/
   restart/diagnostics/teardown operations. The lifecycle is allocate -> start ->
   ready -> test -> collect -> teardown. Failed startup produces diagnostics and
   cleans partial allocations. Run IDs identify all owned resources. Mutating
   scenarios never share a cluster implicitly; explicit read-only reuse requires
   documented isolation. Node/server metadata remains scoped to the fixture.
3. **Layer profiles**: KV starts its required replicas; Diskdb adds its KV
   dependencies; Diskio and Chunk/storage select the required metadata/storage
   services; access and console use their required service chain. Profiles use
   the same runtime image and explicit startup/configuration policies, not a
   different implementation per layer. Readiness verifies usable dependencies,
   not only running containers. Manual single-node bootstrap and explicit
   automatic/read-only single mode remain distinct.
   A three-node cluster consists of three node containers on its private network,
   with the same fixed service-port layout on each node. Profiles select the
   required services within those nodes; test clients remain outside the
   containers. Additional replicas use additional node
   containers rather than host processes distinguished by port offsets.
   Containers are release server deployment units, not test execution
   environments. Each profile selects a server subset or cluster composition
   through supported release configuration and startup interfaces. All profiles
   consume the same release artifact; do not embed test clients, SDK dependencies
   or fault-injection builds into the server image.
   The release runtime supports the complete configured server set by default
   and an explicit validated service selection through its normal launch/profile
   interface. Selection is runtime configuration, not an E2E-only entrypoint or
   rebuilt image. Validate unknown services and dependency requirements before
   starting; omitted services must not be started by later reconciliation.
4. **Networking and clients**: default parallel fixtures use dedicated bridge
   networks and fixed container-internal ports, directly reachable by host clients.
   Container-local services progressively use fixed four-digit ports; the initial
   KV layer uses management `7000` and store RPC pool `7001..7011`. Namespace
   isolation replaces offset, probe and cross-test port-allocation algorithms.
   Define one explicit per-node service port layout; dynamic store endpoints use
   the declared bounded pool and stable persisted assignments, never host free-port
   scans or conflict-driven retries. Multiple cluster runs reuse the same internal
   port layout in independent networks.
   Listeners follow the [restart/ownership contract](../design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart):
   enable `SO_REUSEADDR` before bind; retain fixed endpoints through restart;
   reject unresolved live ownership conflicts without port hopping or default
   `SO_REUSEPORT`. Remove legacy allocation code only after its remaining host test/deployment
   callers migrate. Services listen on container-accessible interfaces and
   advertise reachable container IPs; host clients consume an inspected endpoint
   manifest. Do not use host networking or publish every internal port for ordinary
   bridge tests. Startup verifies direct host access before executing scenarios;
   an unreachable daemon network fails with an explicit prerequisite diagnostic.
   Test clients use locked host dependencies. Dynamic store/server ports must be
   included in endpoint discovery; a management endpoint alone is insufficient.
5. **Storage and fault control**: ordinary fixtures use independent simulated
   disks/data roots and retain real persistence checks. Kill, pause, partition,
   restart and invalid-state scenarios specify which service/node is affected
   and whether monitor automatic recovery is enabled. Faults do not alter
   unrelated fixtures or the host. Tests needing deterministic internal hooks
   retain equivalent component-local coverage; release-container E2E uses public
   APIs and owned process/network faults. Do not use test-only server builds or
   add production fault endpoints for convenience.
   Each node can bind-mount its data root to a run-scoped host directory so
   persisted data, service logs and crash artifacts remain directly searchable.
   The normal launch interface accepts a caller-selected host data directory per
   node; the fixture default is not a hardcoded restriction. All persistent server
   data, service/monitor logs and collected crash reports live below that mounted
   root, with paths recorded for direct local inspection without container exec.
   Failure/interruption teardown removes owned runtime resources but retains
   these diagnostic directories. Core-dump collection follows the actual host
   policy; report a host collector or unavailable core explicitly rather than
   claiming that a bind mount alone guarantees core generation. Preserve the
   matching binary/debug information and build identity needed to inspect a dump.
6. **Scheduling and special deployment tests**: one local runner schedules suites
   using per-profile CPU/memory budgets and a configurable concurrency cap.
   Containers isolate state but do not add machine capacity. Host-network tests
   with fixed ports run serially per host or on separate hosts/VMs. Native host
   networking, real block devices and hardware-dependent scenarios have explicit
   profiles; bridge tests do not claim to validate those capabilities.
7. **CI image and layer jobs**: build the image once, distribute the immutable
   artifact/receipt and split layer suites into Ubuntu runner jobs. Set job-level
   and in-job concurrency limits. Required gates verify prerequisites and never
   succeed because the image/server is absent. Each failure uploads client,
   monitor/server logs, topology, container/resource state and test results, with
   credentials redacted. Publication may consume these gates once integrated
   with the OCI release workflow.
8. **Staged migration and closure**: first migrate a KV real-server scenario and
   prove two isolated fixtures run concurrently; then migrate Diskdb/Diskio,
   Chunk/storage, access and console scenarios in dependency order. Move current
   container deployment acceptance under the same component with distinct
   profiles. For each batch, compare old/new assertions before retiring the old
   server launcher. Preserve existing Pixi entry points as delegates during
   migration; eliminate duplicate E2E ownership at completion. Component-local
   logic tests remain fast and do not require a container runtime.

## Dependencies

- Existing node image, monitor, shared console operations and container tests
  provide the initial deployment contract and migration baseline.
- [OCI build/runtime design](../design/deploy/design-crowdb-oci-image.md) provides
  the shared OCI artifact and containerd adapter. The first centralized fixture
  may use system Docker against that image while preserving the runtime-neutral
  lifecycle/endpoint interface.
- Preserve existing Docker/containerd acceptance while migrating its ownership. macOS VM execution stays deferred.
- Existing production clients, server configuration and `crowdb-test-harness`
  guide dependency setup; central fixtures must not bypass Group-0 authority
  merely to obtain a convenient initialized state.
- GitHub-hosted `ubuntu-24.04` runners include Docker. Use standard Ubuntu VM
  runners, not `ubuntu-slim`, for container/kernel-dependent suites. Upstream
  runner capabilities are references; fixture isolation and budgets remain
  CROWDB responsibilities. References:
  [Ubuntu runner image](https://github.com/actions/runner-images/blob/main/images/ubuntu/Ubuntu2404-Readme.md),
  [GitHub runner execution environments](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).

## Acceptance

- **Inventory/ownership**: given all existing real-server launch sites -> classify
  and migrate each according to its boundary -> every scenario has one owner and
  retained assertions, and component-local logic tests still run without Docker
  (**I2, I4**). Integration test.
- **Review/consolidation**: given each migrated batch's source cases -> compare
  the retained assertion mapping and execute target E2E plus important local
  flows -> every distinct boundary/fault assertion has an owner, local logic
  runs without a container runtime, and no production host launcher remains
  for the migrated cases (**I4, I7**). Integration test.
- **Concurrent fixtures**: given two KV profiles from the same image -> run
  concurrently with overlapping container-internal ports and distinct data ->
  node discovery, writes and cleanup stay isolated; terminating one leaves the
  other's reads and topology intact (**I1, I5**). E2E test.
- **Layer dependencies**: given each declared layer profile -> start it and run
  its migrated operations -> all required real services are ready, exact data
  is checked, and unrelated services are not required (**I2, I4**). E2E test.
- **Endpoint routing**: given host HTTP/RPC/SDK/browser clients ->
  inspect container IPs/fixed ports and exercise topology discovery/callbacks -> all
  returned endpoints are usable from the declared client location and internal
  ports are not publicly bound (**I3**). E2E test.
- **Lifecycle failure**: given partial startup or test interruption -> collect
  diagnostics and teardown -> owned resources are cleaned without touching
  another fixture, and the original error remains the test result (**I1, I5**).
  Integration test.
- **Persistence/faults**: given acknowledged writes and selected recovery policy
  -> crash/pause/partition/recreate the specified service/node -> preserved
  recovery assertions and exact persisted bytes hold, with monitor behavior
  matching the scenario (**I2, I4**). E2E test.
- **One-node policies**: given manual and automatic profiles -> bootstrap manual
  through the UI and start automatic initialization -> manual remains editable
  and expandable; explicit single mode creates virtual disks and rejects edits
  (**I2, I4**). E2E test.
- **Release runtime service selection**: given one release artifact with complete
  server support -> start the default set, then independent fixtures selecting
  valid subsets, and submit unknown/missing-dependency selections -> expected
  services alone start for valid configurations, invalid selections fail before
  partial startup, and all runs reuse the same image digest (**I2, I4**). E2E test.
- **Crash diagnostics**: given release-optimized binaries retaining debug info
  and a node data bind mount -> trigger an owned server crash under a supported
  core policy and teardown the fixture -> host-visible logs/crash evidence and
  matching binary/build identity survive, with symbolized frames verified;
  unsupported core collection is reported explicitly (**I2, I4, I5**). E2E test.
- **Scheduling**: given fixture budgets and a lower host capacity -> request
  parallel suites -> scheduling stays within the declared budget; incompatible
  fixed-port host profiles never run concurrently on the same host (**I5**).
  Unit test.
- **Mixed-domain concurrent execution**: given independently owned fixtures for
  different implemented domains with container servers and host clients ->
  execute them concurrently and crash/retire one -> the other completes its
  exact-data and topology checks, artifacts identify server image and locked
  client build/dependencies, and no scenario uses host servers or cross-run cleanup
  (**I1, I5, I8**). E2E test.
- **Task/job/document closure**: given all migrated scenarios -> reconcile Pixi
  categories, CI image/domain/isolation jobs and `doc/working/test.md` tables ->
  each required case executes once through its canonical task, all domains are
  reachable in CI, and documented counts/timings come from measured final tasks
  with pending platforms marked explicitly (**I4, I6, I7, I8**). Integration test.
- **Native host profile**: given a supported Linux host with explicit resources
  -> run host-network acceptance -> network and actual CPU/memory limits are
  verified; missing required capabilities fail with diagnostics (**I3, I5**).
  E2E test.
- **CI/migration gates**: given a built image and layer matrix -> execute the same
  tasks locally and on Ubuntu CI, then repeat with missing image or failed layer
  -> source/image identities agree, failures preserve diagnostics and cannot
  count as successful skipped tests (**I4, I6**). Integration test.

Target tasks to introduce during implementation; they do not exist yet:

```bash
pixi run test-container-e2e --layer kv --concurrency 2
pixi run test-container-e2e --layer all --concurrency 2
pixi run test-container-e2e --profile host --concurrency 1
pixi run test-container-e2e-fixture
pixi run rs-fmt-check
pixi run rs-lint
```
