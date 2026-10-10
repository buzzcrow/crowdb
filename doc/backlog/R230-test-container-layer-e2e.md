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

Create a dedicated container E2E component, provisionally
`container/crowdb-e2e`, with common cluster lifecycle support and suites grouped
by KV, Diskdb, Diskio, Chunk/storage, access, console and deployment. Production
components retain tests of their own logic; tests requiring real server
processes progressively move into this component. Existing clients and test
languages remain usable; centralization does not require rewriting all tests.

Invariants:

- **I1 — Owned isolated cluster**: each concurrently mutating scenario owns its
  network, containers, persistent volumes, credentials and cluster identity.
  Unrelated tests cannot discover or modify its nodes or data.
- **I2 — Real packaged servers**: E2E executes the intended server binaries from
  an identified container image, not hidden host substitutes or in-process mocks.
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
4. **Networking and clients**: default parallel fixtures use dedicated bridge
   networks, fixed container-internal ports and runtime-assigned host ports.
   Container-local services progressively use fixed four-digit ports; the initial
   KV layer uses management `7000` and store RPC pool `7001..7011`. Namespace
   isolation replaces offset, probe and cross-test port-allocation algorithms.
   Listeners follow the [restart/ownership contract](../design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart):
   enable `SO_REUSEADDR` before bind; retain fixed endpoints through restart;
   reject unresolved live ownership conflicts without port hopping or default
   `SO_REUSEPORT`. Remove legacy allocation code only after its remaining host test/deployment
   callers migrate. Host-published temporary ports remain runtime assigned.
   HTTP/UI clients can run on the host. Clients depending on advertised RPC
   topology or callbacks run in the fixture network unless endpoint routing is
   explicitly supported. A client sidecar joins only its owned network and uses
   the locked test dependencies. Dynamic store/server ports must be included in
   endpoint discovery; a mapped management port alone is insufficient.
5. **Storage and fault control**: ordinary fixtures use independent simulated
   disks/data roots and retain real persistence checks. Kill, pause, partition,
   restart and invalid-state scenarios specify which service/node is affected
   and whether monitor automatic recovery is enabled. Faults do not alter
   unrelated fixtures or the host. Tests needing deterministic internal hooks
   retain equivalent coverage or use an explicit test build whose identity is
   recorded; do not add unrestricted production fault endpoints for convenience.
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
- **Concurrent fixtures**: given two KV profiles from the same image -> run
  concurrently with overlapping container-internal ports and distinct data ->
  node discovery, writes and cleanup stay isolated; terminating one leaves the
  other's reads and topology intact (**I1, I5**). E2E test.
- **Layer dependencies**: given each declared layer profile -> start it and run
  its migrated operations -> all required real services are ready, exact data
  is checked, and unrelated services are not required (**I2, I4**). E2E test.
- **Endpoint routing**: given host HTTP clients and network-local RPC clients ->
  allocate temporary ports and exercise topology discovery/callbacks -> all
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
- **Scheduling**: given fixture budgets and a lower host capacity -> request
  parallel suites -> scheduling stays within the declared budget; incompatible
  fixed-port host profiles never run concurrently on the same host (**I5**).
  Unit test.
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
