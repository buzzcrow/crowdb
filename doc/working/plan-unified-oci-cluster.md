<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified OCI Cluster Plan

Upstream: [R243](../backlog/R243-deploy-unified-oci-cluster.md),
[deployment architecture](../design/deploy/design-crowdb-deploy.md),
[OCI runtime design](../design/deploy/design-crowdb-oci-image.md).

Goal: direct runtime startup, CLI startup and Docker-only Pixi shortcuts use
one editable, recoverable node lifecycle and the same cluster readiness contract.

Status: startup policy, shared metadata ownership and local service placement are
implemented. Diagnostic container startup has started; formal artifact and protocol
acceptance remain outstanding. Preserve existing worktree changes,
including the Pixi/CI/container-guide edits that predate this refinement.
C1–C8 and A01–A31 below refer to the upstream contracts and acceptance cases.
This plan is temporary and is removed after requirement completion.

## Scope and constraints

- Default local startup is one node; three-node startup explicitly requests three
  voters. Both use ordinary authenticated admission, bootstrap and node setup.
  Initial voter count never becomes an automatic membership reconciler.
- Keep Docker and containerd adapters. Local Docker bridge and Linux host
  networking are separate network choices; the simple Pixi scripts use Docker.
  Multi-host acceptance starts each host explicitly. One CLI invocation targets
  one runtime endpoint; remote multi-host scheduling is outside scope.
- No compatibility aliases, old-state conversion, new Kubernetes operator,
  generalized R193 failure budgets or RDMA tuning/hardware certification.
  Unsupported populated roots fail closed and retain all data. RDMA continues
  through the host-network path with existing device/transport behavior.
- Node setup owns application effects; host helpers own runtime resources.
  Status/check are observational; benchmarks attach to an existing target.
  A host record's ownership token is not a cluster UUID or membership authority.
- Never infer a physical host from a container UUID. Local bridge nodes use one
  conservative shared failure domain with unverified physical identity unless
  explicitly supplied; production requires stable physical-host input.
- New locks require the repository's prior contention/ordering review with the
  user. Reuse existing durable fencing and CAS operations; do not add a lock to
  serialize distributed preparation. Keep unsafe confined to existing scopes.
- Follow `coding` for implementation and `console-ui-e2e` for visible UI or
  Playwright changes. This documentation refinement does not implement code,
  change permanent designs to unimplemented behavior. Requirement implementation
  follows its scoped verification and coherent-task commit cadence.

## Review findings and implementation risks

- **P1: missing metadata-group owner.**
  `container/crowdb-monitor/src/node/runtime/setup/execution/storage.rs::select_group`
  requires an ordinary group; Web's `planned.rs::metadata` selects the first one.
  The CLI's `container_cluster/bootstrap/storage.rs::prepare` creates Group 1
  with fixed nodes 1/2/3. Removing it first strands node setup or binds storage to
  an unrelated group. T08–T10 must establish a durable shared binding first.
- **P1: coordinator selection is not fencing.** Web Auto join fixes three voters,
  requires a rack and calls admission with `ssh_password: None`. Different peer
  views can choose different coordinators; scope names and UUID sorting alone
  cannot exclude competing manifests. T05–T07 must test the accepted protocol,
  not assume eventual discovery repairs conflicts. No automatic reset on conflict.
- **P1: historical setup can report false success.**
  `setup/execution/services.rs::advance` skips Ready/Warning; `observe` yields to
  later operator commands. Preserve that yielding behavior and build readiness
  from current intent/observations in T16–T18. A required stop is unmet readiness,
  never an instruction to restart the service from the old setup journal.
- **P1: storage/service dependency cycle and placement.** File preparation can
  wait for DiskIO while DiskIO needs published disk-group metadata. Existing
  `execution.rs::advance` intentionally advances both storage and services.
  Retain this interleaving. The single profile has four sparse files, the CLI
  cluster has one sparse file per node, and ordinary setup has one preallocated
  file. Resolve the local geometry explicitly in T11; do not simply copy defaults
  or satisfy placement by inventing independent physical hosts.
- **P1: cleanup may cross ownership boundaries.** Current CLI state is S3-specific;
  direct Docker startup has no such record. Separate endpoint attachment from
  runtime ownership in T04/T19/T24. Account for interrupted creation, stopped
  image users, retained volumes, context changes and reference races.
- **P2: listener success is not protocol usability.** Ordinary Dataset is optional;
  legacy public ports differ from CLI cluster ports. `access_config` and the
  remote environment builder must align all three access protocols. Published
  ports and advertised Iceberg/object URIs need outside-bridge validation.
  Native KV/storage RPC additionally needs a reachable transport network.
- **P2: false runtime evidence.** `containerd-cluster.py` creates privileged outer
  Docker hosts, starts native containerd inside them and tests Group-0 recovery.
  Record outer provider separately from node runtime. T34 needs an actual
  Docker-free Linux runner and complete storage/access checks; the existing
  profile alone cannot meet A31. Missing infrastructure is unverified coverage.
- **P2: indirect launch callers.** `ops/s3_bench.rs` calls `s3::start_memory/stop`;
  RPC regression/sanitizer scripts depend on the product RPC launcher. Migrate
  their fixtures before deletion so removing commands does not silently drop
  workload or transport coverage.

## Phase 1: typed contracts and target resolution

Dependency: finish T01–T04 before enabling new automatic side effects.

- [x] **T01 — Seal startup policy**: replace the two-value startup mode record with
  versioned accepted deployment inputs and validate before node side effects.
  Resolve explicit overrides > file > fresh defaults; retained accepted fields
  supply omitted values on restart. Reject unknown versions, policy conflicts,
  corrupt/legacy populated roots and duplicate writable-root use without cleanup.
  Reuse existing exclusive ownership/fencing; inspect its coverage before adding
  a new mechanism. Done when policy precedence and rejection preserve snapshots
  of identity/data (A06, A18, A19). Files:
  `lib/crowdb-protocol/src/mgmt/node.rs`,
  `lib/crowdb-protocol/src/mgmt/node/setup.rs`,
  `container/crowdb-monitor/src/node/startup.rs`, owning crate `tests/`.
- [x] **T02 — Resolve direct node inputs**: parse local/production mode, initial
  voters, common scope, storage choices and required listeners once in monitor
  startup. Entrypoint forwards inputs instead of maintaining a competing mode
  detector. Keep one-node defaults with only image/volume/ports; three-node
  examples add scope/voter policy. Validate production identity/credentials before
  local convenience can run. Done when direct invocation and configured restart
  yield the same accepted policy (A01, A03, A06, A20). Files:
  `container/single-node-container/entrypoint.sh`,
  `container/crowdb-monitor/src/main.rs`,
  `container/crowdb-monitor/src/node/startup.rs`,
  `container/crowdb-monitor/tests/`.
- [x] **T03 — Define status wire types**: add a versioned cluster observation with
  optional UUID before bootstrap, scope, accepted policy, members, operation IDs,
  current authority/storage/service/protocol facts, observation time/age and
  reason codes. Include runtime/image facts as optional observations, with source;
  never synthesize a digest from a tag or expose credential contents. Model setup
  completion separately from current readiness. Done when serialization and C6
  aggregate examples cover every state and unknown fact (A15, A26, A27). Files:
  `lib/crowdb-protocol/src/mgmt/`, `lib/crowdb-protocol/tests/`,
  `lib/crowdb-console-shared/src/ops/cluster.rs`.
- [ ] **T04 — Implement one target resolver**: add a cluster-target module returning
  validated cluster identity, management endpoints and optional runtime ownership.
  Expose global `--cluster` and repeatable `--management-endpoint` inputs; resolve
  CLI overrides before saved configuration. A named binding plus explicit endpoints
  must agree; endpoint-only reads/workloads need no Docker or launch record.
  Remove implicit localhost fallback from explicitly selected targets. Identify
  management versus native RPC/access endpoints and fail reachability/identity
  validation before workload writes. Done when every mismatch has a typed error
  and endpoint attach grants no runtime mutation authority (A14, A28). Files:
  `app/crowdb-cli/src/main.rs`,
  new `app/crowdb-cli/src/commands/cluster/target.rs`,
  `app/crowdb-cli/src/commands/container_cluster/state.rs`,
  `app/crowdb-cli/tests/`.

## Phase 2: authenticated formation and shared preparation

Dependency: T01/T02; T08 precedes storage/service execution. No CLI bootstrap
removal until the complete direct-container path passes A01/A03/A25.

- [ ] **T05 — Scope candidates and bootstrap count**: extend discovery/admission
  compatibility with accepted scope/policy. Generalize hardcoded three-voter
  checks; one-node ignores unrelated candidates, three-node waits for intended
  peers, known ambiguous/overfull fresh candidates fail visibly. Duplicate UUIDs
  and bound foreign clusters never join by name. Extra arrivals after acceptance
  cannot become automatic voters. Done when missing/foreign/extra-peer cases
  preserve the accepted manifest (A03, A04, A19–A21). Files:
  `lib/crowdb-protocol/src/mgmt/node.rs`,
  `app/crowdb-web/src/node/auto_join.rs`,
  `app/crowdb-web/src/node/auto_join/runner.rs`,
  `app/crowdb-web/tests/auto_join_test.rs`.
- [ ] **T06 — Supply local rack and admission credentials**: prepare the local
  rack and existing local test SSH credential source before Auto join. Pass it
  only for compatible local candidates, retain host-key checks and public-key
  exchange, and never copy private keys. Production accepts only explicit trust
  inputs and has no local fallback. Done when direct three-container admission
  succeeds and bad credentials/changed host keys have no weaker retry (A03, A20).
  Files: `app/crowdb-web/src/node/auto_join/runner.rs`,
  `lib/crowdb-console-shared/src/deployment/admission.rs`,
  `container/single-node-container/entrypoint.sh`, admission tests.
- [ ] **T07 — Verify accepted-manifest exclusion and recovery**: exercise
  `PreparedBootstrap::open_with_nodes/execute`, node prepare/accept and monitor
  initialization under concurrent coordinators, asymmetric visibility and lost
  replies. Recover only sealed members/operation; reject conflicting manifests
  without reset. Group-0 exclusion remains intact. Done when each acceptance
  boundary has restart/conflict coverage (A04, A05, A21). Files:
  `lib/crowdb-console-shared/src/deployment/bootstrap.rs`,
  `app/crowdb-web/src/node/initialization.rs`,
  `container/crowdb-monitor/src/node/runtime/initialization/`,
  `container/crowdb-monitor/tests/initialization_test.rs`.
- [x] **T08 — Commit default preparation identity**: introduce a focused shared
  preparation record under existing deployment ownership, conditional on current
  Group-0 authority. Seal ordinary group/replica mapping, disk-group bindings and
  shared credential reference once; scope it by accepted cluster identity and
  operation. Use CAS/re-read on unknown outcomes, not a local coordinator lock.
  Done when simultaneous runners converge on one record and changed input fails
  before mutation (A05, A22). Files:
  `lib/crowdb-console-shared/src/deployment/`,
  `lib/crowdb-console-shared/src/ops/cluster/`, corresponding `tests/`.
- [x] **T09 — Create and recover the ordinary metadata group**: replace CLI-only
  Group 1 creation with an authorized operation driven by T08. Derive replicas
  from admitted membership; persist chosen IDs before effects. Verify matching
  existing group/binding, leader/readability and publication before dependent
  work. Conflicting preexisting objects fail instead of being adopted. Done when
  group creation survives success-with-lost-reply and coordinator loss without
  duplicates (A05, A22). Files:
  `lib/crowdb-console-shared/src/ops/kv_logical/`,
  `lib/crowdb-console-shared/src/ops/cluster/`,
  `container/crowdb-monitor/src/node/runtime/setup/`.
- [x] **T10 — Pin consumers to the accepted metadata binding**: replace first-group
  selection in storage and planned Chunk-KV with T08's exact mapping. Validate
  binding on replay; honor retirement/deletion instead of selecting another
  group or recreating deleted defaults. Done when unrelated groups and explicit
  removal cannot redirect/revive setup (A22, A23). Files:
  `container/crowdb-monitor/src/node/runtime/setup/execution/storage.rs`,
  `app/crowdb-web/src/services/deployment/planned.rs`, setup/planned-service tests.
- [x] **T11 — Fix local storage geometry and placement**: derive and persist the
  smallest supported file-count/geometry and mirror/EC configuration for one and
  three nodes. Start from existing 16-GiB local sparse media and supported test
  policies; verify whether the single profile's four files are needed before
  retaining or reducing them. Extend `AcceptedSetup`'s singular file field only
  if the required geometry needs multiple files. Record concrete counts, capacity,
  unit/zone sizes, reserve, mirror/EC settings and budgets in the policy tests and
  guide before this task is complete. Never silently use production defaults or
  fake host identity. Done when one/three configurations validate and perform
  real storage I/O with truthful failure-domain reporting (A01, A07, A24, A25).
  Files: `lib/crowdb-protocol/src/mgmt/node/setup.rs`,
  `container/crowdb-monitor/src/node/runtime/setup/journal.rs`,
  `container/crowdb-monitor/src/node/runtime/setup/execution/storage.rs`,
  `app/crowdb-web/src/services/deployment/remote.rs`,
  `app/crowdb-web/src/services/deployment/launch.rs`.
- [x] **T12 — Advance disks and services without circular waiting**: preserve
  interleaved storage/service passes, gate on publication versus runtime Online
  separately, and reuse disk preparation receipts. Persist every effect's input
  and operation first. Propagate terminal allocation/inode/geometry failures and
  bound transient retries. Done when DiskIO breaks the waiting-for-DiskIO cycle,
  ENOSPC has no sparse fallback and restarts retain media/operations (A05, A24).
  Files: `container/crowdb-monitor/src/node/runtime/setup/execution.rs`,
  `container/crowdb-monitor/src/node/runtime/setup/execution/storage.rs`,
  `container/crowdb-monitor/src/node/runtime/setup/execution/services.rs`,
  monitor setup/storage tests.
- [x] **T13 — Preserve later operator intent**: keep `publish_planned_start`
  conditional on the accepted service operation; retain retired-operation fences
  and separate journal completion from live health. Test stop/delete/reconfigure
  races against old retries and monitor/Web restart. Done when old setup never
  overwrites a later intent and surfaces the unmet dependency (A05, A23). Files:
  `lib/crowdb-console-shared/src/deployment/services/planned_start.rs`,
  `container/crowdb-monitor/src/node/runtime/setup/`,
  `app/crowdb-web/src/services/deployment/planned.rs`, corresponding `tests/`.
- [x] **T14 — Align access listeners, URIs and credentials**: set default local
  UI/S3/Iceberg/Dataset ports to 9090/9091/9092/9093, explicitly enable Dataset,
  and separate internal bind addresses from public URI overrides. Reuse one
  accepted cluster credential set across members/recreation through existing
  credential distribution. Document retrieval without secret-bearing logs/status.
  Done when host clients follow returned URIs and all protocols retain data and
  credentials (A01, A03, A25, A27). Files:
  `app/crowdb-web/src/services/deployment/launch.rs`,
  `app/crowdb-web/src/services/deployment/remote.rs`,
  `container/single-node-container/profile.toml`,
  `container/crowdb-monitor/src/node/runtime/setup/execution/services.rs`.
- [~] **T15 — Enable normal editable startup**: start management during pending
  setup, select the normal lifecycle for both node counts and remove deployment-
  derived readonly checks after caller migration. Preserve actual authorization,
  consistency checks and operator controls; do not invent UI role management.
  Done when authorized mutations work and invalid ones still fail for their
  actual cause (A01, A02). Files: `container/single-node-container/entrypoint.sh`,
  `container/crowdb-monitor/src/node/runtime.rs`,
  `app/crowdb-web/src/state.rs`, managed/readonly call sites and browser tests.

## Phase 3: observational status and runtime operations

- [ ] **T16 — Collect current dependency evidence**: combine committed topology,
  setup operations, current service intent, live ownership/capacity and each
  protocol readiness probe. Timestamp facts and bound endpoint requests; do not
  treat historical Ready/Warning or a PID as health. Verify cluster identity
  across observations; changing views are unknown/retryable inside the caller's
  bound. Done when service stop, stale health and lost quorum invalidate ready
  without repair (A15, A23, A26, A27). Files:
  `lib/crowdb-console-shared/src/ops/cluster/`,
  `container/crowdb-monitor/src/node/runtime/health.rs`,
  `container/crowdb-monitor/src/node/runtime/observation.rs`, Web node APIs.
- [x] **T17 — Reduce status deterministically**: implement C6 as a pure reducer
  over T03 observations, retaining all reasons and a stable primary cause.
  Distinguish prebootstrap missing voters from established quorum loss, and
  strict all-required-service readiness from continued partial availability.
  Done when order-independent unit cases cover failed/unavailable/ready/degraded/
  initializing/waiting and remote runtime unknown (A15, A26). Files:
  `lib/crowdb-console-shared/src/ops/cluster/`,
  `lib/crowdb-console-shared/tests/`.
- [ ] **T18 — Expose bounded status/check**: add `--json` and `check --wait SECONDS`.
  Status takes one bounded observation; check with zero wait observes once,
  otherwise shares one monotonic deadline across requests, retries and polling.
  Default observation budget is 10 seconds and per-request timeout is capped by
  remaining budget; expose a timeout override. Return versioned JSON on stdout,
  diagnostics on stderr. Exit 0 for a valid status report (even unhealthy),
  check 0 only if ready, check 1 for unmet readiness/deadline, and 2 for invalid
  target/arguments or failure to form a report. Report transport unavailability
  as a typed observation when possible. Never cancel accepted work on timeout.
  Done when slow endpoints cannot multiply the deadline (A15, A27). Files:
  `app/crowdb-cli/src/commands/cluster.rs`,
  `app/crowdb-cli/tests/`, shared status API/tests.
- [ ] **T19 — Extract runtime-owned lifecycle**: move creation/start/stop from
  S3-specific state/runtime into the common cluster domain. Record endpoint/context,
  ownership token, exact resource/mount/image/policy inputs and eventual observed
  cluster UUID. Recover partial creation by verified ownership; conflicting
  resources fail. Local bridge start accepts one/three nodes; host mode starts
  one node on the selected host with the intended cluster policy. Start waits
  for T18; readiness timeout leaves resumable resources. Done when interrupted
  starts and stale contexts are safe (A08–A10, A29). Files:
  `app/crowdb-cli/src/commands/container_cluster.rs`,
  `app/crowdb-cli/src/commands/container_cluster/runtime.rs`,
  `app/crowdb-cli/src/commands/container_cluster/state.rs`,
  `container/oci-image/runtime.py`, `container/oci-image/run-node.py`.
- [ ] **T20 — Preserve host resource validation**: reject fixed-port conflicts,
  ineffective publish arguments, unsupported runtime/network combinations and
  invalid devices/resources; verify applied cgroups/network/mounts afterward.
  Require explicit physical identity and production credentials. No Docker
  dependency in native containerd path or new RDMA configuration. Done when
  failures precede application effects and have no bridge fallback (A08, A09,
  A31). Files: `container/oci-image/runtime.py`,
  `container/oci-image/run-node.py`, `container/oci-image/tests/`.

## Phase 4: independent Docker shortcuts and reference examples

Dependency: T14/T19 establish the node and ownership inputs. Scripts may share
runtime-format utilities, but must not invoke CLI or require its records.

- [ ] **T21 — Separate build/load from run**: keep `oci-build` as the root build
  entry, preserve Docker-free OCI/receipt construction and explicit runtime load.
  Remove implicit build dependencies from all run aliases and migrate internal
  CI staging/verification callers without removing their checks. Done when a
  missing image fails with an actionable build/load command and run executes no
  cargo/compiler/download/build (A10, A11, A17). Files: `pixi.toml`,
  `container/oci-image/image-build.sh`, `container/oci-image/transfer.py`,
  `container/single-node-container/build.sh`, `.github/workflows/`.
- [ ] **T22 — Implement Docker run one/three**: expose `oci-run`, `oci-run-1c`,
  `oci-run-3c` with `--cluster`, a default local name and explicit published-port
  overrides. Use independent volumes and a dedicated bridge per three-node
  cluster; publish distinct member UIs. Persist host-owned inputs/token before
  resource effects and reconcile partial starts. Two names must use distinct
  ports; reject collisions or require overrides, never silently change retained
  endpoints. Done when no CLI binary/record exists and starts still work (A07,
  A10, A11, A29). Files: `container/single-node-container/dev-container.sh` or
  replacement under `container/oci-image/`, `pixi.toml`, runtime-script tests.
- [ ] **T23 — Implement scoped stop**: add `oci-stop --cluster NAME` and explicit
  `--all`; verify ownership/endpoint before stopping. Retain containers, volumes,
  mappings and accepted topology. Treat already stopped/missing resources as
  idempotent but propagate daemon/permission errors. Done when stopping one
  scope never affects another and subsequent run resumes (A07, A11, A29).
  Files: host Docker script, `pixi.toml`, `container/oci-image/tests/`.
- [ ] **T24 — Implement conservative clean**: add scoped/explicit-all clean and
  explicit `--delete-data`; no CLI container-clean command. Preserve data and
  reuse metadata by default. Validate exact owned paths/volumes, reject symlink
  escape, recheck network/image references including stopped containers and
  retained bindings, and skip unknown ownership. Handle interruption/resource
  races without force/prune or substring matching. Done when negative ownership
  tests and clean/recreate preserve data/credentials (A07, A11, A16, A30). Files:
  host Docker script, `container/oci-image/tests/`, `pixi.toml`.
  The user explicitly reiterated that bridge networks must be cleaned along with
  containers. On resuming, all 34 prior diagnostic containers were already
  stopped. Saved their inspections and network inventory privately under
  `.crowdb-runtime/artifacts/unified-oci-cleanup/`, verified the exact
  `org.crowdb.test=unified-oci` ownership label again, removed those stopped
  containers without volume deletion, then removed all nine owned empty bridges.
  Docker now has only its standard bridge/host/none networks. Retained data
  volumes, image artifacts and previous private failure logs remain available.
  Future acceptance teardown must capture evidence and remove its owned bridge
  on both success and failure; do not accumulate diagnostic networks again.
- [ ] **T25 — Add executable reference examples**: one direct Docker command for
  one node; network creation plus three independent commands for three nodes;
  matching Compose examples. Use identical policies, ports, volumes and credential
  retrieval, no init sidecar/peer list/order dependency. Explain plain Docker
  endpoint attach versus owned helper cleanup; arbitrary unlabelled containers
  are not auto-adopted. Done when tests execute the actual examples and compare
  UUID/member/service results (A01, A03, A10, A17, A25). Files:
  `container/oci-image/README.md`, `container/single-node-container/README.md`,
  new Compose files beside these guides, deployment fixtures.

## Phase 5: CLI tree and workload migration

Dependency: T04/T18/T19 and the direct path pass before deleting old executors.

- [ ] **T26 — Promote cluster lifecycle**: expose `cluster start/stop/status/check`
  through the common resolver; migrate S3 bucket/object target selection; remove
  `s3 cluster` and its private bootstrap/storage executor. Keep logical store,
  group, replica and working storage maintenance operations. Done when common
  lifecycle and logical operations address direct/helper clusters (A10, A12,
  A14). Files: `app/crowdb-cli/src/main.rs`,
  `app/crowdb-cli/src/commands.rs`, `app/crowdb-cli/src/commands/cluster.rs`,
  `app/crowdb-cli/src/commands/s3.rs`,
  `app/crowdb-cli/src/commands/container_cluster/`.
- [ ] **T27 — Remove benchmark deployment effects**: migrate KV/DiskDB/ChunkDB/
  ChunkIO/S3 workloads to T04. `bench kv prepare` writes declared data only;
  remove `s3_bench::run`'s `start_memory/stop`, require selected access endpoint
  and credentials, and preserve workload measurement/client workers. Validate
  missing target and changed UUID before writes. Done when inventory/intent
  snapshots are unchanged and unavailable targets never start/stop/reset (A13,
  A14, A28). Files: `app/crowdb-cli/src/commands/bench/`,
  `lib/crowdb-console-shared/src/ops/s3_bench.rs`,
  `lib/crowdb-console-shared/src/ops/s3.rs`, benchmark tests.
- [ ] **T28 — Move RPC lifecycle to test tooling**: migrate reusable measurement
  code and explicit echo server/client fixtures before deleting public `bench rpc`
  and `local-deploy -t rpc`. Keep regression/sanitizer result semantics and normal
  application transport workers. Done when RPC tooling owns start/stop and runs
  without product deployment commands (A12–A14). Files:
  `app/crowdb-cli/src/commands/bench/rpc.rs`, `lib/crowdb-rpc/`,
  `tools/benchmark/bench-rpc-regression.sh`,
  `tools/cpp-checks/sanitize-regression.sh`.
- [ ] **T29 — Migrate indirect callers and remove dead public paths**: enumerate
  the full clap tree and follow dispatch/shared-library callers. Remove `launch`,
  public `--registry`, `cluster local-deploy`, process-oriented destroy/clean,
  `kv server`, storage process controls and not-implemented placeholders. Move
  useful inventory to status; retain actual logical deletion/maintenance and
  monitor-owned supervision. Update profiling/sanitizer/CLI fixtures first;
  retain narrow internal process fixtures when required. Done when removed
  commands are absent and all previous meaningful test purposes have owners
  (A10, A12–A14, A17). Files: `app/crowdb-cli/src/commands/launch.rs`,
  `app/crowdb-cli/src/commands/kv/server.rs`,
  `app/crowdb-cli/src/commands/chunk/`,
  `lib/crowdb-console-shared/src/ops/cluster.rs`,
  `e2e-test/console_cli_fixture.py`, `tools/benchmark/`, `tools/profiling/`,
  `tools/cpp-checks/`, affected Cargo manifests/tests.

## Phase 6: acceptance ownership, CI and permanent documentation

- [ ] **T30 — Register contract and recovery tests**: put Rust cases in owning
  crate `tests/` with existing test-util self dev-dependencies when needed.
  Inject failure before/after durable acceptance, external success/lost reply,
  publication and completion. Use deterministic runtime stubs for ownership/error
  paths; avoid real destructive runtime cleanup in unit tests. Done when A05,
  A06, A16 and A18–A24/A26–A30 map to named tests. Files:
  protocol/shared/monitor/Web/CLI `tests/`, `container/oci-image/tests/`.
- [ ] **T31 — Add direct and Compose acceptance**: execute the actual guide
  commands, one/three startup and multiple orders, prolonged missing peers,
  restart during bootstrap/setup, extra peers and two isolated names. Test all
  three protocols outside the bridge and all UIs for the same confirmed UUID;
  recreate with retained volumes and compare acknowledged payloads. Done when
  A01–A04/A07/A10/A25 run with no host bootstrap helper. Files:
  `e2e-test/profiles/node-containers.py`, `e2e-test/profiles/node-bootstrap.py`,
  `e2e-test/profiles/node-default-setup.py`,
  new `e2e-test/unified_oci_fixture.py`, `e2e-test/tests/`,
  `e2e-test/scenarios.py`, `e2e-test/run.py`.
- [ ] **T32 — Add helper/target/workload acceptance**: remove CLI from Pixi test
  PATH; assert no run-time image rebuild. Exercise scoped stop/clean, retained
  volumes/shared images, direct endpoint attach, mixed UUID rejection, all retained
  benchmark families and retired-command absence. Compare service intents,
  containers/topology and image identity, not volatile PIDs alone. Done when
  A11–A14/A16/A28–A30 have both positive and negative assertions. Files:
  `app/crowdb-cli/tests/`, `e2e-test/console_cli_fixture.py`,
  `e2e-test/tests/`, `container/oci-image/tests/`.
- [ ] **T33 — Add current-readiness failure acceptance**: stop required services,
  lose one then two voters, stale observations and run endpoint-only JSON checks
  with slow/unreachable dependencies. Compare before/after intent to prove checks
  do not repair. Exercise actual authorization and editable UI in both topologies.
  Done when A02/A15/A23/A26/A27 have exact state/reason/exit/deadline assertions.
  Files: `e2e-test/tests/deployment.rs`, deployment fixtures,
  `e2e-test/clients/console-ui/e2e/`, `app/crowdb-cli/tests/`.
- [ ] **T34 — Validate runtime coverage honestly**: extend Docker host and
  containerd host/cluster cases to complete setup, protocol I/O, restart and
  compatible image recreation. Check invalid ports/devices/resources. Record
  image artifact digest versus runtime-local image ID and outer host provider.
  Add a native Linux runner without Docker client/socket/daemon for A31; keep
  disposable Docker-hosted containerd tests as separate coverage. Do not count
  a skipped/unavailable runner as passed. Done when A08/A09/A31 evidence names
  actual runtime, hosts, image, cases and results. Files:
  `e2e-test/profiles/containerd-host.py`,
  `e2e-test/profiles/containerd-cluster.py`, `e2e-test/containerd_profile.py`,
  `e2e-test/tests/image_identity_test.py`, `.github/workflows/`.
- [ ] **T35 — Wire gates and update implemented documentation**: assign every
  A-case a test/CI job; migrate removed task names without losing component,
  artifact or release verification. Update root/OCI designs only after matching
  behavior passes, plus help, direct examples, credentials/ports and cleanup
  guides. Remove obsolete read-only/bare-metal advice; explicitly leave Kubernetes
  orchestration and RDMA tuning outside scope. Done when documented commands
  resolve and coverage is traceable (A17 and all acceptance IDs). Files:
  `tools/ci-checks/check-ci-test-tasks.py`, `pixi.toml`, `.github/workflows/`,
  `doc/design/deploy/design-crowdb-deploy.md`,
  `doc/design/deploy/design-crowdb-oci-image.md`, container guides,
  `e2e-test/README.md`, `e2e-test/clients/ecosystem/README.md`,
  `tools/README.md`, benchmark guides.

## Verification and completion gates

- Unit: T01/T03/T04/T17 policy, target and reducer tests; T20/T23/T24 runtime
  selection/error stubs. Required assertions include A18–A20, A26, A28–A30.
- Integration: T07–T13 crash/replay/concurrent preparation, binding and retirement
  tests cover A05/A06/A21–A24. T18/T26–T29 cover command tree, target validation,
  observation deadlines and attach-only callers (A10/A12/A14/A27/A28).
- E2E: T31 owns direct startup/Compose and protocol persistence
  (A01/A03/A04/A07/A10/A25); T32 owns helpers/workloads/isolation
  (A11–A14/A16/A28–A30); T33 owns editable UI and live degradation
  (A02/A15/A23/A26/A27); T34 owns runtime host coverage (A08/A09/A31).
- Cross-cutting A17 belongs to T25/T29/T35: executable examples, migrated callers
  and documented capability boundaries. Every case needs a recorded test name
  and result before checking its task; a broad existing suite name is insufficient.
- First milestone: direct one-node A01/A18/A24/A25. Second: direct three-node
  A03/A04/A21/A22. Third: status/runtime/helpers and migration. Each milestone
  must pass before removing the old code that provided its capability.
- Inject failures at known boundaries rather than arbitrary sleeps. Record
  node/cluster UUID, manifest/group IDs, service operation IDs, disk identity,
  payload checksums and runtime resource inventory before/after recovery.
  E2E failures print the case/error; enable `CROWDB_E2E_PRINT_SERVICE_ERRORS=1`
  during diagnosis and retain complete service logs/artifacts.

Run affected gates once their cases exist; these existing entrypoints need the
new case registration above. Use the verified OCI artifact required by each
runtime profile. The native Docker-free run is separate evidence, not implied
by the containerd-cluster command:

```bash
pixi run clean-env && pixi run cargo test -p crowdb-protocol
pixi run clean-env && pixi run cargo test -p crowdb-monitor
pixi run clean-env && pixi run cargo test -p crowdb-console-shared
pixi run clean-env && pixi run cargo test -p crowdb-web
pixi run clean-env && pixi run cargo test -p crowdb-cli
pixi run test-oci-image
pixi run test-container-e2e-fixture
pixi run test-container-e2e --layer deployment --concurrency 1
pixi run test-container-e2e --scope console.cli --concurrency 1
pixi run test-container-e2e --profile bridge-admission --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 6144
pixi run test-container-e2e --profile host --concurrency 1 --cpus 4 --memory-mib 4096
pixi run test-container-e2e --profile containerd-host --concurrency 1 --cpus 4 --memory-mib 4096
pixi run test-container-e2e --profile containerd-cluster --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 6144
pixi run check-ci-test-tasks
pixi run rs-fmt-check
pixi run rs-lint
```

Add affected RPC/benchmark gates from their migrated test owners in T28/T29;
retain existing component correctness gates. Any changed C++ needs its existing
format/tree-lint gate. Use 60-second shell timeouts by default; start longer or
hang-prone commands in background sessions and poll, preserving complete output.
Report unavailable infrastructure and confirmed pre-existing failures explicitly;
do not mark unexecuted acceptance passed.

## Execution evidence

- Verified startup-policy resolution and acceptance: 6 protocol policy tests,
  6 existing setup-validation tests, 8 monitor policy tests and 4 monitor CLI
  input tests. Cover precedence, retained values, unknown versions, conflicts,
  foreign/symlink roots, concurrent publication and shared-root socket ownership.
- Full `pixi run cargo test -p crowdb-protocol` and
  `pixi run cargo test -p crowdb-monitor` passed. Server-spawning monitor runs
  were preceded by `pixi run clean-env`.
- Eight pure current-readiness tests passed under
  `pixi run cargo test -p crowdb-console-shared --test cluster_readiness_test`.
  These test state reduction, not yet the live collector or CLI status interface.
- Affected format check and
  `pixi run cargo clippy -p crowdb-protocol -p crowdb-monitor -p crowdb-console-shared --all-targets -- -D warnings`
  passed. Compile/lint findings were fixed without suppressed diagnostics.
- T02 parses monitor flags/environment and bounded TOML policy inputs. The
  entrypoint now accepts the policy before preparing SSH identities and uses the
  accepted mode for credential preflight. Private local admission credentials
  survive restart and are read by the formation worker without logging values.
  Existing single-profile paths remain until their callers migrate under T15/T29.
- Scoped formation is implemented: discovery publishes nonsecret policy hints,
  candidate classification rejects mismatched mode/scope/count, incomplete local
  formation does not admit partial nodes, and only the UUID-ordered coordinator
  starts fresh admission. Production defaults Auto join off; accepted clusters
  do not automatically admit later discoveries. T05/T06/T07 remain unchecked
  pending positive SSH/bootstrap, restart and late-peer acceptance.
- Six new Web formation tests, the existing Auto join/candidate suites and
  monitor candidate/management tests passed. Full `pixi run cargo test -p crowdb-web`
  passed, plus affected fmt and
  `pixi run cargo clippy -p crowdb-protocol -p crowdb-monitor -p crowdb-web --all-targets -- -D warnings`.
- Six monitor CLI input tests and seven Web formation tests passed, including
  policy-only acceptance, retained production mode, and rejected missing,
  exposed, empty or invalid local credentials. Affected fmt and all-target
  monitor/Web clippy gates passed.
- Six Docker entrypoint boundary cases passed using the current entrypoint
  mounted over an existing image and a stub monitor: policy rejection before
  keys, private local default/explicit credentials, and production refusal without
  explicit credentials, plus rejection of an empty retained local credential
  without resetting it to the default. This verifies shell ordering only, not application
  startup or the content of a newly built image.
- T04 has a common bounded target resolver and a read-only
  `/api/cluster/target` endpoint backed by a linearizable Group-0 identity read.
  Eight shared tests, three real-Group-0 Web component tests and four CLI target
  rejection tests passed. Explicit targets now feed KV logical/data operations,
  hardware operations, and KV/DiskDB/ChunkDB/ChunkIO workloads; native transport
  must confirm the same committed UUID before use. Runtime-owned bindings fail
  closed until T19 supplies current runtime endpoint/context validation; S3
  attachment and the final command-tree migration remain under T26/T27.
- T08 now reserves one Group-0 default preparation record with fixed ordinary
  metadata replicas, per-node disk-group bindings and an operation-qualified
  shared credential reference. Seven tests passed against a real in-memory
  Group 0: eight concurrent runners converge, another runner with no local
  configuration recovers the winner, incompatible existing membership/bindings
  are rejected, malformed records remain unchanged, and retirement is durable.
  This record API is not yet wired to automatic resource creation; T09/T10 must
  verify native ownership and exact consumer bindings before enabling it.
  Explicit lost-response fault injection remains in the creation/replay tests.
- T09 native creation now persists a private operation-qualified owner before WAL
  or engine effects. Default retries compare that owner, not just replica ID;
  explicit removal retains a default-operation tombstone and the recovery entry
  point rejects retired or mismatched replicas. It reuses the existing node
  initialization atomic exclusion, without a new lock. Five native-management
  ownership tests and four existing WAL/startup tests passed; affected
  protocol/monitor/shared/Web/server all-target clippy passed. Shared replay
  tests cover unknown creation replies and foreign/retired owner rejection.
  Automatic metadata creation and consumer migration are still pending.
- The shared T09 executor now creates, wires and publishes only its accepted
  native group and performs a linearizable read before declaring it readable.
  Seven focused integration tests passed with real native management/WAL paths:
  one and three ordinary replicas, retained data on replay, native transport
  addresses distinct from HTTP registration, unowned/missing replica rejection,
  explicit deletion retirement, and preservation of later operator membership.
  Three-replica tests use a pinned in-memory Group-0 authority and are not the
  three-voter Group-0/container acceptance proof. Projection validation was
  corrected to match authoritative native endpoints after two initial tests
  exposed empty endpoint assumptions. Shared replay/publication suites and the
  affected all-target clippy gate pass. Wiring this executor into normal node
  setup and migrating storage consumers remain T09/T10 work.
- Full `pixi run cargo test -p crowdb-console-shared` passed after the shared
  executor and publication fixes, together with its fmt and all-target clippy
  gates. Existing local launch, bootstrap replay, membership, S3 and service
  intent tests remain passing.
- Target setup acceptance and journal replay now enforce the target's accepted
  deployment choices. Automatic admission fetches those choices through the
  authenticated control channel and verifies its formation policy, instead of
  copying the coordinator's storage/listener choices. Nine monitor setup tests,
  eight policy tests, seven Web formation tests and six Auto join tests pass;
  affected monitor/Web all-target clippy passes. Positive authenticated
  multi-container admission remains under T06 acceptance.
- Full `pixi run cargo test -p crowdb-web` also passed after target-owned setup
  policy resolution and authenticated formation checks.
- Normal node setup now resumes shared metadata before disks/services. Storage
  and planned Chunk-KV resolve the accepted binding instead of scanning for a
  first ordinary group; retired/missing metadata never redirects to another
  group. Automatic media retain the accepted initial disk-group ID; supplied
  media keep their sealed compatible per-class scopes under that same metadata.
  Eleven monitor setup tests, four supplied-media tests, three catalog authority
  tests and eight shared metadata tests pass, including unrelated-group,
  conflicting reserved binding, retirement and pre-service metadata gating.
  Affected shared/monitor/Web all-target clippy and fmt gates pass. Packaged
  application startup, geometry and protocol acceptance remain T11–T15/T31 work.
- Full `pixi run cargo test -p crowdb-monitor` passed after metadata integration
  and the setup fixture's committed-authority upgrade.
- No formal image, Compose or native-containerd acceptance has run yet. Diagnostic
  application results below are not substituted for artifact/runtime acceptance.
- T11 now has one release-supported `local` service mode: one-node defaults use
  mirror copies 1; three-node defaults use mirror copies 2. Each node accepts one
  sparse 16-GiB file, a 16-GiB zone, 1-MiB allocation units and a 1-GiB backing
  reserve. Mirror-to-EC conversion is disabled. The declared physical-node
  failure budget is 0, including the colocated three-container case; truthful
  shared-host identity is retained. Real file-backed protocol I/O is still required.
- ChunkDB's 38 full-stack tests passed, including distinct one/two-copy allocation
  and rejection of overlapping replacement extents. Nine native configuration
  tests and ten Access configuration tests passed. Full monitor/Web suites,
  affected all-target clippy and CLI/E2E compilation passed before the subsequent
  shared catalog-owner change. The new catalog-owner change has its focused
  seven preparation/eight metadata tests passing; remaining gates are pending.
- An explicitly diagnostic Docker container mounted current debug monitor/Web/KV/
  ChunkDB/Access executables over an existing image. It admitted itself, initialized
  Group 0 and ordinary metadata, created one sparse 16-GiB file, and started
  DiskDB/DiskIO. ChunkDB then failed to parse a partial `[conversion]` table.
  The parser now supplies its normal field defaults, with a focused regression
  test passing. This run is not formal image acceptance and its failed state is
  retained until diagnostic cleanup; it did not pass storage/protocol readiness.
- The accepted preparation now also fixes one initial Chunk-KV owner and partition,
  tree and stream IDs. Planned services on other nodes only recover the shared
  catalog; Web recreation cannot generate competing bootstrap identities.
- Native containerd exists on this host, but the current user cannot access its
  root-owned socket and passwordless sudo is unavailable. Docker-free native
  host acceptance remains unverified; no Docker-hosted substitute is counted.
- A fresh diagnostic node with current executables now reaches historical setup
  Ready and registers its file-backed disk. DiskIO required propagation of the
  monitor's resolved physical-host input into child environment; this is now
  retained when an accepted service intent replaces profile environment.
  S3 signed bucket creation and 128-byte/2-MiB put/get checksum checks passed;
  authenticated Iceberg namespace create/load and Dataset create/open passed.
  Dataset open returned 59 protocol bytes; no JSON representation was assumed.
  No secrets were printed. Restart, three-node copies and advertised host-port
  overrides are still pending. The Access ready response had malformed raw-string
  JSON; ready/unready JSON parsing regression tests now pass with the fix.
- After catalog ownership and physical-host propagation, seven shared preparation,
  eight shared metadata, eleven monitor setup, three shared-catalog, seven Web
  formation, three Web target and six Web service lifecycle tests passed. The
  generated ChunkDB configuration is now tested through the actual native
  `ChunkdbConfig` parser/validator, rather than only generic TOML. Affected
  all-target clippy/fmt and Access configuration/service tests also pass.
- The diagnostic singleton also passed stop/start persistence: node/cluster
  identities, complete private server/client credential files, disk/setup IDs,
  both S3 payload checksums, the Iceberg namespace and Dataset open identity
  remained unchanged. This was the mounted-executable fixture, not the packaged
  image. Its current geometry therefore has file-backed one-node evidence.
- Three independently invoked diagnostic containers discovered/admitted each
  other and established one three-voter Group 0 without a configured node list.
  Their first storage pass exposed a global-ID bug: every accepted node had
  disk-group 1, so the global placement reservation rejected two nodes and the
  initial Chunk-KV checkpoint could not complete. The accepted mapping now uses
  distinct IDs 1/2/3 and checks preexisting global reservations before acceptance.
  Eight preparation/eight metadata tests pass, including foreign/retired global
  ownership and real reservation of each accepted location. Three-node startup
  and two-copy I/O must be rerun with this correction; no failed fixture is reset
  or adopted as a successful cluster.
- With distinct groups all three diagnostic disks registered, exposing the next
  first divergence in native tree checkpointing: only single-copy tree transports
  requested 1-MiB strips; two-copy transports requested a whole large strip.
  `tree_strip_capacity_bytes` now persists the local 1-MiB choice through the
  Rust/C ABI into the native RPC allocator, independently of mirror count.
  Zero keeps the existing production automatic geometry. The large RPC source
  was split into allocation geometry and owned-result helpers without new locks
  or unsafe exceptions. Seven Chunk-KV config tests, 49 chunk-RPC-enabled Rust
  FFI tests, six focused native RPC transport tests and full `pixi run test-cpp`
  passed. Full and affected-file tree-lint exited 0; an introduced test-only
  parentheses warning was fixed and affected-file tree-lint is now clean.
- Dataset payload writers now derive mirror/conversion geometry from accepted
  Access choices while retaining their Dataset chunk purpose and bounded pool.
  Ten Access config tests pass, including one/two-copy Dataset policy assertions.
- A verified OCI artifact built from `f85587ec` was imported under the separate
  `crowdb-node:unified-baseline` tag. Its OCI digest is
  `sha256:59b485f54545bec7c37867fe34f426f5e6f11c994fc9bef55acda0c732d3bdcf`
  and Docker config ID is
  `sha256:54816b0eefebf99c0b3fd2f0f68bb3cbca7147eeab926d9563f45d7b7fc0d375`.
  One fresh direct container reached setup Ready without executable mounts or
  host initialization. S3 128-byte/2-MiB checksums, Iceberg namespace create/load,
  Dataset create/open, and a 2-MiB Dataset upload/snapshot/payload-read checksum
  passed. Its ready response parses as JSON. This artifact predates the global
  group/explicit tree-strip changes; it is not final three-node acceptance.
- The next three-node diagnostic attempt exposed an unrelated-scope blocker
  before admission: the old runner counted every discovered bound cluster after
  the scoped selector had already filtered candidates. That redundant global
  target gate is removed; scoped fresh formation advances despite other
  cluster scopes on the same bridge. Eight formation regressions pass.
- All three independent diagnostic containers now reach setup Ready with one
  cluster UUID and three distinct disk groups. Each member passes S3 128-byte
  and 2-MiB checksums, Iceberg namespace reads, Dataset identity reads, and a
  2-MiB Dataset snapshot/payload checksum. Three-node stop/start preserves node
  and cluster identity, private credentials and every disk/setup/service
  operation. Post-restart cross-node protocol reads pass.
- Placement inspection covers 92 chunks and 4,431 strips across three disks:
  two copies per strip, 1-MiB units, nonoverlapping extents, and truthful false
  disk/node/rack protection. Local unsafe placement permits colocated copies;
  distinct disks per strip were an invalid test assumption, not the contract.
  The local storage guide now records counts, geometry and zero node budget.
  These mounted-executable diagnostics establish geometry and persistence;
  packaged final-image and runtime acceptance remain pending.
- Automatic file preparation now makes a terminal disk failure terminal for
  the node setup before further service submission. Explicit retry consumes
  one durable disk-operation authorization and preserves the original file
  request; supplied and automatic media share bounded retry validation.
  A real reserve-space rejection regression proves no file creation, sparse
  fallback, automatic retry or operation replacement. Its explicit retry fails
  with the same request and requires renewed consent. All 25 affected monitor
  setup/file tests pass, including durable inode/size failures and service restart
  budgets. Monitor all-target clippy and Rust formatting pass.
- A monitor integration regression now resumes an old setup after a later
  Stop/Delete/Restart/new Start configuration, then recreates its setup manager
  and reaccepts the original plan. Every later committed intent remains intact;
  the historical service step retains its old operation and reports an operator
  override warning. All twelve node-setup and four planned-start authority tests
  pass, including competing first publishers and revision-preserving replay.
  Historical completion remains separate from live readiness; current evidence
  collection is still T16. Monitor all-target clippy and formatting pass.
- T14 now seals per-node public S3/Iceberg/Dataset origins, with explicit
  argument/environment > TOML > accepted-value precedence and strict origin
  validation. Fresh local origins use host loopback/default ports; production
  derives internal defaults unless explicitly configured. Console projections
  retain a separate protocol-origin map from current internal listeners, so
  custom published ports do not reroute server-side proxy traffic through NAT.
  Dataset credentials are included in the private client export and explicit
  node credential command resolves its Console-owned file. Eight protocol,
  seven input-precedence and seven credential tests pass. Web protocol/proxy
  and lifecycle suites (18 cases) and affected clippy pass. Full monitor runtime
  tests pass; its doctest dependency lookup failed during concurrent Cargo
  builds, then passed when rerun serially. Full protocol tests and 16 affected
  Console configuration/topology tests pass.
- A fresh diagnostic node with explicit host ports 29090–29093 reaches Ready.
  Host-side S3 128-byte/2-MiB and Dataset 2-MiB snapshot checksums pass. PyIceberg
  creates/appends/scans three Parquet rows, reads metadata through delegated
  FileIO, and confirms its vended S3 endpoint is the configured host Iceberg
  origin. Logical `s3://iceberg/...` object names are expected; reachability
  comes from vended FileIO configuration, not the logical URI's bucket name.
  Stop/start retains identity, operations and complete credentials, and all
  three protocols retain their bytes. Console uses internal node origins and
  reports editable node mode. The packaged final-image gate remains pending.
- T15 removes the monitor `--mode` argument and container single/manual branch.
  Obsolete `CROWDB_STARTUP_MODE` now fails with replacement inputs. The packaged
  software catalog is `container/oci-image/node-profile.toml`, without prescribed
  membership, media, service arguments or rendered configurations; management
  has no Access dependency. The image no longer packages the readonly template.
  Catalog validation accepts wholly absent topology, still rejects partial
  topology, and requires exactly the supported seven program entries. Preview
  orchestration is test-only and the duplicate discovery/single launcher is
  removed. Existing component bootstrap fixtures remain until caller migration.
  Normal bootstrap's fixed KV ports are now explicitly validated; unsupported
  overrides cannot be accepted then silently ignored.
- Three catalog/removed-argument, seven input, initialization/setup/service-policy
  and retained component tests pass (38 cases); eight entrypoint boundary cases
  cover accepted local/production credentials, rejected input ordering, retained
  credential corruption and both obsolete modes. Affected clippy/format pass.
  Existing diagnostic one/three clusters report editable node mode: creating an
  empty rack through one UI backend and removing it through another succeeds.
  A rebuilt packaged catalog image is still required before T15 is complete.
- The rebuilt artifact from `e39a3e0d` has OCI digest
  `sha256:b998cec62985252a949b59e7785a535b62150c6e9604e8b9d17563a51e7c7460`
  and config digest
  `sha256:d93df1c146478b9d95b1af6128485613936f52c7d30c89ec547bcc6515706c25`.
  Fresh named-volume one/three clusters on separate owned bridges pass actual
  S3, Dataset and delegated Iceberg/Parquet I/O through their published ports,
  without executable mounts or host bootstrap. Three independent starts use
  order 3/1/2 and no peer list. New single-node management mutation passes.
- Whole-three-container stop/start in order 1/2/3 exposes a real recovery gap:
  Docker reassigns bridge IPs in start order. Node 1/2/3 now own
  `172.20.0.2/.3/.4`, while their retained transport mapping belongs to the old
  3/1/2 start order. Native Group 0 and Group 1 repeatedly fail to gather quorum;
  authoritative target observations return 502 on all three members. Existing
  authenticated node-update requires live Group 0 and cannot repair this case.
  The failed volumes and credentials remain intact. Do not count this restart as
  passing, pin test IPs, reorder the test to preserve IPs or extend timeouts.
  T15 remains active; repair authenticated retained-member transport recovery
  before declaring the packaged lifecycle complete. Preliminary T16 observation
  code is paused while this first divergence is resolved.
- Independent T16 work now exposes a typed private monitor observation without
  provisioning: it pairs live owned processes, fresh health and exact executed
  operation IDs; reports intentional stop and exhausted restart budgets; and
  omits execution error contents. Missing identity/status is never repaired.
  The reducer also treats a required superseded observation without its own
  replacement as unknown, even when another instance of that kind is healthy.
  Three new monitor observation, two status and nine reducer tests pass, with
  affected all-target clippy/format. Aggregate collection/API/CLI remain pending.
- Node Web observation now exposes bounded current local evidence and concurrent
  non-mutating S3, authenticated Iceberg and authenticated Dataset probes.
  Protocol facts require the current Access operation and current local service
  health; listeners, response bodies and deadlines are bounded. Server secrets
  remain local. The observation route bypasses deployment recovery middleware.
  Four protocol probe tests and four target/observation route tests pass;
  affected all-target clippy and formatting pass. Aggregate collection remains
  pending; this does not complete T16 or resolve the retained-address choice.
- Shared member evidence projection now verifies committed discovery/bootstrap
  identity and formation policy, keeps every required service/protocol distinct,
  pairs observations with current committed operations, and retains a missing
  member rather than substituting another member's health. Explicit current
  Stop/Delete wins over historical success. Duplicate evidence fails closed;
  pending bindings and missing intent/protocol facts remain unmet readiness.
  Seven member projection and nine reducer tests pass; shared all-target clippy
  and formatting pass. Collection of live native authority/storage remains T16.

## Accepted member address recovery

- The user selected existing mDNS/DNS-SD plus identity-bound SSH recovery.
  Stable DNS names and fixed IPs are not prerequisites. See deployment design
  section 6.4 and acceptance A32/A33; the previous addressing decision is resolved.
- T15 remains active until a packaged three-node fixture with accepted member
  trust recovers after all nodes restart in a different order with reassigned
  IPs. Preserve the earlier negative fixture's volumes, UUIDs, data and failure
  evidence. No compatibility migration or replacement trust from advertisements
  is authorized for those older roots.
- [ ] **T15a — Seal member trust**: bind cluster/bootstrap identity, discovery
  UUID, numeric ID and Ed25519 SSH host key in a validated local recovery record.
  Reuse authenticated bootstrap/admission to establish it before replica start;
  retries are immutable, missing/corrupt trust fails closed, and retirement or
  cancellation prevents reuse. Files: shared deployment recovery module,
  bootstrap preparation, monitor private control and peer trust tests.
  Initial bootstrap now seals all selected member pins before issuing StartKv;
  each monitor checks its own host key and every prior authenticated peer pin.
  Startup/health resume wait for sealed trust and reject corrupt pins or a
  changed local host key. Explicit bootstrap cleanup removes the matching trust
  record with its other local bootstrap records. Subsequent admission/revocation
  trust distribution is still pending; do not mark this task complete yet.
- [ ] **T15b — Verify rediscovered endpoints**: use an expected host-key SSH
  connection independent of the address-keyed TOFU cache; compare the peer's
  durable binding and return bounded current endpoint facts. Reject foreign
  identities, changed keys and duplicate live claims. Files: shared SSH transport,
  monitor management/control and recovery loop; authentication integration tests.
  Shared `MemberTrust::observe` and `remote_control_pinned` now verify SSH host
  identity without reading/writing the address-keyed TOFU store or using password
  fallback, then match the returned durable binding. Replies are bounded to
  64 KiB and the complete operation to ten seconds. Discovery-loop and native
  route consumption are now connected as described below; packaged acceptance
  is still pending.
  Follow-up implementation now runs monitor-owned rediscovery independently of
  quorum, verifies `RecoveryIdentity` through the pinned SSH channel and publishes
  bounded boot-scoped endpoint snapshots. Duplicate authenticated incarnations
  remove the route. Native restore and membership rebuild fence unverified
  endpoints before election drivers start; an ArcSwap route view changes only
  connections, retaining canonical membership, voting flags and WAL. Consensus
  requests carry their intended replica and cluster, preventing a reused IP from
  supplying another member's reply. Existing RPC code was split by protocol
  responsibility before extending its dispatch boundary.
  Quorum-backed publication reuses `node_update`, captures committed physical
  replica owners and conditionally rebinds current service listeners/contact
  seeds. Each changed intent gets a new operation through CAS; stop/delete and
  concurrent operator changes retain precedence. Client public URIs remain
  unchanged. Refreshed contact seeds preserve the binding's node/bootstrap
  identity. Packaged-image acceptance and admission trust extension still remain.
  Focused gates pass: 122 group tests, five RPC cases including deliberate live
  address reuse, five monitor trust/duplicate-identity cases, three initialization
  cases, twelve setup cases, one native route case, six reconcile cases, four
  restore cases and nine shared recovery cases. Affected all-target clippy and
  Rust formatting pass. The raw malformed-frame test was updated for the new
  consensus destination fields; its rejection behavior remains covered.
  Verification: complete `crowdb-monitor` suite passes, including the new ordered
  bootstrap and four monitor trust cases. Seven shared recovery cases and the
  existing SSH launch case pass. Shared/protocol/monitor/Web all-target clippy
  with test-util and Rust formatting pass. No packaged image or changed-IP
  recovery acceptance is claimed for these foundation changes.
- [ ] **T15c — Route existing replicas by identity**: propagate only verified
  endpoint replacements to current replica connections without rewriting accepted
  membership/WAL/bootstrap manifests. Handle whole-cluster IP permutations,
  invalidate old connections and recover before Group-0 availability. Reconcile
  published addresses only through normal committed operations after quorum.
  Files: KV transport endpoint resolution, monitor recovery and caller wiring.
- [ ] **T15d — Exercise retained recovery**: focused positive/negative trust and
  routing tests, then a fresh packaged three-node fixture with persisted I/O,
  changed-order whole-cluster restart and no executable mounts. Preserve the
  previously failed fixture; do not silently bootstrap trust from advertisements
  for its existing roots. Record A32/A33 evidence and applicable gates.

- Packaged recovery image `224e6dfd` passed fresh one/three-node S3, Dataset and
  PyIceberg I/O; singleton retained restart also passed. Three-node whole-cluster
  restart changed every IP and failed actual Access readiness at the unchanged
  120-second deadline (attempt 1). Pinned member proofs and native routes recovered,
  but Group-0 linearizable reads remained closed despite a healthy heartbeat lease.
  The first logged divergence was a failed conditional proposal at slot 1009 after
  routes temporarily became unavailable. Conditional proposal failure correctly
  poisoned read readiness, but the leader kept renewing its lease indefinitely.
  Fix the upstream liveness transition: completed/failed recovery or subsequent
  uncertain conditional writes must yield that tenure to a new election, retaining
  the read fence until accepted-log recovery succeeds. A three-node regression
  drops an admitted owner proposal while peers remain healthy and verifies a newer
  recovered tenure can commit again. Also return committed registry addresses in
  the cluster target API rather than immutable bootstrap addresses. Packaged
  changed-IP acceptance remains unverified until the rebuilt image passes.

- Rebuilt packaged fixture uses commit `9e7d4ab2`, OCI digest
  `sha256:71c9edd0d5607315a82cfe7306d7c90d8844ea6c272084f82e73404bcf7d4c7e`,
  source tree `33820a07fe338b97d22e857d855097cfcaaa28de` and runtime SHA-256
  `7894c8e38dc8d4740c60b15262147f495de791ea1f3ffc3f0131d246a9310f0e`.
  Archive: `target/crowdb-member-recovery-b.oci.tar`; Docker image:
  `crowdb-node:member-recovery-b`. Independent start order 3/1/2, fixture
  `crowdb-recovery-b-three-{1,2,3}`, no executable mounts or peer list. The original
  failing fixture and its data volumes remain intact. Fresh/restart verification
  is running; this image identity alone is not acceptance evidence.

- Packaged changed-IP attempt 2 (`9e7d4ab2`) restored linearizable Group-0
  authority, committed every node's new address and passed the 120-second actual
  Access listener check with unchanged UUIDs, credentials, setup/storage inputs
  and trust. It still failed the first retained S3 read with `RemoteDisconnected`.
  Thus neither the listener check nor this image is accepted as complete recovery.
  Diagnosis: `/api/chunk-kv/catalog` generation 6 still routes instance 1 to its
  original `.3` and instance 2 to its original `.2`, while their live observations
  correctly report `.2` and `.4`. Publish address-only catalog successors through
  the existing leader-fenced Group-0 monitor, requiring a fresh instance heartbeat
  with the same hosted partition and epoch, even while recovery is pending;
  this changes routes, not serving authority. Keep immutable old pages and every
  ownership/data/transition input; comparison with fixed transition owners uses
  instance identity rather than an obsolete connection address. Chunk-KV startup
  also reported `tree read failed: Internal`; keep that observation for further
  diagnosis if current catalog routes do not restore real retained I/O.
- Address-only catalog publication passes all 14 domain-monitor tests, six
  catalog persistence tests, four cutover cases (including changed-source address
  and changed-target retry), three split recovery cases and two transfer-storage
  cases. Affected all-target clippy and Rust formatting pass. The initial new
  fixture accidentally reused a helper that always asserts split creation; it
  now creates the intended one-partition control-plane fixture directly.

- Third packaged fixture uses commit `bebf5d14`, OCI digest
  `sha256:6ddca82a345972c6d1118cb5d2af3b86a7af6278f194b1d54cb7b055504e8ef6`,
  source tree `3041c5b36edf2031e9bbb2b8e8438a9f67c4125a` and runtime SHA-256
  `710703ad84d10fb9e59d8e595f2f1868065bfc003f93cebf4da03075bf3f7a78`.
  Archive `target/crowdb-member-recovery-c.oci.tar`, image
  `crowdb-node:member-recovery-c`, fixture `crowdb-recovery-c-three-{1,2,3}`.
  Same independent 3/1/2 start and 1/2/3 whole restart; no executable mounts.
  Verification is running; prior failed fixtures remain intact.

- Packaged attempt 3 (`bebf5d14`) passed fresh S3 128-byte and 2-MiB reads
  on all three members, then timed out creating the first Iceberg namespace at
  the unchanged 15-second request bound, before whole-cluster restart. Preserve
  fixture `crowdb-recovery-c-three-*`; no changed-IP acceptance is claimed.
  The earliest confirmed disruption is automatic replacement of fresh accepted
  DiskDB/DiskIO/ChunkDB/Chunk-KV service operations on nodes 1 and 2 while their
  IPs never changed. Incomplete SSH discovery evidence shrank management seeds,
  which the address recovery publisher mistook for configuration changes and
  restarted those services. Preserve accepted contacts when the own address is
  unchanged and discovery is incomplete; a real own-address change can still
  publish a reachable subset, followed by the full set once verified. A focused
  regression exercises this branch with no reachable authority and verifies no
  contact or service-state writes. All six trust/recovery tests, three ordered
  initialization tests and twelve setup tests pass, together with monitor
  all-target clippy and formatting. A new packaged fixture must also preserve
  original service operation IDs throughout fresh startup before doing I/O.

- Fourth fixture: commit `71be016a`, OCI digest
  `sha256:8d466116b401aa16ff2ec514b6d5bfdf53efbbb4b40d97db88c4a3e1a76eb79c`,
  source tree `459022cadc78434a906701028f196f6015510ff9`, runtime SHA-256
  `8d0020aa1726426eb3d121c1df95f2fdcc4d5bcfdbac94610184e77c56ec1944`.
  Archive `target/crowdb-member-recovery-d.oci.tar`, image
  `crowdb-node:member-recovery-d`, fixture `crowdb-recovery-d-three-{1,2,3}`.
  Same independent start/restart and protocol tests; additionally verify fresh
  accepted operation IDs are never replaced before any IP changes. Running;
  not yet acceptance evidence. All earlier fixtures and volumes are retained.

- Packaged attempt 4 (`71be016a`) failed fresh setup at the unchanged 180-second
  deadline, before any protocol I/O or IP restart. Group-0 lost quorum as every
  monitor published only its own endpoint, blocking all outgoing native routes.
  Direct diagnosis on `crowdb-recovery-d-three-1`: independent root liveness
  answered in 0.141 seconds, while `control --json recovery_identity` timed out
  at five seconds. The runtime serializes IPC dispatch with service readiness
  waits; peer SSH proofs have a ten-second bound. Slow consumer startup therefore
  suppresses all peer proofs, fences consensus routes and makes the startup
  wait permanent. This is a control-path dependency cycle, not a missing DNS
  server or permission to relax trust/expiry checks. Make the existing SSH control
  command's RecoveryIdentity branch read validated private state directly after
  the independent root liveness check, bypassing lifecycle IPC. Preserve the
  wire response and key/bootstrap validation. Regression: an unavailable lifecycle
  socket cannot block a live root's authenticated identity command, and an
  offline root still fails. Preserve every failed fixture. This is the fourth
  root-cause-driven packaged failure; the next packaged run is attempt 5.

- Independent identity-read fix passes seven member trust/recovery tests (including
  the real CLI invocation with unavailable lifecycle socket), three initialization
  tests and twelve setup tests, monitor all-target clippy, formatting and whitespace
  checks. Offline roots, changed SSH keys, missing/corrupt trust and retirement
  remain rejected. No handshake or freshness deadline was increased.

- Fifth fixture: commit `4f18d012`, OCI digest
  `sha256:a51dcb1da0553386653c879dda7c0920f3985af2fac61adbf640c14b93d8f5ec`,
  source tree `849a30e9189ebaf6d64fb05653c21493eafc7431`, runtime SHA-256
  `503a355e789c78ba3a5bbd64541f1d23745d3dc810300f9c85ddcd66bce6fb0a`.
  Archive `target/crowdb-member-recovery-e.oci.tar`, image
  `crowdb-node:member-recovery-e`, fixture `crowdb-recovery-e-three-{1,2,3}`.
  Same accepted-operation identity, fresh S3/Dataset/PyIceberg I/O and retained
  changed-order restart pipeline, with unchanged deadlines. Running; no
  acceptance result is claimed yet. Preserve preceding failed fixtures.

## Consolidated file ownership

- Typed policy/status: `lib/crowdb-protocol/src/mgmt/` and `tests/`.
- Node durability: `container/crowdb-monitor/src/node/`, `src/main.rs`, `tests/`.
- Shared authority/preparation: `lib/crowdb-console-shared/src/deployment/`,
  `src/ops/cluster*`, `src/ops/kv_logical/`, `src/ops/s3*`, `tests/`.
- Web planning/observations/UI policy: `app/crowdb-web/src/node/`,
  `src/services/deployment/`, `src/state.rs`, `tests/` and affected UI call sites.
- CLI/runtime/workloads: `app/crowdb-cli/src/`, `tests/`, affected manifests;
  `lib/crowdb-rpc/` and benchmark/profiling/sanitizer scripts.
- Image/helpers: `container/oci-image/`, `container/single-node-container/`,
  `pixi.toml`, `.github/workflows/`.
- Acceptance and documentation: `e2e-test/`,
  `tools/ci-checks/check-ci-test-tasks.py`, root/OCI deployment designs,
  container/ecosystem/benchmark guides and `tools/README.md`.
- Recovery-tenure and target-address fixes pass all 123 KV group tests, all five
  consensus RPC tests, all five cluster-target tests, affected all-target clippy
  with `test-util`, formatting and whitespace checks.

## Resumed verification

- `87b51516` preserves a live service across bounded readiness observations.
  Sixteen monitor cases, all-target clippy, formatting and whitespace gates pass.
- Packaged three-node fixture `crowdb-recovery-f-three-*` passes fresh S3
  128-byte/2-MiB reads on every member, 2-MiB Dataset publish/read checksums,
  and PyIceberg metadata/Parquet reads of three rows through each public origin.
  Whole stop/start in a different order changes every address:
  node 2 `.3 -> .2`, node 3 `.4 -> .3`, node 1 `.2 -> .4` on `172.18.0.0/16`.
  Actual Access readiness returns within the unchanged 120-second bound;
  node identities, cluster, storage, credentials and SSH trust remain stable.
  All retained S3, Dataset and PyIceberg checks pass on all three members.
  Cluster UUID: `98eca62b-e8c1-455c-aa9e-d9b837205a85`.
  OCI digest: `sha256:f5c451cae5e988f8ebbf8f7be8de0771b5835a32942b9dc49b8277964d330426`;
  source tree `0b2af14423075a48e50fe5fc856273616c907560`, runtime
  `102cf4daba1bc39bc09a2364fe7a69399918c421ed5bbf39377098a3d6e295ee`.
  Private logs, setup/trust/ownership records and receipt are retained in
  `.crowdb-runtime/artifacts/member-recovery-f/`; all three containers and their
  bridge were removed after capture, preserving volumes. Host-network and
  negative identity acceptance remain pending; this does not complete A32/A33.
- The new Docker-only helper's fresh singleton `local-clean-check` passes S3
  and namespace/Dataset-open I/O, and clean removes its container and bridge
  while preserving the private receipt and volume. Recreate changes the bridge
  from `172.19.0.2` to `172.18.0.2` and fails actual readiness at 120 seconds.
  Cluster UUID `b327e64e-42f2-4835-9457-ee15b32eed7d` and volume
  `crowdb-local-clean-check-1-data` remain intact; failed recreation was cleaned
  without deleting data. The node-local Group 0 is a healthy leader, but native
  KV keepalive uses only retained management seeds on the old bridge and cannot
  republish its new service address. Normal endpoint publication then waits on
  that stale registration. This is independent of native consensus routing.
  Keepalive now also includes its current own management endpoint, preserving
  retained peer seeds and all quorum/identity checks. A real singleton RPC test
  with unreachable retained seeds publishes the new address in 0.12 seconds;
  packaged verification is pending. No timeout or restart budget was increased.
- Docker helper negative tests cover foreign/relabelled and attached bridges,
  changed daemon, foreign named resources, referenced volumes, immutable/private
  receipts and shared/retained image protection. OCI Python tests pass 39 cases
  through their own Pixi environment (the root Python lacks PyYAML).
  The task-graph check passes for 28 packages and 388 container flows after
  replacing removed root wrapper calls with internal build/import commands.
- Packaged `local-clean-g`, cluster
  `e88f28d5-0e54-49bb-b4dc-8f9bf2020620`, passes fresh S3, 2-MiB Dataset and
  PyIceberg three-row I/O through the actual root Pixi run/clean commands.
  Default clean removes its owned container and bridge while retaining data;
  reserving the old subnet forces recreate from `172.18.0.2` to `172.19.0.2`.
  The keepalive fix restores binding/service addresses and actual Access
  readiness within 120 seconds, and stable identity/credentials/storage checks
  pass. First retained S3 GET still fails with `RemoteDisconnected`.
  Native Chunk-KV rejects activation with
  `committed split does not prove this serving assignment`: the current catalog
  carries the recovered endpoint but the immutable split proof carries the old
  endpoint for the same owner instance. Split/transfer activation, prepared tree
  publication and balance completion now compare stable owner instance IDs;
  all existing epoch, range, artifact, phase and transition fences remain exact.
  Split regression covers both retained and child writers at a changed address;
  transfer activation also runs with a changed target endpoint. Twenty-two
  Chunk-KV and nineteen domain/balance cases plus both all-target clippy and
  formatting gates pass. Packaged validation of this fix is pending.
  OCI digest `sha256:bb020a20fed4e4ae8edacc7afb02b0158435345846b5875a2d176fa843d25b5b`,
  source `428b6cbcf4b201e71fbb5f917556be23df2f2c21`, runtime
  `d59eec9bd31136d9d4a598982aaf4601cf244c8f2235f53b8f0cafc27fc0fdbf`.
  Evidence is private under `.crowdb-runtime/artifacts/local-clean-g/`;
  teardown removed its container and both test bridges, preserving its volume
  and receipt. No networks remain beyond Docker bridge/host/none.

## Previous verification boundary

- The user resumed execution with “完成 R243”. Continue the storage-bootstrap
  investigation and remaining implementation tasks; the five attempts below
  remain historical evidence. The resumed packaged-attempt count starts at zero.
- Resumed diagnosis: DiskDB's final NoSpace diagnostic reports an Offline disk
  with 14,878 contiguous free units, not exhausted capacity. Its write gate
  closes after uncertain persistence. Before that, monitor kills live Chunk-KV
  bootstrap processes at consecutive ten-second readiness boundaries, starting
  new PIDs while storage initialization is in progress. Separate same-operation
  readiness continuation from process restart: preserve PID/attempts, count only
  actual launches, and keep a still-live fifth launch observable. A real-process
  regression crosses the unchanged ten-second probe bound and then becomes ready
  with the same PID; existing failed-launch budget and explicit-intent tests pass.
  This fixes the demonstrated interruption; packaged storage success is pending.

- The packaged automatic-formation / retained-IP recovery pipeline has failed
  after five root-cause-driven attempts. Stop requirement execution here under
  the implementation skill's retry boundary. Do not mark T15 or A32 complete,
  reset test data, increase request/readiness bounds or start a sixth fixture
  without the user's direction. There is no new product-design decision yet;
  the needed direction is whether to continue the storage-bootstrap investigation.
- Committed fixes and focused gates:
  - `9e7d4ab2`: uncertain-leader reelection and current cluster target addresses;
    123 KV group, five consensus RPC and five target API cases pass.
  - `bebf5d14`: address-only Chunk-KV catalog successors and fixed-transition
    owner identity matching; 14 domain-monitor, six catalog, four cutover,
    three split recovery and two transfer-storage cases pass.
  - `71be016a`: incomplete discovery cannot supersede unchanged fresh service
    contacts. Six member recovery, three initialization and twelve setup cases
    pass at that commit.
  - `4f18d012`: live identity reads bypass serialized lifecycle IPC. Seven
    member recovery, three initialization and twelve setup cases pass, including
    a real monitor CLI invocation with an unavailable lifecycle socket.
  - Applicable all-target clippy, Rust formatting and whitespace gates pass.
    The user's original nine dirty files were preserved outside these commits.
- Packaged attempts, all without executable mounts or peer lists:
  - Attempt 1, `224e6dfd`, `crowdb-recovery-three-*`: initial protocol I/O passes;
    whole restart changes all IPs but actual Access readiness fails at 120 seconds.
    Authenticated routes recover; an uncertain conditional proposal permanently
    closes leader reads while heartbeats continue. Fixed by `9e7d4ab2`.
  - Attempt 2, `9e7d4ab2`, `crowdb-recovery-b-three-*`: fresh complete protocol
    I/O passes. Whole restart changes every IP, preserves node/cluster/storage/
    credential/trust identity and restores committed addresses and target API.
    First retained S3 read fails with `RemoteDisconnected`; catalog owner
    endpoints still reference old IPs. Fixed by `bebf5d14`; native tree Internal
    errors also appeared, so retained data acceptance is still unproven.
  - Attempt 3, `bebf5d14`, `crowdb-recovery-c-three-*`: fresh 128-byte and 2-MiB
    S3 reads pass on all members. Initial Iceberg namespace POST times out at
    the original 15 seconds before any restart. Automatic seed shrinking
    supersedes fresh DiskDB/DiskIO/ChunkDB/Chunk-KV service operations despite
    unchanged IPs and disrupts those services. Fixed by `71be016a`.
  - Attempt 4, `71be016a`, `crowdb-recovery-d-three-*`: fresh setup exceeds the
    unchanged 180-second bound. Serialized service readiness waits suppress SSH
    RecoveryIdentity replies; only self proofs remain and all native peer routes
    become blocked. Independent monitor liveness responds in 0.141 seconds but
    lifecycle identity control exceeds a five-second diagnostic bound. Fixed
    by `4f18d012`, retaining all trust and expiry checks.
  - Attempt 5, `4f18d012`, `crowdb-recovery-e-three-*`: all three identity commands
    respond in 121–135 ms during startup and Group-0 target reads remain available.
    Fresh setup fails on node 1 / container 2: Chunk-KV exits with status 1 and
    exhausts its five durable service-start attempts. The pipeline stops before
    fresh protocol I/O and before any whole-cluster restart; no A32 acceptance.
- Exact final error:
  - `failed to bootstrap initial chunk KV partition`
  - `failed to configure chunk stream: internal stream invariant failed: stream
    chunk IO failed: chunk allocation failed: internal error: allocation failed:
    diskdb allocate failed for disk_group 1: diskdb has no space: no space available`
  - Earlier in the same startup: `diskdb server unreachable: no endpoint for
    disk_group 1: no endpoint cached for disk_group 1`.
  - Setup: `service startup exhausted its restart budget; inspect service logs
    and use explicit service controls`.
- Reproduction pipeline (each command is run through Pixi):
  - `python /tmp/crowdb-recovery-e-start.py three`
  - `python /tmp/crowdb-unified-watch.py crowdb-recovery-e-three-1
    crowdb-recovery-e-three-2 crowdb-recovery-e-three-3` — fails as above.
  - Subsequent accepted-operation, S3, Dataset, PyIceberg, changed-order restart
    and retained-I/O steps were not reached and must not be reported as passing.
  - Exact archive and image receipt are recorded above. All five fixture sets,
    their volumes and immutable images remain intact.
- Private durable diagnostic artifacts:
  `.crowdb-runtime/artifacts/member-recovery-e/` contains all final fixture
  service logs, accepted setup/binding, endpoint and replica ownership evidence,
  container/image metadata and copies of every reproduction script. Directory
  mode is 0700 and files are 0600. No server/client credentials were printed.
- Next bounded investigation, if authorized: follow the first unavailable DiskDB
  route into initial Chunk-KV root/stream allocation and its durable restart
  behavior. Determine whether logical capacity, leaked or repeated bootstrap
  reservations, or endpoint publication ordering explains the later NoSpace.
  These are hypotheses, not a confirmed allocator root cause. Preserve existing
  volumes and use the fixed operation identity; do not mask it with a larger
  file, another retry or a longer readiness deadline.

## Docker shortcut verification

- Implemented Docker-only `oci-run`, `oci-run-1c`, `oci-run-3c`, scoped/global
  stop and clean with private immutable launch receipts and exact daemon,
  ownership-label and canonical resource-name checks. Cleanup removes owned
  containers and empty bridges, retains data and restart inputs by default,
  and only removes unreferenced dangling CROWDB images. Explicit `--delete-data`
  removes disposable owned volumes and receipts. No force removal or prune.
- All 41 OCI Python cases pass, including foreign ownership, attached bridge,
  changed daemon/input, copied labels on unknown names, referenced volume and
  stopped-container/retained-image protection. Both Compose configurations
  validate. CI task inventory passes (28 packages, 388 named flows).
- Packaged image H: source revision `7d31e6847976a6ee83c1c989d766177298a66076`,
  OCI `sha256:1549859a566ba50ef76316a9fcb1866550583d4ca3ab87f65bbc569ce608e0b3`,
  local image `sha256:f4fac666dfecf8659baf7b4d4a3277d9bf6048ac80a148783c10b5349e5781a0`.
  Actual `oci-build --staged --load-image` and packaged image smoke pass.
- `local-clean-h`: root helper starts one node; fresh and retained S3 (128 B,
  2 MiB), 2 MiB Dataset and PyIceberg (three rows) pass. Clean removes container
  and bridge while retaining volume/receipt. Recreate changes the observed IP
  from 172.18.0.2 to 172.20.0.2 and preserves node/cluster identity, SSH trust,
  credentials, setup and storage. Actual Access readiness recovers within the
  unchanged 120 seconds. Final explicit disposable-data cleanup also passes.
  UUID `cb643ec6-130a-49e8-a484-6cc8ef580c09`; private evidence:
  `.crowdb-runtime/artifacts/local-clean-h/`.
- `pixi-three-h`: root three-node helper fresh startup, per-node S3 read/write,
  Iceberg namespace and Dataset open pass. Scoped stop retains resources; run
  resumes the same cluster, all three Access readiness checks and retained I/O
  pass. Final cleanup removes owned containers, bridge and disposable volumes.
  UUID `7b850443-ed60-4a9b-afdd-790c2afac341`; private evidence:
  `.crowdb-runtime/artifacts/pixi-three-h/`.
- Compose H: fresh three-node formation, S3, full Dataset and PyIceberg pass.
  Whole stop/up then fails the unchanged 120-second actual Access readiness
  bound on node 3. Logs show the first launch binding an old address, followed
  by rapid Access exits while DiskIO topology has no live owner, exhausting
  the operation launch budget. Investigate upstream recovery dependency gates;
  do not increase the deadline or launch budget. This is resumed failed packaged
  attempt 3. Finally teardown removed all three containers and the Compose
  bridge, retaining failed-state volumes and private logs under
  `.crowdb-runtime/artifacts/compose-check-h-9f4e12/`.
- Remaining: registered E2E migration and Compose retained recovery; common CLI
  lifecycle/status, native containerd host acceptance, admitted-member trust
  extension and negative identity recovery are still unfinished.

- Compose recovery fix: monitor recovery checks the profile dependency DAG against
  committed local intents and live readiness probes before launching dependents.
  DiskIO consumers also require a successful complete topology connection, so
  a local listener cannot spend five Access launches while a remote owner is
  still recovering. Stop/Delete bypass this gate. A real authority/process test
  holds DiskDB readiness unavailable, proves no ChunkDB execution/budget is
  created, then exposes readiness and completes the same accepted operation.
  Both readiness regression cases pass; monitor all-target clippy passes.

- Host runtime launcher migration: removed obsolete startup-mode inputs and
  environment; Docker and containerd now pass the common cluster and initial
  voter policy to the normal node entrypoint. One/three use identical supplied
  media mapping and retained declarations. Containerd namespace derives from
  the cluster (`crowdb-<cluster>`). Host interface existence and occupied fixed
  TCP listeners fail before container launch. All 42 OCI Python cases pass,
  including a real occupied socket and one/three supplied-media preservation.
  Native containerd full preparation/retained protocol acceptance remains pending;
  the installed root-owned socket is inaccessible to this unprivileged account.
