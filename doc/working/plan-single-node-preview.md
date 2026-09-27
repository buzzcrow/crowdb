<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Single-Node Preview Plan

Upstream: [R187](../backlog/R187-deployment-single-node-docker-preview.md)

Goal: ship the `linux/amd64` CROWDB Single-Node Preview image with a reusable,
profile-driven monitor, runtime bootstrap, S3/Iceberg/Web access, restart safety,
and verifiable release assets.

## Phase 1 — Deployment runtime foundation

- [x] **Profile and layout model**: add the `crowdb-monitor` workspace crate under
  `container/crowdb-monitor`; define versioned deployment-profile, path, service,
  dependency, probe, restart, public-endpoint, and bootstrap inputs; validate
  cycles, duplicate identities/listeners, path escape, missing dependencies,
  unsupported versions, and profile-owned topology. Keep single-node constants
  out of the reusable graph/supervision modules. Files:
  `Cargo.toml`, `container/crowdb-monitor/Cargo.toml`,
  `container/crowdb-monitor/src/{lib,profile,layout}.rs`,
  `container/crowdb-monitor/tests/profile_test.rs`.
- [x] **Single-node profile**: add the named `crowdb-single-node-preview`
  profile with Group 0/1, four stable 16 GiB file disks, process graph, ports,
  `/opt/crowdb` paths, probes, log bounds, and public preview labels. Install only
  this profile in R187; add no future-profile placeholders. Files:
  `container/single-node-preview/profile.toml`,
  `container/single-node-preview/templates/*.toml`,
  `container/crowdb-monitor/tests/single_node_profile_test.rs`.
- [x] **Manifest state machine**: implement atomic, mode-0600
  `Initializing`/`Ready` manifest persistence, stable generated identities,
  exact-profile/config digests, empty-root classification, interrupted-step
  replay, Ready validation-only restart, and fail-closed handling for unknown or
  conflicting state. Files:
  `container/crowdb-monitor/src/{manifest,bootstrap}.rs`,
  `container/crowdb-monitor/tests/manifest_test.rs`.
- [x] **Secrets and credentials command**: generate and atomically persist the
  S3 master key/access pair and four distinct Iceberg bearer tokens, split
  server/client env files, redact diagnostics, and implement `credentials show
  --format env` without exposing server-only material. Files:
  `container/crowdb-monitor/src/{credentials,command}.rs`,
  `container/crowdb-monitor/tests/credentials_test.rs`. Server master key and
  four bearer tokens, private file persistence, and explicit client-file retrieval
  are done. Group 0-backed S3 issuance and `client.env` persistence are
  invoked from monitor `run` and covered by focused and real-stack tests.

## Phase 2 — Process supervision and health

- [x] **Config rendering**: render all child configs into
  `/opt/crowdb/run/config` from immutable templates and validated profile values;
  pass durable/log paths explicitly and prevent secrets from entering command
  arguments or rendered non-secret configs. Files:
  `container/crowdb-monitor/src/render.rs`,
  `container/single-node-preview/templates/*.toml`,
  `container/crowdb-monitor/tests/render_test.rs`.
- [~] **PID 1 supervisor**: implement child ownership/reaping, dependency-order
  start, reverse-order drain, SIGTERM restart suppression, functional probes,
  readiness aggregation, affected-dependent restart, finite exponential backoff,
  crash-loop exit, non-overlap fencing, and atomic status/PID output. Files:
  `container/crowdb-monitor/src/{main,process,probe,supervisor,status}.rs`,
  `container/crowdb-monitor/tests/supervisor_test.rs`. Bounded probes, atomic
  status snapshots, child ownership/reaping, TERM/KILL escalation, and bounded
  per-child logs are implemented. The single-loop supervisor now starts after
  healthy dependencies, drops readiness on probe failure, restarts affected
  services with finite backoff, handles SIGTERM drain, and tests exit recovery
  and budget exhaustion. Dependent cascade and transient-probe recovery tests
  also pass. A configurable stable-health period now resets the crash-loop
  budget and logs that transition. Remaining: real-bootstrap staging,
  PID 1 acceptance and durable-authority revalidation after child recovery.
  Profile-declared listeners are now fenced after reaping and before replacement;
  a surviving listener fails the monitor instead of admitting overlap.
- [~] **Monitor lifecycle log**: persist important bootstrap, readiness, child
  lifecycle, probe failure, restart, drain, and exhaustion events under durable
  `log/monitor/`; retain bounded rotation, redact by using fixed event fields,
  and mirror warning-class transitions to stderr. Event storage and child
  start/stop plus supervisor readiness, probe failure, restart, drain, and
  exhaustion logging are implemented. KV bootstrap step start/completion/failure
  events are connected; disk files, hardware, S3, and Iceberg also log their
  durable steps. DiskIO, chunk, and Web authority probes now log start,
  completion, or failure. Full-process log acceptance remains. Files:
  `container/crowdb-monitor/src/monitor_log.rs`,
  `container/crowdb-monitor/tests/monitor_log_test.rs`.
- [~] **Monitor commands**: expose `run`, `liveness`, `readiness`, and credentials
  subcommands with bounded local operation and stable exit codes for Docker
  health checks. Files: `container/crowdb-monitor/src/{main,command}.rs`,
  `container/crowdb-monitor/tests/command_test.rs`. `validate`, `credentials
  show`, `liveness`, and `readiness` are implemented. `run` now stages KV,
  disks, hardware, DiskDB/DiskIO, chunk services, S3 credentials, Iceberg
  catalog, S3/Iceberg listeners, then Web; failed startup drains children and
  cannot mark readiness. The preflight rejects template digest drift and
  foreign nonempty roots. Full-process/container acceptance still remains.
  Liveness now round-trips a local 0700-directory Unix socket instead of
  treating a fresh status file as proof that bootstrap/event-loop work advances;
  readiness remains the durable status plus child-health gate.

## Phase 3 — Single-node runtime bootstrap

- [x] **KV bootstrap**: start one `crowdb-kv-server` at the fixed root/ports,
  create Group 0 through `/system/init`, create Group 1 through management APIs,
  wait for exact leadership/readiness, and on restart prove both groups' durable
  identities without issuing creation calls. Files:
  `container/crowdb-monitor/src/bootstrap/{kv,http}.rs`,
  `container/single-node-preview/templates/kv.toml`,
  `container/crowdb-monitor/tests/kv_bootstrap_test.rs`. The existing management
  API contract is used for Group 0/1, with exact identity/readiness checks,
  response-loss proof before replay, and validation-only Ready restart. Mock
  HTTP tests pass and monitor events are verified. A real-process test now
  starts KV through `Supervisor`, creates Group 0/1 through the management API,
  shuts down, and validates both after restart. Monitor `run` stages this step.
- [x] **Four-disk storage bootstrap**: create sparse files without truncating
  existing bytes; write rack/node/disk-group/four-disk authority to Group 0;
  render and start DiskDB and DiskIO; validate all stable disk IDs, one-zone 16
  GiB capacities, registration, and direct per-disk readiness. Files:
  `container/crowdb-monitor/src/bootstrap/{hardware,storage}.rs`,
  `container/single-node-preview/templates/{diskdb,diskio}.toml`,
  `container/crowdb-monitor/tests/storage_bootstrap_test.rs`. Sparse-file
  provisioning, restart validation, missing/changed disk rejection, and step
  logging are implemented in `bootstrap/disk_files.rs`. Group 0 rack/node/
  disk-group/four-disk authority, stable DiskDB owner, and Group 1 bind are
  reconciled through `HardwareClient` with preflight conflict rejection and
  real-KV tests in `bootstrap/hardware.rs`. Real DiskDB/DiskIO processes now
  register their owner in Group 0; the monitor waits for the matching registry
  record and fsyncs all four disk IDs through DiskIO. The same authority and
  disk probe pass after a persisted restart. Monitor `run` stages these services.
- [x] **Chunk services bootstrap**: render/start ChunkDB in explicit
  `unsafe_colocated` mode and Chunk-KV with metadata Group 1; establish service
  registry/catalog authority and readiness without enabling split or claiming
  a failure domain. Files:
  `container/crowdb-monitor/src/bootstrap/chunk.rs`,
  `container/single-node-preview/templates/{chunkdb,chunk-kv}.toml`,
  `container/crowdb-monitor/tests/chunk_bootstrap_test.rs`. The named ChunkDB
  template now explicitly selects `unsafe_colocated`. Real-process first boot
  reaches ChunkDB and Chunk-KV readiness with Group 0-issued binding and serving
  grant. The file-backed O_DIRECT read path now uses an aligned bounce buffer
  for byte-range requests; a 238-byte RPC regression test and the full
  KV/DiskDB/DiskIO/ChunkDB/Chunk-KV persisted-restart test pass. Monitor `run`
  stages both services. A bounded Group 0 registry probe now requires exactly
  one live matching instance for each service and confirms the bootstrap
  partition is hosted at its configured owner epoch. The real-process restart
  test rejects mismatched ChunkDB and Chunk-KV identities before restoring
  their rendered configs.
- [x] **S3 credential bootstrap**: after Group 0 readiness, issue one preview
  user through the existing authority, recover a lost issuance response via
  `ensure-user`, and use read-only `lookup-user` on Ready restart. Persist
  `client.env` before advancing the manifest, validate it against Group 0 on
  restart, and reject a conflict. The focused monitor tests and real S3 stack
  cover replay. Files: `app/crowdb-access-server/src/{credentials,main}.rs`,
  `container/crowdb-monitor/src/bootstrap/s3.rs`,
  `container/crowdb-monitor/tests/access_bootstrap_test.rs`. Verified by two
  focused monitor tests and the 17-case real S3 full-stack suite.
- [x] **Iceberg catalog and access listeners**: initialize/activate the
  catalog with durable UUIDv7 request identities, start authenticated S3 and
  Iceberg listeners on container ports 8010/80, default client-visible
  Iceberg URI to host port 80, and validate discovery/health without
  trusted-network bypass. Wire both bootstrap steps into monitor `run` after
  the storage services. Files:
  `container/crowdb-monitor/src/bootstrap/iceberg.rs`,
  `container/crowdb-monitor/src/main.rs`, and matching real-process tests.
  The isolated catalog reconciler now reserves durable UUIDv7 operation IDs
  before management calls, pins the catalog identity in the manifest, proves
  lost responses through read-only inspection, and rejects foreign or changed
  catalog state on restart. Mock-process replay and conflict tests pass.
  The profile now references the Iceberg read token for authenticated
  `/v1/config` probes; the supervisor passes it from runtime-only environment
  during start, periodic health, and restart. Focused probe and restart tests
  pass. Monitor `run` stages catalog and access listeners after storage and S3
  bootstrap. The real KV/DiskDB/DiskIO/ChunkDB/Chunk-KV test now initializes
  and activates the catalog, starts the authenticated Iceberg listener, and
  validates both after persisted restart. Full container acceptance remains in
  Phase 5.

## Phase 4 — Web authority cleanup

- [~] **Split configuration models**: replace mixed `ConsoleConfig` persistence
  with versioned `crowdb-web.toml` process configuration and optional bare-metal
  launch-only `registry.toml`; use distinct `--config`/`--registry` inputs,
  reject registry in Docker mode, reject inline secrets/topology/runtime
  fields, and remove the unreleased old parser/writer/fixtures without migration
  or aliases. Files: `lib/crowdb-console-shared/src/config.rs` and focused child
  modules, `app/crowdb-web/src/main.rs`, affected config tests. Strict versioned
  `WebProcessConfig` and `LaunchRegistry` schemas now parse and validate the
  packaged template, reject unknown topology/secrets and malformed paths, and
  have focused tests. `crowdb-web --config` now loads the strict process schema
  before logging or listener bind, uses its bind/log/UI paths, and never loads
  the legacy mixed file in Docker mode. The unreleased mixed file is
  rejected as a `--config` input. `--registry` is now a distinct, validated
  bare-metal-only input; Docker mode rejects it before listener bind. Both
  process-config modes remain fail-closed on topology APIs while the Group 0
  projection is unfinished. Bare-metal launch-policy use and removal of the old
  default parser/writer remain. Legacy bare-metal registry parse errors now
  abort before listener bind instead of silently falling back to empty state.
- [ ] **Unified hardware-topology authority**: make CLI and bare-metal Web
  rack/node/disk-group/disk reads and mutations use the same Group 0 operation
  path instead of local-first changes followed by ignored sysdata errors.
  Docker Web keeps these mutations disabled. Preserve conflicts and uncertain
  results, and use explicit pre-Group-0 bootstrap inputs only during initial
  cluster creation. Files: `lib/crowdb-console-shared/src/ops/hardware.rs`,
  `app/crowdb-cli/src/commands/cluster/hardware.rs`,
  `app/crowdb-web/src/{state,lifecycle,physical}.rs`, and tests.
  Startup no longer replays local topology when Group 0 is ready or cannot be
  confirmed. Configured Group 0 seeds now initialize the shared KV client, and
  `/api/authority` probes Group 0 with the configured request timeout while
  remaining unavailable until the managed API projection is complete. Remaining:
  replace local-first hardware mutations and delete obsolete persistence.
- [ ] **Unified Group 0 logical operations**: make CLI and Web in Docker and
  bare-metal modes call the same store/group/replica orchestration in
  `lib/crowdb-console-shared/src/ops/kv_logical.rs`. Resolve node management
  endpoints from Group 0 service registration with an explicit node identity;
  do not use `ConsoleConfig.servers`, a Docker profile, or a mode-specific
  fallback for normal operations. Keep initial Group 0 bootstrap separate
  because its authority does not yet exist. Remove local store/group/replica
  record updates and commits from both callers. Preserve fan-out, confirmed
  metadata writes, conflict and response-loss reconciliation, and fail-closed
  behavior when Group 0 is unavailable. Tests exercise the same operation from
  CLI and both Web modes against one Group 0. Files:
  `lib/crowdb-console-shared/src/ops/{context,kv_logical}.rs`,
  `lib/crowdb-kv-client/src/service/**`, `app/crowdb-cli/src/commands/kv/logical.rs`,
  `app/crowdb-web/src/mgmt/{store_ops,group_ops,replica_ops}.rs`, and tests.
  KV registration now carries a distinct optional node identity; the preview,
  local deploy, and SSH deploy set it explicitly. Shared logical operations
  resolve live Group 0 registration instead of local server records, and the
  CLI/Web no longer commit local store/group/replica copies. Node-side delete
  failures now retain Group 0 records. Remaining: ambiguous-response
  reconciliation, multi-node fan-out/rollback tests, and removal of legacy
  bootstrap/deployment-state dependencies. Group 0 initialization now waits
  for exactly one live management registration per node before reporting
  success; KV registration retries promptly until its first success.
  Bare-metal and CLI test fixtures verify that readiness boundary. An
  unreachable replica target now fails deletion while retaining Group 0
  membership, and the leader-removal test verifies this fail-closed behavior.
- [ ] **Unified topology reads and routing**: remove Web KV endpoint and
  group-node fallback to `ConsoleConfig.servers/groups`, and remove monitor-cache
  views that present a local copy as authority. Resolve membership from Group 0
  and live endpoints from the service registry in CLI and Web; fail unavailable
  rather than returning stale local topology. Files: `app/crowdb-web/src/{kv,mgmt,physical}.rs`,
  `lib/crowdb-console-shared/src/ops/context.rs`, `lib/crowdb-kv-client/src/service/**`,
  and read/leader-change tests.
  Web KV data-plane requests now check Group 0 replica membership and live
  KV-server registrations before discovery; they no longer use locally persisted
  server/group entries as authority; a cache leader hint is accepted only for
  a Group 0 member with one live registration and a reported store port. The
  endpoint route now shares that discovery and rejects an unconfirmed leader
  instead of using a local or first-healthy fallback. Bare-metal Web store
  list/detail now project Group 0 records using bulk group/replica scans and
  verified cache leader overlays; cached-only stores fail closed when Group 0
  is unavailable. The bare-metal Web group list now reuses that Group 0
  projection. Group detail, replica read views, and monitor-cache refresh
  still need the same authority conversion.
- [~] **Bootstrap and teardown authority boundary**: keep initial Group 0
  bootstrap intent separate because Group 0 does not exist yet. After creating
  Group 0, transfer and verify every hardware/store/group/replica record, then
  remove local topology; only launch policy remains. Persist a bootstrap
  identity/phase sufficient to resume an interrupted transfer without treating
  arbitrary old local state as authority. On restart, prove identity and
  committed content, write only safely missing records, and fail on conflict;
  never serve local topology once Group 0 exists, including during an outage.
  Make destroy/clean read live Group 0 state, not a `ConsoleConfig` snapshot.
  Test crash before/after each commit and before local deletion. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`,
  `app/crowdb-web/src/mgmt/{cluster_init,topology}.rs`, CLI cluster commands,
  bootstrap-state storage, and failure/restart tests. The preview bootstrap now
  records its profile-owned store, groups, and replicas in Group 0 with full
  preflight, read-after-write reconciliation, and restart validation. The
  legacy bare-metal initial transfer and destroy/clean authority path remain.
- [ ] **Deployment records are not topology**: use `registry.toml` only for
  bare-metal launch policy and monitor state only for Docker process lifecycle.
  CLI/Web service deploy, restart, stop, and DiskDB proxy status must discover
  live endpoints from Group 0 service registration, not persisted `ServerEntry`
  or PID fields. Keep deployment control mode-specific, not a second logical or
  hardware authority. Files: `lib/crowdb-console-shared/src/ops/kv_server.rs`,
  `app/crowdb-cli/src/commands/kv/server.rs`,
  `app/crowdb-web/src/{lifecycle,diskdb,mgmt}.rs`, and tests.
- [ ] **S3 mini-cluster authority audit**: keep its local data-dir record for
  process restart and bootstrap seeds only. Transfer any pre-Group-0 topology
  intent into Group 0 and delete that local topology after verification. Once
  Group 0 exists, route normal hardware/logical queries and operations through
  the same shared Group 0 path without local fallback. Files:
  `lib/crowdb-console-shared/src/ops/s3.rs` and mini-cluster restart tests.
- [x] **Web logical authorization**: pass the existing Iceberg management
  token to Docker Web through its environment and require an exact bearer
  token before any logical write RPC in either Web mode. Keep public status
  reads available and hardware/process writes unavailable in Docker even with
  the token. Add malformed/missing/wrong-token and forbidden-hardware tests;
  do not expose logical write routes until the shared operation passes.
- [ ] **Docker-mode Web UI**: start `crowdb-web` from rendered config, overlay
  monitor PID/restart/crash state on Group 0 service records, disable conflicting
  lifecycle controls, and show source/unavailable state in the UI. Add focused
  Rust, component, and real-backend Playwright assertions. Files:
  `app/crowdb-web/src/**`, `app/crowdb-web/ui/src/**`, and the matching
  `app/crowdb-web/ui/e2e/flows/*` specs. The monitor now also requires
  `/api/authority` to affirm `source=group0` and `available=true` before
  publishing readiness, so the existing web health-only behavior cannot
  falsely mark the preview ready. A managed Web process now reports unavailable
  authority and rejects all `/api/*` topology reads/writes rather than serving
  empty local state or accepting local-only mutations. The Group 0 projection
  and UI overlay are in progress. Docker-mode hardware-topology and process
  mutations remain forbidden; logical store/group/replica operations must not
  be rejected by mode once the Group 0 write path is implemented. Managed
  logical reads and writes now use Group 0 directly; writes require the exact
  management bearer. The preview UI has in-memory token entry and logical
  controls, with a focused browser assertion. Bare-metal hardware/process
  management and full real-backend UI acceptance remain. Managed authority
  now waits for live KV management registration for each hosted store node,
  preventing a ready response before logical writes can resolve endpoints.

## Phase 5 — Image and local acceptance

- [x] **Image assets**: add the digest-pinned Ubuntu 24.04 amd64 multi-stage
  Dockerfile, `.dockerignore`, non-root user, `/opt/crowdb` install layout,
  immutable UI/templates/profile, entrypoint, OCI labels from `VERSION`, exposed
  public ports only, and monitor health checks. Files:
  `container/single-node-preview/{Dockerfile,.dockerignore}` and build support.
  Default invocation maps host `80:80` for Iceberg. Enable binding container
  port 80 for the non-root Iceberg process without running the whole image as root.
  Built `crowdb-single-node-preview:dev` with digest-pinned Ubuntu 24.04,
  release binaries and packaged UI, UID 10001, and file-scoped port-80
  capability. The image smoke verifies the profile, binary loading, labels,
  capability, and default anonymous-volume declaration. Source ports changed to
  S3 8010 and Web 8080. The previous image is retained under its backup tag;
  the rebuilt image passes smoke and the complete container E2E suite.
- [x] **Pixi tasks**: add `build-docker-preview` and `test-docker-preview`, include
  the monitor in workspace build/test coverage, and keep Docker prerequisite
  failures explicit. Files: `pixi.toml`, task-coverage configuration/tests.
  Both tasks run through Pixi; the monitor is assigned to `test-monitor` and
  `test-server`. Test-task coverage and monitor tests pass.
- [x] **Container E2E**: test empty boot, directory/permission contract,
  credentials retrieval, AWS CLI/boto3 Parquet PUT/LIST/HEAD/range-GET/GET,
  pinned PyIceberg operations, web health/status, SIGTERM/recreate persistence,
  interrupted bootstrap, every child crash/hang, crash-loop exhaustion, monitor
  failure, invalid manifests/config, and internal-port isolation. Files:
  `container/single-node-preview/tests/**`. The first full-image empty-volume
  boot exposed DiskIO selecting an invalid io_uring engine when container
  seccomp rejects ring initialization; `UringEngine` now rejects the invalid
  ring so the existing blocking fallback can run. The next boot exposed a
  stale DiskIO registration on interrupted-volume restart; the probe now
  requires a post-start heartbeat before connecting. Storage and Iceberg/S3
  initialization then passed, but the Web Group 0 snapshot found no store,
  group, or replica records. The preview now reconciles these records before
  starting storage dependents; a bounded Web startup check keeps monitor status
  fresh while waiting.
  A repeatable boot, public endpoint, credential, internal-port, default
  anonymous-volume, and persisted-restart test is connected to
  `test-docker-preview`. A fresh-volume run exposed the KV monitor planning a
  split despite disabled balance; the planner now skips automatic split and
  transfer when no balance policy exists. The focused domain-monitor tests and
  E2E check that regression. `pixi run test-docker-preview` now passes all
  implemented boot, restart, and anonymous-volume cases without publishing.
  Boto3 Parquet PUT/LIST/HEAD/range-GET/GET and PyIceberg namespace and table
  create/list/property operations now pass through the container and after a
  persisted restart. Web logical store create/read/delete with bearer auth
  also passes through the image. All eight children passed SIGKILL and SIGSTOP
  recovery checks after DiskIO switched from a TCP-only probe to an RPC Ping.
  Crash-loop exhaustion, corrupt manifest/profile rejection, and PID 1 death
  also passed. A real PID 1 kill after a persisted bootstrap step now proves
  replay on the same volume retains deployment identity, completes all steps,
  and serves S3, Iceberg, and Web. That test exposed restore-mode reuse of
  Store 0's persisted RPC port from the allocation pool; restored ports are
  now claimed before new stores are allocated, with a focused restart test.
  The rebuilt amd64 image passes release policy, image smoke, and full
  container E2E without publishing.
- [ ] **Quick start and operations docs**: document the image name
  `crowdb-single-node-preview`, ports, one mount, credential command, restart
  policy, exact limitations, tested clients, backup boundary, and no production/
  compatibility promise. Files: `README.md`, `doc/user-manual/user-guide.md`,
  rebuilt `doc/user-manual/user-guide.html`, Docker overview assets.

## Phase 6 — CI and publication

- [~] **PR Docker CI**: add an amd64 build/test job with no registry write
  credentials and failure artifacts. Files: `.github/workflows/ci.yml`. An
  isolated `ubuntu-24.04` job now runs Pixi image smoke and container E2E with
  read-only repository permission and prints Docker diagnostics on failure;
  upload structured failure artifacts before closing. The container E2E now
  copies monitor and service logs, excluding secrets, into a failure-artifact
  directory; CI and release verify jobs upload that directory.
- [~] **Release workflow**: add manual-only, release-tag-targeted publication to
  `crowdb/crowdb-iceberg`, gated by a protected GitHub environment, with
  immutable version and `git-<commit>` tags, moving `preview`, no `latest`,
  collision rejection, signature, SBOM, and provenance. The workflow now has
  only `workflow_dispatch`, verifies the release tag and all required gates
  without registry credentials, and publishes/signs only after the
  `preview-release` environment. It fails closed until an administrator enables
  that protected environment with required reviewers, sets
  `PREVIEW_RELEASE_ENABLED=true`, `DOCKERHUB_USERNAME`, and `DOCKERHUB_TOKEN`,
  and enables immutable Docker Hub release tags. No publish has been run.
  Files: `.github/workflows/release-container.yml` and release config.
- [ ] **Release acceptance**: test workflow policy, artifact architecture,
  attached evidence, tag immutability, failed-gate/absent-approval behavior, and
  exact source revision without using real publication credentials in PR tests.
  Files: workflow policy tests under `container/single-node-preview/tests/`.
  Local policy checks now require the full verification suite, no publication
  credentials in its job, a protected publish environment, and the release
  enable gate. Image smoke checks amd64, source-revision and version labels.
  GitHub environment protection and Docker Hub tag immutability still require
  administrator configuration before manual publication can be accepted.

## Phase 7 — Verification and cleanup

- [ ] **Focused gates**: run monitor unit/integration tests, changed console tests,
  targeted UI E2E, image build, S3/PyIceberg/container E2E, Rust fmt/clippy, and
  changed C++ format/tree-lint separately; record confirmed pre-existing failures.
- [ ] **Permanent architecture**: update the matched deployment/config/console
  design and user manual with implemented current behavior; index permanent docs.
- [ ] **Requirement cleanup**: after every acceptance case passes, remove R187,
  its backlog index entry, and this plan in the final coherent commit.

## Consolidated files

- New runtime/profile/image: `container/crowdb-monitor/**`,
  `container/single-node-preview/**`.
- Workspace/build: `Cargo.toml`, `Cargo.lock`, `pixi.toml`, task coverage.
- Web/config: `lib/crowdb-console-shared/**`, `app/crowdb-web/**`.
- CI/release: `.github/workflows/ci.yml`,
  `.github/workflows/release-container.yml`.
- Docs: `README.md`, `doc/user-manual/**`, matched permanent designs,
  `doc/backlog/backlog.md`, R187, and this plan.

## Tests

- Unit: profile/layout/manifest/credentials/render/supervisor/config schema and
  release-policy tests.
- Integration: bootstrap replay, Group 0/1, four disks, service graph, Web
  authority, process restart, filesystem and secret boundaries.
- E2E: built amd64 image, S3 clients, PyIceberg, visible Web UI, persistence,
  signals/faults, readiness, and internal-port isolation.
- Gates: `pixi run build-docker-preview`, `pixi run test-docker-preview`,
  `pixi run -e s3-e2e test-boto3-e2e`,
  `pixi run -e iceberg-e2e test-pyiceberg-e2e`, `pixi run test-console`,
  `pixi run test-console-ui`, `pixi run rs-fmt-check`, `pixi run rs-lint`, and
  changed C++ gates when applicable.

## Resolved Decisions

- Docker mode does not manage hardware topology or monitor-owned processes.
  CLI and Web in both modes use one Group 0-backed logical store/group/replica
  flow. Bare-metal mode may manage deployment and hardware topology.
- Web logical writes reuse the existing Iceberg management bearer token in
  both modes. Public status remains unauthenticated; no new credential is
  generated.
