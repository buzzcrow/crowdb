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
- [~] **Secrets and credentials command**: generate and atomically persist the
  S3 master key/access pair and four distinct Iceberg bearer tokens, split
  server/client env files, redact diagnostics, and implement `credentials show
  --format env` without exposing server-only material. Files:
  `container/crowdb-monitor/src/{credentials,command}.rs`,
  `container/crowdb-monitor/tests/credentials_test.rs`. Server master key and
  four bearer tokens, private file persistence, and explicit client-file retrieval
  are done. The S3 pair must still be issued through the existing Group 0
  credential authority during Phase 3, then persisted to `client.env`.

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
  listener-fencing tests, and PID 1 acceptance.
- [~] **Monitor lifecycle log**: persist important bootstrap, readiness, child
  lifecycle, probe failure, restart, drain, and exhaustion events under durable
  `log/monitor/`; retain bounded rotation, redact by using fixed event fields,
  and mirror warning-class transitions to stderr. Event storage and child
  start/stop plus supervisor readiness, probe failure, restart, drain, and
  exhaustion logging are implemented. KV bootstrap step start/completion/failure
  events are connected; remaining bootstrap domains need the same wiring. Files:
  `container/crowdb-monitor/src/monitor_log.rs`,
  `container/crowdb-monitor/tests/monitor_log_test.rs`.
- [~] **Monitor commands**: expose `run`, `liveness`, `readiness`, and credentials
  subcommands with bounded local operation and stable exit codes for Docker
  health checks. Files: `container/crowdb-monitor/src/{main,command}.rs`,
  `container/crowdb-monitor/tests/command_test.rs`. `validate`, `credentials
  show`, `liveness`, and `readiness` are implemented; `run` awaits supervisor
  and bootstrap wiring.

## Phase 3 — Single-node runtime bootstrap

- [~] **KV bootstrap**: start one `crowdb-kv-server` at the fixed root/ports,
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
  shuts down, and validates both after restart. `run` command staging remains.
- [~] **Four-disk storage bootstrap**: create sparse files without truncating
  existing bytes; write rack/node/disk-group/four-disk authority to Group 0;
  render and start DiskDB and DiskIO; validate all stable disk IDs, one-zone 16
  GiB capacities, registration, and direct per-disk readiness. Files:
  `container/crowdb-monitor/src/bootstrap/{hardware,storage}.rs`,
  `container/single-node-preview/templates/{diskdb,diskio}.toml`,
  `container/crowdb-monitor/tests/storage_bootstrap_test.rs`. Sparse-file
  provisioning, restart validation, missing/changed disk rejection, and step
  logging are implemented in `bootstrap/disk_files.rs`. Group 0 rack/node/
  disk-group/four-disk authority is reconciled through `HardwareClient` with
  preflight conflict rejection and real-KV tests in `bootstrap/hardware.rs`;
  DiskDB/DiskIO staging and direct readiness remain.
- [ ] **Chunk services bootstrap**: render/start ChunkDB in explicit
  `unsafe_colocated` mode and Chunk-KV with metadata Group 1; establish service
  registry/catalog authority and readiness without enabling split or claiming
  a failure domain. Files:
  `container/crowdb-monitor/src/bootstrap/chunk.rs`,
  `container/single-node-preview/templates/{chunkdb,chunk-kv}.toml`,
  `container/crowdb-monitor/tests/chunk_bootstrap_test.rs`.
- [ ] **S3 and Iceberg bootstrap**: issue the preview S3 user after Group 0 is
  ready, initialize/activate the Iceberg catalog with durable request identities,
  start authenticated listeners on 16000/8181, set public URI, and validate
  discovery/health without trusted-network bypass. Files:
  `container/crowdb-monitor/src/bootstrap/{s3,iceberg}.rs`,
  `container/crowdb-monitor/tests/access_bootstrap_test.rs`.

## Phase 4 — Web authority cleanup

- [ ] **Split configuration models**: replace mixed `ConsoleConfig` persistence
  with versioned `crowdb-web.toml` process configuration and optional standalone
  launch-only `registry.toml`; use distinct `--config`/`--registry` inputs,
  reject registry in monitor-managed mode, reject inline secrets/topology/runtime
  fields, and remove the unreleased old parser/writer/fixtures without migration
  or aliases. Files: `lib/crowdb-console-shared/src/config.rs` and focused child
  modules, `app/crowdb-web/src/main.rs`, affected config tests.
- [ ] **Group 0 authority reads/writes**: make web topology reads and mutations
  use Group 0 as the sole authority, remove local-first/best-effort sync and local
  topology restore, preserve response-loss/conflict semantics, and fail visibly
  when Group 0 is unavailable. Files: `app/crowdb-web/src/{state,lifecycle}.rs`,
  `app/crowdb-web/src/mgmt/{topology,*.rs}`, shared operation code and tests.
- [ ] **Monitor-managed Web UI**: start `crowdb-web` from rendered config, overlay
  monitor PID/restart/crash state on Group 0 service records, disable conflicting
  lifecycle controls, and show source/unavailable state in the UI. Add focused
  Rust, component, and real-backend Playwright assertions. Files:
  `app/crowdb-web/src/**`, `app/crowdb-web/ui/src/**`, and the matching
  `app/crowdb-web/ui/e2e/flows/*` specs.

## Phase 5 — Image and local acceptance

- [ ] **Image assets**: add the digest-pinned Ubuntu 24.04 amd64 multi-stage
  Dockerfile, `.dockerignore`, non-root user, `/opt/crowdb` install layout,
  immutable UI/templates/profile, entrypoint, OCI labels from `VERSION`, exposed
  public ports only, and monitor health checks. Files:
  `container/single-node-preview/{Dockerfile,.dockerignore}` and build support.
- [ ] **Pixi tasks**: add `build-docker-preview` and `test-docker-preview`, include
  the monitor in workspace build/test coverage, and keep Docker prerequisite
  failures explicit. Files: `pixi.toml`, task-coverage configuration/tests.
- [ ] **Container E2E**: test empty boot, directory/permission contract,
  credentials retrieval, AWS CLI/boto3 Parquet PUT/LIST/HEAD/range-GET/GET,
  pinned PyIceberg operations, web health/status, SIGTERM/recreate persistence,
  interrupted bootstrap, every child crash/hang, crash-loop exhaustion, monitor
  failure, invalid manifests/config, and internal-port isolation. Files:
  `container/single-node-preview/tests/**`.
- [ ] **Quick start and operations docs**: document the image name
  `crowdb-single-node-preview`, ports, one mount, credential command, restart
  policy, exact limitations, tested clients, backup boundary, and no production/
  compatibility promise. Files: `README.md`, `doc/user-manual/user-guide.md`,
  rebuilt `doc/user-manual/user-guide.html`, Docker overview assets.

## Phase 6 — CI and publication

- [ ] **PR Docker CI**: add an amd64 build/test job with no registry write
  credentials and failure artifacts. Files: `.github/workflows/ci.yml`.
- [ ] **Release workflow**: add Git release-tag/manual-approval publication to
  the public Docker Hub repository with immutable version and `git-<commit>`
  tags, moving `preview`, no `latest`, collision rejection, signature, SBOM, and
  provenance. Files: `.github/workflows/release-container.yml` and release config.
- [ ] **Release acceptance**: test workflow policy, artifact architecture,
  attached evidence, tag immutability, failed-gate/absent-approval behavior, and
  exact source revision without using real publication credentials in PR tests.
  Files: workflow policy tests under `container/single-node-preview/tests/`.

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
