<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Single-Node Container Plan

Upstream: [R187](../backlog/R187-deployment-single-node-docker-preview.md).
Follow-up: [R188](../backlog/R188-console-group0-authority.md) owns cross-mode
Console/CLI and bare-metal configuration cleanup.

Goal: finish and verify the `linux/amd64` single-node Docker image, then
continue the separate Console authority cleanup. The Docker Hub release job
remains in place; actual publication verification is deferred until the user
completes administrator preparation. Completed implementation is summarized in
the R187 requirement and git history; this plan tracks only work still needed.

## Boundary

- Group 0 stores CROWDB system metadata: hardware identities, binding and
  ownership maps, KV topology, and service registration. It never stores
  container identity, image, mount, PID, restart generation, or launch policy.
- `crowdb-monitor` owns all Docker child processes and their runtime state.
  Docker Web reads Group 0 for cluster state and monitor status for process
  state. Docker Web does not manage hardware topology or child lifecycles.
- Bare-metal launch registry wiring, old `ConsoleConfig` removal, CLI/Web
  cross-mode authority convergence, interrupted bare-metal topology transfer,
  S3 mini-cluster cleanup, and pre-Group-0 nonmember seed propagation belong
  to R188, not the Docker image gate. Do not introduce a Docker-only
  topology or logical-operation implementation to avoid that follow-up.

## Runtime and Web

- [x] **Full-process log and command acceptance**: prove the durable monitor
  event log records bootstrap, readiness, child restart, probe failure, drain,
  and exhaustion without secrets; check rotation. Exercise `run`, `liveness`,
  `readiness`, and `credentials show` in the built container with stable exit
  codes and bounded operation. Files: `container/crowdb-monitor/src/**`,
  `container/crowdb-monitor/tests/**`,
  `container/single-node-container/tests/container-e2e.sh`.
- [ ] **Crash dump location and retention**: document and test how Linux
  host `core_pattern`, Docker's core ulimit, and the non-root container affect
  CROWDB child and PID 1 crashes. Cover a plain relative core-file pattern,
  Ubuntu Apport, systemd-coredump, and Docker Desktop's Linux VM. Choose a
  bounded, private location under the mounted `/opt/crowdb/data` volume where
  the host permits file dumps; otherwise report the host collector location
  and provide explicit setup guidance instead of claiming the volume contains
  a core. Verify one disposable child crash end to end, retention/cleanup,
  secret exposure, and symbolization against the exact binary build. Do not
  change the host-wide `core_pattern` from inside the container. Files:
  `container/single-node-container/{Dockerfile,entrypoint.sh,tests/**}`,
  `container/crowdb-monitor/src/**`,
  `doc/user-manual/docker-single-node-user-guide.md`.
- [x] **Docker Web read model**: finish the managed-mode Web view using Group 0
  for CROWDB topology and live service registration, and monitor status for
  process health/restart state. Show source and unavailable state rather than
  empty or stale local topology. Keep hardware/process mutations disabled and
  authenticated logical writes enabled. Verify the visible UI and API with a
  real container, including Group 0 outage and monitor child recovery. Do not
  migrate bare-metal reads here. Files: `app/crowdb-web/src/{managed,state}.rs`,
  `app/crowdb-web/ui/src/**`, `app/crowdb-web/ui/e2e/**`,
  `container/single-node-container/tests/container-e2e.sh`.

## Release Readiness

- [x] **Host compilation and runtime packaging**: compile with the existing
  Linux amd64 host toolchain and incremental Cargo/CMake outputs. Stage only
  binaries, required libraries, UI and profile files under
  `target/container-runtime`; build Docker from that directory. Reject missing
  runtime dependencies and mismatched source/version metadata. Preserve the
  staged files through the release verify/publish jobs without recompilation.
  Do not introduce a custom base image or an isolated compilation environment.
  Files: container `build.sh`, `collect-libs.sh`, `Dockerfile`, `pixi.toml`,
  `pixi.lock`, `.github/workflows/release-container.yml`.

- [ ] **Fresh image acceptance**: build a new amd64 image from the final
  revision after the image-size task and run image smoke plus the full container
  E2E on an empty volume and a persisted restart. Recheck S3 Parquet
  PUT/LIST/HEAD/range-GET/GET,
  PyIceberg namespace/table operations, Web access, generated credentials,
  anonymous and named volumes, public ports 80/81/8080, internal-port
  isolation, failure/restart behavior, and non-root operation. Preserve the
  existing image tag until the replacement passes. Files:
  `container/single-node-container/{Dockerfile,tests/**}`, `pixi.toml`.
- [x] **Release policy acceptance**: retain the manual Docker Hub release job,
  credential-free CI and local publication-policy checks. Publish only immutable
  version and commit tags. Actual publication verification is deferred by the
  user; do not trigger the workflow or require external setup to continue.
  Files: `.github/workflows/{ci,release-container}.yml`, container
  `tests/release-policy.sh`.
- [x] **Docker single-node guide**: create
  `doc/user-manual/docker-single-node-user-guide.md` for the image name, one
  mounted volume, ports, credential command, restart policy, tested Iceberg and
  S3 clients, limitations, backup boundary, and non-production/no-upgrade
  promise. Link it directly from `README.md` and `doc/doc_index.md`; update the
  HTML generation path and Docker overview assets. Keep the existing
  `user-guide.md` until its bare-metal content is migrated and verified under
  `bare-metal-user-guide.md`; that later migration is not a Docker release gate.
  Files: `README.md`, `doc/doc_index.md`, `doc/user-manual/**`.

## Deferred Footprint Experiments

These are optional, non-blocking experiments after the current image passes
the release gate. Change `pixi.toml` and `pixi.lock` only after choosing a
source and reproducible build for static Folly: the locked conda-forge `folly`
package supplies `libfolly.so`, not `libfolly.a`.

- [ ] **Folly removal comparison**: first evaluate replacing the RPC pending
  map with a lock-free design preserving
  collision handling, cancellation, timeout, and concurrent completion
  semantics. Benchmark contention and latency before changing the hot path;
  never substitute a mutex-protected map solely to reduce image size. Success
  means removing Folly and its dependency closure from the image, not removing
  every Boost or other library used independently elsewhere. Files:
  `lib/crowdb-rpc/include/crowdb-rpc/client/client.h`,
  `lib/crowdb-rpc/src/client/**`, `lib/crowdb-rpc/tests/**`,
  `lib/crowdb-rpc/CMakeLists.txt`, `pixi.toml`, `pixi.lock`.
- [ ] **Static Folly comparison**: if keeping Folly is preferable after the
  pending-map evaluation, build a pinned Folly source revision as a static
  archive through Pixi, link the existing `ConcurrentHashMap` use, and measure
  the complete image and `crowdb-diskio` dependency closure against the
  dynamic build. Require an equal or smaller image, no new unresolved symbols,
  all affected C++ tests, and container E2E before switching the default. Do
  not assume static linking reduces size; retain the dynamic path until
  measured. Files: `pixi.toml`, `pixi.lock`, `lib/crowdb-rpc/CMakeLists.txt`,
  `container/single-node-container/**`.
- [ ] **Rust binary comparison**: preserve usable CROWDB function names and
  source-line diagnostics before pursuing size reductions. First evaluate
  line tables for CROWDB-owned crates only, C++ `-g1`, and separate versus
  bundled symbols; verify a real container crash can be symbolized from the
  exact image revision. Compare the complete image size before deciding; keep
  debug information and core dumps if the size is acceptable. Do not apply
  `strip --strip-unneeded` to CROWDB binaries as a default optimization. Then
  measure `lto="thin"` versus `lto="fat"`,
  `codegen-units=1`, `opt-level="s"` versus `"z"`, and `panic="abort"`
  separately, including request latency, crash diagnostics, and release build
  time. The current release profile uses Cargo defaults:
  `opt-level=3`, no cross-crate LTO, 16 codegen units, `panic="unwind"`, and
  no DWARF. Do not enable dynamic Rust standard-library linking without a
  portability and total-image-size comparison across all eight Rust programs; a
  shared Rust standard library would not automatically share application
  crates. Files: `Cargo.toml`, `pixi.toml`,
  `container/single-node-container/collect-libs.sh`.

Current baseline: packaged `crowdb-iceberg` is 22,179,480 bytes; its `.text`
is about 13 MiB and `.symtab` plus `.strtab` about 4 MiB. Packaged binaries
currently use `strip --strip-debug`, so DWARF line tables are absent while the
normal symbol table remains. Cargo release also defaults to no debug info; the
builder's original Rust artifacts therefore do not provide release line tables.
For `crowdb-monitor`, the no-DWARF release binary was 11,803,832 bytes.
Enabling `line-tables-only` for all dependencies made it 88,084,496 bytes;
compressing those debug sections with zlib made it 27,761,104 bytes. This
large per-binary increase has not been extrapolated to the complete image.
The experiment enabling line tables only for the CROWDB-owned crate was
interrupted at the user's request and remains unmeasured.
On a disposable copy of `crowdb-iceberg`, `strip --strip-unneeded` reduced
22,179,480 to 18,056,944 bytes, but this is **not** an approved image change
because it removes useful CROWDB symbols. The
dynamic Folly library is about 7.4 MB after
stripping; Boost.Regex pulls in about 39 MB of ICU libraries. Removing the
unused Regex export from the Folly link interface eliminated
`libboost_regex.so` from the local `crowdb-diskio` ELF dependency list and
passed the 128 DiskIO C++ tests. The rebuilt host candidate excludes that
closure and saves 39,406,717 bytes against the previous image.

## Final Gates and Cleanup

- [ ] **Focused gates**: run monitor unit/integration tests, affected Docker
  Web/component/Playwright tests, image smoke and container E2E, S3 and
  PyIceberg client acceptance, Rust fmt/clippy, and changed C++ gates
  separately; diagnose failures without weakening assertions or adding
  caller-side retries. Record exact passed commands and confirmed unrelated
  failures in this plan.
- [x] **Permanent Docker architecture**: update only the matched deployment
  and configuration architecture to reflect the shipped container boundary;
  leave cross-mode Console architecture changes to R188. Reconcile the user
  manual with the quick-start task rather than editing it ahead of the user's
  structure decision.
- [ ] **Requirement cleanup**: after every R187 acceptance case is satisfied
  and the local image passes its gates, remove R187 and its backlog index entry and
  this temporary plan in one coherent final cleanup commit. R188 remains open.

## Current Evidence

- Host candidate complete container E2E passed: interrupted initialization,
  first boot, authenticated clients/logical writes, all eight children under
  KILL and STOP, recovered browser view, persisted-volume restart, restart
  exhaustion, lifecycle/secret-free logs, durable identity rejection, corrupt
  manifest/invalid profile rejection, anonymous volume and monitor death.
- Full S3 client acceptance passed (17 cases); full PyIceberg task passed,
  including native storage/listener restart. Rust fmt/lint and release policy
  checks passed for the packaging change. The five Console browser failures
  are being resolved in the Console authority follow-up; R187 is not yet closed.

- Host-compiled candidate image builds successfully and passes image smoke,
  including container-only ports/capabilities and the Regex/ICU exclusion.
  Size is 259,561,209 bytes versus 298,967,926 for the previous image, a
  39,406,717-byte reduction. Packaging identical staged files with a warm Docker
  cache took 0.476 seconds; this excludes compilation and is not a cold-build
  measurement. An incorrect source-revision build argument is rejected.
- Real-container browser acceptance passes: Group 0 outage clears topology
  while monitor process state remains visible, then topology recovers. Full
  container crash/hang/restart acceptance also passed on the host candidate.
- Workspace Rust fmt and lint pass after host packaging changes. Local release
  policy checks pass; actual Docker Hub publication remains deferred.

- The user superseded the isolated Docker compilation flow with host
  incremental compilation followed by runtime-only Docker packaging. No custom
  CROWDB base image is required. `patchelf` is now a locked Linux host packaging
  tool, not a runtime dependency.
- Full Console UI run: 85 component tests pass; browser suite has 51 passing
  and 5 failing tests. One failure reports missing live registration for a
  pre-Group-0 nonmember node. These failures remain under diagnosis.

- Full `pixi run test-console` completed with exit 0 before the nonmember
  discovery fix. Its affected browser regressions now pass; updated complete
  Console gates are tracked in the Console authority plan.
- Candidate image build attempt 1 failed during dependency download, before
  compilation: compiler-rt package transfer ended with TLS unexpected EOF.
  Attempt 2 uses a BuildKit rattler cache so completed package downloads survive
  failed builds. No release workflow was triggered.
- Release policy and shell syntax checks pass with the retained manual job and
  version/commit tags; moving `preview` and `latest` tags are rejected.
- Folly dependency audit: only `ConcurrentHashMap` is used by CROWDB RPC. The
  locked shared Folly requires Boost.ProgramOptions, Context and Filesystem;
  Filesystem also brings Container. These four packaged libraries total about
  0.84 MB. Removing them without rebuilding/replacing Folly breaks its loader
  contract. The much larger Regex/ICU closure is already absent from local
  DiskIO after the link fix; image smoke now explicitly rejects its return.

- User-facing quick start uses only Docker, a version tag, container name
  `crowdb-iceberg`, and the Iceberg port-80 mapping. S3 on 81 is optional;
  the unfinished GUI on 8080 is not published by guide examples. Container
  listener overrides do not change bare-metal defaults. Common options and
  an extended persistent-volume example follow the minimal startup path.
- Monitor log regression gates pass: complete JSON events survive rotation,
  five files per child log channel are retained across PID generations, and
  unrelated files are preserved. Missing files during concurrent rotation are
  tolerated; active child logs are not unlinked. Child stderr mirrors into
  Docker logs under the existing profile policy. Focused monitor-log/process
  tests, workspace fmt and lint passed on 2026-09-27.
- Docker Hub release workflow and local policy checks are retained. The user
  deferred only actual publication verification until their preparation is done.
- Candidate builds and smoke/E2E scripts accept `CROWDB_CONTAINER_IMAGE` so the
  existing dev tag can remain intact during final-image verification.
- Docker Markdown/HTML guide now records volume, ports, credentials, probes,
  client boundaries, restart, logs and host core-collector limitations. Core
  volume retention and source-line symbolization are still unfinished work.

- Checkpoint gates on 2026-09-27 passed: `pixi run rs-fmt-check`,
  `pixi run rs-lint`, `pixi run test-monitor`, `pixi run test-console-shared`,
  `pixi run tree-lint`, `pixi run test-rpc-ct` (71 tests), and
  `pixi run test-diskio-ct` (128 tests). Tree lint exited zero with warnings
  in unchanged C++ sources; no C++ source formatting changed. Release-policy,
  image-smoke and container-e2e scripts passed through Pixi on image
  `sha256:5cc4ba103df8615061d6c00a49c8bb68f49e2ee208a83d187ccfd10eb8153780`
  (298,967,926 bytes). Its source label predates this checkpoint commit;
  final-image acceptance still requires a rebuild from the final revision.

- Recovery authority acceptance passed on the rebuilt image: supervisor
  readiness remains false after a child restart until the persisted manifest,
  Group 0, storage, credentials, catalog, and Web authority are revalidated.
  The container E2E covered all-child crash/hang recovery, persisted-volume
  restart, monitor-death replay, crash-loop exhaustion, and rejection of a
  changed durable credential identity. `pixi run rs-fmt-check`,
  `pixi run rs-lint`, the focused supervisor test, image smoke, and the full
  container E2E passed.
- The existing `crowdb-single-node-preview:dev` image passed release policy,
  image smoke, full container E2E, S3/PyIceberg client operations, persisted
  restart, all-child SIGKILL/SIGSTOP recovery, crash-loop exhaustion, and PID 1
  replay. This is evidence for the previous revision, not a final-image gate.
- Image-size work is complete: the prior image was 484,206,563 bytes and the
  rebuilt `crowdb-iceberg-single-node:dev` is 298,882,134 bytes. Runtime
  binaries and required libraries have debug sections removed; staging the
  capability change in the same layer avoids a 23.5 MB copy-up. Image smoke
  enforces a 325 MB regression ceiling. Release policy, image smoke, and the
  full container E2E passed on the rebuilt image; a final publishable revision
  still needs the release gates.
- Managed Web already reads logical topology from Group 0, requires the
  management bearer for logical writes, and denies Docker hardware/process
  mutations. The complete process-status UI and outage presentation still
  need acceptance.
- PR Docker CI builds/tests without registry credentials and uploads failure
  logs. The manual release job remains; actual registry publication is unverified
  and deferred by user request.

## Files

- Runtime and probes: `container/crowdb-monitor/src/**`,
  `container/crowdb-monitor/tests/**`.
- Docker image and acceptance: `container/single-node-container/**`,
  `pixi.toml`, `.github/workflows/{ci,release-container}.yml`.
- Managed Web: `app/crowdb-web/src/{managed,state}.rs`,
  `app/crowdb-web/ui/src/**`, `app/crowdb-web/ui/e2e/**`.
- Final documentation: `README.md`, `doc/doc_index.md`,
  `doc/user-manual/docker-single-node-user-guide.md`, the HTML generator,
  deployment and configuration architecture, and Docker overview assets.

## Tests

- Unit: `pixi run test-monitor`, focused managed Web tests, and
  `pixi run bash container/single-node-container/tests/release-policy.sh`.
- Integration: `pixi run -e s3-e2e test-boto3-e2e` and
  `pixi run -e iceberg-e2e test-pyiceberg-e2e`.
- E2E: `pixi run build-single-node-container`, `pixi run test-single-node-container`,
  targeted Playwright through `pixi run`, then `pixi run test-console-ui`.
- Packaging: image smoke verifies the 325 MB ceiling in CI.
- Style: `pixi run rs-fmt-check`, `pixi run rs-lint`; if C++ changes,
  `pixi run tree-lint`, changed-format check, and affected C++ tests.

## Open Questions

- **Crash collection and symbols:** the current host routes `core_pattern` to
  Apport. A container-local file directory/ulimit cannot override that policy,
  and changing the host-wide collector is outside container implementation.
  Choose acceptance on a disposable Linux host with file-based core collection,
  or certify and document a host-collector export workflow. Source-line symbol
  distribution also needs a choice: bundle compressed CROWDB line tables and
  adjust the measured image-size ceiling, or publish exact-build debug symbols
  separately while retaining runtime function names. The existing all-dependency
  experiment increased monitor size substantially; neither complete-image option
  has yet been measured. Bounded volume retention and end-to-end source-line
  symbolization remain incomplete, not claimed acceptance.
