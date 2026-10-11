<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; latest accepted local Deployer split is committed as `90645b38`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains uncommitted Console browser migration drafts. Shared-image CI is committed. S3 node-fault migration and local-state component proofs are accepted.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Formal release image `crowdb-node:dev` is built/imported from `ec070372`, with
  optimized binaries and retained debug information. OCI digest
  `sha256:40b41c9f808a262d78236e5b1acbe685ca7de37e5b695089ea3e0f9c029e5979`;
  config digest
  `sha256:b51eae855a8b8d35aeb65cc1bf30622ee7ae0d04f4d2e7b1a73a10bfb7ff3fe5`.
  Receipt: `target/crowdb.oci.tar.json`. Rebuild only when release inputs change;
  never bypass source/image provenance checks.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 63 pass, `/tmp/crowdb-host-staged-fixture.log`,
  using an export of the staged source. Console browser drafts are excluded.
  Retained S3 component UT pass 5/5; Web/Console-shared/E2E all-target clippy
  and workspace fmt pass.

- Complete Iceberg domain passes 32/32 in 1515.042 seconds on this image,
  `run-9dd41619b7a04bd3b7e0693e3b432b74`; the full retirement grace client
  takes 1205 seconds. Earlier failed evidence remains in
  `run-c955f9edebe2413b8c9469379667d0c8` and `/tmp/crowdb-iceberg-32-final-domain.log`.

## Remaining migrations

- [ ] **CLI S3 bootstrap replay owners**: migrate the two server-starting
  cases in `app/crowdb-cli/tests/s3_cli_test.rs`:
  `interrupted_s3_launch_resumes_confirmed_group_zero_without_topology_file`
  and `interrupted_s3_storage_launch_replays_without_local_topology`.
  Preserve failed-launch markers, relative-root resolution, confirmed Group-0
  replay, retained Diskdb local launch, no local topology/legacy console file,
  completed marker/intent removal and owned deletion. They start real services
  indirectly through CLI; retain unrelated HTTP stubs and argument validation UT.
- [ ] **Console browser migration**: resolve the mode contract below, then
  accept both managed browser flows and audit every spec using
  `app/crowdb-web/ui/e2e/realBackend.config.ts`. Two copied flows under
  `e2e-test/clients/console-ui/e2e/`, `console_fixture.py`, browser configuration
  and fixture tests are unaccepted drafts. Host browser consumes owned container
  endpoints; signaling validates service PID and run ownership. Preserve UI
  assertions and remove native runners only after matching acceptance. Current
  root Console operation inventory is 87 cases; the two draft browser cases are additional.
- [~] **Bridge SSH admission profile**: reconcile
  `container/single-node-container/tests/node-containers.py`, `node-admission.py`,
  `node-bootstrap.py` and `node-recovery.py` with accepted root deployment flows.
  Move remaining distinct public SSH admission/authentication, literal credential,
  stale intent and recovery assertions under the root runner; these already use
  formal server containers. `lib/crowdb-console-shared/tests/launch_ssh_test.rs`
  is a fake SSH peer plus sleep child and remains a component UT. Original
  three-node browser selection failed twice: first the rack POST remained
  pending, then the initial `/api/mode` request remained pending. Traces:
  `app/crowdb-web/ui/e2e/test-results/crowdb-node-test-879d890b` and
  `crowdb-node-test-961c2cc9`; logs `/tmp/crowdb-bridge-admission-before-relocation.log`
  and `/tmp/crowdb-bridge-admission-serial-check.log`. One-node probes before
  and after interrupted-preparation cleanup pass rack/node/server requests in
  milliseconds. Browser first divergence remains unresolved; do not classify
  these pending requests as a confirmed readonly-policy failure.
- [ ] **Containerd profiles**: root explicit profiles must own
  `container/oci-image/tests/containerd-host.py` / `containerd-cluster.py`.
  Host-network acceptance is migrated to `e2e-test/profiles/host-node.py` and
  `--profile host --concurrency 1`, with the old owner removed after acceptance.
  Same-image resource rejection, CPU/memory limits, SSH admission, identity
  recovery and cleanup pass; private bind-mounted node data stays outside
  uploaded artifacts. Root profile reserves 2.5 CPU / 1536 MiB and pins the
  inspected release image and source. Docker preview/release delegates use it.
  Acceptance: `run-9869315124ca45cb9ba3e3b3b245be90`, 6.740 seconds; explicit interruption
  also fails visibly, reaps the owned node and retains report/data
  (`/tmp/crowdb-host-interrupt-check`). Ownership guards reject changed image
  or data mounts before teardown.
  Preserve resource rejection, actual CPU/memory limits, real SSH admission,
  persistent cluster/credential identity and cleanup. Host fixed ports require
  exclusive scheduling; nested privileged Linux hosts and a containerd socket
  are explicit prerequisites. Ordinary Docker bridge results do not prove them.
- [ ] **External engine profile**: classify/move
  `container/single-node-container/tests/ecosystem/` as an explicit manual child
  under `e2e-test/clients/`; Python, DuckDB, Trino, Spark and Flink consume one
  formal server image with host clients and locked engine dependencies. Keep
  external toolchain prerequisites, restart/exact-data assertions and credential
  redaction. Missing prerequisites fail an explicitly requested gate.
- [ ] **Artifact policy boundary**: keep OCI artifact/build/runtime sandbox and
  release-policy component checks with `container/oci-image` / packaging tooling;
  they do not own ordinary service flows. Separate server executable validation
  from `image-smoke.sh` into accepted root deployment validation. Audit
  `docker-preview.yml` and `release-container.yml` references after moving
  special profiles; retain release digest/snapshotter/policy proofs.

## Final integration and closure

- [ ] **Launcher and coverage audit**: enumerate remaining production executable
  and in-process service launch sites, including C++/browser/SDK/manual tasks.
  Map every original distinct flow to an accepted E2E or retained component UT.
  Remove duplicate old owners and unused harness helpers only after verification.
  Audit fixed listener ports/dynamic store pools and retire unused test port
  allocation code; preserve actual deployment callers. Verify data/log/crash
  mount mappings and truthful unsupported host-core diagnostics.
- [ ] **Pixi classification**: finish component versus required container versus
  explicit special/manual task classification, canonical selectors and aggregate
  delegates. Check no duplicate execution or remaining implicit native server
  preparation. `test-storage`/`test-access` component cleanup is committed;
  native Console paths remain. Manual TPC is now an explicit root scope with a separate locked host environment. Java FileIO has SDK ownership and
  foreground GC has the native GC delegate; preserve these distinct selections.
- [ ] **Close CI classification**: finish selectors after the remaining native
  owners and managed browser contract are resolved. Shared-image jobs now cover
  all accepted container domains on one source-pinned formal artifact; debug
  info and diagnostics are retained. Console selects its 87 accepted operation
  owners, excluding unaccepted browser drafts. Iceberg executes the complete
  32-case domain once, including Rust SDK; the separate SDK component job needs
  no image. Resource audit fits every declared case: S3 maximum 3.9 CPU /
  7872 MiB in a 4 CPU / 8192 MiB job, Iceberg maximum 4 CPU / 11264 MiB in
  12288 MiB. YAML and 28-package task reachability pass. The isolated component
  SDK gate passes eight tests. Console 87-operation measurement passes in 627.744 seconds at 0.25 CPU/server,
  one effective case slot, 4 CPU / 8192 MiB,
  `run-29a68eb2454c4fe5baf55c3d6cd8b610`; recovery and TERM/KILL owners are included. Private Access deployment passes in
  `run-39606819b51543abb1a03e7509906a66`;
  source CLI/Web early-exit validation remains component-local. Web TERM/KILL
  acceptance passes with two concurrent isolated namespaces in
  `run-0a9ddc754927435bb0be473e41a70d9f`: TERM reaps the real child, KILL leaves
  it alive with an exact process-owner record, owned cleanup removes ephemeral
  data and preserves operator data. All actual-server owners are removed from
  `standalone_startup_test.rs`; failed-publication UT remains. The unchanged
  component clean-env proof retains stale/persistent-owner and claim-pruning
  assertions, complemented by host-controller PID-reuse ownership guards. Low-quota S3 node-two restart revealed missing
  dependency readiness; staged startup barriers now pass all three node faults
  and protected restart at 0.2 CPU/server with a 4 CPU / 8192 MiB budget. Logs:
  `/tmp/crowdb-s3-ci-quota-ready.log` and
  `/tmp/crowdb-s3-protected-ci-quota-ready.log`. Keep the failed evidence in
  `run-13a1737806c349b3b051ee3f5c68c46e`. Deployer local orchestration now uses a pinned Console protocol peer and fake
  sleep children: two cases pass in 0.20 seconds, including actual local PID
  reaping after early HTTP stop acknowledgement, stop <5 seconds, reset <1
  second and three reset-before-reprovision cycles without store/child leakage.
  The empty Web reset proof remains component-local. Packaged KV leader and
  endpoint resolution are covered by `console::server_management::full_store_group_remote_cycle`;
  packaged deployment/restart/stop by `console::web_kv_lifecycle::deploy_restart_and_stop_retains_fixed_ports_and_workspace`,
  and three-container restoration by `console::web_cluster_recovery`.
  The old real-server Deployer owners and silent binary skips are removed.
  Actual remote CI has not run.
- [ ] **Final regression and measurements**: run full Iceberg/Console and required registered layers on the same image,
  then relevant component/fmt/lint gates. Update `doc/working/test.md` using one
  row per domain and actual complete-run counts/wall times/environment/image.
  Keep per-case detail in artifacts; never sum partial batches into a full pass.
  DiskIO on the fixed image passes 8/8 in 20.802 seconds,
  `run-7ca260ddd87c489e949438bc9d7b9d59`; its domain row is updated.
  The simplified document layout is accepted and needs no further redesign.
- [ ] **Deferred benchmark decision**: review
  `tools/benchmark/bench-chunk-kv-regression.sh` only after migration and coverage
  measurements are complete; user explicitly deferred this decision. Keep it.
- [ ] **Close R230**: after inventory, assertion mapping, local gates and required
  environment results are satisfied, remove R230 detail/backlog entry and this
  plan in the final cleanup commit. Requirement commits use coherent verified
  batches and single-line subjects without requirement numbers.

## Blocked

### Managed Console mode contract

- Formal Docker Web uses readonly middleware: every non-read request returns
  403. Original managed browser flow expects physical writes 503 but successful
  KV writes and writable Iceberg/S3 demos. Original host setup also omits required
  discovery configuration. Do not silently change assertions or Docker policy.
- Pending user choice: preserve Docker readonly and map writable behavior to an
  ordinary supported writable Console profile, or restore logical/data writes
  in Docker while preserving physical restrictions. Previous questions have no
  explicit answer. Continue independent tasks; do not ask repeatedly.
- Failed evidence: `run-ac7bbae4baf9418cac72b3c66a54f37a` shows 403 vs 503 and
  the outage flow reaching its original 10-second bound. Original browser
  owners remain. A legacy node-monitor profile may offer a supported writable
  route, but verify its product contract before treating it as the owner.
