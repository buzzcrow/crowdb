<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; latest accepted RCK archive fix is committed as `372d841f`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains uncommitted Console browser migration drafts. Shared-image CI is committed. S3 node-fault migration and local-state component proofs are accepted.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Current formal release image `crowdb-node:dev` is built/imported from
  `fca2b4fa9f7f51014ef7cff61d940429af393899`, with optimized binaries and
  retained debug information. OCI digest
  `sha256:c79e4be09c63ea8be0de36d06d3796ee3a5254ee3179e808238b3a256362f657`;
  config digest
  `sha256:e102846ba3e0a1171f7c417ae30e77f5665b226bda5bfc210a351fe0a499eedc`.
  Receipt: `target/crowdb.oci.tar.json`. Old image/archives `ec070372` and `d9dd29c4` are retained
  for historical measurements. Rebuild only when release inputs change; never
  bypass source/image provenance checks. Rebuilt/imported receipt matches the committed coalescer fix.
  Exact S3 regression passes all three phases in 154.356 seconds; RCK executes
  all six upstream cases and passes in 51.184 seconds. Artifacts:
  `/tmp/crowdb-coalesce-s3-exact` and `/tmp/crowdb-rck-distribution-exact`.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 78 pass, `/tmp/crowdb-rck-accepted-fixture.log`,
  using an export of the staged source. Console browser drafts are excluded.
  Retained S3 component UT pass 5/5; Web/Console-shared/E2E all-target clippy
  and workspace fmt pass.

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
  Also migrate the server-owning `app/crowdb-web/tests/native_cluster_provisioning_test.rs`
  and its `nativeDiagnostics.config.ts` browser callbacks; they are outside the
  default realBackend selection and remain in `test-console.sh`. Audit
  `71-s3-native.spec.ts` / `nativeS3.config.ts` and `72-managed-native.spec.ts` /
  `managedNative.config.ts` separately; no accepted root replacement is claimed.
## Final integration and closure

- [ ] **Launcher and coverage audit**: enumerate remaining production executable
  and in-process service launch sites, including C++/browser/SDK/manual tasks.
  Map every original distinct flow to an accepted E2E or retained component UT.
  Remove duplicate old owners only after verification. Unused native KV/storage
  harnesses are removed; shared `TestDir`/ownership UTs remain. Explicit production
  local-launch port allocation remains until its deployment callers are audited.
  Audit fixed listener ports/dynamic store pools and retire unused test port
  allocation code; preserve actual deployment callers. Verify data/log/crash
  mount mappings and truthful unsupported host-core diagnostics.
- [ ] **Pixi classification**: finish component versus required container versus
  explicit special/manual task classification, canonical selectors and aggregate
  delegates. Check no duplicate execution or remaining implicit native server
  preparation. `test-storage`/`test-access` component cleanup is committed;
  native Console paths remain. Manual TPC is now an explicit root scope with a separate locked host environment. Java FileIO has SDK ownership and
  foreground GC has the native GC delegate; preserve these distinct selections.
- [ ] **Close CI classification**: finish selectors after native owners and the
  managed browser contract are resolved. Required domain jobs share one verified
  formal artifact; special host/containerd gates use root profiles. Add the
  remaining accepted browser delegates. Bridge admission is already integrated. Check
  each job's CPU/memory reservation and upload only redacted diagnostics.
  Actual remote CI has not run.
- [~] **Final regression and measurements**: run full Iceberg/Console and required registered layers on the same image,
  then relevant component/fmt/lint gates. Update `doc/working/test.md` using one
  row per domain and actual complete-run counts/wall times/environment/image.
  Keep per-case detail in artifacts; never sum partial batches into a full pass.
  Rebuilt formal image source/digests are above. New-image KV passes 50/50 in
  96.977 seconds (`run-7587d4ce83f142c5b6238a8b941d47d8`). Complete S3 and
  Iceberg runs are active in `/tmp/crowdb-coalesce-final-{s3,iceberg}`; each has
  a 16 CPU budget, respectively 16384/32768 MiB, with 0.5 CPU/server.
  Previous image domain counts/timings, including its S3/Iceberg failures,
  remain recorded in `test.md`; do not replace them with partial passing counts.
  Rerun remaining complete registered domains and required special profiles on
  this image after active SDK gates finish. Verify CI quota settings separately;
  remote CI is unmeasured. Workspace clippy passed in 24.78 seconds and fmt passes.
  Do not run global cleanup during an active gate.
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

### CLI S3 local deployment mode

- Original two CLI replay owners generate `deployment.mode = "test_unsafe_placement"`
  via `lifecycle::chunkdb_config` when `protected_test = false`. Formal release
  explicitly rejects that mode in `ChunkdbConfig::validate`; do not bypass it
  with a debug server build, replace generated configuration or weaken safety.
- An owned host CLI adapter experiment ran each server in a separate immutable
  release container, including three KV voters. First attempt failed at the
  standard entrypoint's UID preparation; normal Docker binary entrypoint with
  the host UID then reached the actual mode rejection. Neither is accepted.
  Command: `pixi run python /tmp/crowdb-cli-container-prototype.py`; logs
  `/tmp/crowdb-cli-container-prototype-direct.log`; retained case
  `run-04c5b1f48deb42599edc622f7fc4f946/cli-local/after-kv`. Owned containers and
  adapter processes were cleaned; original CLI tests remain.
- Pending choice asked once: preserve release restrictions and separate local
  launch control component UT from normal container storage recovery, or change
  CLI default local deployment to a supported release mode. Continue independent
  work while waiting; this is a contract choice, not a fifth failing-test retry.
