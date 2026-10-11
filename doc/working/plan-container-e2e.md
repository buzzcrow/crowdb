<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; latest accepted SSH admission migration is committed as `d766eb39`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains uncommitted Console browser migration drafts. Shared-image CI is committed. S3 node-fault migration and local-state component proofs are accepted.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Current formal release image `crowdb-node:dev` is built/imported from
  `d9dd29c41e94ad48a58940fc01b7ad700bdd0cf3`, with optimized binaries and
  retained debug information. OCI digest
  `sha256:bb3c6951ef7f3203ba49354678032bf3016c3db9004412875630a1b0783b57b0`;
  config digest
  `sha256:701d53a74fcdb21b29f963948cdcf06f49846c90174bd5e1ac13765cc79b9fdb`.
  Receipt: `target/crowdb.oci.tar.json`. Old image/archive `ec070372` is retained
  for historical measurements. Rebuild only when release inputs change; never
  bypass source/image provenance checks. Final domain timings need this image.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 77 pass, `/tmp/crowdb-harness-staged-fixture.log`,
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
  DiskIO on the fixed image passes 8/8 in 20.802 seconds,
  `run-7ca260ddd87c489e949438bc9d7b9d59`; its domain row is updated.
  New-image full runs are active: Console `run-6e890de5f4ab479686588a4d23719d23`,
  S3 `run-52be5dd3b8cb4e7abff03daa44b0509e`, Iceberg
  `run-a668558dcb8541919c78c4f6e1b4f13d`; reports/logs under
  `/tmp/crowdb-final-{console,s3,iceberg}`. Do not run `clean-env` while active.
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
