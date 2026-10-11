<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; accepted bootstrap migration is `aeab91d4`; membership fix is `8a8171f0`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains uncommitted Console browser migration drafts. Shared-image CI is committed. S3 node-fault migration and local-state component proofs are accepted.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Last accepted formal release image archive was built from
  `b7b1247b172f43562c9ffccfd95afef109bc3dde`, including ordered S3 batch response
  streaming, optimized binaries and retained debug information. OCI digest
  `sha256:703141e93338f2d0969c32a4a2d4512a0964cf999b8f67e8bd72de352970d0c1`;
  config `sha256:497764c9d5a6e068eaa0666ae15a6da1769d7d2f2304fcdaa31a7c6e84667db0`;
  runtime `bb593260b39841eefa0c012c7bbdfd714ac64fe56fed9c56c4f2be99234f562b`.
  Accepted backup: `target/crowdb.oci-b7b1247b.tar[.json]`, tag
  `crowdb-node:b7b1247b`. A rebuild for the membership fix is in progress;
  logs `/tmp/crowdb-voting-release-{stage,build,import}.log`. The original
  thousand-key test passes all three phases (35.984/49.485/61.853 seconds),
  complete wall 167.160 seconds: `/tmp/crowdb-delete-stream-exact`.
  Full S3 passes 43/43 at CI quotas in 1545.602 seconds:
  `/tmp/crowdb-delete-stream-final-s3-ci`, `run-6e60f23a5a18442ebda5d08fe5ec1842`,
  0.2 CPU/node, 4 CPU / 8192 MiB, effective one case slot.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 84 pass, `/tmp/crowdb-web-bootstrap-fixture.log`,
  using an export of the staged source. Console browser drafts are excluded.
  S3 component UT passes 114/114; Access-server passes 108 plus its pinned
  SDK protocol test; Web/Console-shared/E2E all-target clippy
  and workspace fmt pass.

## Accepted assertion references

The completed execution tasks are removed. These compact mappings retain the
original assertion ownership needed to retire launchers; evidence is in commits
and the named run artifacts.

- Metrics' three original cases → `console::web_metrics`: node shape/nonempty,
  Group prefix and Store aggregate, original three-second readiness. Evidence:
  `/tmp/crowdb-root-web-metrics-standalone`; old file removed (`ee25465b`).
- KV happy flow and `kv_get_delegates_to_ops_kv_data` → `console::web_kv`:
  text/binary round trips, client sequence, delete visibility. Evidence:
  `/tmp/crowdb-root-web-kv-sequence`; unavailable-authority UT retained (`1dac8917`, `217965c9`).
- `full_mgmt_cycle_through_web_routes`, `add_store_delegates_to_ops_kv_logical`,
  `delete_server_returns_409_when_replicas_exist` → `console::web_management`:
  Store identity, empty/two-group listing, detail, deletion and server 409.
  Evidence: `/tmp/crowdb-root-web-management-ops` (`217965c9`).
- `cluster_init_delegates_to_ops_cluster_init` → `console::web_bootstrap`:
  Group-0 IDs, replay, stable replica, missing topology 422, invalid bindings
  publish nothing. Evidence: `/tmp/crowdb-root-web-bootstrap-split`.
- `bare_metal_web_bootstrap_consumes_sealed_topology_input` →
  `console::web_sealed_bootstrap`: sealed source, HTTP 201, no residual intent or
  topology. Evidence: `/tmp/crowdb-root-web-sealed-bootstrap`,
  `run-6d91726b6917420580a4988f6701506e` (`aeab91d4`).
- `legacy_in_memory_web_bootstrap_does_not_persist_topology` → shared root Web
  bootstrap plus component `in_memory_configuration_path_never_publishes_topology_or_bootstrap_intent`.
  `mgmt_routes_test.rs` now retains only cached-authority rejection UT.
- `node_diskio_discovers_disk_groups_created_after_deployment` →
  `console::web_diskio_discovery`: readiness before/after Group-0 init, one
  initially empty DiskIO registration, discovery of groups 11/12 within the
  original ten-second bounds, final storage readiness. Uses fixed port 7200 and
  ordinary release deployment, without test-single-node mode. Evidence:
  `/tmp/crowdb-root-web-diskio-late`, `run-36431c401abc4dfe991287a950d14765`;
  old case/helper removed; the other two native provisioning cases remain.
- `swagger_routes_are_removed` → component `swagger_routes_test.rs`: all three
  retired paths; no real server startup (`ff711b4c`).

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
  root Console operation inventory is 93 cases; the two draft browser cases are additional.
  Also migrate the server-owning `app/crowdb-web/tests/native_cluster_provisioning_test.rs`
  and its `nativeDiagnostics.config.ts` browser callbacks; they are outside the
  default realBackend selection and remain in `test-console.sh`. Audit
  `71-s3-native.spec.ts` / `nativeS3.config.ts` and `72-managed-native.spec.ts` /
  `managedNative.config.ts` separately; no accepted root replacement is claimed.
- [~] **Replica route migration**: accept the three migrated HTTP flows in
  `e2e-test/tests/console/web_replicas.rs` and `web_leader_removal.rs` using
  `WalWebCluster` (two/five actual KV containers plus Web). Preserve bidirectional
  wiring, missing replica 404, leader removal 204, ten-second re-election,
  every surviving node's physical remotes, and unreachable leader 500/error
  text with retained Group-0 membership and unknown role/state. Originals
  `replica_routes_test.rs` and `replica_leader_removal_test.rs` remain until passed.
  First root attempt exposed non-voting Group-0 metadata after physical promotion
  (`/tmp/crowdb-root-web-replicas`, `run-03371a8cafbb47be8f7879372937edeb`).
  Fix `8a8171f0` preserves learner catch-up and describes the final voter in the
  Installing target; 17 affected component UT and all-target clippy pass.
  The rebuilt `8a8171f0` image passes authoritative voting, but the next deletion
  and third-member join expose physical epoch 3 versus authority epoch 2 (HTTP
  409). Evidence: `/tmp/crowdb-voting-web-{replicas,leader,unreachable}`. The next
  fix leaves learner preparation at the old epoch and promotes with the exact
  successor header. Both affected component gates still pass. Rebuild/import,
  verify authoritative voting and equal physical/authority epochs,
  then remove originals. Files: `lib/crowdb-console-shared/src/ops/kv_logical.rs`,
  `e2e-test/scenarios.py`, `e2e-test/tests/selection_test.py`, above test files.
- [ ] **Native Console assertion mapping**: classify the two remaining original cases
  before removal: `one_rack_three_nodes_provision_all_services_without_metadata_repairs`, and
  `native_page_and_iceberg_inspection`. Retain Group-0 initialization hint budget;
  pre-registration binding and live owner acquisition; stable Store selection;
  missing-binding rejection plus explicit repair; exact 18-service inventory;
  multipart replacement bytes and physical locations across each service restart;
  and page/Iceberg inspection. Internal pinned-role, injected-failure and
  geometry-validation proofs remain component-local where appropriate.
  The optional plan-prerequisite, Journal-window, production split/count/transfer,
  mixed-unit and load branches need explicit ownership too; default case coverage
  alone does not execute them. Preserve their browser callbacks and existing
  deadlines. Formal writable deployment control is required for this migration;
  managed Docker readonly cannot silently replace provisioning assertions.

## Final integration and closure

- [ ] **Launcher and coverage audit**: enumerate remaining production executable
  and in-process service launch sites, including C++/browser/SDK/manual tasks.
  Map every original distinct flow to an accepted E2E or retained component UT.
  Remove duplicate old owners only after verification. Unused native KV/storage
  harnesses are removed; shared `TestDir`/ownership UTs remain. Explicit production
  local-launch port allocation remains until its deployment callers are audited.
  Refresh the registered-client/helper audit after route migration; the old
  pre-migration inventory is `/tmp/crowdb-r230-client-helper-audit.txt`.
  SDK/browser flows have separate declared host owners.
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
  Public exports now retain machine-readable measurements and redacted logs,
  excluding mounted data/configuration and host client build trees. YAML and
  28-package task reachability pass; actual remote CI has not run.
- [ ] **Final regression and measurements**: rerun affected complete domains
  after the remaining inventory and formal-image changes settle. Baselines are
  recorded by domain in `test.md`; per-case details remain in artifacts. The last
  complete Console run covered 89 cases, while accepted bootstrap migration adds
  three cases (92 accepted; three further replica drafts awaiting acceptance).
  Do not count individual passes as a complete 95-case domain run. Remote CI and
  macOS remain unmeasured. Files: `doc/working/test.md`, root runner reports.
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
