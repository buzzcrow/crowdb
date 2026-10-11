<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; handoff checkpoint is `808ea848`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains uncommitted Console browser migration drafts. Shared-image CI is committed. S3 node-fault migration and local-state component proofs are accepted.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Current formal release image archive is rebuilt from
  `b7b1247b172f43562c9ffccfd95afef109bc3dde`, including ordered S3 batch response
  streaming, optimized binaries and retained debug information. OCI digest
  `sha256:703141e93338f2d0969c32a4a2d4512a0964cf999b8f67e8bd72de352970d0c1`;
  config `sha256:497764c9d5a6e068eaa0666ae15a6da1769d7d2f2304fcdaa31a7c6e84667db0`;
  runtime `bb593260b39841eefa0c012c7bbdfd714ac64fe56fed9c56c4f2be99234f562b`.
  Receipt: `target/crowdb.oci.tar.json`. Import is complete. The original
  thousand-key test passes all three phases (35.984/49.485/61.853 seconds),
  complete wall 167.160 seconds: `/tmp/crowdb-delete-stream-exact`.
  Full S3 at CI quotas is active: `/tmp/crowdb-delete-stream-final-s3-ci`,
  session 25823, 0.2 CPU/node, 4 CPU / 8192 MiB, effective one case slot.
  Coalescer baseline is retained as `crowdb-node:fca2b4fa` and
  `target/crowdb.oci-fca2b4fa.tar[.json]`; older `ec070372` and `d9dd29c4`
  artifacts are retained too. Never bypass source/image provenance checks.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 81 pass, `/tmp/crowdb-public-export-fixture-final.log`,
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
  root Console operation inventory is 89 cases; the two draft browser cases are additional.
  Also migrate the server-owning `app/crowdb-web/tests/native_cluster_provisioning_test.rs`
  and its `nativeDiagnostics.config.ts` browser callbacks; they are outside the
  default realBackend selection and remain in `test-console.sh`. Audit
  `71-s3-native.spec.ts` / `nativeS3.config.ts` and `72-managed-native.spec.ts` /
  `managedNative.config.ts` separately; no accepted root replacement is claimed.
- [ ] **Remaining Web route server owners**: final direct-launch audit still
  finds four native KV launcher files: `mgmt_routes_test.rs`, `replica_routes_test.rs`,
  `replica_leader_removal_test.rs`, and `ops_migration_test.rs` under
  `app/crowdb-web/tests/`. Map each real-server case before deleting helpers;
  keep malformed-input, unavailable-upstream and isolated router UT portions.
  Management needs
  legacy versus sealed bootstrap persistence, full management cycle, bidirectional
  replica wiring, leader removal/re-election and unreachable-leader membership.
  Ops delegation needs cluster init, Store creation, replica-conflict 409 and KV
  reads. These are unfinished owners, independent of the two managed browser
  drafts. Do not count the previous 87 passing root operations as proof of their coverage.
  Current search evidence: `/tmp/crowdb-r230-remaining-launch-sites.txt`.
  Metrics consolidation is accepted: three original metric cases map to
  `console::web_metrics::structured_metrics_preserve_node_group_and_store_scopes`.
  The formal standalone profile passes all original node structure/nonempty,
  Group prefix and Store aggregate assertions, with the original 3-second leader
  readiness bound. Old `metrics_proxy_test.rs` and its native launch helpers are
  removed. Evidence: `/tmp/crowdb-root-web-metrics-standalone`,
  `run-6b649392966f4925a9ea92304d854db1`. The bare-metal profile correctly rejects
  these legacy routes; no production mode or assertions were changed.
  Web KV consolidation is accepted: original
  `kv_put_get_delete_through_web_routes` maps to
  `console::web_kv::put_get_delete_and_binary_values_through_web_routes`.
  Store/group creation, endpoint status, text values, deletion visibility and
  binary hex values pass at CI quotas (`/tmp/crowdb-root-web-kv`,
  `run-994fc98b33dc4b1bbe62f76df1d820b6`). Shared legacy bootstrap also reruns
  metrics successfully. Keep `kv_get_returns_502_when_leader_unreachable` as
  pinned cached-authority component UT; the original production launcher is removed.
- [ ] **Native Console assertion mapping**: classify all three original cases
  before removal: `node_diskio_discovers_disk_groups_created_after_deployment`,
  `one_rack_three_nodes_provision_all_services_without_metadata_repairs`, and
  `native_page_and_iceberg_inspection`. Retain late DiskIO discovery of groups
  11/12 at the original 10-second bound; Group-0 initialization hint budget;
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
  Compiled root client audit finds 290 tests: 277 registered Rust flows and 13
  explicit preparation/isolation helpers (`/tmp/crowdb-r230-client-helper-audit.txt`).
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
- [~] **Final regression and measurements**: all complete registered domain and
  special-profile runners on the coalescer image have finished. KV 50, Diskdb 5,
  Diskio 8, Chunkdb 11, Chunk-client 40, Chunk-stream 4, Chunk-KV 2, Dataset 2,
  Deployment 33, Console operations 87, and Iceberg 32 pass. Linux host, SSH
  bridge admission, containerd host/cluster and both ecosystem profiles pass.
  Logs/reports: `/tmp/crowdb-coalesce-final-*`. Core UT passes 978/978.
  S3 remains 42/43: the third phase of
  `test_batch_delete_thousand_keys_and_unversioned_retry` reaches its original
  120-second bound. Isolated passing runs do not close this failure.
  All-node CPU sampling shows little throttling. The pinned SDK has a 60-second
  idle-read timeout and legacy retries; the buffered response sends no progress
  until all 1,000 durable sequential deletions finish. SDK retransmission is the
  current diagnosis to verify with the formal image.
  `DeleteSelection::stream` now validates before mutations and yields ordered
  results; production `delete_objects` streams the XML opening/results/closing,
  with whitespace for Quiet successes. No timeout or assertion is changed.
  Full S3 component UT passes 114, Access-server UT 108 plus the separately passing pinned SDK test, and focused clippy passes. Fix committed as `6f191caf`; full workspace clippy/fmt pass.
  The formal image above is built; finish import, then run the exact original
  case and complete S3 at CI quotas.
  Update `test.md` with complete domain counts, timings and actual image identity;
  per-case details stay in artifacts. Remote CI and macOS remain unmeasured.
  The document layout is accepted and needs no redesign. Do not run global
  cleanup during an active container gate.
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
