<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; latest accepted Web KV migration is `1dac8917`. R230 is unfinished. Do not delete the
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
- Accepted fixture gate: 82 pass, `/tmp/crowdb-export-case-fixture.log`,
  using an export of the staged source. Console browser drafts are excluded.
  S3 component UT passes 114/114; Access-server passes 108 plus its pinned
  SDK protocol test; Web/Console-shared/E2E all-target clippy
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
  root Console operation inventory is 91 cases; the two draft browser cases are additional.
  Also migrate the server-owning `app/crowdb-web/tests/native_cluster_provisioning_test.rs`
  and its `nativeDiagnostics.config.ts` browser callbacks; they are outside the
  default realBackend selection and remain in `test-console.sh`. Audit
  `71-s3-native.spec.ts` / `nativeS3.config.ts` and `72-managed-native.spec.ts` /
  `managedNative.config.ts` separately; no accepted root replacement is claimed.
- [ ] **Remaining Web route server owners**: final direct-launch audit still
  finds three native KV launcher files: `mgmt_routes_test.rs`, `replica_routes_test.rs`,
  and `replica_leader_removal_test.rs` under
  `app/crowdb-web/tests/`. Map each real-server case before deleting helpers;
  keep malformed-input, unavailable-upstream and isolated router UT portions.
  Management needs
  legacy versus sealed bootstrap persistence, bidirectional
  replica wiring, leader removal/re-election and unreachable-leader membership.
  These are unfinished owners, independent of the two managed browser
  drafts. Do not count the previous 87 passing root operations as proof of their coverage.
  `swagger_routes_are_removed` now runs without a server in the component,
  directly checking all three retired Swagger/OpenAPI paths; it passes in
  0.01 seconds. It is retained separately as `swagger_routes_test.rs`.
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
  `full_mgmt_cycle_through_web_routes` now maps to
  `console::web_management::store_and_group_management_cycle_preserves_identity`:
  exact Store identity/listing/empty initial groups, two group IDs, group detail,
  group and Store deletion pass (`/tmp/crowdb-root-web-management-ops`).
  The same case covers `add_store_delegates_to_ops_kv_logical` and
  `delete_server_returns_409_when_replicas_exist` without duplicate lifecycles.
  `kv_get_delegates_to_ops_kv_data` maps to the existing Web KV flow, now retaining
  explicit client ID/sequence and exact data (`/tmp/crowdb-root-web-kv-sequence`).
  These four accepted old cases are removed.
  `cluster_init_delegates_to_ops_cluster_init` maps to
  `console::web_bootstrap::bootstrap_replays_data_group_without_changing_replica_or_publishing_invalid_bindings`:
  initial Group-0 IDs, missing-topology 422, completed/replayed data-group creation,
  absent invalid hardware bindings and stable replica ID pass
  (`/tmp/crowdb-root-web-bootstrap`). The old ops file/launcher is removed.
  Management bootstrap variants remain native owners to migrate.
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
  The pre-route-migration root client audit found 290 tests: 277 registered
  Rust flows and 13 explicit preparation/isolation helpers (`/tmp/crowdb-r230-client-helper-audit.txt`).
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
  Ordered S3 response streaming is committed as `6f191caf`; full workspace
  clippy/fmt, S3 UT 114, Access-server UT 108 plus the pinned SDK test pass.
  Full S3 now passes 43/43 on the current formal image at CI quotas, as above.
  Full Console operations passes 89/89 in 644.632 seconds at 0.25 CPU/node,
  4 CPU / 8192 MiB: `/tmp/crowdb-delete-stream-final-console-ci`,
  `run-a7fd884f7ee44daea0056926d1ee1c71`. Focused Iceberg RCK passes all six upstream cases at its CI quota in
  62.324 seconds: `/tmp/crowdb-delete-stream-rck-ci`,
  `run-b90f13d8f8034bfa9a25267f312dd074`. Preserve run reports; no
  production source or global cleanup may change while any gate runs.
  `test.md` records these complete domain counts/timings and actual image identity;
  rerun affected complete domains after remaining migrations change their inventory.
  Per-case details stay in artifacts. Remote CI and macOS remain unmeasured.
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
