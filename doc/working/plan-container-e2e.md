<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; HEAD `6f6442c2`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains uncommitted S3 node-fault migration, Console browser
  migration, shared-image CI drafts and two S3 local-state component proofs.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Formal release image `crowdb-node:dev` is built/imported from `6f6442c2`, with
  optimized binaries and retained debug information. OCI digest
  `sha256:746036d045623ecda6171dcaeaabe175980128b3d7c3e69ff35de297c066fda5`;
  config digest
  `sha256:e33128ddafc2572c9689d5ec887653403ac55f20076fea9a4a7da82ae743df59`.
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
- Last fixture gate: 58 pass, `/tmp/crowdb-fixture-retained-joint.log`. This
  includes pending Console fixture proofs, not browser acceptance. Last S3
  local-state gate: 2 pass, `/tmp/crowdb-s3-local-state-component.log`; fmt and
  all-target clippy for this new file remain pending.

## Remaining migrations

- [~] **S3 joint-node faults**: complete all three cases, including node-two
  persisted degraded EC placement, whole-stack restart, exact recovered bytes
  and the original 25-second repair bound. Files:
  `e2e-test/s3_node_fixture.py`, `tests/access_s3/node_failure{.rs,/recovery.rs}`,
  `fixture.py`, `scenarios.py`, `tests/retained_network_test.py`.
  The fixture has 14 server containers. All twelve KV/Diskdb/Chunkdb/DiskIO
  endpoints now retain network namespaces during process exit, matching native
  closed-port semantics; include twelve owners in resource accounting.
  Latest full run: `run-27ed80aa67d145e5bec28602e640d850`, command
  `pixi run test-container-e2e --scope s3.nodes --concurrency 1 --node-cpus 1 --cpus 16 --memory-mib 16384`.
  Log `/tmp/crowdb-s3-three-nodes-retained-services.log`; run finished, no active
  test session. Nodes one/three pass (31.20/30.73-second clients); node two
  fails at the new-object PUT (`node_failure.rs:187`, response body decoding).
  Overall 2/3 pass, 194.031-second wall; restart/repair assertions were not reached.
  Failed case `crowdb-e2e-c83ea4ef77f74e98b39f584a083a7ddd`: Access logs show
  large-write preparation returning submitted-outcome-unknown after a
  `ConnectionClosed` on stopped node-two Chunkdb endpoint `10.200.62.9:7302`.
  Isolated resume `run-100471f2a80741acb336aed1bfeb13a8` reproduces the PUT
  failure. Registry refresh updated endpoints but retained the populated stale
  service-slot map for ID-less allocation. Periodic refresh now advances both
  maps through `refresh_routes` before selecting an owner. Deterministic pinned
  metadata/fake-RPC proof fails before the fix and passes after; three existing
  dynamic retry proofs also pass, including unknown-mutation no-replay.
  Chunkdb/client all-target clippy passes. Rebuild the formal image and rerun
  node two, then all three faults; do not replay an unknown allocation.
  Isolated corrected node one passes in `run-9ddc8303c6d14dc0a892a3d97557d25a`
  (31.02-second client). Earlier run `run-055cef59e17d4b8fa5339bda92109e31`
  passed node three, failed nodes one/two after Chunkdb metadata RPC timeout;
  surviving DiskIO no longer exited 139. Original DiskIO crash was a late FFI
  callback writing stack storage; fix is committed in HEAD, fatal backtrace
  `/tmp/crowdb-diskio-caught-crash.log`. Host Apport provided no matching core.
- [ ] **Remaining S3 mini-cluster owners**: map and accept
  `persistent_cluster_survives_stop_restart_and_range_read` and
  `protected_cluster_starts_and_reads_after_restart`, plus the three node-fault
  cases in `lib/crowdb-console-shared/tests/s3_mini_cluster_test.rs`. Retain
  Web health/Group-0 preview, exact bytes, range 8..13 = `object`, stable endpoint,
  protected three-rack layout, full restart, and local launch state without
  secrets/topology/legacy `console.toml`. New `s3_local_state_test.rs` covers
  public stop/load serialization and inline-secret rejection without servers;
  finish its gates and assertion mapping. The old five server owners and three
  pure UT remain; do not delete them prematurely. Then run full S3 domain,
  including protocol/SDK/recovery cases, and commit one accepted migration batch.
- [ ] **Iceberg final acceptance**: rerun failures on the new image without
  simultaneous heavy native compilation, then all 32 registered cases.
  `run-c955f9edebe2413b8c9469379667d0c8` failed 5/32, wall 2527.939 seconds,
  `/tmp/crowdb-iceberg-32-final-domain.log`: Java S3FileIO, repeated 100 MiB
  direct/multipart uploads, small routing, and a Chunk-KV startup readiness
  failure. Preserve failed evidence. Do not attribute all failures to DiskIO
  without a fresh result, or replace the historical 30-pass timing with a
  claimed 32-pass result. Existing component crash-cut matrices stay local.
- [ ] **Manual TPC loader owner**: migrate ignored
  `tpc_loader_parallel_stress` in
  `app/crowdb-access-server/tests/iceberg_file_http_test.rs` to a root manual
  scope with explicit prerequisites. Preserve two concurrent host loader
  processes (TPCH workers 8, TPCDS workers 24, SF1, token workers 32) and exact
  validation. `CROWDB_TPC_LOADER_PYTHON` is unset and the external loader Python
  environment is absent. The explicit legacy native preparation path remains
  until equivalent acceptance; do not report a skip as a pass.
- [ ] **Console browser migration**: resolve the mode contract below, then
  accept both managed browser flows and audit every spec using
  `app/crowdb-web/ui/e2e/realBackend.config.ts`. Two copied flows under
  `e2e-test/clients/console-ui/e2e/`, `console_fixture.py`, browser configuration
  and fixture tests are unaccepted drafts. Host browser consumes owned container
  endpoints; signaling validates service PID and run ownership. Preserve UI
  assertions and remove native runners only after matching acceptance. Current
  root Console baseline is 83 cases; the two draft browser cases are additional.
- [ ] **Web standalone and disposable lifetime**: migrate real owners in
  `app/crowdb-web/tests/standalone_startup_test.rs`: three-member Group-0 plus
  Diskdb recovery, graceful test-mode shutdown, and forced-exit orphan cleanup.
  Three voters must occupy separate containers. Preserve authoritative rack
  restoration over corrupted local cache, three replicas, four servers and
  retained Diskdb launch inputs. Split fake-child cleanup/ownership proofs into
  component tests; container PID-namespace teardown alone does not prove native
  orphan cleanup. Public `lifecycle::http_internal_reset` and
  `AppState::set_runtime_pid` are available; existing fake-child reset proof is
  in `lifecycle_routes_test.rs`. Keep the pure failed-publication UT.
- [ ] **Special deployment owners**: finish inventory/disposition of SSH/node
  admission, artifact policies, host-network/real-device and Docker/containerd
  profiles. Classify external Trino/Spark/Flink gates and prerequisites. Preserve
  distinct ownership/resource assertions; ordinary bridge acceptance does not
  establish hardware/host/containerd support.

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
  manual TPC and native Console paths remain. Java FileIO has SDK ownership and
  foreground GC has the native GC delegate; preserve these distinct selections.
- [ ] **CI shared-image jobs**: finish and commit drafts in
  `.github/workflows/ci.yml` and `iceberg-rust-sdk.yml`. All container domain jobs
  consume one source-pinned formal artifact; preserve debug info and diagnostic
  uploads. S3 node quota is 0.2 CPU: maximum declared case budget 3.9 CPU /
  7872 MiB fits its 4 CPU / 8192 MiB job. Console draft uses full domain;
  storage matrix selects domains. YAML parses; package reachability covers 28
  workspace packages. Validate selectors, required result counts and job graph;
  actual remote CI has not run. No push is authorized by this handoff.
- [ ] **Final regression and measurements**: run affected DiskIO after the FFI
  fix, full S3/Iceberg/Console and required registered layers on the same image,
  then relevant component/fmt/lint gates. Update `doc/working/test.md` using one
  row per domain and actual complete-run counts/wall times/environment/image.
  Keep per-case detail in artifacts; never sum partial batches into a full pass.
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
