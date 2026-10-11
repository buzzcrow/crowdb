<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish migration of real-server tests to `e2e-test/`, preserve component UT,
and close the Pixi/CI/measurement audit. This handoff lists unfinished work only.

## Resume context

- Branch `task-deploy`; latest accepted browser migration `99ed5435`; native route migration `da0ab099`; runtime membership fix `cb159e7b`. R230 is unfinished. Do not delete the
  requirement/backlog entry until every remaining owner and required gate passes.
- Working tree contains the unaccepted three-node native Console fixture exploration. Shared-image CI is committed. S3 node-fault migration and local-state component proofs are accepted.
  Preserve these drafts. `e2e-test/clients/console-ui/node_modules/` is local
  dependency state; never stage it.
- Current formal image is built/imported from
  `3923ffc549f8f58fa6656a50824a3b02aff4e070`, including S3 streaming,
  replica voting/epoch fixes and explicit Web readonly policy. OCI `sha256:0c1f070dca690adfac19e96c057806f7bd3ffd33b0c657812908f8b700144eee`;
  config `sha256:7d4876084b4dc093d6d6c0d2a7fab3b92598a8be739647d7a1f9c0de19d0df97`;
  runtime `176bfc8b60ec518315ea1abf7681cd18c614b091396bc751b9e8b1069bd148cc`.
  Receipt: `target/crowdb.oci.tar.json`; optimized with retained debug information.
  Logs: `/tmp/crowdb-explicit-readonly-release-{stage,build,import}.log`.
  Last full S3 43/43 (1545.602 seconds) and Console 89/89 (644.632 seconds)
  used `b7b1247b`; backup `target/crowdb.oci-b7b1247b.tar[.json]` and tag
  `crowdb-node:b7b1247b` remain. These results are baselines, not full acceptance
  of the newer image/inventory. Compact domain reports are linked in `test.md`.
- Containers run formal servers, host processes run clients. Fixed ports,
  inspected IPs, independent networks/data mounts, and three actual KV voters
  in three containers remain mandatory. Runtime service selection reuses one
  image. Never extend deadlines, retry the assertion under test, weaken errors,
  or substitute a test-server build to make migration pass.
- Delete original real-server owners only after equivalent accepted assertions.
  Keep deterministic component UT. Run executable commands through `pixi run`;
  native server gates require `clean-env`, never during another server gate.
- Accepted fixture gate: 89 pass, `/tmp/crowdb-managed-accepted-fixture.log`,
  including the accepted managed browser fixture and ownership controls.
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
- `replica_add_remove_wires_peers_bidirectionally` → `console::web_replicas`:
  two actual KV voters, bidirectional remotes, exact list/detail, missing 404,
  leader delete 204, lone-voter re-election within ten seconds. Added authority
  voting and persisted epoch consistency proof. Evidence:
  `/tmp/crowdb-voting-epoch-web-replicas`, `run-5bd51543ace04ad78fa968985013772b`.
- `remove_leader_from_five_node_group_elects_new_leader` →
  `console::web_leader_removal`: five actual voters, delete 204, four remaining,
  removed peer absent on all nodes, new physical leader within ten seconds.
  Evidence: `/tmp/crowdb-voting-epoch-web-leader`, `run-36708ce7afd8422d8090c81f8591ee7a`.
- `remove_unreachable_leader_retains_group0_membership` → same root module:
  owned kill/death within five seconds, original 500/error text, retained Group-0
  member and unknown role/state. Preserve network namespaces to retain endpoint
  failure semantics; reserve all five helpers. Two independent parallel runs
  pass: `/tmp/crowdb-voting-epoch-web-unreachable-retained-{a,b}`.
  Both original replica files/launchers are removed.
- All three original shell cases → `console.browser.shell` /
  `clients/console-ui/e2e/flows/00-shell.spec.ts`: backend-unavailable alert,
  seven-domain navigation and real-data embedding (apiPrefix, readonly, module
  opt-out). Original spec is removed. Each host browser owns an ordinary release
  Web container and client workspace; KV uses fixed 7000/7001 inside that node.
  Unavailable listener uses closed container port 9091 to avoid loopback access
  from a private-IP page. Proxy page readiness precedes the embedding navigation;
  original three-second request/DOM assertions remain, with no mutation retries.
  Three cases pass together concurrently in
  `/tmp/crowdb-root-shell-proxy-ready/run-11b32d674f504ebfaaa630da85c9077e`,
  then at CI budget in `/tmp/crowdb-root-shell-ci-final`. Browser fixture reserves
  at least 2 CPU plus host client (2.5 CPU / 2560 MiB total); 4 CPU budget gives
  one case slot, 6 CPU allows two. No private browser is installed. Image reuse
  excludes only the audited unbundled source spec; UI source/build guards remain.
- Five independent registry UI cases from `10-cluster-rack-node.spec.ts` and
  `13-todo-ui-behavior.spec.ts` → `console.browser.registry`: empty shell and
  runtime-error check, datacenter/sidebar/canvas rack creation plus API identity,
  Cluster/Chunk selections without ownership probes, Access-only port overrides
  and disabled/waiting states across reload, fixed-slot warning health/detail
  and persisted plan. Only these original cases are removed; multi-service
  cases remain. Each case owns a release Web container; no shared reset or
  native port allocator. Fixed Access plan ports are 9000/9001/9002.
  Exact old cases pass in `/tmp/crowdb-r230-ui-baseline.log`. Root five-case CI
  gate passes in 38.580s at 4 CPU / 8192 MiB / one slot (`run-b02f6a6cf861454e85f49e1d0200f105`).
  Initial pipelined parallel runs fail at pending browser rack/node POSTs while
  peer networks start/stop; extra node CPU does not fix them. Stable-topology
  parallel control passes. Host shell/registry scheduling now starts a bounded
  batch before clients and retires it after every client completes. Five cases
  pass with two slots in 26.301s in `/tmp/crowdb-root-registry-batched`
  (`run-d4552a3c2db5477492d3844aebe9b378`). Existing shell 3/3 also pass
  concurrently after scheduling changes (`run-bb37cf5e8bfd46bcbdc2e63c8d2759ec`); lifecycle component
  proofs cover bounded ordering, client failure and partial-start cleanup.
  Image reuse exempts only the two audited host specs, retaining UI source guards.
- All three original KV data page cases and all four canvas cases →
  `console.browser.kv` / `console.browser.canvas`: exact 60-row pagination,
  selected-key browser history, uppercase binary formatting/CSS, initial and
  reselected auto-scan, system-group readonly UI;
  viewport fit/pan/domain restoration, collapsed children/nested state/context
  menus. Old source files are removed. Owned normal Web/KV uses fixed 7000/7001
  and durable defaults; no host launch, reset, port allocator or mutation retry.
  Baseline: `/tmp/crowdb-r230-local-browser-baseline-valid.log`. New 3/3 and 4/4
  pass with two independent containers in `/tmp/crowdb-root-browser-kv-literal`
  and `/tmp/crowdb-root-browser-canvas`. Browser selectors escape literal test
  names, including parentheses, so exactly one case runs per fixture.
- Both original managed browser cases → `console.managed`: physical write 503,
  KV put/query/delete, Iceberg schema and demo cleanup, S3 preview and actual
  Chunk/disk placement, Group-0 outage under the original ten-second bound,
  retained monitor observation and recovery. Each host browser owns a complete
  release node and workspace; pause/resume targets the owned container KV PID.
  Normal mounted Web configuration explicitly disables readonly internally.
  Both cases pass concurrently in 25.023s (`run-1a8ef3d01e5a45e7a969da8202e25c42`).
  The public readonly browser gate passes (`run-f196be80010c4dbaa5c6fc46b99bb59e`).
  Original managed spec and native server launcher are retired; its old script
  delegates to the root scope. The same immutable image serves both modes.
- `swagger_routes_are_removed` → component `swagger_routes_test.rs`: all three
  retired paths; no real server startup (`ff711b4c`).

## Remaining migrations

- User clarified: Docker Web is writable by default; only explicit readonly
  disables writes. The public single-container profile enables readonly;
  internal test profiles allow writes. Preserve Docker physical-mutation
  restrictions. Implement process configuration, API mode and UI propagation,
  component proofs and both release-container browser owners before closure.
  Configuration 2/2, managed router 5/5 and UI production build pass; both
  writable managed and public readonly release browser gates pass.


- [ ] **CLI S3 bootstrap replay owners**: migrate the two server-starting
  cases in `app/crowdb-cli/tests/s3_cli_test.rs`:
  `interrupted_s3_launch_resumes_confirmed_group_zero_without_topology_file`
  and `interrupted_s3_storage_launch_replays_without_local_topology`.
  CLI startup and lifecycle now operate on a three-node container cluster,
  using the formal image, public admission/bootstrap and fixed per-node ports.
  A container name selects its owning cluster; root paths select mounted data.
  Preserve interrupted-start markers, relative-root resolution, confirmed Group-0
  replay, retained Diskdb container service, no local topology/legacy console
  file, completed marker/intent removal and owned deletion. Migrate the old
  binary-failure injection to owned container/client interruption at equivalent
  checkpoints. Retain unrelated HTTP stubs and argument validation UT.
- [~] **Console browser migration**: audit and migrate the remaining specs using
  `app/crowdb-web/ui/e2e/realBackend.config.ts`. Host browsers consume owned
  container endpoints; signals validate container ownership and current service
  PID. Current accepted Console operation inventory is 113 cases. Shell,
  registry, KV data, canvas and both managed flows have accepted owners.
  Also migrate the server-owning `app/crowdb-web/tests/native_cluster_provisioning_test.rs`
  and its `nativeDiagnostics.config.ts` browser callbacks; they are outside the
  default realBackend selection and remain in `test-console.sh`. Audit
  `71-s3-native.spec.ts` / `nativeS3.config.ts` and `72-managed-native.spec.ts` /
  `managedNative.config.ts` separately; no accepted root replacement is claimed.
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
  Latest Rust client audit: 299 compiled tests = 286 registered required/manual
  owners + 13 preparation/isolation helpers; shell browser cases are separate
  host owners. Evidence: `/tmp/crowdb-r230-current-client-helper-audit.txt`.
  Refresh after further migration; remaining launch-site evidence is
  `/tmp/crowdb-r230-final-launch-sites.txt` and
  `/tmp/crowdb-r230-current-browser-task-launch-audit.txt`.
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
  once the remaining migration inventory settles. The current membership image
  passes complete Console 96/96 in 691.960 seconds and Deployment 33/33 in
  601.028 seconds at CI quotas (0.25 CPU/server, 4 CPU / 8192 MiB, one effective
  case slot). Reports: `run-ab39e156c2c24da39a5db1bfbcba1a7c` and
  `run-5cfba02195364ce483aac4b5a3eaf1ee`; logs under
  `/tmp/crowdb-voting-epoch-final-{console,deployment}-ci.log`.
  The fifteen newer shell/registry/KV/canvas browser cases pass separately and are not
  included in the complete 96-case measurement. Current accepted Console inventory is 113;
  final full-domain measurement remains pending. Remote CI and macOS remain
  unmeasured. Files: `doc/working/test.md`, root runner reports.
- [ ] **Close R230**: after inventory, assertion mapping, local gates and required
  environment results are satisfied, remove R230 detail/backlog entry and this
  plan in the final cleanup commit. Requirement commits use coherent verified
  batches and single-line subjects without requirement numbers.
