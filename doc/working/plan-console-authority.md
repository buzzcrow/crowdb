<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console Authority Plan

Upstream: [R188](../backlog/R188-console-group0-authority.md).

Goal: make Group 0 the shared CLI/Web authority while retaining only process
and launch inputs locally.

Status: active after the single-node CI repair. The remaining authority,
configuration, documentation and crash-diagnostics tasks below are pending.

## Registration and acceptance failures

- [x] **Stable registration across restart**: persist generated instance IDs
  under the KV node's config root, reject changed explicit identities and
  corruption, and prove restart replaces an unexpired old registration rather
  than creating ambiguity. Diagnose the concurrent restart suite without
  increasing election timeouts. Files: KV server startup/background identity,
  discovery integration and Web incremental restart tests.
- [x] **Pre-bootstrap nonmember registration**: reproduce the missing live
  registration for servers started before Group 0 but excluded from its member
  set. Propagate discovery seeds after confirmed initialization, retain them
  across restart as launch inputs, and wait for exactly one live identity before
  declaring the bootstrap complete. Do not create local topology fallback or
  restart processes into a different workspace. Files: KV server keepalive and
  management modules, console shared cluster initialization and HTTP client,
  focused integration tests, UI node-inspection and replica flows.
- [x] **Unavailable logical view**: preserve the explicit unavailable state
  before Group 0 exists and during outages, clear stale logical rows, and avoid
  treating an expected unavailable response as an unhandled browser exception.
  Update the canvas navigation assertions to distinguish unavailable authority
  from a confirmed empty cluster. Files: UI logical-tree data hook, KV panel,
  shell/canvas/full-chain specs.

## Common logical operations

- [x] **Group and replica creation fan-out**: reject missing peer registrations,
  missing peer endpoints and failed
  remote wiring, roll back created local groups, and publish no Group 0 group
  or replica records on these failures. Prove with real Group 0 and controlled
  management endpoints. Files: shared `ops/kv_logical.rs` and
  `tests/ops_logical_fanout_test.rs`.
- [x] **Logical deletion cleanup**: derive store hosts from both store and
  replica membership, confirm every node deletion before removing authority,
  and remove descendant records before parents. Test success, sibling
  preservation, later replica hosts and node-side failure. Files: shared
  `ops/kv_logical.rs`, `tests/ops_logical_delete_test.rs`.
- [x] **Replica creation cleanup**: clean a newly created target store when
  local group creation fails, preserve pre-existing target groups, and report
  incomplete rollback. Test injected creation and cleanup failures.
- [x] **Confirmed logical mutations**: reconcile lost responses through
  confirmed authority, complete replica fan-out/rollback and delete cleanup;
  preserve Group 0 membership when node-side deletion fails. Reuse the common
  flow in CLI and both Web modes, with no local topology commit.
  Conditional publication replaces overwrite writes for stores, groups and
  replicas; test concurrent matching/conflicting records and a real RPC reply
  dropped after commit. New groups and their initial replicas now use one
  conditional batch; confirm the complete member set after a lost reply.
  Reconcile failed delete responses with a confirmed absence read.

## Configuration and hardware operations

- [x] **Launch registry lifecycle**: wire `WebProcessConfig` and `LaunchRegistry`
  into CLI and bare-metal Web deploy/restart paths. Consume binary, service
  config, workspace, host and auto-start policy; retain PIDs only in runtime
  state and resolve SSH credentials through references. Files: console shared
  config/lifecycle, CLI startup, Web startup/state/lifecycle and tests.
  First complete launch arguments/readiness inputs, shared local/SSH lifecycle
  and runtime-only process identity. Then connect Web auto-start and CLI
  deployment/restart callers before removing mixed persistence.
  Shared primitives are implemented in `launch.rs` and its local/remote/runtime
  modules: private PID/start-time records, idempotent start, referenced service
  config and SSH keys, readiness checks, and failure cleanup. Local and real
  SSH transport regressions pass, including a native KV launch and refusal to
  adopt an unrelated healthy endpoint. Complete Console shared tests, fmt and
  clippy pass. Logs: `/tmp/crowdb-launch-{shared,lint,fmt}.log`.
  Web now loads and reconciles auto-start policy, exposes authenticated
  start/restart/stop and runtime views, and reloads policy on each request.
  CLI `--registry` deploy/start/restart/stop/delete uses the same runtime;
  deletion checks confirmed replica membership before removing launch policy.
  Native Web and CLI integration tests pass, including ignored legacy state,
  idempotent start, changed restart identity, and policy edits without a Web
  restart. Complete shared/CLI/Web regressions, fmt and clippy pass.
  Logs: `/tmp/crowdb-launch-consumers-{full,lint-3,fmt}.log`.
  Generic CLI `launch list/start/restart/stop` and chunk diskdb/chunkdb/diskio
  deployment controls now share the same launch runtime. Process controls
  work before Group 0; chunk service lists still read live registration.
  Three-service lifecycle regressions and the complete CLI suite, fmt and
  clippy pass. Logs: `/tmp/crowdb-chunk-launch-*.log`. Removal of legacy
  startup/restore paths remains coupled to bootstrap cutover below.
- [~] **Remove mixed persistence**: remove the unreleased `ConsoleConfig`
  parser/writer, inline SSH secrets, topology restoration and fixtures after
  the launch lifecycle and replay-safe bootstrap paths are wired. Preserve
  bootstrap intent independently until verified cutover. Update CLI commands,
  Web persistence and S3 mini-cluster callers together; no compatibility reader
  or migration path because the old configuration was never released.
  S3 mini-clusters now persist versioned local process/seed state rather than
  `console.toml`; restored KV launch nodes are ephemeral process inputs and the
  bundled Web uses `WebProcessConfig`. The full persistent S3 stop/restart and
  range-read E2E passes. Remaining CLI/Web legacy config paths and the shared
  parser/writer still need removal.
- [ ] **Confirmed hardware operations**: route CLI and bare-metal Web through
  shared Group 0 hardware operations; preserve conflicts and uncertain writes
  without local-first commits. Docker keeps its hardware restrictions. Hardware
  client cascades now stop at a failed child deletion instead of deleting its
  parent while a descendant may survive. Extend Group 0 hardware values with
  rack names, node management hosts, nonsecret SSH connection settings and
  credential reference IDs; resolve secret material locally. Those Group 0
  fields are now part of rack/node records and bootstrap writes the names,
  management host, SSH port/user and reference without copying secret material.
  Bare-metal snapshots expose the same Group 0 values to separate consoles.
  Conditional rack/node creation now confirms matching existing values and
  rejects conflicting values without changing either console's local topology.
  Group 0 rack/node lists expose shared names, management hosts, SSH settings
  and credential references but no private key material. Registry-mode CLI
  add/list commands use these operations; a real CLI process regression caught
  a management-port-as-RPC seed and now refreshes topology before the hardware
  write. Complete Console shared and CLI suites, Rust fmt and workspace clippy
  pass. A real RPC proxy drops the committed rack write reply; a confirmed
  linearizable read recovers the successful outcome. Registry-mode Web,
  deletions, disks and legacy bootstrap still remain.
  Node creation now conditionally updates rack membership and creates the node
  in one Group 0 batch. Two concurrent consoles retain both child IDs; a
  repeated rack add preserves its existing children. Retried node creation
  compares immutable connection identity while preserving live status fields.
  Registry CLI rack removal now conditionally deletes only a confirmed empty
  rack; shared and real CLI regressions cover child conflict and absence.
  Bare-metal Web now exposes authenticated Group 0 rack creation/removal and
  node creation, with public confirmed rack/node reads. Two Web instances
  observe the same records; inline SSH material is rejected. Registry CLI and
  bare-metal Web now remove only unused nodes through one conditional Group 0
  rack-membership/node deletion; occupied nodes and unauthenticated Web writes
  fail. Disk-group and disk creation/removal now update the child and parent
  records in one rack-revision-fenced Group 0 batch. Their names, membership
  and disk attributes are read from confirmed authority by CLI and bare-metal
  Web; tests cover two consoles, conflicts, occupied deletion, private Web
  writes and a lost committed write response. Legacy Web routes and CLI
  mixed-config paths still need removal.
- [ ] **Authority-only reads**: replace local monitor/config topology and
  endpoint fallbacks with Group 0 and live registrations. Missing, ambiguous or
  expired registrations remain unavailable.
  Versioned bare-metal snapshots now read Group 0 without requiring a Docker
  monitor; Docker keeps its monitor requirement and overlay. Validate every
  replica host as well as the store's original hosts. Real Group 0 regressions
  cover missing, duplicate and expired registrations, recovery, and outage
  without stale topology. Docker and launch-route regressions, fmt and clippy
  pass. Logs: `/tmp/crowdb-bare-authority-*.log`. Legacy physical routes and
  monitor refresh still remain for the mixed-config removal. Production Web
  startup now requires a versioned process config, so it never loads the old
  mixed file; bare-metal rack/node/disk-group/disk detail and collection
  routes read Group 0 directly. The old in-process router and CLI no-registry
  paths remain to migrate or remove.
- [ ] **Replay-safe bootstrap cutover**: persist bootstrap identity, verify
  committed records, write only safely missing content, reject conflicts and
  delete topology intent after verified transfer. Clean/destroy use confirmed
  authority. Replace S3 mini-cluster persistence against the same contract,
  without a migration path for its unreleased mixed configuration.
  System initialization now confirms an existing replica's identity after a
  conflict or lost response, preserves groups for retry after peer failures,
  and requires every peer endpoint and remote-wiring request to succeed before
  recording membership. Four focused failure cases, complete shared tests,
  Web deploy/restart/migration suites, fmt and clippy pass. Logs:
  `/tmp/crowdb-bootstrap-replay-*.log`. Durable intent/cutover remain pending.
  A separate versioned bootstrap intent now captures rack/node identity, KV
  management endpoints and selected member order without process PIDs, binary
  paths or inline SSH secrets. It is atomically sealed with mode 0600;
  interrupted retries restore a fresh in-memory context, reject changed
  topology before mutating Group 0, then delete the intent only after
  confirmed publication. Persistent CLI and legacy Web cluster-init callers
  now use this path. Real Web and CLI regressions confirm the intent is removed
  after successful Group 0 publication. Versioned bare-metal Web now exposes
  an authenticated cluster-init route and accepts the same independent
  bootstrap input without writing a mixed console file. Registry CLI accepts
  a versioned bootstrap input, seals an immutable
  retry copy beside the launch registry, runs the same confirmation path and
  deletes that copy after success. It does not write the mixed console file.
  The legacy CLI/Web path still writes that file; S3 mini-cluster still needs
  bootstrap-interruption replay before the old format can be removed. Its
  completed cluster now restarts from launch-only local state and Group 0
  seeds; no local topology is loaded after publication. A full persistent S3
  stop/restart and range-read E2E passes. S3 now saves its local KV launch
  state before sealing bootstrap intent and publishing Group 0. An interrupted
  launch retains that state for identity-checked retry instead of archiving the
  committed cluster. A real failure injected at Chunk KV startup recovers on
  the next CLI invocation, with all 15 services ready and no topology file.
  The CLI integration test reproduces this interruption and recovery. Partial
  storage-service launch sets still fail closed and need completion or an
  explicit operator recovery path; mixed CLI/Web config remains to remove.
  S3 now canonicalizes a relative root before creating child launch paths;
  the interrupted CLI test covers a relative root. The S3 CLI mock fixture
  supplies the required launch-only state; its three previously failing cases
  now pass. The complete Console gate needs rerun.
- [x] **Confirmed bootstrap metadata**: preflight existing hardware and logical
  records, accept matching content without rewriting revisions, reject conflicts,
  and conditionally create missing records. Reconcile uncertain writes with
  confirmed reads; record local membership only after publication is confirmed.
  Three real-authority regressions failed before the fix and now pass. Strict
  publication exposed missing leader discovery in conditional KV writes:
  explicit no-hint not-leader rejections now use the existing bounded retry
  policy, while ambiguous dispatch still returns `OutcomeUnknown`.

## Crash diagnostics follow-up

Transferred from R187 by user request. It does not block the single-node image
requirement.

- [x] **Crash dump location and retention**: document how Linux
  host `core_pattern`, Docker's core ulimit, and the non-root container affect
  CROWDB child and PID 1 crashes. Cover a plain relative core-file pattern,
  Ubuntu Apport, systemd-coredump, and Docker Desktop's Linux VM. Choose a
  bounded, private location under the mounted `/opt/crowdb/data` volume where
  the host permits file dumps; otherwise report the host collector location
  and provide explicit setup guidance instead of claiming the volume contains
  a core. Use a private data-volume directory and retain the newest `core`
  file after child recovery and monitor restart. Require Docker's core ulimit
  for a per-dump size bound. Document how to inspect a real child crash,
  retention, secret exposure, and exact-build symbolization. Do not
  change the host-wide `core_pattern` from inside the container. Files:
  `container/single-node-container/{Dockerfile,entrypoint.sh,tests/**}`,
  `container/crowdb-monitor/src/**`,
  `container/single-node-container/README.md`.
  The single-node README now states the host collector boundary and identifies
  Apport, systemd-coredump and Docker Desktop lookup paths without promising a
  volume dump. This host reports an Apport pipe pattern and core ulimit 0.
  The container now creates a private crash directory after the bootstrap
  manifest is opened, runs the monitor and children there, and retains the
  newest regular `core` file after restart or child recovery. A 1 GiB Docker
  core ulimit example bounds each dump. Focused retention and monitor suites
  pass. The complete container release, image and E2E gate passes, including
  startup, crash and hang recovery, persisted-volume restart, exhausted restart
  budget and monitor death. Rust fmt and clippy pass. This host's Apport pipe
  can export a packaged program's real dump, but a CROWDB KV child abort left
  no report because Apport cannot resolve its container-only executable path.
  The monitor recovered the child. A symbols-enabled image and archive for the
  same revision passed hashes, debuglink CRCs and `.debug_line` checks; the
  symbolizer resolved a debugger-generated CROWDB monitor core to
  `container/crowdb-monitor/src/main.rs:55`. The developer guide at
  `doc/dev/crash_debugging.md` now covers host configuration and rollback,
  private core handling, GDB with exact-image symbols, and bare-metal GDB with
  unstripped binaries. The user accepted documentation as completion and
  deferred live core verification until a future incident; no new crash test
  or host configuration change is required in this task.

## Documentation and completion

- [x] **Bare-metal documentation**: publish verified KV, chunk and access
  setup under `/nv/cpp/crowdb-web/site/docs/`, state the non-production
  boundary, then fix website links and remove obsolete combined material.
  Keep Docker deployment notes independent; do not put these guides in crowdb.
  The KV, chunk and access guides, deployment index, navigation and sitemap
  are published in crowdb-web commit `1c6fc3c`. CLI command shapes were
  checked against the executable help and the website route/link test passes.
  Final R188 acceptance will verify the complete deployment path after the
  remaining hardware operations are wired.
- [ ] **Acceptance and cleanup**: run affected integration cases, full console
  and UI suites, Rust fmt and lint; update the relevant permanent architecture,
  then remove the requirement, backlog entry and this plan when complete.

## Evidence

- Bootstrap checkpoint passes complete KV client and Console shared/CLI/Web
  suites, five affected browser lifecycle/full-chain cases (53.8s), Rust fmt
  and workspace clippy. Logs: `/tmp/crowdb-cas-retry-{baseline,suite,lint}.log`,
  `/tmp/crowdb-bootstrap-confirmed-{console,ui,lint}.log`.
  Lost-response fixtures now advertise their RPC proxy through management
  topology, so discovery refresh cannot bypass the injected reply loss.

- Deletion reconciliation passes all six cases, including a real dropped
  metadata reply for both store and group deletion. Complete Console shared,
  affected Web migration/replica tests, fmt and clippy pass.
  Logs: `/tmp/crowdb-delete-reconcile-*.log`.

- Group publication baseline gives the group and initial replica different
  committed revisions (3 and 4), exposing partial publication on interruption.
  Conditional batch publication passes the shared-revision and lost-batch-reply
  tests, full Console shared tests, and Web restart/migration/replica tests.
  Orphan membership is rejected before local mutation; all three focused
  regressions, fmt and clippy pass. Logs: `/tmp/crowdb-group-publication-*.log`.

- Conditional publication baseline overwrites a competing store record and
  reports success. Both matching and conflicting race tests pass after the
  CAS change. A real RPC proxy discards the committed write reply; linearizable
  confirmation succeeds and exactly one reply is dropped. Full Console shared
  and CLI pass. Full Web passes after the restart-fixture correction below,
  including all five concurrent restart cases. Rust fmt and clippy pass.
  Logs: `/tmp/crowdb-publication-*.log`.
- The complete Console gate reaches a three-node restart failure: no complete
  store view within 3s. Its persisted identities are stable; node logs show
  repeated Group 0 elections and late registration, including election churn
  before restart. This real-process case uses the paused-clock `test` profile
  (5ms heartbeat, 30–60ms election) while all larger clusters use `e2e`.
  Exact isolation passes in 4.43s; default-concurrency rerun passes in 13.33s,
  and serial execution passes. Use the existing `e2e` fixture for the three-node
  process case and retain its 3s acceptance assertion. Add the last HTTP
  observation to store-wait failures, as already done for group waits.
  Original logs remain in `restart-3n-1g-20260927-234030.467` under the ephemeral
  Web E2E root; do not clean them during diagnosis.

- Replica cleanup passes four regressions: failed group creation cleans its
  newly created store, cleanup failure is explicit, existing replica hosts
  are rejected before mutation, and automatic identity exhaustion returns
  validation rather than panicking. Complete Console shared tests, Web replica
  tests, fmt and clippy pass; helper extraction also passes all nine focused
  creation/fan-out regressions. Logs: `/tmp/crowdb-replica-cleanup-*.log`.

- Deletion baseline fails three of four cases: later replica hosts are skipped,
  node-side failure reports success, and group deletion leaves replica records.
  All four now pass, including idempotent node-side 404 and preservation of
  sibling groups. Complete Console shared tests, affected Web migration/replica
  tests, fmt and workspace clippy pass. Logs: `/tmp/crowdb-delete-*.log`.

- Group fan-out baseline: both rejected remote wiring and missing peer endpoint
  returned success. Both failure-injection cases now pass, and the complete
  Console shared/Web gates passed for the group fix. Replica baseline adds
  three failures: missing peer registration/address reports success, and a
  missing new endpoint leaves its local group behind. Resolve all existing
  peers before mutation and roll back the target when its endpoint is missing.
  All five regressions pass, along with complete Console shared tests, affected
  Web replica/migration tests, six browser store/reconfiguration flows, Rust
  fmt and workspace clippy. Logs: `/tmp/crowdb-all-fanout-{shared,web,ui}.log`.

- Complete Web integration gate passes after stable identity persistence.
  Full Console UI passes all 86 component tests and 56 browser tests (4.6m),
  including all five original failures. Logs:
  `/tmp/crowdb-final-console-server.log`, `/tmp/crowdb-final-console-ui.log`.

- Discovery integration passes duplicate seed submission, invalid origins,
  exactly one live nonmember identity, no accidental membership, and restart
  with persisted hints. Rust fmt and workspace clippy pass.
- Affected browser cases now pass: shell dialogs (11.9s), shell health (3.3s),
  node inspection (8.3s), node cross-jump (2.7s), full-chain flow (4.8s), and
  all three canvas cases (3.2s / 0.8s / 4.2s). The canvas assertion is scoped
  to the main panel because the same unavailable text appears in a notification.
- Logical-tree hook regression passes confirmed reads, outage clearing, and
  recovery. Full KV Server and Console shared-library gates pass. Complete
  CLI/Web and browser gates remain pending for requirement completion.
- Full CLI passes. Web gate reached a failure in
  `cluster_restart_incremental_test::restart_6node_2group_overlap`: after all
  nodes restarted, group 11/1 did not converge to one leader within 3s. The
  unchanged exact test passes alone in 13.27s. Preserve the original timeout;
  compare the complete restart suite at default concurrency and serially,
  without concurrent release compilation, before attributing the failure.
  Logs: `/tmp/crowdb-discovery-console.log`,
  `/tmp/crowdb-overlap-restart-isolated.log`.
- Generated registration identity changed across a normal restart in the
  focused baseline (deterministic assertion failure). Persisting identity fixes
  that case and the missed-unregister case; changed explicit IDs, changed node
  IDs and malformed files fail closed. Full KV Server passes. The five restart
  cases pass at default concurrency after the fix (13.55s); before the fix,
  serial execution passed (44.73s) while default execution failed on different
  groups. No election timeout or retry count changed. Full Web still remains.

- Initial full Console UI baseline: 85 component tests pass; 51 browser tests
  pass and five fail. Missing live registration affects node 203 in shell
  replica creation and node 262 in node inspection. Two canvas assertions
  expect an empty-store view before Group 0 exists; the full-chain flow records
  logical-tree fetch errors during that same uninitialized phase.
- Isolated `12-cluster-node-inspect.spec.ts` reproduces HTTP 404 for node 262;
  one test passes and one fails in 25.2 seconds. This is not solely an ordering
  issue in the full browser suite. Keepalive uses its local bootstrap endpoint
  when launched without seeds; cluster initialization currently waits only for
  selected Group 0 members and does not propagate seeds to nonmembers.

## Tests

- Focused: KV server discovery/keepalive integration, console shared bootstrap
  and operation tests, affected shell/node-inspection/canvas/full-chain specs.
- Full: `pixi run clean-env && pixi run test-console` and
  `pixi run clean-env && pixi run test-console-ui`, sequentially.
- Crash diagnostics: monitor retention tests and disposable-container crash,
  collector/export and exact-build source-line symbolization acceptance through
  `pixi run test-monitor` and `pixi run test-single-node-container`.
- Style: `pixi run rs-fmt-check` and `pixi run rs-lint`.
