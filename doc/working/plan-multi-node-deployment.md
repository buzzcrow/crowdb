<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Multi-node Deployment Plan

Upstream: [requirement](../backlog/R227-console-multi-node-deployment.md),
[design](../design/deploy/design-crowdb-deploy.md).
Goal: system-Docker node containers discover peers and bootstrap/manage one
selected cluster from any UI, with durable identity and fail-closed authority.

## Review and scope

- Base: fetched origin/main at 435bee75f; branch task-deploy.
- Existing design edits preserved in stash and applied to the new branch.
- Runtime scope: Docker first; containerd and other OCI runtimes deferred.
- One image and shared monitor/UI implementation. Single-node is an automatic
  bootstrap/virtual-disk startup policy with read-only UI and backend enforcement;
  multi-node is manual bootstrap. Do not create parallel service stacks.
- Review gaps: system/init retries check only replica identity; preview starts
  services before UI; no node discovery management endpoint exists.
- Identity fingerprints must cover operation, cluster and complete initial
  membership, not merely local replica ID. Generic store/group handlers must
  not bypass system-resource ownership.
- Management grants need execution-time quorum validation and fencing; caching
  an earlier successful read is insufficient. Destructive cleanup needs durable
  terminal operation state and delayed-command rejection.
- SSH approval must validate monitor-to-host identity association; credentials
  never enter persisted drafts, logs or Group-0 public records.

## Tasks

- [x] **Node identity**: persist UUID outside preview bootstrap state, reject
  malformed/symlinked identity and preserve it on recreation. Files:
  container/crowdb-monitor/src/node.rs, node/identity.rs, tests/node_identity_test.rs.
- [x] **Discovery foundation**: management-interface-scoped DNS-SD, bounded
  candidate cache, conflict/foreign-cluster classification and read-only handshake.
  Verified actual loopback multicast discovery, cloned UUID conflict and goodbye.
  Files: container/crowdb-monitor/src/node/, lib/crowdb-protocol/src/mgmt/.
- [x] **Discovery completion**: seed fallback, dynamic cluster binding,
  physical hardware/rack handshake and real Docker bridge tests. Files:
  container/crowdb-monitor/src/node/, tests/.
- [x] **Bootstrap exclusion foundation**: operation/config identity, durable pre-store-0
  acceptance, all-member preparation and same-operation recovery. Files:
  app/crowdb-kv-server/src/mgmt/system_init.rs, recovery/, protocol management
  schema, lib/crowdb-console-shared/src/ops/cluster/bootstrap/.
  Verified concurrent prepare, persistence across restart, generic-path rejection,
  same-operation init retry and no init calls after one member rejects prepare.
- [x] **Bootstrap operation integration**: persist UI operation/config identity,
  route node startup through prepared bootstrap and add fenced terminal cleanup
  plus dynamic monitor cluster binding. Files: shared bootstrap, Web management,
  monitor node lifecycle and KV-server cleanup.
- [x] **Node mode and Docker SSH**: monitor/UI-first mode, persistent SSH identity,
  system Docker harness and independent roots/devices; reuse automatic single-node
  bootstrap as a startup policy and enforce read-only UI. Files:
  container/crowdb-monitor/src/node/, container/single-node-container/.
- [x] **Admission and authority**: authenticated SSH preparation, conditional UUID
  mapping allocation, operation progress and execution-time authority checks.
  Files: lib/crowdb-console-shared/src/ops/, app/crowdb-web/src/mgmt/.
- [x] **Candidate observations in Web/UI**: local-only bounded monitor proxy,
  standalone `--runtime-dir` and `--node-monitor`, node console mode and separate
  Cluster sidebar candidate list. Retain observations with a stale marker on
  monitor loss; discovery never changes racks or grants admission. Files:
  app/crowdb-web/src/node.rs, state.rs, main.rs, ui/src/shell/CandidateNodes.tsx,
  tests/node_candidates_test.rs, ui/e2e/flows/12-cluster-discovery.spec.ts.
- [x] **UI lifecycle**: admission and explicit initialization,
  publication/unavailable states, cluster selection and cleanup recovery. Files:
  app/crowdb-web/ui/src/, ui/e2e/flows/.
- [~] **Verify and clean up**: focused crate tests, Docker integration and affected
  UI specs, fmt/clippy, permanent documentation, then requirement/index/plan cleanup.

## Files

- Monitor: container/crowdb-monitor/src/, tests/, Cargo.toml.
- Protocol: lib/crowdb-protocol/src/mgmt.rs and owning children.
- KV server: app/crowdb-kv-server/src/mgmt/, recovery/, tests/.
- Console: lib/crowdb-console-shared/src/ops/cluster/, tests/.
- Web: app/crowdb-web/src/, ui/src/, ui/e2e/flows/, tests/.
- Packaging: container/single-node-container/ and pixi task definitions.

## Verification

- Integration: UUID persistence/conflicts; bootstrap races and restart;
  admission failure/retry; ownership unavailability and delayed commands.
- Docker: real multicast on a dedicated bridge; three node roots and UI ports;
  SSH trust survives replacement; system Docker only.
- E2E: candidates, racks, publication gating, multiple UIs and recovery.
- Gates: pixi run cargo test -p crowdb-monitor; affected crate tests;
  pixi run rs-fmt-check; pixi run rs-lint; affected Playwright specs.
- Server-spawning suites are prefixed with pixi run clean-env.

## Verification results

- Monitor full test suite passed after identity/discovery foundation (including
  actual multicast, HTTP handshake and all existing single-node tests).
- Workspace rs-lint passed for the discovery foundation; subsequent bootstrap
  changes require a fresh gate.
- KV-server focused clippy passed after system-store ownership changes.
- System Docker client/daemon available, version 28.3.3.

- KV-server system_init_test: 7 passed, including new prepare race/restart cases.
- Console bootstrap_intent_test: 5 passed; bootstrap_prepare_test: 1 passed.
- Protocol full suite passed; workspace rs-lint passed with bootstrap API and
  shared all-member preparation path.
- Discovery regression tests and focused monitor clippy passed after SIGTERM
  shutdown and multi-interface self-observation handling adjustments.
- Registry-owned bootstrap execution gate: system_init_test passed (7 tests).
- Web candidate proxy: node_candidates_test passed (3 tests), with local-origin
  validation and discovery before any KV server exists.
- Candidate browser test uses actual monitor and isolated Web roots, without
  response interception. Passed in 3.5 seconds; baseline 2026-10-10.
- Existing Cluster lifecycle spec passed (4 tests; 3.1s, 1.4s, 5.1s, 2.3s),
  within twice the freshly measured baseline.
- Rust fmt/workspace clippy and UI build/E2E TypeScript lint passed.

- Real Docker bridge acceptance passed: three independent volumes and monitors,
  no peer list, mutual Ed25519 SSH, identified bootstrap and all three UIs
  reading the same Group-0 cluster/topology. Later cleanup/admission extensions
  need fresh Docker verification.
- Full monitor suite passed after shared node discovery/automatic startup changes.
- KV system_init_test passed 8 tests including explicit cleanup, delayed-operation
  fencing, new-operation reuse and restart.
- Current additions: sealed bootstrap, private monitor control, SSH operation
  tagging/cancellation, Group-0 CAS admission allocation, UI retry/cleanup,
  per-node bindings and automatic single-node discovery/read-only enforcement.
- Remaining verification/implementation: cleanup with unreachable members,
  post-bootstrap admission/cancellation, quorum partitions and deployment grants,
  remote managed-service lifecycle, cluster selection/reconfiguration, production
  resource inputs, Docker host-network and single-node regression, full gates.

- Later-node admission exposed wildcard RPC listeners in discovered topology.
  Client normalization now resolves only wildcard local listeners through the
  reporting management origin. Real RPC regression topology_endpoint_test passed.
- system_init_test passed all 8 cleanup/exclusion cases after generic cleanup fencing.
- Focused Cluster lifecycle/discovery browser verification passed 5 tests (2.8s,
  1.5s, 5.1s, 2.5s, 2.7s); cluster filtering is covered.
- Service intents now persist in Group 0; target monitor validates current intent,
  retains execution, recovers committed requests and reports application probes.
  Cross-UI lifecycle, DiskDB and KV routing require refreshed Docker verification.
- Four-node Docker extension currently awaits rerun after RPC topology fix.
  Single-node full container regression awaits rerun with container-internal curl.

- Refreshed complete monitor suite passed. Explicit seed expiry and live bindings
  use real HTTP and multicast transports.
- Four-node bridge suite passed with application RPC health, shared service intents,
  cross-UI service lifecycle, minority management rejection and explicit cleanup.
- Host-network production launcher passed digest pinning, explicit secret mount,
  resource boundaries, bootstrap, container replacement and identity/key recovery.
- Native S3 multipart browser reproduction passed after restoring the KV client
  FFI build; the full console gate is running again.
- Bootstrap recovery now copies validated public fixed inputs and authenticated
  private service credentials to each participant before starting its KV replica.
  Other UIs recover the same operation independently of the initiating UI.

## Resumed verification

- Command: `pixi run bash -c 'export CROWDB_CONTAINER_IMAGE=crowdb-node:r227; bash container/single-node-container/tests/container-e2e.sh'`.
- Five root-cause-driven runs remain unsuccessful:
  - Initial discovery identity creation raced automatic bootstrap's empty-root check.
    Identity creation is now synchronous before the single-node bootstrap task.
  - The persistent-root symlink assertion ran as root after adding the SSH entry
    point. It now checks using the container's service user.
  - The single-node internal Access health listener required container-local curl;
    curl is now packaged and that private listener is probed inside the container.
  - A KV client build omitted `ffi`, removing DiskIO C ABI symbols. Packaging now
    preserves that feature and checks unresolved symbols before image assembly.
  - The fifth run passed boot, client writes/readback, browser checks, crash/hang
    recovery for every service, persisted-volume restart and restart-budget checks,
    then failed the corrupt-manifest diagnostic assertion.
- Exact final boundary: `verify_invalid_manifest_rejected` observes nonzero
  container exit, then requires `docker logs ... | grep -F 'Manifest('`.
  Actual log: `Error: "preview manifest failed: bootstrap manifest cannot be decoded: expected value at line 1 column 1"`.
  The invalid manifest is rejected; the old diagnostic spelling is absent.
- Root cause: the same-image single/manual command wrapper now returns the
  contextual display error rather than the former debug enum representation.
  After user authorization, the assertion retains nonzero exit and checks this
  concrete decoding diagnostic. The invalid-profile case similarly checks its
  contextual decode diagnostic.
- Proposed continuation: retain the nonzero-exit assertion, match the concrete
  manifest decoding diagnostic, rerun the complete container suite, then finish
  multi-node browser/recovery coverage, module/documentation cleanup and full gates.
  Alternative: restore the former CLI diagnostic representation, preserving the
  old test spelling, then repeat the same acceptance gate.
- The implement-requirement skill requires recording/committing this state and
  requesting a human decision after five failed root-cause-driven attempts.
- R227 remains open. Recent full workspace clippy passed; focused KV bootstrap
  tests passed (8), the prior four-node application-health suite and host-network
  replacement suite passed. Latest monitor/protocol rerun is finishing. The full
  console gate was stopped before completion; full console UI has not run.
- All implementation edits are retained in the worktree. The most recent browser
  coverage and cross-UI manifest recovery additions still require execution and
  must not be described as verified or complete.

- User confirmed continuing R227; R228 is unrelated and remains untouched.
- Latest monitor and protocol full rerun completed successfully.
- Latest release build and workspace rs-lint passed. Full test-console is running;
  shared-console and CLI tests passed and Web tests are in progress.
- First resumed single-node run failed because the shell source was edited while
  running, producing an unexpected EOF. Run a fixed temporary script copy and
  pin the image digest for the next full execution.
- First real Docker UI run found missing static assets: manual Web startup omitted
  the profile's UI directory. Added explicit --ui-root and Web log-directory
  arguments; image rebuild and browser rerun are pending.
- Monitor acceptance now distinguishes reserved identity from complete preparation.
  Automatic KV startup waits for complete manifest/credential persistence.
- Added Group-0 regression cases for bootstrap registry retries preserving later
  admissions, service retries retaining operation identity, and cross-node service
  ID collision rejection. Their focused execution is pending.
- Remaining contract gaps to implement/verify: authenticated endpoint/rack updates
  with stable IDs; cancellation after target KV preparation; interrupted bootstrap
  recovery from a second UI; disjoint-cluster cleanup and voting catch-up; data-group
  operation during Group-0 loss; partial cleanup with unreachable nodes; final
  permanent docs and full gates. R227 must remain open until these are handled.

## Resumed browser verification

- Remove post-bootstrap admission's repeated handshake between StartKv and
  ProvisionServiceCredentials using an ordered remote_controls session. Bind
  remains after Group 0 confirmation. Initial password-session failures close
  their connection explicitly. SSH directional proofs allow 10 seconds for
  connection and 20 seconds overall, under the user's timeout authorization.
  Final focused Clippy, fmt-check, deployment_test (4 tests), release build
  and complete real Docker rerun passed. The rerun exercises three-node browser
  preparation/bootstrap, fourth-node interrupted admission cancellation/retry,
  cross-UI services, data quorum under Group 0 loss and explicit cleanup.
  Image sha256:50ad2e212287cbe0c102f4a4a063c694f0408a3b9cf87ca5f7a9b14e59b40066.
- Nonzero data groups use confirmed management endpoint hints and their own
  Paxos authority without a Group 0 lookup for each data operation. Peer wiring
  resolves wildcard RPC addresses against each reporting management origin.
  Shared mgmt_e2e/validation tests, focused Clippy and fmt passed. Real Docker
  verification passed three-node data quorum and a surviving single-voter
  data group while Group 0 loses quorum, together with admission cancellation,
  cross-UI lifecycle and cleanup. Image
  sha256:4ad76f601fc1be8240d6a1b1f928afa1d820afd12e248db4c08d9353aec8eb65.

- User authorized continuing SSH session reuse with the existing UI and 3-second
  assertions. The previous five-attempt blocker below is retained as history.
- User subsequently authorized a wider SSH budget after unnecessary overhead is
  removed. Admission dialog completion now has a 10-second budget; ordinary UI
  assertions remain 3 seconds. Leader election is observed separately under the
  existing 10-second election budget.
- Reuse one key-authenticated target session and one session per peer; prove both
  SSH directions concurrently while retaining strict host checking and UUID checks.
  Focused shared/monitor/Web Clippy and release builds passed.
- First resumed browser run passed preparation of all three nodes, then failed
  initialization. Trace showed the default selected only one prepared node and
  the KV keepalive published its loopback management endpoint across containers.
  Correct prepared-node defaults and explicitly advertise the monitor's management
  interface address in the KV service registry. Assertions now check every initial
  checkbox is selected. No timeout increase.
- Full single-node container regression passed: browser, recipes, process crash/hang
  recovery, persistent restart, budget exhaustion and invalid durable inputs.
- Browser/Docker baseline passed with session reuse: all three prepared nodes are
  selected; reachable KV service advertisement; three-UI shared topology; fourth
  node admission; remote service lifecycle; minority gating; explicit cleanup.
  Image sha256:f5f1c042dcfa0f338b36bae92db9412277dca0acd2b39af5dfe0556dde2b7a49.
- Monitor complete suite including doc-tests passed. KV group0_discovery_test
  passed with distinct advertised/listening address and restart registration.
  Workspace rs-lint passed. Native page/Iceberg reproduction passed, including
  21 browser diagnostics after real production balancing (570.59 seconds).
- Active cancellation completion: StartKv carries a Group 0 admission operation;
  monitor validates cluster publication and current mapping before preparation.
  CancelAdmission validates the committed tombstone, writes the local fence before
  stopping KV/clearing preparation and preserves the parent cluster's bootstrap.
  Registry blocks reuse until target and SSH cleanup are complete. Shared
  deployment_test passed 4 tests. Real Docker interruption coverage passed: prepare
  KV, cancel through another UI, reject delayed StartKv, and retry with the same
  numeric ID/new operation. The whole four-node service/partition/cleanup suite
  passed with image sha256:f1153319558aeb0e109dc93a9703acb0ceef46e228d26938eac5ce437b630c69.
  Two test corrections preceded the pass: authentication error asserts the actual
  500 response and diagnostic; minority status polls the existing authority lease
  expiry before asserting management rejection.

- Resumed Docker command: `pixi run bash -c 'export CROWDB_CONTAINER_IMAGE=<pinned-image-id>; python container/single-node-container/tests/node-containers.py'`.
- Five root-cause-driven Docker browser runs failed after user authorization:
  1. Manual Web startup omitted the static UI directory. Added --ui-root.
  2. The admission dialog was trapped in the fixed sidebar stacking context.
     Moved admission/recovery dialogs into a portal.
  3. Preparation restart coverage used the previous randomly published host port.
     Refresh Docker's actual port mapping after restart.
  4. The body portal escaped the console's scoped CSS. Portal now targets the
     nearest .crowdb-console root, retaining embedding styles and ownership.
  5. Real mutual SSH preparation for the third node exceeds the 3-second UI
     assertion budget. No assertion timeout increase or sixth run was made.
- Latest image: sha256:66c08eb74c3daeec89ebfab6a52b54e3cdb486e592f848a656c1271614eb5ac2.
- Exact latest boundary: 12-cluster-discovery.spec.ts:91 expects the admission
  dialog count to reach zero within 3000 ms; it remains open with Verifying SSH.
  Trace records successful first/second admission responses at 368 ms / 2048 ms;
  the third request has not completed when the assertion ends. This establishes
  the first slow boundary, but does not prove whether the third operation would
  eventually succeed.
- Proposed continuation: instrument the SSH preparation stages, reuse authenticated
  sessions for each peer's key/host setup, and avoid repeated handshakes while
  preserving all-pair bidirectional strict-key proof. Rerun the same browser gate
  at its existing assertion budget, then finish the remaining contract gaps.
- Alternative: expose admission as a durable asynchronous UI operation with visible
  per-peer progress, testing the pending state and final admission separately.
  This expands the UI/API implementation but supports larger clusters naturally.
- Required human continuation follows implement-requirement's five-attempt rule.
  Implementation edits and regression assertions remain preserved.
- Other gates: deployment_test passed 4 tests; workspace rs-lint passed; monitor
  runtime/integration suites passed and isolated monitor doc-test rerun passed.
  Full test-console failed native balance seed_values with Deadline; isolated
  reproduction is running. Full UI has two failures so far; duplicate-rack passes
  alone in 1.7 seconds. Single-node container regression is still running through
  crash/hang recovery with its pinned image and fixed script copy.


## Final acceptance closure

- Authenticated node-update implementation now keeps a Group 0 journal with fixed
  source/target records, preserves IDs/physical host, fences rack membership and
  rejects moving nodes that still have disk groups. It updates canonical mapping
  and RPC wiring without changing voting rights; cleanup uses current endpoints.
  Focused deployment/replica fanout/cleanup suites passed 14 tests.
- New replica admission imports snapshots and catches up WAL before promotion.
  Identified system join owns store 0 before creation; generic system join cannot
  bypass ownership. Ambiguous promotion preserves the caught-up replica for retry.
- New real Docker cases cover cross-UI bootstrap recovery, stale service intent,
  foreign cluster selection/explicit cleanup, changed IP/rack with stable ID,
  snapshot/WAL voter admission, and unreachable-node cleanup retry. Fresh run pending.
- Permanent deployment/console/config/Group 0/container docs updated to supported
  Docker behavior. Missing R210/R211/R212 details are confirmed completed/deleted
  in main history (a1878085b, merged 0b08a196a); removed only stale index entries.
- Current release/image and UI build/TypeScript checks passed. Image
  sha256:e4a4eaff0d7f6e25a249242acf85f3ea9f4d0e8c38b71fafab0565ec10742ef7.
- Remaining gates: fresh Docker closure, host resources/recovery, KV/protocol/monitor
  affected suites, full test-console and test-console-ui, fmt/workspace Clippy.

- First final Docker run passed browser preparation, cross-UI fixed bootstrap
  recovery, admission cancellation and stale service-intent rejection, then failed
  minority status with an HTTP timeout. The first publication read was bounded,
  but a subsequent registry read/reload was unbounded if authority expired between
  steps. Bound the complete confirmation and configuration refresh; retain the
  minority assertion and rerun the same harness.

- Second final Docker run confirmed bounded minority status and independent data
  authority, then passed the foreign-cluster browser cleanup. Its delayed-command
  assertion supplied no manifest/credentials and therefore tested request shape
  validation instead of retirement. Capture the original valid inputs before
  cleanup and replay that complete command; retain explicit retirement assertion.
- Confirmed admission retry only resumes binding. It does not recreate hardware
  after an authenticated rack relocation; Group 0 update records fence pending
  admission while relocation is incomplete.
- Fresh monitor/protocol full suites and KV discovery/snapshot-join/system-init
  suites passed; fmt passed. Full console/browser gates remain pending.

- Third final Docker run passed complete old-command retirement and explicit
  independent-cluster cleanup/admission. IP-change fixture failed before exercising
  CROWDB because Docker requires a user-configured subnet for --ip. Create the
  dedicated bridge with an available explicit subnet (check existing Docker IPAM)
  and retain the changed-IP assertion. No product timeout/assertion weakening.

- Fourth final Docker invocation failed during fixture setup: Docker host/none
  networks return null IPAM.Config. Treat it as no subnet and only remove the
  bridge after successful creation. No node/product code ran in this invocation.

- Fifth final Docker invocation passed the complete five-node bridge harness with
  image sha256:d4041d6cbbbbdcb14639748535b33ebaf6020909e54447d61597afab6225b8e6:
  actual browser preparation (7.5s), cross-UI fixed bootstrap recovery (4.7s),
  partial KV admission cancellation/retry and delayed-command rejection, stale
  service intent rejection, cross-UI DiskIO lifecycle, independent data authority
  under Group 0 loss, foreign cluster selection/explicit cleanup (2.4s), complete
  old-command retirement, cleaned-node admission, authenticated IP/rack update
  preserving UUID/numeric ID/physical host/public key (4.9s), snapshot/WAL catch-up
  before voting for Group 0 and a populated data group, old-data readback, and
  cleanup retry after an unreachable node returns. All test-owned containers,
  volumes and bridge were removed. No active Docker acceptance blocker remains.
- Latest workspace rs-lint and rs-fmt-check passed. Full test-console is running;
  shared-console integration stages so far passed. Host-network acceptance and
  the full console browser gate remain required before final completion.

- Fresh host-network run passed first bootstrap and explicit CPU/memory checks,
  then failed the persistent-recreation active-state assertion (authority_unavailable
  after its existing ten-second budget). Add retained logs/topology diagnostics and
  reproduce the exact boundary before changing behavior or assertions. Docker
  bridge acceptance remains passed; host recovery remains unverified this round.

- Restart failure diagnosis: host-network acceptance was incorrectly run alongside
  test-console's simulated S3 cluster. Its retained s3-local-state.toml/namespace
  assigns management 10000 and RPC 10100, the same host ports as the Docker node.
  The standalone host Group 0 (replica 1, no remotes) reported leader 2 and recent
  foreign heartbeats; S3 node-2/node-3 retained endpoints point to 127.0.0.1:10100.
  Host recreation occupied S3 node-1's freed ports during restart, causing both
  authority loss and missing S3 registrations. Confirmed fixture interference,
  not a basis for weakening election/registration checks. Stop disposable native
  services with clean-env and rerun host then test-console serially. Retained
  host diagnostics: .crowdb-runtime/artifacts/crowdb-host-test-efab443d.

- Host-network acceptance passed unchanged election/active-state assertions after
  clean-env and serial execution, using the same d4041d6c image: rejected memory
  beyond host capacity; applied 2-CPU/1-GiB limits visible in handshake; explicit
  private password mount; bootstrap; external container recreation; stable UUID,
  Group 0 cluster identity and Ed25519 public key; explicit cleanup. The two earlier
  host failures were fixture port interference, confirmed by retained topology.
- Restart full test-console alone after host completion; no concurrent host-network
  test or full browser/native fixture may run while it owns fixed service ports.
- Split the bridge acceptance orchestration into bootstrap, admission/service,
  and recovery scenarios without changing assertions. Python compilation passed;
  rerun this layout against the same pinned image after the native gate completes.
- Release-policy gate exposed a pre-existing literal CI branch-list check: main
  already also enables codex/deploy. Allow additional branch entries while still
  requiring main and release/**; the complete release-policy gate now passes.
- Image-smoke passed against the pinned d4041d6c image, including size, labels,
  packaged dependencies, profile validation and missing-Iceberg-config rejection.
  Shared-console and CLI full gates passed; Web cancellation/restart/managed-mode
  and normal three-node service provisioning passed. Page/Iceberg inspection and
  later diagnostic fixture phases remain running in test-console.
- Serial full test-console passed, including all shared/CLI/Web tests, the
  560-second Page/Iceberg fixture with 21 browser passes and three dedicated
  prerequisite/Journal/split browser scenarios. Final production count/transition
  acceptance passed in 353.56 seconds. Full test-console-ui starts separately on
  port 4293 with a dedicated output directory after clean-env.
- Full test-console-ui passed: 171 unit tests and 52 browser cases; four Docker
  scenarios require the bridge harness and are skipped by this ordinary fixture.
  Earlier duplicate-ID/full-chain/default six-service failures did not recur in
  the serial full selection. Kept ordinary three-second assertion budgets.
  Recorded individual Docker scenario baselines separately from local discovery.
  Fresh bridge replay starts against the same pinned image after clean-env.
- Refactored five-node bridge harness passed in full with the same d4041d6c
  image. All four real browser scenarios passed (7.1s / 4.9s / 2.4s / 4.9s),
  as did data authority during Group 0 loss, admission/service fences, stable
  endpoint/rack updates, snapshot/WAL-before-voting and unreachable-node cleanup
  retry. Owned Docker containers, volumes and bridge were removed.
  Fresh full single-node crash/hang/persistent-replacement regression starts next
  against that identical image; host-network acceptance already passed.
- Latest-image full single-node container E2E passed: interrupted/empty boot,
  read-only browser, AWS CLI/rclone and protocol layout checks, all service crash
  and hang recovery, persisted-container replacement/readback, restart budget,
  secret-free lifecycle logs, changed identity/corrupt manifest/invalid profile
  rejection, anonymous volume startup and monitor death. All owned test resources
  cleaned. Final C++ lint, topology endpoint regression, documentation tests and
  TypeScript lint run before implementation and cleanup commits.
- Final gates passed: tree-lint exited 0 (existing unchanged C++ warnings),
  TypeScript lint, remote wildcard-endpoint regression, shared/Web documentation
  checks and Python compilation. Workspace Rust fmt/Clippy already passed for
  the current production changes. Staged diff whitespace check passed.
  Aligned the harness fallback and manual README example with the standard
  build's development tag; all real acceptance runs pinned the image digest.
- Implementation and all required acceptance are complete. Remaining action is
  the final requirement/index/plan removal in its separate cleanup commit.
