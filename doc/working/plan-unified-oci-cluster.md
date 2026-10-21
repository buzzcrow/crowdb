<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified OCI Cluster Implementation Plan and Handoff

Requirement: [R243](../backlog/R243-deploy-unified-oci-cluster.md).
Goal: ordinary OCI lifecycle, authenticated rediscovery, current layered
readiness, attached workloads and scoped cleanup.

## Current handoff

- **R243 and end-to-end fault recovery remain incomplete.** Do not remove the
  requirement/backlog entry or claim that the full fault acceptance passed.
- User requested shutdown after the problem is fixed. **No shutdown scheduled**:
  Access still prevents recovery to Ready after the valid all-DiskIO outage.
- Branch `task-stable`; local commits only, no push authorized.
  Storage fixes: `532af33e`; common CLI fixture: `be0efd32`.
- All owned containers and labeled bridges are cleaned. Failed fixture data/logs
  and earlier retained J/L data remain preserved.
- Preserve user edits in `container/single-node-container/dev-container.sh` and
  `e2e-test/clients/ecosystem/README.md`; exclude them from requirement commits.
- No compatibility requirement. RDMA uses host networking.
- [~] **F02 — Restore current Access readiness after backend recovery**.
  First capture `/_crowdb/health/ready` flags and their exact failure reasons.
  `ProductionS3Operations::record_dependency_outcome` persists metadata/chunk
  Unavailable after failed requests; only matching successful requests reset them.
  Web `node/observation/protocols.rs` skips protocol reads unless the Access
  service is already Ready. This creates a recovery feedback risk; the precise
  stuck flag still needs independent confirmation from the dedicated endpoint.
  Implement bounded read-only dependency observations independent of prior
  request outcomes; expose exact metadata/chunk/auth/cleanup failures. Preserve
  explicit stop/delete and ownership fences. Do not merely clear flags, force
  Ready, restart a healthy Access process, or add mutation/probe objects.
  Files: Access `s3/{operations,health}.rs`, shared S3 `metrics.rs::S3Health`,
  Web `node/observation/protocols.rs` and owning tests.
  Regression: dependency error -> backend recovers -> Ready without user traffic;
  actual unavailable dependencies and operator-stopped services stay non-ready.
  Then rerun the retained-data fault scenario with the existing 180-second budget
  and required old/new object reads on every member. Those reads were not reached
  in the failed all-DiskIO case and must not be counted as verified.

## Blocked

The requirement workflow requires confirmation after five source-driven failed
end-to-end recovery rounds. Harness setup failures are excluded from this count.

- M: DiskDB restart succeeded; terminal ChunkKV handles never replayed.
- N: writer restart missed retained split compatibility handles.
- O: restart exposed competing dead-owner transfer fencing an active split.
- P: transfer guard exposed false unavailable markers on acknowledged WAL prefixes.
- Q: false markers removed; real writer restart/replay and owned ranges recovered,
  but Access remained Initializing and aggregate check exited 1 after 180 seconds.
- Failing case: `console::cli_container::terminal_chunk_writer_recovery`.
  Command: `pixi run python .crowdb-runtime/artifacts/r243-q-chunk-kv-recovery/run.py`.
  Inner failure: `cluster check --wait 180 --json`, phase `degraded`,
  `required-service-not-ready` for `node/3/service/access-3` and all its protocols.
- Recommended continuation: F02 active read-only Access dependency checks, with
  exact diagnostics and a recovery regression. Traffic-driven recovery or clearing
  health flags without fresh evidence would hide the current readiness defect.
- Continue only after the workflow's retry-limit confirmation; then finish the
  user-requested verification, cleanup and shutdown. No product design decision
  is otherwise pending.

## Remaining implementation tasks

Only mark tasks complete after their acceptance is registered and verified.
Existing implementation can be reused; these are the outstanding parts.

- [ ] **T04 — Finish common target coverage** (A14/A28).
  Named/endpoint-only UUID-checked targeting and native signed S3/workloads work.
  Remove remaining legacy logical-command address/registry fallback with T29.
  Owners: CLI `commands/cluster/target.rs`, `commands.rs`, shared deployment target.
- [ ] **T05–T07 — Finish formation negatives and interruption coverage**
  (A03–A05/A19–A21). One/three-node normal startup works. Verify missing/extra/
  duplicate/foreign peers, incompatible credentials/policies, simultaneous
  coordinators, sealed-manifest conflicts and lost replies without rebootstrap.
  Owners: Web auto-join/initialization, shared bootstrap/admission, monitor tests.
- [ ] **T15 — Complete editable startup acceptance** (A01/A02).
  Management starts while setup is pending. Migrate remaining readonly/legacy
  callers; verify authorized edits and actual rejection causes in both topologies.
- [ ] **T15a–T15d — Finish member trust and retained recovery** (A32/A33).
  Initial authenticated member pins, SSH-verified rediscovery, duplicate live
  identity rejection and identity-bound replica routing are implemented. Fresh
  three-node Docker changed-IP recovery with retained S3 bytes passed.
  **Gap:** later non-voter admission/revocation trust distribution and non-voter
  changed-IP recovery. Current trust file is immutable initial membership;
  `recovery::accept/check/ready` special-case admitted nodes and discovery loads
  the initial PreparedBootstrap. Extend only through confirmed Group-0 authority
  and admission operation/host-key pins, never discovery TOFU. NodeRegistry does
  not currently store the authenticated host key. Preserve retirement fencing.
  Verify wrong key/bootstrap, cancellation, copied IDs and missing/corrupt trust;
  add actual S3/Iceberg/Dataset and host-network recovery coverage.
  Owners: shared `deployment/recovery*`, `registry.rs`, Web `node/admission.rs`,
  monitor `node/runtime/{admission,recovery}*`, KV identity-route tests.
- [ ] **T16 — Complete current dependency acceptance** (A15/A23/A26/A27).
  Group0/disk+binding → owner → space layers, current operations, stateless idle
  ChunkKV and owned-range writer readiness exist. Finish service stop, voter loss,
  stale evidence, mixed failures and bounded endpoint checks with non-repair proofs.
  F01 addresses an uncertainty/readiness gap; do not conflate free bytes with
  allocatability. Preserve detailed successful facts as well as errors.
- [ ] **T19–T20 — Add common CLI host/containerd lifecycle** (A08–A10/A29/A31).
  Docker bridge lifecycle/ownership receipts work. Common CLI still lacks host
  and containerd adapters. Reuse production resource/device/port/interface/
  credential validation from `container/oci-image/{run-node.py,runtime.py,storage.py}`.
  Seal daemon/context/endpoint, exact resources/mounts/image and owner before
  effects; namespace is a backend detail, never application cluster identity.
  Verify applied cgroups/host network and one node per physical host.
- [ ] **T25 — Register direct Docker/Compose reference acceptance** (A01/A03/A25).
  Examples/helpers exist. Execute documented one/three-node commands and Compose
  without host bootstrap helpers, with published access to all three protocols.
- [ ] **T29 — Finish removing legacy public paths and migrate their callers**
  (A10/A12–A14/A17).
  - Still public: `launch`, `--registry`, legacy system-address fallback,
    `cluster init/local-deploy/destroy/clean`, `kv server`.
  - Profiling, benchmark and sanitizer scripts still call old config/reset/
    deployment APIs. Preserve useful measurement purposes in narrow test-owned
    fixtures before removing the executors; no product compatibility aliases.
- [ ] **T30–T33 — Complete registered acceptance ownership and CI**.
  Register A01–A33 in owning Rust/Python/container/UI suites; mocks and manual
  observations do not substitute for actual runtime coverage. Direct/Compose,
  isolated names, missing peers, interruption/lost replies, changed UUID/context/
  ownership, unrelated/shared resources, scoped cleanup, all attached workloads,
  current health/quorum and actual authorization need traceable owners.
  Wire the ignored `cluster_workloads_test.rs` pending-free accounting regression
  explicitly using `CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT`; default ignored
  execution is not coverage. `test-container-e2e-fixture` currently has 159 cases;
  the case inventory checker is not evidence that container flows ran.
- [ ] **T34 — Verify host runtimes honestly** (A08/A09/A31).
  Docker-hosted containerd profiles are separate from native Docker-free A31.
  Native rootful containerd is unavailable locally: socket root:root 0660;
  `sudo -n` requires a password. nerdctl 2.4.1 executes, which proves only tool
  availability. Provide an actual Docker-free native runner and record artifact
  digest/runtime-local image ID, hosts and results; do not count skips as passes.
- [ ] **T35 — Final documentation/gates/cleanup** (A17/all acceptance IDs).
  Update deployment/OCI designs, help, examples, credentials/ports, tool guides
  and CI only for verified behavior. Old standard/single/bare-metal registry
  descriptions still need migration. Run applicable full gates, then remove
  requirement, backlog row and this temporary plan in the final cleanup commit.

## Completed foundation (brief)

- Storage fixes: uncertain DiskDB gates stay closed until a new fenced process
  reconstructs allocation state; diagnostics distinguish Unavailable/NoSpace.
  ChunkKV restarts for terminal uncertainty, including retained split writers.
  Active splits exclude competing dead-owner transfers. Mirror repair exhaustion
  preserves acknowledged prefix locations; ordinary I/O errors never become
  corruption markers. Reader CRC and corrupt-replica protection remain unchanged.
- CLI recovery fixture uses common start/stop, fixed accepted state and containers,
  signed retained S3 I/O, read-only checks and independent scoped cleanup.

- **T01–T03, T08–T14:** durable startup identity/policy, default storage and metadata
  setup, monitor-owned services, access listeners and credentials.
- **T17–T18:** layered current readiness; bounded JSON status/check and exit codes.
- **T21–T24:** six OCI shortcuts; independent Docker stop/clean preserves data by
  default, removes unused owned bridges and handles owned image garbage.
- **T26–T28:** common Docker CLI lifecycle, native signed S3, attached workloads
  and separate RPC tooling. Pending-free accounting correction is verified.
- Preserve data safety: never truncate below acknowledged cursor; distinguish
  missing/superseded execution; retain exact monitor observation failures.

## Verification and retained resources

- Passed: full DiskDB, ChunkKV server, KV server, Chunk client and stream suites;
  DiskDB process-session tests;
  focused uncertainty, actual Unavailable/NoSpace RPC and post-split compatibility
  writer and acknowledged-prefix regressions; five affected packages pass all-target
  clippy and fmt; 159 Python fixture tests.
- Image N: both registered `console.cli.container` interruption/lifecycle cases
  passed. Image Q leader-pause case passed: all three DiskDBs restarted, UUID and
  containers stayed fixed, old/new S3 reads passed on every member, and a strict
  normal DiskDB mix passed. Evidence case: `crowdb-e2e-9df33e1979fb4b7aae178f91ca93cae6`.
- Current image: `crowdb-node:r243-q`; archive `target/crowdb-r243-q.oci.tar`.
  Source tree `e9cc4a3f8d31cefbbf2da7e447fb4ab2015ce289` at `eddecc03`;
  production files match fix commit `532af33e` (verified by tree diff);
  OCI `sha256:aa00679345d7ab4fc4a735177d5963735ff152f356b388e156217841e0b540ea`;
  Docker `sha256:5937f17bd9b83d8d4efc90d8beaaee8240dafc7fe31b0c278f108f6846b719a9`.
- Current evidence: `.crowdb-runtime/artifacts/r243-q-uncertain-recovery/` and
  `r243-q-chunk-kv-recovery/`. Each contains `run.py` and per-case logs; the passed
  DiskDB case has `recovery-proof.json`. Valid failed Access case:
  `crowdb-e2e-2bcc4a08714c4c35acdd5b0d132cb108`, cluster UUID
  `385fe4c4-3129-4474-97be-2bb78772cc24`. Its final observations show catalog
  generation 23 and all eight owned ranges Ready; `access-3` remains Initializing.
  It uses executable-checked SIGSTOP/CONT as user `crowdb`, verifies state `T`,
  and holds all three DiskIO processes for 40 seconds, restoring them in `finally`.
  These explicit fault scripts are manual verification;
  registering sustained fault coverage remains T31/T33 work.
- Registered CLI selection: `console.cli.container` (two cases). Full `console.cli`
  requires the regular client bundle including `domain-test`; an ad-hoc
  `target/debug` bundle supports only the migrated container CLI cases.
- Earlier diagnostics remain under `r243-m-*`, `r243-n-*`, `r243-l-address-recovery`
  and `r243-l-current-health`. They are evidence only, not the current fix image.
- Retained L volumes/receipts: `r243-l-one`, port 21600, UUID
  `1d0f87cb-164d-4c22-8905-4fbcff9cacce`; `r243-l-three`, ports 21700/21710/21720,
  UUID `fa9b326e-7d0e-4173-83d6-ddaacd0fdb2f`. Containers and bridges cleaned.
  Acknowledged `r243-recovery/payload.bin` is 2 MiB with SHA256
  `91d3beb88a9b2f778a6c44a1c53b63d3c79931845a9aef84b3fb414610bd1938`.
- Failed fixture volumes and receipts remain under each case's
  `nodes/runtime/persistent/oci-clusters/`. Use that runtime root with independent
  `docker-local.py clean`; never fabricate receipts or broadly prune Docker.
  Preserve older J data/receipts too. Successful disposable fixtures delete their
  own data and containers/bridges through the independent helper.

## Open questions

- No unresolved product choice. Continuing past the retry limit needs user
  confirmation under the implementation workflow.
- login1 `CanPowerOff` is `yes`; shut down only after F02 and its verification pass.
