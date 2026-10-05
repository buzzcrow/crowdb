<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Node service configuration and health Plan

Upstream: [R210](../backlog/R210-console-service-configuration-health.md),
[R211](../backlog/R211-console-access-health-listener.md),
[R212](../backlog/R212-console-node-create-and-cluster-scope.md).

Goal: persist one six-service node plan, resume its dependencies safely, and
verify service health and the Cluster/Chunk boundary against regressions.

## Implementation

- [x] **Measure existing coverage**: run create-flow/planner UT and the exact
  rack/node and ownership browser specs before editing them.
- [x] **Isolate Access health**: add an independent configurable health listener,
  allocate and reserve its port, persist its launch contract, reject legacy
  restart records, and probe it independently from S3.
- [x] **Persist configuration**: store bounded service overrides with the plan;
  restore them on refresh and retain selected/disabled states and ports on retry.
- [x] **Unify node submission**: register once, await durable plan creation,
  remove direct deployments from AddNodeDialog, validate service-specific
  listeners, and keep registration/plan failures visible and retryable.
- [x] **Gate dependencies**: allow only PKV before Group 0; wait for live DiskIO
  ownership before ChunkDB; preserve disabled steps during observation failures.
- [x] **Probe internal health**: use FlatBuffer RPC health for internal services,
  independently from Paxos-KV monitor state and HTTP management listeners.
- [x] **Scope ownership**: show ownership hints and details only in Chunk with
  real ownership targets; assert Cluster selection sends no ownership probes.

## Files

- Access: `app/crowdb-access-server/src/{config,main,s3}.rs`, health module,
  `app/crowdb-access-server/tests/`.
- Backend: `app/crowdb-web/src/services/{plans,defaults,observation,deployment}.rs`,
  deployment launch, `app/crowdb-web/tests/`.
- Lifecycle: `lib/crowdb-console-shared/src/lifecycle/restart.rs` and tests.
- UI: `app/crowdb-web/ui/src/components/dialogs/AddNodeDialog.tsx`, service cards,
  `src/services/`, Chunk browser/tree, create-flow/planner tests and E2E specs.

## Verification

- Unit: focused Vitest create-flow/planner tests, then full frontend UT.
- Integration: console-shared, web, Access configuration/health and restart tests.
- E2E: focused rack/node and ownership specs, then `pixi run test-console-ui`.
- Gates: Rust fmt, affected clippy and `pixi run rs-lint`, `pixi run ts-lint`.
- Commit verified coherent implementation; delete completed requirements,
  backlog entries and this plan in a final cleanup commit.

## Results and remaining work

- Frontend UT: 153 passed. Focused real-backend UI: eight existing scenarios
  passed; two new ownership/Access configuration scenarios passed.
- Rust: Access configuration/service health, canonical service type and legacy
  restart, web health/lifecycle/plans, live DiskIO ownership all passed.
- Native ownership browser acceptance passed on a real three-node chain.
- Requirement functionality and focused regressions are complete. Rust fmt,
  workspace Clippy, TypeScript and tree lint passed. Full console tests passed.
- After requirements: inspect the supplied CI run, fix ordinary CI failures,
  then execute all nine local workflow jobs. Keep blockers as open issues and
  continue the remaining jobs as requested.
- CI repairs include ChunkDB rustdoc formatting, optional catalog/partition
  TypeScript narrowing, cancellation Group 0 fixtures, pre-store PKV health,
  and reset waiting for children already stopping in the background.

## Open issues

- **OPEN: S3 read after native six-service restarts returns 502.** Reproduced
  with `pixi run env CROWDB_NATIVE_UI_E2E=1 CROWDB_NATIVE_UI_E2E_GREP='Chunk ownership' cargo test -p crowdb-web --test native_cluster_provisioning_test one_rack_three_nodes_provision_all_services_without_metadata_repairs -- --ignored --nocapture`.
  Initial writes and reads succeed; restarting all six services on Node 1
  succeeds, then GET multipart.bin returns `502 Bad Gateway` after 4936 ms.
  Evidence: `.crowdb-runtime/artifacts/native-restart-failure-86554`.
  This blocks full restart acceptance, not isolated ownership UI acceptance.
  Skip this problem as requested; retain the failing test unchanged.
- **OPEN: remote CI logs unavailable without GitHub authentication.**
  Run 37274006189 jobs API is readable and reports failures in Lint, UnitTests,
  ServerTests, ConsoleTests and UITests. All five log endpoints return HTTP
  403; no GitHub credential is available. The issue is recorded locally and
  has not been published to GitHub. Continue with the corresponding local jobs.
