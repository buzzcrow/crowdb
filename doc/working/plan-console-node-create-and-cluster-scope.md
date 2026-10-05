<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Reliable node creation and Cluster scope Plan

Upstream: [R212](../backlog/R212-console-node-create-and-cluster-scope.md),
[R210](../backlog/R210-console-service-configuration-health.md),
[R211](../backlog/R211-console-access-health-listener.md).

Goal: make Add Node creation observable and retryable, then keep Chunk
ownership exclusively in the Chunk tab.

## Phase 1 — reproduce and define the request contract

- [x] **Capture the failing flow**: run the web console and record the exact
  node-registration or service-deployment response, including request order
  and persisted plan state. Files: `app/crowdb-web/ui/src/components/dialogs/AddNodeDialog.tsx`,
  `app/crowdb-web/src/services/deployment.rs`, focused browser tests.
- [ ] **Specify phase boundaries**: registration failure stops before service
  deployment; post-registration failures persist one service-scoped failure;
  retries reuse the node id. Files: node deployment route and service-plan
  hooks.

## Phase 2 — node creation and retry behavior

- [~] **Harden registration handling**: preserve the single dialog state,
  surface structured backend errors, and only advance to service deployment
  after a successful node response. Files: `AddNodeDialog.tsx`.
- [ ] **Make plan retry idempotent**: ensure selected/disabled services and
  overrides survive reload, and failed service retries do not call `/api/nodes`
  again. Files: `useNodeServicePlans.ts`, service-plan routes.
- [~] **Gate services on Group 0**: allow only PKV to deploy immediately;
  persist DiskDB and other selected services as queued and poll readiness every
  10 seconds before continuing. Files: `useNodeServicePlans.ts` and plan tests.
- [ ] **Add focused regression coverage**: cover registration failure,
  service failure, retry, and duplicate prevention. Files:
  `app/crowdb-web/ui/src/components/dialogs/createFlows.test.tsx` and
  `app/crowdb-web/tests/*`.

## Phase 3 — Cluster and Chunk ownership boundary

- [x] **Remove Cluster ownership rendering**: make Cluster center content
  topology/service-only and stop ownership probes for rack, node, and
  datacenter selection. Files: `app/crowdb-web/ui/src/shell/ClusterView.tsx`
  and ownership data hooks.
- [ ] **Add Chunk ownership hint and target view**: expose the hint on the
  Chunk left tree item and render details only after a real ChunkDB ownership
  target exists. Files: Chunk tree and ownership panel components.
- [ ] **Verify browser boundary behavior**: add or update Playwright cases for
  Cluster-before-ChunkDB and Chunk-with-target states. Files:
  `app/crowdb-web/ui/e2e/`.

## Verification

- Unit: `cd app/crowdb-web/ui && ./node_modules/.bin/vitest run src/components/dialogs/createFlows.test.tsx`
- Integration: `pixi run cargo test -p crowdb-web`
- E2E: `pixi run` with the console UI suite from `app/crowdb-web/ui/e2e/README.md`
- Gates: `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy -p crowdb-web --all-targets -- -D warnings`

## Files

- `app/crowdb-web/ui/src/components/dialogs/AddNodeDialog.tsx`
- `app/crowdb-web/ui/src/services/useNodeServicePlans.ts`
- `app/crowdb-web/src/services/deployment.rs`
- `app/crowdb-web/src/shell/ClusterView.tsx`
- Chunk ownership tree/panel and focused tests
