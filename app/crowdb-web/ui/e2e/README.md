<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI acceptance

Behavior authority: [Console UI specification](../../../../doc/design/console/design-crowdb-console-ui.md).

## Acceptance policy

- E2E must use real services, APIs, persisted metadata and file bytes. The shared
  fixture rejects Page/Context routing and HAR replay. Mocked scenarios remain
  unaccepted until real equivalents exist; see [UI tasks](../../../../doc/working/ui-todo.md).
- Collection counts do not imply native coverage or passing acceptance. Existing
  page-only configurations and mocked observations below describe historical
  coverage; they cannot satisfy the current acceptance policy.

## Layers

- Page behavior: deterministic large windows, selection, shared layout, failure
  and stale generation fixtures. `pageBehavior.config.ts` selects Chunk,
  Chunk-KV, Iceberg and S3 page-function specs. These establish browser behavior,
  not native parser, routing or data durability acceptance.
- Real management/data: `realBackend.config.ts` owns one isolated mutable Web
  runtime and one worker. Test-mode children live in a disposable namespace;
  global teardown and SIGTERM await stop before deleting their files. Cluster lifecycle, KV membership/CRUD, Capacity and
  cross-links must use actual APIs. Remaining observation mocks require migration
  before those scenarios can pass.
- Native service chain: `managedNative.config.ts` uses an explicitly supplied,
  isolated populated deployment. It verifies shared UI against actual KV,
  Iceberg, S3 and Chunk services and backend hardware capability rejection.
  `nativeS3.config.ts` separately covers large native multipart transfers.
- Backend integration covers provisioning, authority/persistence, owner routing,
  parser byte limits and generation-safe inspection. Passing page mocks does not
  satisfy those contracts. SF=1 loader acceptance belongs outside routine UI runs.

## Ownership of assertions

- `00–01`: embedding, modes, shell, dialog defaults and shared tree behavior.
- `10–13`: Rack/Node creation, six-service plan, exact lifecycle and cross-links.
- `20–22`: Store/Group/Replica management, quorum and membership recovery.
- `30–31`: 20-entry replacement windows, full key/value, binary encoding,
  read-only system Group 0 and real user CRUD.
- `40–41`: activity and graph pan/fit/collapse; right-click preserves expansion.
- `50–53`: DiskGroup/disk lifecycle, owner/binding, 32-zone windows, bitmap and
  partial/unknown capacity. Required ordinary data groups are prepared explicitly.
- `54–55`: 10-chunk pages, Strip/block layout, hierarchy and bounded Split graph,
  exact identities, journal paging and stale continuation.
- `60`: current Catalog/Namespace/Table/Snapshot/Manifest/File tree, nested
  schema, independent reference pages, properties, shared Actions and resizing.
- `70–72`: paged S3 browser, HEAD/preview/multipart and native protocol chain.
- `90`: one short cross-function management/data smoke. Dedicated specs retain
  dialog validation, partial failures and multi-node reconfiguration coverage.

## Cost and evidence

- Keep `stepTimer.step` around setup, mutation, readiness polling, DOM refresh and
  teardown. Each is a Playwright step; failed steps retain their duration.
- `slowReporter` keeps existing 2/5-second step logs and 10/30-second test logs.
  It also writes `test-results/timings.json` for all named phases and test results.
  Compare per-test/phase durations, excluding build and server startup.
- Share setup with `beforeAll`, stop/remove resources in `afterAll` or `finally`,
  keep destructive cases last, and reset only when empty authority is essential.
- Use installed system Chromium/Edge. Never download a browser, add retries,
  sleep to wait for state, or extend an assertion timeout to conceal failure.
  UI actions/assertions have a 3-second deadline; Group 0 election/init has an
  explicit 10-second deadline. Longer transfer deadlines require their own reason.
- Changes run the exact affected native spec first. Full acceptance uses
  `pixi run test-console-ui`; it remains incomplete while mocked scenarios
  await migration. Do not use `pageBehavior.config.ts` as a passing acceptance gate.

- Native acceptance requires `CROWDB_WEB_E2E_BASE_URL` for a disposable deployment:

```sh
pixi run bash -c 'cd app/crowdb-web/ui && npx playwright test --config=e2e/managedNative.config.ts'
```

- Multipart acceptance uses the same explicit disposable deployment:

```sh
pixi run bash -c 'cd app/crowdb-web/ui && npx playwright test --config=e2e/nativeS3.config.ts'
```

Do not point reset or destructive lifecycle suites at the user's persistent UI.
