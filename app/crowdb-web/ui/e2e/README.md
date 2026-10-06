<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI acceptance

Behavior authority: [Console UI specification](../../../../doc/design/console/design-crowdb-console-ui.md).

## Acceptance policy

- E2E must use real services, APIs, persisted metadata and file bytes. The shared
  fixture rejects Page/Context routing and HAR replay. Mocked observations cannot
  satisfy acceptance.
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
  cross-links use actual APIs.
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
- `92`: empty backend → UI-created rack and three default node plans → UI KV
  initialization → late DiskGroups/disks → automatic six-service deployment.
  Verifies Group 1 bindings and DiskDB/DiskIO ownership, UI S3 upload/preview
  with exact object bytes, and UI-created Iceberg tables with SDK append/scan
  of actual Parquet rows. The normal UI task builds all six service binaries.
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
  `pixi run test-console-ui`. Use the owned native provisioning test for actual
  Chunk/Iceberg/S3 parsers and data. Do not use `pageBehavior.config.ts` as a
  passing acceptance gate.

- Native acceptance requires `CROWDB_WEB_E2E_BASE_URL` for a disposable deployment:

```sh
pixi run bash -c 'cd app/crowdb-web/ui && npx playwright test --config=e2e/managedNative.config.ts'
```

- Multipart acceptance uses the same explicit disposable deployment:

```sh
pixi run bash -c 'cd app/crowdb-web/ui && npx playwright test --config=e2e/nativeS3.config.ts'
```

Do not point reset or destructive lifecycle suites at the user's persistent UI.

## Owned native fixture

The ignored Rust provisioning case creates one rack, three nodes and all six
services per node, then stops every owned process on success or failure. Set
an isolated `CROWDB_RUNTIME_ROOT`, clean that same root and run:

```sh
CROWDB_RUNTIME_ROOT=/tmp/crowdb-console-native pixi run clean-env
pixi run env CROWDB_RUNTIME_ROOT=/tmp/crowdb-console-native \
  CROWDB_NATIVE_UI_E2E=1 CROWDB_NATIVE_DATA_WINDOWS=1 \
  cargo test -p crowdb-web --test native_cluster_provisioning_test \
  one_rack_three_nodes_provision_all_services_without_metadata_repairs \
  -- --ignored --nocapture
```

- The main native selection collects 21 browser cases. Its prerequisite-arrival,
  slow production split and small-geometry Journal cases select distinct fixtures;
  three skips in this selection cannot count as three passes.
- Prerequisite arrival remains covered by the ordinary real-backend lifecycle
  spec. `CROWDB_NATIVE_JOURNAL_WINDOWS=1` with
  `CROWDB_NATIVE_UI_E2E_GREP='large Journal replaces'` selects actual 100-extent
  replacement and stale cursors without seeding an unrelated browser suite.
- `CROWDB_NATIVE_WEIGHTED_ACCEPTANCE=1` observes the persisted one-minute
  production cooldown, unequal retained bytes, actual ownership transfers and
  all 512 exact values. Healthy movable data plus an idle healthy owner must
  make actual placement progress within 40 seconds; request and lease budgets
  retain their normal values.
- Add `CROWDB_NATIVE_TRANSITION_ACCEPTANCE=1` and
  `CROWDB_NATIVE_UI_E2E_GREP='production split displays'` to that weighted fixture
  to inspect the real inherited/current streams and reject a stale generation.
- `CROWDB_NATIVE_MIXED_UNITS=1` verifies incompatible allocation geometry fails
  before partial Chunk service bootstrap.
- Actual Docker capability and data CRUD acceptance is independently covered by
  `pixi run test-single-node-container`; a managed host profile is insufficient.

## Verified acceptance

- On 2026-10-05, the ordinary real-backend suite passed 57/57. The three-node
  native chain passed 18 cases with three fixture-specific skips in 95.52 s;
  owned teardown took 8.426 s. Dedicated prerequisite, Journal and production
  split fixtures retain the skipped contracts.
- The production split browser case passed in 29.1 s. The combined real
  weighted fixture passed in 326.42 s with 4/4/4 final assignment, byte-weighted
  transfer, 512 exact records and 5.762 s teardown. The Journal browser case
  passed in 2.9 s, including actual 100-extent replacement and stale cursors.
- Docker container acceptance separately passed UI capability/data operations,
  interrupted bootstrap, all-service crash/hang recovery and volume restart.
  Its 325 MB bound measures uncompressed image layers rather than the daemon's
  compressed-content plus unpacked-snapshot cache accounting.
