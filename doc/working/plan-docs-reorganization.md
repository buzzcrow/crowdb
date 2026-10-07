<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB documentation reorganization plan

Persistent plan: retain until the user publishes the crowdb-web documentation update.

Goal: reorganize crowdb-web documentation into stable concept, Console UI, Console CLI, workflow, operations, deployment, and reference sections, while preserving existing URLs through compatibility redirects or landing pages.

Upstream links:
- Console UI source: `/cjdata/cpp/crowdb/app/crowdb-web/ui/src/`
- Documentation site: `/cjdata/cpp/crowdb-web/site/`
- Site tests: `/cjdata/cpp/crowdb-web/tests/test_site.py`

## Phase 1 — inventory and URL contract

- [x] **Map existing pages**: inventory current manual, deploy, architecture, demo, and quickstart pages and classify each page by audience and purpose. Files: `/cjdata/cpp/crowdb-web/site/docs/`, `/cjdata/cpp/crowdb-web/site/demo/`.
- [x] **Define stable URL registry**: record canonical URLs for concepts, Console UI tabs, Console CLI, workflows, operations, deployment, and reference pages. Files: `/cjdata/cpp/crowdb/app/crowdb-web/ui/src/shell/domainTabs.ts`, `/cjdata/cpp/crowdb-web/site/sitemap.xml`.
- [x] **Add compatibility coverage**: preserve existing public URLs and add tests for redirects or compatibility landing pages. Files: `/cjdata/cpp/crowdb-web/tests/test_site.py`, `/cjdata/cpp/crowdb-web/site/`.

## Phase 2 — concept and Console documentation

- [x] **Write product concepts**: explain Cluster, PaxosKV, Capacity, Chunk, ChunkKV, Iceberg, and S3 in user terms, including boundaries and relationships. Files: `/cjdata/cpp/crowdb-web/site/docs/manual/concepts/`.
- [x] **Write Console UI tab pages**: provide one concise concept page per Console tab under `/docs/manual/console/ui/tab/<tab>/`. Files: `/cjdata/cpp/crowdb-web/site/docs/manual/console/ui/tab/`.
- [x] **Write Console CLI section**: add the parallel `/docs/manual/console/cli/` structure and link each command group to the relevant concept and workflow pages. Files: `/cjdata/cpp/crowdb-web/site/docs/manual/console/cli/`.

## Phase 3 — workflows and operational reference

- [x] **Rewrite workflows**: add task-oriented guides for first deployment, adding nodes, creating Iceberg data, using S3, inspecting ChunkKV, and capacity checks. Files: `/cjdata/cpp/crowdb-web/site/docs/manual/workflows/`.
- [x] **Reorganize operations**: cover health, backup, recovery, upgrade, and troubleshooting with observable symptoms and verified actions. Files: `/cjdata/cpp/crowdb-web/site/docs/manual/operations/`.
- [x] **Split reference material**: move stable HTTP API, CLI syntax, configuration, and environment variable material into reference pages. Files: `/cjdata/cpp/crowdb-web/site/docs/manual/reference/`.
- [x] **Align deployment pages**: keep deployment mechanics under `/docs/deploy/` and link back to concepts and workflows. Files: `/cjdata/cpp/crowdb-web/site/docs/deploy/`.

## Phase 4 — navigation, validation, and handoff

- [x] **Update navigation**: make the new hierarchy discoverable from the docs sidebar and docs index. Files: `/cjdata/cpp/crowdb-web/site/docs/index.html`, `/cjdata/cpp/crowdb-web/site/docs/manual/index.html`, shared site navigation.
- [x] **Validate links and structure**: run crowdb-web site tests and verify every Console `docs` URL resolves to a local page. Files: `/cjdata/cpp/crowdb-web/tests/test_site.py`, `/cjdata/cpp/crowdb/app/crowdb-web/ui/src/shell/domainTabs.ts`.
- [x] **Prepare publish handoff**: summarize changed URLs, compatibility behavior, and the exact publish command. Files: `/cjdata/cpp/crowdb-web/README.md`.

## Consolidated file list

- `/cjdata/cpp/crowdb-web/site/docs/`
- `/cjdata/cpp/crowdb-web/site/sitemap.xml`
- `/cjdata/cpp/crowdb-web/tests/test_site.py`
- `/cjdata/cpp/crowdb-web/README.md`
- `/cjdata/cpp/crowdb/app/crowdb-web/ui/src/shell/domainTabs.ts`
- `doc/working/plan-docs-reorganization.md`

## Tests

- Unit/site: `python3 -m unittest discover -s /cjdata/cpp/crowdb-web/tests -q`.
- UI: `pixi run bash -c 'cd app/crowdb-web/ui && npm run build'`.
- Link check: site test local route and link validation, plus a direct check of all `domainTabs.ts` documentation paths.
