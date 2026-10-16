// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { randomUUID } from 'node:crypto';
import { expect, type APIRequestContext, type Page } from '@playwright/test';
export const id = () => randomUUID().replaceAll('-', '');
export const datasetPath = (name: string) => `/api/access/dataset/api/dataset/v2/namespaces/dataset-ns/datasets/${name}`;
export async function call(request: APIRequestContext, path: string, method = 'GET', data?: unknown) {
  const response = await request.fetch(path, { method, ...(data === undefined ? {} : { data }) });
  expect(response.ok(), `${method} ${path}: ${await response.text()}`).toBe(true); return response.json();
}
export async function seed(request: APIRequestContext, name: string, parent: string | null = null, count = 22, columns = 13) {
  const root = datasetPath(name); if (!parent) await call(request, root, 'PUT');
  const head = await call(request, `${root}/head`); const operation = id();
  const path = `${root}/compositions/${operation}`;
  let draft = await call(request, `${root}/compositions`, 'POST', { operation, parent, source: { kind: 'all' },
    schema: parent ? null : { version: 1, fields: Array.from({ length: columns }, (_, index) => ({ name: `field-${String(index).padStart(2, '0')}`, required: false })) },
    inherit_parent_splits: false, parent_split_revision: null, splits: [], policy: null });
  if (!parent) for (let offset = 0; offset < count; offset += 20) {
    const samples = Array.from({ length: Math.min(20, count - offset) }, (_, index) => ({ kind: 'upsert', id: Buffer.from(`sample-${String(offset + index).padStart(2, '0')}`).toString('base64'), fields: [{ name: 'field-00', value: { kind: 'inline', data: Buffer.from(`value-${offset + index}`).toString('base64') } }] }));
    draft = await call(request, path, 'PUT', { edit: id(), expected_revision: draft.revision, samples });
  }
  let page = '0'; let evaluated = false;
  for (let step = 0; step < 100 && !evaluated; step++) {
    const preview = await call(request, `${path}/evaluate`, 'POST', { expected_revision: draft.revision, expected_page: page }); page = preview.next_page; evaluated = preview.complete;
  }
  expect(evaluated).toBe(true);
  for (let step = 0; step < 100 && !draft.uploads.publication_ready; step++) draft = await call(request, `${path}/prepare`, 'POST', { expected_revision: draft.revision });
  expect(draft.uploads.publication_ready).toBe(true);
  const receipt = await call(request, `${path}/publish`, 'POST', { attempt: id(), snapshot: draft.preparation.snapshot, expected_head: head.generation });
  return receipt.target.snapshot as string;
}
export async function openDataset(page: Page, name: string) {
  await page.goto('/?domain=Dataset');
  await page.getByRole('navigation', { name: 'Dataset tree' }).getByRole('button', { name, exact: true }).click();
}
export async function prepareDraft(page: Page, kind: 'Snapshot' | 'Split revision') {
  const region = page.getByRole('region', { name: 'Draft publication' });
  for (let step = 0; step < 100; step++) {
    const evaluate = region.getByRole('button', { name: /^(Evaluate complete membership|Continue evaluation)$/ });
    if (!await evaluate.isEnabled()) break;
    await evaluate.click();
    await expect(region.getByRole('button', { name: 'Inspect draft status' })).toBeEnabled();
  }
  const publish = region.getByRole('button', { name: `Publish ${kind}`, exact: true });
  for (let step = 0; step < 100 && !await publish.isEnabled(); step++) {
    await region.getByRole('button', { name: 'Prepare candidate' }).click();
    await expect(region.getByRole('button', { name: 'Inspect draft status' })).toBeEnabled();
  }
  await expect(publish).toBeEnabled();
}
export async function publishDraft(page: Page, kind: 'Snapshot' | 'Split revision') {
  await prepareDraft(page, kind);
  await page.getByRole('region', { name: 'Draft publication' }).getByRole('button', { name: `Publish ${kind}`, exact: true }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
}

export async function publishSplits(request: APIRequestContext, name: string, snapshot: string, splits: unknown[]) {
  const root = datasetPath(name); const operation = id(); const path = `${root}/split-drafts/${operation}`;
  const head = await call(request, `${root}/snapshots/${snapshot}/split-head`);
  let draft = await call(request, `${root}/snapshots/${snapshot}/split-drafts`, 'POST', {
    operation, expected_revision: head.revision, splits, policy: { overlap: 'disjoint', coverage: 'allow_unassigned' },
  });
  let next = '0'; let complete = false;
  for (let step = 0; step < 100 && !complete; step++) {
    const value = await call(request, `${path}/evaluate`, 'POST', { expected_revision: draft.revision, expected_page: next });
    complete = value.complete; next = value.next_page;
  }
  expect(complete).toBe(true);
  await call(request, `${path}/prepare`, 'POST', { expected_revision: draft.revision });
  draft = await call(request, path);
  return call(request, `${path}/publish`, 'POST', { attempt: id(), revision: draft.progress.target.revision, expected_head: head.generation });
}
