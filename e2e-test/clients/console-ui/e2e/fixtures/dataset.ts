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
  const head = await call(request, `${root}/head`); const snapshot = id();
  const samples = parent ? [] : Array.from({ length: count }, (_, index) => ({ kind: 'upsert',
    id: Buffer.from(`sample-${String(index).padStart(2, '0')}`).toString('base64'),
    fields: [{ name: 'field-00', value: { kind: 'inline', data: Buffer.from(`value-${index}`).toString('base64') } }] }));
  await call(request, `${root}/snapshots/${snapshot}`, 'PUT', { parent, source: { kind: 'all' },
    schema: parent ? null : { version: 1, fields: Array.from({ length: columns }, (_, index) => ({ name: `field-${String(index).padStart(2, '0')}`, required: false })) },
    samples, splits: [], policy: { overlap: 'disjoint', coverage: 'allow_unassigned' } });
  await call(request, `${root}/head`, 'PUT', { snapshot, expected_head: head.generation });
  return snapshot;
}
export async function openDataset(page: Page, name: string) {
  await page.goto('/?domain=Dataset');
  await page.getByRole('navigation', { name: 'Dataset tree' }).getByRole('button', { name, exact: true }).click();
}
export async function preparedResult(page: Page, kind: 'Snapshot' | 'Split revision') {
  const region = page.getByRole('region', { name: 'Resource publication' });
  await expect(region.getByRole('button', { name: `Publish ${kind}`, exact: true })).toBeEnabled();
}
export async function publishResult(page: Page, kind: 'Snapshot' | 'Split revision') {
  await preparedResult(page, kind);
  await page.getByRole('region', { name: 'Resource publication' }).getByRole('button', { name: `Publish ${kind}`, exact: true }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
}
export async function publishSplits(request: APIRequestContext, name: string, snapshot: string, splits: unknown[]) {
  const path = `${datasetPath(name)}/snapshots/${snapshot}`; const revision = id();
  const head = await call(request, `${path}/split-head`);
  await call(request, `${path}/split-revisions/${revision}`, 'PUT', {
    splits, policy: { overlap: 'disjoint', coverage: 'allow_unassigned' },
  });
  return call(request, `${path}/split-head`, 'PUT', { revision, expected_revision: head.revision, expected_head: head.generation });
}
