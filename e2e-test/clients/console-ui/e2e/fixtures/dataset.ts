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
  await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
}
export async function publishResult(page: Page, kind: 'Snapshot' | 'Split revision') {
  await page.getByRole('button', { name: 'Commit', exact: true }).click();
  await expect(page.getByRole('region', { name: kind === 'Snapshot' ? 'Import Snapshot' : 'Edit Split revision', exact: true })).toHaveCount(0);
}
export async function mapping(page: Page, value: unknown) {
  await expandEditor(page, 'Advanced');
  await page.getByRole('textbox', { name: 'Sample mapping JSON', exact: true }).fill(JSON.stringify(value));
  await page.getByRole('button', { name: 'Apply batch mapping', exact: true }).click();
}
export async function publishSplits(request: APIRequestContext, name: string, snapshot: string, splits: unknown[]) {
  const path = `${datasetPath(name)}/snapshots/${snapshot}`; const revision = id();
  const head = await call(request, `${path}/split-head`);
  await call(request, `${path}/split-revisions/${revision}`, 'PUT', {
    splits, policy: { overlap: 'disjoint', coverage: 'allow_unassigned' },
  });
  return call(request, `${path}/split-head`, 'PUT', { revision, expected_revision: head.revision, expected_head: head.generation });
}

export async function expandEditor(page: Page, title: string) {
  const summary = page.locator('summary').filter({ hasText: new RegExp(`^${title}$`) });
  if (await summary.locator('..').getAttribute('open') === null) await summary.click();
}

export async function addField(page: Page, name: string, required = false) {
  await page.getByRole('button', { name: 'Add Field', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Add Field', exact: true });
  await dialog.getByLabel('Field name', { exact: true }).fill(name);
  await dialog.getByLabel('New field required').setChecked(required);
  await dialog.getByRole('button', { name: 'Add Field', exact: true }).click();
}
export async function addSample(page: Page, name: string) {
  const table = page.getByRole('table', { name: 'Snapshot editor table', exact: true });
  await page.getByRole('button', { name: 'Add Sample', exact: true }).click();
  const input = table.getByRole('textbox', { name: /^Sample ID for sample-[0-9a-f]{8}$/ });
  await input.fill(name); await input.press('Tab');
  await expect(table.getByLabel(`Sample ID for ${name}`, { exact: true })).toHaveValue(name);
}
export async function textCell(page: Page, sample: string, field: string, value: string) {
  const input = page.getByRole('textbox', { name: `${field} for ${sample}`, exact: true });
  await input.focus();
  await expect(input).not.toHaveAttribute('readonly', '');
  await input.fill(value);
  await expect(page.getByRole('dialog', { name: `Edit ${field}`, exact: true })).toHaveCount(0);
}
export async function resumeDraft(page: Page, name: string, resource: string) {
  const tree = page.getByRole('navigation', { name: 'Dataset tree', exact: true });
  await tree.getByRole('button', { name, exact: true }).click();
  await tree.getByRole('button', { name: `Snapshot Draft ${resource.slice(0, 8)}`, exact: true }).click();
}
