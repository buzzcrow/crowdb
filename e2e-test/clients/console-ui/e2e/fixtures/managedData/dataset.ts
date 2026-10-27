// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { call, datasetPath } from '../dataset';
import { expect } from '../realBackend';
import { datasetAction, resourceAction } from '../resourceMenus';
import { step } from '../stepTimer';

const matrix = (page: Page) => page.getByRole('table', { name: 'Sample field matrix', exact: true });

export async function datasetWorkflow(page: Page, request: APIRequestContext) {
  const name = `crud-${Date.now()}`;
  await step('Dataset: create and list an empty Dataset through the tree', async () => {
    await page.getByTestId('domain-dataset').click();
    await createDataset(page, name);
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: 'Datasets', exact: true }).click();
    await page.getByRole('table', { name: 'Datasets', exact: true }).getByRole('button', { name, exact: true }).click();
    await expect(page.getByRole('table', { name: 'Snapshots', exact: true }).getByRole('row')).toHaveCount(1);
    await expect(page.getByRole('navigation', { name: 'Dataset tree pages', exact: true })).toHaveCount(0);
  });
  const root = await step('Dataset: upload local content and publish two Samples', async () => {
    await datasetAction(page, 'Import / New Snapshot…');
    await page.getByLabel('Local payload files', { exact: true }).setInputFiles({ name: 'first.txt', mimeType: 'text/plain', buffer: Buffer.from('Dataset file v1') });
    await field(page, 'content', true); await field(page, 'label', false);
    await sample(page, 'sample-1', 'content', 'file', 'first.txt');
    await sample(page, 'sample-1', 'label', 'inline', 'original');
    await sample(page, 'sample-2', 'content', 'inline', 'initial sample-2');
    await sample(page, 'sample-2', 'label', 'inline', 'keep this label');
    const snapshot = await page.getByTestId('editing-resource-id').innerText();
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBeNull();
    await submit(page);
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(snapshot);
    await expect(matrix(page).getByRole('row')).toHaveCount(3);
    await preview(page, 'sample-1', 0, 'Dataset file v1');
    return snapshot;
  });
  const child = await appendAndRemove(page, request, name, root);
  await step('Dataset: historical Sample membership and payload stay unchanged', async () => {
    await selectDataset(page, name);
    const inventory = page.getByRole('table', { name: 'Snapshots', exact: true });
    await expect(inventory.getByRole('row')).toHaveCount(3);
    await inventory.getByRole('button', { name: root, exact: true }).click();
    await expect(matrix(page).getByRole('button', { name: 'sample-1', exact: true })).toBeVisible();
    await expect(matrix(page).getByRole('button', { name: 'sample-3', exact: true })).toHaveCount(0);
    await preview(page, 'sample-1', 0, 'Dataset file v1');
    await page.getByRole('button', { name: 'Refresh Dataset', exact: true }).click();
    await expect(matrix(page).getByRole('row')).toHaveCount(3);
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(child);
  });
  await deleteProtection(page, request, name, child);
}

async function appendAndRemove(page: Page, request: APIRequestContext, name: string, root: string) {
  return step('Dataset: add, replace and remove Samples in one child publication', async () => {
    await selectDataset(page, name);
    await datasetAction(page, 'Import / New Snapshot…');
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true })).toContainText(`Parent ${root}`);
    await page.getByLabel('Local payload files', { exact: true }).setInputFiles([
      { name: 'replacement.txt', mimeType: 'text/plain', buffer: Buffer.from('Dataset file v2') },
      { name: 'added.txt', mimeType: 'text/plain', buffer: Buffer.from('third Sample file') },
    ]);
    await page.getByLabel('Sample ID (UTF-8)', { exact: true }).fill('sample-1');
    await page.getByRole('button', { name: 'Remove Sample', exact: true }).click();
    await page.getByLabel('Sample ID for replacement.txt', { exact: true }).fill('sample-2');
    await page.getByLabel('Sample ID for added.txt', { exact: true }).fill('sample-3');
    await page.getByRole('button', { name: 'Apply file mappings', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toContainText('already exists');
    await page.getByLabel('Allow updating existing Sample IDs', { exact: true }).check();
    await page.getByRole('button', { name: 'Apply file mappings', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Pending Sample changes', exact: true }).getByRole('row')).toHaveCount(4);
    const child = await page.getByTestId('editing-resource-id').innerText();
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(root);
    await submit(page);
    const summary = await call(request, `${datasetPath(name)}/snapshots/${child}/summary`);
    expect(summary.parent).toBe(root);
    expect(summary.sample_count).toBe('2');
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(child);
    await expect(matrix(page).getByRole('row')).toHaveCount(3);
    await expect(matrix(page).getByRole('button', { name: 'sample-1', exact: true })).toHaveCount(0);
    await preview(page, 'sample-2', 0, 'Dataset file v2');
    await preview(page, 'sample-2', 1, 'keep this label');
    await preview(page, 'sample-3', 0, 'third Sample file');
    return child;
  });
}

async function deleteProtection(page: Page, request: APIRequestContext, name: string, child: string) {
  await step('Dataset: reject nonempty deletion and delete an empty Dataset', async () => {
    await selectDataset(page, name);
    await datasetAction(page, 'Delete Dataset…');
    const dialog = page.getByRole('dialog', { name: 'Delete Dataset', exact: true });
    await dialog.getByRole('button', { name: 'Delete Dataset', exact: true }).click();
    await expect(dialog.getByRole('alert')).toContainText('Dataset is not empty');
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(child);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    const empty = `${name}-empty`;
    await createDataset(page, empty);
    await datasetAction(page, 'Delete Dataset…');
    await dialog.getByRole('button', { name: 'Delete Dataset', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    expect((await request.get(datasetPath(empty))).status()).toBe(404);
    await expect(page.getByRole('table', { name: 'Datasets', exact: true }).getByRole('button', { name: empty, exact: true })).toHaveCount(0);
    await expect(page.getByRole('table', { name: 'Datasets', exact: true }).getByRole('button', { name, exact: true })).toBeVisible();
    await page.reload();
    const listing = page.getByRole('table', { name: 'Datasets', exact: true });
    await expect(listing.getByRole('button', { name, exact: true })).toBeVisible();
    await expect(listing.getByRole('button', { name: empty, exact: true })).toHaveCount(0);
  });
}

async function createDataset(page: Page, name: string) {
  const root = page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: 'Datasets', exact: true });
  await resourceAction(page, root, 'Create Dataset…');
  await page.getByRole('textbox', { name: 'New Dataset name', exact: true }).fill(name);
  await page.getByRole('dialog', { name: 'Create Dataset', exact: true }).getByRole('button', { name: 'Create Dataset', exact: true }).click();
  await expect(page.getByRole('heading', { name: `Dataset · ${name}`, exact: true })).toBeVisible();
}

async function selectDataset(page: Page, name: string) {
  await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
  await expect(page.getByRole('table', { name: 'Snapshots', exact: true })).toBeVisible();
}

async function field(page: Page, name: string, required: boolean) {
  await page.getByLabel('Field name', { exact: true }).fill(name);
  await page.getByLabel('New field required', { exact: true }).setChecked(required);
  await page.getByRole('button', { name: 'Add field', exact: true }).click();
}
async function sample(page: Page, id: string, field: string, kind: 'file' | 'inline', value: string) {
  await page.getByLabel('Sample ID (UTF-8)', { exact: true }).fill(id);
  await page.getByLabel('Sample field', { exact: true }).selectOption(field);
  await page.getByLabel('Value source', { exact: true }).selectOption(kind);
  if (kind === 'file') await page.getByLabel('Payload file', { exact: true }).selectOption(value);
  else await page.getByLabel('Inline text', { exact: true }).fill(value);
  await page.getByRole('button', { name: 'Apply Sample field', exact: true }).click();
}
async function submit(page: Page) {
  await page.getByRole('button', { name: 'Submit Snapshot', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true })).toHaveCount(0);
}

async function preview(page: Page, sample: string, field: number, content: string) {
  const row = matrix(page).getByRole('row').filter({ has: page.getByRole('button', { name: sample, exact: true }) });
  await row.getByRole('cell').nth(field).getByRole('button').click();
  await page.getByRole('button', { name: 'Preview field', exact: true }).click();
  await expect(page.getByLabel('Verified field preview', { exact: true })).toHaveText(content);
}
