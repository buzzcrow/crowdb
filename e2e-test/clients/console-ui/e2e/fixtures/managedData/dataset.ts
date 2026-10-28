// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { call, datasetPath, addField, addSample, textCell, expandEditor } from '../dataset';
import { expect, test } from '../realBackend';
import { datasetAction, resourceAction } from '../resourceMenus';
import { step } from '../stepTimer';

const matrix = (page: Page) => page.getByRole('table', { name: 'Sample field matrix', exact: true });

export async function datasetWorkflow(page: Page, request: APIRequestContext) {
  const name = `crud-${Date.now()}`;
  await step('Dataset: create and list an empty Dataset through the tree', async () => {
    await page.getByTestId('domain-dataset').click();
    await expect(page.getByRole('region', { name: 'Dataset help' }).getByRole('link', { name: 'Help', exact: true })).toHaveAttribute('href', 'https://crowdb.dev/docs/manual/');
    await createDataset(page, name);
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: 'Datasets', exact: true }).click();
    await page.getByRole('table', { name: 'Datasets', exact: true }).getByRole('button', { name, exact: true }).click();
    await expect(page.getByRole('table', { name: 'Snapshots', exact: true }).getByRole('row')).toHaveCount(1);
    await expect(page.getByRole('navigation', { name: 'Dataset tree pages', exact: true })).toHaveCount(0);
  });
  const root = await step('Dataset: upload local content and publish two Samples', async () => {
    await datasetAction(page, 'New Snapshot…');
    await page.getByLabel('Local payload files', { exact: true }).setInputFiles({ name: 'first.txt', mimeType: 'text/plain', buffer: Buffer.from('Dataset file v1') });
    await expect(page.getByRole('button', { name: 'Commit', exact: true })).toBeEnabled();
    await expect(page.getByLabel('Upload target field')).toHaveValue('content');
    await expect(page.getByLabel('Field name', { exact: true })).toHaveCount(0);
    await page.screenshot({ path: test.info().outputPath('dataset-table-editor.png') });
    await addField(page, 'label');
    await page.getByLabel('Sample ID for first.txt', { exact: true }).fill('sample-1');
    await page.getByLabel('Sample ID for first.txt', { exact: true }).press('Tab');
    await textCell(page, 'sample-1', 'label', 'original');
    await addSample(page, 'sample-2');
    await textCell(page, 'sample-2', 'content', 'initial sample-2');
    await textCell(page, 'sample-2', 'label', 'keep this label');
    await expandEditor(page, 'Advanced');
    const snapshot = await page.getByTestId('editing-resource-id').textContent();
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
    await datasetAction(page, 'New Snapshot…');
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true })).toContainText(`Parent: ${root.slice(0, 12)}`);
    await page.getByRole('button', { name: 'Remove Sample sample-1', exact: true }).click();
    await page.getByLabel('Upload content for sample-2', { exact: true }).setInputFiles({ name: 'replacement.txt', mimeType: 'text/plain', buffer: Buffer.from('Dataset file v2') });
    await expect(page.getByRole('button', { name: 'Commit', exact: true })).toBeEnabled();
    await page.getByLabel('Local payload files', { exact: true }).setInputFiles({ name: 'added.txt', mimeType: 'text/plain', buffer: Buffer.from('third Sample file') });
    await expect(page.getByRole('button', { name: 'Commit', exact: true })).toBeEnabled();
    await page.getByLabel('Sample ID for added.txt', { exact: true }).fill('sample-3');
    await page.getByLabel('Sample ID for added.txt', { exact: true }).press('Tab');
    await expect(page.getByLabel('Sample ID for sample-3', { exact: true })).toHaveValue('sample-3');
    await expandEditor(page, 'Advanced');
    const child = await page.getByTestId('editing-resource-id').textContent();
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

async function submit(page: Page) {
  await page.getByRole('button', { name: 'Commit', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true })).toHaveCount(0);
}

async function preview(page: Page, sample: string, field: number, content: string) {
  const row = matrix(page).getByRole('row').filter({ has: page.getByRole('button', { name: sample, exact: true }) });
  await row.getByRole('cell').nth(field).getByRole('button').click();
  await page.getByRole('button', { name: 'Preview field', exact: true }).click();
  await expect(page.getByLabel('Verified field preview', { exact: true })).toHaveText(content);
}
