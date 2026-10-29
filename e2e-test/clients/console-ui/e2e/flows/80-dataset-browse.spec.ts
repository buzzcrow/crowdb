// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { datasetAction } from '../fixtures/resourceMenus';
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, openDataset, seed } from '../fixtures/dataset';
import { consoleProxy } from '../fixtures/consoleProxy';
import { step } from '../fixtures/stepTimer';

// Baseline: 5.5s (2026-10-29), catalog, immutable browsing and Dataset deletion.
test('Dataset native: browse', async ({ page, request }, testInfo) => {
  const name = `browse-${Date.now()}`;
  const root = await step('setup', () => seed(request, name));
  const child = await seed(request, name, root);
  await call(request, `${datasetPath(name)}/names/stable`, 'PUT', { snapshot: root });
  await step('independent Dataset list and tree pages', async () => {
    for (let index = 0; index < 21; index++) await call(request, datasetPath(`zz-page-${index}-${name}`), 'PUT');
    await page.goto('/?domain=Dataset');
    const tree = page.getByRole('navigation', { name: 'Dataset tree', exact: true });
    const listing = page.getByRole('table', { name: 'Datasets', exact: true });
    await expect(listing.getByRole('row')).toHaveCount(21);
    await page.getByRole('button', { name: 'Next Datasets', exact: true }).click();
    await expect(listing.getByRole('row')).toHaveCount(3);
    await expect(tree.getByRole('button', { name, exact: true })).toBeVisible();
    const names = await listing.getByRole('button').allTextContents();
    await listing.getByRole('button', { name: names[0], exact: true }).click();
    await expect(page.getByRole('table', { name: 'Snapshots', exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Back', exact: true }).click();
    await expect(listing.getByRole('button')).toHaveText(names);
  });
  await step('Dataset delete menu and nonempty protection', async () => {
    const empty = `delete-${Date.now()}`;
    await call(request, datasetPath(empty), 'PUT');
    await openDataset(page, empty);
    await datasetAction(page, 'Delete Dataset…');
    const dialog = page.getByRole('dialog', { name: 'Delete Dataset', exact: true });
    await expect(dialog.getByRole('button', { name: 'Delete Dataset', exact: true })).toBeEnabled();
    await dialog.getByRole('button', { name: 'Delete Dataset', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    await expect(page.getByRole('table', { name: 'Datasets', exact: true })).toBeVisible();
    await expect(page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: empty, exact: true })).toHaveCount(0);
    expect((await request.get(datasetPath(empty))).status()).toBe(404);
    await openDataset(page, name);
    await datasetAction(page, 'Delete Dataset…');
    await dialog.getByRole('button', { name: 'Delete Dataset', exact: true }).click();
    await expect(dialog.getByRole('alert')).toContainText('Dataset is not empty');
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(child);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  });
  await step('DOM refresh', async () => {
    await openDataset(page, name);
    await page.getByRole('button', { name: root, exact: true }).click();
    const matrix = page.getByRole('table', { name: 'Sample field matrix' });
    await expect(matrix.getByRole('row')).toHaveCount(21);
    await expect(matrix.getByRole('columnheader')).toHaveCount(13);
    await matrix.getByRole('button', { name: 'field-00', exact: false }).click();
    await expect(page.getByRole('region', { name: 'Dataset properties' })).toContainText('Properties · schema');
    await matrix.getByRole('button', { name: 'sample-00', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Dataset properties' })).toContainText('c2FtcGxlLTAw');
    const properties = page.getByRole('region', { name: 'Sample fields', exact: true });
    await properties.getByRole('button', { name: 'Next fields', exact: true }).click();
    await expect(properties).toContainText('field-12');
    await expect(matrix.getByRole('columnheader')).toHaveCount(13);
    await expect(matrix.getByRole('row').filter({ has: page.getByRole('button', { name: 'sample-00', exact: true }) })).toHaveAttribute('aria-selected', 'true');
    await matrix.getByRole('row').filter({ hasText: 'sample-00' }).getByRole('button', { name: 'inline · 7 B' }).click();
    await page.getByRole('button', { name: 'Preview field' }).click();
    await expect(page.getByLabel('Verified field preview')).toHaveText('value-0');
    await page.getByRole('button', { name: 'Next Samples', exact: true }).click();
    await expect(matrix.getByRole('row')).toHaveCount(3);
    await page.getByRole('button', { name: 'Next fields', exact: true }).click();
    await expect(matrix.getByRole('columnheader')).toHaveCount(2);
    await page.getByTestId('domain-s3').click(); await page.goBack();
    await expect(matrix.getByRole('row')).toHaveCount(3);
    const tree = page.getByRole('navigation', { name: 'Dataset tree', exact: true });
    await tree.getByTitle(root, { exact: true }).locator('..').getByRole('button', { name: 'Expand', exact: true }).click();
    await expect(tree.getByRole('button', { name: child.slice(0, 10), exact: true })).toBeVisible();
    await expect(tree).toContainText('Stable'); await expect(tree).toContainText('Latest');
    await page.screenshot({ path: testInfo.outputPath('dataset-workspace.png'), fullPage: true });
  });
});

// Baseline: 3.6s (2026-10-29), readonly and capability boundaries.
test('Dataset native: capabilities', async ({ page, request, baseURL }) => {
  const name = `capabilities-${Date.now()}`; const snapshot = await seed(request, name, null, 1, 1);
  await openDataset(page, name); await page.getByRole('button', { name: snapshot, exact: true }).click();
  await datasetAction(page, 'Reclaim Snapshot…');
  await page.getByRole('button', { name: 'Confirm reclamation' }).click();
  await expect(page.getByRole('dialog', { name: 'Reclaim Snapshot', exact: true }).getByRole('alert')).toBeVisible();
  expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(snapshot);
  await page.getByRole('dialog', { name: 'Reclaim Snapshot', exact: true }).getByRole('button', { name: 'Cancel', exact: true }).click();
  await datasetAction(page, 'Retain Snapshot');
  await expect.poll(async () => (await call(request, `${datasetPath(name)}/snapshots/${snapshot}/reclaim`)).retained, { intervals: [100], timeout: 3000 }).toBe(true);
  await datasetAction(page, 'Release Snapshot');
  await expect.poll(async () => (await call(request, `${datasetPath(name)}/snapshots/${snapshot}/reclaim`)).retained, { intervals: [100], timeout: 3000 }).toBe(false);
  const proxy = await consoleProxy(baseURL!); const seen: string[] = [];
  let observedModules = 'activity';
  page.on('request', value => {
    const document = new URL(page.url());
    if (document.origin === new URL(proxy.url).origin && document.searchParams.get('disableModules') === observedModules
      && value.url().includes('/dataset/')) seen.push(value.url());
  });
  try {
    await page.goto(`${proxy.url}/?domain=Dataset&readonly=1&disableModules=activity&apiPrefix=/proxy/api`);
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
    await page.getByRole('button', { name: snapshot, exact: true }).click();
    await expect(page.getByRole('table', { name: 'Sample field matrix' })).toContainText('sample-00');
    await expect(page.getByRole('button', { name: 'Reclaim Snapshot' })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Edit Snapshot' })).toHaveCount(0);
    await page.getByTestId('dataset-scope').click({ button: 'right' });
    await expect(page.getByRole('menuitem', { name: 'Reclaim Snapshot…', exact: true })).toBeDisabled();
    await expect(page.getByRole('menuitem', { name: 'Edit Snapshot…', exact: true })).toBeDisabled();
    await page.keyboard.press('Escape');
    await expect(page.getByText('Session activity', { exact: true })).toHaveCount(0);
    expect(seen.length).toBeGreaterThan(0); expect(seen.every(url => new URL(url).pathname.startsWith('/proxy/api/'))).toBe(true);
    seen.length = 0; observedModules = 'dataset';
    await page.goto(`${proxy.url}/?domain=Dataset&disableModules=dataset&apiPrefix=/proxy/api`);
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
    await expect(page.getByTestId('domain-dataset')).toHaveCount(0); expect(seen).toEqual([]);
  } finally { await proxy.close(); }

});
