// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, openDataset, seed } from '../fixtures/dataset';
import { step } from '../fixtures/stepTimer';

test('Dataset native: browse', async ({ page, request }) => {
  const name = `browse-${Date.now()}`;
  const root = await step('setup', () => seed(request, name));
  const child = await seed(request, name, root);
  await call(request, `${datasetPath(name)}/names/stable`, 'PUT', { snapshot: root });
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
    await matrix.getByRole('row').filter({ hasText: 'sample-00' }).getByRole('button', { name: 'inline · 7 B' }).click();
    await page.getByRole('button', { name: 'Preview field' }).click();
    await expect(page.getByLabel('Verified field preview')).toHaveText('value-0');
    await page.getByRole('button', { name: 'Next Samples', exact: true }).click();
    await expect(matrix.getByRole('row')).toHaveCount(3);
    await page.getByRole('button', { name: 'Next fields', exact: true }).click();
    await expect(matrix.getByRole('columnheader')).toHaveCount(2);
    await page.getByTestId('domain-s3').click(); await page.goBack();
    await expect(matrix.getByRole('row')).toHaveCount(3);
    const tree = page.getByRole('navigation', { name: 'Dataset tree' });
    await tree.getByRole('button', { name: `Expand Snapshot ${root}`, exact: true }).click();
    await expect(tree.getByRole('button', { name: child.slice(0, 10), exact: true })).toBeVisible();
    await expect(tree).toContainText('Stable'); await expect(tree).toContainText('Latest');
  });
});

test('Dataset native: capabilities', async ({ page, request }) => {
  const name = `capabilities-${Date.now()}`; const snapshot = await seed(request, name, null, 1, 1);
  await openDataset(page, name); await page.getByRole('button', { name: snapshot, exact: true }).click();
  await page.getByRole('button', { name: 'Reclaim Snapshot', exact: true }).click();
  await page.getByRole('button', { name: 'Confirm reclamation' }).click();
  await expect(page.getByRole('region', { name: 'Dataset actions' }).getByRole('alert')).toBeVisible();
  expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(snapshot);
});
