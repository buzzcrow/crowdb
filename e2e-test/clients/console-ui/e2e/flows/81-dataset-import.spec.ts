// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, openDataset, publishDraft } from '../fixtures/dataset';
import { step } from '../fixtures/stepTimer';

async function create(page: import('@playwright/test').Page, name: string) {
  await page.goto('/?domain=Dataset');
  await page.getByRole('textbox', { name: 'New Dataset name' }).fill(name);
  await page.getByRole('button', { name: 'Create Dataset', exact: true }).click();
  await expect(page.getByRole('heading', { name: `Dataset · ${name}`, exact: true })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Dataset actions', exact: true })).toHaveCount(1);
  await page.getByRole('button', { name: 'Refresh Dataset', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Dataset actions', exact: true })).toHaveCount(1);
  await page.getByRole('button', { name: 'Import / New Snapshot', exact: true }).click();
  await page.getByRole('textbox', { name: 'Inline text', exact: true }).fill('native import payload');
  await page.getByRole('button', { name: 'Use single Sample mapping' }).click();
}
test('Dataset native: import', async ({ page, request }) => {
  const name = `import-${Date.now()}`;
  await step('setup', () => create(page, name));
  await page.getByRole('button', { name: 'Add selection Split' }).click();
  await page.getByLabel('Split 1 name').fill('train');
  await page.getByRole('button', { name: 'Review mapping' }).click();
  await step('mutation response', async () => {
    await page.getByRole('button', { name: 'Save draft and upload local data' }).click();
    await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
  });
  await step('lifecycle poll', () => publishDraft(page, 'Snapshot'));
  const head = await call(request, `${datasetPath(name)}/head`);
  expect(head.latest).toMatch(/^[0-9a-f]{32}$/); expect(head.stable).toBeNull();
  const preview = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/preview?sample=${encodeURIComponent(Buffer.from('sample-1').toString('base64'))}&field=text`);
  expect(Buffer.from(preview.data, 'base64').toString()).toBe('native import payload');
  await expect(page.getByRole('table', { name: 'Sample field matrix' })).toContainText('sample-1');
  const split = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/split-head`);
  const revision = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/split-revisions/${split.revision}`);
  expect(revision.splits.map((value: { name: string }) => value.name)).toEqual(['train']);
});

test('Dataset native: recovery', async ({ page, request }) => {
  const name = `recovery-${Date.now()}`;
  await create(page, name);
  await page.getByRole('button', { name: 'Review mapping' }).click();
  await page.getByRole('button', { name: 'Save draft and upload local data' }).click();
  await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
  await page.reload(); await openDataset(page, name);
  await page.getByRole('button', { name: 'Resume saved draft' }).click();
  await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
  await publishDraft(page, 'Snapshot');
  const inventory = await call(request, `${datasetPath(name)}/snapshot-inventory`);
  expect(inventory.entries).toHaveLength(1);
});
