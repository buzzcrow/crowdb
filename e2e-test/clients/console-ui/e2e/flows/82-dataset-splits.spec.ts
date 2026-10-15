// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, openDataset, publishDraft, seed } from '../fixtures/dataset';
import { step } from '../fixtures/stepTimer';
test('Dataset native: split_revisions', async ({ page, request }) => {
  const name = `splits-${Date.now()}`;
  const snapshot = await step('setup', () => seed(request, name, null, 3, 1));
  await openDataset(page, name); await page.getByRole('button', { name: snapshot, exact: true }).click();
  await page.getByRole('button', { name: 'Edit Split revision', exact: true }).click();
  await page.getByRole('button', { name: 'Add selection Split' }).click();
  await page.getByLabel('Split 1 name').fill('train');
  await page.getByRole('button', { name: 'Save Split draft' }).click();
  await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
  await step('lifecycle poll', () => publishDraft(page, 'Split revision'));
  const path = `${datasetPath(name)}/snapshots/${snapshot}`;
  const first = await call(request, `${path}/split-head`);
  expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(snapshot);
  await page.getByRole('button', { name: 'Edit Split revision', exact: true }).click();
  await expect(page.getByLabel('Split 1 name')).toHaveValue('train');
  await page.getByRole('button', { name: 'Remove train', exact: true }).click();
  await page.getByRole('button', { name: 'Save Split draft' }).click();
  await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
  await publishDraft(page, 'Split revision');
  const second = await call(request, `${path}/split-head`); expect(second.revision).not.toBe(first.revision);
  expect((await call(request, `${path}/split-revisions/${second.revision}`)).splits).toEqual([]);
  expect((await call(request, `${path}/split-revisions/${first.revision}/splits/train/members`)).entries).toHaveLength(3);
  expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(snapshot);
});
