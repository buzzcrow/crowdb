// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { datasetAction, resourceAction } from '../fixtures/resourceMenus';
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, openDataset, publishResult, publishSplits, seed } from '../fixtures/dataset';
import { step } from '../fixtures/stepTimer';
// Baseline: 0.219s (2026-10-24), failed at the retired fixture API before resource migration.
test('Dataset native: split_revisions', async ({ page, request, browser, baseURL }) => {
  const name = `splits-${Date.now()}`;
  const snapshot = await step('setup', () => seed(request, name, null, 3, 1));
  await openDataset(page, name); await page.getByRole('button', { name: snapshot, exact: true }).click();
  const snapshotNode = page.getByTestId(`tree-node-${name}:node:${snapshot}`);
  await snapshotNode.getByRole('button', { name: 'Expand', exact: true }).click();
  const tree = page.getByRole('navigation', { name: 'Dataset tree', exact: true });
  await resourceAction(page, tree.getByRole('button', { name: 'Splits', exact: true }), 'Create Split…');
  await page.getByLabel('Split 1 name').fill('train');
  await step('lifecycle poll', () => publishResult(page, 'Split revision'));
  const path = `${datasetPath(name)}/snapshots/${snapshot}`;
  const first = await call(request, `${path}/split-head`);
  expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(snapshot);
  const staleContext = await browser.newContext({ baseURL });
  const stale = await staleContext.newPage();
  try {
    await openDataset(stale, name); await stale.getByRole('button', { name: snapshot, exact: true }).click();
    await datasetAction(stale, 'Edit Split revision…');
    await expect(stale.getByLabel('Split 1 name')).toHaveValue('train');
    await stale.getByLabel('Split 1 name').fill('stale');
    await datasetAction(page, 'Edit Split revision…');
    await expect(page.getByLabel('Split 1 name')).toHaveValue('train');
    await page.getByLabel('Split 1 name').fill('training');
    await publishResult(page, 'Split revision');
    await stale.getByRole('button', { name: 'Submit Split revision', exact: true }).click();
    await expect(stale.getByRole('button', { name: 'Use reviewed head' })).toBeEnabled();
    const losing = await stale.getByTestId('prepared-resource-id').innerText();
    expect((await call(request, `${path}/split-revisions/${losing}`)).splits[0].name).toBe('stale');
    expect((await call(request, `${path}/split-head`)).revision).not.toBe(losing);
    await stale.getByRole('button', { name: 'Discard local request' }).click();
    await expect(stale.getByRole('region', { name: 'Edit Split revision', exact: true })).toHaveCount(0);
  } finally { await staleContext.close(); }
  const frozen = (await call(request, `${path}/split-head`)).revision;
  await page.getByRole('button', { name: 'Split revision history' }).click();
  await page.getByRole('button', { name: frozen, exact: false }).click();
  await page.getByRole('button', { name: 'training (3)', exact: true }).click();
  await datasetAction(page, 'Create child from Split…');
  await page.getByLabel('Filter frozen Split membership').check();
  const frozenSource = page.getByRole('region', { name: 'Frozen Split filter', exact: true });
  await frozenSource.getByLabel('Selection kind', { exact: true }).selectOption('explicit');
  await frozenSource.getByLabel('Selected Sample IDs (one per line)', { exact: true }).fill('sample-00\nsample-01');
  // A later revision must not change the already chosen historical source.
  await publishSplits(request, name, snapshot, [{ name: 'training', origin: { kind: 'evaluated' }, definition: { kind: 'selection', predicate: { kind: 'explicit', ids: [] } } }]);
  await publishResult(page, 'Snapshot');
  const child = (await call(request, `${datasetPath(name)}/head`)).latest;
  expect((await call(request, `${datasetPath(name)}/snapshots/${child}/summary`)).sample_count).toBe('2');
  const initial = (await call(request, `${datasetPath(name)}/snapshots/${child}/split-head`)).revision;
  const inherited = await call(request, `${datasetPath(name)}/snapshots/${child}/split-revisions/${initial}`);
  expect(inherited.splits[0].origin.source.revision).toBe(frozen); expect(inherited.splits[0].count).toBe('2');
  await openDataset(page, name); await page.getByRole('button', { name: snapshot, exact: true }).click();
  await page.getByRole('button', { name: 'Split revision history' }).click();
  await page.getByRole('button', { name: first.revision, exact: false }).click();
  await datasetAction(page, 'Edit Split revision…');
  await expect(page.getByLabel('Split 1 name')).toHaveValue('train');
  await page.getByRole('button', { name: 'Discard local request', exact: true }).click();
  await page.getByRole('button', { name: 'Go to current Split revision', exact: true }).click();
  await datasetAction(page, 'Edit Split revision…');
  await expect(page.getByLabel('Split 1 name')).toHaveValue('training');
  await page.getByRole('button', { name: 'Discard local request', exact: true }).click();
  await snapshotNode.getByRole('button', { name: 'Expand', exact: true }).click();
  await tree.getByRole('button', { name: 'Splits', exact: true }).click();
  await resourceAction(page, tree.getByRole('button', { name: 'training (0)', exact: true }), 'Remove Split…');
  await expect(page.getByLabel('Split 1 name')).toHaveCount(0);
  await publishResult(page, 'Split revision');
  const second = await call(request, `${path}/split-head`); expect(second.revision).not.toBe(first.revision);
  expect((await call(request, `${path}/split-revisions/${second.revision}`)).splits).toEqual([]);
  expect((await call(request, `${path}/split-revisions/${first.revision}/splits/train/members`)).entries).toHaveLength(3);
  expect((await call(request, `${datasetPath(name)}/head`)).latest).toBe(child);
});
