// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { datasetAction } from '../fixtures/resourceMenus';
import { createHash } from 'node:crypto';
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, mapping, publishResult } from '../fixtures/dataset';
import { consoleProxy } from '../fixtures/consoleProxy';
import { step } from '../fixtures/stepTimer';

async function create(page: import('@playwright/test').Page, name: string, url = '/?domain=Dataset') {
  await page.goto(url);
  await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: 'Datasets', exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Create Dataset…', exact: true }).click();
  await page.getByRole('textbox', { name: 'New Dataset name' }).fill(name);
  await page.getByRole('button', { name: 'Create Dataset', exact: true }).click();
  await expect(page.getByRole('heading', { name: `Dataset · ${name}`, exact: true })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Dataset actions', exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'Refresh Dataset', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Dataset actions', exact: true })).toHaveCount(0);
  await datasetAction(page, 'Import / New Snapshot…');
  await page.getByLabel('Field name', { exact: true }).fill('text');
  await page.getByRole('button', { name: 'Add field', exact: true }).click();
  await page.getByLabel('Sample ID (UTF-8)', { exact: true }).fill('sample-1');
  await page.getByRole('textbox', { name: 'Inline text', exact: true }).fill('native import payload');
  await page.getByRole('button', { name: 'Apply Sample field', exact: true }).click();
}
// Baseline: 3.585s (2026-10-24), failed at the retired capability before resource migration.
test('Dataset native: import', async ({ page, request }) => {
  const name = `import-${Date.now()}`;
  await step('setup', () => create(page, name));
  const payload = Buffer.alloc(4 * 1024 * 1024 + 17, 0xa5);
  await page.getByLabel('Local payload files').setInputFiles({ name: 'payload.bin', mimeType: 'application/octet-stream', buffer: payload });
  await mapping(page, { version: 1,
    schema: { version: 1, fields: [{ name: 'text', required: false }, { name: 'payload', required: false }] },
    samples: [
      { kind: 'upsert', id: Buffer.from('sample-1').toString('base64'), fields: [{ name: 'text', value: { kind: 'inline', data: Buffer.from('native import payload').toString('base64') } }] },
      { kind: 'upsert', id: '/wA=', fields: [{ name: 'payload', value: { kind: 'file', file: 'payload.bin' } }] },
    ] });
  await page.getByRole('button', { name: 'Add selection Split' }).click();
  await page.getByLabel('Split 1 name').fill('train');
  await step('mutation response', async () => {
    const responses: import('@playwright/test').Response[] = [];
    const observe = (response: import('@playwright/test').Response) => { if (response.url().includes(`${datasetPath(name)}/`) && response.request().method() !== 'GET') responses.push(response); };
    page.on('response', observe);
    try {
      await page.getByRole('button', { name: 'Submit Snapshot', exact: true }).click();
      // Assert each actual transfer boundary, rather than timing the entire
      // digest/create/two-part/complete/prepare pipeline as a single response.
      for (const suffix of ['/uploads', '/parts/0', '/parts/1', '/complete']) {
        await expect.poll(() => responses.some(response => response.url().endsWith(suffix)), { intervals: [100], timeout: 3000 }).toBe(true);
        const response = responses.find(response => response.url().endsWith(suffix))!;
        expect(response.ok(), await response.text()).toBe(true);
      }
      const upload = await responses.find(response => response.url().endsWith('/complete'))!.json();
      expect(upload.md5).toBe(createHash('md5').update(payload).digest('hex'));
      expect(upload.parts).toHaveLength(2); expect(upload.length).toBe(String(payload.length));
      await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true })).toHaveCount(0);
      expect(responses.some(response => /\/snapshots\/[0-9a-f]{32}$/.test(response.url()) && response.request().method() === 'PUT')).toBe(true);
      expect(responses.some(response => /\/(compositions|split-drafts)(\/|$)/.test(response.url()))).toBe(false);
    } finally { page.off('response', observe); }
  });
  const head = await call(request, `${datasetPath(name)}/head`);
  expect(head.latest).toMatch(/^[0-9a-f]{32}$/); expect(head.stable).toBeNull();
  const preview = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/preview?sample=${encodeURIComponent(Buffer.from('sample-1').toString('base64'))}&field=text`);
  expect(Buffer.from(preview.data, 'base64').toString()).toBe('native import payload');
  await expect(page.getByRole('table', { name: 'Sample field matrix' })).toContainText('sample-1');
  const split = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/split-head`);
  const revision = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/split-revisions/${split.revision}`);
  expect(revision.splits.map((value: { name: string }) => value.name)).toEqual(['train']);
  const matrix = page.getByRole('table', { name: 'Sample field matrix' });
  await matrix.getByRole('row').filter({ hasText: 'base64: /wA=' }).getByRole('button', { name: `payload · ${payload.length} B` }).click();
  await expect(page.getByRole('button', { name: 'Preview field' })).toBeDisabled();
  await datasetAction(page, 'Edit in child Snapshot…');
  await mapping(page, { version: 1, schema: null,
    samples: [{ kind: 'remove', id: '/wA=' }, { kind: 'upsert', id: Buffer.from('new').toString('base64'), fields: [{ name: 'text', value: { kind: 'tombstone' } }] }] });
  await publishResult(page, 'Snapshot');
  const child = (await call(request, `${datasetPath(name)}/head`)).latest;
  expect((await call(request, `${datasetPath(name)}/snapshots/${child}/summary`)).parent).toBe(head.latest);
  const values = await call(request, `${datasetPath(name)}/snapshots/${child}/matrix`, 'POST', { sample_ids: [Buffer.from('sample-1').toString('base64'), Buffer.from('new').toString('base64')], fields: ['text'] });
  // Composition materializes the effective parent binding in the complete child.
  expect(values.entries[0].fields[0].source_snapshot).toBe(child);
  const inheritedPreview = await call(request, `${datasetPath(name)}/snapshots/${child}/preview?sample=${encodeURIComponent(Buffer.from('sample-1').toString('base64'))}&field=text`);
  expect(inheritedPreview.data).toBe(preview.data);
  const parentPreview = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/preview?sample=${encodeURIComponent(Buffer.from('sample-1').toString('base64'))}&field=text`);
  expect(parentPreview.data).toBe(preview.data);
  expect(values.entries[1].fields[0].storage).toBe('tombstone');
  const childRevision = (await call(request, `${datasetPath(name)}/snapshots/${child}/split-head`)).revision;
  expect((await call(request, `${datasetPath(name)}/snapshots/${child}/split-revisions/${childRevision}/splits/train/members`)).entries).toEqual([Buffer.from('sample-1').toString('base64')]);

});

// Baseline: 3.578s (2026-10-24), failed at the retired capability before resource migration.
test('Dataset native: recovery', async ({ page, request, baseURL }) => {
  const proxy = await consoleProxy(baseURL!);
  const name = `recovery-${Date.now()}`;
  try {
    await create(page, name, `${proxy.url}/?domain=Dataset`);
    const candidate = await page.getByTestId('editing-resource-id').innerText();
    await expect(page.getByText('Edits saved in browser.', { exact: true })).toBeVisible();
    await page.reload();
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
    await datasetAction(page, 'Resume saved request…');
    await expect(page.getByRole('table', { name: 'Pending Sample changes', exact: true })).toContainText('sample-1');
    proxy.dropNextSuccessfulResponse(`/snapshots/${candidate}`);
    await page.getByRole('button', { name: 'Submit Snapshot', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toBeVisible();
    expect(proxy.droppedResponses()).toBe(1);
    expect((await call(request, `${datasetPath(name)}/snapshots/${candidate}/summary`)).sample_count).toBe('1');
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBeNull();
    await page.reload();
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
    await datasetAction(page, 'Resume saved request…');
    proxy.dropNextSuccessfulResponse('/head');
    await page.getByRole('button', { name: 'Submit Snapshot', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toBeVisible();
    expect(proxy.droppedResponses()).toBe(2);
    await page.reload();
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
    await datasetAction(page, 'Resume saved request…');
    await publishResult(page, 'Snapshot');
    const inventory = await call(request, `${datasetPath(name)}/snapshot-inventory`);
    expect(inventory.entries).toHaveLength(1); expect(inventory.entries[0].id).toBe(candidate);
  } finally { await proxy.close(); }
});
