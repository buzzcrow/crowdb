// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { createHash } from 'node:crypto';
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, openDataset, prepareDraft, publishDraft } from '../fixtures/dataset';
import { consoleProxy } from '../fixtures/consoleProxy';
import { step } from '../fixtures/stepTimer';

async function create(page: import('@playwright/test').Page, name: string, url = '/?domain=Dataset') {
  await page.goto(url);
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
// Baseline: 6.5s (2026-10-16), before multipart and inherited-child coverage.
test('Dataset native: import', async ({ page, request }) => {
  const name = `import-${Date.now()}`;
  await step('setup', () => create(page, name));
  const payload = Buffer.alloc(4 * 1024 * 1024 + 17, 0xa5);
  await page.getByLabel('Local payload files').setInputFiles({ name: 'payload.bin', mimeType: 'application/octet-stream', buffer: payload });
  await page.getByRole('textbox', { name: 'Sample mapping JSON' }).fill(JSON.stringify({ version: 1,
    schema: { version: 1, fields: [{ name: 'text', required: false }, { name: 'payload', required: false }] },
    samples: [
      { kind: 'upsert', id: Buffer.from('sample-1').toString('base64'), fields: [{ name: 'text', value: { kind: 'inline', data: Buffer.from('native import payload').toString('base64') } }] },
      { kind: 'upsert', id: '/wA=', fields: [{ name: 'payload', value: { kind: 'file', file: 'payload.bin' } }] },
    ] }));
  await page.getByRole('button', { name: 'Add selection Split' }).click();
  await page.getByLabel('Split 1 name').fill('train');
  await page.getByRole('button', { name: 'Review mapping' }).click();
  await step('mutation response', async () => {
    const responses: import('@playwright/test').Response[] = [];
    const observe = (response: import('@playwright/test').Response) => { if (response.url().includes(`${datasetPath(name)}/`) && response.request().method() !== 'GET') responses.push(response); };
    page.on('response', observe);
    try {
      await page.getByRole('button', { name: 'Save draft and upload local data' }).click();
      // Assert each actual transfer boundary, rather than timing the entire
      // digest/create/two-part/complete/edit pipeline as a single response.
      for (const suffix of ['/compositions', '/uploads', '/parts/0', '/parts/1', '/complete']) {
        await expect.poll(() => responses.some(response => response.url().endsWith(suffix)), { intervals: [100], timeout: 3000 }).toBe(true);
        const response = responses.find(response => response.url().endsWith(suffix))!;
        expect(response.ok(), await response.text()).toBe(true);
      }
      const upload = await responses.find(response => response.url().endsWith('/complete'))!.json();
      expect(upload.md5).toBe(createHash('md5').update(payload).digest('hex'));
      expect(upload.parts).toHaveLength(2); expect(upload.length).toBe(String(payload.length));
      await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
    } finally { page.off('response', observe); }
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
  const matrix = page.getByRole('table', { name: 'Sample field matrix' });
  await matrix.getByRole('row').filter({ hasText: 'base64: /wA=' }).getByRole('button', { name: `payload · ${payload.length} B` }).click();
  await expect(page.getByRole('button', { name: 'Preview field' })).toBeDisabled();
  await page.getByRole('button', { name: 'Create child Snapshot' }).click();
  await page.getByRole('textbox', { name: 'Sample mapping JSON' }).fill(JSON.stringify({ version: 1, schema: null,
    samples: [{ kind: 'remove', id: '/wA=' }, { kind: 'upsert', id: Buffer.from('new').toString('base64'), fields: [{ name: 'text', value: { kind: 'tombstone' } }] }] }));
  await page.getByRole('button', { name: 'Review mapping' }).click();
  await page.getByRole('button', { name: 'Save draft and upload local data' }).click();
  await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
  await publishDraft(page, 'Snapshot');
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

// Baseline: 5.9s (2026-10-16).
test('Dataset native: recovery', async ({ page, request, baseURL }) => {
  const proxy = await consoleProxy(baseURL!);
  const name = `recovery-${Date.now()}`;
  try {
    await create(page, name, `${proxy.url}/?domain=Dataset`);
    await page.getByRole('button', { name: 'Review mapping' }).click();
    await page.getByRole('button', { name: 'Save draft and upload local data' }).click();
    await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
    await page.reload();
    await page.getByRole('navigation', { name: 'Dataset tree' }).getByRole('button', { name, exact: true }).click();
    await page.getByRole('button', { name: 'Resume saved draft' }).click();
    await expect(page.getByRole('button', { name: 'Evaluate complete membership' })).toBeEnabled();
    await prepareDraft(page, 'Snapshot');
    const publication = page.getByRole('region', { name: 'Draft publication' });
    const operation = (await publication.innerText()).match(/Operation ([0-9a-f]{32})/)![1];
    const candidate = (await call(request, `${datasetPath(name)}/compositions/${operation}`)).preparation.snapshot;
    await page.reload();
    await page.getByRole('navigation', { name: 'Dataset tree' }).getByRole('button', { name, exact: true }).click();
    await page.getByRole('button', { name: 'Resume saved draft' }).click();
    await expect(page.getByRole('button', { name: 'Publish Snapshot', exact: true })).toBeEnabled();
    proxy.dropNextSuccessfulResponse('/publish');
    await page.getByRole('button', { name: 'Publish Snapshot', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Reconcile outcome' })).toBeEnabled();
    expect(proxy.droppedResponses()).toBe(1);
    await page.reload();
    await page.getByRole('navigation', { name: 'Dataset tree' }).getByRole('button', { name, exact: true }).click();
    await page.getByRole('button', { name: 'Resume saved draft' }).click();
    await page.getByRole('button', { name: 'Reconcile outcome' }).click();
    await expect(page.getByRole('dialog')).toHaveCount(0);
    const inventory = await call(request, `${datasetPath(name)}/snapshot-inventory`);
    expect(inventory.entries).toHaveLength(1); expect(inventory.entries[0].id).toBe(candidate);
  } finally { await proxy.close(); }
});
