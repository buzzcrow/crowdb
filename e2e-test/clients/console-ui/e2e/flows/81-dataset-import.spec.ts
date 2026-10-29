// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { datasetAction } from '../fixtures/resourceMenus';
import { createHash } from 'node:crypto';
import { test, expect } from '../fixtures/realBackend';
import { call, datasetPath, mapping, publishResult, addField, addSample, textCell, resumeDraft, uploadFiles } from '../fixtures/dataset';
import { consoleProxy } from '../fixtures/consoleProxy';
import { step } from '../fixtures/stepTimer';

async function create(page: import('@playwright/test').Page, name: string, url = '/?domain=Dataset') {
  await page.goto(url);
  await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: 'Datasets', exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Create Dataset…', exact: true }).click();
  await page.getByRole('textbox', { name: 'New Dataset name' }).fill(name);
  await expect(page.getByRole('dialog', { name: 'Create Dataset', exact: true })).toHaveCount(0);
  await expect(page.getByRole('region', { name: 'Dataset actions', exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'Refresh Dataset', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Dataset actions', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Upload target field')).toHaveCount(0);
  const editor = page.getByRole('table', { name: 'Snapshot editor table', exact: true });
  await expect(editor.locator('thead').getByRole('button', { name: 'Add Field', exact: true })).toBeVisible();
  await expect(editor.locator('tfoot').getByRole('button', { name: 'Add Sample', exact: true })).toBeVisible();
  await addField(page, 'text');
  await addSample(page, 'sample-1');
  await textCell(page, 'sample-1', 'text', 'native import payload');

}
// Baseline: 10.5s (2026-10-29), combined creation and bounded scrolling before pinning Add Sample.
test('Dataset native: import', async ({ page, request }) => {
  const name = `import-${Date.now()}`;
  await step('setup', () => create(page, name));
  expect((await request.get(datasetPath(name))).status()).toBe(404);
  const local = (await page.getByTestId('editing-resource-id').textContent())!;
  await page.getByRole('button', { name: 'Save Draft', exact: true }).click();
  await expect(page.getByText('Saved locally', { exact: true })).toBeVisible();
  await page.reload(); await resumeDraft(page, name, local, true);
  await expect(page.getByLabel('text for sample-1', { exact: true })).toHaveValue('native import payload');
  await step('new Field text and file stay in the selected first row', async () => {
    await addSample(page, 'sample-2');
    await addField(page, 'note');
    await textCell(page, 'sample-1', 'note', 'first row only');
    const ids = page.getByRole('textbox', { name: /^Sample ID for/ });
    expect(await ids.evaluateAll(nodes => nodes.map(node => (node as HTMLInputElement).value))).toEqual(['sample-1', 'sample-2']);
    await expect(page.getByLabel('note for sample-2', { exact: true })).toHaveValue('');
    await page.getByLabel('Upload note for sample-1', { exact: true }).setInputFiles({ name: 'first.txt', mimeType: 'text/plain', buffer: Buffer.from('first file') });
    await expect(page.getByRole('button', { name: 'first.txt · 10 B', exact: true })).toBeVisible();
    expect(await ids.evaluateAll(nodes => nodes.map(node => (node as HTMLInputElement).value))).toEqual(['sample-1', 'sample-2']);
    await expect(page.getByLabel('note for sample-2', { exact: true })).toHaveValue('');
    const center = page.locator('[data-navigation-scroll="workbench-center"]').filter({ has: page.getByRole('region', { name: 'Import Snapshot', exact: true }) });
    expect(await center.evaluate(element => ({ left: element.scrollLeft, overflow: element.scrollWidth > element.clientWidth }))).toEqual({ left: 0, overflow: false });
    await expect(center.getByRole('button', { name: 'Add Sample', exact: true })).toBeInViewport();
  });
  const payload = Buffer.alloc(4 * 1024 * 1024 + 17, 0xa5);
  await addField(page, 'payload');
  await step('mutation response', async () => {
    const responses: import('@playwright/test').Response[] = [];
    const observe = (response: import('@playwright/test').Response) => { if (response.url().includes(`${datasetPath(name)}/`) && response.request().method() !== 'GET') responses.push(response); };
    page.on('response', observe);
    try {
      await uploadFiles(page, { name: 'payload.bin', mimeType: 'application/octet-stream', buffer: payload }, 'payload');
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
      await expect(page.getByRole('button', { name: 'Create Dataset', exact: true })).toBeEnabled();
      expect((await call(request, `${datasetPath(name)}/head`)).latest).toBeNull();
      await mapping(page, { version: 1, schema: { version: 1, fields: [{ name: 'text', required: false }, { name: 'payload', required: false }] }, samples: [
        { kind: 'upsert', id: Buffer.from('sample-1').toString('base64'), fields: [{ name: 'text', value: { kind: 'inline', data: Buffer.from('native import payload').toString('base64') } }] },
        { kind: 'upsert', id: '/wA=', fields: [{ name: 'payload', value: { kind: 'upload', id: upload.id, length: upload.length, md5: upload.md5 } }] },
      ] });
      await page.getByText('Advanced', { exact: true }).click();
      await page.screenshot({ path: test.info().outputPath('dataset-table-draft.png'), fullPage: true });
      const draftId = (await page.getByTestId('editing-resource-id').textContent())!;
      await page.getByRole('button', { name: 'Save Draft', exact: true }).click();
      await expect(page.getByText('Saved locally', { exact: true })).toBeVisible();
      const stored = await page.evaluate(async () => new Promise<{ bytes: number; blobs: boolean }>((resolve, reject) => {
        const open = indexedDB.open('crowdb-dataset-requests-v2', 1); open.onerror = () => reject(open.error);
        open.onsuccess = () => { const db = open.result; const request = db.transaction('requests').objectStore('requests').getAll();
          request.onsuccess = () => { const hasBlob = (value: unknown): boolean => value instanceof Blob || !!value && typeof value === 'object' && Object.values(value).some(hasBlob); resolve({ bytes: JSON.stringify(request.result).length, blobs: hasBlob(request.result) }); db.close(); };
        };
      }));
      expect(stored.blobs).toBe(false); expect(stored.bytes).toBeLessThan(16000);
      const uploadCount = responses.filter(value => value.url().includes('/uploads')).length;
      await page.reload(); await resumeDraft(page, name, draftId, true);
      await expect(page.getByRole('table', { name: 'Snapshot editor table' })).toContainText('base64: /wA=');
      expect(responses.filter(value => value.url().includes('/uploads')).length).toBe(uploadCount);
      await page.getByRole('button', { name: 'Create Dataset', exact: true }).click();
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
  await expect(page.getByText(/20 Samples × 12 Schema fields/)).toHaveCount(0);
  await datasetAction(page, 'Create Split…');
  await page.getByLabel('Split 1 name').fill('train');
  await page.getByRole('button', { name: 'View train', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Split preview', exact: true }).getByRole('table')).toContainText('sample-1');
  await publishResult(page, 'Split revision');
  const split = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/split-head`);
  const revision = await call(request, `${datasetPath(name)}/snapshots/${head.latest}/split-revisions/${split.revision}`);
  expect(revision.splits.map((value: { name: string }) => value.name)).toEqual(['train']);
  const matrix = page.getByRole('table', { name: 'Sample field matrix' });
  await matrix.getByRole('row').filter({ hasText: 'base64: /wA=' }).getByRole('button', { name: `payload · ${payload.length} B` }).click();
  await expect(page.getByRole('button', { name: 'Preview field' })).toBeDisabled();
  await datasetAction(page, 'Edit in child Snapshot…');
  await mapping(page, { version: 1, schema: null,
    samples: [{ kind: 'remove', id: '/wA=' }, { kind: 'upsert', id: Buffer.from('new').toString('base64'), fields: [{ name: 'text', value: { kind: 'tombstone' } }] }] });
  await page.getByLabel('Keep parent Splits').check();
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
  await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
  await datasetAction(page, 'New empty Snapshot…');
  const rootEditor = page.getByRole('region', { name: 'Import Snapshot', exact: true });
  await expect(rootEditor).not.toContainText('Parent:');
  await expect(rootEditor.getByRole('textbox', { name: /^Sample ID for/ })).toHaveCount(0);
  await expect(rootEditor.getByRole('table').getByRole('button', { name: 'text', exact: true })).toHaveCount(0);
  await addSample(page, 'independent'); await textCell(page, 'independent', 'content', 'new root');
  await publishResult(page, 'Snapshot');
  const independent = (await call(request, `${datasetPath(name)}/head`)).latest;
  expect((await call(request, `${datasetPath(name)}/snapshots/${independent}/summary`)).parent).toBeNull();
  expect((await call(request, `${datasetPath(name)}/snapshots/${independent}/samples`)).entries).toEqual([Buffer.from('independent').toString('base64')]);
  expect((await call(request, `${datasetPath(name)}/snapshots/${child}/summary`)).parent).toBe(head.latest);


});

// Baseline: 6.3s (2026-10-29), upload recovery and restored local Draft feedback.
test('Dataset native: recovery', async ({ page, request, baseURL, context }) => {
  const proxy = await consoleProxy(baseURL!);
  const name = `recovery-${Date.now()}`;
  try {
    await create(page, name, `${proxy.url}/?domain=Dataset`);
    const candidate = await page.getByTestId('editing-resource-id').textContent();
    const file = { name: 'resume.txt', mimeType: 'text/plain', buffer: Buffer.from('resume completed parts') };
    let partRequests = 0;
    page.on('request', request => { if (request.url().endsWith('/parts/0')) partRequests++; });
    proxy.dropNextSuccessfulResponse('/parts/0');
    await page.getByLabel('Upload content for sample-1', { exact: true }).setInputFiles(file);
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toBeVisible();
    await page.reload(); await resumeDraft(page, name, candidate!, true);
    await expect(page.getByRole('button', { name: 'Create Dataset', exact: true })).toBeEnabled();
    await page.getByLabel('Upload content for sample-1', { exact: true }).setInputFiles(file);
    await expect(page.getByRole('button', { name: 'resume.txt · 22 B', exact: true })).toHaveText('resume.txt · 22 B');
    expect(partRequests).toBe(1);

    await page.getByRole('button', { name: 'Save Draft', exact: true }).click();
    await expect(page.getByText('Saved locally', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Close editor', exact: true }).click();
    await page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name, exact: true }).click();
    await datasetAction(page, 'New empty Snapshot…');
    const other = (await page.getByTestId('editing-resource-id').textContent())!;
    expect(other).not.toBe(candidate);
    await page.getByRole('button', { name: 'Save Draft', exact: true }).click();
    await expect(page.getByText('Saved locally', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Close editor', exact: true }).click();
    await expect(page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: /^(Snapshot|Dataset) Draft / })).toHaveCount(2);
    await resumeDraft(page, name, candidate!, true);
    const stale = await context.newPage();
    try {
      await stale.goto(`${proxy.url}/?domain=Dataset`); await resumeDraft(stale, name, candidate!, true);
      await expect(stale.getByLabel('Sample ID for sample-1', { exact: true })).toHaveValue('sample-1');
      await textCell(page, 'sample-1', 'text', 'native import payload');
      await page.getByRole('button', { name: 'Save Draft', exact: true }).click();
      await expect(page.getByText('Saved locally', { exact: true })).toBeVisible();
      await stale.getByRole('button', { name: 'Save Draft', exact: true }).click();
      await expect(stale.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toContainText('changed in another tab');
      await stale.getByRole('button', { name: 'Create Dataset', exact: true }).click();
      expect((await call(request, `${datasetPath(name)}/head`)).latest).toBeNull();
    } finally { await stale.close(); }
    await page.getByRole('button', { name: 'Close editor', exact: true }).click();
    await resumeDraft(page, name, other);
    await page.getByRole('button', { name: 'Discard Draft', exact: true }).click();
    await expect(page.getByRole('navigation', { name: 'Dataset tree', exact: true }).getByRole('button', { name: /^(Snapshot|Dataset) Draft / })).toHaveCount(1);
    await page.reload();
    await resumeDraft(page, name, candidate!, true);
    await expect(page.getByLabel('Sample ID for sample-1', { exact: true })).toHaveValue('sample-1');
    proxy.dropNextSuccessfulResponse(`/snapshots/${candidate}`);
    await page.getByRole('button', { name: 'Create Dataset', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toBeVisible();
    expect(proxy.droppedResponses()).toBe(2);
    expect((await call(request, `${datasetPath(name)}/snapshots/${candidate}/summary`)).sample_count).toBe('1');
    expect((await call(request, `${datasetPath(name)}/head`)).latest).toBeNull();
    await page.reload();
    await resumeDraft(page, name, candidate!, true);
    proxy.dropNextSuccessfulResponse('/head');
    await page.getByRole('button', { name: 'Create Dataset', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Import Snapshot', exact: true }).getByRole('alert')).toBeVisible();
    expect(proxy.droppedResponses()).toBe(3);
    await page.reload();
    await resumeDraft(page, name, candidate!, true);
    await publishResult(page, 'Dataset');
    const inventory = await call(request, `${datasetPath(name)}/snapshot-inventory`);
    expect(inventory.entries).toHaveLength(1); expect(inventory.entries[0].id).toBe(candidate);
  } finally { await proxy.close(); }
});
