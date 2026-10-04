// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: partition/overlay 1.4s, unavailable 0.208s, journal 0.534s, placement 0.321s (2026-10-03).
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: journal identity survives Chunk navigation and owner interruption', async ({ page, request }) => {
  const bucket = `journal-${process.pid}-${Date.now()}`;
  expect((await request.put(`/api/access/s3/${bucket}`)).ok()).toBe(true);
  const object = `/api/access/s3/${bucket}/value`;
  try {
    expect((await request.put(object, { data: 'native journal observation' })).ok()).toBe(true);
    const response = await request.get('/api/chunk-kv/catalog?page=0&offset=0');
    expect(response.ok(), await response.text()).toBe(true);
    const catalog = await response.json();
    // Earlier native cases can split the catalog; the first range need not
    // contain this write. Select a current range with a real active journal.
    let partition = null;
    for (const entry of catalog.entries) {
      const query = new URLSearchParams({ id: entry.id, epoch: entry.epoch, generation: catalog.generation, page: '0', offset: '0' });
      const response = await request.get(`/api/chunk-kv/runtime?${query}`);
      expect(response.ok(), await response.text()).toBe(true);
      if ((await response.json()).journal.active) { partition = entry; break; }
    }
    expect(partition).not.toBeNull();
    await page.goto('/?domain=Chunk-KV');
    const runtimeResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/runtime');
    await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${partition.id}`, exact: true }).click();
    const observed = await runtimeResponse;
    expect(observed.ok(), await observed.text()).toBe(true);
    const runtime = await observed.json();
    expect(runtime.journal.active).not.toBeNull();
    await page.getByRole('tab', { name: 'Journal', exact: true }).click();
    const journal = page.getByRole('tabpanel', { name: 'Journal', exact: true });
    await expect(journal).toContainText(runtime.journal.active.chunk_id);
    for (const [name, value] of [['Stream manifest generation', runtime.journal.generation], ['Stream writer epoch', runtime.journal.writer_epoch], ['Metadata group', runtime.journal.metadata_group_id]]) {
      await expect(journal.locator('dt').filter({ hasText: new RegExp(`^${name}$`) }).locator('..').locator('dd')).toHaveText(value);
    }
    await expect(journal.getByLabel('Extent page map').getByRole('button')).toHaveCount(runtime.journal.extent_pages.length);
    await journal.getByRole('button', { name: 'Inspect active Chunk', exact: true }).click();
    await expect(page.getByLabel('Exact Chunk ID')).toHaveValue(runtime.journal.active.chunk_id);
    await page.goBack();
    await expect(journal).toBeVisible();
    const owner = `chunk-kv-${partition.owner_id}`;
    expect((await request.post(`/api/services/${owner}/stop`, { data: {} })).ok()).toBe(true);
    try {
      const observation = page.getByRole('region', { name: 'Partition runtime', exact: true });
      await observation.getByRole('button', { name: 'Refresh runtime', exact: true }).click();
      await expect(observation).toContainText('Runtime unavailable');
      await expect(journal.getByLabel('Extent page map')).toHaveCount(0);
      await expect(journal.getByRole('button', { name: 'Inspect active Chunk', exact: true })).toHaveCount(0);
    } finally {
      expect((await request.post(`/api/services/${owner}/restart`, { data: {} })).ok()).toBe(true);
    }
    const observation = page.getByRole('region', { name: 'Partition runtime', exact: true });
    await observation.getByRole('button', { name: 'Refresh runtime', exact: true }).click();
    await expect(observation).not.toContainText('Runtime unavailable');
    await expect(journal.getByRole('heading', { name: 'Published extent index', exact: true })).toBeVisible();
    const stale = new URL(observed.url());
    stale.searchParams.set('stream_generation', runtime.journal.generation);
    stale.searchParams.set('stream_offset', '0');
    expect((await request.get(stale.toString())).status()).toBe(409);
  } finally {
    expect((await request.delete(object)).status()).toBe(204);
    expect((await request.delete(`/api/access/s3/${bucket}`)).status()).toBe(204);
  }
});

// Baseline: new actual native Page flow (2026-10-04).
test('native diagnostics: Chunk-KV actual Page observation', async ({ page, request }) => {
  await page.goto('/?domain=Chunk-KV');
  const catalogResponse = await request.get('/api/chunk-kv/catalog?page=0&offset=0');
  expect(catalogResponse.ok(), await catalogResponse.text()).toBeTruthy();
  const catalog = await catalogResponse.json();
  const partition = catalog.entries[0];
  const query = new URLSearchParams({ id: partition.id, epoch: partition.epoch, generation: catalog.generation, page: '0', offset: '0', page_path: '' });
  const response = await request.get(`/api/chunk-kv/runtime?${query}`);
  expect(response.ok(), await response.text()).toBeTruthy();
  const observation = await response.json();
  expect(observation.page.rows.length).toBeLessThanOrEqual(20);
  await page.getByTestId('chunk-kv-graph').getByRole('button', { name: `KV Tree for ${partition.id}`, exact: true }).click();
  const explorer = page.getByRole('region', { name: 'KV Page explorer', exact: true });
  await expect(explorer.getByRole('button', { name: `Root ${observation.page.root}`, exact: true })).toBeVisible();
  await expect(explorer).toContainText(`${observation.page.kind === 'inner' ? 'Inner' : 'Leaf'} Page ${observation.page.id}`);
  await expect(explorer.getByRole('table', { name: 'KV Page entries' }).getByRole('row')).toHaveCount(observation.page.rows.length + 1);
  await explorer.getByLabel('UTF-8 text').check();
  await expect(explorer.getByRole('columnheader', { name: /UTF-8/ })).toBeVisible();
  await page.screenshot({ path: '/tmp/crowdb-kv-page-inspector.png', fullPage: true });
  query.set('tree_version', '18446744073709551614');
  expect((await request.get(`/api/chunk-kv/runtime?${query}`)).status()).toBe(409);
  query.delete('tree_version'); query.set('entry_offset', '20');
  expect((await request.get(`/api/chunk-kv/runtime?${query}`)).status()).toBe(400);
});

test('native diagnostics: Chunk-KV graph survives tab changes, refresh and resizing', async ({ page, request }) => {
  const { step } = await import('../fixtures/stepTimer');
  const response = await request.get('/api/chunk-kv/catalog?page=0&offset=0');
  expect(response.status()).toBe(200);
  let catalog = await response.json();
  expect(catalog.entries.length).toBeGreaterThan(0);
  const observeCatalog = async (action: () => Promise<unknown>) => {
    const observation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
    await action();
    const response = await observation;
    expect(response.status()).toBe(200);
    catalog = await response.json();
    expect(catalog.entries.length).toBeGreaterThan(0);
  };
  await step('native graph DOM setup', async () => {
    await observeCatalog(() => page.goto('/?domain=Chunk-KV'));
    await expect(page.getByTestId('chunk-kv-graph')).toBeVisible();
  });
  const graph = page.getByTestId('chunk-kv-graph');
  const verify = async () => {
    await expect(graph.getByRole('button', { name: 'Chunk-KV', exact: true })).toBeVisible();
    await expect(graph.getByRole('button', { name: /^CKV-/ })).toHaveCount(3);
    await expect(graph.getByRole('button', { name: /^Partition / })).toHaveCount(catalog.entries.length);
    await expect(graph.getByRole('button', { name: /^KV Tree for / })).toHaveCount(catalog.entries.length);
    await expect.poll(async () => graph.evaluate(element => {
      const outer = element.getBoundingClientRect();
      const cards = [...element.querySelectorAll('.react-flow__node')];
      return cards.length > 0 && cards.every(card => {
        const rect = card.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0 && rect.left >= outer.left && rect.right <= outer.right
          && rect.top >= outer.top && rect.bottom <= outer.bottom;
      });
    }), { intervals: [100] }).toBe(true);
  };
  await step('native graph initial visible bounds', verify);
  await step('native graph tab return', async () => {
    for (let index = 0; index < 3; index++) {
      await page.getByRole('button', { name: 'KV', exact: true }).click();
      await observeCatalog(() => page.getByRole('button', { name: 'Chunk-KV', exact: true }).click());
      await verify();
    }
  });
  await step('native graph panel resize', async () => {
    const divider = page.getByRole('separator', { name: 'Sidebar width' });
    const rect = await divider.boundingBox();
    expect(rect).not.toBeNull();
    await page.mouse.move(rect!.x + rect!.width / 2, rect!.y + 40);
    await page.mouse.down();
    await page.mouse.move(rect!.x + 180, rect!.y + 40);
    await page.mouse.up();
    await verify();
  });
  await step('native graph catalog refresh', async () => {
    for (let index = 0; index < 3; index++) {
      await observeCatalog(() => page.getByRole('button', { name: 'Refresh catalog', exact: true }).click());
      await expect(page.getByRole('button', { name: 'Refresh catalog', exact: true })).toBeEnabled();
      await verify();
    }
  });
});
