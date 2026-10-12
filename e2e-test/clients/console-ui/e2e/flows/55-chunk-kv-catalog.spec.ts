// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: partition/overlay 1.4s, unavailable 0.208s, journal 0.534s, placement 0.321s (2026-10-03).
import { test, expect } from '../fixtures/realBackend';
import type { APIRequestContext, APIResponse, Page, Response } from '@playwright/test';
import type { CatalogPage, Partition } from '../../src/chunk-kv/catalog';

test('native diagnostics: journal identity survives Chunk navigation and owner interruption', async ({ page, request }) => {
  const bucket = `journal-${process.pid}-${Date.now()}`;
  const object = `/api/access/s3/${bucket}/value`;
  try {
    expect((await request.put(`/api/access/s3/${bucket}`)).ok()).toBe(true);
    expect((await request.put(object, { data: 'native journal observation' })).ok()).toBe(true);
    const catalogObservation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
    await page.goto('/?domain=Chunk-KV');
    const response = await catalogObservation;
    expect(response.ok(), await response.text()).toBe(true);
    const { partition, observed } = await selectActiveJournal(page, request, await response.json());
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
    // Back navigation mounts a fresh runtime observation. Complete it before
    // stopping its owner so the unavailable refresh starts from an idle view.
    await expect(page.getByRole('region', { name: 'Partition runtime', exact: true }).getByRole('button', { name: 'Refresh runtime', exact: true })).toBeEnabled();
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
    const refreshedMap = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
    await page.getByRole('button', { name: 'Refresh catalog', exact: true }).click();
    const mapResponse = await refreshedMap;
    expect(mapResponse.ok(), await mapResponse.text()).toBe(true);
    const restored = await selectActiveJournal(page, request, await mapResponse.json(), partition.id);
    expect(restored.partition.id).toBe(partition.id);
    await page.getByRole('tab', { name: 'Journal', exact: true }).click();
    const observation = page.getByRole('region', { name: 'Partition runtime', exact: true });
    await expect(observation).not.toContainText('Runtime unavailable');
    await expect(journal.getByRole('heading', { name: 'Published extent index', exact: true })).toBeVisible();
    const stale = new URL(observed.url());
    stale.searchParams.set('stream_generation', runtime.journal.generation);
    stale.searchParams.set('stream_offset', '0');
    expect((await request.get(stale.toString())).status()).toBe(409);
  } finally {
    try {
      expect((await request.delete(object)).status()).toBe(204);
    } finally {
      expect((await request.delete(`/api/access/s3/${bucket}`)).status()).toBe(204);
    }
  }
});

// Baseline: new actual native Page flow (2026-10-04).
test('native diagnostics: Chunk-KV actual Page observation', async ({ page, request }, testInfo) => {
  const catalogObservation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
  await page.goto('/?domain=Chunk-KV');
  const catalogResponse = await catalogObservation;
  expect(catalogResponse.ok(), await catalogResponse.text()).toBeTruthy();
  const catalog = await catalogResponse.json();
  const partition = catalog.entries.find((entry: { state: string; transition_id: string | null }) => entry.state === 'Serving' && entry.transition_id === null);
  expect(partition, 'inspect a current serving assignment').toBeDefined();
  const tree = page.getByTestId('chunk-kv-graph').getByRole('button', { name: `KV Tree for ${partition.id}`, exact: true });
  await expect(tree).toHaveAttribute('title', 'Inspect base pages, checkpoint and counters.');
  await expect(page.getByTestId('chunk-kv-graph')).not.toContainText('Weight');
  const pageObservation = page.waitForResponse(response => {
    const url = new URL(response.url());
    return url.pathname === '/api/chunk-kv/runtime' && url.searchParams.has('page_path');
  });
  await tree.click();
  const response = await pageObservation;
  expect(response.ok(), await response.text()).toBeTruthy();
  const observation = await response.json();
  const properties = page.getByRole('region', { name: 'Split properties', exact: true });
  await expect(properties.locator('dt').filter({ hasText: /^Weight$/ })).toHaveCount(0);
  await page.getByRole('tab', { name: 'Overview', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Balance explanation', exact: true })).toHaveCount(0);
  await page.getByRole('tab', { name: 'Tree', exact: true }).click();
  expect(observation.page.rows.length).toBeLessThanOrEqual(20);
  const query = new URL(response.url()).searchParams;
  const explorer = page.getByRole('region', { name: 'KV Page explorer', exact: true });
  await expect(explorer.getByRole('button', { name: `Root ${observation.page.root}`, exact: true })).toBeVisible();
  await expect(explorer).toContainText(`${observation.page.kind === 'inner' ? 'Inner' : 'Leaf'} Page ${observation.page.id}`);
  await expect(explorer.getByRole('table', { name: 'KV Page entries' }).getByRole('row')).toHaveCount(observation.page.rows.length + 1);
  await explorer.getByLabel('UTF-8 text').check();
  await expect(explorer.getByRole('columnheader', { name: /UTF-8/ })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath('kv-page-inspector.png'), fullPage: true });
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
    await expect(graph.getByRole('button', { name: 'ChunkKV', exact: true })).toBeVisible();
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
      await page.getByRole('button', { name: 'PaxosKV', exact: true }).click();
      await observeCatalog(() => page.getByRole('navigation', { name: 'Console domains' }).getByRole('button', { name: 'ChunkKV', exact: true }).click());
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

async function selectActiveJournal(page: Page, request: APIRequestContext, initial: CatalogPage, partitionId?: string) {
  let catalog = initial;
  let selected: { partition: Partition; observed: Response } | null = null;
  const refresh = async (action: () => Promise<unknown>) => {
    const observation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
    await action();
    const response = await observation;
    expect(response.ok(), await response.text()).toBe(true);
    catalog = await response.json();
  };
  const changed = async (response: APIResponse | Response) => {
    if (response.status() !== 409) return false;
    expect((await response.json()).error).toMatch(/^(Chunk-KV catalog changed; refresh the range map|Catalog changed during runtime observation|Owner, catalog or tree page observation changed; refresh the catalog or page root)$/);
    await refresh(() => page.getByRole('button', { name: 'Refresh catalog', exact: true }).click());
    return true;
  };
  // Observe a live journal from a coherent current map. A concurrent catalog
  // publication explicitly invalidates the old selection and requires the UI refresh.
  await expect.poll(async () => {
    for (const entry of catalog.entries) {
      if (partitionId && entry.id !== partitionId) continue;
      if (entry.state !== 'Serving') continue;
      const query = new URLSearchParams({ id: entry.id, epoch: entry.epoch, generation: catalog.generation, page: String(catalog.page), offset: String(catalog.offset) });
      const response = await request.get(`/api/chunk-kv/runtime?${query}`);
      if (await changed(response)) return false;
      expect(response.ok(), await response.text()).toBe(true);
      if (!(await response.json()).journal.active) continue;
      await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${entry.id}`, exact: true }).click();
      // Selecting the same partition leaves its identity unchanged. Explicitly
      // refresh runtime so owner restart is observed even without a new catalog.
      const observation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/runtime');
      await page.getByRole('region', { name: 'Partition runtime', exact: true }).getByRole('button', { name: 'Refresh runtime', exact: true }).click();
      const observed = await observation;
      if (await changed(observed)) return false;
      expect(observed.ok(), await observed.text()).toBe(true);
      selected = { partition: entry, observed };
      return true;
    }
    if (!catalog.next) throw new Error('No current catalog range has an active journal');
    await refresh(() => page.getByRole('button', { name: 'Next partitions', exact: true }).click());
    return false;
  }, { intervals: [100] }).toBe(true);
  if (!selected) throw new Error('No coherent journal observation');
  return selected as { partition: Partition; observed: Response };
}
