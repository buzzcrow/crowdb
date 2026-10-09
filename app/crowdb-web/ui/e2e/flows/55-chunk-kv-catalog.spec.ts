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
test('native diagnostics: Chunk-KV actual Page observation', async ({ page, request }) => {
  const catalogObservation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
  await page.goto('/?domain=Chunk-KV');
  const catalogResponse = await catalogObservation;
  expect(catalogResponse.ok(), await catalogResponse.text()).toBeTruthy();
  const catalog = await catalogResponse.json();
  const partition = catalog.entries.find((entry: { state: string; transition_id: string | null }) => entry.state === 'Serving' && entry.transition_id === null);
  expect(partition, 'inspect a current serving assignment').toBeDefined();
  const tree = page.getByTestId('chunk-kv-graph').getByRole('button', { name: `KV Tree for ${partition.id}`, exact: true });
  await expect(tree).toHaveAttribute('title', 'Inspect base pages, checkpoint and counters.');
  const pageObservation = page.waitForResponse(response => {
    const url = new URL(response.url());
    return url.pathname === '/api/chunk-kv/runtime' && url.searchParams.has('page_path');
  });
  await tree.click();
  const response = await pageObservation;
  expect(response.ok(), await response.text()).toBeTruthy();
  const observation = await response.json();
  expect(observation.page.rows.length).toBeLessThanOrEqual(20);
  const query = new URL(response.url()).searchParams;
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
      await page.getByRole('button', { name: 'PaxosKV', exact: true }).click();
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

// The slow fixture uses the one-minute production split cooldown.
// Only catalog preparation has that horizon; DOM assertions keep their budget.
test('native diagnostics: production split displays actual inherited and current journals', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NATIVE_TRANSITION_ACCEPTANCE, 'requires the owned slow production-policy fixture');
  test.setTimeout(12 * 60_000);
  const initial = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
  await page.goto('/?domain=Chunk-KV');
  const first = await initial;
  expect(first.ok(), await first.text()).toBe(true);
  await expect(page.getByRole('button', { name: 'Refresh catalog', exact: true })).toBeEnabled();
  const started = Date.now();
  const observed: { catalog?: CatalogPage; partition?: Partition } = {};
  await expect.poll(async () => {
    const observation = page.waitForResponse(response => new URL(response.url()).pathname === '/api/chunk-kv/catalog');
    await page.getByRole('button', { name: 'Refresh catalog', exact: true }).click();
    const response = await observation;
    expect(response.ok(), await response.text()).toBe(true);
    const catalog: CatalogPage = await response.json();
    observed.catalog = catalog;
    await expect(page.getByText(`Generation ${catalog.generation} ·`, { exact: false })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Refresh catalog', exact: true })).toBeEnabled();
    observed.partition = catalog.entries.find(entry => entry.artifact.tail_overlay);
    return Boolean(observed.partition);
  }, { timeout: 11 * 60_000, intervals: [100], message: 'normal policy must publish a real split overlay' }).toBe(true);
  const { catalog, partition } = observed;
  if (!catalog || !partition || !partition.artifact.tail_overlay) throw new Error('Expected an observed catalog partition with a real split overlay');
  console.log(`Observed production split after ${Date.now() - started}ms, generation ${catalog.generation}`);
  await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${partition.id}`, exact: true }).click();
  const overlay = partition.artifact.tail_overlay;
  const identity = (value: { high: string; low: string }) => `${BigInt(value.high).toString(16).padStart(16, '0')}${BigInt(value.low).toString(16).padStart(16, '0')}`;
  await page.getByRole('tab', { name: 'Journal', exact: true }).click();
  const journal = page.getByRole('tabpanel', { name: 'Journal', exact: true });
  const inherited = journal.getByRole('heading', { name: 'Inherited parent stream', exact: true }).locator('..');
  await expect(inherited.getByRole('heading', { name: 'Inherited parent stream', exact: true })).toBeVisible();
  await expect(journal).toContainText(identity(overlay.source_stream_name));
  await expect(journal).toContainText(identity(partition.artifact.stream_name));
  expect(identity(overlay.source_stream_name)).not.toBe(identity(partition.artifact.stream_name));
  for (const [label, value] of [['Replay offset (bytes)', overlay.replay_offset], ['Cutover offset (bytes)', overlay.cutover_offset], ['Manifest generation', overlay.source_stream_manifest_generation]]) {
    await expect(inherited.locator('dt').filter({ hasText: label }).locator('..').locator('dd')).toHaveText(value);
  }
  await page.getByRole('tab', { name: 'Dependencies', exact: true }).click();
  const dependencies = page.getByRole('tabpanel', { name: 'Dependencies', exact: true });
  await expect(dependencies).toContainText(identity(overlay.source_partition_id));
  await expect(dependencies).toContainText(overlay.base_root_manifest_generation);
  const stale = new URLSearchParams({ id: partition.id, epoch: partition.epoch, generation: catalog.generation, page: '0', offset: '0' });
  // A real head advance invalidates this observation; never synthesize a cursor.
  let current = catalog;
  await expect.poll(async () => {
    const response = await request.get('/api/chunk-kv/catalog?page=0&offset=0');
    if (response.status() === 409) {
      expect(await response.json()).toEqual({ error: 'Chunk-KV catalog changed during observation; refresh the range map' });
      return catalog.generation;
    }
    expect(response.ok(), await response.text()).toBe(true);
    current = await response.json();
    if (!current) throw new Error('Expected an observed catalog generation');
    return current.generation;
  }, { timeout: 11 * 60_000, intervals: [100] }).not.toBe(catalog.generation);
  expect((await request.get(`/api/chunk-kv/runtime?${stale}`)).status()).toBe(409);
});

// Baseline: actual large native Journal directory (2026-10-04).
test('native diagnostics: large Journal replaces 100 extent fences with its remainder', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NATIVE_JOURNAL_WINDOWS, 'requires actual small-geometry native stream writes');
  const response = await request.get('/api/chunk-kv/catalog?page=0&offset=0');
  expect(response.ok(), await response.text()).toBe(true);
  const catalog = await response.json();
  let target = null;
  let first = null;
  for (const entry of catalog.entries) {
    const query = new URLSearchParams({ id: entry.id, epoch: entry.epoch, generation: catalog.generation, page: '0', offset: '0' });
    const observed = await request.get(`/api/chunk-kv/runtime?${query}`);
    expect(observed.ok(), await observed.text()).toBe(true);
    const runtime = await observed.json();
    if (runtime.journal.next_offset === 100) { target = entry; first = runtime.journal; break; }
  }
  expect(target).not.toBeNull();
  expect(first.extent_pages).toHaveLength(100);
  const query = new URLSearchParams({ id: target.id, epoch: target.epoch, generation: catalog.generation, page: '0', offset: '0', stream_generation: first.generation, stream_offset: '100' });
  const remainderResponse = await request.get(`/api/chunk-kv/runtime?${query}`);
  expect(remainderResponse.ok(), await remainderResponse.text()).toBe(true);
  const remainder = (await remainderResponse.json()).journal;
  expect(remainder.extent_pages.length).toBeGreaterThan(0);
  expect(remainder.extent_pages.length).toBeLessThan(100);
  expect(remainder.next_offset).toBeNull();
  await page.goto('/?domain=Chunk-KV');
  await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${target.id}`, exact: true }).click();
  await page.getByRole('tab', { name: 'Journal', exact: true }).click();
  const journal = page.getByRole('tabpanel', { name: 'Journal', exact: true });
  const fences = journal.getByLabel('Extent page map').getByRole('button');
  await expect(fences).toHaveCount(100);
  await journal.getByRole('button', { name: 'Next extent pages', exact: true }).click();
  await expect(fences).toHaveCount(remainder.extent_pages.length);
  await expect(fences).toHaveText(remainder.extent_pages.map((entry: { page_index: string; first_logical: string; end_logical: string }) => `Extent page ${entry.page_index}[${entry.first_logical}, ${entry.end_logical})`));
  await expect(journal.getByRole('button', { name: 'Next extent pages', exact: true })).toBeDisabled();
  const selected = remainder.extent_pages[0];
  await journal.getByLabel('Extent page map').getByRole('button', { name: new RegExp(`^Extent page ${selected.page_index}\\b`) }).click();
  await expect(page.getByRole('complementary', { name: 'Selected extent page', exact: true })).toContainText(selected.page_index);
  await journal.getByRole('button', { name: 'Inspect active Chunk', exact: true }).click();
  await expect(page.getByLabel('Exact Chunk ID')).toHaveValue(first.active.chunk_id);
  await page.goBack();
  await expect(fences).toHaveCount(remainder.extent_pages.length);
  await expect(page.getByRole('complementary', { name: 'Selected extent page', exact: true })).toContainText(selected.page_index);
  await page.goForward();
  await expect(page.getByLabel('Exact Chunk ID')).toHaveValue(first.active.chunk_id);
  await page.goBack();
  await expect(fences).toHaveCount(remainder.extent_pages.length);
  await journal.getByRole('button', { name: 'Previous extent pages', exact: true }).click();
  await expect(fences).toHaveCount(100);
  await journal.getByRole('button', { name: 'Next extent pages', exact: true }).click();
  await expect(fences).toHaveCount(remainder.extent_pages.length);
  const owner = `chunk-kv-${target.owner_id}`;
  expect((await request.post(`/api/services/${owner}/restart`, { data: {} })).ok()).toBe(true);
  expect((await request.get(`/api/chunk-kv/runtime?${query}`)).status()).toBe(409);
  await page.getByRole('region', { name: 'Partition runtime', exact: true }).getByRole('button', { name: 'Refresh runtime', exact: true }).click();
  await expect(fences).toHaveCount(100);
  await expect(journal).toContainText('offset 0');
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
