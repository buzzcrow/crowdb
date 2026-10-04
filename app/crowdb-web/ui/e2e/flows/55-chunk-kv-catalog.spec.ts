// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: partition/overlay 1.4s, unavailable 0.208s, journal 0.534s, placement 0.321s (2026-10-03).
import { test, expect } from '../fixtures/realBackend';

test('Chunk-KV graph bounds expanded split windows', async ({ page }) => {
  const entries = Array.from({ length: 12 }, (_, i) => ({
    id: i.toString(16).padStart(32, '0'), start: '', end: null, owner_id: '1',
    endpoint: '127.0.0.1:15201', epoch: '2', state: 'Serving', transition_id: null,
    artifact: { tree_id: String(100 + i), stream_name: { high: '1', low: String(i) }, tail_overlay: null },
  }));
  await page.route('**/api/servers', route => route.fulfill({ json: [{ id: 'chunk-kv-1', node_id: 1, rpc_url: '127.0.0.1:15201', service_type: 'chunk-kv' }] }));
  await page.route('**/api/chunk-kv/catalog**', route => route.fulfill({ json: {
    generation: '9', page: 0, offset: 0, catalog_pages: 1, next: null, entries,
  } }));
  await page.route('**/api/chunk-kv/runtime**', route => route.fulfill({ status: 502, json: { error: 'Runtime unavailable in graph fixture' } }));
  await page.goto('/?domain=Chunk-KV');
  const graph = page.getByTestId('chunk-kv-graph');
  await expect(graph.getByRole('button', { name: /^Partition / })).toHaveCount(5);
  await expect(graph.getByRole('button', { name: /^KV Tree for / })).toHaveCount(5);
  await expect(graph.locator('.react-flow__edge')).toHaveCount(11);
  const canvas = await graph.boundingBox();
  expect(canvas!.height).toBeGreaterThanOrEqual(640);
  const root = await graph.getByRole('button', { name: 'Chunk-KV', exact: true }).boundingBox();
  const server = await graph.getByRole('button', { name: 'CKV-1', exact: true }).boundingBox();
  expect(server!.y - root!.y - root!.height).toBeGreaterThan(40);
  await graph.getByRole('button', { name: `KV Tree for ${entries[0].id}`, exact: true }).click();
  await expect(page.getByRole('tab', { name: 'Tree', exact: true })).toHaveAttribute('aria-selected', 'true');
  await expect(page.getByRole('tabpanel', { name: 'Tree' })).toContainText('KV Page tree and key/value inspection are not yet available');
  await graph.getByRole('button', { name: 'Next splits for CKV-1', exact: true }).click();
  await expect(graph.getByRole('button', { name: `Partition ${entries[5].id}`, exact: true })).toBeVisible();
  await expect(graph.getByRole('button', { name: `Partition ${entries[0].id}`, exact: true })).toHaveCount(0);
  await graph.getByRole('button', { name: 'CKV-1', exact: true }).click();
  await expect(graph.getByRole('button', { name: /^Partition / })).toHaveCount(0);
  await graph.getByRole('button', { name: 'CKV-1', exact: true }).click();
  await expect(graph.getByRole('button', { name: /^Partition / })).toHaveCount(5);
});

test('Chunk-KV preserves exact partition identity and separates inherited journal tracks', async ({ page }) => {
  const id = 'ffffffffffffffff0000000000000002';
  const stream = { high: '18446744073709551615', low: '2' };
  const entry = {
    id, start: '80', end: null, owner_id: '42', endpoint: '127.0.0.1:45200', epoch: '9007199254740993', state: 'Serving', transition_id: 'transition-1',
    artifact: { tree_id: '9007199254740995', stream_name: stream, tail_overlay: {
      source_partition_id: { ...stream, low: '1' }, source_epoch: '9007199254740991',
      source_stream_name: { ...stream, low: '3' }, source_stream_manifest_generation: '15',
      replay_offset: '4096', cutover_offset: '8192', cutover_seq: '50',
      base_root_manifest_generation: '7', base_tree_manifest: '6', base_applied_seq: '40', target_stream_start_seq: '51',
    } },
  };
  const requests: string[] = [];
  const runtimeRequests: string[] = [];
  const chunkRequests: string[] = [];
  await page.route('**/api/chunks**', route => {
    chunkRequests.push(new URL(route.request().url()).pathname);
    return route.fulfill({ json: { chunk: { id_hex: id, chunk_type: 4, state: 1, capacity: 4096, sealed_length: 0, strips: [] },
      layout_validity_ms: 1000, observed_at_ms: 1000, placement_observed_at_ms: 1000, placements: [], placement_error: null } });
  });
  let runtimeConflict = false;
  await page.route('**/api/chunk-kv/runtime**', route => {
    runtimeRequests.push(route.request().url());
    const offset = Number(new URL(route.request().url()).searchParams.get('stream_offset') || 0);
    return route.fulfill({ status: runtimeConflict ? 409 : 200, json: runtimeConflict ? { error: 'Owner has a different catalog or writer' } : {
      lifecycle: 'Serving', admitting: true, live_grant: false,
      journal_durable_seq: '9007199254740998', applied_seq: '9007199254740997',
      journal_durable_offset: '18446744073709551615', stream_id: id, observed_at_monotonic_ms: '1234',
      tree: { checkpoint_manifest: '9007199254740995', checkpoint_applied_seq: '9007199254740996',
        runtime: { buffer_pool_resident: '12', buffer_pool_dirty: '3', snapshot_pages_total: '105' }, maintenance: { reclaimed_tree_bytes: '4096', materialization_passes: '2', split_pages_reused: '7' } },
      journal: { generation: '9007199254740999', writer_epoch: entry.epoch, metadata_group_id: '7', trim_offset: '4096', sealed_tail: '8192', closed: false,
        active: { chunk_id: id, physical_start: '64', logical_start: '8192', acknowledged_cursor: '128', capacity: '4096' },
        offset, next_offset: offset === 0 ? 100 : null,
        extent_pages: Array.from({ length: offset === 0 ? 100 : 5 }, (_, i) => ({ page_index: String(offset + i), first_logical: String((offset + i) * 64), end_logical: String((offset + i + 1) * 64) })) },
    } });
  });
  await page.route('**/api/chunk-kv/catalog**', route => {
    requests.push(route.request().url());
    const next = new URL(route.request().url()).searchParams.get('page') === '1';
    return route.fulfill({ status: next ? 409 : 200, json: next ? { error: 'Chunk-KV catalog changed; refresh the range map' } : {
      generation: '9007199254740997', page: 0, offset: 0, catalog_pages: 2, entries: [entry], next: { page: 1, offset: 0 }, source: 'group0',
    } });
  });
  await page.route('**/api/servers', route => route.fulfill({ json: [{ node_id: 1, endpoint: entry.endpoint, health: 'unknown', service_type: 'chunk-kv' }] }));
  await page.goto('/?domain=Chunk-KV');
  await expect(page.getByTestId('domain-chunk-kv')).toHaveAttribute('aria-pressed', 'true');
  await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${id}`, exact: true }).click();
  await expect(page.getByRole('tabpanel', { name: 'Overview' })).toContainText('9007199254740993');
  const runtime = page.getByRole('region', { name: 'Partition runtime', exact: true });
  await expect(runtime).toContainText('18446744073709551615');
  await expect(runtime).toContainText('Absent at observation');
  expect(new URL(runtimeRequests[0]).searchParams.get('epoch')).toBe('9007199254740993');
  expect(new URL(runtimeRequests[0]).searchParams.get('generation')).toBe('9007199254740997');
  await page.getByRole('tab', { name: 'Tree', exact: true }).click();
  const tree = page.getByRole('region', { name: 'Tree storage', exact: true });
  await expect(tree).toContainText('9007199254740996');
  await expect(tree).toContainText('buffer pool resident');
  await expect(tree).toContainText('split pages reused');
  await page.getByRole('tab', { name: 'Journal', exact: true }).click();
  const journal = page.getByRole('tabpanel', { name: 'Journal' });
  await expect(journal.getByRole('heading', { name: 'Inherited parent stream' })).toBeVisible();
  await expect(journal.getByRole('heading', { name: 'Partition journal stream' })).toBeVisible();
  await expect(journal).toContainText('ffffffffffffffff0000000000000003');
  await expect(journal).toContainText('ffffffffffffffff0000000000000002');
  await expect(journal.getByLabel('Extent page map').getByRole('button')).toHaveCount(100);
  await journal.getByRole('button', { name: 'Inspect active Chunk' }).click();
  await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByLabel('Exact Chunk ID')).toHaveValue(id);
  await expect(page.getByRole('heading', { name: id, exact: true })).toBeVisible();
  expect(chunkRequests).toEqual([`/api/chunks/${id}`]);
  await page.getByTestId('domain-chunk-kv').click();
  await expect(journal).toBeVisible();
  await journal.getByRole('button', { name: 'Extent page 0 [0, 64)', exact: true }).click();
  await expect(page.getByLabel('Chunk-KV properties').getByLabel('Selected extent page')).toContainText('64');
  await journal.getByRole('button', { name: 'Next extent pages', exact: true }).click();
  await expect(journal.getByLabel('Extent page map').getByRole('button')).toHaveCount(5);
  expect(new URL(runtimeRequests[runtimeRequests.length - 1]).searchParams.get('stream_generation')).toBe('9007199254740999');
  expect(new URL(runtimeRequests[runtimeRequests.length - 1]).searchParams.get('stream_offset')).toBe('100');
  await expect(page.getByLabel('Chunk-KV properties').getByLabel('Selected extent page')).toHaveCount(0);
  await expect(journal.getByRole('button', { name: 'Next extent pages', exact: true })).toBeDisabled();
  await journal.getByRole('button', { name: 'Extent page 100 [6400, 6464)', exact: true }).click();
  await expect(page.getByLabel('Chunk-KV properties').getByLabel('Selected extent page')).toContainText('6464');
  await journal.getByRole('button', { name: 'Inspect active Chunk' }).click();
  await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(journal).toBeVisible();
  await expect(journal.getByLabel('Extent page map').getByRole('button')).toHaveCount(5);
  await expect(page.getByLabel('Chunk-KV properties').getByLabel('Selected extent page')).toContainText('6464');
  expect(new URL(runtimeRequests[runtimeRequests.length - 1]).searchParams.get('stream_offset')).toBe('100');
  expect(new URL(requests[requests.length - 1]).searchParams.get('generation')).toBe('9007199254740997');
  await journal.getByRole('button', { name: 'Previous extent pages', exact: true }).click();
  await expect(journal.getByLabel('Extent page map').getByRole('button')).toHaveCount(100);
  await page.getByTestId('domain-capacity').click();
  await page.getByTestId('domain-chunk-kv').click();
  await expect(journal).toBeVisible();
  await page.getByRole('button', { name: 'Next partitions', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'catalog changed' })).toBeVisible();
  expect(new URL(requests[requests.length - 1]).searchParams.get('generation')).toBe('9007199254740997');
  await expect(page.getByRole('button', { name: 'Next partitions', exact: true })).toBeDisabled();
  await page.getByRole('button', { name: 'Refresh catalog', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'catalog changed' })).toHaveCount(0);
  await page.getByRole('tab', { name: 'Dependencies', exact: true }).click();
  await expect(page.getByRole('tabpanel', { name: 'Dependencies' })).toContainText('Parent recovery dependency');
  runtimeConflict = true;
  await runtime.getByRole('button', { name: 'Refresh runtime', exact: true }).click();
  await expect(runtime).toContainText('Owner has a different catalog or writer');
  await expect(runtime).not.toContainText('18446744073709551615');
});

test('Chunk-KV unavailable catalog is explicit and can be retried', async ({ page }) => {
  await page.route('**/api/chunk-kv/catalog**', route => route.fulfill({ status: 502, json: { error: 'Group 0 is unavailable' } }));
  await page.goto('/?domain=Chunk-KV');
  await expect(page.getByRole('alert').filter({ hasText: 'Group 0 is unavailable' })).toBeVisible();
  await expect(page.getByLabel('Partition range map')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Refresh catalog', exact: true })).toBeEnabled();
});

test('Chunk-KV rejects stale or oversized journal windows and refreshes from the head', async ({ page }) => {
  const id = '00000000000000010000000000000002';
  let oversized = false;
  await page.route('**/api/chunk-kv/catalog**', route => route.fulfill({ json: {
    generation: '9', page: 0, offset: 0, catalog_pages: 1, next: null, entries: [{
      id, start: '', end: null, owner_id: '7', endpoint: '127.0.0.1:45200', epoch: '2', state: 'Serving', transition_id: null,
      artifact: { tree_id: '1', stream_name: { high: '1', low: '2' }, tail_overlay: null },
    }],
  } }));
  await page.route('**/api/chunk-kv/runtime**', route => {
    const continued = new URL(route.request().url()).searchParams.has('stream_generation');
    return route.fulfill({ json: {
      lifecycle: 'Serving', admitting: true, live_grant: true, journal_durable_seq: '1', applied_seq: '1', journal_durable_offset: '64', stream_id: id, observed_at_monotonic_ms: '1',
      journal: { generation: continued ? '18' : '17', writer_epoch: '2', metadata_group_id: '7', trim_offset: '0', sealed_tail: '64', closed: false, active: null,
        offset: continued ? 100 : 0, next_offset: 100, extent_pages: Array.from({ length: oversized ? 101 : 1 }, (_, i) => ({ page_index: String(i), first_logical: '0', end_logical: '64' })) },
    } });
  });
  await page.goto('/?domain=Chunk-KV');
  await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${id}`, exact: true }).click();
  await page.getByRole('tab', { name: 'Journal', exact: true }).click();
  await page.getByRole('button', { name: 'Next extent pages', exact: true }).click();
  const runtime = page.getByRole('region', { name: 'Partition runtime', exact: true });
  await expect(runtime).toContainText('Stream manifest changed; refresh runtime');
  await expect(page.getByLabel('Extent page map')).toHaveCount(0);
  oversized = true;
  await runtime.getByRole('button', { name: 'Refresh runtime', exact: true }).click();
  await expect(runtime).toContainText('Extent index exceeds 100 entries');
  oversized = false;
  await runtime.getByRole('button', { name: 'Refresh runtime', exact: true }).click();
  await expect(page.getByLabel('Extent page map').getByRole('button')).toHaveCount(1);
  await expect(runtime).not.toContainText('Runtime unavailable');
});

test('Managed Chunk-KV placement uses registered node identity instead of instance identity', async ({ page }) => {
  const id = '00000000000000010000000000000002';
  await page.route('**/api/mode', route => route.fulfill({ json: { mode: 'docker' } }));
  await page.route('**/api/preview', route => route.fulfill({ json: {
    source: 'group0', racks: [{ id: 3 }], nodes: [{ id: 7, rack_id: 3 }], disk_groups: [], disks: [], stores: [], groups: [], replicas: [],
    services: [{ kind: 'chunk-kv', instance_id: '9007199254740993', node_id: 7, endpoint: '127.0.0.1:15201', http_endpoint: 'http://127.0.0.1:15101', monitor: null }, { kind: 'chunk-kv', instance_id: '3', node_id: 7, endpoint: '127.0.0.1:15203', http_endpoint: 'http://127.0.0.1:15103', monitor: null }],
  } }));
  await page.route('**/api/chunk-kv/catalog**', route => route.fulfill({ json: {
    generation: '9', page: 0, offset: 0, catalog_pages: 1, next: null, entries: [{
      id, start: '', end: null, owner_id: '9007199254740993', endpoint: '127.0.0.1:15201', epoch: '2', state: 'Serving', transition_id: null,
      artifact: { tree_id: '1', stream_name: { high: '1', low: '2' }, tail_overlay: null },
    }],
  } }));
  await page.goto('/?domain=Chunk-KV');
  const placement = page.getByRole('navigation', { name: 'Partition placement', exact: true });
  await expect(placement.getByTestId('tree-node-ckv-datacenter').getByRole('button', { name: 'datacenter', exact: true })).toBeVisible();
  await expect(placement).toContainText('R-3', { timeout: 3000 });
  await expect(placement).toContainText('N-7');
  await expect(placement.getByRole('button', { name: 'CKV-9007199254740993', exact: true })).toBeVisible();
  await expect(placement).not.toContainText('Unresolved placement');
  await expect(placement.getByRole('heading')).toHaveCount(0);
  const emptyServer = placement.getByRole('button', { name: 'CKV-3', exact: true });
  await expect(emptyServer).toBeVisible();
  await emptyServer.click();
  await expect(page.getByRole('button', { name: 'All loaded servers', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Filter loaded partition IDs')).toHaveCount(0);
  await expect(page.getByTestId('chunk-kv-graph').getByRole('button', { name: 'CKV-3', exact: true })).toBeVisible();
  const server = placement.getByTestId('tree-node-ckv-server-chunk-kv-9007199254740993');
  await server.getByRole('button', { name: 'Expand', exact: true }).click();
  const split = server.getByRole('button', { name: 'Split 0000…0002', exact: true });
  await expect(split).toHaveAttribute('title', /00000000000000010000000000000002/);
  await split.click();
  await expect(page.getByRole('region', { name: 'Split properties', exact: true })).toContainText(id);
  await page.getByTestId('domain-cluster').click();
  await expect(page.getByRole('button', { name: 'Add Rack', exact: true })).toHaveCount(0);
});

// Baseline: native graph 1.4s (2026-10-04).
// Native cluster is owned by native_cluster_provisioning_test; no response interception.
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
    await page.getByRole('button', { name: 'KV', exact: true }).click();
    await observeCatalog(() => page.getByRole('button', { name: 'Chunk-KV', exact: true }).click());
    await verify();
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
    await observeCatalog(() => page.getByRole('button', { name: 'Refresh catalog', exact: true }).click());
    await expect(page.getByRole('button', { name: 'Refresh catalog', exact: true })).toBeEnabled();
    await verify();
  });
});
