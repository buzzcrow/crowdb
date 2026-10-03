// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: layout 1.0s, bounded strips 0.543s, scan 0.730s (2026-10-03)
import { test, expect } from '../fixtures/realBackend';

test('Chunk browser renders stable Mirror and EC sequences with exact placement identities', async ({ page }) => {
  const id = '0500000000000000ffffffffffffffff';
  const segment = { disk_id: { high: '18446744073709551615', low: '18446744073709551615' }, zone_index: 2, unit_offset: '9007199254740993', unit_count: 1, allocation_ts: '18446744073709551615' };
  const strips = [
    { strip_sequence: 9, chunk_offset: 1024, capacity: 1024, unit_kb: 4, sealed_length: 0, strip_type: 1, strip: { EcStrip: { segments: Array.from({ length: 12 }, (_, index) => ({ ...segment, zone_index: index + 2 })), data_num: 8, code_num: 4, ec_state: 1 } }, unavailable_segments: [segment], placement_repair_required: true },
    { strip_sequence: 3, chunk_offset: 0, capacity: 1024, unit_kb: 4, sealed_length: 1024, strip_type: 0, strip: { MirrorStrip: { segments: [segment] } }, unavailable_segments: [], placement_repair_required: false },
    ...[2, 3].map(copies => ({ strip_sequence: 10 + copies, chunk_offset: copies * 1024, capacity: 1024, unit_kb: 4, sealed_length: 0, strip_type: 0, strip: { MirrorStrip: { segments: Array.from({ length: copies }, (_, index) => ({ ...segment, zone_index: index })) } }, unavailable_segments: [], placement_repair_required: false })),
  ];
  const chunk = { id_hex: id, chunk_type: 5, state: 1, modify_ts: '18446744073709551615', capacity: 2048, sealed_length: 1024, acknowledged_cursor: '9007199254740993', writer_epoch: '18446744073709551615', strips };
  await page.route('**/api/chunks**', route => {
    const url = new URL(route.request().url());
    return route.fulfill({ json: url.pathname === '/api/chunks' ? { chunks: [chunk], scanned: 1, next: null, owners: 1, failures: [], observed_at_ms: 1000 }
      : { chunk, layout_validity_ms: 1000, observed_at_ms: 1000, placement_observed_at_ms: 1100, placement_error: null, placements: [{ disk_id: 'ffffffffffffffffffffffffffffffff', rack_id: '1', node_id: '1', disk_group_id: '3', unit_size: 4096 }] } });
  });
  await page.goto('/');
  await page.getByTestId('domain-chunk').click();
  await expect(page.getByLabel('Chunk ID prefix')).toHaveCount(0);
  await page.getByRole('table', { name: 'Chunks' }).getByRole('button', { name: id, exact: true }).click();
  const layout = page.getByLabel('Chunk strips');
  await expect(layout.getByTestId('chunk-strip')).toHaveCount(4);
  await expect(layout.getByTestId('chunk-strip').nth(0)).toContainText('Strip 3');
  const mirror = layout.getByTestId('chunk-strip').filter({ hasText: 'Strip 3' });
  await expect(mirror.getByTestId('chunk-disk-block')).toContainText('Mirror 1');
  await expect(mirror.locator('.chunk-strip-heading')).toHaveText('Strip 3 · Mirror[0M, 1M)');
  expect(await mirror.evaluate(el => el.getBoundingClientRect().height)).toBeLessThan(65);
  for (const copies of [2, 3]) {
    const blocks = layout.getByTestId('chunk-strip').filter({ hasText: `Strip ${10 + copies}` }).getByTestId('chunk-disk-block');
    await expect(blocks).toHaveCount(copies);
    await expect(blocks.nth(copies - 1)).toContainText(`Mirror ${copies}`);
  }
  const ec = layout.getByTestId('chunk-strip').filter({ hasText: 'Strip 9' });
  await expect(ec.getByTestId('chunk-disk-block')).toHaveCount(12);
  await expect(ec.getByTestId('chunk-disk-block').nth(0)).toHaveAccessibleName('Data 0 · unavailable');
  await expect(ec.getByTestId('chunk-disk-block').nth(0)).toHaveText('Data 0');
  expect(await ec.locator('.chunk-blocks').evaluate(el => el.scrollWidth === el.clientWidth)).toBe(true);
  await expect(ec.getByTestId('chunk-disk-block').nth(8)).toContainText('Parity 0');
  await ec.getByTestId('chunk-disk-block').nth(0).click();
  const properties = page.getByLabel('Chunk properties');
  await expect(properties).toContainText('ffffffffffffffffffffffffffffffff');
  await expect(properties).toContainText('9007199254740993');
  await expect(properties).toContainText('36893488147419107328 bytes');
  await expect(properties).not.toContainText('Parity 0');
  await expect(ec.getByTestId('chunk-disk-block').nth(0)).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByRole('table', { name: 'Chunks' })).toBeVisible();
  await page.getByTestId('domain-iceberg').click();
  await page.getByTestId('domain-chunk').click();
  await expect(page.getByLabel('Chunk type')).toHaveValue('');
  await expect(layout.getByRole('button', { name: /Sequence 9/ })).toBeVisible();
  await page.getByRole('button', { name: 'Show disk capacity', exact: true }).nth(0).click();
  await expect(page.getByTestId('domain-capacity')).toHaveAttribute('aria-pressed', 'true');
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByLabel('Chunk type')).toHaveValue('');
  await expect(ec.getByTestId('chunk-disk-block').nth(0)).toHaveAttribute('aria-pressed', 'true');
  await expect(properties).toContainText('9007199254740993');
  await page.getByRole('button', { name: 'Forward', exact: true }).click();
  await expect(page.getByTestId('domain-capacity')).toHaveAttribute('aria-pressed', 'true');
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByTestId('chunk-tab-chunk')).toHaveCount(0);
});


test('Large Chunk renders bounded strip pages and retains selected sequence', async ({ page }) => {
  const id = '05000000000000000000000000000001';
  const strips = Array.from({ length: 45 }, (_, index) => ({ strip_sequence: index * 3, chunk_offset: index * 1024, capacity: 1024, unit_kb: 4, sealed_length: 0, strip_type: 0, strip: { MirrorStrip: { segments: [] } }, unavailable_segments: [], placement_repair_required: false }));
  await page.route('**/api/chunks?**', route => route.fulfill({ json: { chunks: [], scanned: 0, next: null, owners: 1, failures: [], observed_at_ms: 1000 } }));
  await page.route('**/api/chunks/**', route => route.fulfill({ json: { chunk: { id_hex: id, chunk_type: 5, state: 1, strips }, observed_at_ms: 1000, placement_observed_at_ms: 1000, layout_validity_ms: 1000, placements: [], placement_error: null } }));
  await page.goto('/?domain=Chunk');
  await page.getByLabel('Exact Chunk ID').fill(id);
  await page.getByRole('button', { name: 'Lookup ID', exact: true }).click();
  const layout = page.getByLabel('Chunk strips');
  await expect(layout.getByRole('button')).toHaveCount(16);
  await layout.getByRole('button', { name: /Sequence 0 ·/ }).click();
  await page.getByRole('button', { name: 'Next 16 strips', exact: true }).click();
  await expect(layout.getByRole('button')).toHaveCount(16);
  await expect(layout.getByTestId('chunk-strip').nth(0)).toContainText('Strip 48');
  await expect(page.getByRole('heading', { name: 'Strip sequence 0', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Next 16 strips', exact: true }).click();
  await expect(layout.getByRole('button')).toHaveCount(13);
  await page.getByRole('button', { name: 'Previous strips', exact: true }).click();
  await expect(layout.getByRole('button')).toHaveCount(16);
});


test('Chunk auto scan stays bounded and type changes replace the current window', async ({ page }) => {
  const requests: URL[] = [];
  await page.route('**/api/chunks?**', route => {
    const url = new URL(route.request().url());
    requests.push(url);
    const offset = url.searchParams.has('after') ? 10 : 0;
    const kind = url.searchParams.get('chunk_type') ?? '0';
    const chunks = Array.from({ length: 10 }, (_, index) => ({ id_hex: Number(kind).toString(16).padStart(2, '0') + (offset + index).toString(16).padStart(30, '0'), chunk_type: Number(kind), state: 1, strips: [] }));
    return route.fulfill({ json: { chunks, scanned: 10, next: chunks[9].id_hex, owners: 1, failures: [], observed_at_ms: 1000 } });
  });
  await page.goto('/?domain=Cluster');
  await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
  expect(requests).toHaveLength(0);
  await page.getByTestId('domain-chunk').click();
  const rows = page.getByRole('table', { name: 'Chunks' }).getByRole('button');
  await expect(rows).toHaveCount(10);
  await expect(page.getByRole('navigation', { name: 'Chunk hierarchy' }).getByLabel('Exact Chunk ID')).toHaveCount(0);
  await expect(page.getByRole('form', { name: 'Exact chunk lookup' })).toBeVisible();
  expect(await page.locator('.chunk-list').evaluate(el => el.scrollHeight === el.clientHeight)).toBe(true);
  await expect(page.getByTestId('chunk-page').getByText('Session activity', { exact: true })).not.toBeVisible();
  expect(requests[0].searchParams.has('prefix')).toBe(false);
  expect(requests[0].searchParams.get('limit')).toBe('10');
  const pagination = page.getByRole('navigation', { name: 'Chunk page window' });
  await expect(pagination.getByRole('button', { name: 'Prev', exact: true })).toBeDisabled();
  await pagination.getByRole('button', { name: 'Next', exact: true }).click();
  await expect(rows.nth(0)).toHaveText('0000000000000000000000000000000a');
  await expect(rows).toHaveCount(10);
  await expect(pagination).toContainText('Window 2');
  await pagination.getByRole('button', { name: 'Prev', exact: true }).click();
  await expect(rows.nth(0)).toHaveText('00000000000000000000000000000000');
  await expect(pagination).toContainText('Window 1');
  expect(requests[2].searchParams.has('after')).toBe(false);
  await page.getByLabel('Chunk type').selectOption('4');
  await expect(rows.nth(0)).toHaveText('04000000000000000000000000000000');
  expect(requests[3].searchParams.has('after')).toBe(false);
  expect(requests[3].searchParams.get('chunk_type')).toBe('4');
  await page.getByTestId('domain-capacity').click();
  await page.getByTestId('domain-chunk').click();
  await expect(rows).toHaveCount(10);
  expect(requests).toHaveLength(4);
  await page.getByLabel('Filter current window').fill('04000000000000000000000000000000');
  await expect(rows).toHaveCount(1);
  expect(requests).toHaveLength(4);
});


test('Chunk hierarchy lazily separates service slots and storage slots without replicas', async ({ page }) => {
  await page.route('**/api/racks*', route => route.fulfill({ json: [{ id: 1, name: '' }] }));
  await page.route('**/api/nodes?*', route => route.fulfill({ json: [{ id: 1, rack_id: 1, host: '127.0.0.1' }] }));
  await page.route('**/api/nodes', route => route.fulfill({ json: [{ id: 1, rack_id: 1, host: '127.0.0.1' }] }));
  await page.route('**/api/servers', route => route.fulfill({ json: [{ id: '1', node_id: 1, service_type: 'kv', health: 'healthy' }, { id: 'chunkdb-1', node_id: 1, service_type: 'chunkdb', health: 'healthy' }] }));
  let reads = 0;
  await page.route('**/api/nodes/1/stores?*', route => { reads++; return route.fulfill({ json: [{ node_id: 1, store_id: 0, groups: [] }] }); });
  await page.route('**/api/nodes/1/stores/0/groups?*', route => route.fulfill({ json: [{ node_id: 1, store_id: 0, group_id: 1 }] }));
  await page.route('**/api/chunks?*', route => route.fulfill({ json: { chunks: [], scanned: 0, next: null, owners: 1, failures: [], observed_at_ms: 1000 } }));
  const slotRequests: URL[] = [];
  await page.route('**/api/chunk-slots?*', route => {
    const url = new URL(route.request().url()); slotRequests.push(url);
    expect(url.searchParams.get('limit')).toBe('32');
    const next = url.searchParams.has('after');
    if (next) expect(url.searchParams.get('generation')).toBe('9007199254740993');
    return route.fulfill({ json: { generation: '9007199254740993', assigned: true, owned_count: 64,
      slots: Array.from({ length: 32 }, (_, index) => index * 2 + (next ? 64 : 0)), next: next ? null : 62 } });
  });
  await page.goto('/?domain=Chunk');
  const tree = page.getByRole('navigation', { name: 'Chunk hierarchy' });
  await expect(tree.getByRole('button', { name: 'datacenter', exact: true })).toBeVisible();
  await tree.getByTestId('tree-node-chunk-node-1').getByRole('button', { name: 'Expand', exact: true }).click();
  expect(reads).toBe(0);
  await expect(tree.getByRole('button', { name: 'CDB-1', exact: true })).toBeVisible();
  expect(slotRequests).toHaveLength(0);
  const cdb = tree.getByTestId('tree-node-chunk-server-chunkdb-1');
  await cdb.getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(cdb.getByLabel('Slot ownership')).toContainText('64 service slots');
  await expect(cdb.getByLabel('Owned slots').locator('span')).toHaveCount(32);
  expect(slotRequests[0].searchParams.get('instance_id')).toBe('1');
  await cdb.getByRole('button', { name: 'Next', exact: true }).click();
  await expect(cdb.getByLabel('Owned slots')).toContainText('64');
  await expect(cdb.getByRole('button', { name: 'Next', exact: true })).toBeDisabled();
  await cdb.getByRole('button', { name: 'Previous', exact: true }).click();
  await expect(cdb.getByLabel('Owned slots').locator('span').nth(0)).toHaveText('0');
  await tree.getByTestId('tree-node-chunk-server-1').getByRole('button', { name: 'Expand', exact: true }).click();
  await tree.getByTestId('tree-node-chunk-store-1-0').getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(tree.getByRole('button', { name: 'G-1', exact: true })).toBeVisible();
  const group = tree.getByTestId('tree-node-chunk-group-1-0-1');
  await group.getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(group.getByLabel('Slot ownership')).toContainText('64 storage slots');
  expect(slotRequests.at(-1)!.searchParams.get('store_id')).toBe('0');
  expect(slotRequests.at(-1)!.searchParams.get('group_id')).toBe('1');
  await expect(tree).not.toContainText('Chunk placement');
  await expect(tree).not.toContainText('Replica');
  expect(reads).toBe(1);
});
