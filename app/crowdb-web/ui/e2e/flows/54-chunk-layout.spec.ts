// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: layout 1.0s, bounded strips 0.543s, scan 0.730s (2026-10-03)
import { test, expect } from '../fixtures/realBackend';

test('Chunk browser renders stable Mirror and EC sequences with exact placement identities', async ({ page }) => {
  const id = '0500000000000000ffffffffffffffff';
  const segment = { disk_id: { high: '18446744073709551615', low: '18446744073709551615' }, zone_index: 2, unit_offset: '9007199254740993', unit_count: 1, allocation_ts: '18446744073709551615' };
  const strips = [
    { strip_sequence: 9, chunk_offset: 1024, capacity: 1024, unit_kb: 4, sealed_length: 0, strip_type: 1, strip: { EcStrip: { segments: [segment, { ...segment, zone_index: 3 }], data_num: 1, code_num: 1, ec_state: 1 } }, unavailable_segments: [segment], placement_repair_required: true },
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
  await expect(mirror.getByTestId('chunk-disk-block')).toContainText('Copy 1');
  for (const copies of [2, 3]) {
    const blocks = layout.getByTestId('chunk-strip').filter({ hasText: `Strip ${10 + copies}` }).getByTestId('chunk-disk-block');
    await expect(blocks).toHaveCount(copies);
    await expect(blocks.nth(copies - 1)).toContainText(`Copy ${copies}`);
  }
  const ec = layout.getByTestId('chunk-strip').filter({ hasText: 'Strip 9' });
  await expect(ec.getByTestId('chunk-disk-block')).toHaveCount(2);
  await expect(ec.getByTestId('chunk-disk-block').nth(0)).toContainText('Data 0 · unavailable');
  await expect(ec.getByTestId('chunk-disk-block').nth(0)).toContainText('N1 / DG3');
  await expect(ec.getByTestId('chunk-disk-block').nth(1)).toContainText('Parity 0');
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
  await page.getByTestId('domain-chunk').click();
  await expect(page.getByLabel('Chunk type')).toHaveValue('');
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
    const offset = url.searchParams.has('after') ? 20 : 0;
    const kind = url.searchParams.get('chunk_type') ?? '0';
    const chunks = Array.from({ length: 20 }, (_, index) => ({ id_hex: Number(kind).toString(16).padStart(2, '0') + (offset + index).toString(16).padStart(30, '0'), chunk_type: Number(kind), state: 1, strips: [] }));
    return route.fulfill({ json: { chunks, scanned: 20, next: chunks[19].id_hex, owners: 1, failures: [], observed_at_ms: 1000 } });
  });
  await page.goto('/?domain=Cluster');
  await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
  expect(requests).toHaveLength(0);
  await page.getByTestId('domain-chunk').click();
  const rows = page.getByRole('table', { name: 'Chunks' }).getByRole('button');
  await expect(rows).toHaveCount(20);
  expect(requests[0].searchParams.has('prefix')).toBe(false);
  expect(requests[0].searchParams.get('limit')).toBe('20');
  const pagination = page.getByRole('navigation', { name: 'Chunk page window' });
  await expect(pagination.getByRole('button', { name: 'Prev', exact: true })).toBeDisabled();
  await pagination.getByRole('button', { name: 'Next', exact: true }).click();
  await expect(rows.nth(0)).toHaveText('00000000000000000000000000000014');
  await expect(rows).toHaveCount(20);
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
  await expect(rows).toHaveCount(20);
  expect(requests).toHaveLength(4);
  await page.getByLabel('Filter current window').fill('04000000000000000000000000000000');
  await expect(rows).toHaveCount(1);
  expect(requests).toHaveLength(4);
});
