// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

// Baseline: 9.1s (2026-10-04), 21 actual chunks and owner interruption.
test('native diagnostics: Chunk replacement windows and real Mirror EC placement', async ({ page, request }) => {
  const fixture = JSON.parse(process.env.CROWDB_NATIVE_CHUNK_FIXTURE ?? 'null');
  expect(fixture, 'Run with CROWDB_NATIVE_DATA_WINDOWS=1').not.toBeNull();
  await page.goto('/?domain=Chunk');
  const rows = page.getByRole('table', { name: 'Chunks' }).getByRole('button');
  const pages = page.getByRole('navigation', { name: 'Chunk page window' });
  let after: string | null = null;
  const seen = new Set<string>();
  for (let window = 0; window < 20; window++) {
    const response = await request.get(`/api/chunks?limit=10${after ? `&after=${after}` : ''}`);
    expect(response.ok(), await response.text()).toBeTruthy();
    const data = await response.json();
    expect(data.failures).toEqual([]);
    expect(data.scanned).toBeLessThanOrEqual(10);
    const expected = data.chunks.map((chunk: { id_hex: string }) => chunk.id_hex);
    await expect(rows).toHaveCount(expected.length);
    await expect.poll(() => rows.allTextContents(), { intervals: [100] }).toEqual(expected);
    for (const id of expected) { expect(seen.has(id)).toBe(false); seen.add(id); }
    if (!data.next) { await expect(pages.getByRole('button', { name: 'Next', exact: true })).toBeDisabled(); break; }
    after = data.next;
    await pages.getByRole('button', { name: 'Next', exact: true }).click();
  }
  for (const id of fixture.ids) expect(seen.has(id), id).toBe(true);
  await pages.getByRole('button', { name: 'Prev', exact: true }).click();
  await expect(pages).not.toContainText('Window 1 ·');
  for (const [id, expectedStrips, blocks] of [[fixture.mirror, 17, 3], [fixture.ec, 3, 12]] as const) {
    await page.getByLabel('Exact Chunk ID').fill(id);
    await page.getByRole('button', { name: 'Lookup ID', exact: true }).click();
    const response = await request.get(`/api/chunks/${id}`);
    expect(response.ok(), await response.text()).toBeTruthy();
    const data = await response.json();
    expect(data.chunk.strips).toHaveLength(expectedStrips);
    const ordered = [...data.chunk.strips].sort((a, b) => a.chunk_offset - b.chunk_offset || a.strip_sequence - b.strip_sequence);
    const layout = page.getByLabel('Chunk strips', { exact: true });
    await expect(layout.getByTestId('chunk-strip')).toHaveCount(Math.min(16, expectedStrips));
    for (const strip of ordered.slice(0, 16)) {
      const card = layout.getByTestId('chunk-strip').filter({ has: page.getByRole('button', { name: new RegExp(`^Sequence ${strip.strip_sequence} ·`) }) });
      await expect(card.getByTestId('chunk-disk-block')).toHaveCount(blocks);
      const segments = strip.strip.MirrorStrip?.segments ?? strip.strip.EcStrip.segments;
      for (let index = 0; index < segments.length; index++) {
        const segment = segments[index];
        const disk = BigInt(segment.disk_id.high).toString(16).padStart(16, '0') + BigInt(segment.disk_id.low).toString(16).padStart(16, '0');
        const placement = data.placements.find((value: { disk_id: string }) => value.disk_id.replace(/-/g, '') === disk);
        expect(placement).toBeDefined();
        await card.getByTestId('chunk-disk-block').nth(index).click();
        const properties = page.getByLabel('Chunk properties', { exact: true });
        for (const [label, value] of [['Disk', disk], ['Node', placement.node_id], ['Diskgroup', placement.disk_group_id], ['Zone', segment.zone_index], ['Zone offset (units)', segment.unit_offset]]) {
          await expect(properties.locator('dt').filter({ hasText: new RegExp(`^${label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) }).locator('..').locator('dd')).toHaveText(String(value));
        }
      }
    }
    if (expectedStrips > 16) {
      await page.getByRole('button', { name: 'Next 16 strips', exact: true }).click();
      await expect(layout.getByTestId('chunk-strip')).toHaveCount(1);
      await expect(layout).toContainText(`Strip ${ordered[16].strip_sequence}`);
      await page.getByTestId('domain-kv').click(); await page.goBack();
      await expect(layout.getByTestId('chunk-strip')).toHaveCount(1);
      await page.getByRole('button', { name: 'Previous strips', exact: true }).click();
      await expect(layout.getByTestId('chunk-strip')).toHaveCount(16);
    }
  }
  expect((await request.post('/api/services/chunkdb-1/stop', { data: {} })).ok()).toBe(true);
  try {
    await page.getByRole('button', { name: 'Refresh chunks', exact: true }).click();
    await expect(page.getByRole('alert').filter({ hasText: 'Partial result' })).toBeVisible();
    await expect(pages.getByRole('button', { name: 'Next', exact: true })).toBeDisabled();
  } finally {
    expect((await request.post('/api/services/chunkdb-1/restart', { data: {} })).ok()).toBe(true);
  }
  await page.getByRole('button', { name: 'Refresh chunks', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'Partial result' })).toHaveCount(0);
});
