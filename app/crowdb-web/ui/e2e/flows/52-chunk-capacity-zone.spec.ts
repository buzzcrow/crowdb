// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: actual Zone windows and allocation pixels 1.3s (2026-10-04).
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: Zone canvas colors match every actual allocation bit', async ({ page, request }) => {
  const { step } = await import('../fixtures/stepTimer');
  const response = await request.get('/api/diskdb/usage?dg=1&disk=00000000000000000000000000000001&zone=0');
  expect(response.status()).toBe(200);
  const usage = await response.json();
  const zone = usage.disk_groups[0].disks[0].zone_usages[0];
  const total = zone.busy_block_count + zone.free_block_count;
  const bytes = Buffer.from(zone.usage_bitmap, 'hex');
  let states = Array.from({ length: total }, (_, index) => (bytes[index >> 3] >> (index % 8)) & 1);
  expect(states.reduce((sum, bit) => sum + bit, 0)).toBe(zone.busy_block_count);
  expect(zone.busy_block_count).toBeGreaterThan(0);
  expect(zone.free_block_count).toBeGreaterThan(0);
  await step('native zone DOM setup', async () => {
    await page.goto('/?domain=Capacity');
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible();
  });
  // Expand the actual DDB/DG hierarchy rather than injecting a selection.
  const tree = page.getByRole('tree');
  const disk = tree.getByRole('button', { name: '000000000000…', exact: true });
  for (const name of [/^R-1/, /^N-1$/, /^storage \(DG-1\)$/]) {
    const row = tree.getByRole('treeitem').filter({ has: page.getByRole('button', { name }) });
    await expect(row).toBeVisible();
    const expand = row.getByRole('button', { name: 'Expand', exact: true });
    if (await expand.count()) await expand.click();
  }
  await disk.click();
  // Compare pixels with the actual response consumed by the UI. Background
  // allocation can legitimately change the bitmap after the initial probe.
  const bitmapResponse = page.waitForResponse(response => {
    const url = new URL(response.url());
    return url.pathname === '/api/diskdb/usage' && url.searchParams.get('dg') === '1'
      && url.searchParams.get('disk')?.replaceAll('-', '') === '00000000000000000000000000000001'
      && url.searchParams.get('zone') === '0';
  });
  await page.getByRole('button', { name: 'Zone 0', exact: true }).click();
  const displayedResponse = await bitmapResponse;
  expect(displayedResponse.status()).toBe(200);
  const displayedUsage = await displayedResponse.json();
  const displayedZone = displayedUsage.disk_groups[0].disks[0].zone_usages[0];
  expect(displayedZone.busy_block_count + displayedZone.free_block_count).toBe(total);
  const displayedBytes = Buffer.from(displayedZone.usage_bitmap, 'hex');
  states = Array.from({ length: total }, (_, index) => (displayedBytes[index >> 3] >> (index % 8)) & 1);
  expect(states.reduce((sum, bit) => sum + bit, 0)).toBe(displayedZone.busy_block_count);
  await step('native zone bitmap DOM and pixels', async () => {
    const bitmap = page.getByTestId('zone-bitmap');
    await expect(bitmap).toBeVisible();
    const firstWindow = states.slice(0, 4096);
    await expect(bitmap.getByLabel('Displayed block window usage')).toContainText(`${firstWindow.reduce((sum, bit) => sum + bit, 0)} used`);
    const canvas = bitmap.getByLabel('Zone block usage');
    const colors = await canvas.evaluate((element, count) => {
      const canvas = element as HTMLCanvasElement;
      const context = canvas.getContext('2d')!;
      return Array.from({ length: count }, (_, index) => {
        const rgba = context.getImageData((index % 64) * 6 + 2, Math.floor(index / 64) * 6 + 2, 1, 1).data;
        return [...rgba].join(',');
      });
    }, Math.min(total, 4096));
    expect(colors).toEqual(states.slice(0, 4096).map(bit => bit ? '85,127,165,255' : '82,125,104,255'));
    expect(total).toBeGreaterThan(4096);
    await bitmap.getByRole('button', { name: 'Next blocks', exact: true }).click();
    await expect(bitmap).toContainText(`Blocks 4096–${total - 1} of ${total}`);
    const secondColors = await canvas.evaluate((element, count) => {
      const context = (element as HTMLCanvasElement).getContext('2d')!;
      return Array.from({ length: count }, (_, index) => [...context.getImageData((index % 64) * 6 + 2, Math.floor(index / 64) * 6 + 2, 1, 1).data].join(','));
    }, total - 4096);
    expect(secondColors).toEqual(states.slice(4096).map(bit => bit ? '85,127,165,255' : '82,125,104,255'));
    await expect(bitmap.getByRole('button', { name: 'Next blocks', exact: true })).toBeDisabled();
    await bitmap.getByRole('button', { name: 'Previous blocks', exact: true }).click();
    await expect(bitmap).toContainText('Blocks 0–4095');
  });
  await step('native zone replacement pages', async () => {
    const zones = page.getByRole('group', { name: 'Disk zones', exact: true });
    const zoneButtons = zones.getByRole('button', { name: /^Zone \d+$/ });
    await expect(zoneButtons).toHaveCount(32);
    await expect(zones.getByRole('button', { name: 'Previous zones', exact: true })).toBeDisabled();
    await zones.getByRole('button', { name: 'Next zones', exact: true }).click();
    await expect(zoneButtons).toHaveCount(32);
    await expect(zones.getByRole('button', { name: 'Zone 32', exact: true })).toBeVisible();
    await expect(zones.getByRole('button', { name: 'Zone 0', exact: true })).toHaveCount(0);
    await zones.getByRole('button', { name: 'Next zones', exact: true }).click();
    await expect(zoneButtons).toHaveCount(16);
    await expect(zones.getByRole('button', { name: 'Zone 79', exact: true })).toBeVisible();
    await page.getByTestId('domain-chunk').click();
    await page.goBack();
    await expect(zoneButtons).toHaveCount(16);
    await expect(zones.getByRole('button', { name: 'Zone 79', exact: true })).toBeVisible();

    await expect(zones.getByRole('button', { name: 'Next zones', exact: true })).toBeDisabled();
    await zones.getByRole('button', { name: 'Previous zones', exact: true }).click();
    await zones.getByRole('button', { name: 'Previous zones', exact: true }).click();
    await expect(zones.getByRole('button', { name: 'Zone 0', exact: true })).toHaveAttribute('aria-pressed', 'true');
  });
});
