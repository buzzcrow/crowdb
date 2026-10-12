// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: Capacity retains hardware and marks interrupted DDB usage unknown', async ({ page, request }) => {
  await page.goto('/?domain=Capacity');
  const totals = page.getByTestId('capacity-summary');
  const panel = totals.locator('..');
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(0);
  expect((await request.post('/api/nodes/1/diskdb/stop', { data: {} })).ok()).toBe(true);
  try {
    await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await expect(page.getByRole('alert').filter({ hasText: 'Capacity observation unavailable' })).toBeVisible();
    await panel.getByRole('button', { name: /R-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await panel.getByRole('button', { name: /N-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await panel.getByRole('button', { name: /DG-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    const disk = panel.getByRole('button', { name: /00000000…/ });
    await expect(disk).toHaveAttribute('title', /Usage unknown/);
    await disk.click();
    await expect(page.getByTestId('disk-geometry')).toContainText(/80 zones.*Usage unknown/);
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await expect(totals).not.toContainText('100% free');
  } finally {
    expect((await request.post('/api/nodes/1/diskdb/restart', { data: {} })).ok()).toBe(true);
  }
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(0);
  await expect(page.getByTestId('disk-geometry')).not.toContainText('Usage unknown');
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
});

// Baseline: new actual native scanner and scoped recalc (2026-10-04).
test('native diagnostics: scanner and scoped recalc show actual completed observations', async ({ page }) => {
  await page.goto('/?domain=Capacity');
  const panel = page.getByTestId('capacity-summary').locator('..');
  await expect(panel.getByText('No scan has been run yet.', { exact: true })).toBeVisible();
  const scanResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/diskdb/scan' && response.request().method() === 'POST');
  await panel.getByRole('button', { name: 'Run Scan', exact: true }).click();
  const scan = await scanResponse;
  expect(scan.ok(), await scan.text()).toBe(true);
  await expect(panel.getByText('Zones scanned', { exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Run Scan', exact: true })).toBeEnabled();
  await panel.getByRole('button', { name: /R-1 / }).click();
  await expect(panel.getByText('Scanner', { exact: true })).toHaveCount(0);
  await panel.getByRole('button', { name: /N-1 / }).click();
  await panel.getByRole('button', { name: /DG-1 / }).click();
  await panel.getByRole('button', { name: /00000000…/ }).click();
  await expect(page.getByTestId('disk-geometry')).toContainText('80 zones');
  const recalcResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/diskdb/recalc' && response.request().method() === 'POST');
  await panel.getByRole('button', { name: 'Run Recalc', exact: true }).click();
  const recalc = await recalcResponse;
  expect(recalc.ok(), await recalc.text()).toBe(true);
  const observation = await recalc.json();
  expect(observation.results.map((row: { disk_group_id: number }) => row.disk_group_id)).toEqual([1]);
  expect(observation.results.flatMap((row: { zones: { drift_detected: boolean }[] }) => row.zones).every((zone: { drift_detected: boolean }) => !zone.drift_detected)).toBe(true);
  await expect(panel.getByText('No drift detected. All zones match.', { exact: true })).toBeVisible();
  const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
  await expect(inspector.getByText('Name', { exact: true })).toHaveCount(0);
  await expect(inspector.getByText('Parent: disk_id', { exact: true })).toHaveCount(0);
  for (const parent of ['rack_id', 'node_id', 'disk_group_id']) await expect(inspector.getByText(`Parent: ${parent}`, { exact: true })).toBeVisible();
});
